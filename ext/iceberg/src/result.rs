use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType as ArrowDataType;
use arrow_array::RecordBatch;
use magnus::{Class, IntoValue, Module, RArray, RClass, RModule, Ruby, Value};

use crate::RbResult;
use crate::error::todo_error;

pub fn collect_batches(ruby: &Ruby, batches: Vec<RecordBatch>) -> RbResult<Value> {
    let columns = ruby.ary_new();
    let rows = ruby.ary_new();

    for batch in batches {
        if columns.is_empty() {
            for field in &batch.schema().fields {
                columns.push(ruby.str_new(field.name()))?;
            }
        }

        for _ in 0..batch.num_rows() {
            rows.push(ruby.ary_new())?;
        }

        for column in batch.columns() {
            match column.data_type() {
                ArrowDataType::Boolean => collect_column_boolean(ruby, column, rows)?,
                ArrowDataType::Int8 => collect_column_int8(ruby, column, rows)?,
                ArrowDataType::Int16 => collect_column_int16(ruby, column, rows)?,
                ArrowDataType::Int32 => collect_column_int32(ruby, column, rows)?,
                ArrowDataType::Int64 => collect_column_int64(ruby, column, rows)?,
                ArrowDataType::UInt8 => collect_column_uint8(ruby, column, rows)?,
                ArrowDataType::UInt16 => collect_column_uint16(ruby, column, rows)?,
                ArrowDataType::UInt32 => collect_column_uint32(ruby, column, rows)?,
                ArrowDataType::UInt64 => collect_column_uint64(ruby, column, rows)?,
                ArrowDataType::Float32 => collect_column_float32(ruby, column, rows)?,
                ArrowDataType::Float64 => collect_column_float64(ruby, column, rows)?,
                ArrowDataType::Utf8 => collect_column_utf8(ruby, column, rows)?,
                _ => return Err(todo_error()),
            }
        }
    }

    ruby.class_object()
        .const_get::<_, RModule>("Iceberg")?
        .const_get::<_, RClass>("Result")?
        .new_instance((columns, rows))
}

macro_rules! collect_column {
    ($name:ident, $type:ty) => {
        pub fn $name(ruby: &Ruby, column: &Arc<dyn Array>, rows: RArray) -> RbResult<()> {
            let array = column.as_any().downcast_ref::<$type>().unwrap();
            for (i, value) in array.iter().enumerate() {
                rows.entry::<RArray>(i.try_into().unwrap())?
                    .push(value.map(|v| v.into_value_with(ruby)))?;
            }
            Ok(())
        }
    };
}

collect_column!(collect_column_boolean, BooleanArray);
collect_column!(collect_column_int8, Int8Array);
collect_column!(collect_column_int16, Int16Array);
collect_column!(collect_column_int32, Int32Array);
collect_column!(collect_column_int64, Int64Array);
collect_column!(collect_column_uint8, UInt8Array);
collect_column!(collect_column_uint16, UInt16Array);
collect_column!(collect_column_uint32, UInt32Array);
collect_column!(collect_column_uint64, UInt64Array);
collect_column!(collect_column_float32, Float32Array);
collect_column!(collect_column_float64, Float64Array);
collect_column!(collect_column_utf8, StringArray);

use std::sync::Arc;

use arrow::array::{
    Array, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
    Int64Builder, LargeBinaryBuilder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::DataType as ArrowDataType;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use magnus::{RArray, RHash, RString, Ruby, TryConvert, Value, value::ReprValue};

use crate::RbResult;
use crate::capsule::RbCapsule;
use crate::error::todo_error;
use crate::utils::{Wrap, date_to_i32};

#[magnus::wrap(class = "Iceberg::ArrowRecordBatch")]
pub struct RbArrowRecordBatch {
    pub(crate) batch: RecordBatch,
}

impl RbArrowRecordBatch {
    pub fn new(ruby: &Ruby, data: RArray, schema: Wrap<ArrowSchema>) -> RbResult<Self> {
        let schema = Arc::new(schema.0);
        let mut columns = Vec::new();
        for field in &schema.fields {
            let array = match field.data_type() {
                ArrowDataType::Boolean => new_array_boolean(ruby, data, field)?,
                ArrowDataType::Int32 => new_array_int32(ruby, data, field)?,
                ArrowDataType::Int64 => new_array_int64(ruby, data, field)?,
                ArrowDataType::Float32 => new_array_float32(ruby, data, field)?,
                ArrowDataType::Float64 => new_array_float64(ruby, data, field)?,
                ArrowDataType::Date32 => new_array_date32(ruby, data, field)?,
                ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
                    new_array_timestamp_us(ruby, data, field)?
                }
                ArrowDataType::Utf8 => new_array_utf8(ruby, data, field)?,
                ArrowDataType::LargeBinary => new_array_large_binary(ruby, data, field)?,
                _ => return Err(todo_error(field.data_type())),
            };
            columns.push(array);
        }
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        Ok(Self { batch })
    }

    pub fn arrow_c_stream(&self) -> RbCapsule {
        let schema = self.batch.schema();
        let reader = RecordBatchIterator::new([Ok(self.batch.clone())].into_iter(), schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        RbCapsule::new(stream, Some("arrow_array_stream".into()))
    }
}

macro_rules! new_array {
    ($name:ident, $builder:ty, $type:ty) => {
        fn $name(ruby: &Ruby, data: RArray, field: &Arc<ArrowField>) -> RbResult<Arc<dyn Array>> {
            let name = ruby.str_new(field.name());
            let mut builder = <$builder>::new();
            for row in data.into_iter() {
                let row = RHash::try_convert(row)?;
                let value: Option<$type> = row.aref(name)?;
                match value {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            let array: Arc<dyn Array> = Arc::new(builder.finish());
            Ok(array)
        }
    };
}

new_array!(new_array_boolean, BooleanBuilder, bool);
new_array!(new_array_int32, Int32Builder, i32);
new_array!(new_array_int64, Int64Builder, i64);
new_array!(new_array_float32, Float32Builder, f32);
new_array!(new_array_float64, Float64Builder, f64);
new_array!(new_array_utf8, StringBuilder, String);

fn new_array_date32(
    ruby: &Ruby,
    data: RArray,
    field: &Arc<ArrowField>,
) -> RbResult<Arc<dyn Array>> {
    let name = ruby.str_new(field.name());
    let mut builder = Date32Builder::new();
    for row in data.into_iter() {
        let row = RHash::try_convert(row)?;
        let value: Option<Value> = row.aref(name)?;
        match value {
            Some(v) => builder.append_value(date_to_i32(v)?),
            None => builder.append_null(),
        }
    }
    let array: Arc<dyn Array> = Arc::new(builder.finish());
    Ok(array)
}

fn new_array_timestamp_us(
    ruby: &Ruby,
    data: RArray,
    field: &Arc<ArrowField>,
) -> RbResult<Arc<dyn Array>> {
    let name = ruby.str_new(field.name());
    let mut builder = TimestampMicrosecondBuilder::new();
    for row in data.into_iter() {
        let row = RHash::try_convert(row)?;
        let value: Option<Value> = row.aref(name)?;
        match value {
            Some(v) => {
                let sec: i64 = v.funcall("to_i", ())?;
                let usec: i64 = v.funcall("usec", ())?;
                builder.append_value(sec * 1_000_000 + usec);
            }
            None => builder.append_null(),
        }
    }
    let array: Arc<dyn Array> = Arc::new(builder.finish());
    Ok(array)
}

fn new_array_large_binary(
    ruby: &Ruby,
    data: RArray,
    field: &Arc<ArrowField>,
) -> RbResult<Arc<dyn Array>> {
    let name = ruby.str_new(field.name());
    let mut builder = LargeBinaryBuilder::new();
    for row in data.into_iter() {
        let row = RHash::try_convert(row)?;
        let value: Option<RString> = row.aref(name)?;
        match value {
            Some(v) => builder.append_value(unsafe { v.as_slice() }),
            None => builder.append_null(),
        }
    }
    let array: Arc<dyn Array> = Arc::new(builder.finish());
    Ok(array)
}

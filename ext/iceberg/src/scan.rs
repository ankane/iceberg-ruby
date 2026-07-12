use std::sync::{Arc, RwLock};

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType as ArrowDataType;
use futures::TryStreamExt;
use iceberg::scan::TableScan;
use magnus::{IntoValue, RArray, Ruby, Value};

use crate::RbResult;
use crate::error::{to_rb_err, todo_error};
use crate::ruby::GvlExt;
use crate::runtime::runtime;
use crate::utils::rb_snapshot;

#[magnus::wrap(class = "Iceberg::RbTableScan")]
pub struct RbTableScan {
    pub scan: RwLock<TableScan>,
}

impl RbTableScan {
    pub fn plan_files(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let plan_files: Vec<_> = ruby
            .detach(|| {
                let scan = rb_self.scan.read().unwrap();
                let runtime = runtime();
                let plan_files = runtime.block_on(scan.plan_files())?;
                runtime.block_on(plan_files.try_collect())
            })
            .map_err(to_rb_err)?;

        let files = ruby.ary_new();
        for v in plan_files {
            let file = ruby.hash_new();
            file.aset(ruby.to_symbol("start"), v.start)?;
            file.aset(ruby.to_symbol("length"), v.length)?;
            file.aset(ruby.to_symbol("record_count"), v.record_count)?;
            file.aset(ruby.to_symbol("data_file_path"), v.data_file_path)?;
            file.aset(ruby.to_symbol("project_field_ids"), v.project_field_ids)?;

            let deletes = ruby.ary_new();
            for d in v.deletes {
                let delete = ruby.hash_new();
                delete.aset(ruby.to_symbol("file_path"), d.file_path)?;
                delete.aset(ruby.to_symbol("partition_spec_id"), d.partition_spec_id)?;
                delete.aset(ruby.to_symbol("equality_ids"), d.equality_ids)?;
                deletes.push(delete)?;
            }
            file.aset(ruby.to_symbol("deletes"), deletes)?;

            files.push(file)?;
        }
        Ok(files)
    }

    pub fn snapshot(ruby: &Ruby, rb_self: &Self) -> RbResult<Option<Value>> {
        match rb_self.scan.read().unwrap().snapshot() {
            Some(s) => Ok(Some(rb_snapshot(ruby, s)?)),
            None => Ok(None),
        }
    }

    pub fn collect(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let runtime = runtime();
        let scan = rb_self.scan.read().unwrap();
        let stream = runtime.block_on(scan.to_arrow()).map_err(to_rb_err)?;
        // TODO improve performance
        let batches: Vec<_> = runtime.block_on(stream.try_collect()).map_err(to_rb_err)?;

        let columns = ruby.ary_new();
        let rows = ruby.ary_new();

        for batch in batches {
            if columns.is_empty() {
                for field in &batch.schema().fields {
                    columns.push(ruby.str_new(field.name()))?;
                }
            }

            for _ in 0..batch.num_rows() {
                let row = ruby.ary_new();
                rows.push(row)?;
            }

            for column in batch.columns() {
                match column.data_type() {
                    ArrowDataType::Boolean => collect_column_boolean(ruby, column, rows)?,
                    ArrowDataType::Int32 => collect_column_int32(ruby, column, rows)?,
                    ArrowDataType::Int64 => collect_column_int64(ruby, column, rows)?,
                    ArrowDataType::Float32 => collect_column_float32(ruby, column, rows)?,
                    ArrowDataType::Float64 => collect_column_float64(ruby, column, rows)?,
                    ArrowDataType::Utf8 => collect_column_utf8(ruby, column, rows)?,
                    _ => return Err(todo_error()),
                }
            }
        }

        let result = ruby.ary_new();
        result.push(columns)?;
        result.push(rows)?;
        Ok(result)
    }
}

macro_rules! collect_column {
    ($name:ident, $type:ty) => {
        pub fn $name(ruby: &Ruby, column: &Arc<dyn Array>, rows: RArray) -> RbResult<()> {
            let array = column.as_any().downcast_ref::<$type>().unwrap();
            for i in 0..array.len() {
                let v = if array.is_valid(i) {
                    Some(array.value(i).into_value_with(ruby))
                } else {
                    None
                };
                rows.entry::<RArray>(i.try_into().unwrap())?.push(v)?;
            }
            Ok(())
        }
    };
}

collect_column!(collect_column_boolean, BooleanArray);
collect_column!(collect_column_int32, Int32Array);
collect_column!(collect_column_int64, Int64Array);
collect_column!(collect_column_float32, Float32Array);
collect_column!(collect_column_float64, Float64Array);
collect_column!(collect_column_utf8, StringArray);

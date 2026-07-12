use std::sync::Arc;

use arrow::array::{Array, ArrowPrimitiveType, PrimitiveBuilder};
use arrow::datatypes::{DataType as ArrowDataType, Float32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{Field, Schema};
use magnus::{RArray, RHash, Ruby, TryConvert};

use crate::RbResult;
use crate::arrow::RbArrowType;
use crate::error::todo_error;

#[magnus::wrap(class = "Iceberg::ArrowArrayStream")]
pub struct RbArrowArrayStream {
    pub(crate) stream: FFI_ArrowArrayStream,
}

impl RbArrowArrayStream {
    pub fn to_i(&self) -> usize {
        (&self.stream as *const _) as usize
    }
}

#[magnus::wrap(class = "Iceberg::ArrowRecordBatch")]
pub struct RbArrowRecordBatch {
    pub(crate) batch: RecordBatch,
}

fn new_array<T: ArrowPrimitiveType>(
    ruby: &Ruby,
    data: RArray,
    field: &Arc<Field>,
) -> RbResult<Arc<dyn Array>>
where
    <T as ArrowPrimitiveType>::Native: TryConvert,
{
    let name = ruby.str_new(field.name());
    let mut builder = PrimitiveBuilder::<T>::new();
    for row in data.into_iter() {
        let row = RHash::try_convert(row)?;
        let value: Option<T::Native> = row.aref(name)?;
        match value {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    let array: Arc<dyn Array> = Arc::new(builder.finish());
    Ok(array)
}

impl RbArrowRecordBatch {
    pub fn new(ruby: &Ruby, data: RArray, schema: RbArrowType<Schema>) -> RbResult<Self> {
        let schema = Arc::new(schema.0);
        let mut columns = Vec::new();
        for field in &schema.fields {
            let array = match field.data_type() {
                ArrowDataType::Int32 => new_array::<Int32Type>(ruby, data, field)?,
                ArrowDataType::Int64 => new_array::<Int64Type>(ruby, data, field)?,
                ArrowDataType::Float32 => new_array::<Float32Type>(ruby, data, field)?,
                ArrowDataType::Float64 => new_array::<Float64Type>(ruby, data, field)?,
                _ => return Err(todo_error()),
            };
            columns.push(array);
        }
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        Ok(Self { batch })
    }

    pub fn arrow_c_stream(&self) -> RbArrowArrayStream {
        let schema = self.batch.schema();
        let reader = RecordBatchIterator::new([Ok(self.batch.clone())].into_iter(), schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        RbArrowArrayStream { stream }
    }
}

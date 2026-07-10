use arrow_schema::ffi::FFI_ArrowSchema;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::Schema;

use crate::RbResult;
use crate::error::to_rb_err;

#[magnus::wrap(class = "Iceberg::RbSchema")]
pub struct RbSchema {
    pub(crate) schema: Schema,
}

#[magnus::wrap(class = "Iceberg::ArrowSchema")]
pub struct RbArrowSchema {
    pub(crate) schema: FFI_ArrowSchema,
}

impl RbArrowSchema {
    pub fn to_i(&self) -> usize {
        (&self.schema as *const _) as usize
    }
}

impl RbSchema {
    pub fn arrow_c_schema(&self) -> RbResult<RbArrowSchema> {
        let schema = schema_to_arrow_schema(&self.schema).map_err(to_rb_err)?;
        let schema = FFI_ArrowSchema::try_from(&schema).unwrap();
        Ok(RbArrowSchema { schema })
    }
}

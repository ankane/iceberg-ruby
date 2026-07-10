use arrow_schema::Schema as ArrowSchema;
use arrow_schema::ffi::FFI_ArrowSchema;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::spec::{PrimitiveType, Schema, Type};
use magnus::{RArray, Ruby};

use crate::RbResult;
use crate::arrow::RbArrowType;
use crate::error::to_rb_err;
use crate::utils::rb_literal;

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
    pub fn new(ob: RbArrowType<ArrowSchema>) -> RbResult<Self> {
        let schema = arrow_schema_to_schema_auto_assign_ids(&ob.0).map_err(to_rb_err)?;
        Ok(Self { schema })
    }

    pub fn fields(ruby: &Ruby, self_: &Self) -> RbResult<RArray> {
        let fields = ruby.ary_new();
        for f in self_.schema.as_struct().fields() {
            let field = ruby.hash_new();
            field.aset(ruby.to_symbol("id"), f.id)?;
            field.aset(ruby.to_symbol("name"), ruby.str_new(&f.name))?;

            let field_type = match &*f.field_type {
                Type::Primitive(ty) => match ty {
                    PrimitiveType::Boolean => "boolean",
                    PrimitiveType::Int => "int",
                    PrimitiveType::Long => "long",
                    PrimitiveType::Float => "float",
                    PrimitiveType::Double => "double",
                    PrimitiveType::Decimal {
                        precision: _,
                        scale: _,
                    } => "decimal",
                    PrimitiveType::Date => "date",
                    PrimitiveType::Time => "time",
                    PrimitiveType::Timestamp => "timestamp",
                    PrimitiveType::Timestamptz => "timestamptz",
                    PrimitiveType::TimestampNs => "timestamp_ns",
                    PrimitiveType::TimestamptzNs => "timestamptz_ns",
                    PrimitiveType::String => "string",
                    PrimitiveType::Uuid => "uuid",
                    PrimitiveType::Fixed(_) => "fixed",
                    PrimitiveType::Binary => "binary",
                },
                _ => todo!(),
            };
            field.aset(ruby.to_symbol("type"), field_type)?;

            field.aset(ruby.to_symbol("required"), f.required)?;

            let initial_default = f.initial_default.as_ref().map(|v| rb_literal(ruby, v));
            field.aset(ruby.to_symbol("initial_default"), initial_default)?;

            let write_default = f.write_default.as_ref().map(|v| rb_literal(ruby, v));
            field.aset(ruby.to_symbol("write_default"), write_default)?;

            field.aset(
                ruby.to_symbol("doc"),
                f.doc.as_ref().map(|v| ruby.str_new(v)),
            )?;

            if let Type::Primitive(PrimitiveType::Fixed(limit)) = &*f.field_type {
                field.aset(ruby.to_symbol("limit"), *limit)?;
            }

            if let Type::Primitive(PrimitiveType::Decimal { precision, scale }) = &*f.field_type {
                field.aset(ruby.to_symbol("precision"), *precision)?;
                field.aset(ruby.to_symbol("scale"), *scale)?;
            }

            fields.push(field)?;
        }
        Ok(fields)
    }

    pub fn schema_id(&self) -> i32 {
        self.schema.schema_id()
    }

    pub fn arrow_c_schema(&self) -> RbResult<RbArrowSchema> {
        let schema = schema_to_arrow_schema(&self.schema).map_err(to_rb_err)?;
        let schema = FFI_ArrowSchema::try_from(&schema).unwrap();
        Ok(RbArrowSchema { schema })
    }
}

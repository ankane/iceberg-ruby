use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use arrow_schema::ffi::FFI_ArrowSchema;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use magnus::{RArray, RHash, RString, Ruby, Value};

use crate::RbResult;
use crate::arrow::RbArrowType;
use crate::error::to_rb_err;
use crate::utils::rb_literal;

#[magnus::wrap(class = "Iceberg::RbSchema")]
pub struct RbSchema {
    pub(crate) schema: Schema,
}

#[magnus::wrap(class = "Iceberg::NestedField")]
pub struct RbNestedField {
    pub(crate) field: Arc<NestedField>,
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
        for field in self_.schema.as_struct().fields() {
            fields.push(RbNestedField {
                field: field.clone(),
            })?;
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

impl RbNestedField {
    pub fn id(&self) -> i32 {
        self.field.id
    }

    pub fn name(ruby: &Ruby, self_: &Self) -> RString {
        ruby.str_new(&self_.field.name)
    }

    pub fn required(&self) -> bool {
        self.field.required
    }

    // TODO return objects
    pub fn field_type(ruby: &Ruby, self_: &Self) -> RString {
        let v = match &*self_.field.field_type {
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
        ruby.str_new(v)
    }

    pub fn doc(ruby: &Ruby, self_: &Self) -> Option<RString> {
        self_.field.doc.as_ref().map(|v| ruby.str_new(v))
    }

    pub fn initial_default(ruby: &Ruby, self_: &Self) -> Option<Value> {
        self_
            .field
            .initial_default
            .as_ref()
            .map(|v| rb_literal(ruby, v))
    }

    pub fn write_default(ruby: &Ruby, self_: &Self) -> Option<Value> {
        self_
            .field
            .write_default
            .as_ref()
            .map(|v| rb_literal(ruby, v))
    }

    pub fn to_h(ruby: &Ruby, self_: &Self) -> RbResult<RHash> {
        let field = ruby.hash_new();
        field.aset(ruby.to_symbol("id"), self_.id())?;
        field.aset(ruby.to_symbol("name"), RbNestedField::name(ruby, self_))?;

        field.aset(
            ruby.to_symbol("type"),
            RbNestedField::field_type(ruby, self_),
        )?;

        field.aset(ruby.to_symbol("required"), self_.required())?;

        field.aset(
            ruby.to_symbol("initial_default"),
            RbNestedField::initial_default(ruby, self_),
        )?;

        field.aset(
            ruby.to_symbol("write_default"),
            RbNestedField::write_default(ruby, self_),
        )?;

        field.aset(ruby.to_symbol("doc"), RbNestedField::doc(ruby, self_))?;

        if let Type::Primitive(PrimitiveType::Fixed(limit)) = &*self_.field.field_type {
            field.aset(ruby.to_symbol("limit"), *limit)?;
        }

        if let Type::Primitive(PrimitiveType::Decimal { precision, scale }) =
            &*self_.field.field_type
        {
            field.aset(ruby.to_symbol("precision"), *precision)?;
            field.aset(ruby.to_symbol("scale"), *scale)?;
        }

        Ok(field)
    }
}

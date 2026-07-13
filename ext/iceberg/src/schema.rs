use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use arrow_schema::ffi::FFI_ArrowSchema;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use magnus::{Error as RbErr, RArray, RHash, RString, Ruby, TryConvert, Value, prelude::*};

use crate::RbResult;
use crate::arrow::{RbArrowSchema, RbArrowType};
use crate::error::{to_rb_err, todo_error};
use crate::utils::{default_value, rb_literal};

#[magnus::wrap(class = "Iceberg::RbSchema")]
pub struct RbSchema {
    pub(crate) schema: Schema,
}

#[magnus::wrap(class = "Iceberg::NestedField")]
pub struct RbNestedField {
    pub(crate) field: Arc<NestedField>,
}

impl RbSchema {
    pub fn new(ob: Value) -> RbResult<Self> {
        let schema = if let Ok(arrow_schema) =
            ob.funcall::<_, _, RbArrowType<ArrowSchema>>("arrow_c_schema", ())
        {
            arrow_schema_to_schema_auto_assign_ids(&arrow_schema.0).map_err(to_rb_err)?
        } else {
            let mut fields = Vec::new();
            let rb_fields = RArray::try_convert(ob)?;
            for rb_field in rb_fields {
                let field = <&RbNestedField>::try_convert(rb_field)?.field.clone();
                fields.push(field);
            }
            Schema::builder()
                .with_fields(fields)
                .build()
                .map_err(to_rb_err)?
        };
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
    pub fn new(ruby: &Ruby, rb_field: RHash) -> RbResult<Self> {
        let rb_type: Value = rb_field.aref(ruby.to_symbol("type"))?;
        let field_type = if let Ok(s) = String::try_convert(rb_type) {
            match s.as_str() {
                "boolean" => Type::Primitive(PrimitiveType::Boolean),
                "int" => Type::Primitive(PrimitiveType::Int),
                "long" => Type::Primitive(PrimitiveType::Long),
                "float" => Type::Primitive(PrimitiveType::Float),
                "double" => Type::Primitive(PrimitiveType::Double),
                "decimal" => {
                    let precision: u32 = rb_field.aref(ruby.to_symbol("precision"))?;
                    let scale: u32 = rb_field.aref(ruby.to_symbol("scale"))?;
                    Type::Primitive(PrimitiveType::Decimal { precision, scale })
                }
                "date" => Type::Primitive(PrimitiveType::Date),
                "time" => Type::Primitive(PrimitiveType::Time),
                "timestamp" => Type::Primitive(PrimitiveType::Timestamp),
                "timestamptz" => Type::Primitive(PrimitiveType::Timestamptz),
                "timestamp_ns" => Type::Primitive(PrimitiveType::TimestampNs),
                "timestamptz_ns" => Type::Primitive(PrimitiveType::TimestamptzNs),
                "string" => Type::Primitive(PrimitiveType::String),
                "uuid" => Type::Primitive(PrimitiveType::Uuid),
                "fixed" => {
                    let limit: u64 = rb_field.aref(ruby.to_symbol("limit"))?;
                    Type::Primitive(PrimitiveType::Fixed(limit))
                }
                "binary" => Type::Primitive(PrimitiveType::Binary),
                _ => {
                    return Err(RbErr::new(
                        ruby.exception_arg_error(),
                        format!("Type not supported: {}", s),
                    ));
                }
            }
        } else {
            return Err(RbErr::new(
                ruby.exception_arg_error(),
                format!("Type not supported: {}", rb_type),
            ));
        };

        let initial_default = rb_field.aref(ruby.to_symbol("initial_default"))?;
        let write_default = rb_field.aref(ruby.to_symbol("write_default"))?;

        let initial_default = default_value(initial_default, &field_type)?;
        let write_default = default_value(write_default, &field_type)?;

        let field = NestedField {
            id: rb_field.aref(ruby.to_symbol("id"))?,
            name: rb_field.aref(ruby.to_symbol("name"))?,
            required: rb_field.aref(ruby.to_symbol("required"))?,
            field_type: field_type.into(),
            doc: rb_field.aref(ruby.to_symbol("doc"))?,
            initial_default,
            write_default,
        };

        Ok(Self {
            field: field.into(),
        })
    }

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
    pub fn field_type(ruby: &Ruby, self_: &Self) -> RbResult<RString> {
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
            _ => return Err(todo_error()),
        };
        Ok(ruby.str_new(v))
    }

    pub fn doc(ruby: &Ruby, self_: &Self) -> Option<RString> {
        self_.field.doc.as_ref().map(|v| ruby.str_new(v))
    }

    pub fn initial_default(ruby: &Ruby, self_: &Self) -> RbResult<Option<Value>> {
        self_
            .field
            .initial_default
            .as_ref()
            .map(|v| rb_literal(ruby, v))
            .transpose()
    }

    pub fn write_default(ruby: &Ruby, self_: &Self) -> RbResult<Option<Value>> {
        self_
            .field
            .write_default
            .as_ref()
            .map(|v| rb_literal(ruby, v))
            .transpose()
    }

    pub fn inspect(ruby: &Ruby, self_: &Self) -> String {
        format!(
            "#<Iceberg::NestedField id={}, name={}, required={}>",
            self_.id(),
            RbNestedField::name(ruby, self_).inspect(),
            self_.required(),
        )
    }

    pub fn to_h(ruby: &Ruby, self_: &Self) -> RbResult<RHash> {
        let field = ruby.hash_new();
        field.aset(ruby.to_symbol("id"), self_.id())?;
        field.aset(ruby.to_symbol("name"), RbNestedField::name(ruby, self_))?;

        field.aset(
            ruby.to_symbol("type"),
            RbNestedField::field_type(ruby, self_)?,
        )?;

        field.aset(ruby.to_symbol("required"), self_.required())?;

        field.aset(
            ruby.to_symbol("initial_default"),
            RbNestedField::initial_default(ruby, self_)?,
        )?;

        field.aset(
            ruby.to_symbol("write_default"),
            RbNestedField::write_default(ruby, self_)?,
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

use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use arrow_schema::ffi::FFI_ArrowSchema;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use magnus::{
    Error as RbErr, IntoValue, RArray, RClass, RHash, RModule, RString, Ruby, TryConvert, Value,
    prelude::*,
};

use crate::RbResult;
use crate::arrow::{RbArrowSchema, RbArrowType};
use crate::error::{to_rb_err, todo_error};
use crate::utils::{default_value, rb_literal};

#[magnus::wrap(class = "Iceberg::Schema")]
pub struct RbSchema {
    pub(crate) schema: Schema,
}

#[magnus::wrap(class = "Iceberg::NestedField")]
pub struct RbNestedField {
    pub(crate) field: Arc<NestedField>,
}

impl RbSchema {
    pub fn new(ruby: &Ruby, ob: Value) -> RbResult<Self> {
        let schema = if let Ok(arrow_schema) =
            ob.funcall::<_, _, RbArrowType<ArrowSchema>>("arrow_c_schema", ())
        {
            arrow_schema_to_schema_auto_assign_ids(&arrow_schema.0).map_err(to_rb_err)?
        } else {
            let mut fields = Vec::new();
            let rb_fields = RArray::try_convert(ob)?;
            for rb_field in rb_fields {
                let nested_field = if let Some(v) = RHash::from_value(rb_field) {
                    &RbNestedField::new(ruby, v)?
                } else {
                    <&RbNestedField>::try_convert(rb_field)?
                };
                fields.push(nested_field.field.clone());
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

    // TODO remove in 0.12.0
    pub fn fields_hash(ruby: &Ruby, self_: &Self) -> RbResult<RArray> {
        ruby.ary_try_from_iter(
            Self::fields(ruby, self_)?
                .into_iter()
                .map(|v| v.funcall::<_, _, Value>("to_h", ())),
        )
    }

    pub fn schema_id(&self) -> i32 {
        self.schema.schema_id()
    }

    pub fn arrow_c_schema(&self) -> RbResult<RbArrowSchema> {
        let schema = schema_to_arrow_schema(&self.schema).map_err(to_rb_err)?;
        let schema = FFI_ArrowSchema::try_from(&schema).unwrap();
        Ok(RbArrowSchema { schema })
    }

    pub fn inspect(ruby: &Ruby, self_: &Self) -> RbResult<String> {
        Ok(format!(
            "#<Iceberg::Schema fields={}>",
            Self::fields(ruby, self_)?.inspect()
        ))
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
            match unsafe { rb_type.classname() }.to_string().as_str() {
                "Iceberg::BooleanType" => Type::Primitive(PrimitiveType::Boolean),
                "Iceberg::IntType" => Type::Primitive(PrimitiveType::Int),
                "Iceberg::LongType" => Type::Primitive(PrimitiveType::Long),
                "Iceberg::FloatType" => Type::Primitive(PrimitiveType::Float),
                "Iceberg::DoubleType" => Type::Primitive(PrimitiveType::Double),
                "Iceberg::DecimalType" => {
                    let precision: u32 = rb_type.funcall("precision", ())?;
                    let scale: u32 = rb_type.funcall("scale", ())?;
                    Type::Primitive(PrimitiveType::Decimal { precision, scale })
                }
                "Iceberg::DateType" => Type::Primitive(PrimitiveType::Date),
                "Iceberg::TimeType" => Type::Primitive(PrimitiveType::Time),
                "Iceberg::TimestampType" => Type::Primitive(PrimitiveType::Timestamp),
                "Iceberg::TimestamptzType" => Type::Primitive(PrimitiveType::Timestamptz),
                "Iceberg::TimestampNanoType" => Type::Primitive(PrimitiveType::TimestampNs),
                "Iceberg::TimestamptzNanoType" => Type::Primitive(PrimitiveType::TimestamptzNs),
                "Iceberg::StringType" => Type::Primitive(PrimitiveType::String),
                "Iceberg::UUIDType" => Type::Primitive(PrimitiveType::Uuid),
                "Iceberg::FixedType" => {
                    let length: u64 = rb_type.funcall("length", ())?;
                    Type::Primitive(PrimitiveType::Fixed(length))
                }
                "Iceberg::BinaryType" => Type::Primitive(PrimitiveType::Binary),
                _ => {
                    return Err(RbErr::new(
                        ruby.exception_arg_error(),
                        format!("Type not supported: {}", rb_type),
                    ));
                }
            }
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

    pub fn field_type(ruby: &Ruby, self_: &Self) -> RbResult<Value> {
        let iceberg = ruby.class_object().const_get::<_, RModule>("Iceberg")?;
        let field_type = &*self_.field.field_type;
        let v = match field_type {
            Type::Primitive(ty) => match ty {
                PrimitiveType::Boolean => iceberg
                    .const_get::<_, RClass>("BooleanType")?
                    .new_instance(())?,
                PrimitiveType::Int => iceberg
                    .const_get::<_, RClass>("IntType")?
                    .new_instance(())?,
                PrimitiveType::Long => iceberg
                    .const_get::<_, RClass>("LongType")?
                    .new_instance(())?,
                PrimitiveType::Float => iceberg
                    .const_get::<_, RClass>("FloatType")?
                    .new_instance(())?,
                PrimitiveType::Double => iceberg
                    .const_get::<_, RClass>("DoubleType")?
                    .new_instance(())?,
                PrimitiveType::Decimal { precision, scale } => iceberg
                    .const_get::<_, RClass>("DecimalType")?
                    .new_instance((*precision, *scale))?,
                PrimitiveType::Date => iceberg
                    .const_get::<_, RClass>("DateType")?
                    .new_instance(())?,
                PrimitiveType::Time => iceberg
                    .const_get::<_, RClass>("TimeType")?
                    .new_instance(())?,
                PrimitiveType::Timestamp => iceberg
                    .const_get::<_, RClass>("TimestampType")?
                    .new_instance(())?,
                PrimitiveType::Timestamptz => iceberg
                    .const_get::<_, RClass>("TimestamptzType")?
                    .new_instance(())?,
                PrimitiveType::TimestampNs => iceberg
                    .const_get::<_, RClass>("TimestampNanoType")?
                    .new_instance(())?,
                PrimitiveType::TimestamptzNs => iceberg
                    .const_get::<_, RClass>("TimestamptzNanoType")?
                    .new_instance(())?,
                PrimitiveType::String => iceberg
                    .const_get::<_, RClass>("StringType")?
                    .new_instance(())?,
                PrimitiveType::Uuid => iceberg
                    .const_get::<_, RClass>("UUIDType")?
                    .new_instance(())?,
                PrimitiveType::Fixed(length) => iceberg
                    .const_get::<_, RClass>("FixedType")?
                    .new_instance((*length,))?,
                PrimitiveType::Binary => iceberg
                    .const_get::<_, RClass>("BinaryType")?
                    .new_instance(())?,
            },
            Type::Struct(_) => iceberg
                .const_get::<_, RClass>("StructType")?
                .new_instance(())?,
            Type::List(_) => iceberg
                .const_get::<_, RClass>("ListType")?
                .new_instance(())?,
            Type::Map(_) => iceberg
                .const_get::<_, RClass>("MapType")?
                .new_instance(())?,
        };
        Ok(v)
    }

    // TODO remove in 0.12.0
    pub fn field_type_str(ruby: &Ruby, self_: &Self) -> RbResult<RString> {
        let field_type = &*self_.field.field_type;
        let v = match field_type {
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
            _ => return Err(todo_error(field_type)),
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

    pub fn inspect(ruby: &Ruby, self_: &Self) -> RbResult<String> {
        Ok(format!(
            "#<Iceberg::NestedField id={}, name={}, field_type={}, required={}, initial_default={}, write_default={}, doc={}>",
            self_.id().into_value_with(ruby).inspect(),
            Self::name(ruby, self_).inspect(),
            Self::field_type(ruby, self_)?.inspect(),
            self_.required().into_value_with(ruby).inspect(),
            Self::initial_default(ruby, self_)?
                .unwrap_or(ruby.qnil().as_value())
                .inspect(),
            Self::write_default(ruby, self_)?
                .unwrap_or(ruby.qnil().as_value())
                .inspect(),
            Self::doc(ruby, self_)
                .map(|v| v.as_value())
                .unwrap_or(ruby.qnil().as_value())
                .inspect(),
        ))
    }

    pub fn to_h(ruby: &Ruby, self_: &Self) -> RbResult<RHash> {
        let field = ruby.hash_new();
        field.aset(ruby.to_symbol("id"), self_.id())?;
        field.aset(ruby.to_symbol("name"), RbNestedField::name(ruby, self_))?;

        field.aset(
            ruby.to_symbol("type"),
            RbNestedField::field_type_str(ruby, self_)?,
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

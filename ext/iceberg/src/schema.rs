use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use arrow_schema::ffi::FFI_ArrowSchema;
use iceberg::arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use magnus::{
    Error as RbErr, IntoValue, RArray, RClass, RHash, RModule, Ruby, TryConvert, Value, prelude::*,
};

use crate::RbResult;
use crate::arrow::{RbArrowSchema, RbArrowType};
use crate::error::to_rb_err;
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
    pub fn new(ob: Value) -> RbResult<Self> {
        let schema = if let Ok(arrow_schema) =
            ob.funcall::<_, _, RbArrowType<ArrowSchema>>("arrow_c_schema", ())
        {
            arrow_schema_to_schema_auto_assign_ids(&arrow_schema.0).map_err(to_rb_err)?
        } else {
            let mut fields = Vec::new();
            let rb_fields = RArray::try_convert(ob)?;
            for rb_field in rb_fields {
                fields.push(<&RbNestedField>::try_convert(rb_field)?.field.clone());
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

    pub fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
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
        let rb_type: Value = rb_field.aref(ruby.to_symbol("field_type"))?;
        let field_type = match &*unsafe { rb_type.classname() } {
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
        };

        let initial_default = rb_field.aref(ruby.to_symbol("initial_default"))?;
        let write_default = rb_field.aref(ruby.to_symbol("write_default"))?;

        let initial_default = default_value(initial_default, &field_type)?;
        let write_default = default_value(write_default, &field_type)?;

        let field = NestedField {
            id: rb_field.aref(ruby.to_symbol("field_id"))?,
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

    pub fn field_id(&self) -> i32 {
        self.field.id
    }

    pub fn name(&self) -> &str {
        &self.field.name
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

    pub fn required(&self) -> bool {
        self.field.required
    }

    pub fn doc(&self) -> Option<&str> {
        self.field.doc.as_deref()
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

    pub fn eq(&self, other: &Self) -> bool {
        self.field == other.field
    }

    pub fn inspect(ruby: &Ruby, self_: &Self) -> RbResult<String> {
        Ok(format!(
            "#<Iceberg::NestedField field_id={}, name={}, field_type={}, required={}, doc={}, initial_default={}, write_default={}>",
            self_.field_id().into_value_with(ruby).inspect(),
            self_.name().into_value_with(ruby).inspect(),
            Self::field_type(ruby, self_)?.inspect(),
            self_.required().into_value_with(ruby).inspect(),
            self_.doc().into_value_with(ruby).inspect(),
            Self::initial_default(ruby, self_)?
                .into_value_with(ruby)
                .inspect(),
            Self::write_default(ruby, self_)?
                .into_value_with(ruby)
                .inspect(),
        ))
    }
}

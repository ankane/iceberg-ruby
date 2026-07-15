use iceberg::spec::{Transform, UnboundPartitionField, UnboundPartitionSpec};
use magnus::{IntoValue, RArray, RHash, Ruby, TryConvert, value::ReprValue};

use crate::RbResult;
use crate::error::{to_rb_err, todo_error};
use crate::utils::Wrap;

#[magnus::wrap(class = "Iceberg::PartitionSpec")]
pub struct RbPartitionSpec {
    pub(crate) spec: UnboundPartitionSpec,
}

#[magnus::wrap(class = "Iceberg::PartitionField")]
pub struct RbPartitionField {
    pub(crate) field: UnboundPartitionField,
}

impl RbPartitionSpec {
    pub fn new(ob: RArray) -> RbResult<Self> {
        let mut fields = Vec::new();
        for v in ob.into_iter() {
            fields.push(<&RbPartitionField>::try_convert(v)?.field.clone());
        }
        let spec = UnboundPartitionSpec::builder()
            .add_partition_fields(fields)
            .map_err(to_rb_err)?
            .build();
        Ok(Self { spec })
    }

    pub fn spec_id(&self) -> Option<i32> {
        self.spec.spec_id()
    }

    pub fn fields(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(
            rb_self
                .spec
                .fields()
                .iter()
                .map(|v| RbPartitionField { field: v.clone() }),
        )
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::PartitionSpec spec_id={}, fields={}>",
            rb_self.spec_id().into_value_with(ruby).inspect(),
            Self::fields(ruby, rb_self).inspect(),
        )
    }
}

impl RbPartitionField {
    pub fn new(ruby: &Ruby, ob: RHash) -> RbResult<Self> {
        let field = UnboundPartitionField::builder()
            .source_id(ob.aref(ruby.to_symbol("source_id"))?)
            .field_id(ob.aref(ruby.to_symbol("field_id"))?)
            .name(ob.aref(ruby.to_symbol("name"))?)
            .transform(
                ob.aref::<_, Wrap<Transform>>(ruby.to_symbol("transform"))?
                    .0,
            )
            .build();
        Ok(Self { field })
    }

    pub fn source_id(&self) -> i32 {
        self.field.source_id
    }

    pub fn field_id(&self) -> Option<i32> {
        self.field.field_id
    }

    pub fn name(&self) -> &str {
        &self.field.name
    }

    pub fn transform(&self) -> RbResult<&str> {
        let transform = &self.field.transform;
        // TODO move / DRY
        let v = match transform {
            Transform::Identity => "identity",
            Transform::Year => "year",
            Transform::Month => "month",
            Transform::Day => "day",
            Transform::Hour => "hour",
            _ => return Err(todo_error(transform)),
        };
        Ok(v)
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> RbResult<String> {
        Ok(format!(
            "#<Iceberg::PartitionField source_id={}, field_id={}, name={}, transform={}>",
            rb_self.source_id().into_value_with(ruby).inspect(),
            rb_self.field_id().into_value_with(ruby).inspect(),
            rb_self.name().into_value_with(ruby).inspect(),
            rb_self.transform()?.into_value_with(ruby).inspect(),
        ))
    }
}

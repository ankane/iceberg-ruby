use iceberg::spec::{Transform, UnboundPartitionField, UnboundPartitionSpec};
use magnus::{IntoValue, RArray, RHash, RString, Ruby, TryConvert, value::ReprValue};

use crate::RbResult;
use crate::error::{to_rb_err, todo_error};

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

    pub fn inspect(ruby: &Ruby, self_: &Self) -> String {
        format!(
            "#<Iceberg::PartitionSpec spec_id={}>",
            self_.spec_id().into_value_with(ruby).inspect(),
        )
    }
}

impl RbPartitionField {
    pub fn new(ruby: &Ruby, ob: RHash) -> RbResult<Self> {
        let transform = ob.aref::<_, RString>(ruby.to_symbol("transform"))?;
        let transform = match unsafe { transform.as_str()? } {
            "day" => Transform::Day,
            _ => return Err(todo_error(transform)),
        };
        let field = UnboundPartitionField::builder()
            .source_id(ob.aref(ruby.to_symbol("source_id"))?)
            .field_id(ob.aref(ruby.to_symbol("field_id"))?)
            .name(ob.aref(ruby.to_symbol("name"))?)
            .transform(transform)
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

    pub fn inspect(ruby: &Ruby, self_: &Self) -> String {
        format!(
            "#<Iceberg::PartitionField source_id={}, field_id={}, name={}>",
            self_.source_id().into_value_with(ruby).inspect(),
            self_.field_id().into_value_with(ruby).inspect(),
            self_.name().into_value_with(ruby).inspect(),
        )
    }
}

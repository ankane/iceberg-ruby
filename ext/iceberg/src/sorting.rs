use iceberg::spec::{NullOrder, SortDirection, SortField, SortOrder, Transform};
use magnus::{IntoValue, RArray, RHash, RString, Ruby, TryConvert, value::ReprValue};

use crate::RbResult;
use crate::error::{to_rb_err, todo_error};

#[magnus::wrap(class = "Iceberg::SortOrder")]
pub struct RbSortOrder {
    pub(crate) order: SortOrder,
}

#[magnus::wrap(class = "Iceberg::SortField")]
pub struct RbSortField {
    pub(crate) field: SortField,
}

impl RbSortOrder {
    pub fn new(ob: RArray) -> RbResult<Self> {
        let mut fields = Vec::new();
        for v in ob.into_iter() {
            fields.push(<&RbSortField>::try_convert(v)?.field.clone());
        }
        let order = SortOrder::builder()
            .with_fields(fields)
            .build_unbound()
            .map_err(to_rb_err)?;
        Ok(Self { order })
    }

    pub fn order_id(&self) -> i64 {
        self.order.order_id
    }

    pub fn fields(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(
            rb_self
                .order
                .fields
                .iter()
                .map(|v| RbSortField { field: v.clone() }),
        )
    }

    pub fn inspect(ruby: &Ruby, self_: &Self) -> String {
        format!(
            "#<Iceberg::SortOrder order_id={}>",
            self_.order_id().into_value_with(ruby).inspect(),
        )
    }
}

impl RbSortField {
    pub fn new(ruby: &Ruby, ob: RHash) -> RbResult<Self> {
        let transform = ob.aref::<_, RString>(ruby.to_symbol("transform"))?;
        let transform = match unsafe { transform.as_str()? } {
            "identity" => Transform::Identity,
            _ => return Err(todo_error(transform)),
        };
        let field = SortField::builder()
            .source_id(ob.aref(ruby.to_symbol("source_id"))?)
            .transform(transform)
            // TODO improve
            .direction(SortDirection::Ascending)
            // TODO improve
            .null_order(NullOrder::First)
            .build();
        Ok(Self { field })
    }

    pub fn source_id(&self) -> i32 {
        self.field.source_id
    }

    pub fn inspect(ruby: &Ruby, self_: &Self) -> String {
        format!(
            "#<Iceberg::SortField source_id={}>",
            self_.source_id().into_value_with(ruby).inspect(),
        )
    }
}

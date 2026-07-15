use iceberg::spec::{NullOrder, SortDirection, SortField, SortOrder, Transform};
use magnus::{IntoValue, RArray, RHash, Ruby, TryConvert, value::ReprValue};

use crate::RbResult;
use crate::error::to_rb_err;
use crate::utils::Wrap;

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

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::SortOrder order_id={}>",
            rb_self.order_id().into_value_with(ruby).inspect(),
        )
    }
}

impl RbSortField {
    pub fn new(ruby: &Ruby, ob: RHash) -> RbResult<Self> {
        let transform = ob
            .aref::<_, Wrap<Transform>>(ruby.to_symbol("transform"))?
            .0;

        let direction = ob
            .aref::<_, Wrap<SortDirection>>(ruby.to_symbol("direction"))?
            .0;

        let null_order = ob
            .aref::<_, Option<Wrap<NullOrder>>>(ruby.to_symbol("null_order"))?
            .map(|v| v.0)
            .unwrap_or(if direction == SortDirection::Ascending {
                NullOrder::First
            } else {
                NullOrder::Last
            });

        let field = SortField::builder()
            .source_id(ob.aref(ruby.to_symbol("source_id"))?)
            .transform(transform)
            .direction(direction)
            .null_order(null_order)
            .build();
        Ok(Self { field })
    }

    pub fn source_id(&self) -> i32 {
        self.field.source_id
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::SortField source_id={}>",
            rb_self.source_id().into_value_with(ruby).inspect(),
        )
    }
}

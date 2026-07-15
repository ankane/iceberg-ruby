use iceberg::spec::{NullOrder, SortDirection, SortField, SortOrder, Transform};
use magnus::{IntoValue, RArray, RHash, Ruby, TryConvert, Value, value::ReprValue};

use crate::RbResult;
use crate::error::to_rb_err;
use crate::utils::{Wrap, rb_transform};

#[magnus::wrap(class = "Iceberg::SortOrder")]
pub struct RbSortOrder {
    pub(crate) order: SortOrder,
}

#[magnus::wrap(class = "Iceberg::SortField")]
pub struct RbSortField {
    pub(crate) field: SortField,
}

impl RbSortOrder {
    pub fn new(args: &[Value]) -> RbResult<Self> {
        let mut fields = Vec::new();
        for v in args {
            fields.push(<&RbSortField>::try_convert(*v)?.field.clone());
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

    pub fn eq(&self, other: &Self) -> bool {
        self.order == other.order
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::SortOrder order_id={}, fields={}>",
            rb_self.order_id().into_value_with(ruby).inspect(),
            Self::fields(ruby, rb_self).inspect(),
        )
    }
}

impl RbSortField {
    pub fn new(ruby: &Ruby, ob: RHash) -> RbResult<Self> {
        let transform = ob
            .aref::<_, Wrap<Transform>>(ruby.to_symbol("transform"))?
            .0;

        let direction = ob
            .aref::<_, Option<Wrap<SortDirection>>>(ruby.to_symbol("direction"))?
            .map(|v| v.0)
            .unwrap_or(SortDirection::Ascending);

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

    pub fn transform(&self) -> RbResult<Value> {
        rb_transform(&self.field.transform)
    }

    pub fn direction(&self) -> &str {
        match self.field.direction {
            SortDirection::Ascending => "asc",
            SortDirection::Descending => "desc",
        }
    }

    pub fn null_order(&self) -> &str {
        match self.field.null_order {
            NullOrder::First => "first",
            NullOrder::Last => "last",
        }
    }

    pub fn eq(&self, other: &Self) -> bool {
        self.field == other.field
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> RbResult<String> {
        Ok(format!(
            "#<Iceberg::SortField source_id={}, transform={}, direction={}, null_order={}>",
            rb_self.source_id().into_value_with(ruby).inspect(),
            rb_self.transform()?.into_value_with(ruby).inspect(),
            rb_self.direction().into_value_with(ruby).inspect(),
            rb_self.null_order().into_value_with(ruby).inspect(),
        ))
    }
}

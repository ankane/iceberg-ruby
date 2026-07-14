use iceberg::spec::{NullOrder, SortDirection, SortField, SortOrder, Transform};
use magnus::{RArray, RHash, RString, Ruby, TryConvert};

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
}

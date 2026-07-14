use iceberg::spec::{
    EncryptedKey, Literal, PartitionSpec, PartitionStatisticsFile, PrimitiveLiteral, PrimitiveType,
    Schema, Snapshot, SortOrder, StatisticsFile, Transform, Type,
};
use iceberg::{NamespaceIdent, TableIdent};
use magnus::{
    Error as RbErr, IntoValue, RClass, RString, Ruby, TryConvert, Value, prelude::*, value::Lazy,
};
use std::sync::Arc;

use crate::RbResult;
use crate::encryption::RbEncryptedKey;
use crate::error::{to_rb_err, todo_error};
use crate::partitioning::RbPartitionSpec;
use crate::schema::RbSchema;
use crate::snapshot::RbSnapshot;
use crate::sorting::RbSortOrder;
use crate::statistics::{RbPartitionStatisticsFile, RbStatisticsFile};

pub struct Wrap<T>(pub T);

impl TryConvert for Wrap<NamespaceIdent> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let ident = if let Ok(vec) = Vec::<String>::try_convert(ob) {
            // prevent dots due to buggy behavior with iceberg-catalog-{rest,sql} crates
            if vec.iter().any(|v| v.contains(".")) {
                return Err(RbErr::new(
                    Ruby::get_with(ob).exception_arg_error(),
                    "Unsupported namespace",
                ));
            }
            NamespaceIdent::from_vec(vec).map_err(to_rb_err)?
        } else {
            NamespaceIdent::from_strs(String::try_convert(ob)?.split(".")).map_err(to_rb_err)?
        };
        Ok(Wrap(ident))
    }
}

impl TryConvert for Wrap<TableIdent> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let ident = if let Ok(vec) = Vec::<String>::try_convert(ob) {
            TableIdent::from_strs(vec.iter()).map_err(to_rb_err)?
        } else {
            TableIdent::from_strs(String::try_convert(ob)?.split(".")).map_err(to_rb_err)?
        };
        Ok(Wrap(ident))
    }
}

impl TryConvert for Wrap<Transform> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let s = RString::try_convert(ob)?;
        let v = match unsafe { s.as_str() }? {
            "identity" => Transform::Identity,
            "year" => Transform::Year,
            "month" => Transform::Month,
            "day" => Transform::Day,
            "hour" => Transform::Hour,
            _ => {
                return Err(RbErr::new(
                    Ruby::get_with(ob).exception_arg_error(),
                    "Unsupported transform",
                ));
            }
        };
        Ok(Wrap(v))
    }
}

pub fn default_value(ob: Value, field_type: &Type) -> RbResult<Option<Literal>> {
    if ob.is_nil() {
        return Ok(None);
    }

    let lit = match field_type {
        Type::Primitive(ty) => {
            let pl = match ty {
                PrimitiveType::Boolean => PrimitiveLiteral::Boolean(bool::try_convert(ob)?),
                PrimitiveType::Int => PrimitiveLiteral::Int(i32::try_convert(ob)?),
                PrimitiveType::Long => PrimitiveLiteral::Long(i64::try_convert(ob)?),
                PrimitiveType::Float => PrimitiveLiteral::Float(f32::try_convert(ob)?.into()),
                PrimitiveType::Double => PrimitiveLiteral::Double(f64::try_convert(ob)?.into()),
                PrimitiveType::Decimal {
                    precision: _,
                    scale: _,
                } => return Err(todo_error(field_type)),
                PrimitiveType::Date => return Err(todo_error(field_type)),
                PrimitiveType::Time => return Err(todo_error(field_type)),
                PrimitiveType::Timestamp => return Err(todo_error(field_type)),
                PrimitiveType::Timestamptz => return Err(todo_error(field_type)),
                PrimitiveType::TimestampNs => return Err(todo_error(field_type)),
                PrimitiveType::TimestamptzNs => return Err(todo_error(field_type)),
                PrimitiveType::String => PrimitiveLiteral::String(String::try_convert(ob)?),
                PrimitiveType::Uuid => return Err(todo_error(field_type)),
                PrimitiveType::Fixed(_) => return Err(todo_error(field_type)),
                PrimitiveType::Binary => {
                    let s = RString::try_convert(ob)?;
                    PrimitiveLiteral::Binary(unsafe { s.as_slice() }.to_vec())
                }
            };
            Literal::Primitive(pl)
        }
        Type::Struct(_) => return Err(todo_error(field_type)),
        Type::List(_) => return Err(todo_error(field_type)),
        Type::Map(_) => return Err(todo_error(field_type)),
    };
    Ok(Some(lit))
}

impl From<&Arc<Schema>> for RbSchema {
    fn from(schema: &Arc<Schema>) -> Self {
        Self {
            schema: (**schema).clone(),
        }
    }
}

impl From<&Arc<Snapshot>> for RbSnapshot {
    fn from(snapshot: &Arc<Snapshot>) -> Self {
        Self {
            snapshot: (**snapshot).clone(),
        }
    }
}

impl From<&Arc<PartitionSpec>> for RbPartitionSpec {
    fn from(partition_spec: &Arc<PartitionSpec>) -> Self {
        Self {
            spec: (**partition_spec).clone().into(),
        }
    }
}

impl From<&Arc<SortOrder>> for RbSortOrder {
    fn from(sort_order: &Arc<SortOrder>) -> Self {
        Self {
            order: (**sort_order).clone(),
        }
    }
}

impl From<&StatisticsFile> for RbStatisticsFile {
    fn from(statistics_file: &StatisticsFile) -> Self {
        Self {
            file: statistics_file.clone(),
        }
    }
}

impl From<&PartitionStatisticsFile> for RbPartitionStatisticsFile {
    fn from(partition_statistics_file: &PartitionStatisticsFile) -> Self {
        Self {
            file: partition_statistics_file.clone(),
        }
    }
}

impl From<&EncryptedKey> for RbEncryptedKey {
    fn from(encrypted_key: &EncryptedKey) -> Self {
        Self {
            key: encrypted_key.clone(),
        }
    }
}

pub fn rb_literal(ruby: &Ruby, literal: &Literal) -> RbResult<Value> {
    let v = match literal {
        Literal::Primitive(pl) => match pl {
            PrimitiveLiteral::Boolean(v) => v.into_value_with(ruby),
            PrimitiveLiteral::Int(v) => v.into_value_with(ruby),
            PrimitiveLiteral::Long(v) => v.into_value_with(ruby),
            PrimitiveLiteral::Float(v) => v.into_value_with(ruby),
            PrimitiveLiteral::Double(v) => v.into_value_with(ruby),
            PrimitiveLiteral::String(v) => ruby.str_new(v).as_value(),
            PrimitiveLiteral::Binary(v) => ruby.str_from_slice(v).as_value(),
            PrimitiveLiteral::Int128(v) => v.into_value_with(ruby),
            PrimitiveLiteral::UInt128(v) => v.into_value_with(ruby),
            PrimitiveLiteral::AboveMax => return Err(todo_error(literal)),
            PrimitiveLiteral::BelowMin => return Err(todo_error(literal)),
        },
        Literal::Struct(_) => return Err(todo_error(literal)),
        Literal::List(_) => return Err(todo_error(literal)),
        Literal::Map(_) => return Err(todo_error(literal)),
    };
    Ok(v)
}

pub static EPOCH: Lazy<Value> = Lazy::new(|ruby| {
    ruby.class_object()
        .const_get::<_, RClass>("Date")
        .unwrap()
        .new_instance((1970, 1, 1))
        .unwrap()
});

pub fn date_to_i32(value: Value) -> RbResult<i32> {
    let epoch = Ruby::get_with(value).get_inner(&EPOCH);
    value
        .funcall::<_, _, Value>("-", (epoch,))?
        .funcall("to_i", ())
}

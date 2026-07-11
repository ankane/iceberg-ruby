use iceberg::spec::{
    EncryptedKey, Literal, PartitionSpec, PartitionStatisticsFile, PrimitiveLiteral, PrimitiveType,
    Schema, Snapshot, SortOrder, StatisticsFile, Type,
};
use iceberg::{NamespaceIdent, TableIdent};
use magnus::{
    Error as RbErr, IntoValue, RClass, RModule, RObject, Ruby, TryConvert, Value, prelude::*,
};

use crate::RbResult;
use crate::error::{to_rb_err, todo_error};
use crate::schema::RbSchema;

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

impl TryConvert for Wrap<Schema> {
    fn try_convert(ob: Value) -> RbResult<Self> {
        let rb_schema: &RbSchema = RObject::try_convert(ob)?.ivar_get("@schema")?;
        Ok(Wrap(rb_schema.schema.clone()))
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
                PrimitiveType::String => PrimitiveLiteral::String(String::try_convert(ob)?),
                _ => return Err(todo_error()),
            };
            Literal::Primitive(pl)
        }
        _ => return Err(todo_error()),
    };
    Ok(Some(lit))
}

pub fn rb_schema(ruby: &Ruby, schema: &Schema) -> RbResult<Value> {
    ruby.class_object()
        .const_get::<_, RModule>("Iceberg")?
        .const_get::<_, RClass>("Schema")?
        .funcall(
            "new",
            (RbSchema {
                schema: schema.clone(),
            },),
        )
}

pub fn rb_snapshot(ruby: &Ruby, snapshot: &Snapshot) -> RbResult<Value> {
    let rb_snapshot = ruby.hash_new();
    rb_snapshot.aset(ruby.to_symbol("snapshot_id"), snapshot.snapshot_id())?;
    rb_snapshot.aset(
        ruby.to_symbol("parent_snapshot_id"),
        snapshot.parent_snapshot_id(),
    )?;
    rb_snapshot.aset(
        ruby.to_symbol("sequence_number"),
        snapshot.sequence_number(),
    )?;
    rb_snapshot.aset(ruby.to_symbol("manifest_list"), snapshot.manifest_list())?;
    rb_snapshot.aset(ruby.to_symbol("schema_id"), snapshot.schema_id())?;
    Ok(rb_snapshot.as_value())
}

pub fn rb_partition_spec(ruby: &Ruby, partition_spec: &PartitionSpec) -> RbResult<Value> {
    let rb_partition_spec = ruby.hash_new();
    rb_partition_spec.aset(ruby.to_symbol("spec_id"), partition_spec.spec_id())?;
    Ok(rb_partition_spec.as_value())
}

pub fn rb_sort_order(ruby: &Ruby, sort_order: &SortOrder) -> RbResult<Value> {
    let rb_sort_order = ruby.hash_new();
    rb_sort_order.aset(ruby.to_symbol("order_id"), sort_order.order_id)?;
    let rb_fields = ruby.ary_new();
    for field in &sort_order.fields {
        let rb_field = ruby.hash_new();
        rb_field.aset(ruby.to_symbol("source_id"), field.source_id)?;
        rb_fields.push(rb_field)?;
    }
    rb_sort_order.aset(ruby.to_symbol("fields"), rb_fields)?;
    Ok(rb_sort_order.as_value())
}

pub fn rb_statistics_file(ruby: &Ruby, statistics_file: &StatisticsFile) -> RbResult<Value> {
    let rb_statistics_file = ruby.hash_new();
    rb_statistics_file.aset(ruby.to_symbol("snapshot_id"), statistics_file.snapshot_id)?;
    rb_statistics_file.aset(
        ruby.to_symbol("statistics_path"),
        ruby.str_new(&statistics_file.statistics_path),
    )?;
    rb_statistics_file.aset(
        ruby.to_symbol("file_size_in_bytes"),
        statistics_file.file_size_in_bytes,
    )?;
    rb_statistics_file.aset(
        ruby.to_symbol("file_footer_size_in_bytes"),
        statistics_file.file_footer_size_in_bytes,
    )?;
    rb_statistics_file.aset(
        ruby.to_symbol("key_metadata"),
        statistics_file
            .key_metadata
            .as_ref()
            .map(|v| ruby.str_new(v)),
    )?;
    Ok(rb_statistics_file.as_value())
}

pub fn rb_partition_statistics_file(
    ruby: &Ruby,
    partition_statistics_file: &PartitionStatisticsFile,
) -> RbResult<Value> {
    let rb_partition_statistics_file = ruby.hash_new();
    rb_partition_statistics_file.aset(
        ruby.to_symbol("snapshot_id"),
        partition_statistics_file.snapshot_id,
    )?;
    rb_partition_statistics_file.aset(
        ruby.to_symbol("statistics_path"),
        ruby.str_new(&partition_statistics_file.statistics_path),
    )?;
    rb_partition_statistics_file.aset(
        ruby.to_symbol("file_size_in_bytes"),
        partition_statistics_file.file_size_in_bytes,
    )?;
    Ok(rb_partition_statistics_file.as_value())
}

pub fn rb_encrypted_key(ruby: &Ruby, encrypted_key: &EncryptedKey) -> RbResult<Value> {
    let rb_encrypted_key = ruby.hash_new();
    rb_encrypted_key.aset(ruby.to_symbol("key_id"), encrypted_key.key_id())?;
    Ok(rb_encrypted_key.as_value())
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
            _ => return Err(todo_error()),
        },
        _ => return Err(todo_error()),
    };
    Ok(v)
}

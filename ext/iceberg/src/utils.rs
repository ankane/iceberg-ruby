use iceberg::spec::{
    Literal, NestedField, PartitionSpec, PartitionStatisticsFile, PrimitiveLiteral, PrimitiveType,
    Schema, Snapshot, SortOrder, StatisticsFile, Type,
};
use iceberg::{NamespaceIdent, TableIdent};
use magnus::{
    Error as RbErr, IntoValue, RArray, RClass, RHash, RModule, Ruby, TryConvert, Value, kwargs,
    prelude::*,
};

use crate::RbResult;
use crate::error::to_rb_err;

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
        let ruby = Ruby::get_with(ob);
        let mut fields = Vec::new();
        let rb_fields: RArray = ob.funcall("fields", ())?;
        for rb_field in rb_fields {
            let rb_field = RHash::try_convert(rb_field)?;
            let rb_type: Value = rb_field.aref(ruby.to_symbol("type"))?;
            let field_type = if let Ok(s) = String::try_convert(rb_type) {
                match s.as_str() {
                    "boolean" => Type::Primitive(PrimitiveType::Boolean),
                    "int" => Type::Primitive(PrimitiveType::Int),
                    "long" => Type::Primitive(PrimitiveType::Long),
                    "float" => Type::Primitive(PrimitiveType::Float),
                    "double" => Type::Primitive(PrimitiveType::Double),
                    // TODO PrimitiveType::Decimal
                    "date" => Type::Primitive(PrimitiveType::Date),
                    "time" => Type::Primitive(PrimitiveType::Time),
                    "timestamp" => Type::Primitive(PrimitiveType::Timestamp),
                    "timestamptz" => Type::Primitive(PrimitiveType::Timestamptz),
                    "timestamp_ns" => Type::Primitive(PrimitiveType::TimestampNs),
                    "timestamptz_ns" => Type::Primitive(PrimitiveType::TimestamptzNs),
                    "string" => Type::Primitive(PrimitiveType::String),
                    "uuid" => Type::Primitive(PrimitiveType::Uuid),
                    // TODO PrimitiveType::Fixed
                    "binary" => Type::Primitive(PrimitiveType::Binary),
                    _ => {
                        return Err(RbErr::new(
                            ruby.exception_arg_error(),
                            format!("Type not supported: {}", s),
                        ));
                    }
                }
            } else {
                let class_name = unsafe { rb_type.classname() }.to_string();
                match class_name.as_str() {
                    "Polars::Boolean" => Type::Primitive(PrimitiveType::Boolean),
                    "Polars::Int32" => Type::Primitive(PrimitiveType::Int),
                    "Polars::Int64" => Type::Primitive(PrimitiveType::Long),
                    "Polars::Float32" => Type::Primitive(PrimitiveType::Float),
                    "Polars::Float64" => Type::Primitive(PrimitiveType::Double),
                    "Polars::Date" => Type::Primitive(PrimitiveType::Date),
                    "Polars::Time" => Type::Primitive(PrimitiveType::Time),
                    "Polars::String" => Type::Primitive(PrimitiveType::String),
                    "Polars::Binary" => Type::Primitive(PrimitiveType::Binary),
                    _ => {
                        return Err(RbErr::new(
                            ruby.exception_arg_error(),
                            format!("Type not supported: {}", class_name),
                        ));
                    }
                }
            };

            let initial_default = rb_field.aref(ruby.to_symbol("initial_default"))?;
            let write_default = rb_field.aref(ruby.to_symbol("write_default"))?;

            let initial_default = default_value(initial_default, &field_type)?;
            let write_default = default_value(write_default, &field_type)?;

            fields.push(
                NestedField {
                    id: rb_field.aref(ruby.to_symbol("id"))?,
                    name: rb_field.aref(ruby.to_symbol("name"))?,
                    required: rb_field.aref(ruby.to_symbol("required"))?,
                    field_type: field_type.into(),
                    doc: rb_field.aref(ruby.to_symbol("doc"))?,
                    initial_default,
                    write_default,
                }
                .into(),
            );
        }
        let schema = Schema::builder()
            .with_fields(fields)
            .build()
            .map_err(to_rb_err)?;
        Ok(Wrap(schema))
    }
}

fn default_value(ob: Value, field_type: &Type) -> RbResult<Option<Literal>> {
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
                _ => todo!(),
            };
            Literal::Primitive(pl)
        }
        _ => todo!(),
    };
    Ok(Some(lit))
}

pub fn rb_schema(schema: &Schema) -> RbResult<Value> {
    let ruby = Ruby::get().unwrap();
    let fields = ruby.ary_new();
    for f in schema.as_struct().fields() {
        let field = ruby.hash_new();
        field.aset(ruby.to_symbol("id"), f.id)?;
        field.aset(ruby.to_symbol("name"), ruby.str_new(&f.name))?;

        let field_type = match &*f.field_type {
            Type::Primitive(ty) => match ty {
                PrimitiveType::Boolean => "boolean",
                PrimitiveType::Int => "int",
                PrimitiveType::Long => "long",
                PrimitiveType::Float => "float",
                PrimitiveType::Double => "double",
                PrimitiveType::Decimal {
                    precision: _,
                    scale: _,
                } => todo!(),
                PrimitiveType::Date => "date",
                PrimitiveType::Time => "time",
                PrimitiveType::Timestamp => "timestamp",
                PrimitiveType::Timestamptz => "timestamptz",
                PrimitiveType::TimestampNs => "timestamp_ns",
                PrimitiveType::TimestamptzNs => "timestamptz_ns",
                PrimitiveType::String => "string",
                PrimitiveType::Uuid => "uuid",
                PrimitiveType::Binary => "binary",
                PrimitiveType::Fixed(_) => todo!(),
            },
            _ => todo!(),
        };
        field.aset(ruby.to_symbol("type"), field_type)?;

        field.aset(ruby.to_symbol("required"), f.required)?;

        let initial_default = f.initial_default.as_ref().map(rb_literal);
        field.aset(ruby.to_symbol("initial_default"), initial_default)?;

        let write_default = f.write_default.as_ref().map(rb_literal);
        field.aset(ruby.to_symbol("write_default"), write_default)?;

        field.aset(
            ruby.to_symbol("doc"),
            f.doc.as_ref().map(|v| ruby.str_new(v)),
        )?;

        fields.push(field)?;
    }
    let schema_id = schema.schema_id();

    ruby.class_object()
        .const_get::<_, RModule>("Iceberg")
        .unwrap()
        .const_get::<_, RClass>("Schema")
        .unwrap()
        .funcall("new", (fields, kwargs!("schema_id" => schema_id)))
}

pub fn rb_snapshot(snapshot: &Snapshot) -> RbResult<Value> {
    let ruby = Ruby::get().unwrap();
    let rb_snapshot = ruby.hash_new();
    rb_snapshot.aset(ruby.to_symbol("snapshot_id"), snapshot.snapshot_id())?;
    rb_snapshot.aset(ruby.to_symbol("parent_snapshot_id"), snapshot.parent_snapshot_id())?;
    rb_snapshot.aset(ruby.to_symbol("sequence_number"), snapshot.sequence_number())?;
    rb_snapshot.aset(ruby.to_symbol("manifest_list"), snapshot.manifest_list())?;
    rb_snapshot.aset(ruby.to_symbol("schema_id"), snapshot.schema_id())?;
    Ok(rb_snapshot.as_value())
}

pub fn rb_partition_spec(_partition_spec: &PartitionSpec) -> RbResult<Value> {
    todo!();
}

pub fn rb_sort_order(_sort_order: &SortOrder) -> RbResult<Value> {
    todo!();
}

pub fn rb_statistics_file(_statistics_file: &StatisticsFile) -> RbResult<Value> {
    todo!();
}

pub fn rb_partition_statistics_file(
    _partition_statistics_file: &PartitionStatisticsFile,
) -> RbResult<Value> {
    todo!();
}

pub fn rb_literal(literal: &Literal) -> Value {
    let ruby = Ruby::get().unwrap();
    match literal {
        Literal::Primitive(pl) => match pl {
            PrimitiveLiteral::Boolean(v) => v.into_value_with(&ruby),
            PrimitiveLiteral::Int(v) => v.into_value_with(&ruby),
            PrimitiveLiteral::Long(v) => v.into_value_with(&ruby),
            PrimitiveLiteral::Float(v) => v.into_value_with(&ruby),
            PrimitiveLiteral::Double(v) => v.into_value_with(&ruby),
            PrimitiveLiteral::String(v) => ruby.str_new(v).as_value(),
            PrimitiveLiteral::Binary(v) => ruby.str_from_slice(v).as_value(),
            _ => todo!(),
        },
        _ => todo!(),
    }
}

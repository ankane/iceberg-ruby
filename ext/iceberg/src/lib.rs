mod arrow;
mod catalog;
mod error;
mod runtime;
mod scan;
mod table;
mod utils;

use magnus::{Error as RbErr, Ruby, function, method, prelude::*};

use crate::catalog::RbCatalog;
use crate::scan::RbTableScan;
use crate::table::RbTable;

type RbResult<T> = Result<T, RbErr>;

#[magnus::init(name = "iceberg")]
fn init(ruby: &Ruby) -> RbResult<()> {
    let module = ruby.define_module("Iceberg")?;

    let class = module.define_class("RbCatalog", ruby.class_object())?;
    #[cfg(feature = "glue")]
    class.define_singleton_method("new_glue", function!(RbCatalog::new_glue, 1))?;
    class.define_singleton_method("new_memory", function!(RbCatalog::new_memory, 1))?;
    #[cfg(feature = "rest")]
    class.define_singleton_method("new_rest", function!(RbCatalog::new_rest, 3))?;
    #[cfg(feature = "s3tables")]
    class.define_singleton_method("new_s3tables", function!(RbCatalog::new_s3tables, 1))?;
    #[cfg(feature = "sql")]
    class.define_singleton_method("new_sql", function!(RbCatalog::new_sql, 4))?;
    class.define_method("list_namespaces", method!(RbCatalog::list_namespaces, 1))?;
    class.define_method("create_namespace", method!(RbCatalog::create_namespace, 2))?;
    class.define_method("namespace_exists?", method!(RbCatalog::namespace_exists, 1))?;
    class.define_method(
        "namespace_properties",
        method!(RbCatalog::namespace_properties, 1),
    )?;
    class.define_method("update_namespace", method!(RbCatalog::update_namespace, 2))?;
    class.define_method("drop_namespace", method!(RbCatalog::drop_namespace, 1))?;
    class.define_method("list_tables", method!(RbCatalog::list_tables, 1))?;
    class.define_method("create_table", method!(RbCatalog::create_table, 3))?;
    class.define_method("load_table", method!(RbCatalog::load_table, 1))?;
    class.define_method("drop_table", method!(RbCatalog::drop_table, 1))?;
    class.define_method("table_exists?", method!(RbCatalog::table_exists, 1))?;
    class.define_method("rename_table", method!(RbCatalog::rename_table, 2))?;
    class.define_method("register_table", method!(RbCatalog::register_table, 2))?;
    #[cfg(feature = "datafusion")]
    class.define_method("sql", method!(RbCatalog::sql, 1))?;

    let class = module.define_class("RbTable", ruby.class_object())?;
    class.define_method("scan", method!(RbTable::scan, 1))?;
    class.define_method("append", method!(RbTable::append, 2))?;
    class.define_method("format_version", method!(RbTable::format_version, 0))?;
    class.define_method("uuid", method!(RbTable::uuid, 0))?;
    class.define_method("location", method!(RbTable::location, 0))?;
    class.define_method(
        "last_sequence_number",
        method!(RbTable::last_sequence_number, 0),
    )?;
    class.define_method(
        "next_sequence_number",
        method!(RbTable::next_sequence_number, 0),
    )?;
    class.define_method("last_column_id", method!(RbTable::last_column_id, 0))?;
    class.define_method("last_partition_id", method!(RbTable::last_partition_id, 0))?;
    class.define_method("last_updated_ms", method!(RbTable::last_updated_ms, 0))?;
    class.define_method("schemas", method!(RbTable::schemas, 0))?;
    class.define_method("schema_by_id", method!(RbTable::schema_by_id, 1))?;
    class.define_method("current_schema", method!(RbTable::current_schema, 0))?;
    class.define_method("current_schema_id", method!(RbTable::current_schema_id, 0))?;
    class.define_method("partition_specs", method!(RbTable::partition_specs, 0))?;
    class.define_method(
        "partition_spec_by_id",
        method!(RbTable::partition_spec_by_id, 1),
    )?;
    class.define_method(
        "default_partition_spec",
        method!(RbTable::default_partition_spec, 0),
    )?;
    class.define_method(
        "default_partition_spec_id",
        method!(RbTable::default_partition_spec_id, 0),
    )?;
    class.define_method("snapshots", method!(RbTable::snapshots, 0))?;
    class.define_method("snapshot_by_id", method!(RbTable::snapshot_by_id, 1))?;
    class.define_method("history", method!(RbTable::history, 0))?;
    class.define_method("metadata_log", method!(RbTable::metadata_log, 0))?;
    class.define_method("current_snapshot", method!(RbTable::current_snapshot, 0))?;
    class.define_method(
        "current_snapshot_id",
        method!(RbTable::current_snapshot_id, 0),
    )?;
    class.define_method("snapshot_for_ref", method!(RbTable::snapshot_for_ref, 1))?;
    class.define_method("sort_orders", method!(RbTable::sort_orders, 0))?;
    class.define_method("sort_order_by_id", method!(RbTable::sort_order_by_id, 1))?;
    class.define_method(
        "default_sort_order",
        method!(RbTable::default_sort_order, 0),
    )?;
    class.define_method(
        "default_sort_order_id",
        method!(RbTable::default_sort_order_id, 0),
    )?;
    class.define_method("properties", method!(RbTable::properties, 0))?;
    class.define_method("statistics", method!(RbTable::statistics, 0))?;
    class.define_method(
        "partition_statistics",
        method!(RbTable::partition_statistics, 0),
    )?;
    class.define_method(
        "statistics_for_snapshot",
        method!(RbTable::statistics_for_snapshot, 1),
    )?;
    class.define_method(
        "partition_statistics_for_snapshot",
        method!(RbTable::partition_statistics_for_snapshot, 1),
    )?;
    class.define_method("encryption_keys", method!(RbTable::encryption_keys, 0))?;
    class.define_method("encryption_key", method!(RbTable::encryption_key, 1))?;
    class.define_singleton_method(
        "from_metadata_file",
        function!(RbTable::from_metadata_file, 1),
    )?;

    let class = module.define_class("RbTableScan", ruby.class_object())?;
    class.define_method("plan_files", method!(RbTableScan::plan_files, 0))?;
    class.define_method("snapshot", method!(RbTableScan::snapshot, 0))?;

    Ok(())
}

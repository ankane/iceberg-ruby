mod arrow;
mod batch;
mod catalog;
mod encryption;
mod error;
mod partitioning;
mod result;
mod ruby;
mod runtime;
mod scan;
mod schema;
mod snapshot;
mod sorting;
mod statistics;
mod table;
mod utils;

use magnus::{Error as RbErr, Ruby, function, method, prelude::*};

use crate::arrow::{RbArrowArrayStream, RbArrowSchema};
use crate::batch::RbArrowRecordBatch;
use crate::catalog::RbCatalog;
#[cfg(feature = "datafusion")]
use crate::catalog::RbSessionContext;
use crate::encryption::RbEncryptedKey;
use crate::partitioning::{RbPartitionField, RbPartitionSpec};
use crate::scan::RbTableScan;
use crate::schema::{RbNestedField, RbSchema};
use crate::snapshot::RbSnapshot;
use crate::sorting::{RbSortField, RbSortOrder};
use crate::statistics::{RbPartitionStatisticsFile, RbStatisticsFile};
use crate::table::{RbTable, RbTableMetadata};

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
    class.define_method("create_table", method!(RbCatalog::create_table, 5))?;
    class.define_method("load_table", method!(RbCatalog::load_table, 1))?;
    class.define_method("drop_table", method!(RbCatalog::drop_table, 1))?;
    class.define_method("table_exists?", method!(RbCatalog::table_exists, 1))?;
    class.define_method("rename_table", method!(RbCatalog::rename_table, 2))?;
    class.define_method("register_table", method!(RbCatalog::register_table, 2))?;
    #[cfg(feature = "datafusion")]
    class.define_method("session_context", method!(RbCatalog::session_context, 1))?;

    #[cfg(feature = "datafusion")]
    {
        let class = module.define_class("SessionContext", ruby.class_object())?;
        class.define_method("sql", method!(RbSessionContext::sql, 2))?;
    }

    let class = module.define_class("RbTable", ruby.class_object())?;
    class.define_method("scan", method!(RbTable::scan, 1))?;
    class.define_method("append", method!(RbTable::append, 2))?;
    class.define_method("metadata", method!(RbTable::metadata, 0))?;
    class.define_singleton_method(
        "from_metadata_file",
        function!(RbTable::from_metadata_file, 1),
    )?;

    let class = module.define_class("TableMetadata", ruby.class_object())?;
    class.define_method(
        "format_version",
        method!(RbTableMetadata::format_version, 0),
    )?;
    class.define_method("uuid", method!(RbTableMetadata::uuid, 0))?;
    class.define_method("location", method!(RbTableMetadata::location, 0))?;
    class.define_method(
        "last_sequence_number",
        method!(RbTableMetadata::last_sequence_number, 0),
    )?;
    class.define_method(
        "next_sequence_number",
        method!(RbTableMetadata::next_sequence_number, 0),
    )?;
    class.define_method(
        "last_column_id",
        method!(RbTableMetadata::last_column_id, 0),
    )?;
    class.define_method(
        "last_partition_id",
        method!(RbTableMetadata::last_partition_id, 0),
    )?;
    class.define_method(
        "last_updated_ms",
        method!(RbTableMetadata::last_updated_ms, 0),
    )?;
    class.define_method("schemas", method!(RbTableMetadata::schemas, 0))?;
    class.define_method("schema_by_id", method!(RbTableMetadata::schema_by_id, 1))?;
    class.define_method(
        "current_schema",
        method!(RbTableMetadata::current_schema, 0),
    )?;
    class.define_method(
        "current_schema_id",
        method!(RbTableMetadata::current_schema_id, 0),
    )?;
    class.define_method(
        "partition_specs",
        method!(RbTableMetadata::partition_specs, 0),
    )?;
    class.define_method(
        "partition_spec_by_id",
        method!(RbTableMetadata::partition_spec_by_id, 1),
    )?;
    class.define_method(
        "default_partition_spec",
        method!(RbTableMetadata::default_partition_spec, 0),
    )?;
    class.define_method(
        "default_partition_spec_id",
        method!(RbTableMetadata::default_partition_spec_id, 0),
    )?;
    class.define_method("snapshots", method!(RbTableMetadata::snapshots, 0))?;
    class.define_method(
        "snapshot_by_id",
        method!(RbTableMetadata::snapshot_by_id, 1),
    )?;
    class.define_method("history", method!(RbTableMetadata::history, 0))?;
    class.define_method("metadata_log", method!(RbTableMetadata::metadata_log, 0))?;
    class.define_method(
        "current_snapshot",
        method!(RbTableMetadata::current_snapshot, 0),
    )?;
    class.define_method(
        "current_snapshot_id",
        method!(RbTableMetadata::current_snapshot_id, 0),
    )?;
    class.define_method(
        "snapshot_for_ref",
        method!(RbTableMetadata::snapshot_for_ref, 1),
    )?;
    class.define_method("sort_orders", method!(RbTableMetadata::sort_orders, 0))?;
    class.define_method(
        "sort_order_by_id",
        method!(RbTableMetadata::sort_order_by_id, 1),
    )?;
    class.define_method(
        "default_sort_order",
        method!(RbTableMetadata::default_sort_order, 0),
    )?;
    class.define_method(
        "default_sort_order_id",
        method!(RbTableMetadata::default_sort_order_id, 0),
    )?;
    class.define_method("properties", method!(RbTableMetadata::properties, 0))?;
    class.define_method("statistics", method!(RbTableMetadata::statistics, 0))?;
    class.define_method(
        "partition_statistics",
        method!(RbTableMetadata::partition_statistics, 0),
    )?;
    class.define_method(
        "statistics_for_snapshot",
        method!(RbTableMetadata::statistics_for_snapshot, 1),
    )?;
    class.define_method(
        "partition_statistics_for_snapshot",
        method!(RbTableMetadata::partition_statistics_for_snapshot, 1),
    )?;
    class.define_method(
        "encryption_keys",
        method!(RbTableMetadata::encryption_keys, 0),
    )?;
    class.define_method(
        "encryption_key",
        method!(RbTableMetadata::encryption_key, 1),
    )?;
    class.define_method("next_row_id", method!(RbTableMetadata::next_row_id, 0))?;

    let class = module.define_class("RbTableScan", ruby.class_object())?;
    class.define_method("plan_files", method!(RbTableScan::plan_files, 0))?;
    class.define_method("snapshot", method!(RbTableScan::snapshot, 0))?;
    class.define_method("collect", method!(RbTableScan::collect, 0))?;

    let class = module.define_class("Schema", ruby.class_object())?;
    class.define_singleton_method("new", function!(RbSchema::new, 1))?;
    class.define_method("fields", method!(RbSchema::fields, 0))?;
    class.define_method("schema_id", method!(RbSchema::schema_id, 0))?;
    class.define_method("arrow_c_schema", method!(RbSchema::arrow_c_schema, 0))?;
    class.define_method("==", method!(RbSchema::eq, 1))?;
    class.define_method("inspect", method!(RbSchema::inspect, 0))?;

    let class = module.define_class("NestedField", ruby.class_object())?;
    class.define_singleton_method("new", function!(RbNestedField::new, 1))?;
    class.define_method("field_id", method!(RbNestedField::field_id, 0))?;
    class.define_method("name", method!(RbNestedField::name, 0))?;
    class.define_method("field_type", method!(RbNestedField::field_type, 0))?;
    class.define_method("required", method!(RbNestedField::required, 0))?;
    class.define_method("doc", method!(RbNestedField::doc, 0))?;
    class.define_method(
        "initial_default",
        method!(RbNestedField::initial_default, 0),
    )?;
    class.define_method("write_default", method!(RbNestedField::write_default, 0))?;
    class.define_method("==", method!(RbNestedField::eq, 1))?;
    class.define_method("inspect", method!(RbNestedField::inspect, 0))?;

    let class = module.define_class("ArrowSchema", ruby.class_object())?;
    class.define_method("to_i", method!(RbArrowSchema::to_i, 0))?;

    let class = module.define_class("ArrowArrayStream", ruby.class_object())?;
    class.define_method("to_i", method!(RbArrowArrayStream::to_i, 0))?;

    let class = module.define_class("ArrowRecordBatch", ruby.class_object())?;
    class.define_singleton_method("new", function!(RbArrowRecordBatch::new, 2))?;
    class.define_method(
        "arrow_c_stream",
        method!(RbArrowRecordBatch::arrow_c_stream, 0),
    )?;

    let class = module.define_class("PartitionSpec", ruby.class_object())?;
    class.define_singleton_method("new", function!(RbPartitionSpec::new, 1))?;
    class.define_method("spec_id", method!(RbPartitionSpec::spec_id, 0))?;

    let class = module.define_class("PartitionField", ruby.class_object())?;
    class.define_singleton_method("new", function!(RbPartitionField::new, 1))?;
    class.define_method("source_id", method!(RbPartitionField::source_id, 0))?;
    class.define_method("field_id", method!(RbPartitionField::field_id, 0))?;
    class.define_method("name", method!(RbPartitionField::name, 0))?;

    let class = module.define_class("SortOrder", ruby.class_object())?;
    class.define_singleton_method("new", function!(RbSortOrder::new, 1))?;
    class.define_method("order_id", method!(RbSortOrder::order_id, 0))?;
    class.define_method("fields", method!(RbSortOrder::fields, 0))?;

    let class = module.define_class("SortField", ruby.class_object())?;
    class.define_singleton_method("new", function!(RbSortField::new, 1))?;
    class.define_method("source_id", method!(RbSortField::source_id, 0))?;

    let class = module.define_class("Snapshot", ruby.class_object())?;
    class.define_method("snapshot_id", method!(RbSnapshot::snapshot_id, 0))?;
    class.define_method(
        "parent_snapshot_id",
        method!(RbSnapshot::parent_snapshot_id, 0),
    )?;
    class.define_method("sequence_number", method!(RbSnapshot::sequence_number, 0))?;
    class.define_method("manifest_list", method!(RbSnapshot::manifest_list, 0))?;
    class.define_method("schema_id", method!(RbSnapshot::schema_id, 0))?;

    let class = module.define_class("EncryptedKey", ruby.class_object())?;
    class.define_method("key_id", method!(RbEncryptedKey::key_id, 0))?;

    let class = module.define_class("StatisticsFile", ruby.class_object())?;
    class.define_method("snapshot_id", method!(RbStatisticsFile::snapshot_id, 0))?;

    let class = module.define_class("PartitionStatisticsFile", ruby.class_object())?;
    class.define_method(
        "snapshot_id",
        method!(RbPartitionStatisticsFile::snapshot_id, 0),
    )?;

    Ok(())
}

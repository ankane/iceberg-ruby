use arrow_array::ffi_stream::ArrowArrayStreamReader;
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::spec::FormatVersion;
use iceberg::table::{StaticTable, Table};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use magnus::{Error as RbErr, RArray, Ruby, Value};
use parquet::file::properties::WriterProperties;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::RbResult;
use crate::arrow::RbArrowType;
use crate::catalog::RbCatalog;
use crate::error::to_rb_err;
use crate::runtime::runtime;
use crate::scan::RbTableScan;
use crate::utils::*;

#[magnus::wrap(class = "Iceberg::RbTable")]
pub struct RbTable {
    pub table: RefCell<Table>,
}

impl RbTable {
    pub fn scan(&self, snapshot_id: Option<i64>) -> RbResult<RbTableScan> {
        let table = self.table.borrow();
        let mut builder = table.scan();
        if let Some(si) = snapshot_id {
            builder = builder.snapshot_id(si);
        }
        let scan = builder.build().map_err(to_rb_err)?;
        Ok(RbTableScan { scan: scan.into() })
    }

    pub fn append(
        ruby: &Ruby,
        rb_self: &Self,
        data: RbArrowType<ArrowArrayStreamReader>,
        catalog: &RbCatalog,
    ) -> RbResult<RbTable> {
        let runtime = runtime();
        let table = rb_self.table.borrow();
        let catalog = catalog.catalog.borrow();

        let table_schema: Arc<arrow_schema::Schema> = Arc::new(
            table
                .metadata()
                .current_schema()
                .as_ref()
                .try_into()
                .unwrap(),
        );

        let location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).map_err(to_rb_err)?;
        let file_name_generator = DefaultFileNameGenerator::new(
            // TODO move task id to suffix to match Python and Java
            "0".to_string(),
            Some(Uuid::new_v4().to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator.clone(),
            file_name_generator.clone(),
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
        let mut data_file_writer = runtime
            .block_on(data_file_writer_builder.build(None))
            .map_err(to_rb_err)?;

        for batch in data.0 {
            let batch = batch
                .unwrap()
                .with_schema(table_schema.clone())
                .map_err(|e| RbErr::new(ruby.exception_arg_error(), e.to_string()))?;
            runtime
                .block_on(data_file_writer.write(batch))
                .map_err(to_rb_err)?;
        }

        let data_files = runtime
            .block_on(data_file_writer.close())
            .map_err(to_rb_err)?;

        let tx = Transaction::new(&table);
        let append_action = tx.fast_append().add_data_files(data_files.clone());
        let tx = append_action.apply(tx).map_err(to_rb_err)?;
        let table = runtime
            .block_on(tx.commit(catalog.as_catalog()))
            .map_err(to_rb_err)?;

        Ok(RbTable {
            table: table.into(),
        })
    }

    pub fn format_version(&self) -> i32 {
        match self.table.borrow().metadata().format_version() {
            FormatVersion::V1 => 1,
            FormatVersion::V2 => 2,
            FormatVersion::V3 => 3,
        }
    }

    pub fn uuid(&self) -> String {
        self.table.borrow().metadata().uuid().to_string()
    }

    pub fn location(&self) -> String {
        self.table.borrow().metadata().location().to_string()
    }

    pub fn last_sequence_number(&self) -> i64 {
        self.table.borrow().metadata().last_sequence_number()
    }

    pub fn next_sequence_number(&self) -> i64 {
        self.table.borrow().metadata().next_sequence_number()
    }

    pub fn last_column_id(&self) -> i32 {
        self.table.borrow().metadata().last_column_id()
    }

    pub fn last_partition_id(&self) -> i32 {
        self.table.borrow().metadata().last_partition_id()
    }

    pub fn last_updated_ms(&self) -> i64 {
        self.table.borrow().metadata().last_updated_ms()
    }

    pub fn schemas(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let schemas = ruby.ary_new();
        for s in rb_self.table.borrow().metadata().schemas_iter() {
            schemas.push(rb_schema(ruby, s)?)?;
        }
        Ok(schemas)
    }

    pub fn schema_by_id(ruby: &Ruby, rb_self: &Self, schema_id: i32) -> RbResult<Option<Value>> {
        let schema = match rb_self.table.borrow().metadata().schema_by_id(schema_id) {
            Some(s) => Some(rb_schema(ruby, s)?),
            None => None,
        };
        Ok(schema)
    }

    pub fn current_schema(ruby: &Ruby, rb_self: &Self) -> RbResult<Value> {
        rb_schema(ruby, rb_self.table.borrow().metadata().current_schema())
    }

    pub fn current_schema_id(&self) -> i32 {
        self.table.borrow().metadata().current_schema_id()
    }

    pub fn partition_specs(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let partition_specs = ruby.ary_new();
        for s in rb_self.table.borrow().metadata().partition_specs_iter() {
            partition_specs.push(rb_partition_spec(s)?)?;
        }
        Ok(partition_specs)
    }

    pub fn partition_spec_by_id(&self, partition_spec_id: i32) -> RbResult<Option<Value>> {
        let partition_spec = match self
            .table
            .borrow()
            .metadata()
            .partition_spec_by_id(partition_spec_id)
        {
            Some(s) => Some(rb_partition_spec(s)?),
            None => None,
        };
        Ok(partition_spec)
    }

    pub fn default_partition_spec(&self) -> RbResult<Value> {
        rb_partition_spec(self.table.borrow().metadata().default_partition_spec())
    }

    pub fn default_partition_spec_id(&self) -> i32 {
        self.table.borrow().metadata().default_partition_spec_id()
    }

    pub fn snapshots(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let snapshots = ruby.ary_new();
        for s in rb_self.table.borrow().metadata().snapshots() {
            snapshots.push(rb_snapshot(ruby, s)?)?;
        }
        Ok(snapshots)
    }

    pub fn snapshot_by_id(
        ruby: &Ruby,
        rb_self: &Self,
        snapshot_id: i64,
    ) -> RbResult<Option<Value>> {
        let snapshot = match rb_self
            .table
            .borrow()
            .metadata()
            .snapshot_by_id(snapshot_id)
        {
            Some(s) => Some(rb_snapshot(ruby, s)?),
            None => None,
        };
        Ok(snapshot)
    }

    pub fn history(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let history = ruby.ary_new();
        for s in rb_self.table.borrow().metadata().history() {
            let snapshot_log = ruby.hash_new();
            snapshot_log.aset(ruby.to_symbol("snapshot_id"), s.snapshot_id)?;
            // TODO timestamp
            history.push(snapshot_log)?;
        }
        Ok(history)
    }

    pub fn metadata_log(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let metadata_logs = ruby.ary_new();
        for s in rb_self.table.borrow().metadata().metadata_log() {
            let metadata_log = ruby.hash_new();
            metadata_log.aset(
                ruby.to_symbol("metadata_file"),
                ruby.str_new(&s.metadata_file),
            )?;
            // TODO timestamp
            metadata_logs.push(metadata_log)?;
        }
        Ok(metadata_logs)
    }

    pub fn current_snapshot(ruby: &Ruby, rb_self: &Self) -> RbResult<Option<Value>> {
        let snapshot = match rb_self.table.borrow().metadata().current_snapshot() {
            Some(s) => Some(rb_snapshot(ruby, s)?),
            None => None,
        };
        Ok(snapshot)
    }

    pub fn current_snapshot_id(&self) -> Option<i64> {
        self.table.borrow().metadata().current_snapshot_id()
    }

    pub fn snapshot_for_ref(
        ruby: &Ruby,
        rb_self: &Self,
        ref_name: String,
    ) -> RbResult<Option<Value>> {
        let snapshot = match rb_self
            .table
            .borrow()
            .metadata()
            .snapshot_for_ref(&ref_name)
        {
            Some(s) => Some(rb_snapshot(ruby, s)?),
            None => None,
        };
        Ok(snapshot)
    }

    pub fn sort_orders(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let sort_orders = ruby.ary_new();
        for s in rb_self.table.borrow().metadata().sort_orders_iter() {
            sort_orders.push(rb_sort_order(s)?)?;
        }
        Ok(sort_orders)
    }

    pub fn sort_order_by_id(&self, sort_order_id: i64) -> RbResult<Option<Value>> {
        let sort_order = match self
            .table
            .borrow()
            .metadata()
            .sort_order_by_id(sort_order_id)
        {
            Some(s) => Some(rb_sort_order(s)?),
            None => None,
        };
        Ok(sort_order)
    }

    pub fn default_sort_order(&self) -> RbResult<Value> {
        rb_sort_order(self.table.borrow().metadata().default_sort_order())
    }

    pub fn default_sort_order_id(&self) -> i64 {
        self.table.borrow().metadata().default_sort_order_id()
    }

    pub fn properties(&self) -> HashMap<String, String> {
        self.table.borrow().metadata().properties().clone()
    }

    pub fn statistics(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let statistics = ruby.ary_new();
        for s in rb_self.table.borrow().metadata().statistics_iter() {
            statistics.push(rb_statistics_file(s)?)?;
        }
        Ok(statistics)
    }

    pub fn partition_statistics(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let statistics = ruby.ary_new();
        for s in rb_self
            .table
            .borrow()
            .metadata()
            .partition_statistics_iter()
        {
            statistics.push(rb_partition_statistics_file(s)?)?;
        }
        Ok(statistics)
    }

    pub fn statistics_for_snapshot(&self, snapshot_id: i64) -> RbResult<Option<Value>> {
        let statistics = match self
            .table
            .borrow()
            .metadata()
            .statistics_for_snapshot(snapshot_id)
        {
            Some(s) => Some(rb_statistics_file(s)?),
            None => None,
        };
        Ok(statistics)
    }

    pub fn partition_statistics_for_snapshot(&self, snapshot_id: i64) -> RbResult<Option<Value>> {
        let statistics = match self
            .table
            .borrow()
            .metadata()
            .partition_statistics_for_snapshot(snapshot_id)
        {
            Some(s) => Some(rb_partition_statistics_file(s)?),
            None => None,
        };
        Ok(statistics)
    }

    pub fn encryption_keys(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let encryption_keys = ruby.ary_new();
        for k in rb_self.table.borrow().metadata().encryption_keys_iter() {
            encryption_keys.push(rb_encrypted_key(k)?)?;
        }
        Ok(encryption_keys)
    }

    pub fn encryption_key(&self, key_id: String) -> RbResult<Option<Value>> {
        let key = match self.table.borrow().metadata().encryption_key(&key_id) {
            Some(k) => Some(rb_encrypted_key(k)?),
            None => None,
        };
        Ok(key)
    }

    pub fn from_metadata_file(location: String) -> RbResult<Self> {
        let file_io = FileIO::from_path(&location).unwrap().build().unwrap();
        let table_ident = TableIdent::from_strs(["static-table", &location]).unwrap();
        let static_table = runtime()
            .block_on(StaticTable::from_metadata_file(
                &location,
                table_ident,
                file_io,
            ))
            .map_err(to_rb_err)?;
        Ok(Self {
            table: static_table.into_table().into(),
        })
    }
}

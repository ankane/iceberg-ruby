use arrow_array::RecordBatch;
use arrow_array::ffi_stream::ArrowArrayStreamReader;
use arrow_cast::cast;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::spec::{FormatVersion, TableMetadata};
use iceberg::table::{StaticTable, Table};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use magnus::{RArray, Ruby};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

use crate::RbResult;
use crate::arrow::RbArrowType;
use crate::catalog::RbCatalog;
use crate::encryption::RbEncryptedKey;
use crate::error::to_rb_err;
use crate::partitioning::RbPartitionSpec;
use crate::ruby::GvlExt;
use crate::runtime::runtime;
use crate::scan::RbTableScan;
use crate::schema::RbSchema;
use crate::snapshot::{RbMetadataLog, RbSnapshot, RbSnapshotLog};
use crate::sorting::RbSortOrder;
use crate::statistics::{RbPartitionStatisticsFile, RbStatisticsFile};

#[magnus::wrap(class = "Iceberg::RbTable")]
pub struct RbTable {
    pub table: RwLock<Table>,
}

impl RbTable {
    pub fn scan(&self, snapshot_id: Option<i64>) -> RbResult<RbTableScan> {
        let table = self.table.read().unwrap();
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
        let table = ruby
            .detach(|| {
                let runtime = runtime();
                let table = rb_self.table.read().unwrap();
                let catalog = catalog.catalog.read().unwrap();

                let table_schema: Arc<arrow_schema::Schema> = Arc::new(
                    table
                        .metadata()
                        .current_schema()
                        .as_ref()
                        .try_into()
                        .unwrap(),
                );

                let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
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
                let rolling_file_writer_builder =
                    RollingFileWriterBuilder::new_with_default_file_size(
                        parquet_writer_builder,
                        table.file_io().clone(),
                        location_generator.clone(),
                        file_name_generator.clone(),
                    );
                let data_file_writer_builder =
                    DataFileWriterBuilder::new(rolling_file_writer_builder);
                let mut data_file_writer =
                    runtime.block_on(data_file_writer_builder.build(None))?;

                for batch in data.0 {
                    let batch = cast_batch(batch.unwrap(), table_schema.clone())?;
                    runtime.block_on(data_file_writer.write(batch))?;
                }

                let data_files = runtime.block_on(data_file_writer.close())?;

                let tx = Transaction::new(&table);
                let append_action = tx.fast_append().add_data_files(data_files.clone());
                let tx = append_action.apply(tx)?;

                runtime.block_on(tx.commit(catalog.as_catalog()))
            })
            .map_err(to_rb_err)?;

        Ok(RbTable {
            table: table.into(),
        })
    }

    pub fn metadata(&self) -> RbTableMetadata {
        RbTableMetadata {
            metadata: self.table.read().unwrap().metadata().clone(),
        }
    }

    pub fn from_metadata_file(location: String) -> RbResult<Self> {
        let file_io = FileIO::new_with_fs();
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

#[magnus::wrap(class = "Iceberg::TableMetadata")]
pub struct RbTableMetadata {
    pub metadata: TableMetadata,
}

impl RbTableMetadata {
    pub fn format_version(&self) -> i32 {
        match self.metadata.format_version() {
            FormatVersion::V1 => 1,
            FormatVersion::V2 => 2,
            FormatVersion::V3 => 3,
        }
    }

    pub fn uuid(&self) -> String {
        self.metadata.uuid().to_string()
    }

    pub fn location(&self) -> String {
        self.metadata.location().to_string()
    }

    pub fn last_sequence_number(&self) -> i64 {
        self.metadata.last_sequence_number()
    }

    pub fn next_sequence_number(&self) -> i64 {
        self.metadata.next_sequence_number()
    }

    pub fn last_column_id(&self) -> i32 {
        self.metadata.last_column_id()
    }

    pub fn last_partition_id(&self) -> i32 {
        self.metadata.last_partition_id()
    }

    pub fn last_updated_ms(&self) -> i64 {
        self.metadata.last_updated_ms()
    }

    pub fn schemas(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(rb_self.metadata.schemas_iter().map(RbSchema::from))
    }

    pub fn schema_by_id(&self, schema_id: i32) -> Option<RbSchema> {
        self.metadata.schema_by_id(schema_id).map(|v| v.into())
    }

    pub fn current_schema(&self) -> RbSchema {
        self.metadata.current_schema().into()
    }

    pub fn current_schema_id(&self) -> i32 {
        self.metadata.current_schema_id()
    }

    pub fn partition_specs(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(
            rb_self
                .metadata
                .partition_specs_iter()
                .map(RbPartitionSpec::from),
        )
    }

    pub fn partition_spec_by_id(&self, partition_spec_id: i32) -> Option<RbPartitionSpec> {
        self.metadata
            .partition_spec_by_id(partition_spec_id)
            .map(|v| v.into())
    }

    pub fn default_partition_spec(&self) -> RbPartitionSpec {
        self.metadata.default_partition_spec().into()
    }

    pub fn default_partition_spec_id(&self) -> i32 {
        self.metadata.default_partition_spec_id()
    }

    pub fn snapshots(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(rb_self.metadata.snapshots().map(RbSnapshot::from))
    }

    pub fn snapshot_by_id(&self, snapshot_id: i64) -> Option<RbSnapshot> {
        self.metadata.snapshot_by_id(snapshot_id).map(|v| v.into())
    }

    pub fn history(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(rb_self.metadata.history().iter().map(RbSnapshotLog::from))
    }

    pub fn metadata_log(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(
            rb_self
                .metadata
                .metadata_log()
                .iter()
                .map(RbMetadataLog::from),
        )
    }

    pub fn current_snapshot(&self) -> Option<RbSnapshot> {
        self.metadata.current_snapshot().map(|v| v.into())
    }

    pub fn current_snapshot_id(&self) -> Option<i64> {
        self.metadata.current_snapshot_id()
    }

    pub fn snapshot_for_ref(&self, ref_name: String) -> Option<RbSnapshot> {
        self.metadata.snapshot_for_ref(&ref_name).map(|v| v.into())
    }

    pub fn sort_orders(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(rb_self.metadata.sort_orders_iter().map(RbSortOrder::from))
    }

    pub fn sort_order_by_id(&self, sort_order_id: i64) -> Option<RbSortOrder> {
        self.metadata
            .sort_order_by_id(sort_order_id)
            .map(|v| v.into())
    }

    pub fn default_sort_order(&self) -> RbSortOrder {
        self.metadata.default_sort_order().into()
    }

    pub fn default_sort_order_id(&self) -> i64 {
        self.metadata.default_sort_order_id()
    }

    pub fn properties(&self) -> HashMap<String, String> {
        self.metadata.properties().clone()
    }

    pub fn statistics(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(
            rb_self
                .metadata
                .statistics_iter()
                .map(RbStatisticsFile::from),
        )
    }

    pub fn partition_statistics(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(
            rb_self
                .metadata
                .partition_statistics_iter()
                .map(RbPartitionStatisticsFile::from),
        )
    }

    pub fn statistics_for_snapshot(&self, snapshot_id: i64) -> Option<RbStatisticsFile> {
        self.metadata
            .statistics_for_snapshot(snapshot_id)
            .map(|v| v.into())
    }

    pub fn partition_statistics_for_snapshot(
        &self,
        snapshot_id: i64,
    ) -> Option<RbPartitionStatisticsFile> {
        self.metadata
            .partition_statistics_for_snapshot(snapshot_id)
            .map(|v| v.into())
    }

    pub fn encryption_keys(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(
            rb_self
                .metadata
                .encryption_keys_iter()
                .map(RbEncryptedKey::from),
        )
    }

    pub fn encryption_key(&self, key_id: String) -> Option<RbEncryptedKey> {
        self.metadata.encryption_key(&key_id).map(|v| v.into())
    }

    pub fn next_row_id(&self) -> u64 {
        self.metadata.next_row_id()
    }
}

fn cast_batch(batch: RecordBatch, table_schema: Arc<Schema>) -> Result<RecordBatch, ArrowError> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (field, column) in batch.schema().fields.iter().zip(batch.columns()) {
        match field.data_type() {
            DataType::Utf8View => {
                fields.push(Arc::new(Field::new(
                    field.name(),
                    DataType::Utf8,
                    field.is_nullable(),
                )));
                columns.push(cast(column, &DataType::Utf8)?);
            }
            DataType::BinaryView => {
                // TODO convert to FixedSizeBinary if needed
                fields.push(Arc::new(Field::new(
                    field.name(),
                    DataType::LargeBinary,
                    field.is_nullable(),
                )));
                columns.push(cast(column, &DataType::LargeBinary)?);
            }
            DataType::Timestamp(time_unit, Some(_)) => {
                fields.push(Arc::new(Field::new(
                    field.name(),
                    DataType::Timestamp(*time_unit, Some("+00:00".into())),
                    field.is_nullable(),
                )));
                columns.push(cast(
                    column,
                    &DataType::Timestamp(*time_unit, Some("+00:00".into())),
                )?);
            }
            _ => {
                // cloning Arc is cheap
                fields.push(field.clone());
                columns.push(column.clone());
            }
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?.with_schema(table_schema)
}

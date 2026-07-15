use iceberg::spec::{MetadataLog, Snapshot, SnapshotLog};
use magnus::{IntoValue, Ruby, value::ReprValue};

#[magnus::wrap(class = "Iceberg::Snapshot")]
pub struct RbSnapshot {
    pub(crate) snapshot: Snapshot,
}

#[magnus::wrap(class = "Iceberg::SnapshotLogEntry")]
pub struct RbSnapshotLogEntry {
    pub(crate) log: SnapshotLog,
}

#[magnus::wrap(class = "Iceberg::MetadataLogEntry")]
pub struct RbMetadataLogEntry {
    pub(crate) log: MetadataLog,
}

impl RbSnapshot {
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot.snapshot_id()
    }

    pub fn parent_snapshot_id(&self) -> Option<i64> {
        self.snapshot.parent_snapshot_id()
    }

    pub fn sequence_number(&self) -> i64 {
        self.snapshot.sequence_number()
    }

    pub fn manifest_list(&self) -> &str {
        self.snapshot.manifest_list()
    }

    pub fn schema_id(&self) -> Option<i32> {
        self.snapshot.schema_id()
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::Snapshot snapshot_id={}, parent_snapshot_id={}, sequence_number={}, schema_id={}>",
            rb_self.snapshot_id().into_value_with(ruby).inspect(),
            rb_self.parent_snapshot_id().into_value_with(ruby).inspect(),
            rb_self.sequence_number().into_value_with(ruby).inspect(),
            rb_self.schema_id().into_value_with(ruby).inspect(),
        )
    }
}

impl RbSnapshotLogEntry {
    pub fn snapshot_id(&self) -> i64 {
        self.log.snapshot_id
    }

    pub fn timestamp_ms(&self) -> i64 {
        self.log.timestamp_ms
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::SnapshotLogEntry snapshot_id={}, timestamp_ms={}>",
            rb_self.snapshot_id().into_value_with(ruby).inspect(),
            rb_self.timestamp_ms().into_value_with(ruby).inspect(),
        )
    }
}

impl RbMetadataLogEntry {
    pub fn metadata_file(&self) -> &str {
        &self.log.metadata_file
    }

    pub fn timestamp_ms(&self) -> i64 {
        self.log.timestamp_ms
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::MetadataLogEntry metadata_file={}, timestamp_ms={}>",
            rb_self.metadata_file().into_value_with(ruby).inspect(),
            rb_self.timestamp_ms().into_value_with(ruby).inspect(),
        )
    }
}

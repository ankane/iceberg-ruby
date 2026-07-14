use iceberg::spec::Snapshot;

#[magnus::wrap(class = "Iceberg::Snapshot")]
pub struct RbSnapshot {
    pub(crate) snapshot: Snapshot,
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
        &self.snapshot.manifest_list()
    }

    pub fn schema_id(&self) -> Option<i32> {
        self.snapshot.schema_id()
    }
}

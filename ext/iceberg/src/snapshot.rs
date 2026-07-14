use iceberg::spec::Snapshot;
use magnus::{IntoValue, Ruby, value::ReprValue};

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
        self.snapshot.manifest_list()
    }

    pub fn schema_id(&self) -> Option<i32> {
        self.snapshot.schema_id()
    }

    pub fn inspect(ruby: &Ruby, self_: &Self) -> String {
        format!(
            "#<Iceberg::Snapshot snapshot_id={}, parent_snapshot_id={}, sequence_number={}, schema_id={}>",
            self_.snapshot_id().into_value_with(ruby).inspect(),
            self_.parent_snapshot_id().into_value_with(ruby).inspect(),
            self_.sequence_number().into_value_with(ruby).inspect(),
            self_.schema_id().into_value_with(ruby).inspect(),
        )
    }
}

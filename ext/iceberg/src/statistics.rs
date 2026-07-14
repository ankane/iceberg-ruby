use iceberg::spec::{PartitionStatisticsFile, StatisticsFile};

#[magnus::wrap(class = "Iceberg::StatisticsFile")]
pub struct RbStatisticsFile {
    pub(crate) file: StatisticsFile,
}

#[magnus::wrap(class = "Iceberg::PartitionStatisticsFile")]
pub struct RbPartitionStatisticsFile {
    pub(crate) file: PartitionStatisticsFile,
}

impl RbStatisticsFile {
    pub fn snapshot_id(&self) -> i64 {
        self.file.snapshot_id
    }
}

impl RbPartitionStatisticsFile {
    pub fn snapshot_id(&self) -> i64 {
        self.file.snapshot_id
    }
}

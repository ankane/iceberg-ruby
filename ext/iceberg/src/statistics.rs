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

    pub fn statistics_path(&self) -> &str {
        &self.file.statistics_path
    }

    pub fn file_size_in_bytes(&self) -> i64 {
        self.file.file_size_in_bytes
    }

    pub fn file_footer_size_in_bytes(&self) -> i64 {
        self.file.file_footer_size_in_bytes
    }
}

impl RbPartitionStatisticsFile {
    pub fn snapshot_id(&self) -> i64 {
        self.file.snapshot_id
    }

    pub fn statistics_path(&self) -> &str {
        &self.file.statistics_path
    }

    pub fn file_size_in_bytes(&self) -> i64 {
        self.file.file_size_in_bytes
    }
}

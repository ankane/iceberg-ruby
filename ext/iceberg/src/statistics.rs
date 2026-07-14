use iceberg::spec::{PartitionStatisticsFile, StatisticsFile};
use magnus::{IntoValue, Ruby, value::ReprValue};

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

    pub fn inspect(ruby: &Ruby, self_: &Self) -> String {
        format!(
            "#<Iceberg::StatisticsFile snapshot_id={}, statistics_path={}, file_size_in_bytes={}, file_footer_size_in_bytes={}>",
            self_.snapshot_id().into_value_with(ruby).inspect(),
            self_.statistics_path().into_value_with(ruby).inspect(),
            self_.file_size_in_bytes().into_value_with(ruby).inspect(),
            self_
                .file_footer_size_in_bytes()
                .into_value_with(ruby)
                .inspect(),
        )
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

    pub fn inspect(ruby: &Ruby, self_: &Self) -> String {
        format!(
            "#<Iceberg::PartitionStatisticsFile snapshot_id={}, statistics_path={}, file_size_in_bytes={}>",
            self_.snapshot_id().into_value_with(ruby).inspect(),
            self_.statistics_path().into_value_with(ruby).inspect(),
            self_.file_size_in_bytes().into_value_with(ruby).inspect(),
        )
    }
}

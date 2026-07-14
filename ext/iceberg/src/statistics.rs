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

    pub fn key_metadata(&self) -> Option<&str> {
        self.file.key_metadata.as_deref()
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::StatisticsFile snapshot_id={}, statistics_path={}, file_size_in_bytes={}, file_footer_size_in_bytes={}, key_metadata={}>",
            rb_self.snapshot_id().into_value_with(ruby).inspect(),
            rb_self.statistics_path().into_value_with(ruby).inspect(),
            rb_self.file_size_in_bytes().into_value_with(ruby).inspect(),
            rb_self
                .file_footer_size_in_bytes()
                .into_value_with(ruby)
                .inspect(),
            rb_self.key_metadata().into_value_with(ruby).inspect(),
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

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::PartitionStatisticsFile snapshot_id={}, statistics_path={}, file_size_in_bytes={}>",
            rb_self.snapshot_id().into_value_with(ruby).inspect(),
            rb_self.statistics_path().into_value_with(ruby).inspect(),
            rb_self.file_size_in_bytes().into_value_with(ruby).inspect(),
        )
    }
}

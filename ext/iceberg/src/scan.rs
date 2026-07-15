use std::sync::RwLock;

use futures::TryStreamExt;
use iceberg::scan::{FileScanTask, TableScan};
use magnus::{IntoValue, RArray, Ruby, Value, value::ReprValue};

use crate::RbResult;
use crate::error::to_rb_err;
use crate::result::collect_batches;
use crate::ruby::GvlExt;
use crate::runtime::runtime;
use crate::snapshot::RbSnapshot;

#[magnus::wrap(class = "Iceberg::RbTableScan")]
pub struct RbTableScan {
    pub scan: RwLock<TableScan>,
}

#[magnus::wrap(class = "Iceberg::FileScanTask")]
pub struct RbFileScanTask {
    pub scan: FileScanTask,
}

#[magnus::wrap(class = "Iceberg::DataFile")]
pub struct RbDataFile {
    pub file_path: String,
    pub record_count: Option<u64>,
}

impl RbTableScan {
    pub fn plan_files(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let plan_files: Vec<_> = ruby
            .detach(|| {
                let scan = rb_self.scan.read().unwrap();
                let runtime = runtime();
                let plan_files = runtime.block_on(scan.plan_files())?;
                runtime.block_on(plan_files.try_collect())
            })
            .map_err(to_rb_err)?;

        let files = ruby.ary_new();
        for v in plan_files {
            files.push(RbFileScanTask { scan: v })?;
        }
        Ok(files)
    }

    pub fn snapshot(&self) -> Option<RbSnapshot> {
        self.scan.read().unwrap().snapshot().map(|v| v.into())
    }

    pub fn collect(ruby: &Ruby, rb_self: &Self) -> RbResult<Value> {
        let runtime = runtime();
        let scan = rb_self.scan.read().unwrap();
        let stream = runtime.block_on(scan.to_arrow()).map_err(to_rb_err)?;
        let batches: Vec<_> = runtime.block_on(stream.try_collect()).map_err(to_rb_err)?;
        collect_batches(ruby, batches)
    }
}

impl RbFileScanTask {
    pub fn file(&self) -> RbDataFile {
        RbDataFile {
            file_path: self.scan.data_file_path.clone(),
            record_count: self.scan.record_count,
        }
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::FileScanTask file={}>",
            rb_self.file().into_value_with(ruby).inspect(),
        )
    }
}

impl RbDataFile {
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    pub fn record_count(&self) -> Option<u64> {
        self.record_count
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::DataFile file_path={}, record_count={}>",
            rb_self.file_path().into_value_with(ruby).inspect(),
            rb_self.record_count().into_value_with(ruby).inspect(),
        )
    }
}

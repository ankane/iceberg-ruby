use std::sync::RwLock;

use arrow_array::RecordBatchIterator;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use futures::TryStreamExt;
use iceberg::scan::{FileScanTask, TableScan};
use magnus::{IntoValue, RArray, Ruby, Value, value::ReprValue};

use crate::RbResult;
use crate::capsule::RbCapsule;
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
    pub file_size_in_bytes: u64,
    pub equality_ids: Option<Vec<i32>>,
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

    pub fn arrow_c_stream(&self) -> RbResult<RbCapsule> {
        let runtime = runtime();
        let scan = self.scan.read().unwrap();
        let stream = runtime.block_on(scan.to_arrow()).map_err(to_rb_err)?;
        let batches: Vec<_> = runtime.block_on(stream.try_collect()).map_err(to_rb_err)?;
        let stream = if batches.is_empty() {
            // TODO fix schema
            FFI_ArrowArrayStream::empty()
        } else {
            let schema = batches[0].schema();
            let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            FFI_ArrowArrayStream::new(Box::new(reader))
        };
        Ok(RbCapsule::new(stream, Some("arrow_array_stream".into())))
    }
}

impl RbFileScanTask {
    pub fn file(&self) -> RbDataFile {
        RbDataFile {
            file_path: self.scan.data_file_path.clone(),
            record_count: self.scan.record_count,
            file_size_in_bytes: self.scan.file_size_in_bytes,
            equality_ids: None,
        }
    }

    pub fn delete_files(ruby: &Ruby, rb_self: &Self) -> RArray {
        ruby.ary_from_iter(rb_self.scan.deletes.iter().map(|v| RbDataFile {
            file_path: v.file_path.clone(),
            record_count: None,
            file_size_in_bytes: v.file_size_in_bytes,
            equality_ids: v.equality_ids.clone(),
        }))
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::FileScanTask file={}, delete_files={}>",
            rb_self.file().into_value_with(ruby).inspect(),
            Self::delete_files(ruby, rb_self)
                .into_value_with(ruby)
                .inspect(),
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

    pub fn file_size_in_bytes(&self) -> u64 {
        self.file_size_in_bytes
    }

    pub fn equality_ids(&self) -> Option<Vec<i32>> {
        self.equality_ids.clone()
    }

    pub fn inspect(ruby: &Ruby, rb_self: &Self) -> String {
        format!(
            "#<Iceberg::DataFile file_path={}, record_count={}, file_size_in_bytes={}, equality_ids={}>",
            rb_self.file_path().into_value_with(ruby).inspect(),
            rb_self.record_count().into_value_with(ruby).inspect(),
            rb_self.file_size_in_bytes().into_value_with(ruby).inspect(),
            rb_self.equality_ids().into_value_with(ruby).inspect(),
        )
    }
}

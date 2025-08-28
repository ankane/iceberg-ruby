use futures::TryStreamExt;
use iceberg::scan::TableScan;
use magnus::{RArray, Ruby, Value};
use std::cell::RefCell;

use crate::RbResult;
use crate::error::to_rb_err;
use crate::runtime::runtime;
use crate::utils::rb_snapshot;

#[magnus::wrap(class = "Iceberg::RbTableScan")]
pub struct RbTableScan {
    pub scan: RefCell<TableScan>,
}

impl RbTableScan {
    pub fn plan_files(ruby: &Ruby, rb_self: &Self) -> RbResult<RArray> {
        let scan = rb_self.scan.borrow();

        let runtime = runtime();
        let plan_files = runtime.block_on(scan.plan_files()).map_err(to_rb_err)?;
        let plan_files: Vec<_> = runtime
            .block_on(plan_files.try_collect())
            .map_err(to_rb_err)?;
        let files = ruby.ary_new();
        for v in plan_files {
            let file = ruby.hash_new();
            file.aset(ruby.to_symbol("start"), v.start)?;
            file.aset(ruby.to_symbol("length"), v.length)?;
            file.aset(ruby.to_symbol("record_count"), v.record_count)?;
            file.aset(ruby.to_symbol("data_file_path"), v.data_file_path)?;
            file.aset(ruby.to_symbol("project_field_ids"), v.project_field_ids)?;

            let deletes = ruby.ary_new();
            for d in v.deletes {
                let delete = ruby.hash_new();
                delete.aset(ruby.to_symbol("file_path"), d.file_path)?;
                delete.aset(ruby.to_symbol("partition_spec_id"), d.partition_spec_id)?;
                delete.aset(ruby.to_symbol("equality_ids"), d.equality_ids)?;
                deletes.push(delete)?;
            }
            file.aset(ruby.to_symbol("deletes"), deletes)?;

            files.push(file)?;
        }
        Ok(files)
    }

    pub fn snapshot(&self) -> RbResult<Option<Value>> {
        match self.scan.borrow().snapshot() {
            Some(s) => Ok(Some(rb_snapshot(s)?)),
            None => Ok(None),
        }
    }
}

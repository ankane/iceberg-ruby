use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_schema::Schema as ArrowSchema;
use arrow_schema::ffi::FFI_ArrowSchema;
use magnus::{Error as RbErr, Ruby, TryConvert, Value, prelude::*};

use crate::RbResult;
use crate::utils::Wrap;

impl TryConvert for Wrap<ArrowArrayStreamReader> {
    fn try_convert(val: Value) -> RbResult<Self> {
        let ruby = Ruby::get_with(val);
        let addr: usize = val.funcall("to_i", ())?;

        // use similar approach as Polars to consume pointer and avoid copy
        let stream_ptr =
            Box::new(unsafe { std::ptr::replace(addr as _, FFI_ArrowArrayStream::empty()) });

        Ok(Wrap(ArrowArrayStreamReader::try_new(*stream_ptr).map_err(
            |e| RbErr::new(ruby.exception_arg_error(), e.to_string()),
        )?))
    }
}

impl TryConvert for Wrap<ArrowSchema> {
    fn try_convert(val: Value) -> RbResult<Self> {
        let ruby = Ruby::get_with(val);
        let addr: usize = val.funcall("to_i", ())?;

        // use similar approach as Polars to consume pointer and avoid copy
        let schema_ptr =
            Box::new(unsafe { std::ptr::replace(addr as _, FFI_ArrowSchema::empty()) });

        Ok(Wrap(ArrowSchema::try_from(&*schema_ptr).map_err(|e| {
            RbErr::new(ruby.exception_arg_error(), e.to_string())
        })?))
    }
}

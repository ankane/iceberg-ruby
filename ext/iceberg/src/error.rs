use std::error::Error as _;

use iceberg::{Error, ErrorKind};
use magnus::{Error as RbErr, RModule, Ruby, prelude::*};

pub fn to_rb_err(err: Error) -> RbErr {
    let mut class_name = match err.kind() {
        ErrorKind::NamespaceAlreadyExists => "NamespaceAlreadyExistsError",
        ErrorKind::NamespaceNotFound => "NoSuchNamespaceError",
        ErrorKind::TableAlreadyExists => "TableAlreadyExistsError",
        ErrorKind::TableNotFound => "NoSuchTableError",
        ErrorKind::FeatureUnsupported => "UnsupportedFeatureError",
        ErrorKind::DataInvalid => "InvalidDataError",
        _ => "Error",
    };

    // no way to get context separately
    // https://github.com/apache/iceberg-rust/issues/1071
    let mut message = err.message().to_string();

    let mut source = err.source();
    while let Some(s) = source {
        let m = s.to_string();
        if message.contains(&m) {
            break;
        }
        message.push_str(": ");
        message.push_str(&m);
        source = s.source();
    }

    if class_name == "Error" {
        let s = err.to_string();
        // for Glue
        if s.contains("EntityNotFoundException") {
            class_name = "NoSuchTableError";

        // for Glue and S3 Tables
        } else if s.contains("AlreadyExistsException")
            || s.contains("A table with an identical name already exists in the namespace.")
        {
            class_name = "TableAlreadyExistsError";
        }
    }

    let class = Ruby::get()
        .unwrap()
        .class_object()
        .const_get::<_, RModule>("Iceberg")
        .unwrap()
        .const_get(class_name)
        .unwrap();

    RbErr::new(class, message)
}

pub fn todo_error<T: std::fmt::Debug>(message: T) -> RbErr {
    let class = Ruby::get()
        .unwrap()
        .class_object()
        .const_get::<_, RModule>("Iceberg")
        .unwrap()
        .const_get("Todo")
        .unwrap();
    RbErr::new(class, format!("not implemented yet: {:?}", message))
}

#[cfg(feature = "datafusion")]
pub fn datafusion_error(err: datafusion::common::DataFusionError) -> RbErr {
    let class = Ruby::get()
        .unwrap()
        .class_object()
        .const_get::<_, RModule>("Iceberg")
        .unwrap()
        .const_get("Error")
        .unwrap();
    RbErr::new(class, err.to_string())
}

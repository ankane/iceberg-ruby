use iceberg::{Error, ErrorKind};
use magnus::{Error as RbErr, RModule, Ruby, prelude::*};

pub fn to_rb_err(err: Error) -> RbErr {
    let mut class_name = match err.kind() {
        ErrorKind::NamespaceAlreadyExists => "NamespaceAlreadyExistsError",
        ErrorKind::NamespaceNotFound => "NamespaceNotFoundError",
        ErrorKind::TableAlreadyExists => "TableAlreadyExistsError",
        ErrorKind::TableNotFound => "TableNotFoundError",
        ErrorKind::FeatureUnsupported => "UnsupportedFeatureError",
        ErrorKind::DataInvalid => "InvalidDataError",
        _ => "Error",
    };

    // no way to get context separately
    // https://github.com/apache/iceberg-rust/issues/1071
    let message = err.message().to_string();

    // for Glue
    if class_name == "Error" && err.to_string().contains("EntityNotFoundException") {
        class_name = "TableNotFoundError";
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

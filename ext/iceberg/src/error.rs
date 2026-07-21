use iceberg::{Error, ErrorKind};
use magnus::{Error as RbErr, RModule, Ruby, prelude::*};

pub fn to_rb_err(err: Error) -> RbErr {
    let class_name = match err.kind() {
        ErrorKind::NamespaceAlreadyExists => "NamespaceAlreadyExistsError",
        ErrorKind::NamespaceNotFound => "NamespaceNotFoundError",
        ErrorKind::TableAlreadyExists => "TableAlreadyExistsError",
        ErrorKind::TableNotFound => "TableNotFoundError",
        ErrorKind::FeatureUnsupported => "UnsupportedFeatureError",
        ErrorKind::DataInvalid => "InvalidDataError",
        _ => "Error",
    };

    let mut class = Ruby::get()
        .unwrap()
        .class_object()
        .const_get::<_, RModule>("Iceberg")
        .unwrap()
        .const_get(class_name)
        .unwrap();

    // no way to get context separately
    // https://github.com/apache/iceberg-rust/issues/1071
    let mut message = err.to_string();

    // TODO remove in 0.12.0
    if message.contains("target schema is not superset of current schema") {
        class = Ruby::get().unwrap().exception_arg_error();
    }

    if class_name != "Error"
        && let Some(index) = message.find(" => ")
    {
        message = message[(index + 4)..].to_string();
    }

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

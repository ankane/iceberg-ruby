use magnus::{Error as RbErr, RModule, Ruby, prelude::*};

pub fn to_rb_err(err: iceberg::Error) -> RbErr {
    let class_name = match err.kind() {
        iceberg::ErrorKind::NamespaceAlreadyExists => "NamespaceAlreadyExistsError",
        iceberg::ErrorKind::NamespaceNotFound => "NamespaceNotFoundError",
        iceberg::ErrorKind::TableAlreadyExists => "TableAlreadyExistsError",
        iceberg::ErrorKind::TableNotFound => "TableNotFoundError",
        iceberg::ErrorKind::FeatureUnsupported => "UnsupportedFeatureError",
        iceberg::ErrorKind::DataInvalid => "InvalidDataError",
        _ => "Error",
    };

    let class = Ruby::get()
        .unwrap()
        .class_object()
        .const_get::<_, RModule>("Iceberg")
        .unwrap()
        .const_get(class_name)
        .unwrap();

    // no way to get context separately
    // https://github.com/apache/iceberg-rust/issues/1071
    let message = err.to_string();
    let message = message
        // TODO improve
        .strip_prefix("Unexpected => ")
        .map(|v| v.to_string())
        .unwrap_or(message);

    RbErr::new(class, message)
}

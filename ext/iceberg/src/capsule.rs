use std::any::Any;

#[magnus::wrap(class = "Iceberg::Capsule")]
pub struct RbCapsule {
    value: Box<dyn Any + Send>,
    name: Option<String>,
}

impl RbCapsule {
    pub fn new<T: 'static + Send>(value: T, name: Option<String>) -> Self {
        Self {
            value: Box::new(value),
            name,
        }
    }

    pub fn to_i(&self) -> usize {
        (&*self.value as *const dyn Any as *const ()) as usize
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

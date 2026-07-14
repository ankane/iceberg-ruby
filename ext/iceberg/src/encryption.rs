use iceberg::spec::EncryptedKey;
use magnus::{IntoValue, Ruby, value::ReprValue};
use std::collections::HashMap;

#[magnus::wrap(class = "Iceberg::EncryptedKey")]
pub struct RbEncryptedKey {
    pub(crate) key: EncryptedKey,
}

impl RbEncryptedKey {
    pub fn key_id(&self) -> &str {
        self.key.key_id()
    }

    pub fn encrypted_by_id(&self) -> Option<&str> {
        self.key.encrypted_by_id()
    }

    pub fn properties(&self) -> HashMap<String, String> {
        self.key.properties().clone()
    }

    pub fn inspect(ruby: &Ruby, self_: &Self) -> String {
        format!(
            "#<Iceberg::EncryptedKey key_id={}, encrypted_by_id={}, properties={}>",
            self_.key_id().into_value_with(ruby).inspect(),
            self_.encrypted_by_id().into_value_with(ruby).inspect(),
            self_.properties().into_value_with(ruby).inspect(),
        )
    }
}

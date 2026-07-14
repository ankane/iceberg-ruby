use iceberg::spec::EncryptedKey;
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
}

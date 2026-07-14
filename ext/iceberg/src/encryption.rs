use iceberg::spec::EncryptedKey;

#[magnus::wrap(class = "Iceberg::EncryptedKey")]
pub struct RbEncryptedKey {
    pub(crate) key: EncryptedKey,
}

impl RbEncryptedKey {
    pub fn key_id(&self) -> &str {
        self.key.key_id()
    }
}

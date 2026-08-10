use aws_sdk_kms::{Client, primitives::Blob, types::DataKeySpec};
use base64::Engine;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::{EncryptionError, EncryptionProvider, EnvProvider};

pub struct AwsKmsProvider {
    key_id: String,
    region: Option<String>,
    wrapped_key: Vec<u8>,
    local: EnvProvider,
}

impl AwsKmsProvider {
    pub fn new_from_env() -> Result<Self, EncryptionError> {
        let key_id = std::env::var("DASH_AWS_KMS_KEY_ID").map_err(|_| {
            EncryptionError::Config(
                "DASH_AWS_KMS_KEY_ID is required for aws-kms provider".to_string(),
            )
        })?;
        let region = std::env::var("DASH_AWS_KMS_REGION").ok();
        let wrapped_key_file = std::env::var("DASH_ENCRYPTION_WRAPPED_KEY_FILE")
            .or_else(|_| {
                std::env::var("DASH_PERSISTENCE_PATH").map(|p| {
                    let mut path = std::path::PathBuf::from(p);
                    path.push(".dash-wrapped-key");
                    path.to_string_lossy().into_owned()
                })
            })
            .unwrap_or_else(|_| ".dash-wrapped-key".to_string());

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| EncryptionError::Config(format!("failed to start tokio runtime: {e}")))?;

        let client = build_client(region.as_deref(), &runtime)?;

        let (plaintext, wrapped_key) = if let Ok(raw) = std::env::var("DASH_ENCRYPTION_MASTER_KEY")
        {
            let trimmed = raw.trim();
            let encoded = trimmed.strip_prefix("base64:").unwrap_or(trimmed);
            let wrapped = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| {
                    EncryptionError::Config(
                        "DASH_ENCRYPTION_MASTER_KEY is not valid base64".to_string(),
                    )
                })?;
            let plaintext =
                runtime.block_on(decrypt_data_key(&client, &key_id, wrapped.clone()))?;
            (plaintext, wrapped)
        } else if std::path::Path::new(&wrapped_key_file).exists() {
            let encoded = std::fs::read_to_string(&wrapped_key_file).map_err(|e| {
                EncryptionError::Config(format!(
                    "failed to read wrapped key from {wrapped_key_file}: {e}"
                ))
            })?;
            let encoded = encoded.trim();
            if encoded.is_empty() {
                return Err(EncryptionError::Config(format!(
                    "wrapped key file {wrapped_key_file} is empty"
                )));
            }
            let wrapped = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| {
                    EncryptionError::Config("stored wrapped key is not valid base64".to_string())
                })?;
            let plaintext =
                runtime.block_on(decrypt_data_key(&client, &key_id, wrapped.clone()))?;
            (plaintext, wrapped)
        } else {
            let (plaintext, wrapped) = runtime.block_on(generate_data_key(&client, &key_id))?;
            let encoded = base64::engine::general_purpose::STANDARD.encode(&wrapped);
            std::fs::write(&wrapped_key_file, encoded).map_err(|e| {
                EncryptionError::Config(format!(
                    "failed to write wrapped key to {wrapped_key_file}: {e}"
                ))
            })?;
            #[cfg(unix)]
            {
                let mut perms = std::fs::metadata(&wrapped_key_file)
                    .map_err(|e| {
                        EncryptionError::Config(format!(
                            "failed to read permissions for {wrapped_key_file}: {e}"
                        ))
                    })?
                    .permissions();
                perms.set_mode(0o600);
                std::fs::set_permissions(&wrapped_key_file, perms).map_err(|e| {
                    EncryptionError::Config(format!(
                        "failed to set permissions on {wrapped_key_file}: {e}"
                    ))
                })?;
            }
            (plaintext, wrapped)
        };

        let plaintext: [u8; 32] = plaintext.try_into().map_err(|_| {
            EncryptionError::Config("KMS data key is not exactly 32 bytes".to_string())
        })?;

        Ok(Self {
            key_id,
            region,
            wrapped_key,
            local: EnvProvider::new(plaintext),
        })
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    pub fn wrapped_key_base64(&self) -> String {
        base64::engine::general_purpose::STANDARD.encode(&self.wrapped_key)
    }
}

fn build_client(
    region: Option<&str>,
    runtime: &tokio::runtime::Runtime,
) -> Result<Client, EncryptionError> {
    let config = if let Some(region) = region {
        let region = aws_config::Region::new(region.to_string());
        runtime.block_on(async {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(region)
                .load()
                .await
        })
    } else {
        runtime.block_on(async {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .load()
                .await
        })
    };
    Ok(Client::new(&config))
}

async fn generate_data_key(
    client: &Client,
    key_id: &str,
) -> Result<(Vec<u8>, Vec<u8>), EncryptionError> {
    let output = client
        .generate_data_key()
        .key_id(key_id)
        .key_spec(DataKeySpec::Aes256)
        .send()
        .await
        .map_err(|e| EncryptionError::Config(format!("KMS GenerateDataKey failed: {e}")))?;

    let plaintext = output
        .plaintext
        .ok_or_else(|| {
            EncryptionError::Config("KMS GenerateDataKey did not return plaintext".to_string())
        })?
        .into_inner();
    let ciphertext = output
        .ciphertext_blob
        .ok_or_else(|| {
            EncryptionError::Config("KMS GenerateDataKey did not return ciphertext".to_string())
        })?
        .into_inner();
    Ok((plaintext, ciphertext))
}

async fn decrypt_data_key(
    client: &Client,
    key_id: &str,
    wrapped: Vec<u8>,
) -> Result<Vec<u8>, EncryptionError> {
    let output = client
        .decrypt()
        .ciphertext_blob(Blob::new(wrapped))
        .key_id(key_id)
        .send()
        .await
        .map_err(|e| EncryptionError::Config(format!("KMS Decrypt failed: {e}")))?;

    output
        .plaintext
        .ok_or_else(|| EncryptionError::Config("KMS Decrypt did not return plaintext".to_string()))
        .map(|blob| blob.into_inner())
}

impl EncryptionProvider for AwsKmsProvider {
    fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        self.local.encrypt(plaintext, aad)
    }

    fn decrypt(&self, ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        self.local.decrypt(ciphertext, aad)
    }

    fn name(&self) -> &'static str {
        "aws-kms"
    }
}

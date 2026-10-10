//! AWS KMS as KEK provider: `Encrypt` / `Decrypt` of the JSON API
//! (`X-Amz-Target: TrentService.*`), signed with SigV4.
//!
//! The file id is bound through the KMS encryption context
//! (`{"dash-file-id": "<hex>"}`): KMS refuses to decrypt with any other
//! context, and CloudTrail records it with every call. The KMS ciphertext
//! blob names the key and key material version itself, so automatic key
//! rotation in KMS keeps every older file readable.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use url::Url;
use zeroize::Zeroizing;

use super::aws_creds::{CredentialProvider, CredentialSource};
use super::sigv4::{self, SigningRequest, SigningScope};
use super::{
    HttpClient, KmsHttpOptions, Reply, Step, UnwrapCache, host_header, retryable_status, sanitize,
    transport_step, unix_now, with_retries,
};
use crate::{DEK_LEN, EncryptionError, KekProvider, WrappedDek};

const KEY_ID_PREFIX: &str = "aws-kms:";
/// Encryption context key that carries the file id.
pub const CONTEXT_KEY: &str = "dash-file-id";

/// Settings of [`AwsKmsKekProvider`].
#[derive(Debug, Clone)]
pub struct AwsKmsConfig {
    /// Key new files are wrapped with: key id, key ARN, alias name
    /// (`alias/dash`) or alias ARN.
    pub key_id: String,
    /// Further keys that still unwrap (after switching to another key).
    pub previous_key_ids: Vec<String>,
    pub region: String,
    /// Endpoint override (`http://localhost:4566` for LocalStack, a VPC
    /// endpoint). Default `https://kms.<region>.amazonaws.com`.
    pub endpoint: Option<String>,
    pub credentials: CredentialSource,
    pub http: KmsHttpOptions,
    pub allow_insecure_http: bool,
}

/// The id recorded in file headers for a KMS key: `aws-kms:<key>`.
pub fn aws_kms_key_id(key: &str) -> String {
    format!("{KEY_ID_PREFIX}{key}")
}

/// True when `key_id` names a KMS key through an alias. Aliases can be
/// re-pointed, so Decrypt is sent without `KeyId` for them and KMS picks
/// the key from the ciphertext.
fn is_alias(key: &str) -> bool {
    key.starts_with("alias/") || key.contains(":alias/")
}

fn valid_kms_key(key: &str) -> bool {
    !key.is_empty()
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b':' | b'/' | b'-' | b'_' | b'.'))
}

/// The region of a key ARN (`arn:aws:kms:<region>:...`).
pub(crate) fn region_from_arn(key: &str) -> Option<&str> {
    let mut parts = key.split(':');
    (parts.next()? == "arn").then_some(())?;
    parts.next()?;
    (parts.next()? == "kms").then_some(())?;
    parts.next().filter(|r| !r.is_empty())
}

struct KmsKey {
    id: String,
    key: String,
}

/// KEKs held by AWS KMS.
pub struct AwsKmsKekProvider {
    url: Url,
    host: String,
    region: String,
    keys: Vec<KmsKey>,
    creds: CredentialProvider,
    http: HttpClient,
    opts: KmsHttpOptions,
    cache: UnwrapCache,
}

impl std::fmt::Debug for AwsKmsKekProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsKmsKekProvider")
            .field("endpoint", &self.url.as_str())
            .field("active_key_id", &self.keys[0].id)
            .finish_non_exhaustive()
    }
}

impl AwsKmsKekProvider {
    /// Validates the settings. Makes no request; see
    /// [`crate::check_provider`] for the start-up round trip.
    pub fn new(config: AwsKmsConfig) -> Result<Self, EncryptionError> {
        use crate::global::{
            AWS_KMS_ENDPOINT_ENV, AWS_KMS_KEY_ID_ENV, AWS_KMS_PREVIOUS_KEY_IDS_ENV, AWS_REGION_ENV,
        };
        let region = config.region.trim().to_string();
        if region.is_empty()
            || !region
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(EncryptionError::Config(format!(
                "{AWS_REGION_ENV}: '{region}' is not an AWS region"
            )));
        }
        let endpoint = config
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|e| !e.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("https://kms.{region}.amazonaws.com/"));
        let url =
            super::parse_endpoint(&endpoint, AWS_KMS_ENDPOINT_ENV, config.allow_insecure_http)?;
        let mut keys: Vec<KmsKey> = Vec::new();
        let names = std::iter::once(config.key_id.trim())
            .chain(config.previous_key_ids.iter().map(|k| k.trim()));
        for (idx, key) in names.enumerate() {
            let setting = if idx == 0 {
                AWS_KMS_KEY_ID_ENV
            } else {
                AWS_KMS_PREVIOUS_KEY_IDS_ENV
            };
            if key.is_empty() && idx > 0 {
                continue;
            }
            let id = aws_kms_key_id(key);
            if !valid_kms_key(key) || !crate::header::valid_key_id(&id) {
                return Err(EncryptionError::Config(format!(
                    "{setting}: '{key}' is not a KMS key id, key ARN, alias name or alias ARN"
                )));
            }
            if !keys.iter().any(|k| k.id == id) {
                keys.push(KmsKey {
                    id,
                    key: key.to_string(),
                });
            }
        }
        let creds = CredentialProvider::new(config.credentials, &config.http)?;
        let http = HttpClient::new(url.scheme() == "https", None, &config.http)?;
        Ok(Self {
            host: host_header(&url),
            url,
            region,
            keys,
            creds,
            http,
            opts: config.http,
            cache: UnwrapCache::new(),
        })
    }

    fn context_json(context: &[u8]) -> String {
        format!("{{\"{CONTEXT_KEY}\":\"{}\"}}", hex::encode(context))
    }

    /// One KMS call (`Encrypt` or `Decrypt`) with signing, retries and a
    /// credential refresh when KMS reports them expired.
    fn call(
        &self,
        action: &str,
        key: &KmsKey,
        body: &Zeroizing<String>,
    ) -> Result<Zeroizing<Vec<u8>>, EncryptionError> {
        let what = format!("AWS KMS {action} with key {} at {}", key.key, self.url);
        let target = format!("TrentService.{action}");
        let mut force_refresh = false;
        with_retries(&self.opts, &what, || {
            let creds = match self.creds.get(force_refresh) {
                Ok(c) => c,
                Err(err) => return Step::Fail(err),
            };
            force_refresh = false;
            let amz_date = sigv4::amz_date(unix_now());
            let mut headers: Vec<(&str, &str)> = vec![
                ("Content-Type", "application/x-amz-json-1.1"),
                ("Host", self.host.as_str()),
                ("X-Amz-Date", amz_date.as_str()),
                ("X-Amz-Target", target.as_str()),
            ];
            if let Some(token) = &creds.session_token {
                headers.push(("X-Amz-Security-Token", token.as_str()));
            }
            let path = self.url.path().to_string();
            let authorization = sigv4::authorization(
                &creds.access_key_id,
                &creds.secret_access_key,
                &SigningScope {
                    amz_date: &amz_date,
                    region: &self.region,
                    service: "kms",
                },
                &SigningRequest {
                    method: "POST",
                    path: &path,
                    query: &[],
                    headers: &headers,
                    payload: body.as_bytes(),
                },
            );
            headers.push(("Authorization", authorization.as_str()));
            headers.push(("Accept", "application/json"));
            let mut secrets: Vec<&str> = vec![creds.secret_access_key.as_str()];
            if let Some(token) = &creds.session_token {
                secrets.push(token.as_str());
            }
            let reply = match self.http.send(
                "POST",
                self.url.as_str(),
                &headers,
                Some(body.as_bytes()),
                self.opts.attempt_timeout,
                &secrets,
            ) {
                Ok(reply) => reply,
                Err(failure) => return transport_step(failure, &self.opts),
            };
            let step = self.interpret(action, key, reply, &secrets);
            if matches!(step, Step::Again(_)) {
                force_refresh = true;
            }
            step
        })
    }

    fn interpret(
        &self,
        action: &str,
        key: &KmsKey,
        reply: Reply,
        secrets: &[&str],
    ) -> Step<Zeroizing<Vec<u8>>> {
        let status = reply.status;
        if (200..300).contains(&status) {
            return Step::Done(reply.body);
        }
        let parsed: Option<serde_json::Value> = serde_json::from_slice(&reply.body).ok();
        let kind = parsed
            .as_ref()
            .and_then(|v| v.get("__type"))
            .and_then(|t| t.as_str())
            .map(|t| t.rsplit('#').next().unwrap_or(t).to_string())
            .unwrap_or_default();
        let message = parsed
            .as_ref()
            .and_then(|v| v.get("message").or_else(|| v.get("Message")))
            .and_then(|m| m.as_str())
            .unwrap_or("");
        let detail = if kind.is_empty() && message.is_empty() {
            sanitize(&reply.body, secrets)
        } else {
            sanitize(format!("{kind}: {message}").as_bytes(), secrets)
        };
        match kind.as_str() {
            "ExpiredTokenException" | "ExpiredToken" if self.creds.refreshable() => {
                return Step::Again(format!("HTTP {status}: {detail}"));
            }
            "ThrottlingException"
            | "KMSInternalException"
            | "KeyUnavailableException"
            | "DependencyTimeoutException"
            | "RequestLimitExceeded" => {
                return Step::Retry {
                    error: format!("HTTP {status}: {detail}"),
                    retry_after: reply.retry_after,
                };
            }
            "InvalidCiphertextException" | "IncorrectKeyException" if action == "Decrypt" => {
                return Step::Fail(EncryptionError::Authentication(format!(
                    "data key (AWS KMS Decrypt with key {} refused the ciphertext: {detail})",
                    key.key
                )));
            }
            _ => {}
        }
        if retryable_status(status) {
            return Step::Retry {
                error: format!("HTTP {status}: {detail}"),
                retry_after: reply.retry_after,
            };
        }
        let hint = match kind.as_str() {
            "UnrecognizedClientException"
            | "InvalidSignatureException"
            | "ExpiredTokenException" => {
                format!(" (credentials: {})", self.creds.describe())
            }
            "AccessDeniedException" => format!(
                " (the principal needs kms:Encrypt and kms:Decrypt on {})",
                key.key
            ),
            _ => String::new(),
        };
        Step::Fail(EncryptionError::Kms(format!(
            "AWS KMS {action} with key {} at {}: HTTP {status}: {detail}{hint}",
            key.key, self.url
        )))
    }
}

fn json_b64_field(body: &[u8], field: &str) -> Option<Zeroizing<Vec<u8>>> {
    let mut value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let out = value
        .get(field)
        .and_then(|f| f.as_str())
        .and_then(|s| B64.decode(s).ok())
        .map(Zeroizing::new);
    if let Some(serde_json::Value::String(s)) = value.get_mut(field) {
        zeroize::Zeroize::zeroize(s);
    }
    out
}

impl KekProvider for AwsKmsKekProvider {
    fn name(&self) -> &'static str {
        "aws-kms"
    }

    fn active_key_id(&self) -> &str {
        &self.keys[0].id
    }

    fn key_ids(&self) -> Vec<String> {
        self.keys.iter().map(|k| k.id.clone()).collect()
    }

    fn wrap(&self, dek: &[u8; DEK_LEN], context: &[u8]) -> Result<WrappedDek, EncryptionError> {
        let key = &self.keys[0];
        let plaintext = Zeroizing::new(B64.encode(dek));
        let body = Zeroizing::new(format!(
            "{{\"KeyId\":\"{}\",\"Plaintext\":\"{}\",\"EncryptionContext\":{}}}",
            key.key,
            plaintext.as_str(),
            Self::context_json(context)
        ));
        let reply = self.call("Encrypt", key, &body)?;
        let blob = json_b64_field(&reply, "CiphertextBlob").ok_or_else(|| {
            EncryptionError::Kms(format!(
                "AWS KMS Encrypt with key {}: unexpected response (no CiphertextBlob)",
                key.key
            ))
        })?;
        if blob.is_empty() || blob.len() > 4096 {
            return Err(EncryptionError::Kms(format!(
                "AWS KMS Encrypt with key {}: ciphertext of {} bytes does not fit a file header",
                key.key,
                blob.len()
            )));
        }
        Ok(WrappedDek {
            key_id: key.id.clone(),
            bytes: blob.to_vec(),
        })
    }

    fn unwrap(
        &self,
        key_id: &str,
        wrapped: &[u8],
        context: &[u8],
    ) -> Result<Zeroizing<[u8; DEK_LEN]>, EncryptionError> {
        let key = self.keys.iter().find(|k| k.id == key_id).ok_or_else(|| {
            EncryptionError::UnknownKey {
                what: "data key".to_string(),
                key_id: key_id.to_string(),
                configured: self.key_ids().join(", "),
            }
        })?;
        if let Some(dek) = self.cache.get(key_id, wrapped, context) {
            return Ok(dek);
        }
        let key_field = if is_alias(&key.key) {
            String::new()
        } else {
            format!(",\"KeyId\":\"{}\"", key.key)
        };
        let body = Zeroizing::new(format!(
            "{{\"CiphertextBlob\":\"{}\",\"EncryptionContext\":{}{key_field}}}",
            B64.encode(wrapped),
            Self::context_json(context)
        ));
        let reply = self.call("Decrypt", key, &body)?;
        let plain = json_b64_field(&reply, "Plaintext").ok_or_else(|| {
            EncryptionError::Kms(format!(
                "AWS KMS Decrypt with key {}: unexpected response (no Plaintext)",
                key.key
            ))
        })?;
        if plain.len() != DEK_LEN {
            return Err(EncryptionError::Authentication("data key".to_string()));
        }
        let mut dek = Zeroizing::new([0u8; DEK_LEN]);
        dek.copy_from_slice(&plain);
        self.cache.put(key_id, wrapped, context, &dek);
        Ok(dek)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::kms::aws_creds::AwsCredentials;
    use crate::kms::mock::{MOCK_ACCESS_KEY, MOCK_SECRET_KEY, MockKms, MockResponse, MockServer};

    fn static_creds() -> CredentialSource {
        CredentialSource::Static(AwsCredentials {
            access_key_id: MOCK_ACCESS_KEY.to_string(),
            secret_access_key: Zeroizing::new(MOCK_SECRET_KEY.to_string()),
            session_token: None,
            expires_at: None,
        })
    }

    fn config(kms: &MockKms, key: &str) -> AwsKmsConfig {
        AwsKmsConfig {
            key_id: key.to_string(),
            previous_key_ids: vec![],
            region: "us-east-1".to_string(),
            endpoint: Some(kms.addr()),
            credentials: static_creds(),
            http: KmsHttpOptions {
                attempt_timeout: Duration::from_millis(500),
                max_retries: 2,
                backoff_base: Duration::from_millis(5),
                backoff_max: Duration::from_millis(20),
            },
            allow_insecure_http: false,
        }
    }

    const KEY: &str = "1234abcd-12ab-34cd-56ef-1234567890ab";

    #[test]
    fn round_trip_with_signed_requests_and_context_binding() {
        let kms = MockKms::start(&[KEY]);
        let provider = AwsKmsKekProvider::new(config(&kms, KEY)).unwrap();
        assert_eq!(provider.active_key_id(), format!("aws-kms:{KEY}"));
        let dek = [9u8; 32];
        let wrapped = provider.wrap(&dek, b"file-1").unwrap();
        assert!(crate::header::valid_key_id(&wrapped.key_id));
        let fresh = AwsKmsKekProvider::new(config(&kms, KEY)).unwrap();
        assert_eq!(
            *fresh
                .unwrap(&wrapped.key_id, &wrapped.bytes, b"file-1")
                .unwrap(),
            dek
        );
        let err = fresh
            .unwrap(&wrapped.key_id, &wrapped.bytes, b"file-2")
            .unwrap_err();
        assert!(matches!(err, EncryptionError::Authentication(_)), "{err}");
        assert!(
            err.to_string().contains("InvalidCiphertextException"),
            "{err}"
        );
        // Every request carried a valid SigV4 signature (the mock checks).
        assert_eq!(kms.bad_signatures(), 0);
        let last = kms.last_request().unwrap();
        assert_eq!(
            last.header("x-amz-target").as_deref(),
            Some("TrentService.Decrypt")
        );
        let hosts: Vec<_> = last.headers.iter().filter(|(n, _)| n == "host").collect();
        assert_eq!(
            hosts.len(),
            1,
            "exactly one Host header: {:?}",
            last.headers
        );
        assert_eq!(hosts[0].1, kms.addr().trim_start_matches("http://"));
        let body: serde_json::Value = serde_json::from_slice(&last.body).unwrap();
        assert_eq!(
            body["EncryptionContext"][CONTEXT_KEY],
            hex::encode(b"file-2")
        );
        assert_eq!(body["KeyId"], KEY);
    }

    #[test]
    fn a_wrong_secret_is_rejected_by_the_signature_check() {
        let kms = MockKms::start(&[KEY]);
        let mut cfg = config(&kms, KEY);
        cfg.credentials = CredentialSource::Static(AwsCredentials {
            access_key_id: MOCK_ACCESS_KEY.to_string(),
            secret_access_key: Zeroizing::new("not-the-secret".to_string()),
            session_token: None,
            expires_at: None,
        });
        let provider = AwsKmsKekProvider::new(cfg).unwrap();
        let err = provider.wrap(&[1u8; 32], b"f").unwrap_err().to_string();
        assert!(err.contains("InvalidSignatureException"), "{err}");
        assert!(!err.contains("not-the-secret"), "{err}");
        assert_eq!(kms.bad_signatures(), 1);
    }

    #[test]
    fn throttling_and_server_errors_are_retried() {
        let kms = MockKms::start(&[KEY]);
        let provider = AwsKmsKekProvider::new(config(&kms, KEY)).unwrap();
        kms.throttle_next(2);
        provider.wrap(&[1u8; 32], b"f").unwrap();
        assert_eq!(kms.requests(), 3);
        kms.server.fail_next(5, 500);
        let err = provider.wrap(&[1u8; 32], b"f").unwrap_err().to_string();
        assert!(err.contains("gave up after 3 attempts"), "{err}");
    }

    #[test]
    fn aliases_and_previous_keys() {
        let kms = MockKms::start(&[KEY, "old-key"]);
        kms.alias("alias/dash", KEY);
        let old = AwsKmsKekProvider::new(config(&kms, "old-key")).unwrap();
        let old_wrapped = old.wrap(&[5u8; 32], b"f").unwrap();
        let mut cfg = config(&kms, "alias/dash");
        cfg.previous_key_ids = vec!["old-key".to_string()];
        let provider = AwsKmsKekProvider::new(cfg).unwrap();
        assert_eq!(
            provider.key_ids(),
            vec!["aws-kms:alias/dash", "aws-kms:old-key"]
        );
        let wrapped = provider.wrap(&[6u8; 32], b"g").unwrap();
        assert_eq!(wrapped.key_id, "aws-kms:alias/dash");
        assert_eq!(
            *provider
                .unwrap(&old_wrapped.key_id, &old_wrapped.bytes, b"f")
                .unwrap(),
            [5u8; 32]
        );
        let fresh = AwsKmsKekProvider::new(config(&kms, "alias/dash")).unwrap();
        assert_eq!(
            *fresh.unwrap(&wrapped.key_id, &wrapped.bytes, b"g").unwrap(),
            [6u8; 32]
        );
        // Decrypt through an alias is sent without KeyId.
        let body: serde_json::Value =
            serde_json::from_slice(&kms.last_request().unwrap().body).unwrap();
        assert!(body.get("KeyId").is_none(), "{body}");
    }

    #[test]
    fn temporary_credentials_from_web_identity_and_container_endpoints() {
        let dir = tempfile::tempdir().unwrap();
        let kms = MockKms::start(&[KEY]);
        kms.expect_session_token("session-1");
        let sts = MockServer::start(|req| {
            let body = String::from_utf8_lossy(&req.body).into_owned();
            assert!(body.contains("Action=AssumeRoleWithWebIdentity"), "{body}");
            assert!(body.contains("WebIdentityToken=jwt-token"), "{body}");
            MockResponse::new(
                200,
                &format!(
                    "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>{MOCK_ACCESS_KEY}</AccessKeyId><SecretAccessKey>{MOCK_SECRET_KEY}</SecretAccessKey><SessionToken>session-1</SessionToken><Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
                ),
            )
        });
        let token_file = dir.path().join("web-identity");
        std::fs::write(&token_file, "jwt-token\n").unwrap();
        let mut cfg = config(&kms, KEY);
        cfg.credentials = CredentialSource::WebIdentity {
            role_arn: "arn:aws:iam::111122223333:role/dash".to_string(),
            token_file,
            session_name: "dash-encryption".to_string(),
            sts_endpoint: sts.addr(),
        };
        let provider = AwsKmsKekProvider::new(cfg).unwrap();
        let wrapped = provider.wrap(&[3u8; 32], b"f").unwrap();
        provider
            .unwrap(&wrapped.key_id, &wrapped.bytes, b"f")
            .unwrap();
        assert_eq!(
            sts.count(),
            1,
            "credentials are cached until close to expiry"
        );

        let container = MockServer::start(|req| {
            assert_eq!(req.header("authorization").as_deref(), Some("pod-token"));
            MockResponse::new(
                200,
                &format!(
                    "{{\"AccessKeyId\":\"{MOCK_ACCESS_KEY}\",\"SecretAccessKey\":\"{MOCK_SECRET_KEY}\",\"Token\":\"session-1\",\"Expiration\":\"2099-01-01T00:00:00Z\"}}"
                ),
            )
        });
        let auth_file = dir.path().join("pod-token");
        std::fs::write(&auth_file, "pod-token").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&auth_file, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let mut cfg = config(&kms, KEY);
        cfg.credentials = CredentialSource::Container {
            url: format!("{}/v1/credentials", container.addr()),
            auth_token: None,
            auth_token_file: Some(auth_file),
        };
        let provider = AwsKmsKekProvider::new(cfg).unwrap();
        let wrapped = provider.wrap(&[3u8; 32], b"f").unwrap();
        assert_eq!(
            *provider
                .unwrap(&wrapped.key_id, &wrapped.bytes, b"f")
                .unwrap(),
            [3u8; 32]
        );
        assert_eq!(kms.bad_signatures(), 0);
    }

    #[test]
    fn expired_temporary_credentials_are_refreshed_once() {
        let kms = MockKms::start(&[KEY]);
        let served = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = served.clone();
        let container = MockServer::start(move |_| {
            let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            MockResponse::new(
                200,
                &format!(
                    "{{\"AccessKeyId\":\"{MOCK_ACCESS_KEY}\",\"SecretAccessKey\":\"{MOCK_SECRET_KEY}\",\"Token\":\"session-{n}\",\"Expiration\":\"2099-01-01T00:00:00Z\"}}"
                ),
            )
        });
        let mut cfg = config(&kms, KEY);
        cfg.credentials = CredentialSource::Container {
            url: container.addr(),
            auth_token: None,
            auth_token_file: None,
        };
        let provider = AwsKmsKekProvider::new(cfg).unwrap();
        kms.expect_session_token("session-1");
        provider.wrap(&[1u8; 32], b"f").unwrap();
        // KMS now only accepts the next session: the first call sees
        // ExpiredTokenException, refreshes and succeeds.
        kms.expect_session_token("session-2");
        provider.wrap(&[1u8; 32], b"f").unwrap();
        assert_eq!(served.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[test]
    fn region_and_key_settings_are_validated() {
        let kms = MockKms::start(&[KEY]);
        let mut cfg = config(&kms, KEY);
        cfg.region = "US EAST".to_string();
        assert!(
            AwsKmsKekProvider::new(cfg)
                .unwrap_err()
                .to_string()
                .contains("REGION")
        );
        let cfg = config(&kms, "bad key\"");
        assert!(
            AwsKmsKekProvider::new(cfg)
                .unwrap_err()
                .to_string()
                .contains("KEY_ID")
        );
        let mut cfg = config(&kms, KEY);
        cfg.endpoint = Some("http://kms.example".to_string());
        assert!(
            AwsKmsKekProvider::new(cfg)
                .unwrap_err()
                .to_string()
                .contains("plain http")
        );
        assert_eq!(
            region_from_arn("arn:aws:kms:eu-central-1:111122223333:key/abc"),
            Some("eu-central-1")
        );
        assert_eq!(region_from_arn("alias/dash"), None);
    }
}

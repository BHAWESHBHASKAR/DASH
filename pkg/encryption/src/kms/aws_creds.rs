//! AWS credentials for the KMS provider.
//!
//! Supported sources, tried in this order (the order of the AWS SDKs):
//!
//! 1. Static keys: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional
//!    `AWS_SESSION_TOKEN`.
//! 2. Web identity (EKS IRSA and similar): `AWS_WEB_IDENTITY_TOKEN_FILE` and
//!    `AWS_ROLE_ARN` (optional `AWS_ROLE_SESSION_NAME`), exchanged with STS
//!    `AssumeRoleWithWebIdentity`; the token file is re-read on every refresh.
//!    STS endpoint: `AWS_ENDPOINT_URL_STS` or `https://sts.<region>.amazonaws.com`.
//! 3. Container credentials (ECS task roles, EKS Pod Identity):
//!    `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` or
//!    `AWS_CONTAINER_CREDENTIALS_FULL_URI`, with
//!    `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` / `AWS_CONTAINER_AUTHORIZATION_TOKEN`.
//!
//! Not supported: EC2 instance metadata (IMDS), shared credential and config
//! files (`~/.aws`), SSO and `credential_process`. Temporary credentials are
//! refreshed five minutes before they expire, and at once when KMS reports
//! them expired.

use std::path::PathBuf;
use std::sync::Mutex;

use url::{Host, Url};
use zeroize::Zeroizing;

use super::{
    HttpClient, KmsHttpOptions, Step, read_secret_file, retryable_status, sanitize, transport_step,
    unix_now, with_retries,
};
use crate::EncryptionError;

const REFRESH_BEFORE_EXPIRY_SECS: u64 = 300;
const ECS_CREDENTIALS_HOST: &str = "http://169.254.170.2";

/// A set of AWS credentials (secrets cleared on drop, never printed).
#[derive(Clone)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: Zeroizing<String>,
    pub session_token: Option<Zeroizing<String>>,
    /// Unix seconds; `None` for long-lived keys.
    pub expires_at: Option<u64>,
}

impl std::fmt::Debug for AwsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

/// Where credentials come from.
#[derive(Debug, Clone)]
pub enum CredentialSource {
    Static(AwsCredentials),
    WebIdentity {
        role_arn: String,
        token_file: PathBuf,
        session_name: String,
        sts_endpoint: String,
    },
    Container {
        url: String,
        auth_token: Option<Zeroizing<String>>,
        auth_token_file: Option<PathBuf>,
    },
}

fn nonblank(lookup: &dyn Fn(&str) -> Option<String>, name: &str) -> Option<String> {
    lookup(name)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

impl CredentialSource {
    /// Picks the source from the standard AWS environment variables.
    pub fn from_lookup(
        lookup: &dyn Fn(&str) -> Option<String>,
        region: &str,
    ) -> Result<Self, EncryptionError> {
        let akid = nonblank(lookup, "AWS_ACCESS_KEY_ID");
        let secret = lookup("AWS_SECRET_ACCESS_KEY")
            .map(|v| Zeroizing::new(v.trim().to_string()))
            .filter(|v| !v.is_empty());
        match (akid, secret) {
            (Some(access_key_id), Some(secret_access_key)) => {
                let session_token = lookup("AWS_SESSION_TOKEN")
                    .map(|v| Zeroizing::new(v.trim().to_string()))
                    .filter(|v| !v.is_empty());
                return Ok(Self::Static(AwsCredentials {
                    access_key_id,
                    secret_access_key,
                    session_token,
                    expires_at: None,
                }));
            }
            (Some(_), None) => {
                return Err(EncryptionError::Config(
                    "AWS_ACCESS_KEY_ID is set but AWS_SECRET_ACCESS_KEY is not".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(EncryptionError::Config(
                    "AWS_SECRET_ACCESS_KEY is set but AWS_ACCESS_KEY_ID is not".to_string(),
                ));
            }
            (None, None) => {}
        }
        let token_file = nonblank(lookup, "AWS_WEB_IDENTITY_TOKEN_FILE");
        let role_arn = nonblank(lookup, "AWS_ROLE_ARN");
        match (token_file, role_arn) {
            (Some(token_file), Some(role_arn)) => {
                let sts_endpoint = nonblank(lookup, "AWS_ENDPOINT_URL_STS")
                    .unwrap_or_else(|| format!("https://sts.{region}.amazonaws.com"));
                let session_name = nonblank(lookup, "AWS_ROLE_SESSION_NAME")
                    .unwrap_or_else(|| "dash-encryption".to_string());
                return Ok(Self::WebIdentity {
                    role_arn,
                    token_file: PathBuf::from(token_file),
                    session_name,
                    sts_endpoint,
                });
            }
            (Some(_), None) => {
                return Err(EncryptionError::Config(
                    "AWS_WEB_IDENTITY_TOKEN_FILE is set but AWS_ROLE_ARN is not".to_string(),
                ));
            }
            _ => {}
        }
        let auth_token_file =
            nonblank(lookup, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE").map(PathBuf::from);
        let auth_token = lookup("AWS_CONTAINER_AUTHORIZATION_TOKEN")
            .map(|v| Zeroizing::new(v.trim().to_string()))
            .filter(|v| !v.is_empty());
        if let Some(relative) = nonblank(lookup, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
            let path = if relative.starts_with('/') {
                relative
            } else {
                format!("/{relative}")
            };
            return Ok(Self::Container {
                url: format!("{ECS_CREDENTIALS_HOST}{path}"),
                auth_token,
                auth_token_file,
            });
        }
        if let Some(full) = nonblank(lookup, "AWS_CONTAINER_CREDENTIALS_FULL_URI") {
            check_container_url(&full)?;
            return Ok(Self::Container {
                url: full,
                auth_token,
                auth_token_file,
            });
        }
        Err(EncryptionError::Config(
            "no AWS credentials found: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or run with a web identity (AWS_WEB_IDENTITY_TOKEN_FILE and AWS_ROLE_ARN, as EKS IRSA sets them) or container credentials (AWS_CONTAINER_CREDENTIALS_RELATIVE_URI / _FULL_URI, as ECS and EKS Pod Identity set them); instance metadata and ~/.aws files are not read".to_string(),
        ))
    }

    /// Short description for messages (no secrets).
    pub fn describe(&self) -> &'static str {
        match self {
            Self::Static(_) => "static credentials from AWS_ACCESS_KEY_ID",
            Self::WebIdentity { .. } => "web identity (AWS_WEB_IDENTITY_TOKEN_FILE)",
            Self::Container { .. } => "container credentials endpoint",
        }
    }

    fn refreshable(&self) -> bool {
        !matches!(self, Self::Static(_))
    }
}

/// Plain http is allowed for the full container URI only to the addresses
/// the AWS SDKs allow (loopback, the ECS and EKS Pod Identity link-local
/// endpoints); anything else must be https.
fn check_container_url(raw: &str) -> Result<(), EncryptionError> {
    let url = Url::parse(raw).map_err(|e| {
        EncryptionError::Config(format!(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI: invalid URL: {e}"
        ))
    })?;
    match url.scheme() {
        "https" => Ok(()),
        "http" => {
            let allowed = match url.host() {
                Some(Host::Ipv4(ip)) => {
                    ip.is_loopback()
                        || ip.octets() == [169, 254, 170, 2]
                        || ip.octets() == [169, 254, 170, 23]
                }
                Some(Host::Ipv6(ip)) => {
                    ip.is_loopback() || ip.segments() == [0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x23]
                }
                Some(Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
                None => false,
            };
            if allowed {
                Ok(())
            } else {
                Err(EncryptionError::Config(format!(
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI: plain http is only allowed to loopback or the ECS / EKS Pod Identity link-local addresses, not '{}'",
                    url.host_str().unwrap_or("?")
                )))
            }
        }
        other => Err(EncryptionError::Config(format!(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI: unsupported scheme '{other}'"
        ))),
    }
}

/// Hands out current credentials, refreshing temporary ones.
pub(crate) struct CredentialProvider {
    source: CredentialSource,
    cached: Mutex<Option<AwsCredentials>>,
    http: HttpClient,
    opts: KmsHttpOptions,
}

impl CredentialProvider {
    pub fn new(source: CredentialSource, opts: &KmsHttpOptions) -> Result<Self, EncryptionError> {
        let cached = match &source {
            CredentialSource::Static(c) => Some(c.clone()),
            _ => None,
        };
        Ok(Self {
            source,
            cached: Mutex::new(cached),
            http: HttpClient::new(true, None, opts)?,
            opts: opts.clone(),
        })
    }

    pub fn refreshable(&self) -> bool {
        self.source.refreshable()
    }

    pub fn describe(&self) -> &'static str {
        self.source.describe()
    }

    /// Current credentials; `force` refetches temporary ones.
    pub fn get(&self, force: bool) -> Result<AwsCredentials, EncryptionError> {
        let mut cached = self.cached.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(creds) = cached.as_ref() {
            let fresh = match creds.expires_at {
                None => true,
                Some(at) => unix_now() + REFRESH_BEFORE_EXPIRY_SECS < at,
            };
            if !self.source.refreshable() || (fresh && !force) {
                return Ok(creds.clone());
            }
        }
        let creds = match &self.source {
            CredentialSource::Static(c) => c.clone(),
            CredentialSource::WebIdentity {
                role_arn,
                token_file,
                session_name,
                sts_endpoint,
            } => self.assume_role_with_web_identity(
                role_arn,
                token_file,
                session_name,
                sts_endpoint,
            )?,
            CredentialSource::Container {
                url,
                auth_token,
                auth_token_file,
            } => self.container_credentials(url, auth_token.as_ref(), auth_token_file.as_ref())?,
        };
        *cached = Some(creds.clone());
        Ok(creds)
    }

    fn assume_role_with_web_identity(
        &self,
        role_arn: &str,
        token_file: &std::path::Path,
        session_name: &str,
        sts_endpoint: &str,
    ) -> Result<AwsCredentials, EncryptionError> {
        let token = read_web_identity_token(token_file)?;
        let body = Zeroizing::new(
            url::form_urlencoded::Serializer::new(String::new())
                .append_pair("Action", "AssumeRoleWithWebIdentity")
                .append_pair("Version", "2011-06-15")
                .append_pair("RoleArn", role_arn)
                .append_pair("RoleSessionName", session_name)
                .append_pair("WebIdentityToken", &token)
                .finish(),
        );
        let what = format!("STS AssumeRoleWithWebIdentity for {role_arn} at {sts_endpoint}");
        let secrets = [token.as_str()];
        with_retries(&self.opts, &what, || {
            let reply = match self.http.send(
                "POST",
                sts_endpoint,
                &[
                    ("Content-Type", "application/x-www-form-urlencoded"),
                    ("Accept", "application/xml"),
                ],
                Some(body.as_bytes()),
                self.opts.attempt_timeout,
                &secrets,
            ) {
                Ok(reply) => reply,
                Err(failure) => return transport_step(failure, &self.opts),
            };
            let text = String::from_utf8_lossy(&reply.body);
            if (200..300).contains(&reply.status) {
                return match parse_sts_credentials(&text) {
                    Some(creds) => Step::Done(creds),
                    None => Step::Fail(EncryptionError::Kms(format!(
                        "{what}: unexpected response (no credentials)"
                    ))),
                };
            }
            let code = xml_text(&text, "Code").unwrap_or_default();
            let message = xml_text(&text, "Message").unwrap_or_default();
            let detail = sanitize(format!("{code}: {message}").as_bytes(), &secrets);
            if retryable_status(reply.status)
                || code == "IDPCommunicationError"
                || code == "Throttling"
            {
                Step::Retry {
                    error: format!("HTTP {}: {detail}", reply.status),
                    retry_after: reply.retry_after,
                }
            } else {
                Step::Fail(EncryptionError::Kms(format!(
                    "{what}: HTTP {}: {detail}",
                    reply.status
                )))
            }
        })
    }

    fn container_credentials(
        &self,
        url: &str,
        auth_token: Option<&Zeroizing<String>>,
        auth_token_file: Option<&PathBuf>,
    ) -> Result<AwsCredentials, EncryptionError> {
        let token = match auth_token_file {
            Some(path) => Some(read_secret_file(
                path,
                "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
            )?),
            None => auth_token.cloned(),
        };
        let what = "the container credentials endpoint".to_string();
        let secrets: Vec<&str> = token.iter().map(|t| t.as_str()).collect();
        with_retries(&self.opts, &what, || {
            let mut headers = vec![("Accept", "application/json")];
            if let Some(t) = &token {
                headers.push(("Authorization", t.as_str()));
            }
            let reply = match self.http.send(
                "GET",
                url,
                &headers,
                None,
                self.opts.attempt_timeout,
                &secrets,
            ) {
                Ok(reply) => reply,
                Err(failure) => return transport_step(failure, &self.opts),
            };
            if retryable_status(reply.status) {
                return Step::Retry {
                    error: format!("HTTP {}", reply.status),
                    retry_after: reply.retry_after,
                };
            }
            if !(200..300).contains(&reply.status) {
                return Step::Fail(EncryptionError::Kms(format!(
                    "{what}: HTTP {}: {}",
                    reply.status,
                    sanitize(&reply.body, &secrets)
                )));
            }
            match parse_container_credentials(&reply.body) {
                Some(creds) => Step::Done(creds),
                None => Step::Fail(EncryptionError::Kms(format!(
                    "{what}: unexpected response (no credentials)"
                ))),
            }
        })
    }
}

fn read_web_identity_token(path: &std::path::Path) -> Result<Zeroizing<String>, EncryptionError> {
    // Projected service account tokens are mode 0644 by default, so only the
    // content is checked here.
    let shown = path.display();
    let raw = Zeroizing::new(std::fs::read(path).map_err(|e| {
        EncryptionError::Config(format!("AWS_WEB_IDENTITY_TOKEN_FILE {shown}: {e}"))
    })?);
    let text = std::str::from_utf8(&raw)
        .map(str::trim)
        .ok()
        .filter(|t| {
            !t.is_empty() && t.len() <= 64 * 1024 && t.bytes().all(|b| b.is_ascii_graphic())
        })
        .ok_or_else(|| {
            EncryptionError::Config(format!(
                "AWS_WEB_IDENTITY_TOKEN_FILE {shown}: does not hold a token"
            ))
        })?;
    Ok(Zeroizing::new(text.to_string()))
}

fn xml_unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Text of the first `<tag>...</tag>` in `xml`.
fn xml_text(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = start + xml[start..].find(&close)?;
    Some(xml_unescape(xml[start..end].trim()))
}

fn parse_sts_credentials(xml: &str) -> Option<AwsCredentials> {
    let block = {
        let start = xml.find("<Credentials>")?;
        let end = start + xml[start..].find("</Credentials>")?;
        &xml[start..end]
    };
    Some(AwsCredentials {
        access_key_id: xml_text(block, "AccessKeyId")?,
        secret_access_key: Zeroizing::new(xml_text(block, "SecretAccessKey")?),
        session_token: Some(Zeroizing::new(xml_text(block, "SessionToken")?)),
        expires_at: xml_text(block, "Expiration").and_then(|t| super::parse_rfc3339_utc(&t)),
    })
}

fn parse_container_credentials(body: &[u8]) -> Option<AwsCredentials> {
    let mut value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let get =
        |v: &serde_json::Value, k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_string);
    let creds = AwsCredentials {
        access_key_id: get(&value, "AccessKeyId")?,
        secret_access_key: Zeroizing::new(get(&value, "SecretAccessKey")?),
        session_token: get(&value, "Token").map(Zeroizing::new),
        expires_at: get(&value, "Expiration").and_then(|t| super::parse_rfc3339_utc(&t)),
    };
    for key in ["SecretAccessKey", "Token"] {
        if let Some(serde_json::Value::String(s)) = value.get_mut(key) {
            zeroize::Zeroize::zeroize(s);
        }
    }
    Some(creds)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn lookup_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| map.get(k).cloned()
    }

    #[test]
    fn sources_are_picked_in_sdk_order() {
        let both = lookup_from(&[
            ("AWS_ACCESS_KEY_ID", "AKID"),
            ("AWS_SECRET_ACCESS_KEY", "secret"),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", "/t"),
            ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
        ]);
        assert!(matches!(
            CredentialSource::from_lookup(&both, "us-east-1").unwrap(),
            CredentialSource::Static(_)
        ));
        let web = lookup_from(&[
            ("AWS_WEB_IDENTITY_TOKEN_FILE", "/t"),
            ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r"),
        ]);
        match CredentialSource::from_lookup(&web, "eu-west-1").unwrap() {
            CredentialSource::WebIdentity {
                sts_endpoint,
                session_name,
                ..
            } => {
                assert_eq!(sts_endpoint, "https://sts.eu-west-1.amazonaws.com");
                assert_eq!(session_name, "dash-encryption");
            }
            other => panic!("{other:?}"),
        }
        let ecs = lookup_from(&[("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/creds/abc")]);
        match CredentialSource::from_lookup(&ecs, "us-east-1").unwrap() {
            CredentialSource::Container { url, .. } => {
                assert_eq!(url, "http://169.254.170.2/v2/creds/abc")
            }
            other => panic!("{other:?}"),
        }
        let pod_identity = lookup_from(&[(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://169.254.170.23/v1/credentials",
        )]);
        assert!(CredentialSource::from_lookup(&pod_identity, "us-east-1").is_ok());
        let evil = lookup_from(&[(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://attacker.example/creds",
        )]);
        assert!(CredentialSource::from_lookup(&evil, "us-east-1").is_err());
        let half = lookup_from(&[("AWS_ACCESS_KEY_ID", "AKID")]);
        assert!(CredentialSource::from_lookup(&half, "us-east-1").is_err());
        let none = lookup_from(&[]);
        let err = CredentialSource::from_lookup(&none, "us-east-1")
            .unwrap_err()
            .to_string();
        assert!(err.contains("no AWS credentials"), "{err}");
    }

    #[test]
    fn credentials_never_print_secrets() {
        let creds = AwsCredentials {
            access_key_id: "AKID".to_string(),
            secret_access_key: Zeroizing::new("very-secret".to_string()),
            session_token: Some(Zeroizing::new("session-secret".to_string())),
            expires_at: None,
        };
        let shown = format!("{:?}", CredentialSource::Static(creds));
        assert!(
            !shown.contains("very-secret") && !shown.contains("session-secret"),
            "{shown}"
        );
    }

    #[test]
    fn sts_and_container_answers_parse() {
        let xml = r#"<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <SessionToken>FwoGZX&amp;token</SessionToken>
      <SecretAccessKey>wJalr/secret+key</SecretAccessKey>
      <Expiration>2030-01-02T03:04:05Z</Expiration>
      <AccessKeyId>ASIAEXAMPLE</AccessKeyId>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"#;
        let creds = parse_sts_credentials(xml).unwrap();
        assert_eq!(creds.access_key_id, "ASIAEXAMPLE");
        assert_eq!(creds.secret_access_key.as_str(), "wJalr/secret+key");
        assert_eq!(creds.session_token.unwrap().as_str(), "FwoGZX&token");
        assert_eq!(
            creds.expires_at,
            super::super::parse_rfc3339_utc("2030-01-02T03:04:05Z")
        );

        let json = br#"{"AccessKeyId":"ASIA2","SecretAccessKey":"s2","Token":"t2","Expiration":"2030-01-02T03:04:05Z"}"#;
        let creds = parse_container_credentials(json).unwrap();
        assert_eq!(creds.access_key_id, "ASIA2");
        assert_eq!(creds.session_token.unwrap().as_str(), "t2");
        assert!(parse_container_credentials(b"{}").is_none());
    }
}

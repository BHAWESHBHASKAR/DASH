//! Listener TLS settings, resolved from the environment per service.
//!
//! Each service has four variables (`<P>` is `INGEST`, `RETRIEVAL` or
//! `CONTROL_PLANE`):
//!
//! * `DASH_<P>_TLS_CERT_FILE` / `DASH_<P>_TLS_KEY_FILE`: PEM certificate
//!   chain and private key. Setting both turns the listener into HTTPS;
//!   setting only one is a startup error;
//! * `DASH_<P>_TLS_CLIENT_CA_FILE`: PEM bundle client certificates must
//!   chain to (mutual TLS). Clients without a certificate are still
//!   accepted unless the next variable is on;
//! * `DASH_<P>_TLS_REQUIRE_CLIENT_CERT`: refuse clients that present no
//!   certificate (needs the client CA).
//!
//! The files are re-read when their content changes (checked at most once
//! per second), so certificate rotation needs no restart. See
//! `docs/operations/tls.md`.

use std::path::PathBuf;

use dash_http::{TlsAcceptor, TlsSettings};

/// Names of one service's listener TLS variables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListenerTlsEnv {
    pub service: &'static str,
    pub cert_file: &'static str,
    pub key_file: &'static str,
    pub client_ca_file: &'static str,
    pub require_client_cert: &'static str,
}

pub const INGEST_TLS_ENV: ListenerTlsEnv = ListenerTlsEnv {
    service: "ingestion",
    cert_file: "DASH_INGEST_TLS_CERT_FILE",
    key_file: "DASH_INGEST_TLS_KEY_FILE",
    client_ca_file: "DASH_INGEST_TLS_CLIENT_CA_FILE",
    require_client_cert: "DASH_INGEST_TLS_REQUIRE_CLIENT_CERT",
};

pub const RETRIEVAL_TLS_ENV: ListenerTlsEnv = ListenerTlsEnv {
    service: "retrieval",
    cert_file: "DASH_RETRIEVAL_TLS_CERT_FILE",
    key_file: "DASH_RETRIEVAL_TLS_KEY_FILE",
    client_ca_file: "DASH_RETRIEVAL_TLS_CLIENT_CA_FILE",
    require_client_cert: "DASH_RETRIEVAL_TLS_REQUIRE_CLIENT_CERT",
};

pub const CONTROL_PLANE_TLS_ENV: ListenerTlsEnv = ListenerTlsEnv {
    service: "control-plane",
    cert_file: "DASH_CONTROL_PLANE_TLS_CERT_FILE",
    key_file: "DASH_CONTROL_PLANE_TLS_KEY_FILE",
    client_ca_file: "DASH_CONTROL_PLANE_TLS_CLIENT_CA_FILE",
    require_client_cert: "DASH_CONTROL_PLANE_TLS_REQUIRE_CLIENT_CERT",
};

/// Raw values of one service's TLS variables (blank counts as unset).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RawListenerTls {
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub client_ca_file: Option<String>,
    pub require_client_cert: Option<String>,
}

impl RawListenerTls {
    pub fn from_env(env: &ListenerTlsEnv) -> Self {
        let get = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        Self {
            cert_file: get(env.cert_file),
            key_file: get(env.key_file),
            client_ca_file: get(env.client_ca_file),
            require_client_cert: get(env.require_client_cert),
        }
    }
}

fn parse_flag(name: &str, raw: Option<&str>) -> Result<bool, String> {
    match raw.map(|value| value.to_ascii_lowercase()) {
        None => Ok(false),
        Some(value) => match value.as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(format!("{name} must be a boolean (1/0, true/false)")),
        },
    }
}

/// Validate the variables and turn them into settings. `Ok(None)`: TLS off.
pub fn listener_tls_settings(
    env: &ListenerTlsEnv,
    raw: &RawListenerTls,
) -> Result<Option<TlsSettings>, String> {
    let require = parse_flag(env.require_client_cert, raw.require_client_cert.as_deref())?;
    let (cert, key) = match (raw.cert_file.as_deref(), raw.key_file.as_deref()) {
        (None, None) => {
            if raw.client_ca_file.is_some() || require {
                return Err(format!(
                    "{} / {} require {} and {} (TLS is off without them)",
                    env.client_ca_file, env.require_client_cert, env.cert_file, env.key_file
                ));
            }
            return Ok(None);
        }
        (Some(cert), Some(key)) => (cert, key),
        (Some(_), None) => {
            return Err(format!(
                "{} is set but {} is not",
                env.cert_file, env.key_file
            ));
        }
        (None, Some(_)) => {
            return Err(format!(
                "{} is set but {} is not",
                env.key_file, env.cert_file
            ));
        }
    };
    if require && raw.client_ca_file.is_none() {
        return Err(format!(
            "{} needs {} (the CA client certificates must chain to)",
            env.require_client_cert, env.client_ca_file
        ));
    }
    Ok(Some(TlsSettings {
        cert_file: PathBuf::from(cert),
        key_file: PathBuf::from(key),
        client_ca_file: raw.client_ca_file.as_deref().map(PathBuf::from),
        require_client_cert: require,
    }))
}

/// Resolve and load the listener TLS configuration for a service. An error
/// means the service must not start; it names variables and files, never
/// key material.
pub fn listener_tls_from_env(env: &ListenerTlsEnv) -> Result<Option<TlsAcceptor>, String> {
    let Some(settings) = listener_tls_settings(env, &RawListenerTls::from_env(env))? else {
        return Ok(None);
    };
    let acceptor = TlsAcceptor::new(settings.clone())
        .map_err(|err| format!("{} TLS configuration rejected: {err}", env.service))?;
    tracing::info!(
        "{} listener serves HTTPS (certificate '{}', client certificates: {})",
        env.service,
        settings.cert_file.display(),
        match (&settings.client_ca_file, settings.require_client_cert) {
            (None, _) => "not requested",
            (Some(_), false) => "verified when presented",
            (Some(_), true) => "required",
        }
    );
    Ok(Some(acceptor))
}

/// Startup check: the variables are consistent and the files load. Does
/// not log; [`listener_tls_from_env`] loads again when the server starts.
pub fn check_listener_tls(env: &ListenerTlsEnv) -> Result<(), String> {
    if let Some(settings) = listener_tls_settings(env, &RawListenerTls::from_env(env))? {
        TlsAcceptor::new(settings)
            .map_err(|err| format!("{} TLS configuration rejected: {err}", env.service))?;
    }
    Ok(())
}

/// True when the service's listener is configured for TLS (both files set).
pub fn listener_tls_enabled(env: &ListenerTlsEnv) -> bool {
    let raw = RawListenerTls::from_env(env);
    raw.cert_file.is_some() && raw.key_file.is_some()
}

/// Header carrying the verified client-certificate fingerprint from the
/// transport adapter to the handlers. Any copy a client sends is removed
/// first, so it cannot be forged.
pub const CLIENT_CERT_HEADER: &str = "x-dash-verified-client-cert-sha256";

/// Replace any client-sent [`CLIENT_CERT_HEADER`] with the fingerprint the
/// TLS layer verified (if any).
pub fn stamp_client_cert_header(
    headers: &mut std::collections::HashMap<String, String>,
    tls: Option<&dash_http::TlsInfo>,
) {
    headers.remove(CLIENT_CERT_HEADER);
    if let Some(fingerprint) = tls.and_then(|info| info.client_cert_sha256.as_ref()) {
        headers.insert(CLIENT_CERT_HEADER.to_string(), fingerprint.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(
        cert: Option<&str>,
        key: Option<&str>,
        ca: Option<&str>,
        req: Option<&str>,
    ) -> RawListenerTls {
        RawListenerTls {
            cert_file: cert.map(str::to_string),
            key_file: key.map(str::to_string),
            client_ca_file: ca.map(str::to_string),
            require_client_cert: req.map(str::to_string),
        }
    }

    #[test]
    fn unset_means_plain_http() {
        assert_eq!(
            listener_tls_settings(&INGEST_TLS_ENV, &RawListenerTls::default()),
            Ok(None)
        );
        assert_eq!(
            listener_tls_settings(&INGEST_TLS_ENV, &raw(None, None, None, Some("0"))),
            Ok(None)
        );
    }

    #[test]
    fn cert_and_key_must_come_together() {
        let err =
            listener_tls_settings(&INGEST_TLS_ENV, &raw(Some("c"), None, None, None)).unwrap_err();
        assert!(err.contains("DASH_INGEST_TLS_KEY_FILE"), "{err}");
        let err = listener_tls_settings(&RETRIEVAL_TLS_ENV, &raw(None, Some("k"), None, None))
            .unwrap_err();
        assert!(err.contains("DASH_RETRIEVAL_TLS_CERT_FILE"), "{err}");
    }

    #[test]
    fn client_ca_and_require_need_a_certificate() {
        let err = listener_tls_settings(&CONTROL_PLANE_TLS_ENV, &raw(None, None, Some("ca"), None))
            .unwrap_err();
        assert!(err.contains("DASH_CONTROL_PLANE_TLS_CERT_FILE"), "{err}");
        let err =
            listener_tls_settings(&INGEST_TLS_ENV, &raw(Some("c"), Some("k"), None, Some("1")))
                .unwrap_err();
        assert!(err.contains("DASH_INGEST_TLS_CLIENT_CA_FILE"), "{err}");
        let err = listener_tls_settings(
            &INGEST_TLS_ENV,
            &raw(Some("c"), Some("k"), Some("ca"), Some("maybe")),
        )
        .unwrap_err();
        assert!(err.contains("boolean"), "{err}");
    }

    #[test]
    fn full_settings_resolve() {
        let settings = listener_tls_settings(
            &INGEST_TLS_ENV,
            &raw(
                Some("/c.pem"),
                Some("/k.pem"),
                Some("/ca.pem"),
                Some("true"),
            ),
        )
        .unwrap()
        .unwrap();
        assert_eq!(settings.cert_file, PathBuf::from("/c.pem"));
        assert_eq!(settings.client_ca_file, Some(PathBuf::from("/ca.pem")));
        assert!(settings.require_client_cert);
    }

    #[test]
    fn client_cert_header_cannot_be_forged() {
        let mut headers = std::collections::HashMap::from([(
            CLIENT_CERT_HEADER.to_string(),
            "forged".to_string(),
        )]);
        stamp_client_cert_header(&mut headers, None);
        assert!(!headers.contains_key(CLIENT_CERT_HEADER));
        let info = dash_http::TlsInfo {
            client_cert_sha256: Some("ab".repeat(32)),
        };
        headers.insert(CLIENT_CERT_HEADER.to_string(), "forged".to_string());
        stamp_client_cert_header(&mut headers, Some(&info));
        assert_eq!(headers[CLIENT_CERT_HEADER], "ab".repeat(32));
    }
}

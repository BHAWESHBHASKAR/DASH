use std::{collections::HashMap, time::Duration};

use super::json::json_escape;

pub(super) const SOCKET_TIMEOUT_SECS: u64 = 5;
/// Workers reserved for health-class requests (`/health`, `/live`, ...).
const HEALTH_WORKERS: usize = 2;
const HEALTH_QUEUE_CAPACITY: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpRequest {
    pub(crate) method: String,
    pub(crate) target: String,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) content_type: &'static str,
    pub(crate) body: String,
    /// Emitted as a `Retry-After` header (429 responses).
    pub(crate) retry_after_secs: Option<u64>,
}

impl HttpResponse {
    pub(crate) fn ok_json(body: String) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body,
            retry_after_secs: None,
        }
    }

    pub(crate) fn ok_text(body: String) -> Self {
        Self {
            status: 200,
            content_type: "text/plain; version=0.0.4; charset=utf-8",
            body,
            retry_after_secs: None,
        }
    }

    pub(crate) fn ok_plain(body: String) -> Self {
        Self {
            status: 200,
            content_type: "text/plain; charset=utf-8",
            body,
            retry_after_secs: None,
        }
    }

    pub(crate) fn bad_request(message: &str) -> Self {
        Self {
            status: 400,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    pub(crate) fn not_found(message: &str) -> Self {
        Self {
            status: 404,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    pub(crate) fn forbidden(message: &str) -> Self {
        Self {
            status: 403,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    pub(crate) fn conflict(message: &str) -> Self {
        Self {
            status: 409,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    pub(crate) fn method_not_allowed(message: &str) -> Self {
        Self {
            status: 405,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    pub(crate) fn unauthorized(message: &str) -> Self {
        Self {
            status: 401,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    pub(crate) fn internal_server_error(message: &str) -> Self {
        Self {
            status: 500,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    pub(crate) fn service_unavailable(message: &str) -> Self {
        Self {
            status: 503,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }

    pub(crate) fn too_many_requests(message: &str, retry_after_secs: u64) -> Self {
        Self {
            status: 429,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: Some(retry_after_secs),
        }
    }

    pub(crate) fn error_with_status(status: u16, message: &str) -> Self {
        if status == 409 {
            return Self::conflict(message);
        }
        if status == 429 {
            return Self::too_many_requests(message, 1);
        }
        Self {
            status,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
            retry_after_secs: None,
        }
    }
}

impl From<dash_http::Request> for HttpRequest {
    fn from(request: dash_http::Request) -> Self {
        let mut headers = request.headers;
        // Only the TLS layer may vouch for a client certificate.
        dash_common::tls::stamp_client_cert_header(&mut headers, request.tls.as_ref());
        Self {
            method: request.method,
            target: request.target,
            headers,
            body: request.body,
        }
    }
}

impl From<HttpResponse> for dash_http::Response {
    fn from(response: HttpResponse) -> Self {
        let out = dash_http::Response::new(response.status, response.content_type, response.body);
        match response.retry_after_secs {
            Some(secs) => out.with_header("Retry-After", secs.to_string()),
            None => out,
        }
    }
}

/// Server settings for ingestion: the shared defaults plus the
/// `DASH_HTTP_*` environment overrides.
pub(super) fn server_config(worker_count: usize, queue_capacity: usize) -> dash_http::ServerConfig {
    let env = dash_common::conn::ConnConfig::from_env();
    let mut config = dash_http::ServerConfig::new("ingestion", worker_count, queue_capacity);
    config.health_workers = HEALTH_WORKERS;
    config.health_queue_capacity = HEALTH_QUEUE_CAPACITY;
    config.write_timeout = Duration::from_secs(SOCKET_TIMEOUT_SECS);
    config.reject_write_timeout = Duration::from_secs(SOCKET_TIMEOUT_SECS);
    config.request_deadline = env.request_timeout;
    config.first_byte_timeout = env.first_byte_timeout;
    config.max_conns_per_ip = env.max_per_ip;
    config
}

pub(super) fn render_response_text(response: &HttpResponse) -> String {
    dash_http::render_response(&response.clone().into())
}

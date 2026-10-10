//! Thin adapters between the retrieval handlers and the shared `dash-http`
//! server: the request/response types the handlers use convert to and from
//! the shared ones, and the retrieval-specific limits are resolved here.

use std::time::Duration;

pub(super) use dash_http::{query_encoding_is_invalid, split_target};

use super::{HttpRequest, HttpResponse};

pub(super) const SOCKET_TIMEOUT_SECS: u64 = 5;
/// Workers reserved for health-class requests (`/health`, `/live`, ...).
pub(super) const HEALTH_WORKERS: usize = 2;
pub(super) const HEALTH_QUEUE_CAPACITY: usize = 64;

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

/// Server settings for retrieval: the shared defaults plus the
/// `DASH_HTTP_*` environment overrides.
pub(super) fn server_config(worker_count: usize, queue_capacity: usize) -> dash_http::ServerConfig {
    let env = dash_common::conn::ConnConfig::from_env();
    let mut config = dash_http::ServerConfig::new("retrieval", worker_count, queue_capacity);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_with_status_preserves_status_code() {
        for code in [408u16, 411, 413, 429, 431, 501, 502] {
            assert_eq!(HttpResponse::error_with_status(code, "x").status, code);
        }
    }

    #[test]
    fn retry_after_becomes_a_header() {
        let text = render_response_text(&HttpResponse::too_many_requests("slow down", 7));
        assert!(
            text.starts_with("HTTP/1.1 429 Too Many Requests\r\n"),
            "{text}"
        );
        assert!(text.contains("\r\nRetry-After: 7\r\n"), "{text}");
    }
}

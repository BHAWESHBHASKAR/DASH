use std::{collections::HashMap, io::Write, net::TcpStream, time::Duration};

use super::json::json_escape;

const BACKPRESSURE_QUEUE_FULL_MESSAGE: &str = "service unavailable: ingestion worker queue full";

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
}

impl HttpResponse {
    pub(crate) fn ok_json(body: String) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body,
        }
    }

    pub(crate) fn ok_text(body: String) -> Self {
        Self {
            status: 200,
            content_type: "text/plain; version=0.0.4; charset=utf-8",
            body,
        }
    }

    pub(crate) fn ok_plain(body: String) -> Self {
        Self {
            status: 200,
            content_type: "text/plain; charset=utf-8",
            body,
        }
    }

    pub(crate) fn bad_request(message: &str) -> Self {
        Self {
            status: 400,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    pub(crate) fn not_found(message: &str) -> Self {
        Self {
            status: 404,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    pub(crate) fn forbidden(message: &str) -> Self {
        Self {
            status: 403,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    pub(crate) fn conflict(message: &str) -> Self {
        Self {
            status: 409,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    pub(crate) fn method_not_allowed(message: &str) -> Self {
        Self {
            status: 405,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    pub(crate) fn unauthorized(message: &str) -> Self {
        Self {
            status: 401,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    pub(crate) fn internal_server_error(message: &str) -> Self {
        Self {
            status: 500,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    pub(crate) fn service_unavailable(message: &str) -> Self {
        Self {
            status: 503,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(message)),
        }
    }

    pub(crate) fn error_with_status(status: u16, message: &str) -> Self {
        match status {
            400 => Self::bad_request(message),
            401 => Self::unauthorized(message),
            403 => Self::forbidden(message),
            409 => Self::conflict(message),
            404 => Self::not_found(message),
            405 => Self::method_not_allowed(message),
            503 => Self::service_unavailable(message),
            _ => Self::internal_server_error(message),
        }
    }
}

pub(crate) fn backpressure_rejection_response() -> HttpResponse {
    HttpResponse::service_unavailable(BACKPRESSURE_QUEUE_FULL_MESSAGE)
}

pub(crate) fn write_backpressure_response(
    mut stream: TcpStream,
    socket_timeout_secs: u64,
) -> std::io::Result<()> {
    stream.set_write_timeout(Some(Duration::from_secs(socket_timeout_secs)))?;
    let response = backpressure_rejection_response();
    let response = format!(
        "HTTP/1.1 503 Service Unavailable\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response.content_type,
        response.body.len(),
        response.body
    );
    stream.write_all(response.as_bytes())
}

pub(crate) fn write_response(
    stream: &mut TcpStream,
    response: HttpResponse,
) -> std::io::Result<()> {
    stream.write_all(render_response_text(&response).as_bytes())?;
    stream.flush()
}

pub(crate) fn render_response_text(response: &HttpResponse) -> String {
    let status_text = match response.status {
        200 => "200 OK",
        400 => "400 Bad Request",
        401 => "401 Unauthorized",
        403 => "403 Forbidden",
        409 => "409 Conflict",
        404 => "404 Not Found",
        405 => "405 Method Not Allowed",
        503 => "503 Service Unavailable",
        500 => "500 Internal Server Error",
        _ => "500 Internal Server Error",
    };
    let body_len = response.body.len();
    format!(
        "HTTP/1.1 {status_text}\r\nContent-Type: {}\r\nContent-Length: {body_len}\r\nConnection: close\r\n\r\n{}",
        response.content_type, response.body
    )
}

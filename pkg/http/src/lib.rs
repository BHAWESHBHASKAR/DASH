//! Shared HTTP/1.1 server for the DASH services.
//!
//! One strict request parser, one response renderer and one std-thread server
//! (bounded accept queue, worker pool, reserved health lane, per-IP cap,
//! first-byte and whole-request deadlines, panic isolation). Services keep
//! their routing and handlers and pass them to [`serve`] as a closure.
//!
//! The public surface is deliberately transport-agnostic: a handler maps a
//! [`Request`] to a [`Response`] and everything else is owned here, so the
//! server module can later be swapped for a hyper/axum implementation without
//! touching service code.

mod config;
mod conn;
mod parse;
mod request;
mod response;
mod server;

pub use config::{ExpectPolicy, ServerConfig};
pub use conn::{
    Conn, ConnFrontend, Lane, Rejected, default_health_classifier, is_health_class_path,
    linger_close,
};
pub use parse::{ReadError, parse_request_bytes, read_request};
pub use request::{Request, percent_decode, query_encoding_is_invalid, split_target};
pub use response::{Response, json_escape, render_response, status_line};
pub use server::{
    Acceptor, Handler, HealthClassifier, NeverShutdown, NoHooks, RejectReason, ServerHooks,
    Shutdown, serve, serve_once,
};

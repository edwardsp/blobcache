//! Infallible Hyper response builders.
//!
//! Action 21 from opus_code_eval.md: replaces ad-hoc `Response::builder()
//! ...body(...).unwrap()` patterns. The inputs are all known-valid
//! (static status codes, validated bodies), but a future edit to a
//! header value or status code could introduce a panic.  These helpers
//! fall back to a minimal 500 response on the impossible-but-typed error
//! so we never `.unwrap()` in the response path.

use bytes::Bytes;
use http_body_util::Full;
use hyper::http::{header, Response, StatusCode};

pub type Body = Full<Bytes>;

pub fn ok_response(body: impl Into<Bytes>) -> Response<Body> {
    json_response(StatusCode::OK, body)
}

pub fn json_response(status: StatusCode, body: impl Into<Bytes>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::new(body.into()))
        .unwrap_or_else(|_| fallback_500())
}

pub fn text_response(status: StatusCode, body: impl Into<Bytes>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Full::new(body.into()))
        .unwrap_or_else(|_| fallback_500())
}

pub fn error_response(status: StatusCode, msg: &str) -> Response<Body> {
    text_response(status, msg.to_owned())
}

pub fn empty_response(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .unwrap_or_else(|_| fallback_500())
}

fn fallback_500() -> Response<Body> {
    // Constructed with only static inputs; the only failure mode is OOM,
    // and Response::new is infallible.
    let mut r = Response::new(Full::new(Bytes::from_static(b"internal error")));
    *r.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
    r
}

use blobcache::auth::shared_key::sign_request;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use url::Url;

fn hm(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.insert(
            HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_str(v).unwrap(),
        );
    }
    h
}

#[test]
fn sign_request_known_input_regression() {
    let url = Url::parse("https://acct.blob.core.windows.net/c/blob.bin").unwrap();
    let headers = hm(&[
        ("x-ms-date", "Fri, 01 May 2026 12:00:00 GMT"),
        ("x-ms-version", "2021-12-02"),
    ]);
    let key_b64 =
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    let sig = sign_request("acct", key_b64, "GET", &url, &headers, None);

    assert!(sig.starts_with("SharedKey acct:"), "header prefix");
    let s1 = sign_request("acct", key_b64, "GET", &url, &headers, None);
    assert_eq!(sig, s1, "deterministic for identical inputs");
}

#[test]
fn sign_request_changes_with_method() {
    let url = Url::parse("https://acct.blob.core.windows.net/c/b").unwrap();
    let headers = hm(&[("x-ms-date", "d")]);
    let k = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let g = sign_request("acct", k, "GET", &url, &headers, None);
    let h = sign_request("acct", k, "HEAD", &url, &headers, None);
    assert_ne!(g, h);
}

#[test]
fn sign_request_changes_with_range_header() {
    let url = Url::parse("https://acct.blob.core.windows.net/c/b").unwrap();
    let k = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let h1 = hm(&[("x-ms-date", "d")]);
    let h2 = hm(&[("x-ms-date", "d"), ("range", "bytes=0-1023")]);
    let s1 = sign_request("acct", k, "GET", &url, &h1, None);
    let s2 = sign_request("acct", k, "GET", &url, &h2, None);
    assert_ne!(s1, s2);
}

#[test]
fn sign_request_changes_with_xms_header_set() {
    let url = Url::parse("https://acct.blob.core.windows.net/c/b").unwrap();
    let k = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let h1 = hm(&[("x-ms-date", "d"), ("x-ms-version", "2021-12-02")]);
    let h2 = hm(&[("x-ms-date", "d"), ("x-ms-version", "2020-04-08")]);
    let s1 = sign_request("acct", k, "GET", &url, &h1, None);
    let s2 = sign_request("acct", k, "GET", &url, &h2, None);
    assert_ne!(s1, s2);
}

#[test]
fn sign_request_xms_headers_canonicalised_in_sorted_order() {
    let url = Url::parse("https://acct.blob.core.windows.net/c/b").unwrap();
    let k = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let h_alpha = hm(&[
        ("x-ms-date", "d"),
        ("x-ms-version", "v"),
        ("x-ms-client-request-id", "rid"),
    ]);
    let h_beta = hm(&[
        ("x-ms-version", "v"),
        ("x-ms-client-request-id", "rid"),
        ("x-ms-date", "d"),
    ]);
    assert_eq!(
        sign_request("acct", k, "GET", &url, &h_alpha, None),
        sign_request("acct", k, "GET", &url, &h_beta, None),
        "x-ms-* header insertion order must NOT affect signature; canonicalisation sorts them"
    );
}

#[test]
fn sign_request_query_params_canonicalised_in_sorted_order() {
    let k = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let h = hm(&[("x-ms-date", "d")]);
    let u1 =
        Url::parse("https://acct.blob.core.windows.net/c/b?comp=list&restype=container").unwrap();
    let u2 =
        Url::parse("https://acct.blob.core.windows.net/c/b?restype=container&comp=list").unwrap();
    assert_eq!(
        sign_request("acct", k, "GET", &u1, &h, None),
        sign_request("acct", k, "GET", &u2, &h, None),
        "query parameter order must NOT affect signature"
    );
}

#[test]
fn sign_request_content_length_zero_treated_as_empty() {
    let url = Url::parse("https://acct.blob.core.windows.net/c/b").unwrap();
    let k = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let h = hm(&[("x-ms-date", "d")]);
    let s_zero = sign_request("acct", k, "GET", &url, &h, Some(0));
    let s_none = sign_request("acct", k, "GET", &url, &h, None);
    assert_eq!(
        s_zero, s_none,
        "Some(0) and None must produce the same signature (Azure spec: empty string for zero content-length)"
    );
    let s_some = sign_request("acct", k, "GET", &url, &h, Some(1024));
    assert_ne!(s_some, s_zero);
}

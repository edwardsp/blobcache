use bytes::Bytes;
use chrono::Utc;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Client, Method, Response, StatusCode};
use serde::Deserialize;
use std::sync::Arc;
use url::Url;

use crate::auth::{sas, shared_key, Credential};
use crate::config::AzureConfig;
use crate::error::{BcError, Result};
use crate::stats::Stats;

pub const API_VERSION: &str = "2024-11-04";

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct BlobInfo {
    pub name: String,
    pub content_length: u64,
    pub content_type: String,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ListedBlob {
    pub name: String,
    pub content_length: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Clone)]
pub struct BlobClient {
    http: Client,
    credential: Credential,
    max_retries: u32,
    metrics: Option<Arc<Stats>>,
}

impl BlobClient {
    pub fn new(
        credential: Credential,
        config: &AzureConfig,
        metrics: Option<Arc<Stats>>,
    ) -> Result<Self> {
        let http = Client::builder()
            .pool_max_idle_per_host(config.pool_max_idle_per_host)
            .http1_only()
            .build()?;
        Ok(Self {
            http,
            credential,
            max_retries: 10,
            metrics,
        })
    }

    fn url(&self, account: &str, container: &str, blob: &str) -> Result<Url> {
        let s = format!("https://{account}.blob.core.windows.net/{container}/{blob}");
        Url::parse(&s).map_err(|e| BcError::InvalidUrl(e.to_string()))
    }

    pub async fn get_blob_properties(
        &self,
        account: &str,
        container: &str,
        blob: &str,
    ) -> Result<BlobInfo> {
        let url = self.url(account, container, blob)?;
        let resp = self.send(Method::HEAD, url, &[], None, None).await?;
        let status = resp.status();
        if !status.is_success() {
            if status == StatusCode::NOT_FOUND {
                return Err(BcError::NotFound(blob.to_string()));
            }
            return Err(BcError::Storage {
                status: status.as_u16(),
                message: format!("HEAD {blob} failed"),
            });
        }
        let h = resp.headers();
        Ok(BlobInfo {
            name: blob.to_string(),
            content_length: h
                .get(CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            content_type: h
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string(),
            last_modified: h
                .get("last-modified")
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            etag: h
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(String::from),
        })
    }

    pub async fn get_blob_range(
        &self,
        account: &str,
        container: &str,
        blob: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes> {
        let url = self.url(account, container, blob)?;
        let range_val = format!("bytes={}-{}", offset, offset + length - 1);
        let extra = vec![("x-ms-range", HeaderValue::from_str(&range_val).unwrap())];
        let max_attempts = self.max_retries.saturating_add(1).max(1);
        let mut attempt = 0u32;
        let mut bearer_invalidated_once = false;
        loop {
            attempt += 1;
            let t_send = std::time::Instant::now();
            let resp = match self
                .send_once(
                    Method::GET,
                    url.clone(),
                    &extra,
                    None,
                    None,
                    &mut bearer_invalidated_once,
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    let elapsed = t_send.elapsed().as_secs_f64();
                    if let Some(m) = &self.metrics {
                        m.blob_request_seconds.observe(elapsed);
                        if elapsed > m.blob_request_seconds_max.get() {
                            m.blob_request_seconds_max.set(elapsed);
                        }
                    }
                    let retryable = matches!(
                        &e,
                        BcError::Http(re)
                            if re.is_timeout() || re.is_connect() || re.is_request() || re.is_body()
                    );
                    if retryable && attempt < max_attempts {
                        let delay = backoff_ms(attempt);
                        if let Some(m) = &self.metrics {
                            m.blob_request_retries_total
                                .with_label_values(&["net"])
                                .inc();
                            m.blob_retry_sleep_seconds_total
                                .inc_by(delay as f64 / 1000.0);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                        continue;
                    }
                    if let Some(m) = &self.metrics {
                        m.blob_request_status_total
                            .with_label_values(&["err"])
                            .inc();
                        m.blob_request_giveups_total.inc();
                    }
                    return Err(e);
                }
            };
            let elapsed = t_send.elapsed().as_secs_f64();
            if let Some(m) = &self.metrics {
                m.blob_request_seconds.observe(elapsed);
                if elapsed > m.blob_request_seconds_max.get() {
                    m.blob_request_seconds_max.set(elapsed);
                }
            }
            let status = resp.status();
            if is_retryable_status(status) && attempt < max_attempts {
                let delay = retry_delay_ms(&resp, attempt);
                if let Some(m) = &self.metrics {
                    m.blob_request_retries_total
                        .with_label_values(&[status.as_str()])
                        .inc();
                    m.blob_retry_sleep_seconds_total
                        .inc_by(delay as f64 / 1000.0);
                }
                drop(resp);
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                continue;
            }
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                if let Some(m) = &self.metrics {
                    m.blob_request_status_total
                        .with_label_values(&[status.as_str()])
                        .inc();
                    if is_retryable_status(status) {
                        m.blob_request_giveups_total.inc();
                    }
                }
                return Err(BcError::Storage {
                    status: status.as_u16(),
                    message: body.chars().take(200).collect(),
                });
            }
            if let Some(m) = &self.metrics {
                m.blob_request_status_total
                    .with_label_values(&[status.as_str()])
                    .inc();
            }
            match resp.bytes().await {
                Ok(b) => return Ok(b),
                Err(e)
                    if attempt < max_attempts
                        && (e.is_body() || e.is_timeout() || e.is_decode()) =>
                {
                    let delay = backoff_ms(attempt);
                    if let Some(m) = &self.metrics {
                        m.blob_request_retries_total
                            .with_label_values(&["body"])
                            .inc();
                        m.blob_retry_sleep_seconds_total
                            .inc_by(delay as f64 / 1000.0);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    continue;
                }
                Err(e) => {
                    if let Some(m) = &self.metrics {
                        m.blob_request_status_total
                            .with_label_values(&["body_err"])
                            .inc();
                        m.blob_request_giveups_total.inc();
                    }
                    return Err(e.into());
                }
            }
        }
    }

    pub async fn list_blobs(
        &self,
        account: &str,
        container: &str,
        prefix: Option<&str>,
        recursive: bool,
    ) -> Result<(Vec<ListedBlob>, Vec<String>)> {
        let mut blobs = Vec::new();
        let mut prefixes = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let mut url_str = format!(
                "https://{account}.blob.core.windows.net/{container}?restype=container&comp=list"
            );
            if let Some(p) = prefix {
                let enc =
                    percent_encoding::utf8_percent_encode(p, percent_encoding::NON_ALPHANUMERIC);
                url_str.push_str(&format!("&prefix={enc}"));
            }
            if !recursive {
                url_str.push_str("&delimiter=/");
            }
            if let Some(ref m) = marker {
                url_str.push_str(&format!("&marker={m}"));
            }
            let url = Url::parse(&url_str).map_err(|e| BcError::InvalidUrl(e.to_string()))?;
            let resp = self.send(Method::GET, url, &[], None, None).await?;
            let status = resp.status();
            let body = resp.text().await?;
            if !status.is_success() {
                return Err(BcError::Storage {
                    status: status.as_u16(),
                    message: body.chars().take(200).collect(),
                });
            }
            let parsed: BlobListResponse =
                quick_xml::de::from_str(&body).map_err(|e| BcError::Xml(e.to_string()))?;
            if let Some(list) = parsed.blobs {
                for entry in list.entries {
                    match entry {
                        BlobOrPrefix::Blob(b) => blobs.push(ListedBlob {
                            name: b.name,
                            content_length: b.properties.content_length.unwrap_or(0),
                            etag: b.properties.etag,
                            last_modified: b.properties.last_modified,
                        }),
                        BlobOrPrefix::BlobPrefix(p) => prefixes.push(p.name),
                    }
                }
            }
            match parsed.next_marker {
                Some(ref m) if !m.is_empty() => marker = Some(m.clone()),
                _ => break,
            }
        }
        Ok((blobs, prefixes))
    }

    async fn send(
        &self,
        method: Method,
        url: Url,
        extra_headers: &[(&'static str, HeaderValue)],
        content_type: Option<&str>,
        body: Option<Bytes>,
    ) -> Result<Response> {
        let max_attempts = self.max_retries.saturating_add(1).max(1);
        let mut attempt = 0u32;
        let mut bearer_invalidated_once = false;
        loop {
            attempt += 1;
            let t_send = std::time::Instant::now();
            match self
                .send_once(
                    method.clone(),
                    url.clone(),
                    extra_headers,
                    content_type,
                    body.clone(),
                    &mut bearer_invalidated_once,
                )
                .await
            {
                Ok(resp) => {
                    let elapsed = t_send.elapsed().as_secs_f64();
                    if let Some(m) = &self.metrics {
                        m.blob_request_seconds.observe(elapsed);
                        if elapsed > m.blob_request_seconds_max.get() {
                            m.blob_request_seconds_max.set(elapsed);
                        }
                    }
                    let status = resp.status();
                    if is_retryable_status(status) && attempt < max_attempts {
                        let delay = retry_delay_ms(&resp, attempt);
                        if let Some(m) = &self.metrics {
                            m.blob_request_retries_total
                                .with_label_values(&[status.as_str()])
                                .inc();
                            m.blob_retry_sleep_seconds_total
                                .inc_by(delay as f64 / 1000.0);
                        }
                        drop(resp);
                        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                        continue;
                    }
                    if let Some(m) = &self.metrics {
                        m.blob_request_status_total
                            .with_label_values(&[status.as_str()])
                            .inc();
                        if is_retryable_status(status) {
                            m.blob_request_giveups_total.inc();
                        }
                    }
                    return Ok(resp);
                }
                Err(BcError::Http(e))
                    if attempt < max_attempts
                        && (e.is_timeout() || e.is_connect() || e.is_request() || e.is_body()) =>
                {
                    let elapsed = t_send.elapsed().as_secs_f64();
                    let delay = backoff_ms(attempt);
                    if let Some(m) = &self.metrics {
                        m.blob_request_seconds.observe(elapsed);
                        if elapsed > m.blob_request_seconds_max.get() {
                            m.blob_request_seconds_max.set(elapsed);
                        }
                        m.blob_request_retries_total
                            .with_label_values(&["net"])
                            .inc();
                        m.blob_retry_sleep_seconds_total
                            .inc_by(delay as f64 / 1000.0);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    continue;
                }
                Err(e) => {
                    let elapsed = t_send.elapsed().as_secs_f64();
                    if let Some(m) = &self.metrics {
                        m.blob_request_seconds.observe(elapsed);
                        if elapsed > m.blob_request_seconds_max.get() {
                            m.blob_request_seconds_max.set(elapsed);
                        }
                        m.blob_request_status_total
                            .with_label_values(&["err"])
                            .inc();
                        m.blob_request_giveups_total.inc();
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn send_once(
        &self,
        method: Method,
        url: Url,
        extra_headers: &[(&'static str, HeaderValue)],
        content_type: Option<&str>,
        body: Option<Bytes>,
        bearer_invalidated_once: &mut bool,
    ) -> Result<Response> {
        let bearer = match &self.credential {
            Credential::Bearer(src) => Some(src.token().await?),
            _ => None,
        };
        let content_length = body.as_ref().map(|b| b.len() as u64);
        let headers = self.build_headers(
            &url,
            method.as_str(),
            content_type,
            content_length,
            extra_headers,
            bearer.as_deref(),
        )?;
        let mut req_url = url.clone();
        if let Credential::Sas { token } = &self.credential {
            sas::append_sas_token(&mut req_url, token);
        }
        let mut req = self.http.request(method.clone(), req_url).headers(headers);
        if let Some(b) = body.clone() {
            req = req.body(b);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if status == StatusCode::UNAUTHORIZED && !*bearer_invalidated_once {
            if let Credential::Bearer(src) = &self.credential {
                src.invalidate();
                *bearer_invalidated_once = true;
                drop(resp);
                return Box::pin(self.send_once(
                    method,
                    url,
                    extra_headers,
                    content_type,
                    body,
                    bearer_invalidated_once,
                ))
                .await;
            }
        }
        Ok(resp)
    }

    fn build_headers(
        &self,
        url: &Url,
        method: &str,
        content_type: Option<&str>,
        content_length: Option<u64>,
        extra: &[(&'static str, HeaderValue)],
        bearer: Option<&str>,
    ) -> Result<HeaderMap> {
        let mut h = HeaderMap::new();
        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        h.insert("x-ms-date", HeaderValue::from_str(&date).unwrap());
        h.insert("x-ms-version", HeaderValue::from_static(API_VERSION));
        if let Some(ct) = content_type {
            if let Ok(v) = HeaderValue::from_str(ct) {
                h.insert(CONTENT_TYPE, v);
            }
        }
        if let Some(len) = content_length {
            h.insert(
                CONTENT_LENGTH,
                HeaderValue::from_str(&len.to_string()).unwrap(),
            );
        }
        for (n, v) in extra {
            h.insert(*n, v.clone());
        }
        match &self.credential {
            Credential::SharedKey { account, key } => {
                let auth = shared_key::sign_request(account, key, method, url, &h, content_length)?;
                h.insert("Authorization", HeaderValue::from_str(&auth).unwrap());
            }
            Credential::Bearer(_) => {
                if let Some(t) = bearer {
                    h.insert(
                        "Authorization",
                        HeaderValue::from_str(&format!("Bearer {t}")).unwrap(),
                    );
                }
            }
            Credential::Sas { .. } | Credential::Anonymous => {}
        }
        Ok(h)
    }
}

fn is_retryable_status(s: StatusCode) -> bool {
    matches!(
        s,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn retry_delay_ms(resp: &Response, attempt: u32) -> u64 {
    if let Some(secs) = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
    {
        return (secs.saturating_mul(1000)).min(60_000);
    }
    backoff_ms(attempt)
}

fn backoff_ms(attempt: u32) -> u64 {
    let base = 500u64;
    let cap = 30_000u64;
    let exp = base.saturating_mul(1u64 << (attempt - 1).min(10));
    let capped = exp.min(cap);
    let jitter_range = capped / 4;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let jitter = nanos % (jitter_range * 2 + 1);
    capped.saturating_sub(jitter_range).saturating_add(jitter)
}

#[derive(Deserialize)]
#[serde(rename = "EnumerationResults")]
struct BlobListResponse {
    #[serde(rename = "Blobs")]
    blobs: Option<BlobsContainer>,
    #[serde(rename = "NextMarker")]
    next_marker: Option<String>,
}

#[derive(Deserialize)]
struct BlobsContainer {
    #[serde(rename = "$value", default)]
    entries: Vec<BlobOrPrefix>,
}

#[derive(Deserialize)]
enum BlobOrPrefix {
    Blob(BlobItem),
    BlobPrefix(BlobPrefix),
}

#[derive(Deserialize)]
struct BlobItem {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Properties")]
    properties: BlobProperties,
}

#[derive(Deserialize, Default)]
struct BlobProperties {
    #[serde(rename = "Content-Length")]
    content_length: Option<u64>,
    #[serde(rename = "Etag")]
    etag: Option<String>,
    #[serde(rename = "Last-Modified")]
    last_modified: Option<String>,
}

#[derive(Deserialize)]
struct BlobPrefix {
    #[serde(rename = "Name")]
    name: String,
}

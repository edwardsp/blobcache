use std::fmt;

/// 128-bit UUID v4 newtype used to correlate one logical "request" (a FUSE
/// read, a hydrate coordinator invocation, etc.) across the local node and
/// across peer transport hops. Originated by the entry handler; transmitted
/// to peers on the `x-blobcache-rid` HTTP header (TCP transport) or the
/// trailing `request_id` field of `ChunkRequest` (UCX transport). Logged
/// via the `rid` field on the entry span, which `tracing-subscriber`'s
/// default fmt layer attaches to every nested log line for free.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RequestId(String);

impl RequestId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub fn from_header(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.is_empty() || s.len() > 128 {
            return None;
        }
        if !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return None;
        }
        Some(Self(s.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

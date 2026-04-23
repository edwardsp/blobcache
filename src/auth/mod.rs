pub mod imds;
pub mod sas;
pub mod shared_key;

use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use tokio::sync::Mutex as AsyncMutex;
use tracing::warn;

use crate::error::{BcError, Result};

// Refresh the bearer token if it expires within this many seconds. AAD
// storage tokens live ~24h; refreshing 5 min early avoids races with in-flight
// requests that may take seconds to complete.
const REFRESH_SKEW_SECS: u64 = 300;

#[derive(Clone, Debug)]
enum BearerKind {
    Workload,
    Imds,
}

pub struct BearerSource {
    kind: BearerKind,
    // (token, expires_at_unix). Sync mutex - only ever held briefly across
    // a clone; never held across an await.
    cached: Mutex<Option<(String, u64)>>,
    // Async mutex serializes refresh attempts so concurrent callers don't all
    // hit IMDS at once when the token is about to expire (singleflight).
    refresh_lock: AsyncMutex<()>,
}

impl std::fmt::Debug for BearerSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BearerSource")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl BearerSource {
    fn new(kind: BearerKind, initial: (String, u64)) -> Self {
        Self {
            kind,
            cached: Mutex::new(Some(initial)),
            refresh_lock: AsyncMutex::new(()),
        }
    }

    pub async fn token(&self) -> Result<String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        // Fast path: cached and well within validity.
        {
            let g = self.cached.lock().expect("bearer cache poisoned");
            if let Some((t, exp)) = g.as_ref() {
                if *exp > now + REFRESH_SKEW_SECS {
                    return Ok(t.clone());
                }
            }
        }
        // Slow path: serialize the refresh.
        let _refresh = self.refresh_lock.lock().await;
        // Double-check after winning the lock - another task may have
        // refreshed while we waited.
        {
            let g = self.cached.lock().expect("bearer cache poisoned");
            if let Some((t, exp)) = g.as_ref() {
                if *exp > now + REFRESH_SKEW_SECS {
                    return Ok(t.clone());
                }
            }
        }
        // Blocking HTTP call (reqwest::blocking) on a Tokio worker would stall
        // the runtime; push to the blocking pool.
        let kind = self.kind.clone();
        let new = tokio::task::spawn_blocking(move || match kind {
            BearerKind::Workload => imds::get_storage_token_workload(),
            BearerKind::Imds => imds::get_storage_token_imds(),
        })
        .await
        .map_err(|e| BcError::Auth(format!("token refresh join: {e}")))??
        .ok_or_else(|| BcError::Auth("token source no longer available".into()))?;
        let token = new.0.clone();
        *self.cached.lock().expect("bearer cache poisoned") = Some(new);
        Ok(token)
    }

    // Force the next token() call to re-fetch. Called after a 401 so a
    // freshly-refreshed token isn't cached and re-served.
    pub fn invalidate(&self) {
        *self.cached.lock().expect("bearer cache poisoned") = None;
    }
}

#[derive(Clone, Debug)]
pub enum Credential {
    SharedKey { account: String, key: String },
    Sas { token: String },
    Bearer(Arc<BearerSource>),
    Anonymous,
}

fn ambient_cache() -> &'static Mutex<Option<Credential>> {
    static C: OnceLock<Mutex<Option<Credential>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(None))
}

impl Credential {
    pub fn from_env() -> Result<Option<Self>> {
        if let (Ok(account), Ok(key)) = (
            std::env::var("AZURE_STORAGE_ACCOUNT"),
            std::env::var("AZURE_STORAGE_KEY"),
        ) {
            return Ok(Some(Credential::SharedKey { account, key }));
        }
        if let Ok(sas) = std::env::var("AZURE_STORAGE_SAS_TOKEN") {
            return Ok(Some(Credential::Sas { token: sas }));
        }
        Ok(None)
    }

    pub fn resolve(account_name: &str, sas_token: Option<&str>) -> Result<Self> {
        if let Some(sas) = sas_token {
            return Ok(Credential::Sas {
                token: sas.to_string(),
            });
        }
        if let Some(c) = Self::from_env()? {
            return Ok(c);
        }

        let mut guard = ambient_cache().lock().expect("ambient cred mutex poisoned");
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }

        match imds::get_storage_token_workload() {
            Ok(Some(initial)) => {
                let c =
                    Credential::Bearer(Arc::new(BearerSource::new(BearerKind::Workload, initial)));
                *guard = Some(c.clone());
                return Ok(c);
            }
            Ok(None) => {}
            Err(e) => return Err(e),
        }
        match imds::get_storage_token_imds() {
            Ok(Some(initial)) => {
                let c = Credential::Bearer(Arc::new(BearerSource::new(BearerKind::Imds, initial)));
                *guard = Some(c.clone());
                return Ok(c);
            }
            Ok(None) => {}
            Err(e) => return Err(e),
        }
        warn!(
            "no credentials for account {account_name}; using anonymous (will fail on private containers)"
        );
        Ok(Credential::Anonymous)
    }
}

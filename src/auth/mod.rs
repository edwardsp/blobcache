pub mod imds;
pub mod sas;
pub mod shared_key;

use std::sync::{Mutex, OnceLock};
use tracing::warn;

use crate::error::Result;

#[derive(Clone, Debug)]
pub enum Credential {
    SharedKey { account: String, key: String },
    Sas { token: String },
    Bearer { token: String },
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
            return Ok(Credential::Sas { token: sas.to_string() });
        }
        if let Some(c) = Self::from_env()? { return Ok(c); }

        let mut guard = ambient_cache().lock().expect("ambient cred mutex poisoned");
        if let Some(c) = guard.as_ref() { return Ok(c.clone()); }

        match imds::get_storage_token_workload() {
            Ok(Some(token)) => {
                let c = Credential::Bearer { token };
                *guard = Some(c.clone());
                return Ok(c);
            }
            Ok(None) => {}
            Err(e) => return Err(e),
        }
        match imds::get_storage_token_imds() {
            Ok(Some(token)) => {
                let c = Credential::Bearer { token };
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

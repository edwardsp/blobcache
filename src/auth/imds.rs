use serde::Deserialize;
use std::time::Duration;

use crate::error::BcError;

const IMDS_ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";
const IMDS_API_VERSION: &str = "2018-02-01";
const STORAGE_RESOURCE: &str = "https://storage.azure.com/";

const WORKLOAD_CLIENT_ID: &str = "AZURE_CLIENT_ID";
const WORKLOAD_TENANT_ID: &str = "AZURE_TENANT_ID";
const WORKLOAD_TOKEN_FILE: &str = "AZURE_FEDERATED_TOKEN_FILE";
const WORKLOAD_AUTHORITY: &str = "AZURE_AUTHORITY_HOST";

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

pub fn get_storage_token_workload() -> Result<Option<String>, BcError> {
    let (Ok(client_id), Ok(tenant_id), Ok(token_file)) = (
        std::env::var(WORKLOAD_CLIENT_ID),
        std::env::var(WORKLOAD_TENANT_ID),
        std::env::var(WORKLOAD_TOKEN_FILE),
    ) else {
        return Ok(None);
    };
    let authority = std::env::var(WORKLOAD_AUTHORITY)
        .unwrap_or_else(|_| "https://login.microsoftonline.com/".into());
    let assertion = std::fs::read_to_string(&token_file)
        .map_err(|e| BcError::Auth(format!("read federated token {token_file}: {e}")))?;
    let assertion = assertion.trim();
    let url = format!(
        "{}/{}/oauth2/v2.0/token",
        authority.trim_end_matches('/'),
        tenant_id
    );
    let params = [
        ("client_id", client_id.as_str()),
        ("scope", "https://storage.azure.com/.default"),
        ("grant_type", "client_credentials"),
        (
            "client_assertion_type",
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        ),
        ("client_assertion", assertion),
    ];
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| BcError::Auth(format!("http client: {e}")))?;
    let resp = client
        .post(&url)
        .form(&params)
        .send()
        .map_err(|e| BcError::Auth(format!("workload token exchange: {e}")))?;
    let status = resp.status();
    let body = resp
        .text()
        .map_err(|e| BcError::Auth(format!("read body: {e}")))?;
    if !status.is_success() {
        return Err(BcError::Auth(format!(
            "workload token HTTP {status}: {body}"
        )));
    }
    let parsed: TokenResponse = serde_json::from_str(&body)
        .map_err(|e| BcError::Auth(format!("parse: {e} body={body}")))?;
    Ok(Some(parsed.access_token))
}

pub fn get_storage_token_imds() -> Result<Option<String>, BcError> {
    let client_id = std::env::var(WORKLOAD_CLIENT_ID).ok();
    let resource_id = std::env::var("AZURE_MSI_RESOURCE_ID").ok();
    let mut url =
        format!("{IMDS_ENDPOINT}?api-version={IMDS_API_VERSION}&resource={STORAGE_RESOURCE}");
    if let Some(ref cid) = client_id {
        url.push_str("&client_id=");
        url.push_str(cid);
    } else if let Some(ref rid) = resource_id {
        url.push_str("&msi_res_id=");
        url.push_str(rid);
    }
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .map_err(|e| BcError::Auth(format!("http client: {e}")))?;
    let resp: reqwest::blocking::Response = match client.get(&url).header("Metadata", "true").send()
    {
        Ok(r) => r,
        Err(_) => return Ok(None),
    };
    let status = resp.status();
    let body = resp
        .text()
        .map_err(|e| BcError::Auth(format!("read body: {e}")))?;
    if !status.is_success() {
        if body.contains("Multiple user assigned identities") {
            return Err(BcError::Auth(
                "IMDS: multiple user-assigned identities; set AZURE_CLIENT_ID".into(),
            ));
        }
        let not_applicable = status.as_u16() == 404
            || (status.as_u16() == 400
                && (body.contains("Identity not found")
                    || body.contains("No managed identity")
                    || body.contains("identity_not_found")));
        if not_applicable {
            return Ok(None);
        }
        return Err(BcError::Auth(format!("IMDS HTTP {status}: {body}")));
    }
    let parsed: TokenResponse = serde_json::from_str(&body)
        .map_err(|e| BcError::Auth(format!("parse: {e} body={body}")))?;
    Ok(Some(parsed.access_token))
}

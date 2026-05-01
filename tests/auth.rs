use blobcache::auth::sas::append_sas_token;
use blobcache::auth::Credential;
use std::sync::Mutex;
use url::Url;

static ENV_LOCK: Mutex<()> = Mutex::new(());

const VARS: &[&str] = &[
    "AZURE_STORAGE_ACCOUNT",
    "AZURE_STORAGE_KEY",
    "AZURE_STORAGE_SAS_TOKEN",
];

fn clear_auth_env() {
    for v in VARS {
        unsafe {
            std::env::remove_var(v);
        }
    }
}

fn set(k: &str, v: &str) {
    unsafe {
        std::env::set_var(k, v);
    }
}

#[test]
fn from_env_returns_none_when_nothing_set() {
    let _g = ENV_LOCK.lock().unwrap();
    clear_auth_env();
    let c = Credential::from_env().expect("ok");
    assert!(c.is_none());
}

#[test]
fn from_env_prefers_shared_key_when_account_and_key_set() {
    let _g = ENV_LOCK.lock().unwrap();
    clear_auth_env();
    set("AZURE_STORAGE_ACCOUNT", "myacct");
    set("AZURE_STORAGE_KEY", "Zm9v");
    set("AZURE_STORAGE_SAS_TOKEN", "sig=ignored");
    let c = Credential::from_env().expect("ok").expect("some");
    match c {
        Credential::SharedKey { account, key } => {
            assert_eq!(account, "myacct");
            assert_eq!(key, "Zm9v");
        }
        other => panic!("expected SharedKey, got {other:?}"),
    }
    clear_auth_env();
}

#[test]
fn from_env_falls_back_to_sas_when_only_token_set() {
    let _g = ENV_LOCK.lock().unwrap();
    clear_auth_env();
    set("AZURE_STORAGE_SAS_TOKEN", "sv=2021&sig=abc");
    let c = Credential::from_env().expect("ok").expect("some");
    match c {
        Credential::Sas { token } => assert_eq!(token, "sv=2021&sig=abc"),
        other => panic!("expected Sas, got {other:?}"),
    }
    clear_auth_env();
}

#[test]
fn from_env_ignores_partial_shared_key() {
    let _g = ENV_LOCK.lock().unwrap();
    clear_auth_env();
    set("AZURE_STORAGE_ACCOUNT", "x");
    let c = Credential::from_env().expect("ok");
    assert!(
        c.is_none(),
        "account-only without key must NOT produce SharedKey"
    );
    clear_auth_env();
    set("AZURE_STORAGE_KEY", "Zm9v");
    let c = Credential::from_env().expect("ok");
    assert!(
        c.is_none(),
        "key-only without account must NOT produce SharedKey"
    );
    clear_auth_env();
}

#[test]
fn resolve_inline_sas_short_circuits_env_and_imds() {
    let _g = ENV_LOCK.lock().unwrap();
    clear_auth_env();
    set("AZURE_STORAGE_ACCOUNT", "envacct");
    set("AZURE_STORAGE_KEY", "Zm9v");
    let c = Credential::resolve("acct", Some("?sig=inline-wins")).expect("ok");
    match c {
        Credential::Sas { token } => assert_eq!(
            token, "?sig=inline-wins",
            "inline SAS must win over env-set SharedKey"
        ),
        other => panic!("expected Sas, got {other:?}"),
    }
    clear_auth_env();
}

#[test]
fn resolve_env_shared_key_used_when_no_inline_sas() {
    let _g = ENV_LOCK.lock().unwrap();
    clear_auth_env();
    set("AZURE_STORAGE_ACCOUNT", "envacct");
    set("AZURE_STORAGE_KEY", "Zm9v");
    let c = Credential::resolve("acct", None).expect("ok");
    match c {
        Credential::SharedKey { account, .. } => assert_eq!(account, "envacct"),
        other => panic!("expected SharedKey, got {other:?}"),
    }
    clear_auth_env();
}

#[test]
fn append_sas_token_to_url_without_existing_query() {
    let mut u = Url::parse("https://acct.blob.core.windows.net/c/b").unwrap();
    append_sas_token(&mut u, "sv=2021&sig=abc");
    assert_eq!(u.query(), Some("sv=2021&sig=abc"));
}

#[test]
fn append_sas_token_strips_leading_question_mark() {
    let mut u = Url::parse("https://acct.blob.core.windows.net/c/b").unwrap();
    append_sas_token(&mut u, "?sv=2021&sig=abc");
    assert_eq!(
        u.query(),
        Some("sv=2021&sig=abc"),
        "leading '?' must be stripped to avoid '??' double-prefix"
    );
}

#[test]
fn append_sas_token_merges_with_existing_query() {
    let mut u = Url::parse("https://acct.blob.core.windows.net/c/b?comp=list").unwrap();
    append_sas_token(&mut u, "sv=2021&sig=abc");
    assert_eq!(u.query(), Some("comp=list&sv=2021&sig=abc"));
}

#[test]
fn append_sas_token_merges_with_existing_query_and_strips_question() {
    let mut u = Url::parse("https://acct.blob.core.windows.net/c/b?comp=list").unwrap();
    append_sas_token(&mut u, "?sv=2021&sig=abc");
    assert_eq!(u.query(), Some("comp=list&sv=2021&sig=abc"));
}

use thiserror::Error;

#[derive(Error, Debug)]
pub enum BcError {
    #[error("auth: {0}")]
    Auth(String),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("config: {0}")]
    Config(String),
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    #[error("xml: {0}")]
    Xml(String),
    #[error("storage {status}: {message}")]
    Storage { status: u16, message: String },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("peer: {0}")]
    Peer(String),
    #[error("cluster: {0}")]
    Cluster(String),
    #[error("other: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, BcError>;

impl From<anyhow::Error> for BcError {
    fn from(e: anyhow::Error) -> Self {
        BcError::Other(e.to_string())
    }
}
impl From<serde_json::Error> for BcError {
    fn from(e: serde_json::Error) -> Self {
        BcError::Other(format!("json: {e}"))
    }
}
impl From<toml::de::Error> for BcError {
    fn from(e: toml::de::Error) -> Self {
        BcError::Config(e.to_string())
    }
}

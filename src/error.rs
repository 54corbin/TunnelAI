#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP parse error: {0}")]
    HttpParse(String),
    #[error("request validation: {0}")]
    RequestValidation(String),
    #[error("tunnel: {0}")]
    Tunnel(String),
    #[error("provider: {0}")]
    Provider(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("connection limit: {0}")]
    ConnectionLimit(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

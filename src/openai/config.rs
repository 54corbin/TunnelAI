use anyhow::{Context, Result, bail};
use iroh::PublicKey;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub(crate) const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const DEFAULT_TUNNEL_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const DEFAULT_RECONNECT_ATTEMPTS: usize = 1;
pub(crate) const DEFAULT_MAX_CONCURRENT_SESSIONS: usize = 128;
pub(crate) const DEFAULT_MAX_CONNECTIONS: usize = 128;
pub(crate) const DEFAULT_MAX_STREAMS_PER_CONNECTION: usize = 128;

#[derive(Debug, Clone)]
pub struct OpenAiServerConfig {
    pub provider_base_url: reqwest::Url,
    pub bind_addr: SocketAddr,
    pub allow_peers: Vec<PublicKey>,
    pub request_read_timeout: Duration,
    pub provider_connect_timeout: Duration,
    pub provider_request_timeout: Duration,
    pub max_connections: usize,
    pub max_streams_per_connection: usize,
    pub identity_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct OpenAiClientConfig {
    pub local_request_timeout: Duration,
    pub tunnel_operation_timeout: Duration,
    pub connect_timeout: Duration,
    pub reconnect_attempts: usize,
    pub health_check_interval: Option<Duration>,
    pub max_concurrent_sessions: usize,
}

impl Default for OpenAiClientConfig {
    fn default() -> Self {
        Self {
            local_request_timeout: REQUEST_READ_TIMEOUT,
            tunnel_operation_timeout: DEFAULT_TUNNEL_OPERATION_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            reconnect_attempts: DEFAULT_RECONNECT_ATTEMPTS,
            health_check_interval: None,
            max_concurrent_sessions: DEFAULT_MAX_CONCURRENT_SESSIONS,
        }
    }
}

pub(crate) fn validate_server_config(config: &OpenAiServerConfig) -> Result<()> {
    validate_provider_url_policy(&config.provider_base_url)?;
    if config.request_read_timeout.is_zero() {
        bail!("request read timeout must be greater than zero");
    }
    if config.provider_connect_timeout.is_zero() {
        bail!("provider connect timeout must be greater than zero");
    }
    if config.provider_request_timeout.is_zero() {
        bail!("provider request timeout must be greater than zero");
    }
    if config.max_connections == 0 {
        bail!("max connections must be greater than zero");
    }
    if config.max_streams_per_connection == 0 {
        bail!("max streams per connection must be greater than zero");
    }
    Ok(())
}

pub(crate) fn validate_client_config(config: &OpenAiClientConfig) -> Result<()> {
    if config.local_request_timeout.is_zero() {
        bail!("local request timeout must be greater than zero");
    }
    if config.tunnel_operation_timeout.is_zero() {
        bail!("tunnel operation timeout must be greater than zero");
    }
    if config.connect_timeout.is_zero() {
        bail!("connect timeout must be greater than zero");
    }
    if config.reconnect_attempts == 0 {
        bail!("reconnect attempts must be greater than zero");
    }
    if config
        .health_check_interval
        .is_some_and(|interval| interval.is_zero())
    {
        bail!("health check interval must be greater than zero");
    }
    if config.max_concurrent_sessions == 0 {
        bail!("max concurrent sessions must be greater than zero");
    }
    Ok(())
}
pub fn validate_provider_base_url(base_url: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(base_url).context("parse provider base URL")?;
    validate_provider_url_policy(&url)?;
    Ok(url)
}

fn validate_provider_url_policy(url: &reqwest::Url) -> Result<()> {
    match url.scheme() {
        "http" | "https" => {}
        scheme => bail!("provider base URL must use http or https, got {scheme}"),
    }
    if url.host().is_none() {
        bail!("provider base URL must include a host");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("provider base URL must not include credentials");
    }
    Ok(())
}

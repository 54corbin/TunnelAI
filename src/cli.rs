use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

fn duration_millis(value: &str) -> Result<Duration, String> {
    let millis = value
        .parse::<u64>()
        .map_err(|err| format!("invalid millisecond value: {err}"))?;
    if millis == 0 {
        return Err("must be greater than zero".to_string());
    }
    Ok(Duration::from_millis(millis))
}

fn nonzero_usize(value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|err| format!("invalid integer value: {err}"))?;
    if parsed == 0 {
        return Err("must be greater than zero".to_string());
    }
    Ok(parsed)
}

#[derive(Debug, Parser)]
#[command(
    name = "tunnelAI",
    version,
    about = "SOCKS5 and OpenAI-compatible API proxy over iroh"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the remote TCP exit server over iroh.
    Server(ServerArgs),
    /// Run a local SOCKS5 listener and tunnel through an iroh server.
    Client(ClientArgs),
    /// Run the server-side OpenAI-compatible provider proxy over iroh.
    #[command(name = "openai-server")]
    OpenAiServer(OpenAiServerArgs),
    /// Run a local OpenAI-compatible HTTP listener and tunnel through an iroh server.
    #[command(name = "openai-client")]
    OpenAiClient(OpenAiClientArgs),
}

#[derive(Debug, Parser, Clone)]
pub struct ServerArgs {
    /// Optional UDP bind address for the local iroh endpoint.
    #[arg(long, default_value = "0.0.0.0:0")]
    pub bind_addr: SocketAddr,

    /// Allow connecting to private/loopback/link-local target addresses.
    #[arg(long, default_value_t = false)]
    pub allow_private_targets: bool,

    /// Restrict incoming iroh connections to these endpoint IDs. If omitted, all peers are allowed.
    #[arg(long = "allow-peer")]
    pub allow_peers: Vec<String>,
}

#[derive(Debug, Parser, Clone)]
pub struct ClientArgs {
    /// Iroh endpoint ticket printed by the server.
    #[arg(long)]
    pub server_ticket: String,

    /// Local SOCKS5 listen address.
    #[arg(long, default_value = "127.0.0.1:1080")]
    pub listen: SocketAddr,
}

#[derive(Debug, Parser, Clone)]
pub struct OpenAiServerArgs {
    /// Provider base URL that the server forwards requests to, e.g. https://api.openai.com.
    #[arg(long)]
    pub provider_base_url: String,

    /// UDP bind address for the server iroh endpoint. Use a fixed port with --identity-path for restart-stable tickets.
    #[arg(long, default_value = "0.0.0.0:0")]
    pub bind_addr: SocketAddr,

    /// Restrict incoming iroh connections to these endpoint IDs. If omitted, all peers are allowed.
    #[arg(long = "allow-peer")]
    pub allow_peers: Vec<String>,

    /// Maximum concurrent incoming iroh connections.
    #[arg(long, value_parser = nonzero_usize, default_value = "128")]
    pub max_connections: usize,

    /// Maximum concurrent bidirectional streams per iroh connection.
    #[arg(long, value_parser = nonzero_usize, default_value = "128")]
    pub max_streams_per_connection: usize,

    /// Timeout for receiving a tunneled HTTP request from the OpenAI client, in milliseconds.
    #[arg(long, value_parser = duration_millis, default_value = "10000")]
    pub request_read_timeout_ms: Duration,

    /// Timeout for connecting to the provider, in milliseconds.
    #[arg(long, value_parser = duration_millis, default_value = "10000")]
    pub provider_connect_timeout_ms: Duration,

    /// Timeout for receiving provider response headers, in milliseconds. Streaming response bodies are not capped by this value.
    #[arg(long, value_parser = duration_millis, default_value = "120000")]
    pub provider_request_timeout_ms: Duration,

    /// Path to a persistent iroh identity secret for stable server tickets across restarts.
    #[arg(long)]
    pub identity_path: Option<PathBuf>,
}

#[derive(Debug, Parser, Clone)]
pub struct OpenAiClientArgs {
    /// Iroh endpoint ticket printed by the OpenAI proxy server.
    #[arg(long)]
    pub server_ticket: String,

    /// Local OpenAI-compatible HTTP listen address.
    #[arg(long, default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,

    /// Maximum concurrent local HTTP sessions.
    #[arg(long, value_parser = nonzero_usize, default_value = "128")]
    pub max_concurrent_sessions: usize,

    /// Timeout for reading one local HTTP request, in milliseconds.
    #[arg(long, value_parser = duration_millis, default_value = "10000")]
    pub local_request_timeout_ms: Duration,

    /// Timeout for opening/writing/reading through the iroh tunnel, in milliseconds.
    #[arg(long, value_parser = duration_millis, default_value = "10000")]
    pub tunnel_operation_timeout_ms: Duration,

    /// Timeout for dialing the iroh OpenAI proxy server, in milliseconds.
    #[arg(long, value_parser = duration_millis, default_value = "10000")]
    pub connect_timeout_ms: Duration,

    /// Number of reconnect attempts after the first failed tunnel stream open.
    #[arg(long, value_parser = nonzero_usize, default_value = "1")]
    pub reconnect_attempts: usize,

    /// Optional interval for background OpenAI proxy tunnel health checks, in milliseconds.
    #[arg(long, value_parser = duration_millis)]
    pub health_check_interval_ms: Option<Duration>,
}

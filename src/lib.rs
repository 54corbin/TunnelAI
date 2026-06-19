pub mod cli;
pub mod client;
pub mod copy;
pub mod error;
pub mod iroh_endpoint;
pub mod openai;
pub mod route_logging;
pub mod server;
pub mod socks5;
pub mod tunnel;

pub const ALPN: &[u8] = b"iroh-socks5-proxy/0";

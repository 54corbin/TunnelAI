pub mod config;

pub(crate) use config::{
    DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_STREAMS_PER_CONNECTION, PROVIDER_CONNECT_TIMEOUT,
    PROVIDER_REQUEST_TIMEOUT, REQUEST_READ_TIMEOUT, validate_client_config, validate_server_config,
};
pub use config::{OpenAiClientConfig, OpenAiServerConfig, validate_provider_base_url};

pub mod server;
pub use server::{
    OpenAiServerHandle, run_server, start_openai_server, start_openai_server_for_test,
    start_openai_server_for_test_with_config, start_openai_server_with_allow_peers,
};

pub mod client;
pub use client::{
    OpenAiClientHandle, run_client, start_openai_client, start_openai_client_for_test,
    start_openai_client_for_test_with_config,
};

pub mod client_handler;
pub use client_handler::openai_health_check_for_test;
pub(crate) use client_handler::{openai_client_accept_loop, openai_health_check_loop};

pub mod server_handler;
pub(crate) use server_handler::{OpenAiServerRuntime, spawn_openai_accept_loop};
pub(crate) const OPENAI_CONNECTION_HANDSHAKE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(10);

pub mod connection;
pub(crate) use connection::OpenAiConnectionManager;

pub mod url;
pub use url::provider_request_url;
pub(crate) use url::provider_request_url_from_base;

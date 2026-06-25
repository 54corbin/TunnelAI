use anyhow::{Context, Result};
use iroh::{Endpoint, PublicKey};
use tokio::task::JoinHandle;

use crate::cli::OpenAiServerArgs;
use crate::iroh_endpoint::{endpoint_ticket_string, start_server_endpoint_with_optional_identity};
use crate::server::parse_allow_peers;

use super::{
    DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_STREAMS_PER_CONNECTION, OpenAiServerConfig,
    OpenAiServerRuntime, PROVIDER_CONNECT_TIMEOUT, PROVIDER_REQUEST_TIMEOUT, REQUEST_READ_TIMEOUT,
    spawn_openai_accept_loop, validate_provider_base_url, validate_server_config,
};

pub struct OpenAiServerHandle {
    pub ticket: String,
    endpoint: Endpoint,
    accept_task: JoinHandle<()>,
}

impl OpenAiServerHandle {
    pub async fn shutdown(self) -> Result<()> {
        self.accept_task.abort();
        let _ = self.accept_task.await;
        self.endpoint.close().await;
        Ok(())
    }
}

pub async fn run_server(args: OpenAiServerArgs) -> Result<()> {
    let allow_peers = parse_allow_peers(&args.allow_peers)?;
    let config = OpenAiServerConfig {
        provider_base_url: validate_provider_base_url(&args.provider_base_url)?,
        bind_addr: args.bind_addr,
        allow_peers,
        request_read_timeout: args.request_read_timeout_ms,
        provider_connect_timeout: args.provider_connect_timeout_ms,
        provider_request_timeout: args.provider_request_timeout_ms,
        max_connections: args.max_connections,
        max_streams_per_connection: args.max_streams_per_connection,
        identity_path: args.identity_path,
    };
    let handle = start_openai_server_for_test_with_config(config).await?;
    println!("openai proxy server ticket: {}", handle.ticket);
    tokio::signal::ctrl_c().await.context("wait for ctrl-c")?;
    handle.shutdown().await
}

pub async fn start_openai_server(provider_base_url: String) -> Result<OpenAiServerHandle> {
    start_openai_server_with_allow_peers(provider_base_url, Vec::new()).await
}

pub async fn start_openai_server_with_allow_peers(
    provider_base_url: String,
    allow_peers: Vec<PublicKey>,
) -> Result<OpenAiServerHandle> {
    let config = OpenAiServerConfig {
        provider_base_url: validate_provider_base_url(&provider_base_url)?,
        bind_addr: "0.0.0.0:0".parse().expect("valid default bind addr"),
        allow_peers,
        request_read_timeout: REQUEST_READ_TIMEOUT,
        provider_connect_timeout: PROVIDER_CONNECT_TIMEOUT,
        provider_request_timeout: PROVIDER_REQUEST_TIMEOUT,
        max_connections: DEFAULT_MAX_CONNECTIONS,
        max_streams_per_connection: DEFAULT_MAX_STREAMS_PER_CONNECTION,
        identity_path: None,
    };
    start_openai_server_for_test_with_config(config).await
}

pub async fn start_openai_server_for_test(provider_base_url: String) -> Result<OpenAiServerHandle> {
    start_openai_server(provider_base_url).await
}

pub async fn start_openai_server_for_test_with_config(
    config: OpenAiServerConfig,
) -> Result<OpenAiServerHandle> {
    validate_server_config(&config)?;
    let endpoint = start_server_endpoint_with_optional_identity(
        config.bind_addr,
        config.identity_path.as_deref(),
    )
    .await?;
    let ticket = endpoint_ticket_string(&endpoint).await?;
    let provider_client = reqwest::Client::builder()
        .connect_timeout(config.provider_connect_timeout)
        .build()
        .context("build OpenAI provider HTTP client")?;
    let runtime = OpenAiServerRuntime {
        config,
        provider_client,
    };
    let accept_task = spawn_openai_accept_loop(endpoint.clone(), runtime);
    Ok(OpenAiServerHandle {
        ticket,
        endpoint,
        accept_task,
    })
}

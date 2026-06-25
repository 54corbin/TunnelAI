use anyhow::{Context, Result};
use iroh::Endpoint;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::cli::OpenAiClientArgs;
use crate::iroh_endpoint::{parse_server_ticket, start_client_endpoint};

use super::{
    OpenAiClientConfig, OpenAiConnectionManager, openai_client_accept_loop,
    openai_health_check_loop, validate_client_config,
};

pub struct OpenAiClientHandle {
    pub listen_addr: SocketAddr,
    endpoint: Endpoint,
    listener_task: JoinHandle<()>,
    pub(crate) manager: Arc<OpenAiConnectionManager>,
    pub(crate) config: OpenAiClientConfig,
    health_task: Option<JoinHandle<()>>,
}

impl OpenAiClientHandle {
    pub async fn shutdown(self) -> Result<()> {
        if let Some(health_task) = self.health_task {
            health_task.abort();
            let _ = health_task.await;
        }
        self.endpoint.close().await;
        self.listener_task.abort();
        let _ = self.listener_task.await;
        Ok(())
    }
}
pub async fn run_client(args: OpenAiClientArgs) -> Result<()> {
    let config = OpenAiClientConfig {
        local_request_timeout: args.local_request_timeout_ms,
        tunnel_operation_timeout: args.tunnel_operation_timeout_ms,
        connect_timeout: args.connect_timeout_ms,
        reconnect_attempts: args.reconnect_attempts,
        health_check_interval: args.health_check_interval_ms,
        max_concurrent_sessions: args.max_concurrent_sessions,
    };
    let handle =
        start_openai_client_for_test_with_config(args.server_ticket, args.listen, config).await?;
    println!(
        "local OpenAI-compatible API listening on {}",
        handle.listen_addr
    );
    tokio::signal::ctrl_c().await.context("wait for ctrl-c")?;
    handle.shutdown().await
}

pub async fn start_openai_client(
    server_ticket: String,
    listen: SocketAddr,
) -> Result<OpenAiClientHandle> {
    start_openai_client_for_test_with_config(server_ticket, listen, OpenAiClientConfig::default())
        .await
}

pub async fn start_openai_client_for_test(
    server_ticket: String,
    listen: SocketAddr,
) -> Result<OpenAiClientHandle> {
    start_openai_client(server_ticket, listen).await
}

pub async fn start_openai_client_for_test_with_config(
    server_ticket: String,
    listen: SocketAddr,
    config: OpenAiClientConfig,
) -> Result<OpenAiClientHandle> {
    validate_client_config(&config)?;
    let server_addr = parse_server_ticket(&server_ticket)?;
    let endpoint = start_client_endpoint().await?;
    let manager = Arc::new(
        OpenAiConnectionManager::connect(endpoint.clone(), server_addr, config.connect_timeout)
            .await?,
    );

    let listener = TcpListener::bind(listen)
        .await
        .context("bind local OpenAI-compatible listener")?;
    let listen_addr = listener
        .local_addr()
        .context("get local OpenAI-compatible listen addr")?;
    let session_limit = Arc::new(Semaphore::new(config.max_concurrent_sessions));
    let health_task = config.health_check_interval.map(|interval| {
        tokio::spawn(openai_health_check_loop(
            manager.clone(),
            interval,
            config.tunnel_operation_timeout,
            config.reconnect_attempts,
        ))
    });
    let listener_task = tokio::spawn(openai_client_accept_loop(
        listener,
        manager.clone(),
        config.clone(),
        session_limit,
    ));

    Ok(OpenAiClientHandle {
        listen_addr,
        endpoint,
        listener_task,
        manager,
        config,
        health_task,
    })
}

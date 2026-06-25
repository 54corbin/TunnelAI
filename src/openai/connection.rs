use anyhow::{Context, Result, bail};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::info;

use crate::route_logging::spawn_path_event_logger;

pub(crate) struct OpenAiConnectionManager {
    endpoint: Endpoint,
    server_addr: EndpointAddr,
    connect_timeout: Duration,
    current: Mutex<Option<ManagedOpenAiConnection>>,
    next_id: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct ManagedOpenAiConnection {
    id: u64,
    connection: Connection,
}

impl OpenAiConnectionManager {
    pub(crate) async fn connect(
        endpoint: Endpoint,
        server_addr: EndpointAddr,
        connect_timeout: Duration,
    ) -> Result<Self> {
        let manager = Self {
            endpoint,
            server_addr,
            connect_timeout,
            current: Mutex::new(None),
            next_id: AtomicU64::new(1),
        };
        manager.connection().await?;
        Ok(manager)
    }

    async fn connection(&self) -> Result<ManagedOpenAiConnection> {
        if let Some(connection) = self.current.lock().await.clone() {
            return Ok(connection);
        }
        self.dial().await
    }

    async fn dial(&self) -> Result<ManagedOpenAiConnection> {
        let mut current = self.current.lock().await;
        if let Some(connection) = current.clone() {
            return Ok(connection);
        }

        let connection = timeout(
            self.connect_timeout,
            self.endpoint.connect(self.server_addr.clone(), crate::ALPN),
        )
        .await
        .context("timed out connecting to iroh OpenAI proxy server")?
        .context("connect to iroh OpenAI proxy server")?;
        info!(server = %self.server_addr.id, "connected to iroh OpenAI proxy server");
        spawn_path_event_logger("openai_client", &connection);
        let managed = ManagedOpenAiConnection {
            id: self.next_id.fetch_add(1, Ordering::Relaxed),
            connection,
        };
        *current = Some(managed.clone());
        Ok(managed)
    }

    async fn invalidate(&self, id: u64) {
        let mut current = self.current.lock().await;
        if current
            .as_ref()
            .is_some_and(|connection| connection.id == id)
        {
            *current = None;
        }
    }

    pub(crate) async fn open_bi_with_reconnect(
        &self,
        reconnect_attempts: usize,
        operation_timeout: Duration,
    ) -> Result<(SendStream, RecvStream)> {
        let mut remaining_reconnects = reconnect_attempts;
        loop {
            let managed = self.connection().await?;
            match timeout(operation_timeout, managed.connection.open_bi()).await {
                Ok(Ok(streams)) => return Ok(streams),
                Ok(Err(err)) => {
                    self.invalidate(managed.id).await;
                    if remaining_reconnects == 0 {
                        return Err(err.into());
                    }
                    remaining_reconnects -= 1;
                }
                Err(_) => {
                    self.invalidate(managed.id).await;
                    if remaining_reconnects == 0 {
                        bail!("timed out opening iroh OpenAI proxy stream");
                    }
                    remaining_reconnects -= 1;
                }
            }
        }
    }
}

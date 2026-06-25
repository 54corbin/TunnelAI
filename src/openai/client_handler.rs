use anyhow::{Context, Result, bail};
use iroh::endpoint::RecvStream;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::debug;

use crate::http::{read_one_local_http_request, write_plain_error};

use super::{OpenAiClientConfig, OpenAiClientHandle, OpenAiConnectionManager};

pub async fn openai_health_check_for_test(client: &OpenAiClientHandle) -> Result<()> {
    run_openai_health_check(
        client.manager.clone(),
        client.config.tunnel_operation_timeout,
        client.config.reconnect_attempts,
    )
    .await
}

pub(crate) async fn openai_health_check_loop(
    manager: Arc<OpenAiConnectionManager>,
    interval: Duration,
    operation_timeout: Duration,
    reconnect_attempts: usize,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        if let Err(err) =
            run_openai_health_check(manager.clone(), operation_timeout, reconnect_attempts).await
        {
            debug!(?err, "OpenAI proxy health check failed");
        }
    }
}

async fn run_openai_health_check(
    manager: Arc<OpenAiConnectionManager>,
    operation_timeout: Duration,
    reconnect_attempts: usize,
) -> Result<()> {
    let (mut send, mut recv) = manager
        .open_bi_with_reconnect(reconnect_attempts, operation_timeout)
        .await?;
    timeout(
        operation_timeout,
        send.write_all(b"GET /__tunnelAI/healthz HTTP/1.1\r\nHost: tunnelAI\r\n\r\n"),
    )
    .await
    .context("timed out writing OpenAI proxy health check")??;
    send.finish()?;

    let mut response = Vec::new();
    let mut buf = [0_u8; 256];
    loop {
        let read = timeout(operation_timeout, recv.read(&mut buf))
            .await
            .context("timed out reading OpenAI proxy health check response")??;
        let Some(read) = read else {
            break;
        };
        response.extend_from_slice(&buf[..read]);
        if response
            .windows(b"\r\n\r\n".len())
            .any(|window| window == b"\r\n\r\n")
        {
            break;
        }
    }

    if response.starts_with(b"HTTP/1.1 200") {
        Ok(())
    } else {
        bail!("OpenAI proxy health check returned non-200 response")
    }
}

pub(crate) async fn openai_client_accept_loop(
    listener: TcpListener,
    manager: Arc<OpenAiConnectionManager>,
    config: OpenAiClientConfig,
    session_limit: Arc<Semaphore>,
) {
    loop {
        let Ok((stream, peer_addr)) = listener.accept().await else {
            break;
        };
        let manager = manager.clone();
        let permit = match session_limit.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                debug!(%peer_addr, "rejecting local OpenAI HTTP connection over concurrency limit");
                drop(stream);
                continue;
            }
        };
        let config = config.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = handle_openai_client_stream(stream, manager, peer_addr, config).await
            {
                debug!(?err, %peer_addr, "OpenAI client stream ended with error");
            }
        });
    }
}

async fn handle_openai_client_stream(
    local: TcpStream,
    manager: Arc<OpenAiConnectionManager>,
    peer_addr: SocketAddr,
    config: OpenAiClientConfig,
) -> Result<()> {
    let (local_read, mut local_write) = local.into_split();
    let Some(request) =
        read_local_openai_request(local_read, &mut local_write, config.local_request_timeout)
            .await?
    else {
        return Ok(());
    };

    let Some((mut send, mut recv)) = open_tunnel_stream(
        &manager,
        &mut local_write,
        peer_addr,
        config.reconnect_attempts,
        config.tunnel_operation_timeout,
    )
    .await?
    else {
        return Ok(());
    };

    if !send_request_to_tunnel(
        &mut send,
        &mut local_write,
        &request,
        config.tunnel_operation_timeout,
    )
    .await?
    {
        return Ok(());
    }

    copy_tunnel_response_with_idle_timeout(
        &mut recv,
        &mut local_write,
        config.tunnel_operation_timeout,
    )
    .await?;
    local_write.shutdown().await?;
    Ok(())
}

async fn read_local_openai_request(
    local_read: OwnedReadHalf,
    local_write: &mut OwnedWriteHalf,
    local_request_timeout: Duration,
) -> Result<Option<Vec<u8>>> {
    let mut reader = BufReader::new(local_read);
    match timeout(
        local_request_timeout,
        read_one_local_http_request(&mut reader),
    )
    .await
    {
        Ok(Ok(request)) => Ok(Some(request)),
        Ok(Err(err)) => {
            let response = err.response();
            write_error_and_shutdown(local_write, response.status, response.reason, response.body)
                .await?;
            Ok(None)
        }
        Err(_) => {
            write_error_and_shutdown(local_write, 408, "Request Timeout", "request timeout")
                .await?;
            Ok(None)
        }
    }
}

async fn open_tunnel_stream(
    manager: &OpenAiConnectionManager,
    local_write: &mut OwnedWriteHalf,
    peer_addr: SocketAddr,
    reconnect_attempts: usize,
    tunnel_operation_timeout: Duration,
) -> Result<Option<(iroh::endpoint::SendStream, RecvStream)>> {
    match manager
        .open_bi_with_reconnect(reconnect_attempts, tunnel_operation_timeout)
        .await
    {
        Ok(streams) => Ok(Some(streams)),
        Err(err) => {
            debug!(?err, %peer_addr, "failed to open OpenAI proxy tunnel stream");
            write_error_and_shutdown(local_write, 504, "Gateway Timeout", "gateway timeout")
                .await?;
            Ok(None)
        }
    }
}

async fn send_request_to_tunnel(
    send: &mut iroh::endpoint::SendStream,
    local_write: &mut OwnedWriteHalf,
    request: &[u8],
    tunnel_operation_timeout: Duration,
) -> Result<bool> {
    match timeout(tunnel_operation_timeout, send.write_all(request)).await {
        Ok(Ok(())) => {
            send.finish()?;
            Ok(true)
        }
        Ok(Err(err)) => Err(err.into()),
        Err(_) => {
            let _ = send.finish();
            write_error_and_shutdown(local_write, 504, "Gateway Timeout", "gateway timeout")
                .await?;
            Ok(false)
        }
    }
}

async fn write_error_and_shutdown(
    local_write: &mut OwnedWriteHalf,
    status: u16,
    reason: &str,
    body: &str,
) -> Result<()> {
    write_plain_error(local_write, status, reason, body).await?;
    local_write.shutdown().await?;
    Ok(())
}

async fn copy_tunnel_response_with_idle_timeout(
    recv: &mut RecvStream,
    local_write: &mut (impl AsyncWrite + Unpin),
    idle_timeout: Duration,
) -> Result<()> {
    let mut state = TunnelResponseCopyState::WaitingForFirstByte;
    let mut buf = [0u8; 8192];
    loop {
        let outcome = read_tunnel_response_chunk(recv, &mut buf, idle_timeout, state).await?;
        match outcome {
            TunnelResponseReadOutcome::Chunk(read) => {
                local_write.write_all(&buf[..read]).await?;
                state = TunnelResponseCopyState::Streaming;
            }
            TunnelResponseReadOutcome::TimeoutBeforeFirstByte => {
                write_plain_error(local_write, 504, "Gateway Timeout", "gateway timeout").await?;
                return Ok(());
            }
            TunnelResponseReadOutcome::Complete => return Ok(()),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum TunnelResponseCopyState {
    WaitingForFirstByte,
    Streaming,
}

enum TunnelResponseReadOutcome {
    Chunk(usize),
    TimeoutBeforeFirstByte,
    Complete,
}

async fn read_tunnel_response_chunk(
    recv: &mut RecvStream,
    buf: &mut [u8],
    idle_timeout: Duration,
    state: TunnelResponseCopyState,
) -> Result<TunnelResponseReadOutcome> {
    match timeout(idle_timeout, recv.read(buf)).await {
        Ok(Ok(Some(0)) | Ok(None)) => Ok(TunnelResponseReadOutcome::Complete),
        Ok(Ok(Some(read))) => Ok(TunnelResponseReadOutcome::Chunk(read)),
        Ok(Err(err)) => Err(err.into()),
        Err(_) if state == TunnelResponseCopyState::WaitingForFirstByte => {
            Ok(TunnelResponseReadOutcome::TimeoutBeforeFirstByte)
        }
        Err(_) => Ok(TunnelResponseReadOutcome::Complete),
    }
}

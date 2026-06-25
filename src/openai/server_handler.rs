use anyhow::Result;
use iroh::Endpoint;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::route_logging::{log_connection_route, spawn_path_event_logger};
use crate::server::peer_allowed;

use crate::http::{
    OpenAiHttpRequest, is_health_request, read_http_request, should_forward_request_header,
    write_http_response, write_plain_error, write_plain_response,
};

use super::{
    OPENAI_CONNECTION_HANDSHAKE_TIMEOUT, OpenAiServerConfig, provider_request_url_from_base,
};

#[derive(Clone)]
pub(crate) struct OpenAiServerRuntime {
    pub(crate) config: OpenAiServerConfig,
    pub(crate) provider_client: reqwest::Client,
}

pub(crate) fn spawn_openai_accept_loop(
    endpoint: Endpoint,
    runtime: OpenAiServerRuntime,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let connection_limit = Arc::new(Semaphore::new(runtime.config.max_connections));
        while let Some(incoming) = endpoint.accept().await {
            let runtime = runtime.clone();
            let permit = match connection_limit.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    warn!("rejecting OpenAI proxy iroh connection over concurrency limit");
                    continue;
                }
            };
            tokio::spawn(async move {
                let _permit = permit;
                match timeout(OPENAI_CONNECTION_HANDSHAKE_TIMEOUT, incoming).await {
                    Ok(Ok(connection)) => {
                        let remote = connection.remote_id();
                        if !peer_allowed(&remote, &runtime.config.allow_peers) {
                            warn!(%remote, "rejecting disallowed OpenAI proxy peer");
                            connection.close(1u32.into(), b"peer not allowed");
                            return;
                        }
                        info!(%remote, "accepted OpenAI proxy iroh connection");
                        spawn_path_event_logger("openai_server", &connection);
                        if let Err(err) = handle_openai_connection(connection, runtime).await {
                            debug!(?err, "OpenAI proxy connection handler ended");
                        }
                    }
                    Ok(Err(err)) => error!(?err, "failed to accept OpenAI proxy iroh connection"),
                    Err(_) => warn!("timed out accepting OpenAI proxy iroh connection"),
                }
            });
        }
    })
}

async fn handle_openai_connection(
    connection: Connection,
    runtime: OpenAiServerRuntime,
) -> Result<()> {
    let stream_limit = Arc::new(Semaphore::new(runtime.config.max_streams_per_connection));
    loop {
        let (mut send, recv) = connection.accept_bi().await?;
        let Some(permit) = acquire_stream_permit(stream_limit.clone(), &mut send).await? else {
            drop(recv);
            continue;
        };
        spawn_openai_stream_handler(send, recv, connection.clone(), runtime.clone(), permit);
    }
}

async fn acquire_stream_permit(
    stream_limit: Arc<Semaphore>,
    send: &mut SendStream,
) -> Result<Option<OwnedSemaphorePermit>> {
    match stream_limit.try_acquire_owned() {
        Ok(permit) => Ok(Some(permit)),
        Err(_) => {
            debug!("rejecting OpenAI proxy stream over concurrency limit");
            let _ =
                write_plain_error(send, 503, "Service Unavailable", "too many open streams").await;
            let _ = send.finish();
            Ok(None)
        }
    }
}

fn spawn_openai_stream_handler(
    send: SendStream,
    recv: RecvStream,
    connection: Connection,
    runtime: OpenAiServerRuntime,
    permit: OwnedSemaphorePermit,
) {
    tokio::spawn(async move {
        let _permit = permit;
        if let Err(err) = handle_openai_stream(send, recv, connection, runtime).await {
            debug!(?err, "OpenAI proxy stream ended with error");
        }
    });
}

async fn handle_openai_stream(
    mut send: SendStream,
    recv: RecvStream,
    connection: Connection,
    runtime: OpenAiServerRuntime,
) -> Result<()> {
    log_connection_route(
        "openai_server",
        &connection,
        None,
        Some(runtime.config.provider_base_url.as_str()),
    );

    let Some(request) =
        read_and_validate_request(&mut send, recv, runtime.config.request_read_timeout).await?
    else {
        return Ok(());
    };

    if respond_to_health_check(&mut send, &request).await? {
        return Ok(());
    }

    let Some(response) = forward_request_to_provider(&mut send, &runtime, request).await? else {
        return Ok(());
    };

    copy_provider_response_and_finish(&mut send, response).await
}

async fn read_and_validate_request(
    send: &mut SendStream,
    recv: RecvStream,
    request_read_timeout: Duration,
) -> Result<Option<OpenAiHttpRequest>> {
    match timeout(request_read_timeout, read_http_request(recv)).await {
        Ok(Ok(request)) => Ok(Some(request)),
        Ok(Err(err)) => {
            let response = err.response();
            write_error_and_finish(send, response.status, response.reason, response.body).await?;
            Ok(None)
        }
        Err(_) => {
            write_error_and_finish(send, 408, "Request Timeout", "request timeout").await?;
            Ok(None)
        }
    }
}

async fn respond_to_health_check(
    send: &mut SendStream,
    request: &OpenAiHttpRequest,
) -> Result<bool> {
    if !is_health_request(request) {
        return Ok(false);
    }

    write_plain_response(send, 200, "OK", "ok\n").await?;
    send.finish()?;
    Ok(true)
}

async fn forward_request_to_provider(
    send: &mut SendStream,
    runtime: &OpenAiServerRuntime,
    request: OpenAiHttpRequest,
) -> Result<Option<reqwest::Response>> {
    let request = match provider_request_builder(runtime, request) {
        Ok(request) => request,
        Err(err) => {
            debug!(?err, "failed to build OpenAI provider request URL");
            write_error_and_finish(send, 400, "Bad Request", "bad request").await?;
            return Ok(None);
        }
    };

    match timeout(runtime.config.provider_request_timeout, request.send()).await {
        Ok(Ok(response)) => Ok(Some(response)),
        Ok(Err(err)) => {
            debug!(?err, "failed to forward request to OpenAI provider");
            write_error_and_finish(send, 502, "Bad Gateway", "provider request failed").await?;
            Ok(None)
        }
        Err(_) => {
            debug!("OpenAI provider response headers timed out");
            write_error_and_finish(send, 504, "Gateway Timeout", "provider request timed out")
                .await?;
            Ok(None)
        }
    }
}

fn provider_request_builder(
    runtime: &OpenAiServerRuntime,
    request: OpenAiHttpRequest,
) -> Result<reqwest::RequestBuilder> {
    let url = provider_request_url_from_base(&runtime.config.provider_base_url, &request.target)?;
    let mut builder = runtime.provider_client.request(request.method, url);

    for name in request.headers.keys() {
        if should_forward_request_header(name.as_str()) {
            for value in request.headers.get_all(name) {
                builder = builder.header(name, value.clone());
            }
        }
    }

    Ok(builder.body(request.body))
}

async fn copy_provider_response_and_finish(
    send: &mut SendStream,
    mut response: reqwest::Response,
) -> Result<()> {
    let first_chunk = match response.chunk().await {
        Ok(chunk) => chunk.map(|chunk| chunk.to_vec()),
        Err(err) => {
            debug!(?err, "failed to read OpenAI provider response body");
            write_error_and_finish(send, 502, "Bad Gateway", "provider response failed").await?;
            return Ok(());
        }
    };

    write_http_response(send, response, first_chunk).await?;
    send.finish()?;
    Ok(())
}

async fn write_error_and_finish(
    send: &mut SendStream,
    status: u16,
    reason: &str,
    body: &str,
) -> Result<()> {
    write_plain_error(send, status, reason, body).await?;
    send.finish()?;
    Ok(())
}

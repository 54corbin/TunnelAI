use crate::cli::ClientArgs;
use crate::iroh_endpoint::{parse_server_ticket, start_client_endpoint};
use crate::route_logging::{log_connection_route, spawn_path_event_logger};
use crate::socks5::{
    ConnectRequest, ReplyCode, negotiate_no_auth, read_connect_request, write_reply,
};
use crate::tunnel::{read_response, write_request};
use anyhow::{Context, Result};
use iroh::Endpoint;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, copy};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tracing::{debug, error, info};

pub use crate::openai::start_openai_client_for_test;

const DEFAULT_SOCKS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_TUNNEL_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_CONCURRENT_SESSIONS: usize = 128;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub socks_handshake_timeout: Duration,
    pub tunnel_operation_timeout: Duration,
    pub max_concurrent_sessions: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            socks_handshake_timeout: DEFAULT_SOCKS_HANDSHAKE_TIMEOUT,
            tunnel_operation_timeout: DEFAULT_TUNNEL_OPERATION_TIMEOUT,
            max_concurrent_sessions: DEFAULT_MAX_CONCURRENT_SESSIONS,
        }
    }
}

pub struct ClientHandle {
    pub listen_addr: SocketAddr,
    endpoint: Endpoint,
    listener_task: JoinHandle<()>,
}

impl ClientHandle {
    pub async fn shutdown(self) -> Result<()> {
        self.endpoint.close().await;
        self.listener_task.abort();
        let _ = self.listener_task.await;
        Ok(())
    }
}

pub async fn run(args: ClientArgs) -> Result<()> {
    let handle = start_client_for_test(args.server_ticket, args.listen).await?;
    println!("local socks5 listening on {}", handle.listen_addr);
    tokio::signal::ctrl_c().await.context("wait for ctrl-c")?;
    handle.shutdown().await
}

pub async fn start_client_for_test(
    server_ticket: String,
    listen: SocketAddr,
) -> Result<ClientHandle> {
    start_client_for_test_with_config(server_ticket, listen, ClientConfig::default()).await
}

pub async fn start_client_for_test_with_handshake_timeout(
    server_ticket: String,
    listen: SocketAddr,
    socks_handshake_timeout: Duration,
) -> Result<ClientHandle> {
    let config = ClientConfig {
        socks_handshake_timeout,
        ..ClientConfig::default()
    };
    start_client_for_test_with_config(server_ticket, listen, config).await
}

pub async fn start_client_for_test_with_config(
    server_ticket: String,
    listen: SocketAddr,
    config: ClientConfig,
) -> Result<ClientHandle> {
    let server_addr = parse_server_ticket(&server_ticket)?;
    let endpoint = start_client_endpoint().await?;
    let connection = endpoint
        .connect(server_addr.clone(), crate::ALPN)
        .await
        .context("connect to iroh server")?;
    println!("connected to iroh server {}", server_addr.id);
    spawn_path_event_logger("client", &connection);

    let listener = TcpListener::bind(listen)
        .await
        .context("bind local SOCKS5 listener")?;
    let listen_addr = listener
        .local_addr()
        .context("get local SOCKS5 listen addr")?;
    let connection = Arc::new(connection);
    let session_limit = Arc::new(Semaphore::new(config.max_concurrent_sessions));
    let listener_task = tokio::spawn(accept_local_loop(
        listener,
        connection,
        config,
        session_limit,
    ));

    Ok(ClientHandle {
        listen_addr,
        endpoint,
        listener_task,
    })
}

async fn accept_local_loop(
    listener: TcpListener,
    connection: Arc<Connection>,
    config: ClientConfig,
    session_limit: Arc<Semaphore>,
) {
    loop {
        let Ok((stream, peer_addr)) = listener.accept().await else {
            break;
        };
        let connection = connection.clone();
        let permit = match session_limit.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                debug!(%peer_addr, "rejecting local SOCKS connection over concurrency limit");
                drop(stream);
                continue;
            }
        };
        let config = config.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = handle_socks_connection(
                stream,
                connection,
                config.socks_handshake_timeout,
                config.tunnel_operation_timeout,
                Some(peer_addr),
            )
            .await
            {
                debug!(?err, %peer_addr, "local SOCKS connection ended with error");
            }
        });
    }
}

pub async fn handle_socks_connection(
    mut local: TcpStream,
    connection: Arc<Connection>,
    socks_handshake_timeout: Duration,
    tunnel_operation_timeout: Duration,
    socks_peer: Option<SocketAddr>,
) -> Result<()> {
    if !perform_socks_handshake(&mut local, socks_handshake_timeout).await? {
        return Ok(());
    }

    let Some(request) = read_socks_connect_request(&mut local, socks_handshake_timeout).await?
    else {
        return Ok(());
    };
    let target = request.target_display();
    info!(target = %target, "tunnel opened");
    log_connection_route("client", &connection, socks_peer, Some(&target));

    let Some((mut send, mut recv)) =
        open_tunnel_stream(&mut local, &connection, tunnel_operation_timeout).await?
    else {
        return Ok(());
    };

    if !send_tunnel_request(&mut local, &mut send, &request, tunnel_operation_timeout).await? {
        return Ok(());
    }

    let Some(status) = read_tunnel_status(&mut local, &mut recv, tunnel_operation_timeout).await?
    else {
        return Ok(());
    };

    write_reply(&mut local, status).await?;
    if status != ReplyCode::Succeeded {
        send.finish()?;
        return Ok(());
    }

    relay_socks_tunnel(local, send, recv).await
}

async fn perform_socks_handshake(
    local: &mut TcpStream,
    handshake_timeout: Duration,
) -> Result<bool> {
    match timeout(handshake_timeout, negotiate_no_auth(local)).await {
        Ok(Ok(())) => Ok(true),
        Ok(Err(err)) => {
            error!(?err, "SOCKS method negotiation failed");
            Err(err.into())
        }
        Err(_) => {
            debug!("SOCKS method negotiation timed out");
            Ok(false)
        }
    }
}

async fn read_socks_connect_request(
    local: &mut TcpStream,
    handshake_timeout: Duration,
) -> Result<Option<ConnectRequest>> {
    match timeout(handshake_timeout, read_connect_request(local)).await {
        Ok(Ok(request)) => Ok(Some(request)),
        Ok(Err(err)) => {
            let _ = write_reply(local, err.reply_code()).await;
            Err(err.into())
        }
        Err(_) => {
            debug!("SOCKS CONNECT request timed out");
            Ok(None)
        }
    }
}

async fn open_tunnel_stream(
    local: &mut TcpStream,
    connection: &Connection,
    tunnel_operation_timeout: Duration,
) -> Result<Option<(SendStream, RecvStream)>> {
    match timeout(tunnel_operation_timeout, connection.open_bi()).await {
        Ok(Ok(streams)) => Ok(Some(streams)),
        Ok(Err(err)) => {
            let _ = write_reply(local, ReplyCode::GeneralFailure).await;
            Err(err.into())
        }
        Err(_) => {
            let _ = write_reply(local, ReplyCode::TtlExpired).await;
            Ok(None)
        }
    }
}

async fn send_tunnel_request(
    local: &mut TcpStream,
    send: &mut SendStream,
    request: &ConnectRequest,
    tunnel_operation_timeout: Duration,
) -> Result<bool> {
    match timeout(tunnel_operation_timeout, write_request(send, request)).await {
        Ok(Ok(())) => Ok(true),
        Ok(Err(err)) => {
            let _ = write_reply(local, ReplyCode::GeneralFailure).await;
            Err(err.into())
        }
        Err(_) => {
            let _ = send.finish();
            let _ = write_reply(local, ReplyCode::TtlExpired).await;
            Ok(false)
        }
    }
}

async fn read_tunnel_status(
    local: &mut TcpStream,
    recv: &mut RecvStream,
    tunnel_operation_timeout: Duration,
) -> Result<Option<ReplyCode>> {
    match timeout(tunnel_operation_timeout, read_response(recv)).await {
        Ok(Ok(status)) => Ok(Some(status)),
        Ok(Err(err)) => {
            let _ = write_reply(local, ReplyCode::GeneralFailure).await;
            Err(err.into())
        }
        Err(_) => {
            let _ = write_reply(local, ReplyCode::TtlExpired).await;
            Ok(None)
        }
    }
}

async fn relay_socks_tunnel(
    local: TcpStream,
    mut send: SendStream,
    mut recv: RecvStream,
) -> Result<()> {
    let (mut local_read, mut local_write) = local.into_split();
    let local_to_iroh = async {
        let copied = copy(&mut local_read, &mut send).await?;
        send.finish()?;
        anyhow::Ok(copied)
    };
    let iroh_to_local = async {
        let copied = copy(&mut recv, &mut local_write).await?;
        local_write.shutdown().await?;
        anyhow::Ok(copied)
    };
    let _ = tokio::try_join!(local_to_iroh, iroh_to_local)?;
    Ok(())
}

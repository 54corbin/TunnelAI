use crate::cli::ServerArgs;
use crate::iroh_endpoint::{
    endpoint_ticket_string, start_server_endpoint, start_server_endpoint_with_bind_addr,
};
use crate::route_logging::{log_connection_route, spawn_path_event_logger};
use crate::socks5::{ConnectRequest, ReplyCode, TargetAddr};
use crate::tunnel::{read_request, write_response};
use anyhow::{Context, Result};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, PublicKey};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use tokio::io::{AsyncWriteExt, copy};
use tokio::net::{TcpStream, lookup_host};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tracing::{debug, error, info, warn};

pub use crate::openai::start_openai_server_for_test;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub allow_private_targets: bool,
    pub allow_peers: Vec<PublicKey>,
    pub target_connect_timeout: Duration,
    pub tunnel_request_timeout: Duration,
    pub max_connections: usize,
    pub max_streams_per_connection: usize,
}

impl ServerConfig {
    pub fn new(allow_private_targets: bool, allow_peers: Vec<PublicKey>) -> Self {
        Self {
            allow_private_targets,
            allow_peers,
            target_connect_timeout: Duration::from_secs(15),
            tunnel_request_timeout: Duration::from_secs(10),
            max_connections: 128,
            max_streams_per_connection: 128,
        }
    }
}

pub struct ServerHandle {
    pub ticket: String,
    endpoint: Endpoint,
    accept_task: JoinHandle<()>,
}

impl ServerHandle {
    pub async fn shutdown(self) -> Result<()> {
        self.endpoint.close().await;
        self.accept_task.abort();
        let _ = self.accept_task.await;
        Ok(())
    }
}

pub async fn run(args: ServerArgs) -> Result<()> {
    let allow_peers = parse_allow_peers(&args.allow_peers)?;
    let config = ServerConfig::new(args.allow_private_targets, allow_peers);
    let endpoint = start_server_endpoint_with_bind_addr(args.bind_addr).await?;
    let ticket = endpoint_ticket_string(&endpoint).await?;
    println!("listening as endpoint {}", endpoint.id());
    println!("server ticket: {ticket}");

    let accept_task = spawn_accept_loop(endpoint.clone(), config);
    tokio::signal::ctrl_c().await.context("wait for ctrl-c")?;
    endpoint.close().await;
    accept_task.abort();
    let _ = accept_task.await;
    Ok(())
}

pub async fn start_server_for_test(allow_private_targets: bool) -> Result<ServerHandle> {
    start_server_for_test_with_request_timeout(allow_private_targets, Duration::from_secs(10)).await
}

pub async fn start_server_for_test_with_request_timeout(
    allow_private_targets: bool,
    tunnel_request_timeout: Duration,
) -> Result<ServerHandle> {
    let mut config = ServerConfig::new(allow_private_targets, Vec::new());
    config.tunnel_request_timeout = tunnel_request_timeout;
    start_server_for_test_with_config(config).await
}

pub async fn start_server_for_test_with_config(config: ServerConfig) -> Result<ServerHandle> {
    let endpoint = start_server_endpoint().await?;
    let ticket = endpoint_ticket_string(&endpoint).await?;
    let accept_task = spawn_accept_loop(endpoint.clone(), config);
    Ok(ServerHandle {
        ticket,
        endpoint,
        accept_task,
    })
}

fn spawn_accept_loop(endpoint: Endpoint, config: ServerConfig) -> JoinHandle<()> {
    tokio::spawn(async move {
        let connection_limit = std::sync::Arc::new(Semaphore::new(config.max_connections));
        while let Some(incoming) = endpoint.accept().await {
            let config = config.clone();
            let permit = match connection_limit.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    warn!("rejecting iroh connection over concurrency limit");
                    continue;
                }
            };
            tokio::spawn(async move {
                let _permit = permit;
                match incoming.await {
                    Ok(connection) => {
                        let remote = connection.remote_id();
                        if !peer_allowed(&remote, &config.allow_peers) {
                            warn!(%remote, "rejecting disallowed peer");
                            connection.close(1u32.into(), b"peer not allowed");
                            return;
                        }
                        info!(%remote, "accepted iroh connection");
                        spawn_path_event_logger("server", &connection);
                        if let Err(err) = handle_connection(connection, config).await {
                            debug!(?err, "iroh connection handler ended");
                        }
                    }
                    Err(err) => error!(?err, "failed to accept iroh connection"),
                }
            });
        }
    })
}

async fn handle_connection(connection: Connection, config: ServerConfig) -> Result<()> {
    let stream_limit = std::sync::Arc::new(Semaphore::new(config.max_streams_per_connection));
    loop {
        match connection.accept_bi().await {
            Ok((mut send, recv)) => {
                let permit = match stream_limit.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        debug!("rejecting tunnel stream over concurrency limit");
                        let _ = write_response(&mut send, ReplyCode::GeneralFailure).await;
                        let _ = send.finish();
                        drop(recv);
                        continue;
                    }
                };
                let config = config.clone();
                let connection = connection.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(err) = handle_tunnel_stream(send, recv, config, connection).await {
                        debug!(?err, "tunnel stream ended with error");
                    }
                });
            }
            Err(err) => return Err(err.into()),
        }
    }
}

pub async fn handle_tunnel_stream(
    mut send: SendStream,
    mut recv: RecvStream,
    config: ServerConfig,
    connection: Connection,
) -> Result<()> {
    let request = match timeout(config.tunnel_request_timeout, read_request(&mut recv)).await {
        Ok(Ok(request)) => request,
        Ok(Err(err)) => return Err(err).context("read tunnel request"),
        Err(_) => {
            debug!("timed out waiting for tunnel request");
            send.finish()?;
            return Ok(());
        }
    };
    debug!(target = %request.target_display(), "received tunnel request");
    let target_display = request.target_display();
    log_connection_route("server", &connection, None, Some(&target_display));

    if !target_allowed(&request, config.allow_private_targets) {
        write_response(&mut send, ReplyCode::ConnectionNotAllowed).await?;
        send.finish()?;
        return Ok(());
    }

    let target = match timeout(
        config.target_connect_timeout,
        resolve_target(&request, &config),
    )
    .await
    {
        Ok(Ok(target)) => target,
        Ok(Err(code)) => {
            write_response(&mut send, code).await?;
            send.finish()?;
            return Ok(());
        }
        Err(_) => {
            write_response(&mut send, ReplyCode::TtlExpired).await?;
            send.finish()?;
            return Ok(());
        }
    };
    let tcp = match timeout(config.target_connect_timeout, TcpStream::connect(target)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(err)) => {
            let code = map_connect_error(&err);
            write_response(&mut send, code).await?;
            send.finish()?;
            return Ok(());
        }
        Err(_) => {
            write_response(&mut send, ReplyCode::TtlExpired).await?;
            send.finish()?;
            return Ok(());
        }
    };

    write_response(&mut send, ReplyCode::Succeeded).await?;
    let (mut tcp_read, mut tcp_write) = tcp.into_split();

    let client_to_target = async {
        let copied = copy(&mut recv, &mut tcp_write).await?;
        tcp_write.shutdown().await?;
        anyhow::Ok(copied)
    };
    let target_to_client = async {
        let copied = copy(&mut tcp_read, &mut send).await?;
        send.finish()?;
        anyhow::Ok(copied)
    };

    let _ = tokio::try_join!(client_to_target, target_to_client)?;
    Ok(())
}

async fn resolve_target(
    request: &ConnectRequest,
    config: &ServerConfig,
) -> Result<SocketAddr, ReplyCode> {
    match &request.target {
        TargetAddr::Ip(ip) => {
            let addr = SocketAddr::new(*ip, request.port);
            if target_ip_allowed(*ip, config.allow_private_targets) {
                Ok(addr)
            } else {
                Err(ReplyCode::ConnectionNotAllowed)
            }
        }
        TargetAddr::Domain(domain) => {
            let mut saw_denied = false;
            let addrs = lookup_host((domain.as_str(), request.port))
                .await
                .map_err(|_| ReplyCode::HostUnreachable)?;
            for addr in addrs {
                if target_ip_allowed(addr.ip(), config.allow_private_targets) {
                    return Ok(addr);
                }
                saw_denied = true;
            }
            if saw_denied {
                Err(ReplyCode::ConnectionNotAllowed)
            } else {
                Err(ReplyCode::HostUnreachable)
            }
        }
    }
}

fn map_connect_error(error: &std::io::Error) -> ReplyCode {
    use std::io::ErrorKind;
    match error.kind() {
        ErrorKind::ConnectionRefused => ReplyCode::ConnectionRefused,
        ErrorKind::NotFound => ReplyCode::HostUnreachable,
        ErrorKind::TimedOut => ReplyCode::TtlExpired,
        ErrorKind::AddrNotAvailable => ReplyCode::HostUnreachable,
        ErrorKind::NetworkUnreachable => ReplyCode::NetworkUnreachable,
        _ => ReplyCode::GeneralFailure,
    }
}

pub fn parse_allow_peers(values: &[String]) -> Result<Vec<PublicKey>> {
    values
        .iter()
        .map(|value| {
            PublicKey::from_str(value).with_context(|| format!("invalid peer id: {value}"))
        })
        .collect()
}

pub fn peer_allowed(peer: &PublicKey, allow_peers: &[PublicKey]) -> bool {
    allow_peers.is_empty() || allow_peers.iter().any(|allowed| allowed == peer)
}

pub fn target_allowed(request: &ConnectRequest, allow_private_targets: bool) -> bool {
    match &request.target {
        TargetAddr::Domain(_) => true,
        TargetAddr::Ip(ip) => target_ip_allowed(*ip, allow_private_targets),
    }
}

fn target_ip_allowed(ip: IpAddr, allow_private_targets: bool) -> bool {
    allow_private_targets || is_public_ip(ip)
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_ipv4(ip),
        IpAddr::V6(ip) => is_public_ipv6(ip),
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.octets()[0] == 0
        || ip.octets()[0] >= 224)
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return is_public_ipv4(mapped);
    }

    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_unique_local()
        || is_ipv6_unicast_link_local(ip))
}

fn is_ipv6_unicast_link_local(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    (segments[0] & 0xffc0) == 0xfe80
}

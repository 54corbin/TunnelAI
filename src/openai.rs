use anyhow::{Context, Result, bail};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, PublicKey};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::cli::{OpenAiClientArgs, OpenAiServerArgs};
use crate::iroh_endpoint::{
    endpoint_ticket_string, parse_server_ticket, start_client_endpoint,
    start_server_endpoint_with_optional_identity,
};
use crate::route_logging::{log_connection_route, spawn_path_event_logger};
use crate::server::{parse_allow_peers, peer_allowed};

const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_HEADER_COUNT: usize = 100;
const MAX_BODY_BYTES: usize = 1024 * 1024;
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);
const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_TUNNEL_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_RECONNECT_ATTEMPTS: usize = 1;
const OPENAI_CONNECTION_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_CONCURRENT_SESSIONS: usize = 128;
const DEFAULT_MAX_CONNECTIONS: usize = 128;
const DEFAULT_MAX_STREAMS_PER_CONNECTION: usize = 128;

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

pub struct OpenAiClientHandle {
    pub listen_addr: SocketAddr,
    endpoint: Endpoint,
    listener_task: JoinHandle<()>,
    manager: Arc<OpenAiConnectionManager>,
    config: OpenAiClientConfig,
    health_task: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone)]
pub struct OpenAiServerConfig {
    pub provider_base_url: reqwest::Url,
    pub bind_addr: SocketAddr,
    pub allow_peers: Vec<PublicKey>,
    pub request_read_timeout: Duration,
    pub provider_connect_timeout: Duration,
    pub provider_request_timeout: Duration,
    pub max_connections: usize,
    pub max_streams_per_connection: usize,
    pub identity_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct OpenAiClientConfig {
    pub local_request_timeout: Duration,
    pub tunnel_operation_timeout: Duration,
    pub connect_timeout: Duration,
    pub reconnect_attempts: usize,
    pub health_check_interval: Option<Duration>,
    pub max_concurrent_sessions: usize,
}

impl Default for OpenAiClientConfig {
    fn default() -> Self {
        Self {
            local_request_timeout: REQUEST_READ_TIMEOUT,
            tunnel_operation_timeout: DEFAULT_TUNNEL_OPERATION_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            reconnect_attempts: DEFAULT_RECONNECT_ATTEMPTS,
            health_check_interval: None,
            max_concurrent_sessions: DEFAULT_MAX_CONCURRENT_SESSIONS,
        }
    }
}

fn validate_server_config(config: &OpenAiServerConfig) -> Result<()> {
    validate_provider_url_policy(&config.provider_base_url)?;
    if config.request_read_timeout.is_zero() {
        bail!("request read timeout must be greater than zero");
    }
    if config.provider_connect_timeout.is_zero() {
        bail!("provider connect timeout must be greater than zero");
    }
    if config.provider_request_timeout.is_zero() {
        bail!("provider request timeout must be greater than zero");
    }
    if config.max_connections == 0 {
        bail!("max connections must be greater than zero");
    }
    if config.max_streams_per_connection == 0 {
        bail!("max streams per connection must be greater than zero");
    }
    Ok(())
}

fn validate_client_config(config: &OpenAiClientConfig) -> Result<()> {
    if config.local_request_timeout.is_zero() {
        bail!("local request timeout must be greater than zero");
    }
    if config.tunnel_operation_timeout.is_zero() {
        bail!("tunnel operation timeout must be greater than zero");
    }
    if config.connect_timeout.is_zero() {
        bail!("connect timeout must be greater than zero");
    }
    if config.reconnect_attempts == 0 {
        bail!("reconnect attempts must be greater than zero");
    }
    if config
        .health_check_interval
        .is_some_and(|interval| interval.is_zero())
    {
        bail!("health check interval must be greater than zero");
    }
    if config.max_concurrent_sessions == 0 {
        bail!("max concurrent sessions must be greater than zero");
    }
    Ok(())
}

#[derive(Clone)]
struct OpenAiServerRuntime {
    config: OpenAiServerConfig,
    provider_client: reqwest::Client,
}

struct OpenAiConnectionManager {
    endpoint: Endpoint,
    server_addr: EndpointAddr,
    connect_timeout: Duration,
    current: Mutex<Option<ManagedOpenAiConnection>>,
    next_id: AtomicU64,
}

#[derive(Clone)]
struct ManagedOpenAiConnection {
    id: u64,
    connection: Connection,
}

impl OpenAiConnectionManager {
    async fn connect(
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

    async fn open_bi_with_reconnect(
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

pub fn validate_provider_base_url(base_url: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(base_url).context("parse provider base URL")?;
    validate_provider_url_policy(&url)?;
    Ok(url)
}

fn validate_provider_url_policy(url: &reqwest::Url) -> Result<()> {
    match url.scheme() {
        "http" | "https" => {}
        scheme => bail!("provider base URL must use http or https, got {scheme}"),
    }
    if url.host().is_none() {
        bail!("provider base URL must include a host");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("provider base URL must not include credentials");
    }
    Ok(())
}

pub fn provider_request_url(base_url: &str, request_target: &str) -> Result<reqwest::Url> {
    let base = validate_provider_base_url(base_url)?;
    provider_request_url_from_base(&base, request_target)
}

fn provider_request_url_from_base(
    base_url: &reqwest::Url,
    request_target: &str,
) -> Result<reqwest::Url> {
    if !request_target.starts_with('/') || request_target.starts_with("//") {
        bail!("OpenAI proxy requires origin-form request targets starting with /");
    }
    let mut base = base_url.clone();
    let base_path = base
        .path()
        .trim_end_matches('/')
        .trim_start_matches('/')
        .to_string();
    if !base.path().ends_with('/') {
        let path = format!("{}/", base.path());
        base.set_path(&path);
    }

    let (path, query) = request_target
        .split_once('?')
        .map_or((request_target, None), |(path, query)| (path, Some(query)));
    reject_unsafe_request_path(path)?;
    let mut relative_path = path.trim_start_matches('/');
    if !base_path.is_empty()
        && (relative_path == base_path || relative_path.starts_with(&format!("{base_path}/")))
    {
        relative_path = relative_path[base_path.len()..].trim_start_matches('/');
    }

    let mut url = base
        .join(relative_path)
        .context("join provider request path")?;
    url.set_query(query);
    Ok(url)
}

fn reject_unsafe_request_path(path: &str) -> Result<()> {
    for segment in path.split('/') {
        let decoded = percent_decode_path_segment(segment)?;
        if decoded == b"." || decoded == b".." {
            bail!("request target contains unsafe dot segment");
        }
        if decoded.iter().any(|byte| matches!(byte, b'/' | b'\\')) {
            bail!("request target contains encoded path separator");
        }
    }
    Ok(())
}

fn percent_decode_path_segment(segment: &str) -> Result<Vec<u8>> {
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                bail!("request target contains invalid percent encoding");
            }
            let high = hex_value(bytes[index + 1])?;
            let low = hex_value(bytes[index + 2])?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Ok(decoded)
}

fn hex_value(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("request target contains invalid percent encoding"),
    }
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

fn spawn_openai_accept_loop(endpoint: Endpoint, runtime: OpenAiServerRuntime) -> JoinHandle<()> {
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
        match connection.accept_bi().await {
            Ok((mut send, recv)) => {
                let permit = match stream_limit.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        debug!("rejecting OpenAI proxy stream over concurrency limit");
                        let _ = write_plain_error(
                            &mut send,
                            503,
                            "Service Unavailable",
                            "too many open streams",
                        )
                        .await;
                        let _ = send.finish();
                        drop(recv);
                        continue;
                    }
                };
                let connection = connection.clone();
                let runtime = runtime.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(err) = handle_openai_stream(send, recv, connection, runtime).await {
                        debug!(?err, "OpenAI proxy stream ended with error");
                    }
                });
            }
            Err(err) => return Err(err.into()),
        }
    }
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
    let request = match timeout(runtime.config.request_read_timeout, read_http_request(recv)).await
    {
        Ok(Ok(request)) => request,
        Ok(Err(err)) => {
            let response = err.response();
            write_plain_error(&mut send, response.status, response.reason, response.body).await?;
            send.finish()?;
            return Ok(());
        }
        Err(_) => {
            write_plain_error(&mut send, 408, "Request Timeout", "request timeout").await?;
            send.finish()?;
            return Ok(());
        }
    };
    if is_health_request(&request) {
        write_plain_response(&mut send, 200, "OK", "ok\n").await?;
        send.finish()?;
        return Ok(());
    }

    let url =
        match provider_request_url_from_base(&runtime.config.provider_base_url, &request.target) {
            Ok(url) => url,
            Err(err) => {
                debug!(?err, "failed to build OpenAI provider request URL");
                write_plain_error(&mut send, 400, "Bad Request", "bad request").await?;
                send.finish()?;
                return Ok(());
            }
        };
    let mut builder = runtime.provider_client.request(request.method, url);

    for name in request.headers.keys() {
        if should_forward_request_header(name.as_str()) {
            for value in request.headers.get_all(name) {
                builder = builder.header(name, value.clone());
            }
        }
    }

    let mut response = match timeout(
        runtime.config.provider_request_timeout,
        builder.body(request.body).send(),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => {
            debug!(?err, "failed to forward request to OpenAI provider");
            write_plain_error(&mut send, 502, "Bad Gateway", "provider request failed").await?;
            send.finish()?;
            return Ok(());
        }
        Err(_) => {
            debug!("OpenAI provider response headers timed out");
            write_plain_error(
                &mut send,
                504,
                "Gateway Timeout",
                "provider request timed out",
            )
            .await?;
            send.finish()?;
            return Ok(());
        }
    };
    let first_chunk = match response.chunk().await {
        Ok(chunk) => chunk.map(|chunk| chunk.to_vec()),
        Err(err) => {
            debug!(?err, "failed to read OpenAI provider response body");
            write_plain_error(&mut send, 502, "Bad Gateway", "provider response failed").await?;
            send.finish()?;
            return Ok(());
        }
    };
    write_http_response(&mut send, response, first_chunk).await?;
    send.finish()?;
    Ok(())
}

struct OpenAiHttpRequest {
    method: reqwest::Method,
    target: String,
    headers: reqwest::header::HeaderMap,
    body: Vec<u8>,
}

struct HttpErrorResponse {
    status: u16,
    reason: &'static str,
    body: &'static str,
}

#[derive(Debug, Error)]
enum HttpRequestError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("request headers too large")]
    HeadersTooLarge,
    #[error("payload too large")]
    PayloadTooLarge,
    #[error("chunked request bodies are not supported")]
    UnsupportedChunked,
}

impl HttpRequestError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    fn response(&self) -> HttpErrorResponse {
        match self {
            Self::BadRequest(_) => HttpErrorResponse {
                status: 400,
                reason: "Bad Request",
                body: "bad request",
            },
            Self::HeadersTooLarge => HttpErrorResponse {
                status: 431,
                reason: "Request Header Fields Too Large",
                body: "request headers too large",
            },
            Self::PayloadTooLarge => HttpErrorResponse {
                status: 413,
                reason: "Payload Too Large",
                body: "payload too large",
            },
            Self::UnsupportedChunked => HttpErrorResponse {
                status: 501,
                reason: "Not Implemented",
                body: "chunked request bodies are not supported",
            },
        }
    }
}

async fn read_http_request(
    recv: RecvStream,
) -> std::result::Result<OpenAiHttpRequest, HttpRequestError> {
    let mut reader = BufReader::new(recv);
    let line = read_limited_line(&mut reader, MAX_REQUEST_LINE_BYTES)
        .await
        .map_err(|err| match err {
            HttpRequestError::HeadersTooLarge => {
                HttpRequestError::BadRequest("request line too large".into())
            }
            err => err,
        })?;
    let request_line = line.trim_end_matches(['\r', '\n']);
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing method"))?
        .parse::<reqwest::Method>()
        .map_err(|_| HttpRequestError::bad_request("parse HTTP method"))?;
    let target = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing target"))?
        .to_string();
    let version = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing version"))?;
    if parts.next().is_some() {
        return Err(HttpRequestError::bad_request(
            "HTTP request line has too many parts",
        ));
    }
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return Err(HttpRequestError::bad_request(format!(
            "unsupported HTTP request version: {version}"
        )));
    }
    if !target.starts_with('/') || target.starts_with("//") {
        return Err(HttpRequestError::bad_request(
            "OpenAI proxy requires origin-form request target",
        ));
    }

    let mut headers = reqwest::header::HeaderMap::new();
    let mut content_length = None::<usize>;
    let mut has_chunked_body = false;
    let mut header_bytes = 0usize;
    let mut header_count = 0usize;
    loop {
        let line = read_limited_line(&mut reader, MAX_HEADER_BYTES).await?;
        header_bytes += line.len();
        if header_bytes > MAX_HEADER_BYTES {
            return Err(HttpRequestError::HeadersTooLarge);
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        header_count += 1;
        if header_count > MAX_HEADER_COUNT {
            return Err(HttpRequestError::HeadersTooLarge);
        }
        let header_line = line.trim_end_matches(['\r', '\n']);
        let Some((name, value)) = header_line.split_once(':') else {
            return Err(HttpRequestError::bad_request(format!(
                "invalid HTTP header line: {header_line}"
            )));
        };
        let name = reqwest::header::HeaderName::from_bytes(name.trim().as_bytes())
            .map_err(|_| HttpRequestError::bad_request("parse HTTP header name"))?;
        let value = reqwest::header::HeaderValue::from_str(value.trim())
            .map_err(|_| HttpRequestError::bad_request("parse HTTP header value"))?;
        if name == reqwest::header::CONTENT_LENGTH {
            let parsed = value
                .to_str()
                .map_err(|_| HttpRequestError::bad_request("content-length is not valid text"))?
                .parse::<usize>()
                .map_err(|_| HttpRequestError::bad_request("parse content-length"))?;
            if parsed > MAX_BODY_BYTES {
                return Err(HttpRequestError::PayloadTooLarge);
            }
            if let Some(existing) = content_length
                && existing != parsed
            {
                return Err(HttpRequestError::bad_request(
                    "conflicting content-length headers",
                ));
            }
            content_length = Some(parsed);
        }
        if name == reqwest::header::TRANSFER_ENCODING {
            has_chunked_body = value
                .to_str()
                .map_err(|_| HttpRequestError::bad_request("transfer-encoding is not valid text"))?
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"));
        }
        headers.append(name, value);
    }

    if has_chunked_body {
        return Err(HttpRequestError::UnsupportedChunked);
    }

    let content_length = content_length.unwrap_or(0);
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader
            .read_exact(&mut body)
            .await
            .map_err(|_| HttpRequestError::bad_request("read request body"))?;
    }

    Ok(OpenAiHttpRequest {
        method,
        target,
        headers,
        body,
    })
}

async fn read_limited_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_len: usize,
) -> std::result::Result<String, HttpRequestError> {
    let bytes = read_limited_line_bytes(reader, max_len).await?;
    String::from_utf8(bytes).map_err(|_| HttpRequestError::bad_request("HTTP line is not utf-8"))
}

async fn read_limited_line_bytes<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_len: usize,
) -> std::result::Result<Vec<u8>, HttpRequestError> {
    let mut bytes = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|_| HttpRequestError::bad_request("read HTTP line"))?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Err(HttpRequestError::bad_request("HTTP request ended early"));
            }
            break;
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if bytes.len() + take > max_len {
            return Err(HttpRequestError::HeadersTooLarge);
        }
        bytes.extend_from_slice(&available[..take]);
        reader.consume(take);
        if bytes.ends_with(b"\n") {
            break;
        }
    }
    Ok(bytes)
}

fn should_forward_request_header(name: &str) -> bool {
    !matches!(
        name,
        "host"
            | "content-length"
            | "connection"
            | "transfer-encoding"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "upgrade"
    )
}

fn is_health_request(request: &OpenAiHttpRequest) -> bool {
    request.method == reqwest::Method::GET && request.target == "/__tunnelAI/healthz"
}

async fn write_plain_error<W: AsyncWrite + Unpin>(
    send: &mut W,
    status: u16,
    reason: &str,
    body: &str,
) -> Result<()> {
    write_plain_response(send, status, reason, body).await
}

async fn write_plain_response<W: AsyncWrite + Unpin>(
    send: &mut W,
    status: u16,
    reason: &str,
    body: &str,
) -> Result<()> {
    send.write_all(format!("HTTP/1.1 {status} {reason}\r\n").as_bytes())
        .await?;
    send.write_all(b"content-type: text/plain\r\n").await?;
    send.write_all(b"connection: close\r\n").await?;
    send.write_all(format!("content-length: {}\r\n", body.len()).as_bytes())
        .await?;
    send.write_all(b"\r\n").await?;
    send.write_all(body.as_bytes()).await?;
    Ok(())
}

async fn write_http_response(
    send: &mut SendStream,
    mut response: reqwest::Response,
    first_chunk: Option<Vec<u8>>,
) -> Result<()> {
    let status = response.status();
    let headers = response.headers().clone();
    let reason = status.canonical_reason().unwrap_or("");

    send.write_all(format!("HTTP/1.1 {} {}\r\n", status.as_u16(), reason).as_bytes())
        .await?;
    for (name, value) in headers.iter() {
        if should_forward_response_header(name.as_str()) {
            send.write_all(name.as_str().to_ascii_lowercase().as_bytes())
                .await?;
            send.write_all(b": ").await?;
            send.write_all(value.as_bytes()).await?;
            send.write_all(b"\r\n").await?;
        }
    }
    send.write_all(b"connection: close\r\n").await?;
    send.write_all(b"transfer-encoding: chunked\r\n").await?;
    send.write_all(b"\r\n").await?;

    if let Some(chunk) = first_chunk {
        write_response_chunk(send, &chunk).await?;
    }
    while let Some(chunk) = response
        .chunk()
        .await
        .context("read provider response body chunk")?
    {
        write_response_chunk(send, &chunk).await?;
    }
    send.write_all(b"0\r\n\r\n").await?;
    Ok(())
}

async fn write_response_chunk(send: &mut SendStream, chunk: &[u8]) -> Result<()> {
    send.write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
        .await?;
    send.write_all(chunk).await?;
    send.write_all(b"\r\n").await?;
    Ok(())
}

fn should_forward_response_header(name: &str) -> bool {
    !matches!(
        name,
        "content-length"
            | "connection"
            | "transfer-encoding"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "upgrade"
    )
}

pub async fn openai_health_check_for_test(client: &OpenAiClientHandle) -> Result<()> {
    run_openai_health_check(
        client.manager.clone(),
        client.config.tunnel_operation_timeout,
        client.config.reconnect_attempts,
    )
    .await
}

async fn openai_health_check_loop(
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

async fn openai_client_accept_loop(
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
    let mut reader = BufReader::new(local_read);
    let request = match timeout(
        config.local_request_timeout,
        read_one_local_http_request(&mut reader),
    )
    .await
    {
        Ok(Ok(request)) => request,
        Ok(Err(err)) => {
            let response = err.response();
            write_plain_error(
                &mut local_write,
                response.status,
                response.reason,
                response.body,
            )
            .await?;
            local_write.shutdown().await?;
            return Ok(());
        }
        Err(_) => {
            write_plain_error(&mut local_write, 408, "Request Timeout", "request timeout").await?;
            local_write.shutdown().await?;
            return Ok(());
        }
    };

    let (mut send, mut recv) = match manager
        .open_bi_with_reconnect(config.reconnect_attempts, config.tunnel_operation_timeout)
        .await
    {
        Ok(streams) => streams,
        Err(err) => {
            debug!(?err, %peer_addr, "failed to open OpenAI proxy tunnel stream");
            write_plain_error(&mut local_write, 504, "Gateway Timeout", "gateway timeout").await?;
            local_write.shutdown().await?;
            return Ok(());
        }
    };
    match timeout(config.tunnel_operation_timeout, send.write_all(&request)).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => return Err(err.into()),
        Err(_) => {
            write_plain_error(&mut local_write, 504, "Gateway Timeout", "gateway timeout").await?;
            local_write.shutdown().await?;
            return Ok(());
        }
    }
    send.finish()?;
    copy_tunnel_response_with_idle_timeout(
        &mut recv,
        &mut local_write,
        config.tunnel_operation_timeout,
    )
    .await?;
    local_write.shutdown().await?;
    Ok(())
}

async fn copy_tunnel_response_with_idle_timeout(
    recv: &mut RecvStream,
    local_write: &mut (impl AsyncWrite + Unpin),
    idle_timeout: Duration,
) -> Result<()> {
    let mut wrote_any = false;
    let mut buf = [0u8; 8192];
    loop {
        let read = match timeout(idle_timeout, recv.read(&mut buf)).await {
            Ok(Ok(read)) => read,
            Ok(Err(err)) => return Err(err.into()),
            Err(_) if wrote_any => return Ok(()),
            Err(_) => {
                write_plain_error(local_write, 504, "Gateway Timeout", "gateway timeout").await?;
                return Ok(());
            }
        };
        let Some(read) = read else {
            return Ok(());
        };
        if read == 0 {
            return Ok(());
        }
        local_write.write_all(&buf[..read]).await?;
        wrote_any = true;
    }
}

async fn read_one_local_http_request(
    reader: &mut BufReader<OwnedReadHalf>,
) -> std::result::Result<Vec<u8>, HttpRequestError> {
    let mut raw = Vec::new();
    let line = read_limited_line_bytes(reader, MAX_REQUEST_LINE_BYTES)
        .await
        .map_err(|err| match err {
            HttpRequestError::HeadersTooLarge => {
                HttpRequestError::BadRequest("request line too large".into())
            }
            err => err,
        })?;
    let request_line = std::str::from_utf8(&line)
        .map_err(|_| HttpRequestError::bad_request("HTTP request line is not utf-8"))?
        .trim_end_matches(['\r', '\n']);
    let mut parts = request_line.split_whitespace();
    let _method = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing method"))?
        .parse::<reqwest::Method>()
        .map_err(|_| HttpRequestError::bad_request("parse HTTP method"))?;
    let target = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing target"))?;
    let version = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing version"))?;
    if parts.next().is_some() {
        return Err(HttpRequestError::bad_request(
            "HTTP request line has too many parts",
        ));
    }
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return Err(HttpRequestError::bad_request(format!(
            "unsupported HTTP request version: {version}"
        )));
    }
    if !target.starts_with('/') || target.starts_with("//") {
        return Err(HttpRequestError::bad_request(
            "OpenAI proxy requires origin-form request target",
        ));
    }
    raw.extend_from_slice(&line);

    let mut header_bytes = 0usize;
    let mut header_count = 0usize;
    let mut content_length = None::<usize>;
    let mut has_chunked_body = false;
    loop {
        let line = read_limited_line_bytes(reader, MAX_HEADER_BYTES).await?;
        header_bytes += line.len();
        if header_bytes > MAX_HEADER_BYTES {
            return Err(HttpRequestError::HeadersTooLarge);
        }
        raw.extend_from_slice(&line);
        if line == b"\r\n" || line == b"\n" {
            break;
        }
        header_count += 1;
        if header_count > MAX_HEADER_COUNT {
            return Err(HttpRequestError::HeadersTooLarge);
        }
        let header_line = std::str::from_utf8(&line)
            .map_err(|_| HttpRequestError::bad_request("HTTP header line is not utf-8"))?
            .trim_end_matches(['\r', '\n']);
        let Some((name, value)) = header_line.split_once(':') else {
            return Err(HttpRequestError::bad_request(format!(
                "invalid HTTP header line: {header_line}"
            )));
        };
        let name = reqwest::header::HeaderName::from_bytes(name.trim().as_bytes())
            .map_err(|_| HttpRequestError::bad_request("parse HTTP header name"))?;
        let value = reqwest::header::HeaderValue::from_str(value.trim())
            .map_err(|_| HttpRequestError::bad_request("parse HTTP header value"))?;
        if name == reqwest::header::CONTENT_LENGTH {
            let parsed = value
                .to_str()
                .map_err(|_| HttpRequestError::bad_request("content-length is not valid text"))?
                .parse::<usize>()
                .map_err(|_| HttpRequestError::bad_request("parse content-length"))?;
            if parsed > MAX_BODY_BYTES {
                return Err(HttpRequestError::PayloadTooLarge);
            }
            if let Some(existing) = content_length
                && existing != parsed
            {
                return Err(HttpRequestError::bad_request(
                    "conflicting content-length headers",
                ));
            }
            content_length = Some(parsed);
        }
        if name == reqwest::header::TRANSFER_ENCODING {
            has_chunked_body = value
                .to_str()
                .map_err(|_| HttpRequestError::bad_request("transfer-encoding is not valid text"))?
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"));
        }
    }

    if !has_chunked_body {
        let content_length = content_length.unwrap_or(0);
        let start = raw.len();
        raw.resize(start + content_length, 0);
        if content_length > 0 {
            reader
                .read_exact(&mut raw[start..])
                .await
                .map_err(|_| HttpRequestError::bad_request("read request body"))?;
        }
        if !reader.buffer().is_empty() {
            return Err(HttpRequestError::bad_request(
                "local connection contains more than one HTTP request",
            ));
        }
    }

    Ok(raw)
}

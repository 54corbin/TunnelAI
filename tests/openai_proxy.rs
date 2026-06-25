mod common;

use common::{
    chunked_body, endpoint_ticket_string, openai_error_response_for_request, parse_server_ticket,
    start_client_endpoint, start_openai_client_for_test, start_openai_client_for_test_with_config,
    start_openai_server_for_test, start_openai_server_for_test_with_config, start_server_endpoint,
};
use iroh::{SecretKey, TransportAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep, timeout};
use tunnel_ai::openai::{
    OpenAiClientConfig, OpenAiServerConfig, openai_health_check_for_test, provider_request_url,
    start_openai_server_with_allow_peers, validate_provider_base_url,
};

#[test]
fn provider_url_preserves_openai_path_and_query() {
    let url =
        provider_request_url("https://api.openai.com/v1", "/chat/completions?stream=true").unwrap();

    assert_eq!(
        url.as_str(),
        "https://api.openai.com/v1/chat/completions?stream=true"
    );
}

#[test]
fn provider_url_does_not_duplicate_provider_base_path() {
    let url = provider_request_url(
        "http://localhost:20128/v1",
        "/v1/chat/completions?stream=true",
    )
    .unwrap();

    assert_eq!(
        url.as_str(),
        "http://localhost:20128/v1/chat/completions?stream=true"
    );
}

#[test]
fn provider_url_rejects_absolute_form_request_targets() {
    let error = provider_request_url("https://api.openai.com/v1", "http://evil.test/v1/models")
        .expect_err("absolute-form request targets must not override provider base URL");

    assert!(error.to_string().contains("origin-form"));
}

#[test]
fn provider_url_rejects_dot_segment_path_escape() {
    for target in [
        "/v1/../admin",
        "/v1/%2e%2e/admin",
        "/v1/%2E%2E/admin",
        "/v1/%2e/%2e%2e/admin",
        "/v1/foo/%2f../admin",
    ] {
        let error = provider_request_url("https://api.openai.com/v1", target)
            .expect_err("dot-segment request targets must not escape provider base path");
        let message = error.to_string();
        assert!(
            message.contains("dot segment") || message.contains("encoded path separator"),
            "unexpected error: {message}"
        );
    }
}

#[test]
fn provider_base_url_rejects_non_http_and_credentials() {
    assert!(validate_provider_base_url("file:///tmp/provider").is_err());
    assert!(validate_provider_base_url("https://user:pass@example.test/v1").is_err());
    assert!(validate_provider_base_url("https://api.openai.com/v1").is_ok());
}

#[tokio::test]
async fn openai_server_config_rejects_invalid_provider_urls() {
    for raw_url in ["file:///tmp/provider", "https://user:pass@example.test/v1"] {
        let result = start_openai_server_for_test_with_config(OpenAiServerConfig {
            provider_base_url: reqwest::Url::parse(raw_url).unwrap(),
            bind_addr: "0.0.0.0:0".parse().unwrap(),
            allow_peers: Vec::new(),
            request_read_timeout: Duration::from_secs(1),
            provider_connect_timeout: Duration::from_secs(1),
            provider_request_timeout: Duration::from_secs(1),
            max_connections: 1,
            max_streams_per_connection: 1,
            identity_path: None,
        })
        .await;

        match result {
            Ok(handle) => {
                handle.shutdown().await.unwrap();
                panic!("server config should reject invalid provider URL policy");
            }
            Err(error) => assert!(
                error.to_string().contains("provider base URL"),
                "unexpected error: {error:#}"
            ),
        }
    }
}

#[tokio::test]
async fn openai_server_config_rejects_zero_limits_and_timeouts() {
    let mut config = OpenAiServerConfig {
        provider_base_url: validate_provider_base_url("http://127.0.0.1:9").unwrap(),
        bind_addr: "0.0.0.0:0".parse().unwrap(),
        allow_peers: Vec::new(),
        request_read_timeout: Duration::from_secs(1),
        provider_connect_timeout: Duration::from_secs(1),
        provider_request_timeout: Duration::from_secs(1),
        max_connections: 1,
        max_streams_per_connection: 1,
        identity_path: None,
    };
    config.max_connections = 0;

    match start_openai_server_for_test_with_config(config).await {
        Ok(handle) => {
            handle.shutdown().await.unwrap();
            panic!("server config should reject zero max connections");
        }
        Err(error) => assert!(error.to_string().contains("max connections")),
    }
}

#[tokio::test]
async fn openai_server_config_reuses_persistent_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let identity_path = temp_dir.path().join("openai-server.key");
    let base_config = OpenAiServerConfig {
        provider_base_url: validate_provider_base_url("http://127.0.0.1:9").unwrap(),
        bind_addr: "0.0.0.0:0".parse().unwrap(),
        allow_peers: Vec::new(),
        request_read_timeout: Duration::from_secs(1),
        provider_connect_timeout: Duration::from_secs(1),
        provider_request_timeout: Duration::from_secs(1),
        max_connections: 1,
        max_streams_per_connection: 1,
        identity_path: Some(identity_path),
    };

    let first = start_openai_server_for_test_with_config(base_config.clone())
        .await
        .unwrap();
    let first_id = parse_server_ticket(&first.ticket).unwrap().id;
    first.shutdown().await.unwrap();

    let second = start_openai_server_for_test_with_config(base_config)
        .await
        .unwrap();
    let second_id = parse_server_ticket(&second.ticket).unwrap().id;
    second.shutdown().await.unwrap();

    assert_eq!(first_id, second_id);
}

#[tokio::test]
async fn openai_client_reconnects_after_persistent_identity_server_restart() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let provider_task = tokio::spawn(async move {
        let (mut stream, _) = provider.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            request.extend_from_slice(&buf[..read]);
            if request
                .windows(b"\r\n\r\n".len())
                .any(|window| window == b"\r\n\r\n")
            {
                break;
            }
        }
        assert!(String::from_utf8_lossy(&request).starts_with("GET /v1/models HTTP/1.1\r\n"));
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\nreconnected")
            .await
            .unwrap();
    });

    let temp_dir = tempfile::tempdir().unwrap();
    let bind_addr = std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let server_config = OpenAiServerConfig {
        provider_base_url: validate_provider_base_url(&format!("http://{provider_addr}/v1"))
            .unwrap(),
        bind_addr,
        allow_peers: Vec::new(),
        request_read_timeout: Duration::from_secs(2),
        provider_connect_timeout: Duration::from_secs(2),
        provider_request_timeout: Duration::from_secs(2),
        max_connections: 4,
        max_streams_per_connection: 4,
        identity_path: Some(temp_dir.path().join("server.key")),
    };

    let first_server = start_openai_server_for_test_with_config(server_config.clone())
        .await
        .unwrap();
    let ticket = first_server.ticket.clone();
    let parsed_ticket = parse_server_ticket(&ticket).unwrap();
    assert!(
        parsed_ticket
            .addrs
            .iter()
            .any(|addr| matches!(addr, TransportAddr::Ip(addr) if *addr == bind_addr)),
        "old ticket should carry the fixed direct bind address: {parsed_ticket:?}"
    );
    let client = start_openai_client_for_test_with_config(
        ticket,
        "127.0.0.1:0".parse().unwrap(),
        OpenAiClientConfig {
            local_request_timeout: Duration::from_secs(2),
            tunnel_operation_timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(2),
            reconnect_attempts: 2,
            health_check_interval: None,
            max_concurrent_sessions: 4,
        },
    )
    .await
    .unwrap();
    first_server.shutdown().await.unwrap();

    let second_server = start_openai_server_for_test_with_config(server_config)
        .await
        .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream
        .write_all(b"GET /v1/models HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    stream.shutdown().await.unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response_text = String::from_utf8_lossy(&response);
    assert!(
        response_text.starts_with("HTTP/1.1 200 OK\r\n"),
        "unexpected response after reconnect: {response_text}"
    );
    assert!(response_text.contains("reconnected"));

    client.shutdown().await.unwrap();
    second_server.shutdown().await.unwrap();
    provider_task.await.unwrap();
}

#[tokio::test]
async fn openai_client_returns_gateway_timeout_when_reconnect_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let bind_addr = std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let server = start_openai_server_for_test_with_config(OpenAiServerConfig {
        provider_base_url: validate_provider_base_url("http://127.0.0.1:9/v1").unwrap(),
        bind_addr,
        allow_peers: Vec::new(),
        request_read_timeout: Duration::from_millis(500),
        provider_connect_timeout: Duration::from_millis(500),
        provider_request_timeout: Duration::from_millis(500),
        max_connections: 4,
        max_streams_per_connection: 4,
        identity_path: Some(temp_dir.path().join("server.key")),
    })
    .await
    .unwrap();

    let client = start_openai_client_for_test_with_config(
        server.ticket.clone(),
        "127.0.0.1:0".parse().unwrap(),
        OpenAiClientConfig {
            local_request_timeout: Duration::from_millis(500),
            tunnel_operation_timeout: Duration::from_millis(500),
            connect_timeout: Duration::from_millis(500),
            reconnect_attempts: 1,
            health_check_interval: None,
            max_concurrent_sessions: 4,
        },
    )
    .await
    .unwrap();
    server.shutdown().await.unwrap();

    let response = timeout(Duration::from_secs(5), async {
        let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
        stream
            .write_all(b"GET /v1/models HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8_lossy(&response).into_owned()
    })
    .await
    .unwrap();

    assert!(
        response.starts_with("HTTP/1.1 504 Gateway Timeout\r\n"),
        "unexpected response while server is down: {response}"
    );

    client.shutdown().await.unwrap();
}

#[tokio::test]
async fn openai_client_config_rejects_zero_limits_and_timeouts() {
    let base_config = OpenAiClientConfig {
        local_request_timeout: Duration::from_secs(1),
        tunnel_operation_timeout: Duration::from_secs(1),
        connect_timeout: Duration::from_secs(1),
        reconnect_attempts: 1,
        health_check_interval: Some(Duration::from_secs(1)),
        max_concurrent_sessions: 1,
    };

    for (config, expected) in [
        (
            OpenAiClientConfig {
                local_request_timeout: Duration::ZERO,
                ..base_config.clone()
            },
            "local request timeout",
        ),
        (
            OpenAiClientConfig {
                tunnel_operation_timeout: Duration::ZERO,
                ..base_config.clone()
            },
            "tunnel operation timeout",
        ),
        (
            OpenAiClientConfig {
                connect_timeout: Duration::ZERO,
                ..base_config.clone()
            },
            "connect timeout",
        ),
        (
            OpenAiClientConfig {
                reconnect_attempts: 0,
                ..base_config.clone()
            },
            "reconnect attempts",
        ),
        (
            OpenAiClientConfig {
                health_check_interval: Some(Duration::ZERO),
                ..base_config.clone()
            },
            "health check interval",
        ),
        (
            OpenAiClientConfig {
                max_concurrent_sessions: 0,
                ..base_config
            },
            "max concurrent sessions",
        ),
    ] {
        match start_openai_client_for_test_with_config(
            "not-a-ticket".to_string(),
            "127.0.0.1:0".parse().unwrap(),
            config,
        )
        .await
        {
            Ok(handle) => {
                handle.shutdown().await.unwrap();
                panic!("client config should reject {expected} before parsing ticket");
            }
            Err(error) => assert!(
                error.to_string().contains(expected),
                "expected error to contain {expected}, got {error:#}"
            ),
        }
    }
}

#[tokio::test]
async fn openai_health_request_returns_ok_without_contacting_provider() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let (contact_tx, contact_rx) = oneshot::channel::<()>();
    let provider_task = tokio::spawn(async move {
        let (mut stream, _) = provider.accept().await.unwrap();
        let _ = contact_tx.send(());
        let _ = stream
            .write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
            .await;
    });

    let server = start_openai_server_for_test(format!("http://{provider_addr}"))
        .await
        .unwrap();
    let client =
        start_openai_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream
        .write_all(b"GET /__tunnelAI/healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    stream.shutdown().await.unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response_text = String::from_utf8_lossy(&response);
    assert!(
        response_text.starts_with("HTTP/1.1 200 OK\r\n"),
        "unexpected response: {response_text}"
    );
    assert!(response_text.contains("ok"));
    assert!(
        timeout(Duration::from_millis(100), contact_rx)
            .await
            .is_err()
    );

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
    provider_task.abort();
    let _ = provider_task.await;
}

#[tokio::test]
async fn openai_client_health_probe_uses_internal_health_endpoint() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let (contact_tx, contact_rx) = oneshot::channel::<()>();
    let provider_task = tokio::spawn(async move {
        let _ = provider.accept().await;
        let _ = contact_tx.send(());
    });

    let server = start_openai_server_for_test_with_config(OpenAiServerConfig {
        provider_base_url: validate_provider_base_url(&format!("http://{provider_addr}/v1"))
            .unwrap(),
        bind_addr: "0.0.0.0:0".parse().unwrap(),
        allow_peers: Vec::new(),
        request_read_timeout: Duration::from_secs(2),
        provider_connect_timeout: Duration::from_secs(2),
        provider_request_timeout: Duration::from_secs(2),
        max_connections: 4,
        max_streams_per_connection: 4,
        identity_path: None,
    })
    .await
    .unwrap();
    let client = start_openai_client_for_test_with_config(
        server.ticket.clone(),
        "127.0.0.1:0".parse().unwrap(),
        OpenAiClientConfig {
            local_request_timeout: Duration::from_secs(2),
            tunnel_operation_timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(2),
            reconnect_attempts: 1,
            health_check_interval: Some(Duration::from_millis(50)),
            max_concurrent_sessions: 4,
        },
    )
    .await
    .unwrap();

    openai_health_check_for_test(&client).await.unwrap();
    assert!(
        timeout(Duration::from_millis(100), contact_rx)
            .await
            .is_err()
    );

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
    provider_task.abort();
    let _ = provider_task.await;
}

#[tokio::test]
async fn client_http_request_is_forwarded_by_server_to_provider() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let provider_task = tokio::spawn(async move {
        let (mut stream, _) = provider.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            assert_ne!(
                read, 0,
                "provider connection closed before request body arrived"
            );
            request.extend_from_slice(&buf[..read]);
            if request
                .windows(b"\r\n\r\n".len())
                .position(|window| window == b"\r\n\r\n")
                .and_then(|header_end| {
                    let headers = String::from_utf8_lossy(&request[..header_end + 4]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then_some(value.trim())
                        })?
                        .parse::<usize>()
                        .ok()?;
                    Some(request.len() >= header_end + 4 + content_length)
                })
                .unwrap_or(false)
            {
                break;
            }
        }

        let request_text = String::from_utf8_lossy(&request);
        assert!(request_text.starts_with("POST /v1/chat/completions HTTP/1.1\r\n"));
        assert!(request_text.contains("x-test-forwarded: yes\r\n"));
        assert!(request_text.contains("content-type: application/json\r\n"));
        assert!(request_text.ends_with(r#"{"model":"test-model","messages":[]}"#));

        let body = br#"{"id":"chatcmpl-test","object":"chat.completion"}"#;
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Provider-Test: yes\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        stream.write_all(body).await.unwrap();
    });

    let server = start_openai_server_for_test(format!("http://{provider_addr}"))
        .await
        .unwrap();
    let client =
        start_openai_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

    let body = br#"{"model":"test-model","messages":[]}"#;
    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\nx-test-forwarded: yes\r\ncontent-type: application/json\r\nContent-Length: {}\r\n\r\n",
                client.listen_addr,
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    stream.write_all(body).await.unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response_text = String::from_utf8_lossy(&response);
    assert!(response_text.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response_text.contains("content-type: application/json\r\n"));
    assert!(response_text.contains("x-provider-test: yes\r\n"));
    assert!(response_text.contains("transfer-encoding: chunked\r\n"));
    assert_eq!(
        chunked_body(&response),
        br#"{"id":"chatcmpl-test","object":"chat.completion"}"#
    );

    provider_task.await.unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn reqwest_client_content_length_request_completes_without_keep_alive_eof() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let provider_task = tokio::spawn(async move {
        let (mut stream, _) = provider.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            assert_ne!(
                read, 0,
                "provider connection closed before request body arrived"
            );
            request.extend_from_slice(&buf[..read]);
            if request
                .windows(b"\r\n\r\n".len())
                .position(|window| window == b"\r\n\r\n")
                .and_then(|header_end| {
                    let headers = String::from_utf8_lossy(&request[..header_end + 4]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then_some(value.trim())
                        })?
                        .parse::<usize>()
                        .ok()?;
                    Some(request.len() >= header_end + 4 + content_length)
                })
                .unwrap_or(false)
            {
                break;
            }
        }

        let request_text = String::from_utf8_lossy(&request);
        assert!(request_text.starts_with("POST /v1/chat/completions HTTP/1.1\r\n"));
        assert!(request_text.ends_with(r#"{"model":"test-model","messages":[]}"#));

        let body = br#"{"id":"chatcmpl-keepalive","object":"chat.completion"}"#;
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        stream.write_all(body).await.unwrap();
    });

    let server = start_openai_server_for_test(format!("http://{provider_addr}"))
        .await
        .unwrap();
    let client =
        start_openai_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let response_text = http
        .post(format!("http://{}/v1/chat/completions", client.listen_addr))
        .header("content-type", "application/json")
        .body(r#"{"model":"test-model","messages":[]}"#)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(
        response_text,
        r#"{"id":"chatcmpl-keepalive","object":"chat.completion"}"#
    );

    provider_task.await.unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn openai_server_rejects_disallowed_peer_before_forwarding() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let provider_task = tokio::spawn(async move {
        timeout(Duration::from_millis(500), provider.accept())
            .await
            .expect_err("provider should not receive request from disallowed peer");
    });
    let allowed_peer = SecretKey::generate().public();
    let server =
        start_openai_server_with_allow_peers(format!("http://{provider_addr}"), vec![allowed_peer])
            .await
            .unwrap();

    if let Ok(client) =
        start_openai_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap()).await
    {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let result = http
            .post(format!("http://{}/v1/chat/completions", client.listen_addr))
            .header("content-type", "application/json")
            .body(r#"{"model":"test-model","messages":[]}"#)
            .send()
            .await;
        assert!(result.is_err(), "disallowed peer request should fail");
        client.shutdown().await.unwrap();
    }

    provider_task.await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn provider_connection_failure_returns_bad_gateway_response() {
    let unused_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = unused_listener.local_addr().unwrap();
    drop(unused_listener);

    let server = start_openai_server_for_test(format!("http://{provider_addr}"))
        .await
        .unwrap();
    let client =
        start_openai_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\ncontent-type: application/json\r\nContent-Length: 0\r\n\r\n",
                client.listen_addr
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response_text = String::from_utf8_lossy(&response);
    assert!(response_text.starts_with("HTTP/1.1 502 Bad Gateway\r\n"));
    assert!(response_text.contains("content-type: text/plain\r\n"));
    assert!(response_text.ends_with("provider request failed"));

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn provider_response_body_failure_before_first_chunk_returns_bad_gateway() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let provider_task = tokio::spawn(async move {
        let (mut stream, _) = provider.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            assert_ne!(read, 0, "provider connection closed before request arrived");
            request.extend_from_slice(&buf[..read]);
            if request
                .windows(b"\r\n\r\n".len())
                .any(|window| window == b"\r\n\r\n")
            {
                break;
            }
        }
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 64\r\n\r\n",
            )
            .await
            .unwrap();
    });

    let server = start_openai_server_for_test(format!("http://{provider_addr}"))
        .await
        .unwrap();
    let client =
        start_openai_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\ncontent-type: application/json\r\nContent-Length: 0\r\n\r\n",
                client.listen_addr
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response_text = String::from_utf8_lossy(&response);
    assert!(response_text.starts_with("HTTP/1.1 502 Bad Gateway\r\n"));
    assert!(response_text.contains("content-type: text/plain\r\n"));
    assert!(response_text.ends_with("provider response failed"));

    provider_task.await.unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn chunked_client_request_is_rejected_without_contacting_provider() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let provider_task = tokio::spawn(async move {
        timeout(Duration::from_millis(500), provider.accept())
            .await
            .expect_err("provider should not receive unsupported chunked request");
    });

    let server = start_openai_server_for_test(format!("http://{provider_addr}"))
        .await
        .unwrap();
    let client =
        start_openai_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ntest\r\n0\r\n\r\n",
                client.listen_addr
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response_text = String::from_utf8_lossy(&response);
    assert!(response_text.starts_with("HTTP/1.1 501 Not Implemented\r\n"));
    assert!(response_text.contains("content-type: text/plain\r\n"));
    assert!(response_text.ends_with("chunked request bodies are not supported"));

    provider_task.await.unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_http_request_returns_bad_request_without_contacting_provider() {
    let response = openai_error_response_for_request("not-an-http-request\r\n\r\n").await;

    assert!(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
    assert!(response.contains("connection: close\r\n"));
    assert!(response.ends_with("bad request"));
}

#[tokio::test]
async fn unsupported_http_version_returns_bad_request_without_contacting_provider() {
    let response = openai_error_response_for_request(
        "POST /v1/chat/completions HTTP/2.0\r\nHost: local\r\nContent-Length: 0\r\n\r\n",
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
    assert!(response.contains("connection: close\r\n"));
    assert!(response.ends_with("bad request"));
}

#[tokio::test]
async fn pipelined_local_requests_are_rejected_without_contacting_provider() {
    let response = openai_error_response_for_request(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\n\r\nGET /v1/models HTTP/1.1\r\nHost: local\r\n\r\n",
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
    assert!(response.contains("connection: close\r\n"));
    assert!(response.ends_with("bad request"));
}

#[tokio::test]
async fn conflicting_content_length_returns_bad_request_without_contacting_provider() {
    let response = openai_error_response_for_request(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: local\r\nContent-Length: 0\r\nContent-Length: 5\r\n\r\n",
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
    assert!(response.ends_with("bad request"));
}

#[tokio::test]
async fn oversized_content_length_returns_payload_too_large_without_contacting_provider() {
    let response = openai_error_response_for_request(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: local\r\nContent-Length: 1048577\r\n\r\n",
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 413 Payload Too Large\r\n"));
    assert!(response.ends_with("payload too large"));
}

#[tokio::test]
async fn forwarded_responses_advertise_connection_close() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let provider_task = tokio::spawn(async move {
        let (mut stream, _) = provider.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            assert_ne!(read, 0, "provider connection closed before request arrived");
            request.extend_from_slice(&buf[..read]);
            if request
                .windows(b"\r\n\r\n".len())
                .any(|window| window == b"\r\n\r\n")
            {
                break;
            }
        }
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
            )
            .await
            .unwrap();
    });

    let server = start_openai_server_for_test(format!("http://{provider_addr}"))
        .await
        .unwrap();
    let client =
        start_openai_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\nContent-Length: 0\r\n\r\n",
                client.listen_addr
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response_text = String::from_utf8_lossy(&response);
    assert!(response_text.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response_text.contains("connection: close\r\n"));

    provider_task.await.unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn streaming_provider_response_reaches_client_before_provider_finishes() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let (first_sent_tx, first_sent_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let provider_task = tokio::spawn(async move {
        let (mut stream, _) = provider.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            assert_ne!(read, 0, "provider connection closed before request arrived");
            request.extend_from_slice(&buf[..read]);
            if request
                .windows(b"\r\n\r\n".len())
                .any(|window| window == b"\r\n\r\n")
            {
                break;
            }
        }

        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
            .await
            .unwrap();
        stream.write_all(b"data: first\n\n").await.unwrap();
        first_sent_tx.send(()).unwrap();
        finish_rx.await.unwrap();
        stream.write_all(b"data: [DONE]\n\n").await.unwrap();
    });

    let server = start_openai_server_for_test(format!("http://{provider_addr}"))
        .await
        .unwrap();
    let client =
        start_openai_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\ncontent-type: application/json\r\nContent-Length: 0\r\n\r\n",
                client.listen_addr
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    first_sent_rx.await.unwrap();
    let mut response = Vec::new();
    let mut buf = [0u8; 64];
    timeout(Duration::from_secs(2), async {
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            assert_ne!(
                read, 0,
                "client connection closed before first event arrived"
            );
            response.extend_from_slice(&buf[..read]);
            if response
                .windows(b"data: first\n\n".len())
                .any(|window| window == b"data: first\n\n")
            {
                break;
            }
        }
    })
    .await
    .unwrap();

    finish_tx.send(()).unwrap();
    provider_task.await.unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn provider_response_body_stream_can_exceed_provider_request_timeout() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let provider_task = tokio::spawn(async move {
        let (mut stream, _) = provider.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            assert_ne!(read, 0, "provider connection closed before request arrived");
            request.extend_from_slice(&buf[..read]);
            if request
                .windows(b"\r\n\r\n".len())
                .any(|window| window == b"\r\n\r\n")
            {
                break;
            }
        }

        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
            .await
            .unwrap();
        stream.write_all(b"data: first\n\n").await.unwrap();
        sleep(Duration::from_millis(150)).await;
        stream.write_all(b"data: [DONE]\n\n").await.unwrap();
    });

    let server = start_openai_server_for_test_with_config(OpenAiServerConfig {
        provider_base_url: validate_provider_base_url(&format!("http://{provider_addr}")).unwrap(),
        bind_addr: "0.0.0.0:0".parse().unwrap(),
        allow_peers: Vec::new(),
        request_read_timeout: Duration::from_secs(2),
        provider_connect_timeout: Duration::from_secs(1),
        provider_request_timeout: Duration::from_millis(50),
        max_connections: 128,
        max_streams_per_connection: 128,
        identity_path: None,
    })
    .await
    .unwrap();
    let client =
        start_openai_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\ncontent-type: application/json\r\nContent-Length: 0\r\n\r\n",
                client.listen_addr
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response_text = String::from_utf8_lossy(&response);
    assert!(response_text.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response_text.contains("data: first"));
    assert!(response_text.contains("data: [DONE]"));
    assert!(!response_text.contains("502 Bad Gateway"));
    assert!(!response_text.contains("504 Gateway Timeout"));

    provider_task.await.unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn openai_client_rejects_local_http_sessions_over_limit() {
    let server = start_openai_server_for_test("http://127.0.0.1:9".to_string())
        .await
        .unwrap();
    let client = start_openai_client_for_test_with_config(
        server.ticket.clone(),
        "127.0.0.1:0".parse().unwrap(),
        OpenAiClientConfig {
            local_request_timeout: Duration::from_secs(2),
            tunnel_operation_timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(2),
            reconnect_attempts: 1,
            health_check_interval: None,
            max_concurrent_sessions: 1,
        },
    )
    .await
    .unwrap();

    let _held = TcpStream::connect(client.listen_addr).await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let mut rejected = TcpStream::connect(client.listen_addr).await.unwrap();
    let mut buf = [0u8; 1];
    let read = timeout(Duration::from_secs(2), rejected.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read, 0);

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn openai_client_tunnel_response_timeout_returns_gateway_timeout() {
    let server_endpoint = start_server_endpoint().await.unwrap();
    let ticket = endpoint_ticket_string(&server_endpoint).await.unwrap();
    let server_task = tokio::spawn({
        let server_endpoint = server_endpoint.clone();
        async move {
            let incoming = server_endpoint.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            let (_send, mut recv) = conn.accept_bi().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = recv.read(&mut buf).await;
            sleep(Duration::from_secs(2)).await;
        }
    });

    let client = start_openai_client_for_test_with_config(
        ticket,
        "127.0.0.1:0".parse().unwrap(),
        OpenAiClientConfig {
            local_request_timeout: Duration::from_secs(2),
            tunnel_operation_timeout: Duration::from_millis(50),
            connect_timeout: Duration::from_secs(2),
            reconnect_attempts: 1,
            health_check_interval: None,
            max_concurrent_sessions: 4,
        },
    )
    .await
    .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\nContent-Length: 0\r\n\r\n",
                client.listen_addr
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response_text = String::from_utf8_lossy(&response);
    assert!(response_text.starts_with("HTTP/1.1 504 Gateway Timeout\r\n"));
    assert!(response_text.ends_with("gateway timeout"));

    client.shutdown().await.unwrap();
    server_endpoint.close().await;
    let _ = server_task.await;
}

#[tokio::test]
async fn openai_client_response_timeout_after_partial_response_closes_without_second_http_response()
{
    let server_endpoint = start_server_endpoint().await.unwrap();
    let ticket = endpoint_ticket_string(&server_endpoint).await.unwrap();
    let server_task = tokio::spawn({
        let server_endpoint = server_endpoint.clone();
        async move {
            let incoming = server_endpoint.accept().await.unwrap();
            let conn = incoming.await.unwrap();
            let (mut send, mut recv) = conn.accept_bi().await.unwrap();
            let mut buf = [0u8; 64];
            let _ = recv.read(&mut buf).await;
            send.write_all(
                b"HTTP/1.1 200 OK\r\nconnection: close\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n",
            )
            .await
            .unwrap();
            sleep(Duration::from_secs(2)).await;
        }
    });

    let client = start_openai_client_for_test_with_config(
        ticket,
        "127.0.0.1:0".parse().unwrap(),
        OpenAiClientConfig {
            local_request_timeout: Duration::from_secs(2),
            tunnel_operation_timeout: Duration::from_millis(50),
            connect_timeout: Duration::from_secs(2),
            reconnect_attempts: 1,
            health_check_interval: None,
            max_concurrent_sessions: 4,
        },
    )
    .await
    .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\nContent-Length: 0\r\n\r\n",
                client.listen_addr
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response_text = String::from_utf8_lossy(&response);
    assert!(response_text.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response_text.contains("hello"));
    assert!(!response_text.contains("504 Gateway Timeout"));
    assert_eq!(response_text.matches("HTTP/1.1 ").count(), 1);

    client.shutdown().await.unwrap();
    server_endpoint.close().await;
    server_task.abort();
    let _ = server_task.await;
}

#[tokio::test]
async fn openai_server_rejects_connections_over_limit() {
    let config = OpenAiServerConfig {
        provider_base_url: validate_provider_base_url("http://127.0.0.1:9").unwrap(),
        bind_addr: "0.0.0.0:0".parse().unwrap(),
        allow_peers: Vec::new(),
        request_read_timeout: Duration::from_secs(2),
        provider_connect_timeout: Duration::from_secs(1),
        provider_request_timeout: Duration::from_secs(1),
        max_connections: 1,
        max_streams_per_connection: 128,
        identity_path: None,
    };
    let server = start_openai_server_for_test_with_config(config)
        .await
        .unwrap();
    let first_endpoint = start_client_endpoint().await.unwrap();
    let second_endpoint = start_client_endpoint().await.unwrap();
    let server_addr = parse_server_ticket(&server.ticket).unwrap();
    let first_conn = first_endpoint
        .connect(server_addr.clone(), tunnel_ai::ALPN)
        .await
        .unwrap();
    let (_first_send, _first_recv) = first_conn.open_bi().await.unwrap();
    sleep(Duration::from_millis(50)).await;

    if let Ok(second_conn) = second_endpoint.connect(server_addr, tunnel_ai::ALPN).await {
        let result = timeout(Duration::from_secs(2), second_conn.open_bi())
            .await
            .unwrap();
        assert!(
            result.is_err(),
            "second connection should not open streams over connection limit"
        );
        second_conn.close(0u32.into(), b"test done");
    }

    first_conn.close(0u32.into(), b"test done");
    first_endpoint.close().await;
    second_endpoint.close().await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn openai_server_rejects_streams_over_limit() {
    let config = OpenAiServerConfig {
        provider_base_url: validate_provider_base_url("http://127.0.0.1:9").unwrap(),
        bind_addr: "0.0.0.0:0".parse().unwrap(),
        allow_peers: Vec::new(),
        request_read_timeout: Duration::from_secs(2),
        provider_connect_timeout: Duration::from_secs(1),
        provider_request_timeout: Duration::from_secs(1),
        max_connections: 128,
        max_streams_per_connection: 1,
        identity_path: None,
    };
    let server = start_openai_server_for_test_with_config(config)
        .await
        .unwrap();
    let client_endpoint = start_client_endpoint().await.unwrap();
    let server_addr = parse_server_ticket(&server.ticket).unwrap();
    let conn = client_endpoint
        .connect(server_addr, tunnel_ai::ALPN)
        .await
        .unwrap();

    let (mut first_send, _first_recv) = conn.open_bi().await.unwrap();
    first_send.write_all(b"P").await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let (mut second_send, mut second_recv) = conn.open_bi().await.unwrap();
    second_send.write_all(b"P").await.unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(2), async {
        let mut buf = [0u8; 64];
        while let Some(read) = second_recv.read(&mut buf).await.unwrap() {
            response.extend_from_slice(&buf[..read]);
        }
    })
    .await
    .unwrap();
    let response_text = String::from_utf8_lossy(&response);
    assert!(response_text.starts_with("HTTP/1.1 503 Service Unavailable\r\n"));
    assert!(response_text.ends_with("too many open streams"));

    conn.close(0u32.into(), b"test done");
    client_endpoint.close().await;
    server.shutdown().await.unwrap();
}

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep, timeout};
use tunnel_ai::client::{
    ClientConfig, start_client_for_test_with_config, start_client_for_test_with_handshake_timeout,
};
use tunnel_ai::iroh_endpoint::{
    endpoint_ticket_string, parse_server_ticket, start_client_endpoint, start_server_endpoint,
};
use tunnel_ai::server::{
    ServerConfig, start_server_for_test, start_server_for_test_with_config,
    start_server_for_test_with_request_timeout,
};

#[tokio::test]
async fn socks_handshake_timeout_closes_idle_client() {
    let server = start_server_for_test(true).await.unwrap();
    let client = start_client_for_test_with_handshake_timeout(
        server.ticket.clone(),
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_millis(50),
    )
    .await
    .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    let mut buf = [0u8; 1];
    let read = timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read, 0);

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_stops_listener_and_closes_endpoint() {
    let server = start_server_for_test(true).await.unwrap();
    let client = start_client_for_test_with_handshake_timeout(
        server.ticket.clone(),
        "127.0.0.1:0".parse().unwrap(),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let listen_addr = client.listen_addr;

    client.shutdown().await.unwrap();
    let rebound = TcpListener::bind(listen_addr).await.unwrap();
    drop(rebound);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn server_tunnel_request_timeout_closes_idle_stream() {
    let server = start_server_for_test_with_request_timeout(true, Duration::from_millis(50))
        .await
        .unwrap();
    let client_endpoint = start_client_endpoint().await.unwrap();
    let server_addr = parse_server_ticket(&server.ticket).unwrap();
    let conn = client_endpoint
        .connect(server_addr, tunnel_ai::ALPN)
        .await
        .unwrap();

    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    send.write_all(b"I").await.unwrap();
    let mut buf = [0u8; 1];
    let read = timeout(Duration::from_secs(2), recv.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert!(read.is_none() || read == Some(0));

    conn.close(0u32.into(), b"test done");
    client_endpoint.close().await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn client_rejects_local_socks_sessions_over_limit() {
    let server = start_server_for_test(true).await.unwrap();
    let client = start_client_for_test_with_config(
        server.ticket.clone(),
        "127.0.0.1:0".parse().unwrap(),
        ClientConfig {
            socks_handshake_timeout: Duration::from_secs(2),
            tunnel_operation_timeout: Duration::from_secs(2),
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
async fn client_tunnel_response_timeout_returns_socks_failure() {
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

    let client = start_client_for_test_with_config(
        ticket,
        "127.0.0.1:0".parse().unwrap(),
        ClientConfig {
            socks_handshake_timeout: Duration::from_secs(2),
            tunnel_operation_timeout: Duration::from_millis(50),
            max_concurrent_sessions: 4,
        },
    )
    .await
    .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
    let mut method = [0u8; 2];
    stream.read_exact(&mut method).await.unwrap();
    assert_eq!(method, [0x05, 0x00]);

    stream
        .write_all(b"\x05\x01\x00\x03\x0bexample.com\x00\x50")
        .await
        .unwrap();
    let mut reply = [0u8; 10];
    timeout(Duration::from_secs(2), stream.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply[0], 0x05);
    assert_eq!(reply[1], 0x06);

    client.shutdown().await.unwrap();
    server_endpoint.close().await;
    let _ = server_task.await;
}

#[tokio::test]
async fn server_rejects_tunnel_streams_over_limit() {
    let mut config = ServerConfig::new(true, Vec::new());
    config.max_streams_per_connection = 1;
    config.tunnel_request_timeout = Duration::from_secs(2);
    let server = start_server_for_test_with_config(config).await.unwrap();
    let client_endpoint = start_client_endpoint().await.unwrap();
    let server_addr = parse_server_ticket(&server.ticket).unwrap();
    let conn = client_endpoint
        .connect(server_addr, tunnel_ai::ALPN)
        .await
        .unwrap();

    let (mut first_send, _first_recv) = conn.open_bi().await.unwrap();
    first_send.write_all(b"I").await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let (mut second_send, mut second_recv) = conn.open_bi().await.unwrap();
    second_send.write_all(b"I").await.unwrap();
    let mut reply = [0u8; 8];
    timeout(Duration::from_secs(2), second_recv.read_exact(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&reply[..4], b"IRPX");
    assert_eq!(reply[4], 0x01);
    assert_eq!(reply[5], 0x01);
    assert_eq!(&reply[6..], &[0x00, 0x00]);

    conn.close(0u32.into(), b"test done");
    client_endpoint.close().await;
    server.shutdown().await.unwrap();
}

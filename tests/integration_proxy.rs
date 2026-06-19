use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{Duration, timeout};
use tunnel_ai::client::start_client_for_test;
use tunnel_ai::iroh_endpoint::{parse_server_ticket, start_client_endpoint};
use tunnel_ai::server::start_server_for_test;
use tunnel_ai::socks5::{ConnectRequest, ReplyCode, TargetAddr};
use tunnel_ai::tunnel::{read_response, write_request};

async fn start_echo_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let (mut read, mut write) = stream.split();
                let _ = tokio::io::copy(&mut read, &mut write).await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn server_stream_connects_to_echo_target() {
    let echo_addr = start_echo_server().await;
    let server = start_server_for_test(true).await.unwrap();
    let client_endpoint = start_client_endpoint().await.unwrap();
    let server_addr = parse_server_ticket(&server.ticket).unwrap();
    let conn = client_endpoint
        .connect(server_addr, tunnel_ai::ALPN)
        .await
        .unwrap();

    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    let request = ConnectRequest {
        target: TargetAddr::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        port: echo_addr.port(),
    };
    write_request(&mut send, &request).await.unwrap();
    assert_eq!(
        read_response(&mut recv).await.unwrap(),
        ReplyCode::Succeeded
    );

    send.write_all(b"ping").await.unwrap();
    send.finish().unwrap();
    let response = timeout(Duration::from_secs(5), recv.read_to_end(16))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(response, b"ping");
    conn.close(0u32.into(), b"test done");
    client_endpoint.close().await;
    server.shutdown().await.unwrap();
}

async fn socks_connect(proxy_addr: SocketAddr, target: SocketAddr) -> [u8; 10] {
    let mut stream = tokio::net::TcpStream::connect(proxy_addr).await.unwrap();
    stream.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
    let mut method = [0u8; 2];
    stream.read_exact(&mut method).await.unwrap();
    assert_eq!(method, [0x05, 0x00]);

    let mut request = vec![0x05, 0x01, 0x00, 0x01];
    match target.ip() {
        IpAddr::V4(ip) => request.extend_from_slice(&ip.octets()),
        IpAddr::V6(_) => unreachable!("test helper only supports IPv4"),
    }
    request.extend_from_slice(&target.port().to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut reply = [0u8; 10];
    stream.read_exact(&mut reply).await.unwrap();
    reply
}

async fn socks_connect_domain(proxy_addr: SocketAddr, domain: &str, port: u16) -> [u8; 10] {
    let mut stream = tokio::net::TcpStream::connect(proxy_addr).await.unwrap();
    stream.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
    let mut method = [0u8; 2];
    stream.read_exact(&mut method).await.unwrap();
    assert_eq!(method, [0x05, 0x00]);

    let mut request = vec![0x05, 0x01, 0x00, 0x03, domain.len() as u8];
    request.extend_from_slice(domain.as_bytes());
    request.extend_from_slice(&port.to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut reply = [0u8; 10];
    stream.read_exact(&mut reply).await.unwrap();
    reply
}

#[tokio::test]
async fn client_maps_tunnel_failure_to_socks_reply() {
    let server = start_server_for_test(false).await.unwrap();
    let client = start_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();

    let reply = socks_connect(client.listen_addr, "127.0.0.1:9".parse().unwrap()).await;
    assert_eq!(reply, [0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0]);

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn server_denies_localhost_domain_when_private_targets_disabled() {
    let echo_addr = start_echo_server().await;
    let server = start_server_for_test(false).await.unwrap();
    let client = start_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();

    let reply = socks_connect_domain(client.listen_addr, "localhost", echo_addr.port()).await;
    assert_eq!(reply, [0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0]);

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn client_bridges_after_success_reply() {
    let echo_addr = start_echo_server().await;
    let server = start_server_for_test(true).await.unwrap();
    let client = start_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();

    let mut stream = tokio::net::TcpStream::connect(client.listen_addr)
        .await
        .unwrap();
    stream.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
    let mut method = [0u8; 2];
    stream.read_exact(&mut method).await.unwrap();
    assert_eq!(method, [0x05, 0x00]);

    let mut request = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
    request.extend_from_slice(&echo_addr.port().to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut reply = [0u8; 10];
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[1], 0x00);

    stream.write_all(b"ping").await.unwrap();
    let mut got = [0u8; 4];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"ping");

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn proxies_echo_server_through_socks5_and_iroh() {
    let echo_addr = start_echo_server().await;
    let server = start_server_for_test(true).await.unwrap();
    let client = start_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();

    let mut stream = tokio::net::TcpStream::connect(client.listen_addr)
        .await
        .unwrap();
    stream.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
    let mut method = [0u8; 2];
    stream.read_exact(&mut method).await.unwrap();
    assert_eq!(method, [0x05, 0x00]);

    let mut request = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
    request.extend_from_slice(&echo_addr.port().to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut reply = [0u8; 10];
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0]);

    stream.write_all(b"ping").await.unwrap();
    let mut got = [0u8; 4];
    stream.read_exact(&mut got).await.unwrap();
    assert_eq!(&got, b"ping");

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
}

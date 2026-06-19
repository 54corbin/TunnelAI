use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tunnel_ai::socks5::{
    ConnectRequest, ReplyCode, TargetAddr, negotiate_no_auth, read_connect_request, write_reply,
};

#[tokio::test]
async fn method_negotiation_selects_no_auth_when_offered() {
    let (mut client, mut server) = duplex(64);
    let server_task = tokio::spawn(async move { negotiate_no_auth(&mut server).await });

    client.write_all(&[0x05, 0x02, 0x02, 0x00]).await.unwrap();
    let mut response = [0u8; 2];
    client.read_exact(&mut response).await.unwrap();

    assert_eq!(response, [0x05, 0x00]);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn method_negotiation_rejects_when_no_supported_method() {
    let (mut client, mut server) = duplex(64);
    let server_task = tokio::spawn(async move { negotiate_no_auth(&mut server).await });

    client.write_all(&[0x05, 0x01, 0x02]).await.unwrap();
    let mut response = [0u8; 2];
    client.read_exact(&mut response).await.unwrap();

    assert_eq!(response, [0x05, 0xff]);
    assert!(server_task.await.unwrap().is_err());
}

#[tokio::test]
async fn connect_domain_request_parses_target_and_port() {
    let (mut client, mut server) = duplex(64);
    let server_task = tokio::spawn(async move { read_connect_request(&mut server).await });

    let mut request = vec![0x05, 0x01, 0x00, 0x03, 11];
    request.extend_from_slice(b"example.com");
    request.extend_from_slice(&443u16.to_be_bytes());
    client.write_all(&request).await.unwrap();

    assert_eq!(
        server_task.await.unwrap().unwrap(),
        ConnectRequest {
            target: TargetAddr::Domain("example.com".into()),
            port: 443,
        }
    );
}

#[tokio::test]
async fn connect_ipv4_request_parses_target_and_port() {
    let (mut client, mut server) = duplex(64);
    let server_task = tokio::spawn(async move { read_connect_request(&mut server).await });

    client
        .write_all(&[0x05, 0x01, 0x00, 0x01, 192, 0, 2, 1, 0x1f, 0x90])
        .await
        .unwrap();

    assert_eq!(
        server_task.await.unwrap().unwrap(),
        ConnectRequest {
            target: TargetAddr::Ip(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))),
            port: 8080,
        }
    );
}

#[tokio::test]
async fn connect_ipv6_request_parses_target_and_port() {
    let (mut client, mut server) = duplex(128);
    let server_task = tokio::spawn(async move { read_connect_request(&mut server).await });

    let ip = Ipv6Addr::LOCALHOST.octets();
    let mut request = vec![0x05, 0x01, 0x00, 0x04];
    request.extend_from_slice(&ip);
    request.extend_from_slice(&1080u16.to_be_bytes());
    client.write_all(&request).await.unwrap();

    assert_eq!(
        server_task.await.unwrap().unwrap(),
        ConnectRequest {
            target: TargetAddr::Ip(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            port: 1080,
        }
    );
}

#[tokio::test]
async fn unsupported_command_returns_error() {
    let (mut client, mut server) = duplex(64);
    let server_task = tokio::spawn(async move { read_connect_request(&mut server).await });

    client
        .write_all(&[0x05, 0x02, 0x00, 0x01, 127, 0, 0, 1, 0, 80])
        .await
        .unwrap();

    let err = server_task.await.unwrap().unwrap_err();
    assert_eq!(err.reply_code(), ReplyCode::CommandNotSupported);
}

#[tokio::test]
async fn unsupported_address_type_returns_error() {
    let (mut client, mut server) = duplex(64);
    let server_task = tokio::spawn(async move { read_connect_request(&mut server).await });

    client.write_all(&[0x05, 0x01, 0x00, 0x09]).await.unwrap();

    let err = server_task.await.unwrap().unwrap_err();
    assert_eq!(err.reply_code(), ReplyCode::AddressTypeNotSupported);
}

#[tokio::test]
async fn nonzero_reserved_byte_returns_error() {
    let (mut client, mut server) = duplex(64);
    let server_task = tokio::spawn(async move { read_connect_request(&mut server).await });

    client
        .write_all(&[0x05, 0x01, 0x01, 0x01, 127, 0, 0, 1, 0, 80])
        .await
        .unwrap();

    let err = server_task.await.unwrap().unwrap_err();
    assert_eq!(err.reply_code(), ReplyCode::GeneralFailure);
    assert!(err.to_string().contains("reserved"));
}

#[tokio::test]
async fn invalid_domain_names_return_error() {
    for domain in ["", " bad.com", "bad.com\nnext", "bad\0host"] {
        let (mut client, mut server) = duplex(64);
        let server_task = tokio::spawn(async move { read_connect_request(&mut server).await });

        let mut request = vec![0x05, 0x01, 0x00, 0x03, domain.len() as u8];
        request.extend_from_slice(domain.as_bytes());
        request.extend_from_slice(&443u16.to_be_bytes());
        client.write_all(&request).await.unwrap();

        let err = server_task.await.unwrap().unwrap_err();
        assert_eq!(err.reply_code(), ReplyCode::GeneralFailure);
        assert!(err.to_string().contains("domain"));
    }
}

#[tokio::test]
async fn writes_socks5_reply_with_unspecified_bind_address() {
    let (mut client, mut server) = duplex(64);
    let server_task =
        tokio::spawn(async move { write_reply(&mut server, ReplyCode::ConnectionRefused).await });

    let mut response = [0u8; 10];
    client.read_exact(&mut response).await.unwrap();

    assert_eq!(response, [0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0]);
    server_task.await.unwrap().unwrap();
}

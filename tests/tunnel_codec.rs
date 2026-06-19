use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tokio::io::{AsyncWriteExt, duplex};
use tunnel_ai::socks5::{ConnectRequest, ReplyCode, TargetAddr};
use tunnel_ai::tunnel::{read_request, read_response, write_request, write_response};

async fn roundtrip_request(request: ConnectRequest) -> ConnectRequest {
    let (mut writer, mut reader) = duplex(128);
    let write_req = request.clone();
    let writer_task = tokio::spawn(async move { write_request(&mut writer, &write_req).await });
    let parsed = read_request(&mut reader).await.unwrap();
    writer_task.await.unwrap().unwrap();
    parsed
}

#[tokio::test]
async fn domain_request_round_trips() {
    let request = ConnectRequest {
        target: TargetAddr::Domain("example.com".into()),
        port: 443,
    };
    assert_eq!(roundtrip_request(request.clone()).await, request);
}

#[tokio::test]
async fn ipv4_request_round_trips() {
    let request = ConnectRequest {
        target: TargetAddr::Ip(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10))),
        port: 8080,
    };
    assert_eq!(roundtrip_request(request.clone()).await, request);
}

#[tokio::test]
async fn ipv6_request_round_trips() {
    let request = ConnectRequest {
        target: TargetAddr::Ip(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        port: 1080,
    };
    assert_eq!(roundtrip_request(request.clone()).await, request);
}

#[tokio::test]
async fn bad_magic_is_rejected() {
    let (mut writer, mut reader) = duplex(64);
    writer
        .write_all(b"NOPE\x01\x01\x03\x00\x50\x00")
        .await
        .unwrap();
    let err = read_request(&mut reader).await.unwrap_err();
    assert!(err.to_string().contains("magic"));
}

#[tokio::test]
async fn bad_version_is_rejected() {
    let (mut writer, mut reader) = duplex(64);
    writer
        .write_all(b"IRPX\x02\x01\x03\x00\x50\x00")
        .await
        .unwrap();
    let err = read_request(&mut reader).await.unwrap_err();
    assert!(err.to_string().contains("version"));
}

#[tokio::test]
async fn domain_longer_than_255_is_rejected_before_encoding() {
    let (mut writer, _reader) = duplex(512);
    let request = ConnectRequest {
        target: TargetAddr::Domain("a".repeat(256)),
        port: 443,
    };
    let err = write_request(&mut writer, &request).await.unwrap_err();
    assert!(err.to_string().contains("domain"));
}

#[tokio::test]
async fn invalid_domain_names_are_rejected_before_encoding() {
    for domain in ["", " bad.com", "bad.com\nnext", "bad\0host"] {
        let (mut writer, _reader) = duplex(512);
        let request = ConnectRequest {
            target: TargetAddr::Domain(domain.into()),
            port: 443,
        };
        let err = write_request(&mut writer, &request).await.unwrap_err();
        assert!(err.to_string().contains("domain"));
    }
}

#[tokio::test]
async fn invalid_domain_names_are_rejected_when_decoding() {
    for domain in ["", " bad.com", "bad.com\nnext", "bad\0host"] {
        let (mut writer, mut reader) = duplex(128);
        let mut request = b"IRPX\x01\x01\x03\x01\xbb".to_vec();
        request.push(domain.len() as u8);
        request.extend_from_slice(domain.as_bytes());
        writer.write_all(&request).await.unwrap();

        let err = read_request(&mut reader).await.unwrap_err();
        assert!(err.to_string().contains("domain"));
    }
}

#[tokio::test]
async fn response_ok_round_trips() {
    let (mut writer, mut reader) = duplex(32);
    let writer_task =
        tokio::spawn(async move { write_response(&mut writer, ReplyCode::Succeeded).await });
    let status = read_response(&mut reader).await.unwrap();
    writer_task.await.unwrap().unwrap();
    assert_eq!(status, ReplyCode::Succeeded);
}

#[tokio::test]
async fn response_status_maps_to_socks5_reply_codes() {
    for code in [
        ReplyCode::GeneralFailure,
        ReplyCode::ConnectionNotAllowed,
        ReplyCode::NetworkUnreachable,
        ReplyCode::HostUnreachable,
        ReplyCode::ConnectionRefused,
        ReplyCode::TtlExpired,
        ReplyCode::CommandNotSupported,
        ReplyCode::AddressTypeNotSupported,
    ] {
        let (mut writer, mut reader) = duplex(32);
        let writer_task = tokio::spawn(async move { write_response(&mut writer, code).await });
        assert_eq!(read_response(&mut reader).await.unwrap(), code);
        writer_task.await.unwrap().unwrap();
    }
}

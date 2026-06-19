use iroh::SecretKey;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use tunnel_ai::server::{parse_allow_peers, peer_allowed, target_allowed};
use tunnel_ai::socks5::{ConnectRequest, TargetAddr};

fn req(target: TargetAddr) -> ConnectRequest {
    ConnectRequest { target, port: 80 }
}

#[test]
fn target_policy_allows_domains_by_default() {
    assert!(target_allowed(
        &req(TargetAddr::Domain("example.com".into())),
        false
    ));
}

#[test]
fn target_policy_allows_public_ips_by_default() {
    assert!(target_allowed(
        &req(TargetAddr::Ip(IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)))),
        false
    ));
    assert!(target_allowed(
        &req(TargetAddr::Ip(IpAddr::V6(
            "2606:2800:220:1:248:1893:25c8:1946".parse().unwrap()
        ))),
        false
    ));
}

#[test]
fn target_policy_denies_private_and_special_ips_by_default() {
    for ip in [
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)),
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)),
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    ] {
        assert!(
            !target_allowed(&req(TargetAddr::Ip(ip)), false),
            "{ip} should be denied"
        );
    }
}

#[test]
fn target_policy_denies_ipv4_mapped_private_ipv6_by_default() {
    let mapped_loopback: Ipv6Addr = "::ffff:127.0.0.1".parse().unwrap();
    let mapped_private: Ipv6Addr = "::ffff:10.0.0.1".parse().unwrap();

    assert!(!target_allowed(
        &req(TargetAddr::Ip(IpAddr::V6(mapped_loopback))),
        false
    ));
    assert!(!target_allowed(
        &req(TargetAddr::Ip(IpAddr::V6(mapped_private))),
        false
    ));
}

#[test]
fn target_policy_allows_private_ips_when_enabled() {
    assert!(target_allowed(
        &req(TargetAddr::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST))),
        true
    ));
}

#[test]
fn peer_allow_list_allows_everyone_when_empty() {
    let peer = SecretKey::generate().public();
    assert!(peer_allowed(&peer, &[]));
}

#[test]
fn peer_allow_list_accepts_only_configured_peers() {
    let allowed = SecretKey::generate().public();
    let denied = SecretKey::generate().public();
    let parsed = parse_allow_peers(&[allowed.to_string()]).unwrap();
    assert!(peer_allowed(&allowed, &parsed));
    assert!(!peer_allowed(&denied, &parsed));
}

#[test]
fn peer_allow_list_rejects_invalid_peer_text() {
    assert!(parse_allow_peers(&["not-a-peer".to_string()]).is_err());
}

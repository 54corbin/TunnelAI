use tunnel_ai::route_logging::{PathKind, PathSummary, classify_path_kind, selected_path};

#[test]
fn classifies_iroh_path_kind_for_logs() {
    assert_eq!(classify_path_kind(true, false), PathKind::Relay);
    assert_eq!(classify_path_kind(false, true), PathKind::DirectIp);
    assert_eq!(classify_path_kind(false, false), PathKind::CustomOrUnknown);
    assert_eq!(classify_path_kind(true, true), PathKind::Relay);
}

#[test]
fn path_kind_display_is_log_stable() {
    assert_eq!(PathKind::Relay.to_string(), "relay");
    assert_eq!(PathKind::DirectIp.to_string(), "direct_ip");
    assert_eq!(PathKind::CustomOrUnknown.to_string(), "custom_or_unknown");
}

#[test]
fn selected_path_returns_selected_path_for_connection_route_logs() {
    let paths = vec![
        PathSummary {
            path_id: "1".into(),
            remote_addr: "relay://example".into(),
            local_addr: "udp/0.0.0.0:12345".into(),
            kind: PathKind::Relay,
            selected: false,
            rtt_ms: 25,
        },
        PathSummary {
            path_id: "2".into(),
            remote_addr: "203.0.113.10:443".into(),
            local_addr: "192.0.2.10:54321".into(),
            kind: PathKind::DirectIp,
            selected: true,
            rtt_ms: 8,
        },
    ];

    assert_eq!(selected_path(&paths).unwrap().path_id, "2");
    assert_eq!(selected_path(&paths).unwrap().kind, PathKind::DirectIp);
}

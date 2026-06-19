use tunnel_ai::iroh_endpoint::{
    endpoint_ticket_string, parse_server_ticket, start_server_endpoint,
    start_server_endpoint_with_optional_identity,
};

#[test]
fn parse_server_ticket_rejects_invalid_text() {
    let err = parse_server_ticket("not-a-ticket").unwrap_err();
    assert!(err.to_string().contains("ticket") || err.to_string().contains("invalid"));
}

#[tokio::test]
async fn endpoint_ticket_round_trips() {
    let endpoint = start_server_endpoint().await.unwrap();
    let ticket = endpoint_ticket_string(&endpoint).await.unwrap();
    let parsed = parse_server_ticket(&ticket).unwrap();
    assert_eq!(parsed.id, endpoint.id());
    endpoint.close().await;
}

#[tokio::test]
async fn persistent_server_identity_reuses_endpoint_id() {
    let temp_dir = tempfile::tempdir().unwrap();
    let identity_path = temp_dir.path().join("openai-server.key");
    let bind_addr = "0.0.0.0:0".parse().unwrap();

    let first = start_server_endpoint_with_optional_identity(bind_addr, Some(&identity_path))
        .await
        .unwrap();
    let first_id = first.id();
    first.close().await;

    let second = start_server_endpoint_with_optional_identity(bind_addr, Some(&identity_path))
        .await
        .unwrap();
    let second_id = second.id();
    second.close().await;

    assert_eq!(first_id, second_id);
}

#[tokio::test]
async fn persistent_server_identity_creates_missing_parent_directory() {
    let temp_dir = tempfile::tempdir().unwrap();
    let identity_path = temp_dir.path().join("nested").join("openai-server.key");

    let endpoint = start_server_endpoint_with_optional_identity(
        "0.0.0.0:0".parse().unwrap(),
        Some(&identity_path),
    )
    .await
    .unwrap();
    endpoint.close().await;

    assert!(identity_path.exists());
}

#[tokio::test]
async fn malformed_persistent_server_identity_is_rejected() {
    let temp_dir = tempfile::tempdir().unwrap();
    let identity_path = temp_dir.path().join("openai-server.key");
    std::fs::write(&identity_path, "not-a-valid-iroh-secret").unwrap();

    let error = start_server_endpoint_with_optional_identity(
        "0.0.0.0:0".parse().unwrap(),
        Some(&identity_path),
    )
    .await
    .expect_err("malformed identity should be rejected");
    let message = error.to_string();
    assert!(
        message.contains("identity") || message.contains("secret"),
        "unexpected error: {message:#}"
    );
}

use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
use tokio::time::{Duration, timeout};
use tunnel_ai::copy::pump_bidirectional;

#[tokio::test]
async fn pumps_bytes_in_both_directions_and_reports_counts() {
    let (mut left_peer, left_pump) = duplex(128);
    let (right_pump, mut right_peer) = duplex(128);
    let (mut reverse_peer, reverse_pump) = duplex(128);
    let (left_reverse_pump, mut left_reverse_peer) = duplex(128);

    let pump = tokio::spawn(async move {
        pump_bidirectional(left_pump, left_reverse_pump, reverse_pump, right_pump).await
    });

    left_peer.write_all(b"left->right").await.unwrap();
    left_peer.shutdown().await.unwrap();
    reverse_peer.write_all(b"right->left").await.unwrap();
    reverse_peer.shutdown().await.unwrap();

    let mut right_received = Vec::new();
    right_peer.read_to_end(&mut right_received).await.unwrap();
    let mut left_received = Vec::new();
    left_reverse_peer
        .read_to_end(&mut left_received)
        .await
        .unwrap();

    let counts = timeout(Duration::from_secs(2), pump)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(right_received, b"left->right");
    assert_eq!(left_received, b"right->left");
    assert_eq!(counts, (11, 11));
}

#[tokio::test]
async fn shuts_down_write_halves_after_eof() {
    let (mut left_peer, left_pump) = duplex(64);
    let (right_pump, mut right_peer) = duplex(64);
    let (mut reverse_peer, reverse_pump) = duplex(64);
    let (left_reverse_pump, mut left_reverse_peer) = duplex(64);

    let pump = tokio::spawn(async move {
        pump_bidirectional(left_pump, left_reverse_pump, reverse_pump, right_pump).await
    });

    left_peer.write_all(b"a").await.unwrap();
    left_peer.shutdown().await.unwrap();
    reverse_peer.write_all(b"b").await.unwrap();
    reverse_peer.shutdown().await.unwrap();

    let mut right_received = Vec::new();
    timeout(
        Duration::from_secs(2),
        right_peer.read_to_end(&mut right_received),
    )
    .await
    .unwrap()
    .unwrap();
    let mut left_received = Vec::new();
    timeout(
        Duration::from_secs(2),
        left_reverse_peer.read_to_end(&mut left_received),
    )
    .await
    .unwrap()
    .unwrap();

    timeout(Duration::from_secs(2), pump)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(right_received, b"a");
    assert_eq!(left_received, b"b");
}

#![allow(dead_code, unused_imports)]

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, timeout};

pub use tunnel_ai::client::start_openai_client_for_test;
pub use tunnel_ai::iroh_endpoint::{
    endpoint_ticket_string, parse_server_ticket, start_client_endpoint, start_server_endpoint,
};
pub use tunnel_ai::openai::{
    start_openai_client_for_test_with_config, start_openai_server_for_test_with_config,
};
pub use tunnel_ai::server::start_openai_server_for_test;

pub async fn openai_error_response_for_request(request: &str) -> String {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let provider_addr = provider.local_addr().unwrap();
    let provider_task = tokio::spawn(async move {
        timeout(Duration::from_millis(500), provider.accept())
            .await
            .expect_err("provider should not receive malformed local request");
    });

    let server = start_openai_server_for_test(format!("http://{provider_addr}"))
        .await
        .unwrap();
    let client =
        start_openai_client_for_test(server.ticket.clone(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

    let mut stream = TcpStream::connect(client.listen_addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut response = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();

    provider_task.await.unwrap();
    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
    String::from_utf8_lossy(&response).to_string()
}

pub fn chunked_body(response: &[u8]) -> Vec<u8> {
    let header_end = response
        .windows(b"\r\n\r\n".len())
        .position(|window| window == b"\r\n\r\n")
        .unwrap()
        + 4;
    let mut cursor = header_end;
    let mut body = Vec::new();
    loop {
        let line_end = response[cursor..]
            .windows(b"\r\n".len())
            .position(|window| window == b"\r\n")
            .unwrap()
            + cursor;
        let size = usize::from_str_radix(
            std::str::from_utf8(&response[cursor..line_end]).unwrap(),
            16,
        )
        .unwrap();
        cursor = line_end + 2;
        if size == 0 {
            return body;
        }
        body.extend_from_slice(&response[cursor..cursor + size]);
        cursor += size + 2;
    }
}

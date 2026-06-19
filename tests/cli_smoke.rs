use assert_cmd::Command;
use predicates::str::contains;

#[test]
fn help_lists_client_server_and_openai_modes() {
    let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(contains("tunnelAI"))
        .stdout(contains("client"))
        .stdout(contains("server"))
        .stdout(contains("openai-client"))
        .stdout(contains("openai-server"));
}

#[test]
fn client_requires_server_ticket() {
    let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
    cmd.args(["client", "--listen", "127.0.0.1:1080"])
        .assert()
        .failure()
        .stderr(contains("server-ticket"));
}

#[test]
fn openai_client_requires_server_ticket() {
    let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
    cmd.args(["openai-client", "--listen", "127.0.0.1:8080"])
        .assert()
        .failure()
        .stderr(contains("server-ticket"));
}

#[test]
fn openai_server_requires_provider_base_url() {
    let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
    cmd.arg("openai-server")
        .assert()
        .failure()
        .stderr(contains("provider-base-url"));
}

#[test]
fn openai_server_rejects_invalid_allow_peer() {
    let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
    cmd.args([
        "openai-server",
        "--provider-base-url",
        "http://localhost:20128/v1",
        "--allow-peer",
        "not-a-peer",
    ])
    .assert()
    .failure()
    .stderr(contains("invalid peer id"));
}

#[test]
fn openai_server_rejects_invalid_provider_base_url() {
    let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
    cmd.args([
        "openai-server",
        "--provider-base-url",
        "file:///tmp/provider",
    ])
    .assert()
    .failure()
    .stderr(contains("provider base URL must use http or https"));
}

#[test]
fn openai_server_rejects_provider_base_url_credentials() {
    let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
    cmd.args([
        "openai-server",
        "--provider-base-url",
        "https://user:pass@example.test/v1",
    ])
    .assert()
    .failure()
    .stderr(contains("provider base URL must not include credentials"));
}

#[test]
fn openai_server_help_lists_resource_limit_flags() {
    let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
    cmd.args(["openai-server", "--help"])
        .assert()
        .success()
        .stdout(contains("bind-addr"))
        .stdout(contains("max-connections"))
        .stdout(contains("max-streams-per-connection"))
        .stdout(contains("request-read-timeout-ms"))
        .stdout(contains("provider-connect-timeout-ms"))
        .stdout(contains("provider-request-timeout-ms"))
        .stdout(contains("identity-path"));
}

#[test]
fn openai_client_help_lists_resource_limit_flags() {
    let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
    cmd.args(["openai-client", "--help"])
        .assert()
        .success()
        .stdout(contains("max-concurrent-sessions"))
        .stdout(contains("local-request-timeout-ms"))
        .stdout(contains("tunnel-operation-timeout-ms"))
        .stdout(contains("connect-timeout-ms"))
        .stdout(contains("reconnect-attempts"))
        .stdout(contains("health-check-interval-ms"));
}

#[test]
fn openai_server_rejects_zero_resource_limits() {
    for (flag, value) in [
        ("--max-connections", "0"),
        ("--max-streams-per-connection", "0"),
        ("--request-read-timeout-ms", "0"),
        ("--provider-connect-timeout-ms", "0"),
        ("--provider-request-timeout-ms", "0"),
    ] {
        let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
        cmd.args([
            "openai-server",
            "--provider-base-url",
            "http://localhost:20128/v1",
            flag,
            value,
        ])
        .assert()
        .failure()
        .stderr(contains("must be greater than zero"));
    }
}

#[test]
fn openai_client_rejects_zero_resource_limits() {
    for (flag, value) in [
        ("--max-concurrent-sessions", "0"),
        ("--local-request-timeout-ms", "0"),
        ("--tunnel-operation-timeout-ms", "0"),
        ("--connect-timeout-ms", "0"),
        ("--reconnect-attempts", "0"),
        ("--health-check-interval-ms", "0"),
    ] {
        let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
        cmd.args([
            "openai-client",
            "--server-ticket",
            "not-a-ticket",
            flag,
            value,
        ])
        .assert()
        .failure()
        .stderr(contains("must be greater than zero"));
    }
}

#[test]
fn server_process_prints_parseable_ticket() {
    use iroh_tickets::endpoint::EndpointTicket;
    use std::io::{BufRead, BufReader};
    use std::process::{Command as StdCommand, Stdio};
    use std::str::FromStr;
    use std::time::{Duration, Instant};

    let mut child =
        StdCommand::new(std::env::var("CARGO_BIN_EXE_tunnelAI").expect("tunnelAI binary path"))
            .args(["server", "--allow-private-targets"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn server process");

    let stdout = child.stdout.take().expect("stdout piped");
    let mut reader = BufReader::new(stdout);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut ticket = None;

    while Instant::now() < deadline {
        let mut line = String::new();
        if reader.read_line(&mut line).expect("read server stdout") == 0 {
            break;
        }
        if let Some(raw_ticket) = line.strip_prefix("server ticket: ") {
            ticket = Some(raw_ticket.trim().to_string());
            break;
        }
    }

    child.kill().ok();
    child.wait().ok();

    let ticket = ticket.expect("server printed ticket");
    EndpointTicket::from_str(&ticket).expect("ticket parses");
}

#[test]
fn server_bind_addr_is_used() {
    use std::net::UdpSocket;

    let occupied = UdpSocket::bind("127.0.0.1:0").expect("bind occupied udp port");
    let addr = occupied.local_addr().expect("occupied addr").to_string();

    let mut cmd = Command::cargo_bin("tunnelAI").unwrap();
    cmd.args(["server", "--bind-addr", &addr])
        .assert()
        .failure()
        .stderr(contains("bind iroh server endpoint"));
}

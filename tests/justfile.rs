use std::process::Command;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

struct JustFailure {
    code: Option<i32>,
    output: String,
}

fn just_command() -> Command {
    let mut command = Command::new("just");
    command.arg("--justfile").arg("justfile");
    command
}

fn skip_if_just_is_unavailable() -> bool {
    if Command::new("just").arg("--version").output().is_ok() {
        false
    } else {
        eprintln!("skipping justfile integration test because `just` is not installed");
        true
    }
}

fn dry_run(args: &[&str]) -> String {
    let output = just_command()
        .arg("--dry-run")
        .args(args)
        .output()
        .expect("run just --dry-run");

    assert!(
        output.status.success(),
        "just --dry-run failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn failed_dry_run(args: &[&str]) -> String {
    let output = just_command()
        .arg("--dry-run")
        .args(args)
        .output()
        .expect("run just --dry-run");

    assert!(
        !output.status.success(),
        "just --dry-run unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn failed_just(args: &[&str]) -> JustFailure {
    let output = just_command()
        .args(args)
        .output()
        .expect("run just command");

    assert!(
        !output.status.success(),
        "just command unexpectedly succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    JustFailure {
        code: output.status.code(),
        output: format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

#[cfg(unix)]
fn just_with_fake_cargo(args: &[&str]) -> String {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let cargo_path = temp_dir.path().join("cargo");
    let args_path = temp_dir.path().join("cargo-args.txt");
    std::fs::write(
        &cargo_path,
        format!(
            "#!/usr/bin/env bash\nprintf '%s\\n' \"$*\" > {}\n",
            args_path.display()
        ),
    )
    .expect("write fake cargo");
    let mut permissions = std::fs::metadata(&cargo_path)
        .expect("fake cargo metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&cargo_path, permissions).expect("make fake cargo executable");

    let path = format!(
        "{}:{}",
        temp_dir.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = just_command()
        .args(args)
        .env("PATH", path)
        .output()
        .expect("run just with fake cargo");

    assert!(
        output.status.success(),
        "just command unexpectedly failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    std::fs::read_to_string(args_path).expect("fake cargo should capture args")
}

#[test]
fn justfile_exposes_easy_server_and_client_start_recipes() {
    if skip_if_just_is_unavailable() {
        return;
    }

    let output = just_command()
        .arg("--summary")
        .output()
        .expect("run just --summary");

    assert!(
        output.status.success(),
        "just --summary failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let summary = String::from_utf8(output.stdout).expect("summary is utf-8");
    for recipe in [
        "server",
        "client",
        "openai-server",
        "openai-client",
        "socks-server",
        "socks-client",
    ] {
        assert!(
            summary.split_whitespace().any(|item| item == recipe),
            "missing {recipe} recipe in {summary:?}"
        );
    }
}

#[test]
fn justfile_openai_server_defaults_to_local_identity_path() {
    if skip_if_just_is_unavailable() {
        return;
    }

    let server_dry_run = dry_run(&["server"]);
    assert!(
        server_dry_run.contains("'./openai-server.key'"),
        "server recipe should default to ./openai-server.key: {server_dry_run}"
    );

    let openai_server_dry_run = dry_run(&["openai-server"]);
    assert!(
        openai_server_dry_run.contains("--identity-path './openai-server.key'"),
        "openai-server recipe should pass --identity-path ./openai-server.key by default: {openai_server_dry_run}"
    );

    let client_dry_run = dry_run(&["client", "endpoint1placeholder"]);
    assert!(
        !client_dry_run.contains("'./openai-server.key'"),
        "client recipe should not forward an identity path by default: {client_dry_run}"
    );

    let openai_client_dry_run = dry_run(&["openai-client", "endpoint1placeholder"]);
    assert!(
        !openai_client_dry_run.contains("--identity-path './openai-server.key'"),
        "openai-client recipe should not pass a concrete --identity-path by default: {openai_client_dry_run}"
    );

    let persistent_openai_client_dry_run = dry_run(&[
        "openai-client",
        "endpoint1placeholder",
        "127.0.0.1:8080",
        "./openai-client.key",
    ]);
    assert!(
        persistent_openai_client_dry_run.contains("identity_path='./openai-client.key'"),
        "openai-client recipe should accept an explicit client identity path: {persistent_openai_client_dry_run}"
    );
}

#[cfg(unix)]
#[test]
fn justfile_openai_client_forwards_explicit_identity_path_to_cargo() {
    if skip_if_just_is_unavailable() {
        return;
    }

    let cargo_args = just_with_fake_cargo(&[
        "openai-client",
        "endpoint1placeholder",
        "127.0.0.1:8080",
        "./openai-client.key",
    ]);
    assert!(
        cargo_args.contains("--identity-path ./openai-client.key"),
        "explicit identity path should reach cargo command: {cargo_args}"
    );
}

#[test]
fn justfile_openai_client_recipes_require_server_ticket() {
    if skip_if_just_is_unavailable() {
        return;
    }

    for args in [&["client"][..], &["openai-client"][..]] {
        let dry_run = failed_dry_run(args);
        assert!(
            dry_run.contains("server_ticket"),
            "missing-ticket error should mention server_ticket: {dry_run}"
        );
    }
}

#[test]
fn justfile_openai_client_rejects_identity_key_as_server_ticket() {
    if skip_if_just_is_unavailable() {
        return;
    }

    for args in [
        &["client", "./openai-server.key"][..],
        &["openai-client", "./openai-server.key"][..],
        &["client", "keys/openai-server"][..],
    ] {
        let failure = failed_just(args);
        assert_eq!(
            failure.code,
            Some(64),
            "identity-key misuse should exit 64: {}",
            failure.output
        );
        assert!(
            failure
                .output
                .contains("server ticket printed by `openai-server`")
                && failure.output.contains("not an identity key path"),
            "identity-key misuse should fail with a helpful message: {}",
            failure.output
        );
        assert!(
            !failure.output.contains("invalid endpoint ticket"),
            "identity-key misuse should be rejected before cargo runs openai-client: {}",
            failure.output
        );
    }
}

#[test]
fn justfile_dry_runs_startup_recipes_with_shell_safe_argument_quoting() {
    if skip_if_just_is_unavailable() {
        return;
    }

    for args in [
        vec![
            "server",
            "http://127.0.0.1:8081/v1",
            "0.0.0.0:17777",
            "key path/with spaces",
        ],
        vec![
            "client",
            "endpoint1$(printf CLIENT_INJECTION >&2)",
            "127.0.0.1:8080",
        ],
        vec![
            "openai-server",
            "http://example.test/v1?x=$(printf SERVER_INJECTION >&2)",
            "0.0.0.0:17777",
            "key\"path",
        ],
        vec!["openai-client", "endpoint1foo\" bar", "127.0.0.1:8080"],
        vec!["socks-server", "127.0.0.1:0", "true"],
        vec![
            "socks-client",
            "endpoint1$(printf SOCKS_INJECTION >&2)",
            "127.0.0.1:1080",
        ],
    ] {
        let dry_run = dry_run(&args);
        assert!(
            !dry_run.contains("\"endpoint1$(printf"),
            "command substitution must not appear inside double quotes: {dry_run}"
        );
        assert!(
            !dry_run.contains("--server-ticket \"endpoint1foo\" bar\""),
            "embedded double quotes must be shell-escaped: {dry_run}"
        );
    }
}

#[test]
fn justfile_documents_recipe_parameters() {
    let justfile = std::fs::read_to_string("justfile").expect("read justfile");

    for expected in [
        "openai-server provider_base_url",
        "openai-client server_ticket",
        "socks-server bind_addr",
        "socks-client server_ticket",
        "--provider-base-url",
        "--server-ticket",
        "--listen",
        "--identity-path",
    ] {
        assert!(
            justfile.contains(expected),
            "missing {expected:?} in justfile"
        );
    }
}

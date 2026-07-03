# Common tunnelAI development and startup commands.
# Run `just --list` to see available recipes.

set shell := ["bash", "-uc"]

_default:
    @just --list

# Build the release binary.
build:
    cargo build --release

# Run cargo check.
check:
    cargo check

# Run the workspace test suite.
test:
    cargo test --workspace

# Alias for the main OpenAI server path.
server provider_base_url="http://127.0.0.1:20128/v1" bind_addr="0.0.0.0:17777" identity_path="./openai-server.key":
    @just openai-server {{quote(provider_base_url)}} {{quote(bind_addr)}} {{quote(identity_path)}}

# Alias for the main OpenAI client path.
client server_ticket listen="127.0.0.1:8080" identity_path="":
    @just openai-client {{quote(server_ticket)}} {{quote(listen)}} {{quote(identity_path)}}

# Start the OpenAI-compatible provider proxy server.
openai-server provider_base_url="http://127.0.0.1:20128/v1" bind_addr="0.0.0.0:17777" identity_path="./openai-server.key":
    #!/usr/bin/env bash
    set -euo pipefail
    RUST_LOG="${RUST_LOG:-info}" cargo run --release -- openai-server \
      --provider-base-url {{quote(provider_base_url)}} \
      --bind-addr {{quote(bind_addr)}} \
      --identity-path {{quote(identity_path)}}

# Start the local OpenAI-compatible HTTP tunnel client.
openai-client server_ticket listen="127.0.0.1:8080" identity_path="":
    #!/usr/bin/env bash
    set -euo pipefail
    server_ticket={{quote(server_ticket)}}
    if [[ -f "$server_ticket" || "$server_ticket" == *.key || "$server_ticket" == */* ]]; then
      echo "error: the first argument must be the server ticket printed by \`openai-server\`, not an identity key path." >&2
      echo "usage: just openai-client '<PASTE_SERVER_TICKET>' [listen] [identity_path]" >&2
      echo "identity path is optional; pass it as the third argument only when needed." >&2
      exit 64
    fi
    args=(openai-client --server-ticket "$server_ticket" --listen {{quote(listen)}})
    identity_path={{quote(identity_path)}}
    if [[ -n "$identity_path" ]]; then
      args+=(--identity-path "$identity_path")
    fi
    RUST_LOG="${RUST_LOG:-info}" cargo run --release -- "${args[@]}"

# Start the generic SOCKS5/TCP exit server.
socks-server bind_addr="0.0.0.0:0" allow_private_targets="false":
    #!/usr/bin/env bash
    set -euo pipefail
    args=(server --bind-addr {{quote(bind_addr)}})
    if [[ {{quote(allow_private_targets)}} == "true" ]]; then
      args+=(--allow-private-targets)
    fi
    RUST_LOG="${RUST_LOG:-info}" cargo run --release -- "${args[@]}"

# Start the local SOCKS5 tunnel client.
socks-client server_ticket listen="127.0.0.1:1080":
    #!/usr/bin/env bash
    set -euo pipefail
    RUST_LOG="${RUST_LOG:-info}" cargo run --release -- client \
      --server-ticket {{quote(server_ticket)}} \
      --listen {{quote(listen)}}

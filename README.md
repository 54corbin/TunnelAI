# tunnelAI

Share one OpenAI-compatible LLM API across your WLAN out of the box, then extend the same setup across the WAN when you need it.

Run `openai-server` on the machine that can reach your LLM provider. On another machine, run `openai-client` and point apps at its local `/v1` endpoint. If both machines sit on the same Wi-Fi or wired LAN, that is enough to start using the shared API. The same iroh ticket can also reach the server from other networks.

Typical use cases:

- Share a home, lab, or office GPU box with laptops on the same WLAN in a few minutes.
- Re-share that same setup to cloud VMs, remote dev machines, and teammates later.
- Keep provider network access on one machine. Use dummy client API keys for local providers; forward real credentials only when your upstream provider requires them.
- Give tools on different networks the same `OPENAI_BASE_URL` shape.
- Avoid standing up Nginx, TLS certificates, DNS, VPN accounts, or public firewall rules for a first working setup.

`tunnelAI` also includes a SOCKS5 CONNECT tunnel for generic TCP traffic. The LLM API mode is the main out-of-box path.

## How it works

```text
OpenAI-compatible app on another WLAN or remote machine
  -> http://127.0.0.1:8080/v1       # local openai-client
  -> iroh tunnel                    # works on the same WLAN out of the box; can also cross NAT/WAN
  -> openai-server                  # machine with provider access
  -> http://127.0.0.1:PORT/v1       # provider_base_url
```

The server prints an iroh ticket. A client can use that ticket from another machine on the same WLAN with no extra reverse proxy or API gateway. The same ticket can also reach the server from another network. The provider can stay bound to localhost on the server machine.

## Build

```bash
cargo build --release
```

The binary will be at:

```text
target/release/tunnelAI
```

For development:

```bash
cargo check
cargo test --workspace
```

## Quick start: share an LLM API across your WLAN

### 1. Start your OpenAI-compatible provider

On the machine with the model or upstream API, start any provider that exposes an OpenAI-compatible `/v1` API.

Examples:

```bash
# llama.cpp server example
llama-server --host 127.0.0.1 --port 8081 --model ./model.gguf

# vLLM example
vllm serve Qwen/Qwen2.5-7B-Instruct --host 127.0.0.1 --port 8000
```

Use the provider's real base URL in the next step. Include `/v1` if the provider expects `/v1/...` paths.

### 2. Run the iroh OpenAI server

On the same machine as the provider:

```bash
RUST_LOG=info target/release/tunnelAI openai-server \
  --provider-base-url http://127.0.0.1:8081/v1 \
  --identity-path ~/.config/tunnelAI/openai-server.key \
  --bind-addr 0.0.0.0:17777
```

The server prints:

```text
openai proxy server ticket: endpoint1...
```

Copy the whole ticket. Treat it like a bearer connection detail.

Use `--identity-path` plus a fixed `--bind-addr` when you want old tickets to keep working after a restart. Without `--identity-path`, the server creates a new ephemeral identity each run.

### 3. Run a client on another WLAN machine

On each laptop, workstation, or other machine on the same WLAN that should use the shared LLM API:

```bash
RUST_LOG=info target/release/tunnelAI openai-client \
  --server-ticket '<PASTE_SERVER_TICKET>' \
  --listen 127.0.0.1:8080
```

Point tools at:

```text
OPENAI_BASE_URL=http://127.0.0.1:8080/v1
```

For clients that use command-line environment variables:

```bash
export OPENAI_BASE_URL=http://127.0.0.1:8080/v1
export OPENAI_API_KEY=anything
```

Most local providers ignore the API key, so client apps can use a dummy value. If your upstream provider requires credentials, the local app must send the provider's expected `Authorization` header. The proxy forwards request headers through the tunnel, so the server and provider side can see those credentials.

### 4. Test it

```bash
curl http://127.0.0.1:8080/v1/models
```

Chat completion example:

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"test-model","messages":[{"role":"user","content":"Say ping."}]}'
```

If your upstream provider requires authentication, add its expected `Authorization` header on the client request. The proxy forwards that header through the tunnel.

If your provider supports streaming, streaming responses pass back through the tunnel.

## Recommended WAN setup

After the WLAN path is working, you can keep the same server and extend it to remote networks.

For a personal or team deployment:

1. Run the model/provider and `openai-server` on the GPU box or trusted gateway machine.
2. Keep the provider bound to localhost when possible.
3. Use `--identity-path ~/.config/tunnelAI/openai-server.key` so the server keeps the same iroh identity across restarts.
4. Use `--bind-addr 0.0.0.0:17777` so the ticket can include a stable direct address when the network allows it.
5. Run `openai-client` on each remote machine with `--listen 127.0.0.1:8080`.
6. Set each app's `OPENAI_BASE_URL` to `http://127.0.0.1:8080/v1`.
7. Add `--allow-peer <CLIENT_ENDPOINT_ID>` on the server for machines you trust.
8. Use host firewall rules when the server is on a public or semi-public network.

Iroh handles peer connection setup and NAT traversal. A fixed server UDP port improves repeatability, but the first usable version does not require you to design a public HTTP service.

## Re-sharing to the rest of your WLAN

The default client listener binds to localhost:

```text
127.0.0.1:8080
```

That is the safest default. It lets apps on the same machine use the shared API.

If you want one client machine to re-share the API endpoint to the rest of your WLAN, bind the client to a LAN address:

```bash
RUST_LOG=info target/release/tunnelAI openai-client \
  --server-ticket '<PASTE_SERVER_TICKET>' \
  --listen 0.0.0.0:8080
```

Then nearby machines can use:

```text
OPENAI_BASE_URL=http://<client-lan-ip>:8080/v1
```

Only do this on a trusted network or behind a firewall. `openai-client` does not implement HTTP authentication.

## OpenAI mode flags

Server flags:

```bash
tunnelAI openai-server \
  --provider-base-url <URL> \
  --bind-addr 0.0.0.0:17777 \
  --identity-path ~/.config/tunnelAI/openai-server.key \
  --allow-peer <CLIENT_ENDPOINT_ID> \
  --max-connections 128 \
  --max-streams-per-connection 128 \
  --request-read-timeout-ms 10000 \
  --provider-connect-timeout-ms 10000 \
  --provider-request-timeout-ms 120000
```

Client flags:

```bash
tunnelAI openai-client \
  --server-ticket '<TICKET>' \
  --listen 127.0.0.1:8080 \
  --max-concurrent-sessions 128 \
  --local-request-timeout-ms 10000 \
  --tunnel-operation-timeout-ms 10000 \
  --connect-timeout-ms 10000 \
  --reconnect-attempts 1 \
  --health-check-interval-ms 30000
```

`--health-check-interval-ms` is optional. The health check calls the server's internal `/__tunnelAI/healthz` endpoint over the tunnel and does not reach the configured provider.

## Provider URL behavior

`--provider-base-url` must be an `http` or `https` URL with a host and no embedded credentials.

If a local client sends `/v1/chat/completions` and the provider base URL ends in `/v1`, the proxy forwards to the provider without adding a second `/v1`.

Examples:

```text
provider base: http://127.0.0.1:8081/v1
client path:   /v1/chat/completions
forwarded to:  http://127.0.0.1:8081/v1/chat/completions
```

```text
provider base: http://127.0.0.1:8081
client path:   /v1/chat/completions
forwarded to:  http://127.0.0.1:8081/v1/chat/completions
```

The proxy accepts origin-form HTTP request targets such as `/v1/chat/completions`. It rejects absolute-form targets.

## Security notes

- Treat server tickets like bearer connection details. Anyone with a reusable ticket can try to dial the server.
- Use `openai-server --allow-peer <CLIENT_ENDPOINT_ID>` for WAN use. Without it, any peer with the ticket can try to connect.
- Keep `openai-client --listen` on `127.0.0.1` unless you intend to expose the local API port to nearby machines.
- `openai-client` does not implement HTTP authentication.
- Provider credentials pass through request headers from the local app to the configured provider.
- This is not an anonymity tool. The server can see OpenAI-compatible HTTP requests and provider targets.
- OpenAI mode uses bounded concurrency and timeouts on both client and server.

use anyhow::{Context, Result, bail};
use iroh::{Endpoint, EndpointAddr, SecretKey, endpoint::presets};
use iroh_tickets::endpoint::EndpointTicket;
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::str::FromStr;

const SECRET_KEY_PREFIX: &str = "iroh-secret-key-v1:";
const SECRET_KEY_HEX_LEN: usize = 64;

pub fn parse_server_ticket(ticket: &str) -> Result<EndpointAddr> {
    let ticket = EndpointTicket::from_str(ticket).context("invalid endpoint ticket")?;
    Ok(ticket.into())
}

pub async fn start_server_endpoint() -> Result<Endpoint> {
    start_server_endpoint_with_bind_addr("0.0.0.0:0".parse().expect("valid default bind addr"))
        .await
}

pub async fn start_server_endpoint_with_bind_addr(bind_addr: SocketAddr) -> Result<Endpoint> {
    start_server_endpoint_with_optional_identity(bind_addr, None).await
}

pub async fn start_server_endpoint_with_optional_identity(
    bind_addr: SocketAddr,
    identity_path: Option<&Path>,
) -> Result<Endpoint> {
    let mut builder = Endpoint::builder(presets::N0)
        .clear_ip_transports()
        .bind_addr(bind_addr)
        .context("configure iroh server bind address")?
        .alpns(vec![crate::ALPN.to_vec()]);

    if let Some(path) = identity_path {
        builder = builder.secret_key(load_or_create_secret_key(path)?);
    }

    let endpoint = builder.bind().await.context("bind iroh server endpoint")?;
    Ok(endpoint)
}

fn load_or_create_secret_key(path: &Path) -> Result<SecretKey> {
    if path.exists() {
        let contents = fs::read_to_string(path)
            .with_context(|| format!("read iroh identity secret from {}", path.display()))?;
        return decode_secret_key(&contents)
            .with_context(|| format!("decode iroh identity secret from {}", path.display()));
    }

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create iroh identity directory {}", parent.display()))?;
    }

    let secret_key = SecretKey::generate();
    let encoded = encode_secret_key(&secret_key);
    let temp_path = path.with_extension("tmp");

    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);

    let write_result = (|| -> Result<()> {
        let mut file = options.open(&temp_path).with_context(|| {
            format!(
                "create temporary iroh identity file {}",
                temp_path.display()
            )
        })?;
        file.write_all(encoded.as_bytes()).with_context(|| {
            format!("write temporary iroh identity file {}", temp_path.display())
        })?;
        file.flush().with_context(|| {
            format!("flush temporary iroh identity file {}", temp_path.display())
        })?;
        file.sync_all().with_context(|| {
            format!("sync temporary iroh identity file {}", temp_path.display())
        })?;
        Ok(())
    })();

    if let Err(error) = write_result {
        let _ = fs::remove_file(&temp_path);
        return Err(error);
    }

    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "install iroh identity file {} from {}",
            path.display(),
            temp_path.display()
        )
    })?;

    Ok(secret_key)
}

fn encode_secret_key(secret_key: &SecretKey) -> String {
    let mut encoded = String::with_capacity(SECRET_KEY_PREFIX.len() + SECRET_KEY_HEX_LEN + 1);
    encoded.push_str(SECRET_KEY_PREFIX);
    for byte in secret_key.to_bytes() {
        encoded.push(hex_char(byte >> 4));
        encoded.push(hex_char(byte & 0x0f));
    }
    encoded.push('\n');
    encoded
}

fn decode_secret_key(contents: &str) -> Result<SecretKey> {
    let trimmed = contents.trim();
    let Some(hex) = trimmed.strip_prefix(SECRET_KEY_PREFIX) else {
        bail!("invalid iroh identity secret prefix");
    };
    if hex.len() != SECRET_KEY_HEX_LEN {
        bail!("invalid iroh identity secret length");
    }

    let mut bytes = [0_u8; 32];
    let hex_bytes = hex.as_bytes();
    for (index, chunk) in hex_bytes.chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(chunk[0])?;
        let low = decode_hex_nibble(chunk[1])?;
        bytes[index] = (high << 4) | low;
    }

    Ok(SecretKey::from_bytes(&bytes))
}

fn hex_char(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'a' + (nibble - 10)) as char,
        _ => unreachable!("nibble is masked to four bits"),
    }
}

fn decode_hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => bail!("invalid iroh identity secret hex"),
    }
}

pub async fn start_client_endpoint() -> Result<Endpoint> {
    start_client_endpoint_with_optional_identity(None).await
}

pub async fn start_client_endpoint_with_optional_identity(
    identity_path: Option<&Path>,
) -> Result<Endpoint> {
    let mut builder = Endpoint::builder(presets::N0);

    if let Some(path) = identity_path {
        builder = builder.secret_key(load_or_create_secret_key(path)?);
    }

    builder.bind().await.context("bind iroh client endpoint")
}

pub async fn endpoint_ticket_string(endpoint: &Endpoint) -> Result<String> {
    let addr = endpoint.addr();
    Ok(EndpointTicket::new(addr).to_string())
}

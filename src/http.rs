use anyhow::{Context, Result};
use iroh::endpoint::{RecvStream, SendStream};
use thiserror::Error;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};
use tokio::net::tcp::OwnedReadHalf;

const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_HEADER_COUNT: usize = 100;
const MAX_BODY_BYTES: usize = 1024 * 1024;

pub(crate) struct OpenAiHttpRequest {
    pub(crate) method: reqwest::Method,
    pub(crate) target: String,
    pub(crate) headers: reqwest::header::HeaderMap,
    pub(crate) body: Vec<u8>,
}

pub(crate) struct HttpErrorResponse {
    pub(crate) status: u16,
    pub(crate) reason: &'static str,
    pub(crate) body: &'static str,
}

#[derive(Debug, Error)]
pub(crate) enum HttpRequestError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("request headers too large")]
    HeadersTooLarge,
    #[error("payload too large")]
    PayloadTooLarge,
    #[error("chunked request bodies are not supported")]
    UnsupportedChunked,
}

impl HttpRequestError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub(crate) fn response(&self) -> HttpErrorResponse {
        match self {
            Self::BadRequest(_) => HttpErrorResponse {
                status: 400,
                reason: "Bad Request",
                body: "bad request",
            },
            Self::HeadersTooLarge => HttpErrorResponse {
                status: 431,
                reason: "Request Header Fields Too Large",
                body: "request headers too large",
            },
            Self::PayloadTooLarge => HttpErrorResponse {
                status: 413,
                reason: "Payload Too Large",
                body: "payload too large",
            },
            Self::UnsupportedChunked => HttpErrorResponse {
                status: 501,
                reason: "Not Implemented",
                body: "chunked request bodies are not supported",
            },
        }
    }
}

pub(crate) async fn read_http_request(
    recv: RecvStream,
) -> std::result::Result<OpenAiHttpRequest, HttpRequestError> {
    let mut reader = BufReader::new(recv);
    let line = read_limited_line(&mut reader, MAX_REQUEST_LINE_BYTES)
        .await
        .map_err(|err| match err {
            HttpRequestError::HeadersTooLarge => {
                HttpRequestError::BadRequest("request line too large".into())
            }
            err => err,
        })?;
    let request_line = line.trim_end_matches(['\r', '\n']);
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing method"))?
        .parse::<reqwest::Method>()
        .map_err(|_| HttpRequestError::bad_request("parse HTTP method"))?;
    let target = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing target"))?
        .to_string();
    let version = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing version"))?;
    if parts.next().is_some() {
        return Err(HttpRequestError::bad_request(
            "HTTP request line has too many parts",
        ));
    }
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return Err(HttpRequestError::bad_request(format!(
            "unsupported HTTP request version: {version}"
        )));
    }
    if !target.starts_with('/') || target.starts_with("//") {
        return Err(HttpRequestError::bad_request(
            "OpenAI proxy requires origin-form request target",
        ));
    }

    let mut headers = reqwest::header::HeaderMap::new();
    let mut content_length = None::<usize>;
    let mut has_chunked_body = false;
    let mut header_bytes = 0usize;
    let mut header_count = 0usize;
    loop {
        let line = read_limited_line(&mut reader, MAX_HEADER_BYTES).await?;
        header_bytes += line.len();
        if header_bytes > MAX_HEADER_BYTES {
            return Err(HttpRequestError::HeadersTooLarge);
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        header_count += 1;
        if header_count > MAX_HEADER_COUNT {
            return Err(HttpRequestError::HeadersTooLarge);
        }
        let header_line = line.trim_end_matches(['\r', '\n']);
        let Some((name, value)) = header_line.split_once(':') else {
            return Err(HttpRequestError::bad_request(format!(
                "invalid HTTP header line: {header_line}"
            )));
        };
        let name = reqwest::header::HeaderName::from_bytes(name.trim().as_bytes())
            .map_err(|_| HttpRequestError::bad_request("parse HTTP header name"))?;
        let value = reqwest::header::HeaderValue::from_str(value.trim())
            .map_err(|_| HttpRequestError::bad_request("parse HTTP header value"))?;
        if name == reqwest::header::CONTENT_LENGTH {
            let parsed = value
                .to_str()
                .map_err(|_| HttpRequestError::bad_request("content-length is not valid text"))?
                .parse::<usize>()
                .map_err(|_| HttpRequestError::bad_request("parse content-length"))?;
            if parsed > MAX_BODY_BYTES {
                return Err(HttpRequestError::PayloadTooLarge);
            }
            if let Some(existing) = content_length
                && existing != parsed
            {
                return Err(HttpRequestError::bad_request(
                    "conflicting content-length headers",
                ));
            }
            content_length = Some(parsed);
        }
        if name == reqwest::header::TRANSFER_ENCODING {
            has_chunked_body = value
                .to_str()
                .map_err(|_| HttpRequestError::bad_request("transfer-encoding is not valid text"))?
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"));
        }
        headers.append(name, value);
    }

    if has_chunked_body {
        return Err(HttpRequestError::UnsupportedChunked);
    }

    let content_length = content_length.unwrap_or(0);
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader
            .read_exact(&mut body)
            .await
            .map_err(|_| HttpRequestError::bad_request("read request body"))?;
    }

    Ok(OpenAiHttpRequest {
        method,
        target,
        headers,
        body,
    })
}

async fn read_limited_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_len: usize,
) -> std::result::Result<String, HttpRequestError> {
    let bytes = read_limited_line_bytes(reader, max_len).await?;
    String::from_utf8(bytes).map_err(|_| HttpRequestError::bad_request("HTTP line is not utf-8"))
}

async fn read_limited_line_bytes<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    max_len: usize,
) -> std::result::Result<Vec<u8>, HttpRequestError> {
    let mut bytes = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|_| HttpRequestError::bad_request("read HTTP line"))?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Err(HttpRequestError::bad_request("HTTP request ended early"));
            }
            break;
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if bytes.len() + take > max_len {
            return Err(HttpRequestError::HeadersTooLarge);
        }
        bytes.extend_from_slice(&available[..take]);
        reader.consume(take);
        if bytes.ends_with(b"\n") {
            break;
        }
    }
    Ok(bytes)
}

pub(crate) fn should_forward_request_header(name: &str) -> bool {
    !matches!(
        name,
        "host"
            | "content-length"
            | "connection"
            | "transfer-encoding"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "upgrade"
    )
}

pub(crate) fn is_health_request(request: &OpenAiHttpRequest) -> bool {
    request.method == reqwest::Method::GET && request.target == "/__tunnelAI/healthz"
}

pub(crate) async fn write_plain_error<W: AsyncWrite + Unpin>(
    send: &mut W,
    status: u16,
    reason: &str,
    body: &str,
) -> Result<()> {
    write_plain_response(send, status, reason, body).await
}

pub(crate) async fn write_plain_response<W: AsyncWrite + Unpin>(
    send: &mut W,
    status: u16,
    reason: &str,
    body: &str,
) -> Result<()> {
    send.write_all(format!("HTTP/1.1 {status} {reason}\r\n").as_bytes())
        .await?;
    send.write_all(b"content-type: text/plain\r\n").await?;
    send.write_all(b"connection: close\r\n").await?;
    send.write_all(format!("content-length: {}\r\n", body.len()).as_bytes())
        .await?;
    send.write_all(b"\r\n").await?;
    send.write_all(body.as_bytes()).await?;
    Ok(())
}

pub(crate) async fn write_http_response(
    send: &mut SendStream,
    mut response: reqwest::Response,
    first_chunk: Option<Vec<u8>>,
) -> Result<()> {
    let status = response.status();
    let headers = response.headers().clone();
    let reason = status.canonical_reason().unwrap_or("");

    send.write_all(format!("HTTP/1.1 {} {}\r\n", status.as_u16(), reason).as_bytes())
        .await?;
    for (name, value) in headers.iter() {
        if should_forward_response_header(name.as_str()) {
            send.write_all(name.as_str().to_ascii_lowercase().as_bytes())
                .await?;
            send.write_all(b": ").await?;
            send.write_all(value.as_bytes()).await?;
            send.write_all(b"\r\n").await?;
        }
    }
    send.write_all(b"connection: close\r\n").await?;
    send.write_all(b"transfer-encoding: chunked\r\n").await?;
    send.write_all(b"\r\n").await?;

    if let Some(chunk) = first_chunk {
        write_response_chunk(send, &chunk).await?;
    }
    while let Some(chunk) = response
        .chunk()
        .await
        .context("read provider response body chunk")?
    {
        write_response_chunk(send, &chunk).await?;
    }
    send.write_all(b"0\r\n\r\n").await?;
    Ok(())
}

async fn write_response_chunk(send: &mut SendStream, chunk: &[u8]) -> Result<()> {
    send.write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
        .await?;
    send.write_all(chunk).await?;
    send.write_all(b"\r\n").await?;
    Ok(())
}

fn should_forward_response_header(name: &str) -> bool {
    !matches!(
        name,
        "content-length"
            | "connection"
            | "transfer-encoding"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "upgrade"
    )
}

pub(crate) async fn read_one_local_http_request(
    reader: &mut BufReader<OwnedReadHalf>,
) -> std::result::Result<Vec<u8>, HttpRequestError> {
    let mut raw = Vec::new();
    let line = read_limited_line_bytes(reader, MAX_REQUEST_LINE_BYTES)
        .await
        .map_err(|err| match err {
            HttpRequestError::HeadersTooLarge => {
                HttpRequestError::BadRequest("request line too large".into())
            }
            err => err,
        })?;
    let request_line = std::str::from_utf8(&line)
        .map_err(|_| HttpRequestError::bad_request("HTTP request line is not utf-8"))?
        .trim_end_matches(['\r', '\n']);
    let mut parts = request_line.split_whitespace();
    let _method = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing method"))?
        .parse::<reqwest::Method>()
        .map_err(|_| HttpRequestError::bad_request("parse HTTP method"))?;
    let target = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing target"))?;
    let version = parts
        .next()
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing version"))?;
    if parts.next().is_some() {
        return Err(HttpRequestError::bad_request(
            "HTTP request line has too many parts",
        ));
    }
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return Err(HttpRequestError::bad_request(format!(
            "unsupported HTTP request version: {version}"
        )));
    }
    if !target.starts_with('/') || target.starts_with("//") {
        return Err(HttpRequestError::bad_request(
            "OpenAI proxy requires origin-form request target",
        ));
    }
    raw.extend_from_slice(&line);

    let mut header_bytes = 0usize;
    let mut header_count = 0usize;
    let mut content_length = None::<usize>;
    let mut has_chunked_body = false;
    loop {
        let line = read_limited_line_bytes(reader, MAX_HEADER_BYTES).await?;
        header_bytes += line.len();
        if header_bytes > MAX_HEADER_BYTES {
            return Err(HttpRequestError::HeadersTooLarge);
        }
        raw.extend_from_slice(&line);
        if line == b"\r\n" || line == b"\n" {
            break;
        }
        header_count += 1;
        if header_count > MAX_HEADER_COUNT {
            return Err(HttpRequestError::HeadersTooLarge);
        }
        let header_line = std::str::from_utf8(&line)
            .map_err(|_| HttpRequestError::bad_request("HTTP header line is not utf-8"))?
            .trim_end_matches(['\r', '\n']);
        let Some((name, value)) = header_line.split_once(':') else {
            return Err(HttpRequestError::bad_request(format!(
                "invalid HTTP header line: {header_line}"
            )));
        };
        let name = reqwest::header::HeaderName::from_bytes(name.trim().as_bytes())
            .map_err(|_| HttpRequestError::bad_request("parse HTTP header name"))?;
        let value = reqwest::header::HeaderValue::from_str(value.trim())
            .map_err(|_| HttpRequestError::bad_request("parse HTTP header value"))?;
        if name == reqwest::header::CONTENT_LENGTH {
            let parsed = value
                .to_str()
                .map_err(|_| HttpRequestError::bad_request("content-length is not valid text"))?
                .parse::<usize>()
                .map_err(|_| HttpRequestError::bad_request("parse content-length"))?;
            if parsed > MAX_BODY_BYTES {
                return Err(HttpRequestError::PayloadTooLarge);
            }
            if let Some(existing) = content_length
                && existing != parsed
            {
                return Err(HttpRequestError::bad_request(
                    "conflicting content-length headers",
                ));
            }
            content_length = Some(parsed);
        }
        if name == reqwest::header::TRANSFER_ENCODING {
            has_chunked_body = value
                .to_str()
                .map_err(|_| HttpRequestError::bad_request("transfer-encoding is not valid text"))?
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"));
        }
    }

    if !has_chunked_body {
        let content_length = content_length.unwrap_or(0);
        let start = raw.len();
        raw.resize(start + content_length, 0);
        if content_length > 0 {
            reader
                .read_exact(&mut raw[start..])
                .await
                .map_err(|_| HttpRequestError::bad_request("read request body"))?;
        }
        if !reader.buffer().is_empty() {
            return Err(HttpRequestError::bad_request(
                "local connection contains more than one HTTP request",
            ));
        }
    }

    Ok(raw)
}

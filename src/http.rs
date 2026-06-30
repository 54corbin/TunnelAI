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
    let head = read_http_head(&mut reader).await?;
    let parsed = parse_http_head(&head)?;

    if parsed.has_chunked_body {
        return Err(HttpRequestError::UnsupportedChunked);
    }

    let content_length = parsed.content_length.unwrap_or(0);
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader
            .read_exact(&mut body)
            .await
            .map_err(|_| HttpRequestError::bad_request("read request body"))?;
    }

    Ok(OpenAiHttpRequest {
        method: parsed.method,
        target: parsed.target,
        headers: parsed.headers,
        body,
    })
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

struct ParsedHttpHead {
    method: reqwest::Method,
    target: String,
    headers: reqwest::header::HeaderMap,
    content_length: Option<usize>,
    has_chunked_body: bool,
}

async fn read_http_head<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> std::result::Result<Vec<u8>, HttpRequestError> {
    let mut head = Vec::new();
    let request_line = read_limited_line_bytes(reader, MAX_REQUEST_LINE_BYTES)
        .await
        .map_err(|err| match err {
            HttpRequestError::HeadersTooLarge => {
                HttpRequestError::BadRequest("request line too large".into())
            }
            err => err,
        })?;
    head.extend_from_slice(&request_line);

    let mut header_bytes = 0usize;
    loop {
        let line = read_limited_line_bytes(reader, MAX_HEADER_BYTES).await?;
        header_bytes += line.len();
        if header_bytes > MAX_HEADER_BYTES {
            return Err(HttpRequestError::HeadersTooLarge);
        }
        let is_end = line == b"\r\n" || line == b"\n";
        head.extend_from_slice(&line);
        if is_end {
            break;
        }
    }

    Ok(head)
}

fn parse_http_head(head: &[u8]) -> std::result::Result<ParsedHttpHead, HttpRequestError> {
    let mut parsed_headers = [httparse::EMPTY_HEADER; MAX_HEADER_COUNT];
    let mut request = httparse::Request::new(&mut parsed_headers);
    let status = request.parse(head).map_err(|err| match err {
        httparse::Error::TooManyHeaders => HttpRequestError::HeadersTooLarge,
        _ => HttpRequestError::bad_request("parse HTTP request head"),
    })?;
    if status.is_partial() {
        return Err(HttpRequestError::bad_request("HTTP request ended early"));
    }

    let method = request
        .method
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing method"))?
        .parse::<reqwest::Method>()
        .map_err(|_| HttpRequestError::bad_request("parse HTTP method"))?;
    let target = request
        .path
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing target"))?
        .to_string();
    let version = request
        .version
        .ok_or_else(|| HttpRequestError::bad_request("HTTP request line is missing version"))?;
    if !matches!(version, 0 | 1) {
        return Err(HttpRequestError::bad_request(format!(
            "unsupported HTTP request version: HTTP/1.{version}"
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

    for header in request.headers.iter() {
        let name = reqwest::header::HeaderName::from_bytes(header.name.as_bytes())
            .map_err(|_| HttpRequestError::bad_request("parse HTTP header name"))?;
        let value =
            reqwest::header::HeaderValue::from_bytes(trim_optional_whitespace(header.value))
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

    Ok(ParsedHttpHead {
        method,
        target,
        headers,
        content_length,
        has_chunked_body,
    })
}

fn trim_optional_whitespace(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\t'))
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !matches!(byte, b' ' | b'\t'))
        .map_or(start, |position| position + 1);
    &value[start..end]
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
    let mut raw = read_http_head(reader).await?;
    let parsed = parse_http_head(&raw)?;

    // Do not buffer an unbounded chunked body from a local client. Preserve the
    // request head and let the remote HTTP parser reject chunked request bodies.
    if !parsed.has_chunked_body {
        let content_length = parsed.content_length.unwrap_or(0);
        let body_start = raw.len();
        raw.resize(body_start + content_length, 0);
        if content_length > 0 {
            reader
                .read_exact(&mut raw[body_start..])
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn local_request_rejects_invalid_header_name_whitespace() {
        let mut reader =
            local_request_reader(b"GET /v1/models HTTP/1.1\r\n Bad: value\r\n\r\n").await;

        let error = read_one_local_http_request(&mut reader)
            .await
            .expect_err("header names with leading whitespace must be rejected");

        assert!(
            matches!(error, HttpRequestError::BadRequest(_)),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn local_request_rejects_conflicting_content_lengths() {
        let mut reader = local_request_reader(
            b"POST /v1/chat/completions HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx",
        )
        .await;

        let error = read_one_local_http_request(&mut reader)
            .await
            .expect_err("conflicting content-length headers must be rejected");

        assert!(
            matches!(error, HttpRequestError::BadRequest(_)),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn local_request_preserves_raw_head_and_body() {
        let request =
            b"POST /v1/chat/completions HTTP/1.1\r\nContent-Length: 7\r\nX-Test:\t value \t\r\n\r\npayload";
        let mut reader = local_request_reader(request).await;

        let raw = read_one_local_http_request(&mut reader).await.unwrap();

        assert_eq!(raw, request);
    }

    #[tokio::test]
    async fn local_request_allows_chunked_head_without_body_read() {
        let request = b"POST /v1/chat/completions HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n";
        let mut reader = local_request_reader(request).await;

        let raw = read_one_local_http_request(&mut reader).await.unwrap();

        assert_eq!(raw, request);
    }

    #[test]
    fn parsed_head_marks_chunked_transfer_encoding() {
        let parsed = parse_http_head(
            b"POST /v1/chat/completions HTTP/1.1\r\nTransfer-Encoding: gzip, chunked\r\n\r\n",
        )
        .unwrap();

        assert!(parsed.has_chunked_body);
    }

    #[test]
    fn parsed_head_rejects_conflicting_content_lengths() {
        let error = match parse_http_head(
            b"POST /v1/chat/completions HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n",
        ) {
            Ok(_) => panic!("conflicting content-length headers must be rejected"),
            Err(error) => error,
        };

        assert!(
            matches!(error, HttpRequestError::BadRequest(_)),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn parsed_head_rejects_body_over_limit() {
        let request = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY_BYTES + 1
        );

        let error = match parse_http_head(request.as_bytes()) {
            Ok(_) => panic!("oversized content-length must be rejected"),
            Err(error) => error,
        };

        assert!(
            matches!(error, HttpRequestError::PayloadTooLarge),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn parsed_head_accepts_duplicate_identical_content_lengths() {
        let parsed = parse_http_head(
            b"POST /v1/chat/completions HTTP/1.1\r\nContent-Length: 7\r\nContent-Length: 7\r\n\r\n",
        )
        .unwrap();

        assert_eq!(parsed.content_length, Some(7));
    }

    #[test]
    fn parsed_head_accepts_exactly_max_header_count() {
        let mut request = b"GET /v1/models HTTP/1.1\r\n".to_vec();
        for index in 0..MAX_HEADER_COUNT {
            request.extend_from_slice(format!("X-Test-{index}: value\r\n").as_bytes());
        }
        request.extend_from_slice(b"\r\n");

        let parsed = parse_http_head(&request).unwrap();

        assert_eq!(parsed.headers.len(), MAX_HEADER_COUNT);
    }

    #[test]
    fn parsed_head_rejects_more_than_max_header_count() {
        let mut request = b"GET /v1/models HTTP/1.1\r\n".to_vec();
        for index in 0..=MAX_HEADER_COUNT {
            request.extend_from_slice(format!("X-Test-{index}: value\r\n").as_bytes());
        }
        request.extend_from_slice(b"\r\n");

        let error = match parse_http_head(&request) {
            Ok(_) => panic!("too many headers must be rejected"),
            Err(error) => error,
        };

        assert!(
            matches!(error, HttpRequestError::HeadersTooLarge),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn parsed_head_accepts_obs_text_header_values() {
        let parsed = parse_http_head(b"GET /v1/models HTTP/1.1\r\nX-Binary: \xff\r\n\r\n")
            .expect("obs-text header values are valid HTTP bytes");

        assert_eq!(parsed.headers["x-binary"].as_bytes(), b"\xff");
    }

    #[tokio::test]
    async fn local_request_maps_too_many_headers_to_headers_too_large() {
        let mut request = b"GET /v1/models HTTP/1.1\r\n".to_vec();
        for index in 0..=MAX_HEADER_COUNT {
            request.extend_from_slice(format!("X-Test-{index}: value\r\n").as_bytes());
        }
        request.extend_from_slice(b"\r\n");
        let mut reader = local_request_reader(request).await;

        let error = read_one_local_http_request(&mut reader)
            .await
            .expect_err("too many headers must be rejected with the header-size error");

        assert!(
            matches!(error, HttpRequestError::HeadersTooLarge),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn local_request_rejects_aggregate_headers_over_limit() {
        let mut request = b"GET /v1/models HTTP/1.1\r\nX-A: ".to_vec();
        request.extend(std::iter::repeat_n(b'a', MAX_HEADER_BYTES - 8));
        request.extend_from_slice(b"\r\nX-B: b\r\n\r\n");
        let mut reader = local_request_reader(request).await;

        let error = read_one_local_http_request(&mut reader)
            .await
            .expect_err("aggregate header bytes over the limit must be rejected");

        assert!(
            matches!(error, HttpRequestError::HeadersTooLarge),
            "unexpected error: {error:?}"
        );
    }

    async fn local_request_reader(request: impl Into<Vec<u8>>) -> BufReader<OwnedReadHalf> {
        let request = request.into();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let writer = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write_all(&request).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let (stream, _) = listener.accept().await.unwrap();
        writer.await.unwrap();
        BufReader::new(stream.into_split().0)
    }
}

use anyhow::{Context, Result, bail};

use super::validate_provider_base_url;

pub fn provider_request_url(base_url: &str, request_target: &str) -> Result<reqwest::Url> {
    let base = validate_provider_base_url(base_url)?;
    provider_request_url_from_base(&base, request_target)
}

pub(crate) fn provider_request_url_from_base(
    base_url: &reqwest::Url,
    request_target: &str,
) -> Result<reqwest::Url> {
    if !request_target.starts_with('/') || request_target.starts_with("//") {
        bail!("OpenAI proxy requires origin-form request targets starting with /");
    }
    let mut base = base_url.clone();
    let base_path = base
        .path()
        .trim_end_matches('/')
        .trim_start_matches('/')
        .to_string();
    if !base.path().ends_with('/') {
        let path = format!("{}/", base.path());
        base.set_path(&path);
    }

    let (path, query) = request_target
        .split_once('?')
        .map_or((request_target, None), |(path, query)| (path, Some(query)));
    reject_unsafe_request_path(path)?;
    let mut relative_path = path.trim_start_matches('/');
    if !base_path.is_empty()
        && (relative_path == base_path || relative_path.starts_with(&format!("{base_path}/")))
    {
        relative_path = relative_path[base_path.len()..].trim_start_matches('/');
    }

    let mut url = base
        .join(relative_path)
        .context("join provider request path")?;
    url.set_query(query);
    Ok(url)
}

fn reject_unsafe_request_path(path: &str) -> Result<()> {
    for segment in path.split('/') {
        let decoded = percent_decode_path_segment(segment)?;
        if decoded == b"." || decoded == b".." {
            bail!("request target contains unsafe dot segment");
        }
        if decoded.iter().any(|byte| matches!(byte, b'/' | b'\\')) {
            bail!("request target contains encoded path separator");
        }
    }
    Ok(())
}

fn percent_decode_path_segment(segment: &str) -> Result<Vec<u8>> {
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                bail!("request target contains invalid percent encoding");
            }
            let high = hex_value(bytes[index + 1])?;
            let low = hex_value(bytes[index + 2])?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Ok(decoded)
}

fn hex_value(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => bail!("request target contains invalid percent encoding"),
    }
}

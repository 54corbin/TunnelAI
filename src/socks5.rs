use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetAddr {
    Ip(IpAddr),
    Domain(String),
}

impl std::fmt::Display for TargetAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetAddr::Ip(ip) => write!(f, "{ip}"),
            TargetAddr::Domain(domain) => write!(f, "{domain}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRequest {
    pub target: TargetAddr,
    pub port: u16,
}

impl ConnectRequest {
    pub fn target_display(&self) -> String {
        format!("{}:{}", self.target, self.port)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReplyCode {
    Succeeded = 0x00,
    GeneralFailure = 0x01,
    ConnectionNotAllowed = 0x02,
    NetworkUnreachable = 0x03,
    HostUnreachable = 0x04,
    ConnectionRefused = 0x05,
    TtlExpired = 0x06,
    CommandNotSupported = 0x07,
    AddressTypeNotSupported = 0x08,
}

impl TryFrom<u8> for ReplyCode {
    type Error = SocksError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Succeeded),
            0x01 => Ok(Self::GeneralFailure),
            0x02 => Ok(Self::ConnectionNotAllowed),
            0x03 => Ok(Self::NetworkUnreachable),
            0x04 => Ok(Self::HostUnreachable),
            0x05 => Ok(Self::ConnectionRefused),
            0x06 => Ok(Self::TtlExpired),
            0x07 => Ok(Self::CommandNotSupported),
            0x08 => Ok(Self::AddressTypeNotSupported),
            other => Err(SocksError::InvalidReplyCode(other)),
        }
    }
}

#[derive(Debug, Error)]
pub enum SocksError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unsupported SOCKS version {0}")]
    UnsupportedVersion(u8),
    #[error("no supported authentication method")]
    NoSupportedAuthMethod,
    #[error("unsupported command {0}")]
    UnsupportedCommand(u8),
    #[error("unsupported address type {0}")]
    UnsupportedAddressType(u8),
    #[error("invalid reserved byte {0}")]
    InvalidReserved(u8),
    #[error("invalid domain name")]
    InvalidDomain,
    #[error("invalid reply code {0}")]
    InvalidReplyCode(u8),
}

impl SocksError {
    pub fn reply_code(&self) -> ReplyCode {
        match self {
            SocksError::UnsupportedCommand(_) => ReplyCode::CommandNotSupported,
            SocksError::UnsupportedAddressType(_) => ReplyCode::AddressTypeNotSupported,
            _ => ReplyCode::GeneralFailure,
        }
    }
}

pub fn validate_domain(domain: &str) -> bool {
    !domain.is_empty()
        && !domain
            .bytes()
            .any(|byte| byte == 0 || byte.is_ascii_control() || byte.is_ascii_whitespace())
}

pub async fn negotiate_no_auth<S>(stream: &mut S) -> Result<(), SocksError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let version = stream.read_u8().await?;
    if version != 0x05 {
        return Err(SocksError::UnsupportedVersion(version));
    }

    let nmethods = stream.read_u8().await? as usize;
    let mut methods = vec![0u8; nmethods];
    stream.read_exact(&mut methods).await?;

    if methods.contains(&0x00) {
        stream.write_all(&[0x05, 0x00]).await?;
        stream.flush().await?;
        Ok(())
    } else {
        stream.write_all(&[0x05, 0xff]).await?;
        stream.flush().await?;
        Err(SocksError::NoSupportedAuthMethod)
    }
}

pub async fn read_connect_request<S>(stream: &mut S) -> Result<ConnectRequest, SocksError>
where
    S: AsyncRead + Unpin,
{
    let version = stream.read_u8().await?;
    if version != 0x05 {
        return Err(SocksError::UnsupportedVersion(version));
    }

    let command = stream.read_u8().await?;
    let reserved = stream.read_u8().await?;
    let atyp = stream.read_u8().await?;

    if command != 0x01 {
        return Err(SocksError::UnsupportedCommand(command));
    }
    if reserved != 0x00 {
        return Err(SocksError::InvalidReserved(reserved));
    }

    let target = match atyp {
        0x01 => {
            let mut octets = [0u8; 4];
            stream.read_exact(&mut octets).await?;
            TargetAddr::Ip(IpAddr::V4(Ipv4Addr::from(octets)))
        }
        0x03 => {
            let len = stream.read_u8().await? as usize;
            let mut bytes = vec![0u8; len];
            stream.read_exact(&mut bytes).await?;
            let domain = String::from_utf8(bytes).map_err(|_| SocksError::InvalidDomain)?;
            if !validate_domain(&domain) {
                return Err(SocksError::InvalidDomain);
            }
            TargetAddr::Domain(domain)
        }
        0x04 => {
            let mut octets = [0u8; 16];
            stream.read_exact(&mut octets).await?;
            TargetAddr::Ip(IpAddr::V6(Ipv6Addr::from(octets)))
        }
        other => return Err(SocksError::UnsupportedAddressType(other)),
    };

    let port = stream.read_u16().await?;
    Ok(ConnectRequest { target, port })
}

pub async fn write_reply<S>(stream: &mut S, code: ReplyCode) -> Result<(), SocksError>
where
    S: AsyncWrite + Unpin,
{
    stream
        .write_all(&[0x05, code as u8, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await?;
    stream.flush().await?;
    Ok(())
}

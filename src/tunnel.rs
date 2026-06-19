use crate::socks5::{ConnectRequest, ReplyCode, TargetAddr, validate_domain};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAGIC: &[u8; 4] = b"IRPX";
pub const VERSION: u8 = 1;
pub const CMD_CONNECT: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunnelRequest(pub ConnectRequest);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TunnelResponse {
    pub status: ReplyCode,
}

#[derive(Debug, Error)]
pub enum TunnelError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("bad tunnel magic")]
    BadMagic,
    #[error("unsupported tunnel version {0}")]
    BadVersion(u8),
    #[error("unsupported tunnel command {0}")]
    UnsupportedCommand(u8),
    #[error("unsupported tunnel address type {0}")]
    UnsupportedAddressType(u8),
    #[error("domain name is too long for tunnel request")]
    DomainTooLong,
    #[error("invalid domain name")]
    InvalidDomain,
    #[error("invalid tunnel response status {0}")]
    InvalidStatus(u8),
}

pub async fn write_request<W>(writer: &mut W, request: &ConnectRequest) -> Result<(), TunnelError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(MAGIC).await?;
    writer.write_u8(VERSION).await?;
    writer.write_u8(CMD_CONNECT).await?;

    match &request.target {
        TargetAddr::Ip(IpAddr::V4(ip)) => {
            writer.write_u8(0x01).await?;
            writer.write_u16(request.port).await?;
            writer.write_all(&ip.octets()).await?;
        }
        TargetAddr::Domain(domain) => {
            if !validate_domain(domain) {
                return Err(TunnelError::InvalidDomain);
            }
            let bytes = domain.as_bytes();
            if bytes.len() > u8::MAX as usize {
                return Err(TunnelError::DomainTooLong);
            }
            writer.write_u8(0x03).await?;
            writer.write_u16(request.port).await?;
            writer.write_u8(bytes.len() as u8).await?;
            writer.write_all(bytes).await?;
        }
        TargetAddr::Ip(IpAddr::V6(ip)) => {
            writer.write_u8(0x04).await?;
            writer.write_u16(request.port).await?;
            writer.write_all(&ip.octets()).await?;
        }
    }
    writer.flush().await?;
    Ok(())
}

pub async fn read_request<R>(reader: &mut R) -> Result<ConnectRequest, TunnelError>
where
    R: AsyncRead + Unpin,
{
    read_magic_and_version(reader).await?;
    let command = reader.read_u8().await?;
    if command != CMD_CONNECT {
        return Err(TunnelError::UnsupportedCommand(command));
    }
    let atyp = reader.read_u8().await?;
    let port = reader.read_u16().await?;
    let target = match atyp {
        0x01 => {
            let mut octets = [0u8; 4];
            reader.read_exact(&mut octets).await?;
            TargetAddr::Ip(IpAddr::V4(Ipv4Addr::from(octets)))
        }
        0x03 => {
            let len = reader.read_u8().await? as usize;
            let mut bytes = vec![0u8; len];
            reader.read_exact(&mut bytes).await?;
            let domain = String::from_utf8(bytes).map_err(|_| TunnelError::InvalidDomain)?;
            if !validate_domain(&domain) {
                return Err(TunnelError::InvalidDomain);
            }
            TargetAddr::Domain(domain)
        }
        0x04 => {
            let mut octets = [0u8; 16];
            reader.read_exact(&mut octets).await?;
            TargetAddr::Ip(IpAddr::V6(Ipv6Addr::from(octets)))
        }
        other => return Err(TunnelError::UnsupportedAddressType(other)),
    };
    Ok(ConnectRequest { target, port })
}

pub async fn write_response<W>(writer: &mut W, status: ReplyCode) -> Result<(), TunnelError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(MAGIC).await?;
    writer.write_u8(VERSION).await?;
    writer.write_u8(status as u8).await?;
    writer.write_u16(0).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_response<R>(reader: &mut R) -> Result<ReplyCode, TunnelError>
where
    R: AsyncRead + Unpin,
{
    read_magic_and_version(reader).await?;
    let status = reader.read_u8().await?;
    let _reserved = reader.read_u16().await?;
    ReplyCode::try_from(status).map_err(|_| TunnelError::InvalidStatus(status))
}

async fn read_magic_and_version<R>(reader: &mut R) -> Result<(), TunnelError>
where
    R: AsyncRead + Unpin,
{
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic).await?;
    if &magic != MAGIC {
        return Err(TunnelError::BadMagic);
    }
    let version = reader.read_u8().await?;
    if version != VERSION {
        return Err(TunnelError::BadVersion(version));
    }
    Ok(())
}

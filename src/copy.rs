use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

pub async fn pump_bidirectional<LR, LW, RR, RW>(
    mut left_read: LR,
    mut left_write: LW,
    mut right_read: RR,
    mut right_write: RW,
) -> anyhow::Result<(u64, u64)>
where
    LR: AsyncRead + Unpin,
    LW: AsyncWrite + Unpin,
    RR: AsyncRead + Unpin,
    RW: AsyncWrite + Unpin,
{
    let left_to_right = async {
        let copied = tokio::io::copy(&mut left_read, &mut right_write).await?;
        right_write.shutdown().await?;
        anyhow::Ok(copied)
    };

    let right_to_left = async {
        let copied = tokio::io::copy(&mut right_read, &mut left_write).await?;
        left_write.shutdown().await?;
        anyhow::Ok(copied)
    };

    let (left_to_right, right_to_left) = tokio::try_join!(left_to_right, right_to_left)?;
    Ok((left_to_right, right_to_left))
}

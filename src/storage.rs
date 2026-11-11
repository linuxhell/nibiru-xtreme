use anyhow::{Result, anyhow};
use std::{fs::File, io, sync::Arc};

pub(crate) mod checkpoints;

pub async fn write_at<D>(file: Arc<File>, offset: u64, data: D) -> Result<()>
where
    D: AsRef<[u8]> + Send + 'static,
{
    write_at_reusing(file, offset, data).await.map(|_| ())
}
/// Return the allocation only after the positional write completes.
pub(crate) async fn write_at_reusing<D>(file: Arc<File>, offset: u64, data: D) -> Result<D>
where
    D: AsRef<[u8]> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        write_all_at(&file, offset, data.as_ref())?;
        Ok::<_, io::Error>(data)
    })
    .await
    .map_err(|error| anyhow!("storage worker failed: {error}"))?
    .map_err(Into::into)
}

#[cfg(unix)]
fn write_all_at(file: &File, mut offset: u64, mut data: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    while !data.is_empty() {
        let written = file.write_at(data, offset)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "zero-byte positional write",
            ));
        }
        offset = offset.saturating_add(written as u64);
        data = &data[written..];
    }
    Ok(())
}

#[cfg(windows)]
fn write_all_at(file: &File, mut offset: u64, mut data: &[u8]) -> io::Result<()> {
    use std::os::windows::fs::FileExt;

    while !data.is_empty() {
        let written = file.seek_write(data, offset)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "zero-byte positional write",
            ));
        }
        offset = offset.saturating_add(written as u64);
        data = &data[written..];
    }
    Ok(())
}

use anyhow::{Result, anyhow};
use std::{fs::File, io, sync::Arc};

pub async fn write_at(file: Arc<File>, offset: u64, data: Vec<u8>) -> Result<()> {
    tokio::task::spawn_blocking(move || write_all_at(&file, offset, &data))
        .await
        .map_err(|error| anyhow!("storage worker failed: {error}"))??;
    Ok(())
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
            return Err(io::Error::new(io::ErrorKind::WriteZero, "zero-byte positional write"));
        }
        offset = offset.saturating_add(written as u64);
        data = &data[written..];
    }
    Ok(())
}

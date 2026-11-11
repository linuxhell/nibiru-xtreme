use std::{
    fs::{File, OpenOptions},
    io,
    os::windows::io::AsRawHandle,
    path::Path,
};
use windows_sys::Win32::{
    Storage::FileSystem::{
        FILE_COMPRESSION_INFO, FileCompressionInfo, GetFileInformationByHandleEx,
    },
    System::{IO::DeviceIoControl, Ioctl::FSCTL_SET_SPARSE},
};
/// Owns a synchronous handle; arbitrary or overlapped handles are not accepted.
pub struct SparseFile {
    file: File,
    sparse: bool,
    preparation_error: Option<io::Error>,
}
impl SparseFile {
    pub fn open(path: &Path, fresh: bool, length: u64) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(fresh)
            .open(path)?;
        let mut this = Self {
            file,
            sparse: false,
            preparation_error: None,
        };
        match this.set_sparse(true) {
            Ok(()) => this.sparse = true,
            Err(error) => this.preparation_error = Some(error),
        }
        if fresh {
            this.file.set_len(length)?;
        }
        Ok(this)
    }
    pub fn file(&self) -> &File {
        &self.file
    }
    pub fn is_sparse(&self) -> bool {
        self.sparse
    }
    pub fn preparation_error(&self) -> Option<&io::Error> {
        self.preparation_error.as_ref()
    }
    /// Call after all blocks are written. Failure preserves a valid sparse file.
    pub fn finish(&mut self) -> io::Result<()> {
        if self.sparse {
            self.set_sparse(false)?;
            self.sparse = false;
        }
        Ok(())
    }
    fn set_sparse(&self, enabled: bool) -> io::Result<()> {
        let flag = u8::from(enabled);
        let mut returned = 0;
        // SAFETY: this live handle was opened synchronously above. The one-byte
        // BOOLEAN input and output count outlive this synchronous call. No
        // output buffer or OVERLAPPED operation exists; ownership is retained.
        let ok = unsafe {
            DeviceIoControl(
                self.file.as_raw_handle(),
                FSCTL_SET_SPARSE,
                (&flag as *const u8).cast(),
                1,
                std::ptr::null_mut(),
                0,
                &mut returned,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    pub fn allocated_bytes(&self) -> io::Result<u64> {
        let mut info = FILE_COMPRESSION_INFO::default();
        // SAFETY: a live borrowed handle and initialized, correctly sized output
        // struct. This synchronous query retains neither pointer nor handle.
        let ok = unsafe {
            GetFileInformationByHandleEx(
                self.file.as_raw_handle(),
                FileCompressionInfo,
                (&mut info as *mut FILE_COMPRESSION_INFO).cast(),
                std::mem::size_of_val(&info) as u32,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        u64::try_from(info.CompressedFileSize)
            .map_err(|_| io::Error::other("negative allocated file size"))
    }
}

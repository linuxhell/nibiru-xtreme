#![cfg(windows)]
use nibiru_platform::SparseFile;
use std::{
    io::{Read, Seek, SeekFrom, Write},
    os::windows::fs::MetadataExt,
};
#[test]
fn sparse_preallocation_preserves_holes_and_clears_after_full_write() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("partial");
    let mut prepared = SparseFile::open(&path, true, 2 << 30).unwrap();
    assert!(prepared.is_sparse(), "{:?}", prepared.preparation_error());
    let mut file = prepared.file().try_clone().unwrap();
    file.seek(SeekFrom::Start(1 << 30)).unwrap();
    file.write_all(&[0x5a; 4096]).unwrap();
    file.sync_all().unwrap();
    assert!(prepared.allocated_bytes().unwrap() < 64 << 20);
    let mut bytes = [1; 4096];
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, [0; 4096]);
    file.seek(SeekFrom::Start(1 << 30)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, [0x5a; 4096]);
    file.set_len(8192).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[0x37; 8192]).unwrap();
    prepared.finish().unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().file_attributes() & 0x200,
        0
    );
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, [0x37; 4096]);
}

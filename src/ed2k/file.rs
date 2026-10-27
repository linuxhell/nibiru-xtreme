use anyhow::{Result, bail};
use md4::{Digest, Md4};
use serde::{Deserialize, Serialize};

pub const PART_SIZE: u64 = 9_728_000;
pub const BLOCK_SIZE: u64 = 184_320;

/// Validated single-file ED2K link. Names are data, never directory paths.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileLink {
    pub name: String,
    pub size: u64,
    pub hash: [u8; 16],
}
impl FileLink {
    pub fn parse(link: &str) -> Result<Self> {
        if link.len() > 4096 || link.chars().any(char::is_control) {
            bail!("invalid ED2K file link");
        }
        let fields: Vec<_> = link.split('|').collect();
        if fields.len() != 6 || fields[0] != "ed2k://" || fields[1] != "file" || fields[5] != "/" {
            bail!("expected a standard ED2K file link");
        }
        let name = percent_encoding::percent_decode_str(fields[2])
            .decode_utf8()?
            .into_owned();
        if name.len() > 240 || !crate::file_names::portable_component(&name) {
            bail!("ED2K filename is not a safe single filename");
        }
        if fields[3].is_empty() || !fields[3].bytes().all(|b| b.is_ascii_digit()) {
            bail!("invalid ED2K file size");
        }
        let size: u64 = fields[3].parse()?;
        // The baseline hashset uses a 16-bit count. Bound memory and disk seeks.
        if size == 0 || size / PART_SIZE >= u16::MAX as u64 {
            bail!("ED2K file size exceeds supported hashset bounds");
        }
        Ok(Self {
            name,
            size,
            hash: super::server::file_hash(fields[4])?,
        })
    }
    pub fn hash_hex(&self) -> String {
        self.hash.iter().map(|b| format!("{b:02x}")).collect()
    }
    pub fn link(&self) -> String {
        let name =
            percent_encoding::utf8_percent_encode(&self.name, percent_encoding::NON_ALPHANUMERIC);
        format!("ed2k://|file|{name}|{}|{}|/", self.size, self.hash_hex())
    }
    pub fn part_count(&self) -> usize {
        self.size.div_ceil(PART_SIZE) as usize
    }
    pub fn part_len(&self, index: usize) -> usize {
        self.size
            .saturating_sub(index as u64 * PART_SIZE)
            .min(PART_SIZE) as usize
    }
    pub(crate) fn validate_hashset(&self, hashes: Vec<[u8; 16]>) -> Result<Vec<[u8; 16]>> {
        if self.size < PART_SIZE {
            if !hashes.is_empty() && hashes != [self.hash] {
                bail!("invalid single-part ED2K hashset");
            }
            return Ok(vec![self.hash]);
        }
        if hashes.len() != (self.size / PART_SIZE + 1) as usize {
            bail!("ED2K hashset count does not match file size");
        }
        if self.size.is_multiple_of(PART_SIZE) && hashes.last() != Some(&digest(&[])) {
            bail!("ED2K hashset has invalid final empty part");
        }
        let mut root = Md4::new();
        for hash in &hashes {
            root.update(hash);
        }
        if <[u8; 16]>::from(root.finalize()) != self.hash {
            bail!("ED2K hashset does not match the requested root hash");
        }
        Ok(hashes)
    }
}
pub(crate) fn digest(bytes: &[u8]) -> [u8; 16] {
    Md4::digest(bytes).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn links_round_trip_and_cannot_escape_destination() {
        let link = "ed2k://|file|hello%20world.txt|42|0123456789abcdef0123456789ABCDEF|/";
        let parsed = FileLink::parse(link).unwrap();
        assert_eq!(parsed.name, "hello world.txt");
        assert_eq!(FileLink::parse(&parsed.link()).unwrap(), parsed);
        for name in [
            "..",
            "%2e%2e%2ftest",
            "a%5cb",
            "CON.txt",
            "LPT1",
            "a%00b",
            "a%3ab",
            "a.",
            "CON%20.txt",
            "COM%C2%B9.txt",
            "LPT%C2%B2",
            "CONOUT%24.txt",
            "movie%C2%A0",
            "%FF",
        ] {
            assert!(
                FileLink::parse(&link.replace("hello%20world.txt", name)).is_err(),
                "{name}"
            );
        }
        for size in ["0", "-1", "+1", "18446744073709551616"] {
            assert!(FileLink::parse(&link.replace("|42|", &format!("|{size}|"))).is_err());
        }
    }
    #[test]
    fn ed2k_hashsets_use_md4_and_include_empty_exact_boundary_part() {
        assert_eq!(
            digest(b"abc"),
            [
                0xa4, 0x48, 0x01, 0x7a, 0xaf, 0x21, 0xd8, 0x52, 0x5f, 0xc1, 0x0a, 0xe8, 0x7a, 0xa6,
                0x72, 0x9d
            ]
        );
        let first = digest(b"part fixture");
        let empty = digest(&[]);
        let root = digest(&[first, empty].concat());
        let file = FileLink {
            name: "test".into(),
            size: PART_SIZE,
            hash: root,
        };
        assert_eq!(file.part_count(), 1);
        assert_eq!(file.validate_hashset(vec![first, empty]).unwrap().len(), 2);
        assert!(file.validate_hashset(vec![first]).is_err());
        assert!(file.validate_hashset(vec![first, [0; 16]]).is_err());
        let file = FileLink {
            name: "test".into(),
            size: PART_SIZE + 1,
            hash: root,
        };
        assert_eq!(file.part_count(), 2);
        assert_eq!(file.part_len(1), 1);
    }
}

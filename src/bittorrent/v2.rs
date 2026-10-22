//! BEP 52 file trees, SHA-256 Merkle roots and authenticated piece layers.
use super::{
    bencode::Value,
    metainfo::{FileEntry, sanitize_component},
};
use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};

pub const BLOCK: u64 = 16 * 1024;
const MAX_FILES: usize = 100_000;
const MAX_PIECES: usize = 1_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct File {
    pub root: [u8; 32],
    pub length: u64,
    pub offset: u64,
    pub first_piece: usize,
    pub pieces: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct V2Info {
    pub info_hash: [u8; 32],
    pub files: Vec<File>,
    pub layout: Vec<FileEntry>,
    pub hashes: Vec<Option<[u8; 32]>>,
    pub total: u64,
    pub piece_length: u64,
}

pub fn pair(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(a);
    h.update(b);
    h.finalize().into()
}

pub fn zero_at_layer(layer: u32) -> [u8; 32] {
    let mut h = [0; 32];
    for _ in 0..layer {
        h = pair(&h, &h);
    }
    h
}

pub fn root(hashes: &[[u8; 32]], padding: [u8; 32]) -> Result<[u8; 32]> {
    if hashes.is_empty() || hashes.len() > MAX_PIECES {
        bail!("invalid Merkle layer size");
    }
    let mut layer = hashes.to_vec();
    layer.resize(hashes.len().next_power_of_two(), padding);
    while layer.len() > 1 {
        layer = layer.chunks_exact(2).map(|p| pair(&p[0], &p[1])).collect();
    }
    Ok(layer[0])
}

/// The final data block is hashed at its real length; absent leaves are zero
/// hashes, not hashes of zero-filled 16 KiB blocks.
pub fn piece_root(data: &[u8], leaf_capacity: usize) -> Result<[u8; 32]> {
    if data.is_empty() || !leaf_capacity.is_power_of_two() || leaf_capacity > 8192 {
        bail!("invalid v2 piece geometry");
    }
    let mut leaves: Vec<[u8; 32]> = data
        .chunks(BLOCK as usize)
        .map(|b| Sha256::digest(b).into())
        .collect();
    if leaves.len() > leaf_capacity {
        bail!("piece exceeds Merkle subtree");
    }
    leaves.resize(leaf_capacity, [0; 32]);
    root(&leaves, [0; 32])
}

fn tree(
    node: &Value,
    path: &mut Vec<String>,
    files: &mut Vec<(Vec<String>, u64, [u8; 32])>,
    names: &mut HashSet<Vec<String>>,
) -> Result<()> {
    if path.len() > 32 || files.len() > MAX_FILES {
        bail!("v2 file tree exceeds bounds");
    }
    let map = node
        .dict()
        .context("v2 file tree node is not a dictionary")?;
    if let Some(properties) = map.get(&Vec::new()) {
        if path.is_empty() || map.len() != 1 {
            bail!("v2 file cannot also be a directory");
        }
        if properties
            .get("attr")
            .and_then(Value::str)
            .is_some_and(|a| a.contains('l'))
        {
            bail!("v2 symlinks are not supported");
        }
        let length = u64::try_from(
            properties
                .get("length")
                .and_then(Value::int)
                .context("v2 file has no length")?,
        )?;
        if length > (1 << 50) {
            bail!("v2 file is too large");
        }
        let hash = if length == 0 {
            [0; 32]
        } else {
            properties
                .get("pieces root")
                .and_then(Value::bytes)
                .context("nonempty v2 file has no root")?
                .try_into()
                .context("v2 root must be 32 bytes")?
        };
        if !names.insert(path.clone()) {
            bail!("v2 sanitized file paths collide");
        }
        files.push((path.clone(), length, hash));
    } else {
        for (component, child) in map {
            if component.is_empty() {
                bail!("empty v2 path component");
            }
            path.push(sanitize_component(&String::from_utf8_lossy(component)));
            tree(child, path, files, names)?;
            path.pop();
        }
    }
    Ok(())
}

impl V2Info {
    pub fn request(&self, file: &File, index: usize) -> super::message::HashRequest {
        let length = file.pieces.next_power_of_two().clamp(2, 128);
        super::message::HashRequest {
            root: file.root,
            base_layer: (self.piece_length / BLOCK).ilog2(),
            index: (index / length * length) as u32,
            length: length as u32,
            proof_layers: file.pieces.next_power_of_two().ilog2().saturating_sub(1),
        }
    }

    pub fn geometry(
        &self,
        file: &File,
        request: &super::message::HashRequest,
    ) -> Result<(u32, u32)> {
        let base = (self.piece_length / BLOCK).ilog2();
        let top = file.length.div_ceil(BLOCK).next_power_of_two().ilog2();
        let r = request;
        if r.root != file.root
            || (r.base_layer != 0 && r.base_layer != base)
            || r.base_layer > top
            || r.length < 2
            || r.length > 512
            || !r.length.is_power_of_two()
            || !r.index.is_multiple_of(r.length)
            || r.index
                .checked_add(r.length)
                .is_none_or(|end| end as u64 > 1u64 << (top - r.base_layer))
            || r.proof_layers > (top - r.base_layer).saturating_sub(1)
        {
            bail!("invalid BEP52 hash request geometry");
        }
        let level = r.length.ilog2();
        let proofs = r.proof_layers.saturating_sub(level.saturating_sub(1));
        Ok((level, proofs))
    }

    pub fn verify_response(
        &self,
        file: &File,
        request: &super::message::HashRequest,
        hashes: &[[u8; 32]],
    ) -> Result<()> {
        let (level, proofs) = self.geometry(file, request)?;
        let top = file.length.div_ceil(BLOCK).next_power_of_two().ilog2();
        if request.base_layer != (self.piece_length / BLOCK).ilog2()
            || request.base_layer + level + proofs != top
            || hashes.len() != request.length as usize + proofs as usize
        {
            bail!("incomplete BEP52 Merkle proof");
        }
        let mut hash = root(
            &hashes[..request.length as usize],
            zero_at_layer(request.base_layer),
        )?;
        let mut index = request.index / request.length;
        for uncle in &hashes[request.length as usize..] {
            hash = if index & 1 == 0 {
                pair(&hash, uncle)
            } else {
                pair(uncle, &hash)
            };
            index >>= 1;
        }
        if hash != file.root {
            bail!("BEP52 hash proof does not match the file root");
        }
        for (i, hash) in hashes[..request.length as usize].iter().enumerate() {
            if request.index as usize + i >= file.pieces
                && *hash != zero_at_layer(request.base_layer)
            {
                bail!("invalid padding hash in v2 layer");
            }
        }
        Ok(())
    }

    pub fn parse(info: &Value, raw: &[u8], piece_length: u64) -> Result<Self> {
        if info.get("meta version").and_then(Value::int) != Some(2) {
            bail!("unsupported torrent meta version");
        }
        if piece_length < BLOCK
            || !piece_length.is_power_of_two()
            || piece_length > 128 * 1024 * 1024
        {
            bail!("invalid v2 piece length");
        }
        let mut entries = vec![];
        tree(
            info.get("file tree")
                .context("v2 torrent has no file tree")?,
            &mut vec![],
            &mut entries,
            &mut HashSet::new(),
        )?;
        if entries.is_empty() || entries.len() > MAX_FILES {
            bail!("empty or oversized v2 file tree");
        }
        let mut out = Self {
            info_hash: Sha256::digest(raw).into(),
            files: vec![],
            layout: vec![],
            hashes: vec![],
            total: 0,
            piece_length,
        };
        for (path, length, hash) in entries {
            if length == 0 {
                out.layout.push(FileEntry {
                    path,
                    length,
                    offset: out.total,
                    pad: false,
                });
                continue;
            }
            let aligned = out
                .total
                .div_ceil(piece_length)
                .checked_mul(piece_length)
                .context("v2 alignment overflow")?;
            if aligned > out.total {
                out.layout.push(FileEntry {
                    path: vec![".pad".into(), out.total.to_string()],
                    length: aligned - out.total,
                    offset: out.total,
                    pad: true,
                });
            }
            let first = (aligned / piece_length) as usize;
            let pieces = length.div_ceil(piece_length) as usize;
            if first.checked_add(pieces).is_none_or(|n| n > MAX_PIECES) {
                bail!("v2 piece count exceeds limit");
            }
            out.hashes.resize(first + pieces, None);
            if pieces == 1 {
                out.hashes[first] = Some(hash);
            }
            out.files.push(File {
                root: hash,
                length,
                offset: aligned,
                first_piece: first,
                pieces,
            });
            out.layout.push(FileEntry {
                path,
                length,
                offset: aligned,
                pad: false,
            });
            out.total = aligned
                .checked_add(length)
                .filter(|n| *n <= 1 << 50)
                .context("v2 torrent is too large")?;
        }
        if out.files.is_empty() {
            bail!("v2 torrent has no data");
        }
        Ok(out)
    }

    pub fn file_for(&self, piece: usize) -> Option<&File> {
        let i = self
            .files
            .partition_point(|f| f.first_piece + f.pieces <= piece);
        self.files.get(i).filter(|f| piece >= f.first_piece)
    }

    pub fn piece_len(&self, piece: usize) -> u64 {
        self.file_for(piece)
            .map(|f| {
                (f.length - (piece - f.first_piece) as u64 * self.piece_length)
                    .min(self.piece_length)
            })
            .unwrap_or(0)
    }

    pub fn leaf_capacity(&self, piece: usize) -> usize {
        self.file_for(piece)
            .map(|f| {
                if f.pieces == 1 {
                    f.length.div_ceil(BLOCK).next_power_of_two() as usize
                } else {
                    (self.piece_length / BLOCK) as usize
                }
            })
            .unwrap_or(0)
    }

    pub fn validate_layer(&self, file: &File, hashes: &[[u8; 32]]) -> Result<()> {
        if hashes.len() != file.pieces {
            bail!("v2 piece layer has the wrong length");
        }
        let padding = zero_at_layer((self.piece_length / BLOCK).ilog2());
        if root(hashes, padding)? != file.root {
            bail!("v2 piece layer does not match its file root");
        }
        Ok(())
    }

    pub fn load_layers(&mut self, layers: Option<&Value>, required: bool) -> Result<()> {
        for file in &self.files {
            if file.pieces == 1 {
                continue;
            }
            let bytes = layers
                .and_then(Value::dict)
                .and_then(|m| m.get(file.root.as_slice()))
                .and_then(Value::bytes);
            let Some(bytes) = bytes else {
                if required {
                    bail!("missing v2 piece layer");
                }
                continue;
            };
            if bytes.len() != file.pieces * 32 {
                bail!("invalid v2 piece layer byte length");
            }
            let hashes: Vec<[u8; 32]> = bytes
                .chunks_exact(32)
                .map(|h| h.try_into().expect("32 bytes"))
                .collect();
            self.validate_layer(file, &hashes)?;
            for (i, h) in hashes.into_iter().enumerate() {
                self.hashes[file.first_piece + i] = Some(h);
            }
        }
        Ok(())
    }

    pub fn layers_value(&self) -> Value {
        let mut layers = BTreeMap::new();
        for file in &self.files {
            if file.pieces == 1 {
                continue;
            }
            if let Some(hashes) = self.hashes[file.first_piece..file.first_piece + file.pieces]
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
            {
                layers.insert(
                    file.root.to_vec(),
                    Value::Bytes(hashes.into_iter().flatten().collect()),
                );
            }
        }
        Value::Dict(layers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partial_blocks_and_zero_leaf_padding_match_libtorrent_vectors() {
        let small = piece_root(&vec![b'a'; 20_000], 2).unwrap();
        let expected = "4e15d8ed29f74ed4b4953e205673b4b46c63f7a717d693a6d27e7e8373354d48";
        assert_eq!(
            small.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            expected
        );
        let first = piece_root(&vec![b'b'; 65536], 4).unwrap();
        let last = piece_root(&vec![b'b'; 34464], 4).unwrap();
        let root = pair(&first, &last);
        assert_eq!(
            root.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "88a67c2acf1e59cec68abd04b5eacfce86411f81d1d1a85108f0a2a2830f8297"
        );
    }
}

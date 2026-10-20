//! A fixed-size bit set in BitTorrent wire order (bit 7 of byte 0 is piece 0).

use anyhow::{Result, bail};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bitfield {
    bytes: Vec<u8>,
    len: usize,
}

impl Bitfield {
    pub fn new(len: usize) -> Self {
        Self {
            bytes: vec![0; len.div_ceil(8)],
            len,
        }
    }

    pub fn full(len: usize) -> Self {
        let mut field = Self::new(len);
        for i in 0..len {
            field.set(i);
        }
        field
    }

    /// Validates a bitfield received from a peer: exact length, and spare bits must be zero.
    pub fn from_wire(bytes: &[u8], len: usize) -> Result<Self> {
        if bytes.len() != len.div_ceil(8) {
            bail!(
                "bitfield has {} bytes, expected {}",
                bytes.len(),
                len.div_ceil(8)
            );
        }
        if !len.is_multiple_of(8) && bytes[bytes.len() - 1] & (0xffu8 >> (len % 8)) != 0 {
            bail!("bitfield has spare bits set");
        }
        Ok(Self {
            bytes: bytes.to_vec(),
            len,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, index: usize) -> bool {
        index < self.len && self.bytes[index / 8] & (0x80 >> (index % 8)) != 0
    }

    pub fn set(&mut self, index: usize) {
        if index < self.len {
            self.bytes[index / 8] |= 0x80 >> (index % 8);
        }
    }

    pub fn clear(&mut self, index: usize) {
        if index < self.len {
            self.bytes[index / 8] &= !(0x80 >> (index % 8));
        }
    }

    pub fn count(&self) -> usize {
        self.bytes.iter().map(|b| b.count_ones() as usize).sum()
    }

    pub fn is_complete(&self) -> bool {
        self.count() == self.len
    }

    pub fn iter_set(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len).filter(|i| self.get(*i))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_wire_bit_order_and_counts() {
        let mut f = Bitfield::new(10);
        f.set(0);
        f.set(9);
        assert_eq!(f.as_bytes(), &[0b1000_0000, 0b0100_0000]);
        assert!(f.get(0) && f.get(9) && !f.get(1));
        assert_eq!(f.count(), 2);
        f.clear(0);
        assert_eq!(f.iter_set().collect::<Vec<_>>(), vec![9]);
        f.set(100); // out of range is ignored
        assert!(!f.get(100));
    }

    #[test]
    fn full_fields_are_complete_and_leave_spare_bits_clear() {
        let f = Bitfield::full(10);
        assert!(f.is_complete());
        assert_eq!(f.as_bytes(), &[0xff, 0b1100_0000]);
        assert!(Bitfield::from_wire(f.as_bytes(), 10).is_ok());
    }

    #[test]
    fn rejects_wrong_lengths_and_spare_bits() {
        assert!(Bitfield::from_wire(&[0xff], 10).is_err());
        assert!(Bitfield::from_wire(&[0xff, 0xff, 0], 10).is_err());
        assert!(Bitfield::from_wire(&[0xff, 0b1100_0001], 10).is_err());
        assert!(Bitfield::from_wire(&[], 0).is_ok());
    }
}

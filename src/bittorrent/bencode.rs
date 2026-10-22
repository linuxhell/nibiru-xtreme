//! Bencode (BEP 3): a bounded decoder that remembers where the `info` dictionary sits in the
//! input, because the torrent's identity is the SHA-1 of those exact bytes.

use anyhow::{Result, bail};
use std::{collections::BTreeMap, ops::Range};

/// Nesting limit; real torrents need fewer than ten levels.
const MAX_DEPTH: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(BTreeMap<Vec<u8>, Value>),
}

impl Value {
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Dict(map) => map.get(key.as_bytes()),
            _ => None,
        }
    }
    pub fn int(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }
    pub fn bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }
    pub fn str(&self) -> Option<&str> {
        self.bytes().and_then(|b| std::str::from_utf8(b).ok())
    }
    pub fn list(&self) -> Option<&[Value]> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }
    pub fn dict(&self) -> Option<&BTreeMap<Vec<u8>, Value>> {
        match self {
            Value::Dict(d) => Some(d),
            _ => None,
        }
    }
    pub fn text(s: &str) -> Value {
        Value::Bytes(s.as_bytes().to_vec())
    }
}

/// A decoded document plus the byte range of its top-level `info` value, when it has one.
#[derive(Debug)]
pub struct Decoded {
    pub value: Value,
    pub info_span: Option<Range<usize>>,
}

struct Parser<'a> {
    data: &'a [u8],
    pos: usize,
    info_span: Option<Range<usize>>,
}

impl Parser<'_> {
    fn peek(&self) -> Result<u8> {
        match self.data.get(self.pos) {
            Some(b) => Ok(*b),
            None => bail!("unexpected end of bencode data"),
        }
    }

    fn integer(&mut self, terminator: u8) -> Result<i64> {
        let start = self.pos;
        let negative = self.peek()? == b'-';
        if negative {
            self.pos += 1;
        }
        let digits = self.pos;
        while self.peek()?.is_ascii_digit() {
            self.pos += 1;
        }
        let text = &self.data[digits..self.pos];
        if text.is_empty() || text.len() > 19 {
            bail!("invalid bencode integer at byte {start}");
        }
        // No leading zeros, and no negative zero.
        if text.len() > 1 && text[0] == b'0' || (negative && text == b"0") {
            bail!("non-canonical bencode integer at byte {start}");
        }
        if self.peek()? != terminator {
            bail!("invalid bencode integer terminator at byte {}", self.pos);
        }
        self.pos += 1;
        let magnitude: i128 = std::str::from_utf8(text)?.parse()?;
        let value = if negative { -magnitude } else { magnitude };
        Ok(i64::try_from(value)?)
    }

    fn byte_string(&mut self) -> Result<Vec<u8>> {
        let length = self.integer(b':')?;
        let length = usize::try_from(length)?;
        let end = self
            .pos
            .checked_add(length)
            .filter(|e| *e <= self.data.len());
        let Some(end) = end else {
            bail!("bencode string runs past the end of the data");
        };
        let bytes = self.data[self.pos..end].to_vec();
        self.pos = end;
        Ok(bytes)
    }

    fn value(&mut self, depth: usize) -> Result<Value> {
        if depth > MAX_DEPTH {
            bail!("bencode nesting is too deep");
        }
        match self.peek()? {
            b'i' => {
                self.pos += 1;
                Ok(Value::Int(self.integer(b'e')?))
            }
            b'l' => {
                self.pos += 1;
                let mut items = Vec::new();
                while self.peek()? != b'e' {
                    items.push(self.value(depth + 1)?);
                }
                self.pos += 1;
                Ok(Value::List(items))
            }
            b'd' => {
                self.pos += 1;
                let mut map = BTreeMap::new();
                while self.peek()? != b'e' {
                    if !self.peek()?.is_ascii_digit() {
                        bail!("bencode dictionary key must be a string");
                    }
                    let key = self.byte_string()?;
                    let start = self.pos;
                    let value = self.value(depth + 1)?;
                    if depth == 0 && key == b"info" {
                        self.info_span = Some(start..self.pos);
                    }
                    // Key order varies between writers and is tolerated; repeats are not.
                    if map.insert(key, value).is_some() {
                        bail!("duplicate key in bencode dictionary");
                    }
                }
                self.pos += 1;
                Ok(Value::Dict(map))
            }
            b'0'..=b'9' => Ok(Value::Bytes(self.byte_string()?)),
            other => bail!("unexpected byte 0x{other:02x} in bencode data"),
        }
    }
}

/// Decodes exactly one value; trailing bytes are an error.
pub fn decode(data: &[u8]) -> Result<Decoded> {
    let mut parser = Parser {
        data,
        pos: 0,
        info_span: None,
    };
    let value = parser.value(0)?;
    if parser.pos != data.len() {
        bail!("trailing data after bencode value");
    }
    Ok(Decoded {
        value,
        info_span: parser.info_span,
    })
}

/// Decodes one value from the start of `data` and returns how many bytes it used. Peer
/// messages (BEP 9/10) put raw bytes right after a bencoded dictionary.
pub fn decode_prefix(data: &[u8]) -> Result<(Value, usize)> {
    let mut parser = Parser {
        data,
        pos: 0,
        info_span: None,
    };
    let value = parser.value(0)?;
    Ok((value, parser.pos))
}

pub fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write(value, &mut out);
    out
}

fn write(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Int(n) => out.extend(format!("i{n}e").as_bytes()),
        Value::Bytes(b) => {
            out.extend(format!("{}:", b.len()).as_bytes());
            out.extend(b);
        }
        Value::List(items) => {
            out.push(b'l');
            for item in items {
                write(item, out);
            }
            out.push(b'e');
        }
        Value::Dict(map) => {
            out.push(b'd');
            for (key, item) in map {
                out.extend(format!("{}:", key.len()).as_bytes());
                out.extend(key);
                write(item, out);
            }
            out.push(b'e');
        }
    }
}

/// Builds a dictionary from `(key, value)` pairs.
pub fn dict<const N: usize>(pairs: [(&str, Value); N]) -> Value {
    Value::Dict(
        pairs
            .into_iter()
            .map(|(k, v)| (k.as_bytes().to_vec(), v))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_every_type_and_round_trips_canonical_data() {
        let data = b"d3:bar4:spam3:fooi-42e4:listl1:a1:bi7eee";
        let decoded = decode(data).unwrap();
        assert_eq!(decoded.value.get("foo").unwrap().int(), Some(-42));
        assert_eq!(decoded.value.get("bar").unwrap().str(), Some("spam"));
        assert_eq!(decoded.value.get("list").unwrap().list().unwrap().len(), 3);
        assert_eq!(encode(&decoded.value), data);
    }

    #[test]
    fn records_the_exact_bytes_of_the_info_dictionary() {
        let data = b"d8:announce3:url4:infod4:name1:x6:lengthi5eee";
        let decoded = decode(data).unwrap();
        let span = decoded.info_span.unwrap();
        assert_eq!(&data[span], b"d4:name1:x6:lengthi5ee");
        // Only the top-level key counts.
        let nested = decode(b"d1:ad4:infoi1eee").unwrap();
        assert!(nested.info_span.is_none());
    }

    #[test]
    fn rejects_malformed_and_non_canonical_input() {
        for bad in [
            &b"i03e"[..],
            b"i-0e",
            b"ie",
            b"i1",
            b"i12x",
            b"5:abc",
            b"l",
            b"d1:a",
            b"di1e1:ae",
            b"d1:ai1e1:ai2ee",
            b"x",
            b"",
            b"i1ei2e",
            b"99999999999999999999:a",
            b"i99999999999999999999e",
        ] {
            assert!(
                decode(bad).is_err(),
                "{:?} should be rejected",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn nesting_is_bounded() {
        let deep = [vec![b'l'; 200], vec![b'e'; 200]].concat();
        assert!(decode(&deep).is_err());
        let fine = [vec![b'l'; 20], vec![b'e'; 20]].concat();
        assert!(decode(&fine).is_ok());
    }

    #[test]
    fn unsorted_keys_are_accepted_but_reencoded_sorted() {
        let decoded = decode(b"d1:bi1e1:ai2ee").unwrap();
        assert_eq!(encode(&decoded.value), b"d1:ai2e1:bi1ee");
    }

    #[test]
    fn a_prefix_can_be_followed_by_raw_payload() {
        let mut message = encode(&dict([
            ("msg_type", Value::Int(1)),
            ("piece", Value::Int(0)),
        ]));
        let used = message.len();
        message.extend(b"RAWDATA");
        let (value, consumed) = decode_prefix(&message).unwrap();
        assert_eq!(consumed, used);
        assert_eq!(value.get("piece").unwrap().int(), Some(0));
        assert_eq!(&message[consumed..], b"RAWDATA");
    }

    #[test]
    fn binary_strings_survive() {
        let value = Value::Bytes(vec![0, 255, 10, 13]);
        assert_eq!(decode(&encode(&value)).unwrap().value, value);
    }
}

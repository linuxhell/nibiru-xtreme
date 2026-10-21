//! Kad UDP obfuscation (packet level).
//!
//! ```text
//! <marker 1><random key part 2> | <magic 4 LE><pad len 1><pad><receiver verify key 4 LE>
//!                                  <sender verify key 4 LE><Kad packet>
//! ```
//! The part after `|` is RC4 encrypted without discarding keystream. Two keys exist:
//! `MD5(<Kad ID of the receiver 16><random 2>)` (marker bits `00`) and
//! `MD5(<receiver verify key 4 LE><random 2>)` (marker bits `10`), the latter for a receiver whose
//! ID the sender does not know. The Kad ID is the 16 bytes exactly as they appear in Kad packets.
//! Written from the documented format and checked against aMule 2.3.3 (tests/interop_amule.py).

use super::obfuscation::Rc4;

const MAGIC: u32 = 0x395F_2EC1;
const HEADER: usize = 8;

/// Protocol bytes that mark a plain packet; an encrypted one never starts with them.
pub fn is_plain(first: u8) -> bool {
    matches!(first, 0xc5 | 0xe5 | 0xe4 | 0xa3 | 0xb2 | 0xd4)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Key<'a> {
    /// The receiver's Kad ID.
    NodeId(&'a [u8; 16]),
    /// The verify key the receiver handed out for the sender's IP address.
    Receiver(u32),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Opened {
    pub payload: Vec<u8>,
    /// The key the sender expects us to use when we answer it.
    pub sender_verify: u32,
    pub receiver_verify: u32,
    /// Keyed with the receiver's ID (true) or with the receiver verify key (false).
    pub by_node_id: bool,
}

fn rc4_for(key: Key<'_>, random: [u8; 2]) -> Rc4 {
    let digest = match key {
        Key::NodeId(id) => {
            let mut material = [0u8; 18];
            material[..16].copy_from_slice(id);
            material[16..].copy_from_slice(&random);
            md5::compute(material).0
        }
        Key::Receiver(verify) => {
            let mut material = [0u8; 6];
            material[..4].copy_from_slice(&verify.to_le_bytes());
            material[4..].copy_from_slice(&random);
            md5::compute(material).0
        }
    };
    Rc4::new(&digest)
}

/// Seals `payload` (a complete Kad packet, starting with `0xE4`/`0xE5`).
pub fn seal(
    payload: &[u8],
    key: Key<'_>,
    receiver_verify: u32,
    sender_verify: u32,
    random: [u8; 2],
    marker_noise: u8,
) -> Vec<u8> {
    let mut marker = match key {
        Key::NodeId(_) => marker_noise & 0xfc,
        Key::Receiver(_) => (marker_noise & 0xfe) | 0x02,
    };
    // Never look like a plain protocol byte; flipping a high bit keeps the two key-select bits.
    while is_plain(marker) {
        marker ^= 0x40;
        marker |= 0x04;
    }
    let mut body = Vec::with_capacity(HEADER + 8 + payload.len());
    body.extend(MAGIC.to_le_bytes());
    body.push(0); // padding length: none
    body.extend(receiver_verify.to_le_bytes());
    body.extend(sender_verify.to_le_bytes());
    body.extend(payload);
    rc4_for(key, random).apply(&mut body);
    let mut out = Vec::with_capacity(3 + body.len());
    out.push(marker);
    out.extend(random);
    out.extend(body);
    out
}

/// Tries to open an incoming datagram. `verify_key_for_sender` is the key we handed out to the
/// sender's IP address (for packets keyed with it); `own_id` is our Kad ID.
pub fn open(packet: &[u8], own_id: &[u8; 16], verify_key_for_sender: u32) -> Option<Opened> {
    if packet.len() <= HEADER || is_plain(packet[0]) {
        return None;
    }
    let random = [packet[1], packet[2]];
    // The marker says which key to try first; the other is the fallback.
    let order: [bool; 2] = if packet[0] & 0x03 == 2 {
        [false, true]
    } else {
        [true, false]
    };
    for by_node_id in order {
        let key = if by_node_id {
            Key::NodeId(own_id)
        } else {
            Key::Receiver(verify_key_for_sender)
        };
        let mut rc4 = rc4_for(key, random);
        let mut head = [0u8; 5];
        head.copy_from_slice(&packet[3..8]);
        rc4.apply(&mut head);
        if u32::from_le_bytes(head[..4].try_into().ok()?) != MAGIC {
            continue;
        }
        let padding = head[4] as usize;
        let rest = &packet[HEADER..];
        if rest.len() < padding + 8 + 1 {
            return None;
        }
        let mut tail = rest.to_vec();
        rc4.apply(&mut tail);
        let verify = &tail[padding..padding + 8];
        return Some(Opened {
            receiver_verify: u32::from_le_bytes(verify[..4].try_into().ok()?),
            sender_verify: u32::from_le_bytes(verify[4..].try_into().ok()?),
            payload: tail[padding + 8..].to_vec(),
            by_node_id,
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const NODE: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

    #[test]
    fn packets_match_the_independent_python_model_byte_for_byte() {
        // Vectors produced by tests/interop_kad_crypto.py.
        let by_id = seal(
            &[0xe4, 0x60],
            Key::NodeId(&NODE),
            0x1122_3344,
            0x5566_7788,
            [0x12, 0x34],
            0x40,
        );
        assert_eq!(hex(&by_id), "40123423e7db85107c8758eb77fa6dc06fd6");
        let by_key = seal(
            &[0xe4, 0x60],
            Key::Receiver(0xdead_beef),
            1,
            2,
            [0x12, 0x34],
            0x40,
        );
        assert_eq!(hex(&by_key), "42123424e4214b94340d245ff820e9a97a38");
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn sealed_packets_open_with_either_key_and_carry_the_verify_keys() {
        let packet = seal(&[0xe4, 0x60], Key::NodeId(&NODE), 7, 9, [1, 2], 0xff);
        assert!(!is_plain(packet[0]));
        let opened = open(&packet, &NODE, 0).unwrap();
        assert_eq!(opened.payload, vec![0xe4, 0x60]);
        assert_eq!((opened.receiver_verify, opened.sender_verify), (7, 9));
        assert!(opened.by_node_id);

        let packet = seal(
            &[0xe4, 0x61, 1, 2],
            Key::Receiver(0xabcd),
            0,
            0x77,
            [9, 8],
            0x00,
        );
        let opened = open(&packet, &[0xaa; 16], 0xabcd).unwrap();
        assert_eq!(opened.payload, vec![0xe4, 0x61, 1, 2]);
        assert_eq!(opened.sender_verify, 0x77);
        assert!(!opened.by_node_id);
    }

    #[test]
    fn wrong_keys_plain_packets_and_junk_are_not_opened() {
        let packet = seal(&[0xe4, 0x60], Key::NodeId(&NODE), 0, 0, [1, 2], 0x10);
        assert!(open(&packet, &[0xff; 16], 5).is_none());
        assert!(open(&[0xe4, 0x60, 0, 0, 0, 0, 0, 0, 0, 0], &NODE, 0).is_none());
        assert!(open(&[1, 2, 3], &NODE, 0).is_none());
        let mut tampered = packet;
        tampered[5] ^= 0x80;
        assert!(open(&tampered, &NODE, 0).is_none());
    }

    #[test]
    fn markers_never_collide_with_plain_protocol_bytes() {
        for noise in 0..=255u8 {
            for key in [Key::NodeId(&NODE), Key::Receiver(1)] {
                let packet = seal(&[0xe4, 0x60], key, 0, 0, [3, 4], noise);
                assert!(!is_plain(packet[0]), "{noise}");
                // The two key-select bits survive the collision fix-up.
                let wanted = if matches!(key, Key::NodeId(_)) { 0 } else { 2 };
                assert_eq!(packet[0] & 0x03, wanted);
            }
        }
    }
}

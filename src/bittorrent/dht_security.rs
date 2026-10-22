//! BEP 42 node ID prefixes. CRC32C is the protocol's address binding, not a MAC.
use std::net::IpAddr;

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f63b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

pub fn local(ip: IpAddr) -> bool {
    super::tracker::is_private(ip)
}

fn prefix(ip: IpAddr, random: u8) -> [u8; 4] {
    let r = (random & 7) as u64;
    let bytes = match ip {
        IpAddr::V4(ip) => {
            (((u32::from(ip) & 0x030f3fff) | ((r as u32) << 29)).to_be_bytes()).to_vec()
        }
        IpAddr::V6(ip) => {
            let high = u64::from_be_bytes(ip.octets()[..8].try_into().expect("eight bytes"));
            ((high & 0x0103070f1f3f7fff) | (r << 61))
                .to_be_bytes()
                .to_vec()
        }
    };
    crc32c(&bytes).to_be_bytes()
}

pub fn bind(ip: IpAddr, mut random: [u8; 20]) -> [u8; 20] {
    if local(ip) {
        return random;
    }
    let crc = prefix(ip, random[19]);
    random[0] = crc[0];
    random[1] = crc[1];
    random[2] = (crc[2] & 0xf8) | (random[2] & 7);
    random
}

pub fn valid(ip: IpAddr, id: &[u8; 20]) -> bool {
    if local(ip) {
        return true;
    }
    let crc = prefix(ip, id[19]);
    id[0] == crc[0] && id[1] == crc[1] && id[2] & 0xf8 == crc[2] & 0xf8
}

/// A single responder cannot claim multiple votes by opening more UDP ports.
pub fn voting_network(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(ip) => IpAddr::V4((u32::from(ip) & 0xffffff00).into()),
        IpAddr::V6(ip) => IpAddr::V6((u128::from(ip) & (!0u128 << 80)).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn official_bep42_vectors() {
        for (ip, r, prefix) in [
            ("124.31.75.21", 1, [0x5f, 0xbf, 0xbf]),
            ("21.75.31.124", 86, [0x5a, 0x3c, 0xe9]),
            ("65.23.51.170", 22, [0xa5, 0xd4, 0x32]),
            ("84.124.73.14", 65, [0x1b, 0x03, 0x21]),
            ("43.213.53.83", 90, [0xe5, 0x6f, 0x6c]),
        ] {
            let ip = ip.parse().unwrap();
            let mut random = [0; 20];
            random[19] = r;
            let mut id = bind(ip, random);
            assert_eq!(&id[..2], &prefix[..2]);
            assert_eq!(id[2] & 0xf8, prefix[2] & 0xf8);
            assert!(valid(ip, &id));
            id[0] ^= 1;
            assert!(!valid(ip, &id));
        }
    }
    #[test]
    fn ipv6_prefix_and_lan_exemption() {
        let ip = "2001:4860:4860::8888".parse().unwrap();
        let id = bind(ip, [81; 20]);
        assert!(valid(ip, &id));
        assert!(!valid("2606:4700:4700::1111".parse().unwrap(), &id));
        assert!(valid("127.0.0.1".parse().unwrap(), &[0; 20]));
        assert!(valid("::1".parse().unwrap(), &[0; 20]));
    }
}

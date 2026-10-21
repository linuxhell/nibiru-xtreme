"""ED2K file hashing in pure Python (MD4 is missing from many OpenSSL builds)."""
import struct

def _rol(x, n):
    return ((x << n) | (x >> (32 - n))) & 0xFFFFFFFF

def md4(data: bytes) -> bytes:
    a, b, c, d = 0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476
    length = len(data)
    data += b"\x80" + b"\x00" * ((55 - length) % 64) + struct.pack("<Q", length * 8)
    f = lambda x, y, z: (x & y) | (~x & z)
    g = lambda x, y, z: (x & y) | (x & z) | (y & z)
    h = lambda x, y, z: x ^ y ^ z
    for i in range(0, len(data), 64):
        x = struct.unpack("<16I", data[i:i + 64])
        aa, bb, cc, dd = a, b, c, d
        for k, s in zip(range(16), [3, 7, 11, 19] * 4):
            a = _rol((a + f(b, c, d) + x[k]) & 0xFFFFFFFF, s); a, b, c, d = d, a, b, c
        for k, s in zip([0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15], [3, 5, 9, 13] * 4):
            a = _rol((a + g(b, c, d) + x[k] + 0x5A827999) & 0xFFFFFFFF, s); a, b, c, d = d, a, b, c
        for k, s in zip([0, 8, 4, 12, 2, 10, 6, 14, 1, 9, 5, 13, 3, 11, 7, 15], [3, 9, 11, 15] * 4):
            a = _rol((a + h(b, c, d) + x[k] + 0x6ED9EBA1) & 0xFFFFFFFF, s); a, b, c, d = d, a, b, c
        a, b, c, d = (a + aa) & 0xFFFFFFFF, (b + bb) & 0xFFFFFFFF, (c + cc) & 0xFFFFFFFF, (d + dd) & 0xFFFFFFFF
    return struct.pack("<4I", a, b, c, d)

PART = 9_728_000

def ed2k_hash(content: bytes) -> str:
    parts = [md4(content[i:i + PART]) for i in range(0, len(content), PART)] or [md4(b"")]
    if len(content) % PART == 0 and len(content) > 0:
        parts.append(md4(b""))
    return (parts[0] if len(parts) == 1 else md4(b"".join(parts))).hex()

def ed2k_link(name: str, content: bytes) -> str:
    return f"ed2k://|file|{name}|{len(content)}|{ed2k_hash(content)}|/"

if __name__ == "__main__":
    assert md4(b"").hex() == "31d6cfe0d16ae931b73c59d7e0c089c0"
    assert md4(b"abc").hex() == "a448017aaf21d8525fc10ae87aa6729d"
    print("md4 ok")

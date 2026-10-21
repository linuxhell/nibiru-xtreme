"""Independent Python model of the ED2K obfuscation keys and handshake bytes (MD5 + RC4),
used to pin the vectors in src/ed2k/obfuscation.rs and as a wire-level peer in tests."""
import hashlib, os, struct

MAGIC_SYNC = 0x835E6FC4

def rc4(key):
    s = list(range(256)); j = 0
    for i in range(256):
        j = (j + s[i] + key[i % len(key)]) % 256
        s[i], s[j] = s[j], s[i]
    i = j = 0
    def stream(data):
        nonlocal i, j
        out = bytearray()
        for b in data:
            i = (i + 1) % 256; j = (j + s[i]) % 256
            s[i], s[j] = s[j], s[i]
            out.append(b ^ s[(s[i] + s[j]) % 256])
        return bytes(out)
    stream(bytes(1024))  # the first 1024 keystream bytes are discarded
    return stream

def keys(target_hash, random, initiator):
    a = rc4(hashlib.md5(target_hash + bytes([34]) + random).digest())   # initiator -> responder
    b = rc4(hashlib.md5(target_hash + bytes([203]) + random).digest())  # responder -> initiator
    return (a, b) if initiator else (b, a)

def initiator_request(target_hash, padding=0):
    random = os.urandom(4)
    send, recv = keys(target_hash, random, True)
    marker = next(m for m in os.urandom(64) if m not in (0xE3, 0xC5, 0xD4))
    body = struct.pack("<IBBB", MAGIC_SYNC, 0, 0, padding) + os.urandom(padding)
    return bytes([marker]) + random + send(body), send, recv

if __name__ == "__main__":
    send, _ = keys(b"0123456789abcdef", bytes([1, 2, 3, 4]), True)
    print(list(send(bytes(16))))

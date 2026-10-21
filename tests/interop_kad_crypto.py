"""Independent Python model of the Kad UDP obfuscation packet format (MD5 + RC4, no discard)."""
import hashlib, struct

MAGIC = 0x395F2EC1

def rc4(key):
    s = list(range(256)); j = 0
    for i in range(256):
        j = (j + s[i] + key[i % len(key)]) % 256
        s[i], s[j] = s[j], s[i]
    i = j = 0
    def run(data):
        nonlocal i, j
        out = bytearray()
        for b in data:
            i = (i + 1) % 256; j = (j + s[i]) % 256
            s[i], s[j] = s[j], s[i]
            out.append(b ^ s[(s[i] + s[j]) % 256])
        return bytes(out)
    return run

def encrypt(payload, node_id=None, receiver_key=None, receiver_verify=0, sender_verify=0, random_part=b"\x12\x34", marker=0x40):
    """node_id: 16 wire bytes of the target's Kad ID; otherwise receiver_key (u32) keys the packet."""
    if node_id is not None:
        key = hashlib.md5(node_id + random_part).digest(); marker &= 0xFC
    else:
        key = hashlib.md5(struct.pack("<I", receiver_key) + random_part).digest(); marker = (marker & 0xFE) | 0x02
    run = rc4(key)
    body = struct.pack("<I", MAGIC) + bytes([0]) + struct.pack("<II", receiver_verify, sender_verify) + payload
    return bytes([marker]) + random_part + run(body)

if __name__ == "__main__":
    node = bytes(range(16))
    payload = bytes([0xE4, 0x60])
    print("by_node_id", encrypt(payload, node_id=node, receiver_verify=0x11223344, sender_verify=0x55667788).hex())
    print("by_recv_key", encrypt(payload, receiver_key=0xDEADBEEF, receiver_verify=1, sender_verify=2).hex())

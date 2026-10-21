"""Minimal BitTorrent peer used as a probe: handshake, BEP 10 extension handshake, ut_holepunch."""
import socket, struct, os, time

def bencode(v):
    if isinstance(v, int): return b"i%de" % v
    if isinstance(v, bytes): return b"%d:%s" % (len(v), v)
    if isinstance(v, str): return bencode(v.encode())
    if isinstance(v, dict): return b"d" + b"".join(bencode(k) + bencode(v[k]) for k in sorted(v)) + b"e"
    if isinstance(v, list): return b"l" + b"".join(bencode(x) for x in v) + b"e"
    raise TypeError(v)

def bdecode(data, i=0):
    c = data[i:i+1]
    if c == b"i":
        j = data.index(b"e", i); return int(data[i+1:j]), j+1
    if c == b"l":
        out = []; i += 1
        while data[i:i+1] != b"e":
            v, i = bdecode(data, i); out.append(v)
        return out, i+1
    if c == b"d":
        out = {}; i += 1
        while data[i:i+1] != b"e":
            k, i = bdecode(data, i); v, i = bdecode(data, i); out[k] = v
        return out, i+1
    j = data.index(b":", i); n = int(data[i:j]); return data[j+1:j+1+n], j+1+n

class Raw:
    def __init__(self, host, port, info_hash, tcp_port_for_udp=True):
        self.sock = socket.create_connection((host, port), timeout=10)
        self.local_port = self.sock.getsockname()[1]
        self.peer_id = b"-RAW001-" + os.urandom(12)
        reserved = bytearray(8); reserved[5] |= 0x10
        self.sock.sendall(b"\x13BitTorrent protocol" + bytes(reserved) + info_hash + self.peer_id)
        hello = self.read(68)
        assert hello[:20] == b"\x13BitTorrent protocol", hello[:20]
        self.their_reserved = hello[20:28]
        self.their_id = hello[48:68]
        self.ext_ids = {}
        self.messages = []   # (ext_name or 'msg id', payload)
        self.buf = b""
    def read(self, n):
        data = b""
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk: raise EOFError
            data += chunk
        return data
    def send(self, msg_id, payload=b""):
        self.sock.sendall(struct.pack(">IB", len(payload) + 1, msg_id) + payload)
    def send_ext_handshake(self, names):
        m = {n: i + 1 for i, n in enumerate(names)}
        self.my_ids = {i + 1: n for i, n in enumerate(names)}
        self.send(20, b"\x00" + bencode({"m": m, "p": self.local_port, "v": "raw-probe"}))
    def send_ext(self, name, payload):
        self.send(20, bytes([self.ext_ids[name]]) + payload)
    def pump(self, seconds):
        """Read frames for a while; learn extension ids; return newly seen extension payloads."""
        end = time.time() + seconds
        seen = []
        self.sock.settimeout(0.2)
        while time.time() < end:
            try:
                head = self.read(4)
            except (socket.timeout, TimeoutError):
                continue
            n = struct.unpack(">I", head)[0]
            if n == 0: continue
            self.sock.settimeout(5)
            body = self.read(n)
            self.sock.settimeout(0.2)
            if body[0] == 20:
                ext_id, payload = body[1], body[2:]
                if ext_id == 0:
                    d, _ = bdecode(payload)
                    self.ext_ids = {k.decode(): v for k, v in d.get(b"m", {}).items()}
                    seen.append(("handshake", d))
                else:
                    seen.append((self.my_ids.get(ext_id, ext_id), payload))
        return seen

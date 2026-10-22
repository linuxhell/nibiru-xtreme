"""Independent-client ED2K tests against a real aMule daemon (amuled).

Run: python tests/interop_amule.py --binary target/debug/nibiru-xtreme
Requires `amuled` (Debian/Ubuntu package amule-daemon). aMule is only a test oracle: it is never
linked into or distributed with Nibiru. Nothing here touches the public ED2K or Kad networks.
"""

import argparse, hashlib, json, os, pathlib, re, shutil, socket, struct, subprocess, sys, tempfile, time, urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from interop_ed2k_hash import ed2k_link


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Amule:
    def __init__(self, root, tcp, udp, ec, required):
        self.root, self.tcp, self.ec = root, tcp, ec
        self.config = root / "cfg"
        (self.config / "Incoming").mkdir(parents=True)
        (self.config / "Temp").mkdir()
        self.set(required)
        self.process = None

    def set(self, required):
        conf = f"""[eMule]
Nick=interop
Port={self.tcp}
UDPPort={free_port()}
UDPEnable=1
Autoconnect=0
Reconnect=0
ConnectToKad=0
ConnectToED2K=0
MaxUpload=100000
MaxDownload=0
SlotAllocation=10000
UploadCapacity=100000
DownloadCapacity=100000
TempDir={self.config / 'Temp'}
IncomingDir={self.config / 'Incoming'}
[ExternalConnect]
AcceptExternalConnections=1
ECAddress=127.0.0.1
ECPort={self.ec}
ECPassword={hashlib.md5(b'pw').hexdigest()}
[Obfuscation]
IsClientCryptLayerSupported=1
IsCryptLayerRequested=1
IsClientCryptLayerRequired={1 if required else 0}
CryptoPaddingLenght=254
"""
        (self.config / "amule.conf").write_text(conf)

    def start(self):
        self.process = subprocess.Popen(["amuled", "-c", str(self.config)],
                                        stdout=open(self.root / "amuled.log", "a"), stderr=subprocess.STDOUT)
        for _ in range(200):
            try:
                socket.create_connection(("127.0.0.1", self.tcp), timeout=0.5).close()
                break
            except OSError:
                time.sleep(0.2)
        else:
            raise AssertionError("amuled did not open its TCP port\\n" + (self.root / "amuled.log").read_text())
        time.sleep(8)  # let it finish starting and hash the shared file

    def stop(self):
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None

    def user_hash(self):
        """The hash aMule advertises (it sets two marker bytes in memory, so the file is not enough)."""
        body = bytes([16]) + os.urandom(16) + struct.pack("<IH", 0x01020304, 4662) + struct.pack("<I", 0) + bytes(6)
        for _ in range(10):
            with socket.create_connection(("127.0.0.1", self.tcp), timeout=5) as s:
                s.sendall(struct.pack("<BIB", 0xE3, len(body) + 1, 1) + body)
                answer = s.recv(2000)
            if len(answer) > 22 and answer[5] == 0x4C:
                return answer[6:22]
            time.sleep(1)
        raise AssertionError("aMule did not answer a plain hello (is it still starting?)")


class Nibiru:
    def __init__(self, binary, root):
        self.port, self.secret = free_port(), "k"
        self.dir = root / "nibiru"
        self.dir.mkdir()
        self.process = subprocess.Popen(
            [str(binary), "serve", "--port", str(self.port), "--secret", self.secret, "--dir", str(self.dir),
             "--ed2k", "--enable-dht=false", "--enable-dht6=false"],
            stdout=open(root / "nibiru.log", "w"), stderr=subprocess.STDOUT)
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        for _ in range(100):
            try:
                self.rpc("nibiru.getVersion", [])
                break
            except OSError:
                time.sleep(0.1)

    def rpc(self, method, params):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": ["token:" + self.secret] + params}).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}/jsonrpc", body, {"Content-Type": "application/json"})
        with self.opener.open(request, timeout=10) as response:
            data = json.load(response)
        if "error" in data:
            raise AssertionError(data["error"])
        return data["result"]

    def download(self, link, options, seconds=180):
        gid = self.rpc("nibiru.addUri", [[link], options])
        end = time.time() + seconds
        while time.time() < end:
            status = self.rpc("nibiru.tellStatus", [gid])
            if status["status"] in ("complete", "error"):
                return status
            time.sleep(0.5)
        raise AssertionError("download timed out")

    def stop(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()


def run(binary):
    with tempfile.TemporaryDirectory(prefix="nibiru-amule-") as directory:
        root = pathlib.Path(directory)
        tcp, ec = free_port(), free_port()
        amule = Amule(root, tcp, 0, ec, required=False)
        content = bytes((i * 31 + i // 251) % 256 for i in range(10_500_017))  # two ED2K parts
        (amule.config / "Incoming" / "interop.bin").write_bytes(content)
        link = ed2k_link("interop.bin", content)
        results = []
        nibiru = None
        user_hash = None
        try:
            for required, label in [(False, "plain"), (True, "required")]:
                amule.stop()
                amule.set(required)
                amule.start()
                # A plain hello is refused once obfuscation is required, so ask only the first time.
                user_hash = user_hash if required else amule.user_hash()
                nibiru = Nibiru(binary, root / f"n{label}") if (root / f"n{label}").mkdir() is None else None
                try:
                    source = f"127.0.0.1:{tcp}"
                    if not required:
                        status = nibiru.download(link, {"x-nibiru-ed2k-source": source, "x-nibiru-ed2k-obfuscation": "off"})
                        assert status["status"] == "complete", status
                        assert (nibiru.dir / "interop.bin").read_bytes() == content
                        results.append({"mode": "plain", "bytes_verified": len(content), "result": "passed"})
                    else:
                        refused = nibiru.download(link, {"x-nibiru-ed2k-source": source, "x-nibiru-ed2k-obfuscation": "off"}, 60)
                        assert refused["status"] == "error", "aMule should refuse a plain connection when it requires obfuscation"
                        assert not (nibiru.dir / "interop.bin").exists()
                        status = nibiru.download(link, {"x-nibiru-ed2k-source": f"{source}#{user_hash.hex()}",
                                                        "x-nibiru-ed2k-obfuscation": "require"})
                        assert status["status"] == "complete", status
                        assert (nibiru.dir / "interop.bin").read_bytes() == content
                        results.append({"mode": "obfuscation required by aMule", "plain_refused": True,
                                        "bytes_verified": len(content), "result": "passed"})
                finally:
                    nibiru.stop()
        finally:
            amule.stop()
        for item in results:
            print(json.dumps({"independent_client": "aMule", **item}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if not shutil.which("amuled"):
        sys.exit("amuled not found (install amule-daemon)")
    run(args.binary.resolve())

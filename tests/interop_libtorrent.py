"""Independent-client wire tests. Requires the optional Python libtorrent wheel.

Run: python tests/interop_libtorrent.py --binary target/debug/nibiru-xtreme
libtorrent is a test oracle only; it is never linked into Nibiru.
"""

import argparse
import base64
import contextlib
import json
import pathlib
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.request

import libtorrent as lt


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait(predicate, label, seconds=40):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out: {label}")


def add(session, info, directory):
    params = lt.add_torrent_params()
    params.ti = info
    params.save_path = str(directory)
    params.flags &= ~(lt.torrent_flags.paused | lt.torrent_flags.auto_managed)
    return session.add_torrent(params)


@contextlib.contextmanager
def engine(binary, root, utp, encrypted, extra=()):
    port, peer_port = free_port(), free_port()
    secret = secrets.token_hex(16)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def rpc(method, params):
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                           "params": ["token:" + secret] + params}).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{port}/jsonrpc", body,
                                        {"Content-Type": "application/json"})
        with opener.open(request, timeout=10) as response:
            data = json.load(response)
        if "error" in data:
            raise AssertionError(data["error"])
        return data["result"]

    with (root / "engine.log").open("w") as log:
        process = subprocess.Popen([
            str(binary), "--enable-rpc=true", f"--rpc-listen-port={port}",
            f"--rpc-secret={secret}", f"--listen-port={peer_port}",
            "--enable-dht=false", "--seed-ratio=0", "--x-nibiru-allow-loopback-peers=true",
            f"--enable-utp={str(utp).lower()}",
            f"--x-nibiru-enable-tcp={str(not utp).lower()}",
            f"--bt-force-encryption={str(encrypted).lower()}",
            *extra,
        ], stdout=log, stderr=log)
        try:
            for _ in range(100):
                try:
                    rpc("aria2.getVersion", [])
                    break
                except (OSError, ValueError):
                    if process.poll() is not None:
                        raise AssertionError((root / "engine.log").read_text())
                    time.sleep(0.05)
            else:
                raise AssertionError("engine did not start")
            yield rpc, peer_port
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def scenario(binary, utp, encrypted, incoming, protocol="v1"):
    with tempfile.TemporaryDirectory(prefix="nibiru-lt-") as directory:
        root = pathlib.Path(directory)
        seed = root / "seed"
        output = root / "output"
        seed.mkdir()
        output.mkdir()
        length=9_000_123 if protocol=="v2" else 700_123
        content = bytes((i * 7 + i // 251) % 256 for i in range(length))
        (seed / "payload.bin").write_bytes(content)
        fs = lt.file_storage()
        fs.add_file("payload.bin", len(content))
        flags={"v1":lt.create_torrent.v1_only,"v2":lt.create_torrent.v2_only,"hybrid":0}[protocol]
        create = lt.create_torrent(fs, 65_536, flags=flags)
        lt.set_piece_hashes(create, str(seed))
        encoded = lt.bencode(create.generate())
        info = lt.torrent_info(encoded)
        settings = {
            "listen_interfaces": "127.0.0.1:0", "enable_dht": False,
            "enable_lsd": False, "enable_upnp": False, "enable_natpmp": False,
            # Loopback fixtures have no interface changes. Avoid requiring a
            # privileged OS network-notification socket in restricted containers.
            "enable_ip_notifier": False,
            "enable_outgoing_tcp": not utp, "enable_incoming_tcp": not utp,
            "enable_outgoing_utp": utp, "enable_incoming_utp": utp,
            "out_enc_policy": 0 if encrypted else 2,
            "in_enc_policy": 0 if encrypted else 2, "allowed_enc_level": 2,
            "allow_multiple_connections_per_ip": True,
            "alert_mask": lt.alert.category_t.error_notification | lt.alert.category_t.peer_notification | lt.alert.category_t.status_notification,
        }
        session = lt.session(settings)
        try:
            with engine(binary, root, utp, encrypted) as (rpc, port):
                if incoming:
                    gid = rpc("aria2.addTorrent", [base64.b64encode(encoded).decode(), [], {"dir": str(seed)}])
                    peer = add(session, info, output)
                    wait(lambda: rpc("aria2.tellStatus", [gid])["seeder"] == "true", "Nibiru seeder ready")
                    peer.connect_peer(("127.0.0.1", port))
                    try:
                        wait(lambda: peer.status().is_seeding, "libtorrent downloads from Nibiru")
                    except AssertionError:
                        status=peer.status()
                        print(json.dumps({"progress":status.progress,"state":str(status.state),"error":status.errc.message(),"peers":status.num_peers,"downloaded":status.total_done}),flush=True)
                        for alert in session.pop_alerts()[-100:]:
                            print(alert.message(),flush=True)
                        print((root/"engine.log").read_text(),flush=True)
                        raise
                else:
                    peer = add(session, info, seed)
                    wait(lambda: peer.status().is_seeding, "libtorrent seeder ready")
                    xt=f"urn:btih:{info.info_hashes().v1}" if protocol=="v1" else f"urn:btmh:1220{info.info_hashes().v2}"
                    magnet = f"magnet:?xt={xt}&x.pe=127.0.0.1:{session.listen_port()}"
                    gid = rpc("aria2.addUri", [[magnet], {"dir": str(output), "seed-time": "0"}])
                    wait(lambda: rpc("aria2.tellStatus", [gid])["status"] == "complete", "Nibiru downloads magnet from libtorrent")
                assert (output / "payload.bin").read_bytes() == content
                print(json.dumps({"transport": "utp" if utp else "tcp", "encrypted": encrypted,
                                  "protocol":protocol,
                                  "direction": "upload" if incoming else "download+magnet",
                                  "bytes_verified": len(content), "result": "passed"}), flush=True)
        finally:
            session.pause()
            del session


def lsd_scenario(binary, incoming):
    """Neither side is told the other's address: only BEP 14 multicast announcements connect them."""
    with tempfile.TemporaryDirectory(prefix="nibiru-lsd-") as directory:
        root = pathlib.Path(directory)
        seed, output = root / "seed", root / "output"
        seed.mkdir()
        output.mkdir()
        content = bytes((i * 11 + i // 199) % 256 for i in range(600_001))
        (seed / "payload.bin").write_bytes(content)
        fs = lt.file_storage()
        fs.add_file("payload.bin", len(content))
        create = lt.create_torrent(fs, 65_536, flags=lt.create_torrent.v1_only)
        lt.set_piece_hashes(create, str(seed))
        encoded = lt.bencode(create.generate())
        info = lt.torrent_info(encoded)
        session = lt.session({
            "listen_interfaces": "127.0.0.1:0", "enable_dht": False, "enable_lsd": True,
            "enable_upnp": False, "enable_natpmp": False, "enable_ip_notifier": False,
            "allow_multiple_connections_per_ip": True,
        })
        try:
            with engine(binary, root, False, False,
                        ["--bt-enable-lpd=true", "--bt-lpd-interface=127.0.0.1"]) as (rpc, port):
                assert "LSD" in rpc("aria2.getVersion", [])["enabledFeatures"]
                if incoming:
                    # Nibiru seeds; libtorrent has only the infohash (a magnet) and must find it.
                    gid = rpc("aria2.addTorrent", [base64.b64encode(encoded).decode(), [], {"dir": str(seed)}])
                    wait(lambda: rpc("aria2.tellStatus", [gid])["seeder"] == "true", "Nibiru seeder ready")
                    params = lt.parse_magnet_uri(f"magnet:?xt=urn:btih:{info.info_hashes().v1}")
                    params.save_path = str(output)
                    peer = session.add_torrent(params)
                    wait(lambda: peer.status().is_seeding, "libtorrent finds Nibiru through LSD", 60)
                else:
                    peer = add(session, info, seed)
                    wait(lambda: peer.status().is_seeding, "libtorrent seeder ready")
                    magnet = f"magnet:?xt=urn:btih:{info.info_hashes().v1}"
                    gid = rpc("aria2.addUri", [[magnet], {"dir": str(output), "seed-time": "0"}])
                    wait(lambda: rpc("aria2.tellStatus", [gid])["status"] == "complete",
                         "Nibiru finds libtorrent through LSD", 60)
                assert (output / "payload.bin").read_bytes() == content
                print(json.dumps({"feature": "lsd", "direction": "libtorrent finds nibiru" if incoming else "nibiru finds libtorrent",
                                  "bytes_verified": len(content), "result": "passed"}), flush=True)
        finally:
            session.pause()
            del session


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--protocols",default="v1,v2,hybrid")
    args = parser.parse_args()
    print(json.dumps({"independent_client": "libtorrent", "version": lt.__version__}), flush=True)
    for incoming in [False, True]:
        lsd_scenario(args.binary.resolve(), incoming)
    for protocol in args.protocols.split(","):
        for utp, encrypted in [(False, True), (True, False), (True, True)]:
            for incoming in [False, True]:
                scenario(args.binary.resolve(), utp, encrypted, incoming,protocol)

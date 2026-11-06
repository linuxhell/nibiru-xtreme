"""Compare HTTP startup and total download time through the same RPC options as ADM.

Synthetic loopback fixture; this does not measure the user's Internet connection.
Only Python's standard library is required. Every payload is verified with SHA-256.
"""

import argparse
import hashlib
import http.server
import json
import pathlib
import socket
import statistics
import subprocess
import tempfile
import threading
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--connections", type=int, nargs="+", default=[1, 8])
    parser.add_argument("--size-mib", type=float, nargs="+", default=[0.125, 128])
    parser.add_argument("--latency-ms", type=float, default=40)
    parser.add_argument("--rate-mib", type=float, default=100)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--report", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if (args.rounds < 1 or args.latency_ms < 0 or args.rate_mib <= 0
            or any(n < 1 or n > 16 for n in args.connections)
            or any(int(n * 2**20) < 2 for n in args.size_mib)):
        parser.error("invalid size, connection count, rounds, latency or rate")
    engines = [(label, pathlib.Path(path).resolve()) for label, path in
               (value.split("=", 1) for value in args.engine)]
    if len({label for label, _ in engines}) != len(engines):
        parser.error("engine labels must be unique")
    for label, binary in engines:
        if not binary.is_file():
            parser.error(f"missing binary: {label}={binary}")
    block = bytes(range(256)) * 256
    doubled = block * 2
    lock = threading.Lock()
    state = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self):
            super().setup()
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        def log_message(self, *_):
            pass

        def do_HEAD(self):
            self.respond(False)

        def do_GET(self):
            self.respond(True)

        def respond(self, body):
            size = state["size"]
            start, end = 0, size - 1
            requested_range = self.headers.get("Range")
            if requested_range:
                first, last = requested_range.removeprefix("bytes=").split("-", 1)
                start, end = int(first), min(int(last) if last else end, end)
            with lock:
                state["requests"].append(dict(method=self.command, range=requested_range))
                if body and end > start and state["first_payload"] is None:
                    state["first_payload"] = time.perf_counter()
            time.sleep(args.latency_ms / 1000)
            self.send_response(206 if requested_range else 200)
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", '"startup-fixture"')
            if requested_range:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            if not body:
                return
            try:
                while start <= end:
                    n = min(len(block), end - start + 1)
                    with lock:
                        deadline = max(time.perf_counter(), state["deadline"]) + n / (args.rate_mib * 2**20)
                        state["deadline"] = deadline
                    delay = deadline - time.perf_counter()
                    if delay > 0:
                        time.sleep(delay)
                    offset = start % len(block)
                    self.wfile.write(memoryview(doubled)[offset:offset + n])
                    start += n
            except (BrokenPipeError, ConnectionResetError):
                pass

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def rpc(port, method, params=()):
        payload = json.dumps(dict(jsonrpc="2.0", id=1, method=f"nibiru.{method}",
                                  params=["token:benchmark", *params])).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{port}/jsonrpc", payload,
                                         {"Content-Type": "application/json"})
        with opener.open(request, timeout=2) as response:
            result = json.load(response)
        if "error" in result:
            raise RuntimeError(result["error"])
        return result["result"]

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    rows = []
    profiles = {label: dict(sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                            version=subprocess.check_output([str(binary), "--version"], text=True).strip())
                for label, binary in engines}
    try:
        with tempfile.TemporaryDirectory(prefix="nibiru-http-startup-") as folder:
            root = pathlib.Path(folder)
            for round_index in range(args.rounds):
                order = engines[round_index % len(engines):] + engines[:round_index % len(engines)]
                for label, binary in order:
                    with socket.socket() as reservation:
                        reservation.bind(("127.0.0.1", 0))
                        port = reservation.getsockname()[1]
                    command = [str(binary), "serve", "--port", str(port), "--secret", "benchmark",
                               "--enable-dht=false", "--enable-dht6=false", "--enable-utp=false",
                               "--dir", str(root)]
                    with subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as process:
                        try:
                            deadline = time.monotonic() + 10
                            while True:
                                try:
                                    rpc(port, "getVersion")
                                    break
                                except (OSError, ValueError):
                                    if process.poll() is not None or time.monotonic() >= deadline:
                                        raise RuntimeError(f"{label}: RPC did not start")
                                    time.sleep(0.01)
                            for mib in args.size_mib:
                                size = int(mib * 2**20)
                                expected = hashlib.sha256()
                                for at in range(0, size, len(block)):
                                    expected.update(block[:min(len(block), size - at)])
                                for connections in args.connections:
                                    output = root / f"payload-{round_index}-{label}-{size}-{connections}.bin"
                                    with lock:
                                        state.update(size=size, deadline=0.0, requests=[], first_payload=None)
                                    started = time.perf_counter()
                                    gid = rpc(port, "addUri", [[f"http://127.0.0.1:{server.server_port}/payload"],
                                              dict(dir=str(root), out=output.name, split=str(connections),
                                                   **{"max-connection-per-server": str(connections), "min-split-size": "1M"})])
                                    deadline = time.monotonic() + 60
                                    while True:
                                        status = rpc(port, "tellStatus", [gid])
                                        if status["status"] == "complete":
                                            break
                                        if status["status"] == "error" or time.monotonic() >= deadline:
                                            raise RuntimeError(f"{label}: {status}")
                                        time.sleep(0.005)
                                    elapsed = time.perf_counter() - started
                                    with output.open("rb") as stream:
                                        digest = hashlib.file_digest(stream, "sha256").hexdigest()
                                    if output.stat().st_size != size or digest != expected.hexdigest():
                                        raise RuntimeError(f"{label}: payload size/hash mismatch")
                                    row = dict(label=label, round=round_index + 1, bytes=size, connections=connections,
                                               seconds=elapsed, mib_per_second=mib / elapsed,
                                               payload_request_ms=1000 * (state["first_payload"] - started),
                                               requests=state["requests"], sha256=digest)
                                    rows.append(row)
                                    print(json.dumps(row), flush=True)
                                    output.unlink()
                        finally:
                            process.terminate()
                            try:
                                process.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait()
    finally:
        server.shutdown()
        server.server_close()
    medians = []
    for label, _ in engines:
        for mib in args.size_mib:
            for connections in args.connections:
                samples = [row for row in rows if row["label"] == label
                           and row["bytes"] == int(mib * 2**20) and row["connections"] == connections]
                medians.append(dict(label=label, size_mib=mib, connections=connections,
                                    seconds=statistics.median(row["seconds"] for row in samples),
                                    payload_request_ms=statistics.median(row["payload_request_ms"] for row in samples)))
    report = dict(fixture="synthetic loopback; global rate pacing without catch-up credit",
                  latency_ms=args.latency_ms, rate_mib=args.rate_mib, rounds=args.rounds,
                  profiles=profiles, samples=rows, medians=medians)
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(dict(medians=medians)), flush=True)


if __name__ == "__main__":
    main()

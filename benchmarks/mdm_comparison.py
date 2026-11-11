"""Compare released headless clients on a private synthetic HTTP/1.1 fixture.

No Internet downloads, GUI automation or IDM execution. Defaults are compared:
MDM's eight-connection ceiling and Nibiru's size-based worker policy. Completion
requires process success, exact length and an independently computed SHA-256.
The timer includes cold CLI startup and finalization, but excludes verification.
"""

import argparse
import hashlib
import http.server
import json
import os
import pathlib
import platform
import socket
import statistics
import subprocess
import tempfile
import threading
import time


MIB = 1024 * 1024
BLOCK = bytes(range(256)) * 256
SCENARIOS = {
    "small": dict(size=256 * 1024, latency_ms=40, rate_mib=128),
    "large": dict(size=256 * MIB, latency_ms=40, rate_mib=128),
    "latency": dict(size=64 * MIB, latency_ms=150, rate_mib=64),
    "per_connection": dict(size=64 * MIB, latency_ms=40, rate_mib=128, per_connection=8),
    "quota": dict(size=64 * MIB, latency_ms=40, rate_mib=128, per_connection=8, max_active=2),
    # Deliberately non-conforming server: a range's ETag and data change even
    # though it received If-Range for the original resource. Never use such a
    # response to assemble a mixed file successfully.
    "changed_validator": dict(size=16 * MIB, latency_ms=20, rate_mib=128, change_etag=True),
}


def expected_hash(size):
    digest = hashlib.sha256()
    for offset in range(0, size, len(BLOCK)):
        digest.update(BLOCK[:min(len(BLOCK), size - offset)])
    return digest.hexdigest()


class Fixture:
    def __init__(self, settings):
        self.settings = settings
        self.lock = threading.Lock()
        self.requests = []
        self.active = self.peak = self.sent = 0
        self.deadline = 0.0
        self.first_data = None
        fixture = self

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
                cfg = fixture.settings
                size = cfg["size"]
                start, end = 0, size - 1
                requested_range = self.headers.get("Range")
                if requested_range:
                    first, last = requested_range.removeprefix("bytes=").split("-", 1)
                    start, end = int(first), min(int(last) if last else end, end)
                with fixture.lock:
                    refused = body and fixture.active >= cfg.get("max_active", 1000)
                    if body and not refused:
                        fixture.active += 1
                        fixture.peak = max(fixture.peak, fixture.active)
                    fixture.requests.append(dict(method=self.command, range=requested_range,
                                                 if_range=self.headers.get("If-Range"), refused=refused))
                try:
                    time.sleep(cfg["latency_ms"] / 1000)
                    if refused:
                        self.send_response(503)
                        self.send_header("Content-Length", "0")
                        self.send_header("Retry-After", "1")
                        self.end_headers()
                        return
                    changed = cfg.get("change_etag", False) and requested_range and start > 0
                    self.send_response(206 if requested_range else 200)
                    self.send_header("Content-Length", str(end - start + 1))
                    self.send_header("Accept-Ranges", "bytes")
                    self.send_header("ETag", '"version-b"' if changed else '"version-a"')
                    if requested_range:
                        self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
                    self.end_headers()
                    if not body:
                        return
                    connection_deadline = time.perf_counter()
                    while start <= end:
                        length = min(len(BLOCK), end - start + 1)
                        with fixture.lock:
                            fixture.deadline = max(time.perf_counter(), fixture.deadline) + length / (cfg["rate_mib"] * MIB)
                            deadline = fixture.deadline
                        if cfg.get("per_connection"):
                            connection_deadline = max(time.perf_counter(), connection_deadline) + length / (cfg["per_connection"] * MIB)
                            deadline = max(deadline, connection_deadline)
                        delay = deadline - time.perf_counter()
                        if delay > 0:
                            time.sleep(delay)
                        if changed:
                            data = bytes([0xEE]) * length
                        else:
                            at = start % len(BLOCK)
                            data = (BLOCK + BLOCK)[at:at + length]
                        self.wfile.write(data)
                        with fixture.lock:
                            if fixture.first_data is None:
                                fixture.first_data = time.perf_counter()
                            fixture.sent += length
                        start += length
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    if body and not refused:
                        with fixture.lock:
                            fixture.active -= 1

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nibiru", required=True, type=pathlib.Path)
    parser.add_argument("--mdm", required=True, type=pathlib.Path,
                        help="main misterdownloadmanager executable, carrying --cli")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=list(SCENARIOS))
    parser.add_argument("--timeout", type=float, default=45)
    parser.add_argument("--report", required=True, type=pathlib.Path)
    parser.add_argument("--baseline", type=pathlib.Path, help="Previous Nibiru executable measured in the same rounds")
    parser.add_argument("--compare-eight", action="store_true", help="Also test candidate with an explicit eight-connection maximum")
    args = parser.parse_args()
    if args.rounds < 1 or args.timeout <= 0:
        parser.error("rounds and timeout must be positive")
    engines = {"nibiru": args.nibiru.resolve(), "mdm": args.mdm.resolve()}
    if args.baseline: engines["nibiru_baseline"] = args.baseline.resolve()
    if args.compare_eight: engines["nibiru_eight"] = args.nibiru.resolve()
    profiles = {}
    for name, path in engines.items():
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        cmd = [str(path)] + (["--cli"] if name == "mdm" else []) + ["--version"]
        profiles[name] = dict(binary_sha256=digest, version=subprocess.check_output(cmd, text=True).strip(), connection_maximum=8 if name=="nibiru_eight" else "default")
    rows = []
    report = dict(platform=platform.platform(), python=platform.python_version(),
                  protocol="HTTP/1.1 loopback", completion="successful CLI exit; offline size and SHA-256 check",
                  settings="cold processes; size-based Nibiru defaults and MDM default ceiling 8; nibiru_eight explicitly requests 8",
                  profiles=profiles, scenarios={s: SCENARIOS[s] for s in args.scenarios}, rows=rows)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="nibiru-mdm-audit-") as folder:
        root = pathlib.Path(folder)
        env = dict(os.environ, XDG_CONFIG_HOME=str(root / "config"),
                   XDG_DATA_HOME=str(root / "data"), XDG_CACHE_HOME=str(root / "cache"),
                   NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost")
        for scenario in args.scenarios:
            cfg = SCENARIOS[scenario]
            expected = expected_hash(cfg["size"])
            for run in range(args.rounds):
                for name in (list(engines) if run % 2 == 0 else list(reversed(engines))):
                    destination = root / f"{scenario}-{run}-{name}.bin"
                    with Fixture(cfg) as fixture:
                        url = f"http://127.0.0.1:{fixture.server.server_port}/payload.bin"
                        cmd = ([str(engines[name]), "--cli", "get", url, "-o", str(destination), "--direct", "--json"]
                               if name == "mdm" else [str(engines[name]), "download", url, "--out", str(destination)])
                        if name == "nibiru_eight": cmd += ["--connections", "8"]
                        began = time.perf_counter()
                        stop_watch = threading.Event()
                        file_ready = []
                        child = []
                        peak_rss_kib = []

                        def watch_destination():
                            while not stop_watch.is_set():
                                try:
                                    if not file_ready and destination.stat().st_size == cfg["size"]:
                                        file_ready.append(time.perf_counter() - began)
                                except FileNotFoundError:
                                    pass
                                if child:
                                    try:
                                        status=pathlib.Path(f"/proc/{child[0].pid}/status").read_text()
                                        rss=next(int(line.split()[1]) for line in status.splitlines() if line.startswith("VmRSS:"))
                                        if not peak_rss_kib or rss>peak_rss_kib[0]: peak_rss_kib[:]=[rss]
                                    except (OSError,StopIteration): pass
                                stop_watch.wait(0.001)

                        watcher = threading.Thread(target=watch_destination, daemon=True)
                        watcher.start()
                        try:
                            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,env=env)
                            child.append(process)
                            stdout,stderr=process.communicate(timeout=args.timeout)
                            elapsed = time.perf_counter() - began
                            code, error = process.returncode, stderr.decode(errors="replace")[-600:] if process.returncode else None
                        except subprocess.TimeoutExpired:
                            process.kill(); process.communicate()
                            elapsed, code, error = time.perf_counter() - began, None, "process_timeout"
                        finally:
                            stop_watch.set()
                            watcher.join()
                        first_data = None if fixture.first_data is None else fixture.first_data - began
                        requests, peak, sent = list(fixture.requests), fixture.peak, fixture.sent
                    digest, size = None, None
                    if destination.is_file():
                        size = destination.stat().st_size
                        with destination.open("rb") as stream:
                            digest = hashlib.file_digest(stream, "sha256").hexdigest()
                    ok = code == 0 and size == cfg["size"] and digest == expected
                    row = dict(engine=name, scenario=scenario, run=run + 1, seconds=elapsed,
                               final_name_size_seconds=file_ready[0] if file_ready else None,
                               server_first_write_seconds=first_data, exit_code=code, correct=ok,
                               output_bytes=size, sha256=digest, expected_sha256=expected,
                               requests=requests, peak_active_gets=peak, server_bytes=sent, error=error)
                    row["sampled_peak_rss_kib"]=peak_rss_kib[0] if peak_rss_kib else None
                    rows.append(row)
                    ready = f"{file_ready[0]:.4f}s" if file_ready else "unknown"
                    print(f"{scenario} {run + 1} {name}: cli={elapsed:.4f}s file={ready} correct={ok} GETs={sum(r['method'] == 'GET' for r in requests)} peak={peak}", flush=True)
                    destination.unlink(missing_ok=True)
                    args.report.write_text(json.dumps(report, indent=2) + "\n")
    report["summary"] = [dict(scenario=scenario, engine=name,
                              median_seconds=statistics.median(r["seconds"] for r in rows if r["scenario"] == scenario and r["engine"] == name),
                              correct=sum(r["correct"] for r in rows if r["scenario"] == scenario and r["engine"] == name),
                              runs=args.rounds)
                         for scenario in args.scenarios for name in engines]
    args.report.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()

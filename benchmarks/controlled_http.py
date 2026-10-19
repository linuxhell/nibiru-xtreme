"""Compare release binaries against a local, rate/latency-controlled HTTP server.

Python standard library only. The fixture is synthetic, not a CDN benchmark.
"""

import argparse
import hashlib
import http.server
import json
import os
import pathlib
import resource
import statistics
import subprocess
import tempfile
import threading
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", action="append", required=True, metavar="LABEL=PATH")
    parser.add_argument("--no-log-label", action="append", default=[])
    parser.add_argument("--checkpoint", action="append", default=[], metavar="LABEL=MIB",
                        help="Experimental external fsync every MIB of logged progress (Unix only)")
    parser.add_argument("--size-mib", type=int, default=512)
    parser.add_argument("--rate-mib", type=float, default=105)
    parser.add_argument("--latency-ms", type=float, default=40)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--report", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if min(args.size_mib, args.rate_mib, args.rounds) <= 0 or args.latency_ms < 0:
        parser.error("size, rate and rounds must be positive; latency must be nonnegative")
    engines = [(label, pathlib.Path(path).resolve()) for label, path in
               (item.split("=", 1) for item in args.engine)]
    checkpoints = {label: int(mib) * 2**20 for label, mib in
                   (item.split("=", 1) for item in args.checkpoint)}
    labels = {label for label, _ in engines}
    if any(label not in labels or size <= 0 or label in args.no_log_label
           for label, size in checkpoints.items()):
        parser.error("checkpoint requires a known, logged label and positive size")
    for label, path in engines:
        if not path.is_file():
            parser.error(f"missing binary: {label}={path}")
    size = args.size_mib * 1024 * 1024
    block = bytes(range(256)) * 256
    doubled = block * 2
    expected = hashlib.sha256()
    for _ in range(size // len(block)):
        expected.update(block)
    expected = expected.hexdigest()
    lock = threading.Lock()
    state = {"started": 0.0, "sent": 0, "requests": 0}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_HEAD(self):
            time.sleep(args.latency_ms / 1000)
            self.send_response(200)
            self.send_header("Content-Length", str(size))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def do_GET(self):
            start, end = 0, size - 1
            value = self.headers.get("Range")
            if value:
                a, b = value.removeprefix("bytes=").split("-", 1)
                start, end = int(a), min(int(b) if b else size - 1, size - 1)
            time.sleep(args.latency_ms / 1000)
            self.send_response(206 if value else 200)
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Accept-Ranges", "bytes")
            if value:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            with lock:
                state["requests"] += 1
            try:
                while start <= end:
                    n = min(len(block), end - start + 1)
                    with lock:
                        if not state["started"]:
                            state["started"] = time.monotonic()
                        state["sent"] += n
                        deadline = state["started"] + state["sent"] / (args.rate_mib * 2**20)
                    delay = deadline - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    offset = start % len(block)
                    self.wfile.write(memoryview(doubled)[offset:offset + n])
                    start += n
            except (BrokenPipeError, ConnectionResetError):
                pass  # The one-byte range probe may be closed after headers.

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    rows = []
    try:
        with tempfile.TemporaryDirectory(prefix="nibiru-perf-") as folder:
            root = pathlib.Path(folder)
            for run in range(args.rounds):
                # Rotate order so a binary is not always first or last.
                order = engines[run % len(engines):] + engines[:run % len(engines)]
                for label, binary in order:
                    output = root / "payload.bin"
                    log = root / "download.jsonl"
                    output.unlink(missing_ok=True)
                    log.unlink(missing_ok=True)
                    with lock:
                        state.update(started=0.0, sent=0, requests=0)
                    command = [str(binary), "download", f"http://127.0.0.1:{server.server_port}/payload.bin",
                               "--out", str(output)]
                    if label not in args.no_log_label:
                        command += ["--log", str(log)]
                    before = resource.getrusage(resource.RUSAGE_CHILDREN)
                    checkpoint_result = dict(count=0, seconds=0.0)
                    stop_checkpoint = threading.Event()

                    def checkpoint(interval):
                        # A separate file handle approximates background flush without
                        # changing the engine. Final engine sync is always preserved.
                        next_at = interval
                        try:
                            with log.open(encoding="utf-8") as records:
                                pending = ""
                                while not stop_checkpoint.is_set():
                                    line = records.readline()
                                    if not line:
                                        stop_checkpoint.wait(0.02)
                                        continue
                                    pending += line
                                    if not pending.endswith("\n"):
                                        continue
                                    record = json.loads(pending)
                                    pending = ""
                                    if record["event"] != "http.progress":
                                        continue
                                    fields = dict(item.split("=", 1) for item in
                                                  record["detail"].split() if "=" in item)
                                    done = int(fields["downloaded"])
                                    if done >= next_at:
                                        part = root / ".payload.bin.nibiru.part"
                                        try:
                                            fd = os.open(part, os.O_RDWR)
                                        except FileNotFoundError:
                                            break
                                        try:
                                            syncing = time.perf_counter()
                                            os.fsync(fd)
                                            checkpoint_result["seconds"] += time.perf_counter() - syncing
                                            checkpoint_result["count"] += 1
                                        finally:
                                            os.close(fd)
                                        next_at = done + interval
                        except Exception as error:
                            checkpoint_result["error"] = repr(error)

                    started = time.perf_counter()
                    with subprocess.Popen(command, stdout=subprocess.DEVNULL,
                                          stderr=subprocess.DEVNULL) as process:
                        worker = None
                        if label in checkpoints:
                            # LogSink opens the log before requests start.
                            while not log.exists() and process.poll() is None:
                                stop_checkpoint.wait(0.005)
                            worker = threading.Thread(target=checkpoint,
                                                      args=(checkpoints[label],))
                            worker.start()
                        try:
                            returncode = process.wait(timeout=120)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                            raise
                        finally:
                            stop_checkpoint.set()
                            if worker:
                                worker.join()
                        elapsed = time.perf_counter() - started
                    after = resource.getrusage(resource.RUSAGE_CHILDREN)
                    if returncode:
                        raise RuntimeError(f"{label} exited with {returncode}")
                    if "error" in checkpoint_result:
                        raise RuntimeError(f"{label}: checkpoint failed: {checkpoint_result}")
                    with output.open("rb") as stream:
                        digest = hashlib.file_digest(stream, "sha256").hexdigest()
                    if output.stat().st_size != size or digest != expected:
                        raise RuntimeError(f"{label}: size/hash mismatch")
                    row = dict(label=label, run=run + 1, seconds=elapsed,
                               mib_per_second=size / elapsed / 2**20,
                               cpu_seconds=after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
                               requests=state["requests"], sha256=digest,
                               log_bytes=log.stat().st_size if log.exists() else 0)
                    if log.exists():
                        records = [json.loads(line) for line in log.read_text().splitlines()]
                        final = next(record for record in reversed(records)
                                     if record["event"] == "http.download_completed")
                        fields = dict(item.split("=", 1) for item in
                                      final["detail"].split() if "=" in item)
                        row["engine_timings_ms"] = {key: int(value) for key, value in fields.items()
                                                   if key.endswith("_ms")}
                    if label in checkpoints:
                        row["external_checkpoint"] = checkpoint_result
                    rows.append(row)
                    print(json.dumps(row), flush=True)
    finally:
        server.shutdown()
        server.server_close()
    medians = {label: {"seconds": statistics.median(r["seconds"] for r in rows if r["label"] == label),
                       "cpu_seconds": statistics.median(r["cpu_seconds"] for r in rows if r["label"] == label)}
               for label, _ in engines}
    report = dict(size_bytes=size, rate_mib=args.rate_mib, latency_ms=args.latency_ms,
                  rounds=args.rounds, checkpoint_bytes=checkpoints, samples=rows, medians=medians)
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"medians": medians}), flush=True)


if __name__ == "__main__":
    main()

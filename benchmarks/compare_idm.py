"""Full-file, alternating IDM/Nibiru benchmark. Python standard library only.

IDM runs through its documented /d /p /f /n /q command line on Windows.
Every run uses a new destination; completion requires process exit, exact length
and SHA-256 consistency. Signed source URLs never enter the report or console.
"""

import argparse
import csv
import datetime as dt
import getpass
import hashlib
import json
import os
import pathlib
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def binary_version(engine, path):
    if engine == "nibiru":
        return subprocess.check_output([str(path), "--version"], text=True).strip()
    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes
    library = ctypes.WinDLL("version", use_last_error=True)
    library.GetFileVersionInfoSizeW.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD)]
    library.GetFileVersionInfoSizeW.restype = wintypes.DWORD
    library.GetFileVersionInfoW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p]
    library.VerQueryValueW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR, ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(wintypes.UINT)]
    handle = wintypes.DWORD()
    size = library.GetFileVersionInfoSizeW(str(path), ctypes.byref(handle))
    if not size:
        return None
    buffer = ctypes.create_string_buffer(size)
    if not library.GetFileVersionInfoW(str(path), 0, size, buffer):
        return None
    pointer, length = ctypes.c_void_p(), wintypes.UINT()
    if not library.VerQueryValueW(buffer, "\\", ctypes.byref(pointer), ctypes.byref(length)) or length.value < 52:
        return None
    fixed = ctypes.cast(pointer, ctypes.POINTER(wintypes.DWORD * 13)).contents
    return ".".join(str(v) for v in [fixed[2] >> 16, fixed[2] & 65535, fixed[3] >> 16, fixed[3] & 65535])


def native_metrics(path):
    metrics = {}
    if not path.is_file():
        return metrics
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            record = json.loads(line)
        except (ValueError, TypeError):
            continue
        event = record.get("event")
        fields = {key: int(value) for key, value in re.findall(r"(?:^| )([a-z][a-z0-9_]*)=([0-9]+)(?= |$)", record.get("detail", ""))}
        if event == "http.progress" and fields.get("downloaded", 0) > 0:
            metrics.setdefault("first_reported_progress_ms", fields.get("elapsed_ms"))
        if event in {"http.segmented_metrics", "http.download_completed", "http.range_handoffs"}:
            metrics[event] = fields
    return metrics


def idm_is_running():
    if os.name != "nt":
        return False
    result = subprocess.run(
        ["tasklist", "/fi", "IMAGENAME eq IDMan.exe", "/fo", "csv", "/nh"],
        capture_output=True, text=True, check=True,
    )
    return any(row and row[0].lower() == "idman.exe" for row in csv.reader(result.stdout.splitlines()))


def source_metadata(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("A fonte precisa ser uma URL HTTP/HTTPS.")
    request = urllib.request.Request(url, method="HEAD", headers={"Accept-Encoding": "identity"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.status != 200:
                raise ValueError(f"A fonte respondeu HTTP {response.status}.")
            size = int(response.headers.get("Content-Length", "0"))
            if size <= 0:
                raise ValueError("A fonte nao informou um tamanho positivo.")
            return dict(host=parsed.hostname, name=pathlib.PurePosixPath(parsed.path).name,
                        size_bytes=size, etag=response.headers.get("ETag"),
                        last_modified=response.headers.get("Last-Modified"),
                        signed_url_sha256=hashlib.sha256(url.encode()).hexdigest())
    except urllib.error.HTTPError as error:
        raise ValueError(f"A fonte respondeu HTTP {error.code}; confira a validade do link.") from None
    except urllib.error.URLError:
        raise ValueError("Nao foi possivel acessar a fonte por HTTP/HTTPS.") from None


def command(engine, binary, url, destination, connections, log, http1_only=False):
    if engine == "idm":
        return [str(binary), "/d", url, "/p", str(destination.parent),
                "/f", destination.name, "/n", "/q"]
    args = [str(binary), "download", url, "--out", str(destination),
            "--connections", str(connections), "--log", str(log)]
    return args + (["--http1-only"] if http1_only else [])


def run_one(engine, binary, url, directory, source, connections, timeout, keep, expected_hash, http1_only=False):
    if engine == "idm" and idm_is_running():
        raise ValueError("Feche o IDM antes do teste: /q so encerra a primeira instancia.")
    # mkdtemp guarantees a fresh file, without removing any existing user download.
    folder = pathlib.Path(tempfile.mkdtemp(prefix=f"{engine}-", dir=directory))
    destination = folder / "windows11.iso"
    log = folder / "nibiru.jsonl"
    arguments = command(engine, binary, url, destination, connections, log, http1_only)
    started_utc = dt.datetime.now(dt.timezone.utc).isoformat()
    started = time.perf_counter()
    with (folder / "process-output.txt").open("wb") as output:
        process = subprocess.Popen(arguments, stdout=output, stderr=subprocess.STDOUT, shell=False)
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise ValueError(f"{engine}: prazo excedido; dados preservados em {folder}.") from None
    elapsed = time.perf_counter() - started
    size = destination.stat().st_size if destination.is_file() else 0
    if code != 0 or size != source["size_bytes"]:
        raise ValueError(f"{engine}: transferencia incompleta (codigo {code}, {size} bytes); veja {folder}.")
    checksum = digest(destination)  # excluded from the download timer
    if expected_hash and checksum != expected_hash.lower():
        raise ValueError(f"{engine}: SHA-256 divergente; dados preservados em {folder}.")
    row = dict(engine=engine, started_utc=started_utc, size_bytes=size,
               elapsed_seconds=elapsed, mib_per_second=size / 2**20 / elapsed,
               mb_per_second=size / 1e6 / elapsed, mbit_per_second=size * 8 / 1e6 / elapsed,
               sha256=checksum, output=str(destination), log=str(log) if log.exists() else None)
    row["native_metrics"] = native_metrics(log)
    if not keep:
        destination.unlink()
        row["output"] = None
    return row


def summarize(rows, engines, tolerance):
    summaries = {}
    for engine in engines:
        samples = [row["elapsed_seconds"] for row in rows if row["engine"] == engine]
        summaries[engine] = dict(runs=len(samples), median_seconds=statistics.median(samples),
                                 minimum_seconds=min(samples), maximum_seconds=max(samples))
    comparison = None
    if "idm" in summaries and "nibiru" in summaries:
        ratio = summaries["idm"]["median_seconds"] / summaries["nibiru"]["median_seconds"]
        comparison = dict(nibiru_speed_ratio=ratio, tolerance_fraction=tolerance,
                          result="faster" if ratio > 1 + tolerance else
                          "slower" if ratio < 1 - tolerance else "within_tolerance",
                          scope="Only these runs, on this host and source; not a universal speed guarantee.")
    return summaries, comparison


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="Prefer the private prompt or --url-file to avoid shell history.")
    parser.add_argument("--url-file", type=pathlib.Path)
    parser.add_argument("--nibiru", type=pathlib.Path, required=True)
    parser.add_argument("--idm", type=pathlib.Path)
    parser.add_argument("--engines", nargs="+", choices=["nibiru", "idm"], default=["nibiru", "idm"])
    parser.add_argument("--connections", type=int, choices=range(1, 17), default=16)
    parser.add_argument("--idm-connections", type=int, help="Setting manually confirmed in IDM Options; not changed by this script.")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=3600)
    parser.add_argument("--directory", type=pathlib.Path)
    parser.add_argument("--expected-sha256")
    parser.add_argument("--tolerance", type=float, default=0.05)
    parser.add_argument("--keep-files", action="store_true")
    parser.add_argument("--http1-only", action="store_true", help="Measure the engine's explicit HTTP/1.1 mode.")
    args = parser.parse_args()
    if args.rounds < 1 or args.timeout <= 0 or not 0 <= args.tolerance < 1:
        parser.error("Rounds/timeout must be positive and tolerance must be in [0, 1).")
    if len(args.engines) != len(set(args.engines)):
        parser.error("Each engine may occur only once.")
    if args.url and args.url_file:
        parser.error("Use either --url or --url-file.")
    if "idm" in args.engines:
        if os.name != "nt":
            parser.error("IDM must run on Windows; no IDM speed will be fabricated on this host.")
        if args.idm_connections != args.connections:
            parser.error("Set IDM to the same connection limit and confirm it with --idm-connections.")
        if args.idm is None:
            args.idm = pathlib.Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "Internet Download Manager" / "IDMan.exe"
        if idm_is_running():
            parser.error("Close IDM before starting the comparison.")
    binaries = {name: getattr(args, name).resolve() for name in args.engines}
    for name, path in binaries.items():
        if not path.is_file():
            parser.error(f"Executable not found for {name}: {path}")
    url = args.url_file.read_text().strip() if args.url_file else args.url
    if not url:
        url = getpass.getpass("Cole o link da ISO e pressione Enter (o link nao aparece): ").strip()
    directory = args.directory or pathlib.Path.cwd() / ("comparacao-idm-" + dt.datetime.now().strftime("%Y%m%d-%H%M%S"))
    directory = directory.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    source = source_metadata(url)
    space_multiplier = 2 if "idm" in args.engines else 1
    required = source["size_bytes"] * space_multiplier + 256 * 2**20
    if shutil.disk_usage(directory).free < required:
        raise ValueError("Espaco livre insuficiente para a ISO e os temporarios. Escolha outra pasta.")
    report = dict(schema_version=1, environment=dict(system=platform.system(), release=platform.release(),
                  machine=platform.machine(), python=platform.python_version()), source=source,
                  connections=args.connections, idm_connections_user_confirmed=args.idm_connections,
                  nibiru_http_mode="http1-only" if args.http1_only else "auto",
                  official_sha256_supplied=bool(args.expected_sha256),
                  binaries={name: dict(path=str(path), sha256=digest(path), version=binary_version(name, path)) for name, path in binaries.items()},
                  rows=[], failures=[])
    summary_file = directory / "comparacao.json"

    def save():
        summary_file.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"ISO: {source['name']} | {source['size_bytes'] / 2**30:.2f} GiB")
    print("Tempos incluem inicio do processo e finalizacao; SHA-256 e calculado depois.")
    save()
    expected = args.expected_sha256
    try:
        for number in range(args.rounds):
            order = args.engines if number % 2 == 0 else list(reversed(args.engines))
            for engine in order:
                print(f"Rodada {number + 1}/{args.rounds}: {engine}...", flush=True)
                row = run_one(engine, binaries[engine], url, directory, source, args.connections,
                              args.timeout, args.keep_files, expected, args.http1_only)
                row["round"] = number + 1
                report["rows"].append(row)
                expected = expected or row["sha256"]
                save()
                print(f"  {row['elapsed_seconds']:.2f} s | {row['mib_per_second']:.2f} MiB/s", flush=True)
    except Exception as error:
        report["failures"].append(str(error).replace(url, "<URL omitida>"))
        save()
        raise
    report["summary"], report["comparison"] = summarize(report["rows"], args.engines, args.tolerance)
    save()
    print(f"Resultado: {summary_file}")
    print("Envie comparacao.json para analisar a diferenca e orientar os proximos ajustes.")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as error:
        # Network exceptions can carry signed URLs. Only controlled errors are printed.
        message = str(error) if isinstance(error, ValueError) else type(error).__name__
        print(f"Erro: {message}", file=sys.stderr)
        sys.exit(1)

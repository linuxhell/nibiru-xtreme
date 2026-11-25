"""Package the tested Windows engine and a verified portable Python runtime."""

import argparse
import hashlib
import io
import json
import pathlib
import struct
import subprocess
import tomllib
import urllib.request
import zipfile

PYTHON_VERSION = "3.14.10"
PYTHON_URL = f"https://www.python.org/ftp/python/{PYTHON_VERSION}/python-{PYTHON_VERSION}-embed-amd64.zip"
PYTHON_SHA256 = "90905e64a840557d8c1bc4f5b74db3d7ee85e3ffb217611cac1c7813acea4159"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    root = pathlib.Path(__file__).resolve().parents[1]
    binary = args.binary.read_bytes()
    if binary[:2] != b"MZ":
        parser.error("The comparison kit requires a Windows x64 executable.")
    pe = struct.unpack_from("<I", binary, 0x3C)[0]
    if binary[pe:pe + 4] != b"PE\0\0" or struct.unpack_from("<H", binary, pe + 4)[0] != 0x8664:
        parser.error("The engine is not a Windows x64 PE executable.")
    with urllib.request.urlopen(PYTHON_URL, timeout=90) as response:
        runtime = response.read(32 * 2**20)
    if hashlib.sha256(runtime).hexdigest() != PYTHON_SHA256:
        raise ValueError("Portable Python SHA-256 mismatch")
    files = {"nibiru-xtreme.exe": binary}
    for name in ["compare_idm.py", "comparar-idm.cmd", "COMPARAR-IDM.md"]:
        files[name] = (root / "benchmarks" / name).read_bytes()
    for name in ["LICENSE", "LICENSE-APACHE-2.0.txt", "THIRD_PARTY_NOTICES.md"]:
        files[name] = (root / name).read_bytes()
    with zipfile.ZipFile(io.BytesIO(runtime)) as archive:
        for entry in archive.infolist():
            path = pathlib.PurePosixPath(entry.filename)
            if path.is_absolute() or ".." in path.parts or entry.file_size > 64 * 2**20:
                raise ValueError("Unsafe portable runtime entry")
            if not entry.is_dir():
                files["runtime/" + entry.filename] = archive.read(entry)
    version = tomllib.loads((root / "Cargo.toml").read_text())["package"]["version"]
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    info = dict(engine_version=version, source_commit=commit, python_version=PYTHON_VERSION,
                python_archive_sha256=PYTHON_SHA256,
                engine_sha256=hashlib.sha256(binary).hexdigest(),
                source_dirty=bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=root)))
    files["build-info.json"] = (json.dumps(info, indent=2) + "\n").encode()
    sums = "".join(f"{hashlib.sha256(data).hexdigest()}  {name}\n" for name, data in sorted(files.items()))
    files["SHA256SUMS.txt"] = sums.encode()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    print(json.dumps(dict(output=str(args.output), **info)))


if __name__ == "__main__":
    main()

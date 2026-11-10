"""Inspect the provided IDM installer without loading or executing Windows code.

Python standard library only. Produces metadata/hashes, never extracted binaries.
The record format is specific to the supplied 6.43 build 18 installer.
"""
import argparse
import hashlib
import json
import pathlib
import re
import struct
import zlib


def pe_metadata(data):
    if data[:2] != b"MZ":
        raise ValueError("not a PE image")
    pe = struct.unpack_from("<I", data, 0x3C)[0]
    if data[pe:pe + 4] != b"PE\0\0":
        raise ValueError("invalid PE signature")
    machine, sections = struct.unpack_from("<HH", data, pe + 4)
    optional_size = struct.unpack_from("<H", data, pe + 20)[0]
    optional = pe + 24
    magic = struct.unpack_from("<H", data, optional)[0]
    if magic not in (0x10B, 0x20B):
        raise ValueError("unsupported PE optional header")
    table = optional + optional_size
    entries = []
    for i in range(sections):
        offset = table + i * 40
        name = data[offset:offset + 8].rstrip(b"\0").decode("ascii", "replace")
        virtual_size, rva, size, position = struct.unpack_from("<IIII", data, offset + 8)
        if position + size > len(data):
            raise ValueError("section outside image")
        entries.append(dict(name=name, rva=rva, virtual_size=virtual_size,
                            size=size, offset=position))

    def address(rva):
        for entry in entries:
            relative = rva - entry["rva"]
            if 0 <= relative < entry["size"]:
                return entry["offset"] + relative
        raise ValueError("RVA outside file-backed sections")

    def cstring(offset):
        end = data.find(b"\0", offset, min(offset + 1024, len(data)))
        if end < 0:
            raise ValueError("unterminated PE name")
        return data[offset:end].decode("ascii", "replace")

    directories = optional + (96 if magic == 0x10B else 112)
    import_rva, _ = struct.unpack_from("<II", data, directories + 8)
    imports = {}
    if import_rva:
        descriptor = address(import_rva)
        for _ in range(512):
            lookup, timestamp, forwarder, name_rva, first = struct.unpack_from("<5I", data, descriptor)
            if not any((lookup, timestamp, forwarder, name_rva, first)):
                break
            names = []
            thunk = address(lookup or first)
            width, code = (4, "<I") if magic == 0x10B else (8, "<Q")
            for index in range(65536):
                value = struct.unpack_from(code, data, thunk + index * width)[0]
                if not value:
                    break
                if value & (1 << (width * 8 - 1)):
                    continue
                names.append(cstring(address(value) + 2))
            imports[cstring(address(name_rva))] = names
            descriptor += 20
    return dict(machine=hex(machine), bits=32 if magic == 0x10B else 64,
                overlay_offset=max(e["offset"] + e["size"] for e in entries),
                sections=entries, imports=imports)


def inspect(path):
    data = path.read_bytes()
    metadata = pe_metadata(data)
    position = metadata["overlay_offset"]
    records, names, engine = [], {}, None
    while position + 32 <= len(data):
        packed = struct.unpack_from("<I", data, position + 4)[0]
        start, end = position + 32, position + 32 + packed
        if packed == 0 or end > len(data):
            break  # installer trailer and Authenticode certificate table
        decoder = zlib.decompressobj()
        try:
            payload = decoder.decompress(data[start:end], 64 * 1024 * 1024 + 1)
        except zlib.error:
            break
        if not decoder.eof or decoder.unused_data:
            raise ValueError("oversized or incomplete compressed record")
        index = len(records)
        if index == 0:
            manifest = payload.decode("utf-8", "replace")
            names = {int(n): name for n, name in re.findall(r'<P(\d+)="([^"]+)"', manifest)}
        name = "installer-manifest" if index == 0 else names.get(index, f"entry-{index}")
        record = dict(index=index, name=name, offset=position, packed=packed,
                      size=len(payload), sha256=hashlib.sha256(payload).hexdigest())
        records.append(record)
        if pathlib.PureWindowsPath(name).name.lower() == "idman.exe":
            engine = dict(record, pe=pe_metadata(payload), indicators={
                term: term.encode("ascii") in payload for term in
                ("Content-Range:", "Range: bytes=%I64d-%I64d", "If-Range:",
                 "Connection: Keep-Alive", "SSL_read", "SSL_write",
                 "Assembling all downloaded portions into one file...")})
        position = end
    if engine is None:
        raise ValueError("no IDMan.exe payload found")
    return dict(installer=dict(name=path.name, size=len(data),
                              sha256=hashlib.sha256(data).hexdigest(), pe=metadata),
                record_count=len(records), records=records, engine=engine,
                executed=False, algorithm_recovered=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("installer", type=pathlib.Path)
    parser.add_argument("--report", type=pathlib.Path, required=True)
    args = parser.parse_args()
    result = inspect(args.installer)
    args.report.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(dict(report=str(args.report), records=result["record_count"],
                          engine_sha256=result["engine"]["sha256"])))


if __name__ == "__main__":
    main()

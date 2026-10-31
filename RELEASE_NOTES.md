# Nibiru Xtreme 0.2.3

- Support a validated RPC `out` basename for multi-file torrent roots and single-file payloads; keep torrent metadata and info hashes unchanged.
- Advertise `BitTorrent-Out` so ADM can require exact destination support instead of silently creating nested folders.
- Preserve nested content paths, selected files, default layouts and resume verification. Reject traversal and changes to `out` after creation.
- Includes storage and real loopback RPC download regressions for reserved destinations, single-file output and resume.

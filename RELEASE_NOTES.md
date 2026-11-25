# Nibiru Xtreme 0.2.9

- Fix excluded torrent files being created or overwritten when they share a verified piece with a selected file. Keep required boundary bytes in internal piece storage while preserving verified resume, upload and later file selection.
- Apply file-selection changes after the initial disk check; release deselected buffers and cancel outstanding peer requests. Reject invalid selections and handle zero-length reads safely.
- Merge repeated selection ranges before expanding file indexes, bounding temporary allocations and keeping file-mask construction linear.
- Increase the maximum HTTP range from 32 to 128 MiB while retaining 4 MiB write buffers and the selected limit of up to 16 workers. Preserve identity validation, Retry-After, resumable blocks, tail redistribution and atomic publication.
- Add Windows IDM comparison tooling with alternating full downloads, executable/file hashes, completeness checks and a checksum-verified portable Python runtime in CI artifacts. The engine continues to use its own implementation; no proprietary IDM code is included.
- In the documented Microsoft ISO test, HTTP/2 range requests fell from 288 to 114. The small timing difference from a single run per profile does not establish a sustained speed increase or universal superiority over IDM.

See [the audit, measurements and limits](https://github.com/linuxhell/nibiru-xtreme/blob/v0.2.9/docs/AUDIT-2026-11-25.md). Windows x64, Linux x64 and macOS Intel x64 binaries include SHA-256 sidecars and license notices.

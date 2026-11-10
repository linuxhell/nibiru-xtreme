# Nibiru Xtreme 0.2.7

- Remove the discarded one-byte GET from segmented HTTP startup; validate the real payload ranges instead. The explicit probe API retains its range test.
- Use bounded larger ranges and let idle workers take block-aligned unreserved tails from busy workers. Preserve per-block ownership, validated resume, connection caps and atomic publication.
- Report all bytes written before disk synchronization/publication, allowing ADM to distinguish finalization from completion.
- Include reproducible static inspection of the supplied IDM installer, without executing or distributing its payloads, and controlled before/after Nibiru measurements. Five-run synthetic medians improved by 13.6% and 6.0% in two scenarios; these are not benchmarks against IDM or proof of Internet/P2P acceleration.
- Preserve the following 0.2.6 corrections:

- Announce the actual incoming TCP listener port in ED2K HELLOANSWER packets, including ephemeral fallback ports; previously this field was always zero.
- Discover UPnP gateways across distinct local IPv4 addresses, retaining the default route and existing gateway URL trust checks. Retry lost SSDP requests within the same bounded discovery deadline and cancel remaining searches when one succeeds.
- Expose accepted TCP connections separately from completed ED2K handshakes for diagnostics.
- Add wire-level regression assertions for the announced port, a dropped first SSDP search batch, interface filtering and discovery bounds. Preserve callback, upload, obfuscation and torrent behavior.
- Retain the shared UDP/TCP peer-port and HTTP startup fixes from 0.2.5.

UPnP mapping and a TCP port check alone do not guarantee High ID. The real ED2K server response remains authoritative; the user must retest their network.

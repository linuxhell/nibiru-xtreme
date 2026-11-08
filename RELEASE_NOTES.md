# Nibiru Xtreme 0.2.6

- Announce the actual incoming TCP listener port in ED2K HELLOANSWER packets, including ephemeral fallback ports; previously this field was always zero.
- Discover UPnP gateways across distinct local IPv4 addresses, retaining the default route and existing gateway URL trust checks. Retry lost SSDP requests within the same bounded discovery deadline and cancel remaining searches when one succeeds.
- Expose accepted TCP connections separately from completed ED2K handshakes for diagnostics.
- Add wire-level regression assertions for the announced port, a dropped first SSDP search batch, interface filtering and discovery bounds. Preserve callback, upload, obfuscation and torrent behavior.
- Retain the shared UDP/TCP peer-port and HTTP startup fixes from 0.2.5.

UPnP mapping and a TCP port check alone do not guarantee High ID. The real ED2K server response remains authoritative; the user must retest their network.

# Nibiru Xtreme 0.2.5

- Fix automatic peer-port allocation on Windows: reserve UDP/uTP first, then bind TCP to that same port. A TCP-only ephemeral allocation can be unavailable to UDP.
- Retry automatic allocations on TCP address conflicts or access denial, with a bound of 32 attempts. Keep explicitly requested ports and uTP enabled.
- Cancel and release abandoned uTP dispatchers on failed attempts or cancelled startup. Preserve IPv4 fallback for an unspecified IPv6 bind.
- Add regressions for shared TCP/uTP ports, bind conflicts, retry exhaustion, cleanup and explicit UDP reservations. Existing torrent, magnet and forced uTP interoperability tests remain enabled.
- Correct a flaky ED2K wire test by checking the exact full ciphertext rather than assuming an encrypted byte can never match its plaintext value. ED2K encryption is unchanged.
- Retain the HTTP startup improvements from 0.2.4.

# Third-party transport notices

The BitTorrent uTP transport uses `librqbit-utp` 0.7 and dual-stack socket binding
uses `librqbit-dualstack-sockets` 0.7, by Igor Katson, under Apache License 2.0.
These are Rust dependencies. They are not libtorrent.

Copyright 2021 Igor Katson

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

The complete Apache 2.0 text is included in `LICENSE-APACHE-2.0.txt`.
The project's own code remains MIT-licensed. Other dependencies retain their
respective licenses; this file records the transport dependencies added here.


## Direct dependencies

Inventory from this branch’s `Cargo.lock` and downloaded crate manifests.
Optional transports retain their feature gates. This is a direct-dependency
inventory; transitive dependencies retain their own notices and licenses.

| Crate | Locked version | License expression |
| --- | --- | --- |
| `anyhow` | 1.0.104 | MIT OR Apache-2.0 |
| `base64` | 0.22.1 | MIT OR Apache-2.0 |
| `base64` | 0.23.1 | MIT OR Apache-2.0 |
| `clap` | 4.6.7 | MIT OR Apache-2.0 |
| `flate2` | 1.1.10 | MIT OR Apache-2.0 |
| `futures-util` | 0.3.34 | MIT OR Apache-2.0 |
| `getrandom` | 0.2.17 | MIT OR Apache-2.0 |
| `getrandom` | 0.3.4 | MIT OR Apache-2.0 |
| `getrandom` | 0.4.3 | MIT OR Apache-2.0 |
| `librqbit-dualstack-sockets` | 0.7.0 | Apache-2.0 |
| `librqbit-utp` | 0.7.0 | Apache-2.0 |
| `md4` | 0.10.2 | MIT OR Apache-2.0 |
| `md5` | 0.8.1 | Apache-2.0 OR MIT |
| `num-bigint` | 0.4.8 | MIT OR Apache-2.0 |
| `num-bigint` | 0.5.1 | MIT OR Apache-2.0 |
| `percent-encoding` | 2.3.2 | MIT OR Apache-2.0 |
| `reqwest` | 0.12.28 | MIT OR Apache-2.0 |
| `russh` | 0.64.1 | Apache-2.0 |
| `russh-sftp` | 3.0.1 | Apache-2.0 |
| `rustls` | 0.23.46 | Apache-2.0 OR ISC OR MIT |
| `serde` | 1.0.229 | MIT OR Apache-2.0 |
| `serde_json` | 1.0.154 | MIT OR Apache-2.0 |
| `sha1` | 0.10.7 | MIT OR Apache-2.0 |
| `sha1` | 0.11.0 | MIT OR Apache-2.0 |
| `sha2` | 0.10.9 | MIT OR Apache-2.0 |
| `sha2` | 0.11.1 | MIT OR Apache-2.0 |
| `socket2` | 0.6.5 | MIT OR Apache-2.0 |
| `suppaftp` | 12.2.1 | MIT OR Apache-2.0 |
| `tempfile` | 3.27.0 | MIT OR Apache-2.0 |
| `tokio` | 1.54.0 | MIT |
| `tokio-util` | 0.7.20 | MIT |
| `url` | 2.5.8 | MIT OR Apache-2.0 |
| `uuid` | 1.28.0 | Apache-2.0 OR MIT |
| `webpki-roots` | 1.0.10 | CDLA-Permissive-2.0 |

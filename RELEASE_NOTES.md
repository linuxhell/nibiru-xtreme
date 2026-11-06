# Nibiru Xtreme 0.2.4

- Start single-connection HTTP/HTTPS downloads with GET instead of waiting for HEAD and an unused one-byte range test.
- Skip the one-byte test when the file length or minimum split size rules out segmentation. Keep the public diagnostic probe and segmented range validation intact.
- Preserve connection caps, validated resume, destination protection, headers, proxies and rate limits.
- Include RPC request-count regressions and a reproducible local comparison with 40 SHA-256-verified transfers. The measured gain is startup latency; sustained multi-connection throughput was unchanged within measurement noise.

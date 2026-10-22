[Português](README.md) · [English](README.en.md) · [简体中文](README.zh-CN.md)

# Nibiru Xtreme

Rust 下载引擎，提供原生 JSON-RPC 契约及部分 aria2 兼容接口。

HTTPS 同时使用公共根证书和系统信任库，继续验证 TLS 证书。RPC 诊断会标识证书信任错误，不暴露 URL 或凭据。

## 状态

**稳定版 0.2.1。** Apocalipse Download Manager 的原生下载引擎，提供 Windows、Linux 和 macOS 程序及 SHA-256 校验文件。

ED2K 预览不依赖固定的 30–40 MB 总下载量：首个分块通过 MD4 验证后即可启用（最多 9,728,000 字节）。其他来源会优先协助完成文件开头和末尾，避免慢速来源拖延预览。MP4/AVI 可能还需要末尾索引；尚未下载的范围会等待数据，不会向播放器发送文件空洞。

用户在 2026-10-22 提供的日志证实了公网 ED2K 下载、UPnP 高 ID 和 ADM 自动重试。该文件已验证 86 个分块中的 77 个，已连接来源缺少其余 9 个；这并不代表该文件已下载完成或所有路由器都兼容。详见[协议](docs/protocols.md)和[能力矩阵](docs/CAPABILITY-MATRIX.md)。

## 协议与限制

| 领域 | 实现与限制 |
| --- | --- |
| HTTP/1.1、HTTPS、HTTP/2 | 分段下载、资源身份校验的 Range 续传、请求头/代理及 HTTP 带宽限制 |
| HTTP/3 | 使用 `--http3-only` 显式选择 HTTPS/QUIC；无自动 H2/H3 选择 |
| FTP/FTPS | 下载和 REST 续传；显式/隐式 TLS；隐式续传缺少独立测试 |
| SFTP | 下载和 seek 续传；必须提供 SSH 指纹；密码或未加密 OpenSSH 私钥 |
| WebDAV | 单文件 GET 别名；无集合、PROPFIND、锁或写入 |
| BitTorrent | v1/v2/混合、`btih`/`btmh` 磁力链接、元数据、PEX、Tracker、Web seed、选择文件、做种和流式预览 |
| TCP/uTP 与 MSE/PE | 可选或强制混淆；MSE 不提供对端身份认证 |
| DHT / LSD | IPv4/IPv6 DHT；LSD 需主动开启，仅支持 IPv4 单接口 |
| 打洞 | 基于 uTP 的 BEP 55；仅验证受限回环模拟，未验证真实 NAT 穿透 |
| ED2K | 按分块 MD4 下载，最多同时八个来源，续传时保留已验证的哈希集并在联系节点前先校验部分文件，停止原因报告，客户端间混淆，服务器/buddy 回调 |
| UPnP | 在路由器上映射所请求的端口（`serve --upnp`），自动续期并在退出时移除；自动测试使用模拟路由器；用户日志证实了真实映射与高 ID |
| Kad2 | 持久会话、来源查找、联系人验证、IP 过滤和 UDP 混淆；高级功能尚未完成 |

显式共享、上传及 ED2K 队列仅提供库 API，尚不是完整的 RPC/CLI 共享服务。待完成项目包括 Metalink、完整 WebDAV、积分、安全身份认证、AICH、来源交换、ED2K 客户端与服务器混淆、Kad 关键词搜索/发布、防火墙检测及作为 buddy 服务。

## 使用

```sh
cargo build --release --locked
nibiru-xtreme capabilities --json
nibiru-xtreme download https://example.test/file.bin --out file.bin
nibiru-xtreme serve --port 6800 --secret SECRET --dir downloads --session-file session.json
```

`serve` 绑定 IPv4 回环地址。`nibiru.*` 是原生契约；`aria2.*` 别名只覆盖已实现方法。`--session-file` 以稳定 GID 恢复未完成任务，恢复后处于暂停状态。带有可识别凭据的任务不会保存，需要管理器重建。旧版 `--input-file`、`--save-session` 不等同于原生持久化。

`nibiru-xtreme diagnose URL --dir 文件夹` 会分别测量无磁盘的网络、磁盘以及同一链接上的引擎，并写出 `nibiru-diagnose-report.txt`。`download` 中按 Ctrl+C 即暂停，再次运行同一命令会从中断处继续。

ED2K 使用 `--ed2k`、`--ed2k-server host:port`，可选 `--ed2k-nodes-file nodes.dat`。LSD 使用 `--enable-lpd [--lpd-interface IP]`。选项与限制请参阅 `--help` 和协议文档。不要将真实凭据写入终端历史。

## 结构与验证

| 路径 | 职责 |
| --- | --- |
| `src/engine.rs`、`src/storage` | HTTP、调度和存储 |
| `src/protocols` | FTP/FTPS、SFTP 和 HTTP/3 |
| `src/bittorrent` | BitTorrent、DHT、Tracker 和传输 |
| `src/ed2k` | ED2K、Kad、混淆及显式共享 |
| `src/rpc` 和 `src/rpc.rs` | 任务、持久化和 JSON-RPC |
| `tests`、`benchmarks` | 回归、互操作及测量 |

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```

独立测试将 libtorrent 2.1.1 和 aMule 作为其他客户端，而非内置引擎，需要单独安装。缺少外部程序或 IPv6 时，部分测试可能跳过，请检查输出。用户的 Windows 日志显示了公网 ED2K 流量和真实 UPnP 映射；协议自动测试仍采用回环环境，尚未验证所有 NAT 配置或该不完整文件的完成。HTTP 默认采用固定调度，自适应和后台检查点需要主动开启；基准测试不代表所有环境都更快。

## 安全、CI 与许可

项目自身 Rust 禁止 `unsafe`。TLS 校验证书，SFTP 要求已知主机密钥。JSON-RPC 提供可选令牌及连接、读取和大小限制；应为管理器设置密钥。JSON Lines 日志中的 HTTP 诊断 URL 移除凭据、查询及片段，包括请求错误。

`ci.yml` 测试三个系统，在 Linux 上验证互操作性，每个平台的测试产物仅包含可执行文件；Windows 测试产物只有 EXE。`release.yml` 复用这些二进制文件，发布 Windows x64、Linux x64（静态 musl）和 macOS x64 版本，并提供 SHA-256 校验文件与许可证。此版本后将根据日常使用中发现的问题进行维护。TCP、uTP、DHT 和预览监听器现在会随其所有者关闭。

MIT；依赖保留各自许可。参见 [LICENSE](LICENSE)、[第三方声明](THIRD_PARTY_NOTICES.md)及 [Apache 2.0](LICENSE-APACHE-2.0.txt)。

## 赞助项目

Nibiru Xtreme 是免费的。如果它对您有用，可以通过 PayPal 支持：[捐赠给 jv12802@gmail.com](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=jv12802%40gmail.com&currency_code=BRL)。捐赠完全自愿，除已公开的内容外不提供任何额外权益，也不影响许可、支持或进度。

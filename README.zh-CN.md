[Português](README.md) · [English](README.en.md) · [简体中文](README.zh-CN.md)

# Nibiru Xtreme

Rust 下载引擎，提供原生 JSON-RPC 契约及部分 aria2 兼容接口。

## 状态

**0.2.0-alpha.2 仍在开发，尚未完成全部多协议功能。** 在 Xtreme 完成并通过集成验证前，ADM 保留现有引擎。2026-10-21 查询时尚无已发布 release；CI 产物是测试候选文件。

代码审计仍在进行。“已编译”“已通过自动测试”和“已在真实使用环境验证”是不同状态。证据见[能力矩阵](docs/CAPABILITY-MATRIX.md)，格式和限制见[协议文档](docs/protocols.md)。

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
| ED2K | MD4 分块下载、最多四个并行来源、续传、客户端混淆及服务器/buddy 回调 |
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

独立测试将 libtorrent 2.1.1 和 aMule 作为其他客户端，而非内置引擎，需要单独安装。缺少外部程序或 IPv6 时，部分测试可能跳过，请检查输出。本基线未在公网 ED2K/Kad、真实 NAT 或 Windows/macOS 实际使用中验证。HTTP 默认采用固定调度，自适应和后台检查点需要主动开启；基准测试不代表所有环境都更快。

## 安全、CI 与许可

项目自身 Rust 禁止 `unsafe`。TLS 校验证书，SFTP 要求已知主机密钥。JSON-RPC 提供可选令牌及连接、读取和大小限制；应为管理器设置密钥。JSON Lines 日志中的 HTTP 诊断 URL 移除凭据、查询及片段，包括请求错误。

`ci.yml` 检查 Windows、Linux、macOS，并在 Linux 上运行互操作测试。`release.yml` 构建安装包，发布需明确授权。ADM 的分发及签名清单契约仍待完成。

MIT；依赖保留各自许可。参见 [LICENSE](LICENSE)、[第三方声明](THIRD_PARTY_NOTICES.md)及 [Apache 2.0](LICENSE-APACHE-2.0.txt)。

# Nibiru Xtreme

Nibiru Xtreme 是使用 Rust 开发的下载引擎，提供部分兼容 aria2 的 JSON-RPC。
目前仍是开发中的 alpha 版本；在功能对齐和迁移测试完成之前，ADM 保留现有生产引擎。

## 本分支已实现

- HTTP/HTTPS/HTTP2 下载、分段、Range 断点续传和带宽限制。
- 显式选择的 HTTP/3、FTP/FTPS、SFTP 下载；SFTP 必须提供可信 SSH 主机密钥指纹。
- WebDAV 单文件 GET 别名，不支持集合遍历。
- BitTorrent v1/v2/混合、btih/btmh Magnet、元数据交换、Merkle 证明验证、PEX、Tracker、
  哈希校验续传、文件选择、做种和下载时预览。
- TCP/uTP、MSE/PE 协商及强制加密模式。
- 独立的 IPv4/IPv6 DHT、BEP 42 节点 ID 和节点持久化。
- 原生 ED2K 搜索、传输和显式共享基础；Kad 仅实现 bootstrap。

独立客户端 libtorrent 2.1.1 的 18 个场景覆盖 v1/v2/混合及 TCP/uTP/MSE，
并逐字节比较完整上传和 Magnet 下载结果。引擎没有嵌入 libtorrent。
uTP 使用 Rust 依赖 librqbit-utp；参见[第三方声明](THIRD_PARTY_NOTICES.md)。

## 尚未完成

Hole punching、本地发现、Kad 持久路由/搜索/发布、ED2K 积分/安全身份/混淆/AICH、
Metalink、完整 WebDAV、FTP/SFTP 续传和完整 aria2 RPC/选项兼容仍待实现。
MSE 是传统混淆协议，不提供身份认证。项目自身的 Rust 代码禁止 unsafe；依赖有各自的实现和许可证。

详见[协议支持和限制](docs/protocols.md)。

Português: [README.md](README.md)  
English: [README.en.md](README.en.md)

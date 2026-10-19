# Nibiru Xtreme

Nibiru Xtreme 是一个使用 Rust 从零开发的跨平台下载引擎。目标是：体积小、安全边界清晰、吞吐量高、支持断点续传，并提供简洁而有效的诊断日志。

## 当前状态

仓库目前包含第一版可运行基础引擎：

- HTTP/HTTPS 下载；
- 支持 HTTP/2 的 Rustls 客户端；
- 服务器支持 Range 时可安全断点续传；
- 简洁的 JSON Lines 诊断日志；
- 仅监听本机回环地址的 JSON-RPC；
- 初步兼容 ADM 迁移所需的 aria2 RPC 子集；
- 严格验证 RPC 输出文件名，防止路径穿越；
- 项目自有代码禁止使用 `unsafe`。

BitTorrent v1/v2/混合、Magnet、DHT、PEX、uTP、HTTP/3/QUIC、FTP/FTPS、SFTP、WebDAV 和 ED2K/Kad 将作为独立模块逐步实现，并且只有通过互操作性、回归和性能测试后才会标记为稳定。

在 Nibiru 达到功能完整并通过迁移测试之前，ADM 不会移除现有生产下载引擎。

Português: [README.md](README.md)  
English: [README.en.md](README.en.md)

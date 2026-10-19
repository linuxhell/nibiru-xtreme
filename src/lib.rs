#![forbid(unsafe_code)]

pub mod ed2k;
pub mod engine;
pub mod logging;
pub mod protocols;
pub mod rpc;
pub mod storage;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

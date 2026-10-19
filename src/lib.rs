#![forbid(unsafe_code)]

pub mod engine;
pub mod logging;
pub mod rpc;
pub mod storage;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

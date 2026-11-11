//! Safe platform boundary for synchronous files opened here. The engine keeps
//! its unsafe-code prohibition; Win32 FFI is confined to this small dependency.
#![deny(unsafe_op_in_unsafe_fn)]
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::SparseFile;

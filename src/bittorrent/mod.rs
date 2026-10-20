//! Native BitTorrent: bencode, metainfo, magnet links, trackers, peer wire protocol and the
//! download/upload engine. Implemented from the BEP specifications; see docs/bittorrent.md.
pub mod bencode;
pub mod bitfield;
pub mod magnet;
pub mod message;
pub mod metainfo;
pub mod picker;
pub mod storage;
pub mod tracker;

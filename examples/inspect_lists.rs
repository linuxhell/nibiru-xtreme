//! Read downloaded interchange files with the same parser used by the client.
use anyhow::{Context, Result};
fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let servers = std::fs::read(args.next().context("server.met path missing")?)?;
    let nodes = std::fs::read(args.next().context("nodes.dat path missing")?)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "servers":nibiru_xtreme::ed2k::metadata::parse_servers(&servers)?,
            "nodes":nibiru_xtreme::ed2k::metadata::parse_nodes(&nodes)?
        }))?
    );
    Ok(())
}

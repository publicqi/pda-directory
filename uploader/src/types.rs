use std::path::PathBuf;

use clap::Parser;
use serde::{Deserialize, Serialize};
use solana_address::Address;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(C)]
pub struct PdaSqlite {
    pub pda: Address,
    pub seeds: Vec<Vec<u8>>,
    pub program_id: Address,
}

#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// Path to the directory containing hashmaps
    #[arg(short, long)]
    pub path: PathBuf,

    /// Path of existing dedup hashset
    #[arg(short, long, default_value = "/tmp/dedup")]
    pub dedup_hashset_file: PathBuf,

    /// Cloudflare token
    #[arg(short, long)]
    pub token: String,

    /// Cloudflare account id
    #[arg(short, long)]
    pub account_id: String,

    /// Blue D1 database id
    #[arg(long)]
    pub blue_db_id: Option<String>,

    /// Green D1 database id
    #[arg(long)]
    pub green_db_id: Option<String>,
}

// two subcommands:
// 1. one time merge (into one sqlite)
// 2. loop upload

use std::{
    fs::{self, File},
    io::BufReader,
    path::PathBuf,
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use clap::{Parser, Subcommand, arg, command};
use eyre::Result;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use solana_address::Address;

#[derive(Debug, Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Merge(MergeArgs),
    Upload(UploadArgs),
}

#[derive(Debug, Parser)]
struct MergeArgs {
    #[arg(short, long)]
    path: PathBuf,

    #[arg(short, long)]
    output: PathBuf,
}

#[derive(Debug, Parser)]
struct UploadArgs {
    #[arg(short, long)]
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(C)]
struct PdaSqlite {
    pda: Address,
    seeds: Vec<Vec<u8>>,
    program_id: Address,
}

const PDA_SQLITE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS pda_registry (
    pda BLOB PRIMARY KEY,
    program_id BLOB NOT NULL,
    seed_count INTEGER NOT NULL,
    seed_bytes BLOB NOT NULL
) WITHOUT ROWID;
"#;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match args.command {
        Command::Merge(args) => match merge(args) {
            Ok(_) => println!("Merged successfully"),
            Err(e) => println!("Failed to merge: {e}"),
        },
        Command::Upload(args) => match upload_loop(args).await {
            Ok(_) => println!("Uploaded successfully"),
            Err(e) => println!("Failed to upload: {e}"),
        },
    }
}

#[allow(unused)]
async fn upload_loop(args: UploadArgs) -> Result<()> {
    Ok(())
}

fn merge(args: MergeArgs) -> Result<()> {
    let files: Vec<PathBuf> = all_valid_files(args.path)?;
    println!("Found {} files", files.len());

    let rw_entries: Arc<RwLock<Vec<PdaSqlite>>> = Arc::new(RwLock::new(Vec::new()));
    let num_files_dealt = Arc::new(AtomicUsize::new(0));
    let total_files = files.len();

    println!("Starting deserialization of {total_files} files");

    files.clone().into_par_iter().for_each(|file| {
        println!("Deserializing file {file:?}...");

        let pda_sqlite = deserialize_pda_sqlite(&file).unwrap_or_else(|err| {
            eprintln!("Failed to deserialize file {file:?}: {err}");
            panic!("Failed to deserialize pda sqlite");
        });

        rw_entries.write().unwrap().extend(pda_sqlite);
        let processed = num_files_dealt.fetch_add(1, Ordering::Relaxed) + 1;
        println!("Finished deserializing file {file:?} ({processed}/{total_files})");
    });

    let entries = rw_entries.read().unwrap();

    println!("Deserialized {} entries", entries.len());

    let sqlite = rusqlite::Connection::open(args.output)?;
    sqlite.execute_batch(PDA_SQLITE_SCHEMA)?;
    println!("Executed schema");
    for pda_sqlite in &*entries {
        let encoded_seeds = encode_seeds_for_storage(&pda_sqlite.seeds);
        sqlite.execute(
            "INSERT INTO pda_registry (pda, program_id, seed_count, seed_bytes) VALUES (?, ?, ?, ?)",
            (pda_sqlite.pda.to_string(), pda_sqlite.program_id.to_string(), pda_sqlite.seeds.len(), encoded_seeds),
        )?;
    }
    println!("Inserted {} entries", entries.len());
    sqlite.close().unwrap();
    // delete all files
    for file in &files {
        fs::remove_file(file)?;
    }
    println!("Deleted {} files", files.len());
    Ok(())
}

fn encode_seeds_for_storage(seeds: &[Vec<u8>]) -> Vec<u8> {
    let total_seed_bytes = seeds.iter().map(|seed| seed.len()).sum::<usize>();
    let mut encoded =
        Vec::with_capacity(total_seed_bytes + (seeds.len() + 1) * std::mem::size_of::<u32>());
    encoded.extend_from_slice(&(seeds.len() as u32).to_le_bytes());
    for seed in seeds {
        encoded.extend_from_slice(&(seed.len() as u32).to_le_bytes());
        encoded.extend_from_slice(seed);
    }
    encoded
}

fn all_valid_files(root: PathBuf) -> Result<Vec<PathBuf>> {
    let now = SystemTime::now();
    // for all file that starts with "pda_collector_" and ends with ".blob"
    // and age > 5 seconds
    let mut files = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let filename = path.file_name().unwrap().to_str().unwrap();
        if filename.starts_with("pda_collector_") && filename.ends_with(".blob") {
            let metadata = entry.metadata()?;
            let age = now.duration_since(metadata.modified()?).unwrap_or_default();
            if age > Duration::from_secs(5) {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn deserialize_pda_sqlite(path: &PathBuf) -> Result<Vec<PdaSqlite>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    bincode::deserialize_from(reader).map_err(eyre::Report::from)
}

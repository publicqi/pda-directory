// two subcommands:
// 1. one time merge (into one sqlite)
// 2. loop upload

use std::{
    cmp,
    fs::{self, File},
    io::BufReader,
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use clap::{Parser, Subcommand, arg, command};
use eyre::Result;
use rayon::prelude::*;
use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use solana_address::Address;

// Tune these to your data & disk
const SHARD_CAP: usize = 8; // max number of shard DBs to write in parallel
const INSERT_BATCH_SIZE: usize = 10_000; // rows per transaction

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
        let pda_sqlite = deserialize_pda_sqlite(&file).unwrap_or_else(|err| {
            eprintln!("Failed to deserialize file {file:?}: {err}");
            panic!("Failed to deserialize pda sqlite");
        });

        rw_entries.write().unwrap().extend(pda_sqlite);
        let processed = num_files_dealt.fetch_add(1, Ordering::Relaxed) + 1;
        println!(
            "Finished deserializing file ({processed}/{total_files}) {} entries so far",
            rw_entries.read().unwrap().len()
        );
    });

    let entries = rw_entries.read().unwrap();
    println!("Deserialized {} entries", entries.len());

    // === Parallel batched writes into shard DBs ===
    let shard_count = cmp::min(
        cmp::max(2, num_cpus::get()),
        cmp::min(
            SHARD_CAP,
            cmp::max(1, entries.len() / INSERT_BATCH_SIZE).max(1),
        ),
    );

    println!("Using {shard_count} shard DBs (batch size {INSERT_BATCH_SIZE})");

    // Compute chunk size so all shards get roughly equal rows.
    let chunk_size = entries.len().div_ceil(shard_count);

    // Pre-create shard paths
    let shard_paths: Vec<PathBuf> = (0..shard_count)
        .map(|i| args.output.with_extension(format!("shard{i}.sqlite")))
        .collect();

    // Write to shards in parallel
    entries
        .par_chunks(chunk_size.max(1))
        .enumerate()
        .try_for_each(|(i, chunk)| -> Result<()> {
            let shard_path = &shard_paths[i];
            if shard_path.exists() {
                fs::remove_file(shard_path)?;
            }
            let mut conn = open_sqlite_for_fast_ingest(shard_path)?;
            init_schema(&conn)?;
            let inserted = fast_insert_slice(&mut conn, chunk, INSERT_BATCH_SIZE)?;
            conn.close().unwrap();
            println!(
                "Shard #{i}: inserted {inserted} rows → {}",
                shard_path.display()
            );
            Ok(())
        })?;

    // === Merge shards into the final DB ===
    if args.output.exists() {
        fs::remove_file(&args.output)?;
    }
    let dst = open_sqlite_for_fast_ingest(&args.output)?;
    init_schema(&dst)?;

    for (i, shard) in shard_paths.iter().enumerate() {
        if !shard.exists() {
            continue;
        }
        attach_and_copy(&dst, shard)?;
        fs::remove_file(shard)?;
        println!("Merged shard #{i} from {}", shard.display());
    }

    dst.close().unwrap();
    println!(
        "Executed schema + merged shards into {}",
        args.output.display()
    );

    // Clean up source blobs only after successful merge
    for file in &files {
        fs::remove_file(file)?;
    }
    println!("Deleted {} input blob files", files.len());
    Ok(())
}

// Open a connection tuned for fast bulk insert
fn open_sqlite_for_fast_ingest(path: &Path) -> rusqlite::Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX; // rusqlite handles thread-safety at the Rust level

    let conn = Connection::open_with_flags(path, flags)?;
    conn.busy_timeout(std::time::Duration::from_secs(60))?;

    // Apply pragmas before creating the schema for max effect
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;      -- balance safety & speed (FULL is safer, OFF is fastest)
        PRAGMA temp_store=MEMORY;
        PRAGMA mmap_size=134217728;     -- 128 MiB
        PRAGMA page_size=4096;          -- applies only to new DBs before first write
        PRAGMA cache_size=-262144;      -- 256 MiB page cache in KiB (negative => KiB)
        PRAGMA locking_mode=EXCLUSIVE;  -- fewer lock transitions during bulk load
        PRAGMA wal_autocheckpoint=1000; -- checkpoint less frequently during ingest
        "#,
    )?;

    Ok(conn)
}

// Create the table if needed
fn init_schema(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(PDA_SQLITE_SCHEMA)?;
    Ok(())
}

// Batch insert a slice of rows using transactions and a cached prepared statement.
fn fast_insert_slice(
    conn: &mut Connection,
    rows: &[PdaSqlite],
    batch: usize,
) -> rusqlite::Result<usize> {
    let mut total = 0usize;
    for chunk in rows.chunks(batch.max(1)) {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Exclusive)?;
        {
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO pda_registry \
             (pda, program_id, seed_count, seed_bytes) \
             VALUES (?1, ?2, ?3, ?4)",
            )?;

            // NOTE: Address implements AsRef<[u8]> / to_bytes() in `solana_address`, so we can bind as BLOBs.
            // (https://docs.rs/solana-address/latest/solana_address/struct.Address.html)

            for row in chunk {
                let pda_bytes: &[u8] = row.pda.as_ref();
                let program_bytes: &[u8] = row.program_id.as_ref();
                let seed_bytes = encode_seeds_for_storage(&row.seeds);

                stmt.execute(params![
                    pda_bytes,
                    program_bytes,
                    row.seeds.len() as i64,
                    seed_bytes
                ])?;
            }
        }
        tx.commit()?;
        total += chunk.len();
    }
    Ok(total)
}

// Merge a shard DB into the destination using ATTACH + INSERT
fn attach_and_copy(dst: &Connection, shard_path: &Path) -> rusqlite::Result<()> {
    let mut literal = shard_path.to_string_lossy().into_owned();
    // Escape single quotes for SQL literal
    if literal.contains('\'') {
        literal = literal.replace('\'', "''");
    }
    let sql = format!(
        "ATTACH DATABASE '{literal}' AS shard;
         INSERT OR IGNORE INTO main.pda_registry (pda, program_id, seed_count, seed_bytes)
         SELECT pda, program_id, seed_count, seed_bytes FROM shard.pda_registry;
         DETACH DATABASE shard;"
    );
    dst.execute_batch(&sql)?;
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

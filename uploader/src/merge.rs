use eyre::{Result, WrapErr, eyre};
use log::{info, warn};
use rayon::prelude::*;
use std::{
    collections::HashSet,
    convert::TryInto,
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, RwLock,
        atomic::{self, AtomicUsize},
    },
    time::{Duration, SystemTime},
};

use solana_address::Address;

use crate::types::PdaSqlite;

pub fn merge(path: PathBuf, dedup_hashset_path: PathBuf) -> Result<(Vec<PdaSqlite>, Vec<PathBuf>)> {
    let mut dedup_hashset: HashSet<Address> = if dedup_hashset_path.exists() {
        let dedup_hashset = File::open(&dedup_hashset_path)?;
        let dedup_hashset = BufReader::new(dedup_hashset);
        bincode::deserialize_from(dedup_hashset).unwrap_or_default()
    } else {
        HashSet::new()
    };

    let blob_files = collect_blob_files(&path)?;
    let sqlite_files = collect_sqlite_files(&path)?;

    info!(
        "Discovered {} blob file(s) and {} sqlite file(s) in {}",
        blob_files.len(),
        sqlite_files.len(),
        path.display()
    );

    let total_sources = blob_files.len() + sqlite_files.len();
    let entries: Arc<RwLock<Vec<PdaSqlite>>> = Arc::new(RwLock::new(Vec::new()));
    let processed = AtomicUsize::new(0);

    if total_sources > 0 {
        info!("Starting deserialization of {total_sources} files");
        process_paths(
            "blob",
            &blob_files,
            &entries,
            &processed,
            total_sources,
            from_blob,
        )?;

        process_paths(
            "sqlite",
            &sqlite_files,
            &entries,
            &processed,
            total_sources,
            from_sqlite,
        )?;
    } else {
        info!("No PDA sources found under {}", path.display());
    }

    let mut entries = Arc::try_unwrap(entries)
        .map_err(|_| eyre!("failed to unwrap entries lock"))?
        .into_inner()
        .map_err(eyre::Report::from)?;

    let initial_count = entries.len();
    entries.sort_by_key(|entry| entry.pda);
    entries.dedup_by_key(|entry| entry.pda);
    let after_vec_dedup = entries.len();
    let vec_deduped = initial_count.saturating_sub(after_vec_dedup);

    entries.retain(|entry| !dedup_hashset.contains(&entry.pda));
    let after_hashset_dedup = entries.len();
    let hashset_deduped = after_vec_dedup.saturating_sub(after_hashset_dedup);

    info!(
        "Deduplication stats: {vec_deduped} deduped from vec, {hashset_deduped} deduped from hashset, {after_hashset_dedup} new entries"
    );

    dedup_hashset.extend(entries.iter().map(|entry| entry.pda));

    let temp_path = dedup_hashset_path.with_extension("tmp");
    let mut writer = BufWriter::new(File::create(&temp_path)?);
    bincode::serialize_into(&mut writer, &dedup_hashset)?;
    writer.flush()?;
    writer.get_mut().sync_all()?;

    match std::fs::rename(&temp_path, &dedup_hashset_path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            std::fs::remove_file(&dedup_hashset_path)?;
            std::fs::rename(&temp_path, &dedup_hashset_path)?;
        }
        Err(err) => {
            std::fs::remove_file(&temp_path).ok();
            return Err(eyre!(
                "failed to replace dedup hashset at {}: {err}",
                dedup_hashset_path.display()
            ));
        }
    }

    Ok((entries, blob_files))
}

fn process_paths(
    label: &'static str,
    paths: &[PathBuf],
    entries: &Arc<RwLock<Vec<PdaSqlite>>>,
    processed_count: &AtomicUsize,
    total_sources: usize,
    parser: fn(&Path) -> Result<Vec<PdaSqlite>>,
) -> Result<()> {
    paths.par_iter().try_for_each(|path| -> Result<()> {
        let parsed = parser(path.as_path())
            .wrap_err_with(|| format!("failed to parse {label} file {}", path.display()))?;

        let current_len = {
            let mut guard = entries
                .write()
                .map_err(|err| eyre!("entries lock poisoned: {err}"))?;
            guard.extend(parsed);
            guard.len()
        };

        let processed = processed_count.fetch_add(1, atomic::Ordering::Relaxed) + 1;
        info!(
            "Finished processing {label} file ({processed}/{total_sources}) {current_len} entries so far from {}",
            path.display()
        );

        Ok(())
    })
}

fn collect_blob_files(root: &Path) -> Result<Vec<PathBuf>> {
    let now = SystemTime::now();
    let mut files = Vec::new();

    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let Some(filename_os) = path.file_name() else {
            warn!(
                "Skipping path without filename while scanning {}",
                path.display()
            );
            continue;
        };

        let Some(filename) = filename_os.to_str() else {
            warn!(
                "Skipping non-UTF-8 filename while scanning {}",
                path.display()
            );
            continue;
        };

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

fn collect_sqlite_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let Some(extension) = path.extension().and_then(|ext| ext.to_str()) else {
            continue;
        };

        if extension == "sqlite" {
            files.push(path);
        }
    }

    Ok(files)
}

fn from_blob(path: &Path) -> Result<Vec<PdaSqlite>> {
    let file = File::open(path)
        .wrap_err_with(|| format!("failed to open blob file {}", path.display()))?;
    let reader = BufReader::new(file);
    bincode::deserialize_from(reader)
        .map_err(|err| eyre!("failed to deserialize blob file {}: {err}", path.display()))
}

fn from_sqlite(path: &Path) -> Result<Vec<PdaSqlite>> {
    let conn = rusqlite::Connection::open(path)
        .wrap_err_with(|| format!("failed to open sqlite file {}", path.display()))?;
    let mut stmt = conn
        .prepare("SELECT pda, program_id, seed_bytes FROM pda_registry")
        .wrap_err_with(|| format!("failed to prepare statement for {}", path.display()))?;

    let mut rows = stmt
        .query([])
        .wrap_err_with(|| format!("failed to query sqlite file {}", path.display()))?;

    let mut entries = Vec::new();
    while let Some(row) = rows
        .next()
        .wrap_err_with(|| format!("failed to read row in {}", path.display()))?
    {
        let pda_bytes: Vec<u8> = row.get(0)?;
        let program_id_bytes: Vec<u8> = row.get(1)?;
        let seed_bytes: Vec<u8> = row.get(2)?;

        let seeds_raw: Vec<u8> = bincode::deserialize(&seed_bytes).map_err(|err| {
            eyre!(
                "failed to deserialize seeds blob in {}: {err}",
                path.display()
            )
        })?;

        #[allow(unused)]
        fn encode_seeds_for_storage(seeds: &[Vec<u8>]) -> Vec<u8> {
            let total_seed_bytes = seeds.iter().map(|seed| seed.len()).sum::<usize>();
            let mut encoded = Vec::with_capacity(
                total_seed_bytes + (seeds.len() + 1) * std::mem::size_of::<u32>(),
            );
            encoded.extend_from_slice(&(seeds.len() as u32).to_le_bytes());
            for seed in seeds {
                encoded.extend_from_slice(&(seed.len() as u32).to_le_bytes());
                encoded.extend_from_slice(seed);
            }
            encoded
        }

        fn decode_seeds_from_storage(seeds_raw: Vec<u8>) -> Vec<Vec<u8>> {
            let mut cursor = 0;
            let mut seeds = Vec::new();

            // Read the number of seeds
            if seeds_raw.len() < 4 {
                return seeds; // Empty or invalid data
            }

            let num_seeds = u32::from_le_bytes([
                seeds_raw[cursor],
                seeds_raw[cursor + 1],
                seeds_raw[cursor + 2],
                seeds_raw[cursor + 3],
            ]) as usize;
            cursor += 4;

            // Read each seed
            for _ in 0..num_seeds {
                if cursor + 4 > seeds_raw.len() {
                    break; // Not enough data for seed length
                }

                let seed_len = u32::from_le_bytes([
                    seeds_raw[cursor],
                    seeds_raw[cursor + 1],
                    seeds_raw[cursor + 2],
                    seeds_raw[cursor + 3],
                ]) as usize;
                cursor += 4;

                if cursor + seed_len > seeds_raw.len() {
                    break; // Not enough data for seed content
                }

                let seed = seeds_raw[cursor..cursor + seed_len].to_vec();
                seeds.push(seed);
                cursor += seed_len;
            }

            seeds
        }

        let seeds = decode_seeds_from_storage(seeds_raw);

        entries.push(PdaSqlite {
            pda: decode_address(pda_bytes, "pda", path)?,
            program_id: decode_address(program_id_bytes, "program_id", path)?,
            seeds,
        });
    }

    Ok(entries)
}

fn decode_address(bytes: Vec<u8>, field: &str, path: &Path) -> Result<Address> {
    let array: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        eyre!(
            "expected 32 bytes for {field} in sqlite file {}, got {}",
            path.display(),
            bytes.len()
        )
    })?;

    Ok(Address::new_from_array(array))
}

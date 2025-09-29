use eyre::{Result, WrapErr, eyre};
use log::{info, warn};
use rayon::prelude::*;
use std::{
    collections::HashSet,
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::PathBuf,
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

    let files: Vec<PathBuf> = all_valid_files(&path)?;
    info!("Found {} files in {}", files.len(), path.display());

    let rw_entries: Arc<RwLock<Vec<PdaSqlite>>> = Arc::new(RwLock::new(Vec::new()));
    let num_files_dealt = Arc::new(AtomicUsize::new(0));
    let total_files = files.len();

    info!("Starting deserialization of {total_files} files");

    files.par_iter().try_for_each(|file| -> Result<()> {
        let pda_sqlite = deserialize_pda_sqlite(file)
            .wrap_err_with(|| format!("failed to deserialize file {}", file.display()))?;

        let current_len = {
            let mut entries = rw_entries
                .write()
                .map_err(|err| eyre!("rw_entries poisoned: {err}"))?;
            entries.extend(pda_sqlite);
            entries.len()
        };

        let processed = num_files_dealt.fetch_add(1, atomic::Ordering::Relaxed) + 1;
        info!(
            "Finished deserializing file ({processed}/{total_files}) {} entries so far",
            current_len
        );

        Ok(())
    })?;

    // deduplicate entries
    {
        let mut entries = rw_entries
            .write()
            .map_err(|err| eyre!("rw_entries poisoned: {err}"))?;
        let initial_count = entries.len();

        entries.sort_by_key(|entry| entry.pda);
        entries.dedup_by_key(|entry| entry.pda);
        let after_vec_dedup = entries.len();
        let vec_deduped = initial_count - after_vec_dedup;

        entries.retain(|entry| !dedup_hashset.contains(&entry.pda));
        let after_hashset_dedup = entries.len();
        let hashset_deduped = after_vec_dedup - after_hashset_dedup;

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
    }

    let entries = Arc::try_unwrap(rw_entries)
        .map_err(|_| eyre::eyre!("Failed to unwrap rw_entries"))?
        .into_inner()
        .map_err(eyre::Report::from)?;

    Ok((entries, files))
}

fn all_valid_files(root: &PathBuf) -> Result<Vec<PathBuf>> {
    let now = SystemTime::now();
    // for all file that starts with "pda_collector_" and ends with ".blob"
    // and age > 5 seconds
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

fn deserialize_pda_sqlite(path: &PathBuf) -> Result<Vec<PdaSqlite>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    bincode::deserialize_from(reader).map_err(eyre::Report::from)
}

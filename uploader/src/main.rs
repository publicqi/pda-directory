use ::cloudflare::framework::auth::Credentials;
use clap::Parser;
use log::info;

use crate::{
    cloudflare::{get_kv, new_client, put_kv, upload_to_d1},
    types::Args,
};

mod cloudflare;
mod merge;
mod types;

const NAMESPACE_ID: &str = "05dc24c1e32e433ba403340ffcb21fb2";
const ACTIVE_DB_KEY: &str = "ACTIVE_DB";

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    let api_token = args.token.clone();

    let client = new_client(Credentials::UserAuthToken {
        token: api_token.clone(),
    })
    .expect("failed to create client");
    let active_db = get_kv(
        client.clone(),
        &args.account_id,
        NAMESPACE_ID,
        ACTIVE_DB_KEY,
    )
    .await
    .expect("failed to get current db")
    .expect("no current db");

    info!("Current production db: {active_db}");

    // merge
    let (entries, files, mut dedup_hashset) =
        merge::merge(args.path.clone(), args.dedup_hashset_file.clone()).unwrap();
    info!(
        "Merged {} files into {} new entries",
        files.len(),
        entries.len()
    );

    if let (Some(blue_db_id), Some(green_db_id)) =
        (args.blue_db_id.as_deref(), args.green_db_id.as_deref())
    {
        let (inactive_db_id, new_active_label, secondary_db_id) = match active_db.as_str() {
            "blue" => (green_db_id, "green", blue_db_id),
            "green" => (blue_db_id, "blue", green_db_id),
            other => panic!("unexpected active db: {other}"),
        };

        const CHUNK_SIZE: usize = 1000;
        let total_entries = entries.len();
        let num_chunks = total_entries.div_ceil(CHUNK_SIZE);

        // Step 1: Upload to inactive database in chunks
        info!(
            "Step 1: Uploading {total_entries} entries to inactive database {inactive_db_id} in {num_chunks} chunk(s) of up to {CHUNK_SIZE} entries"
        );

        for (chunk_idx, chunk) in entries.chunks(CHUNK_SIZE).enumerate() {
            let chunk_num = chunk_idx + 1;
            info!(
                "Uploading chunk {}/{} to inactive database: {} entries",
                chunk_num,
                num_chunks,
                chunk.len()
            );

            upload_to_d1(&api_token, &args.account_id, inactive_db_id, chunk)
                .await
                .expect("failed to upload chunk to inactive D1 database");

            info!("Successfully uploaded chunk {chunk_num}/{num_chunks} to inactive database");
        }

        // Step 2: Toggle the active database
        info!("Step 2: Toggling active database to {new_active_label}");
        put_kv(
            client.clone(),
            &args.account_id,
            NAMESPACE_ID,
            ACTIVE_DB_KEY,
            new_active_label,
        )
        .await
        .expect("failed to put kv");
        info!("Database toggle complete");

        // Step 3: Upload to secondary database in chunks
        info!(
            "Step 3: Uploading {total_entries} entries to secondary database {secondary_db_id} in {num_chunks} chunk(s)"
        );

        for (chunk_idx, chunk) in entries.chunks(CHUNK_SIZE).enumerate() {
            let chunk_num = chunk_idx + 1;
            info!(
                "Uploading chunk {}/{} to secondary database: {} entries",
                chunk_num,
                num_chunks,
                chunk.len()
            );

            upload_to_d1(&api_token, &args.account_id, secondary_db_id, chunk)
                .await
                .expect("failed to upload chunk to secondary D1 database");

            info!("Successfully uploaded chunk {chunk_num}/{num_chunks} to secondary database");
        }

        // Step 4: Update and save dedup hashset to disk only after all uploads succeed
        info!("Step 4: Updating and saving dedup hashset to disk");
        dedup_hashset.extend(entries.iter().map(|entry| entry.pda));
        info!(
            "Extended dedup hashset with {} new entries (now contains {} total)",
            entries.len(),
            dedup_hashset.len()
        );
        merge::save_dedup_hashset(&dedup_hashset, &args.dedup_hashset_file)
            .expect("failed to save dedup hashset");

        info!("All operations completed successfully!");
    } else {
        info!("Skipping D1 uploads because --blue-db-id and --green-db-id were not provided");
        // Still save the hashset even when skipping uploads (for testing)
        merge::save_dedup_hashset(&dedup_hashset, &args.dedup_hashset_file)
            .expect("failed to save dedup hashset");
    }

    // todo: update telegram bot

    // remove old files
    // for file in files {
    //     if let Err(err) = std::fs::remove_file(&file) {
    //         warn!("Failed to remove source blob {}: {err}", file.display());
    //     }
    // }
}

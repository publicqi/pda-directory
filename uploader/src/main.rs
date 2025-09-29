use ::cloudflare::framework::auth::Credentials;
use clap::Parser;
use log::{info, warn};

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
    let (entries, files) = merge::merge(args.path, args.dedup_hashset_file).unwrap();
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

        upload_to_d1(&api_token, &args.account_id, inactive_db_id, &entries)
            .await
            .expect("failed to upload to inactive D1 database");

        // toggle database
        put_kv(
            client.clone(),
            &args.account_id,
            NAMESPACE_ID,
            ACTIVE_DB_KEY,
            new_active_label,
        )
        .await
        .expect("failed to put kv");

        upload_to_d1(&api_token, &args.account_id, secondary_db_id, &entries)
            .await
            .expect("failed to upload to active D1 database");
    } else {
        info!("Skipping D1 uploads because --blue-db-id and --green-db-id were not provided");
    }

    // update telegram bot

    // remove old files
    for file in files {
        if let Err(err) = std::fs::remove_file(&file) {
            warn!("Failed to remove source blob {}: {err}", file.display());
        }
    }
}

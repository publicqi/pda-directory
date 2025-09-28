use std::{sync::Arc, time::Duration};

use cloudflare::{
    endpoints::workerskv::{
        read_key::ReadKey,
        write_key::{WriteKey, WriteKeyBody, WriteKeyParams},
    },
    framework::{
        Environment,
        auth::Credentials,
        client::{ClientConfig, async_api::Client},
    },
};
use eyre::{Result, WrapErr, eyre};
use log::{debug, info};
use md5::compute as md5_compute;
use reqwest::{
    Client as HttpClient,
    header::{AUTHORIZATION, CONTENT_TYPE},
};
use serde::Deserialize;
use serde_json::json;
use tokio::time::sleep;

use crate::types::PdaSqlite;

pub fn new_client(credentials: Credentials) -> Result<Arc<Client>> {
    Ok(Arc::new(Client::new(
        credentials,
        ClientConfig::default(),
        Environment::Production,
    )?))
}

pub async fn get_kv(
    client: Arc<Client>,
    account_identifier: &str,
    namespace_identifier: &str,
    key: &str,
) -> Result<Option<String>> {
    Ok(Some(String::from_utf8(
        client
            .request(&ReadKey {
                account_identifier,
                namespace_identifier,
                key,
            })
            .await
            .map_err(|e| eyre!("Failed to get kv: {e}"))?,
    )?))
}

pub async fn put_kv(
    client: Arc<Client>,
    account_identifier: &str,
    namespace_identifier: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    client
        .request(&WriteKey {
            account_identifier,
            namespace_identifier,
            key,
            params: WriteKeyParams::default(),
            body: WriteKeyBody::Value(value.as_bytes().to_vec()),
        })
        .await
        .map_err(|e| eyre!("Failed to put kv: {e}"))?;

    Ok(())
}

pub async fn upload_to_d1(
    api_token: &str,
    account_identifier: &str,
    database_identifier: &str,
    entries: &[PdaSqlite],
) -> Result<()> {
    if entries.is_empty() {
        info!("Skip D1 upload for database {database_identifier}: no new entries");
        return Ok(());
    }

    let script = match build_insert_script(entries)? {
        Some(script) => script,
        None => {
            info!("Skip D1 upload for database {database_identifier}: nothing to insert");
            return Ok(());
        }
    };

    let payload_size_bytes = script.len();
    let checksum = format!("{:x}", md5_compute(script.as_bytes()));
    info!(
        "Uploading {} entries ({} bytes) to D1 database {database_identifier}",
        entries.len(),
        payload_size_bytes
    );

    let sql_payload = script.into_bytes();
    let http = HttpClient::builder()
        .user_agent("pda-directory-uploader/1.0")
        .build()
        .wrap_err("failed to construct HTTP client")?;

    let import_url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{account_identifier}/d1/database/{database_identifier}/import"
    );

    let init_result: InitUploadResult = unpack_response(
        http.post(&import_url)
            .header(CONTENT_TYPE, "application/json")
            .header(AUTHORIZATION, format!("Bearer {api_token}"))
            .json(&json!({
                "action": "init",
                "etag": checksum,
            }))
            .send()
            .await
            .wrap_err("failed to send D1 init request")?
            .error_for_status()
            .wrap_err("D1 init request returned error status")?
            .json::<CloudflareResponse<InitUploadResult>>()
            .await
            .wrap_err("failed to deserialize D1 init response")?,
    )?;

    debug!(
        "Received upload URL {} and filename {}",
        init_result.upload_url, init_result.filename
    );

    let upload_response = http
        .put(&init_result.upload_url)
        .body(sql_payload)
        .send()
        .await
        .wrap_err("failed to upload SQL payload to R2")?
        .error_for_status()
        .wrap_err("D1 upload to R2 returned error status")?;

    let response_etag = upload_response
        .headers()
        .get("ETag")
        .and_then(|value| value.to_str().ok())
        .map(|etag| etag.trim_matches('"').to_owned())
        .ok_or_else(|| eyre!("missing ETag header in R2 upload response"))?;

    if response_etag != checksum {
        return Err(eyre!(
            "ETag mismatch: expected {checksum}, got {response_etag}"
        ));
    }

    debug!("Verified upload etag {response_etag}");

    let ingest_result: IngestResult = unpack_response(
        http.post(&import_url)
            .header(CONTENT_TYPE, "application/json")
            .header(AUTHORIZATION, format!("Bearer {api_token}"))
            .json(&json!({
                "action": "ingest",
                "etag": checksum,
                "filename": init_result.filename,
            }))
            .send()
            .await
            .wrap_err("failed to send D1 ingest request")?
            .error_for_status()
            .wrap_err("D1 ingest request returned error status")?
            .json::<CloudflareResponse<IngestResult>>()
            .await
            .wrap_err("failed to deserialize D1 ingest response")?,
    )?;

    let mut bookmark = ingest_result.at_bookmark;
    let mut attempts = 0usize;
    const MAX_ATTEMPTS: usize = 300;

    loop {
        attempts += 1;
        let current_bookmark = bookmark.clone();
        let poll_result: PollResult = unpack_response(
            http.post(&import_url)
                .header(CONTENT_TYPE, "application/json")
                .header(AUTHORIZATION, format!("Bearer {api_token}"))
                .json(&json!({
                    "action": "poll",
                    "current_bookmark": current_bookmark,
                }))
                .send()
                .await
                .wrap_err("failed to send D1 poll request")?
                .error_for_status()
                .wrap_err("D1 poll request returned error status")?
                .json::<CloudflareResponse<PollResult>>()
                .await
                .wrap_err("failed to deserialize D1 poll response")?,
        )?;

        debug!(
            "Poll attempt {attempts}: success={}, error={:?}, status={:?}",
            poll_result.success, poll_result.error, poll_result.status
        );

        if poll_result.success {
            info!("D1 import completed for database {database_identifier}");
            break;
        }

        if let Some(err) = poll_result.error.as_deref() {
            if err == "Not currently importing anything." {
                info!("D1 import already complete for database {database_identifier}");
                break;
            }
        }

        if let Some(next) = poll_result.at_bookmark {
            bookmark = Some(next);
        }

        if attempts >= MAX_ATTEMPTS {
            return Err(eyre!(
                "Timed out after {MAX_ATTEMPTS} attempts while polling D1 import"
            ));
        }

        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

fn build_insert_script(entries: &[PdaSqlite]) -> Result<Option<String>> {
    if entries.is_empty() {
        return Ok(None);
    }

    const CHUNK_SIZE: usize = 500;
    let mut script = String::with_capacity(entries.len() * 256);
    script.push_str("BEGIN TRANSACTION;\n");

    for chunk in entries.chunks(CHUNK_SIZE) {
        script.push_str(
            "INSERT OR IGNORE INTO pda_registry (pda, program_id, seed_count, seed_bytes) VALUES\n",
        );

        for (index, entry) in chunk.iter().enumerate() {
            let pda_blob = to_blob_literal(entry.pda.as_ref());
            let program_blob = to_blob_literal(entry.program_id.as_ref());
            let seed_bytes =
                bincode::serialize(&entry.seeds).wrap_err("failed to serialize seeds")?;
            let seed_blob = to_blob_literal(&seed_bytes);

            script.push_str(&format!(
                "({pda}, {program}, {seed_count}, {seed})",
                pda = pda_blob,
                program = program_blob,
                seed_count = entry.seeds.len(),
                seed = seed_blob
            ));

            if index + 1 == chunk.len() {
                script.push_str(";\n");
            } else {
                script.push_str(",\n");
            }
        }
    }

    script.push_str("COMMIT;\n");

    Ok(Some(script))
}

fn to_blob_literal(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "X''".to_owned();
    }

    let mut literal = String::with_capacity(bytes.len() * 2 + 3);
    literal.push_str("X'");
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut literal, "{:02X}", byte).expect("writing to string cannot fail");
    }
    literal.push_str("'");
    literal
}

fn unpack_response<T>(response: CloudflareResponse<T>) -> Result<T> {
    if response.success {
        Ok(response.result)
    } else {
        let mut message = response
            .errors
            .unwrap_or_default()
            .into_iter()
            .map(|err| match (err.code, err.message) {
                (Some(code), msg) => format!("{code}: {msg}"),
                (None, msg) => msg,
            })
            .collect::<Vec<_>>()
            .join(", ");

        if message.is_empty() {
            message = "unknown error".to_owned();
        }

        Err(eyre!("Cloudflare API error: {message}"))
    }
}

#[derive(Debug, Deserialize)]
struct CloudflareResponse<T> {
    result: T,
    success: bool,
    #[serde(default)]
    errors: Option<Vec<CloudflareApiError>>,
}

#[derive(Debug, Deserialize)]
struct CloudflareApiError {
    code: Option<u64>,
    message: String,
}

#[derive(Debug, Deserialize)]
struct InitUploadResult {
    upload_url: String,
    filename: String,
}

#[derive(Debug, Deserialize)]
struct IngestResult {
    #[serde(default)]
    at_bookmark: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PollResult {
    success: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    at_bookmark: Option<String>,
}

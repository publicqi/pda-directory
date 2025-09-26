#!/bin/bash

# This script takes a local SQLite file, splits it into chunks,
# and uploads them to Cloudflare D1.

# Exit immediately if a command exits with a non-zero status.
set -e

# Check for required arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <local_sqlite_path> <num_rows>"
    echo "Example: $0 my_database.sqlite 100000"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "Error: jq is not installed. Please install it to continue."
    exit 1
fi

# --- Configuration ---
SQLITE_FILE="$1"
NUM_ROWS="$2"
CHUNKS_DIR="d1_chunks_local"
D1_DATABASE="pda-directory"
MAX_RETRIES=3
RETRY_DELAY_SECONDS=5
CHUNK_SIZE=100000 # Internal chunk size for splitting

if [ ! -f "$SQLITE_FILE" ]; then
    echo "Error: SQLite file not found at $SQLITE_FILE"
    exit 1
fi

# --- Main logic ---
echo "Processing SQLite file: $SQLITE_FILE"

# Clean up previous chunks if they exist
if [ -d "$CHUNKS_DIR" ]; then
    echo "Removing existing chunks directory: $CHUNKS_DIR"
    rm -r "$CHUNKS_DIR"
fi

mkdir -p "$CHUNKS_DIR"

echo "Dumping schema and first $NUM_ROWS rows, and creating chunks of $CHUNK_SIZE lines each..."
# The sed commands ensure that existing records on the remote database are ignored
# and that tables are created only if they don't exist.
# We also grep -v to remove transaction statements that D1 does not support.
DUMP_OUTPUT=$(sqlite3 "$SQLITE_FILE" .dump)
SCHEMA_PART=$(echo "$DUMP_OUTPUT" | grep -v '^INSERT INTO')
INSERTS_PART=$(echo "$DUMP_OUTPUT" | grep '^INSERT INTO' | head -n "$NUM_ROWS")

(
    echo "$SCHEMA_PART";
    echo "$INSERTS_PART";
) | \
  grep -vE '^BEGIN TRANSACTION;|^COMMIT;' | \
  sed 's/^INSERT INTO/INSERT OR IGNORE INTO/' | \
  sed 's/^CREATE TABLE/CREATE TABLE IF NOT EXISTS/' | \
  split -l "$CHUNK_SIZE" - "$CHUNKS_DIR/chunk_"

# Add .sql suffix for macOS compatibility
for f in "$CHUNKS_DIR"/chunk_*; do
  if [ -f "$f" ]; then
    mv -- "$f" "$f.sql"
  fi
done

echo "Database successfully split into chunks in '$CHUNKS_DIR/'"

# Upload chunks to D1 with retry
echo "Uploading chunks to D1 database '$D1_DATABASE'..."
upload_failed=false
total_rows_written=0

for file in "$CHUNKS_DIR"/*.sql; do
  if [ ! -f "$file" ]; then
    continue
  fi

  echo "Uploading '$file'..."
  retries=$MAX_RETRIES
  upload_output=""
  until upload_output=$(npx wrangler d1 execute "$D1_DATABASE" --remote --file="$file" -y --json)
  do
    retries=$((retries-1))
    if [ $retries -eq 0 ]; then
      echo "ERROR: Failed to upload '$file' after $MAX_RETRIES attempts."
      upload_failed=true
      break
    fi
    echo "WARNING: Upload of '$file' failed. Retrying in $RETRY_DELAY_SECONDS seconds... ($retries retries left)"
    sleep $RETRY_DELAY_SECONDS
  done

  if [ "$upload_failed" = "true" ]; then
    break
  fi

  json_output=$(echo "$upload_output" | sed -n '/^\[/,$p')
  if [ -n "$json_output" ]; then
    rows_written=$(echo "$json_output" | jq '.[0].meta.rows_written // 0')
  else
    rows_written=0
  fi
  total_rows_written=$((total_rows_written + rows_written))

  echo "Successfully uploaded '$file'. Rows written: $rows_written"
done

# Cleanup local chunks
rm -r "$CHUNKS_DIR"
echo "Removed chunk directory: $CHUNKS_DIR"

if [ "$upload_failed" = "true" ]; then
    echo "ERROR: Upload failed. Aborting."
    exit 1
fi

echo "All chunks for $SQLITE_FILE uploaded successfully."
echo "Total rows written: $total_rows_written"
exit 0

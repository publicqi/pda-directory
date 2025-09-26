#!/bin/bash

# This script continuously monitors for SQLite files on remote server,
# downloads them, splits them into chunks, uploads to Cloudflare D1,
# and cleans up both local and remote files.

# Exit immediately if a command exits with a non-zero status.
set -e

# Load environment variables if .env file exists
if [ -f ".env" ]; then
    source .env
fi

# Check for required argument
if [ $# -eq 0 ]; then
    echo "Usage: $0 <remote_path>"
    echo "Example: $0 mega:~/pda/"
    exit 1
fi

# --- Configuration ---
REMOTE_PATH="$1"
LOCAL_DOWNLOAD_DIR="downloaded_sqlite"
CHUNKS_DIR="d1_chunks"
CHUNK_SIZE=100000
D1_DATABASE="pda-directory"
MAX_RETRIES=3
RETRY_DELAY_SECONDS=5
MONITOR_INTERVAL=600  # 10 minutes in seconds

# Validate required environment variables for Telegram
if [ -z "$TELEGRAM_BOT_TOKEN" ] || [ -z "$TELEGRAM_CHAT_ID" ]; then
    echo "Warning: TELEGRAM_BOT_TOKEN and/or TELEGRAM_CHAT_ID not set. Telegram notifications disabled."
    TELEGRAM_ENABLED=false
else
    TELEGRAM_ENABLED=true
fi

# --- Functions ---

# Function to send Telegram message
send_telegram() {
    local text="$1"
    
    if [ "$TELEGRAM_ENABLED" != "true" ]; then
        return 0
    fi
    
    # Escape special characters for JSON
    text=$(echo "$text" | sed 's/\\/\\\\/g; s/"/\\"/g; s/\t/\\t/g; s/\r/\\r/g; s/\n/\\n/g')
    
    # Build JSON safely with printf
    local payload
    payload=$(printf '{"chat_id": "%s", "text": "%s", "disable_notification": true}' \
                     "$TELEGRAM_CHAT_ID" "$text")
    
    curl -s --retry 5 --max-time 10 -X POST \
         -H "Content-Type: application/json" \
         -d "$payload" \
         "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" >/dev/null 2>&1 || {
        echo "Warning: Failed to send Telegram notification"
        return 1
    }
}

# Function to count entries in SQLite file
count_sqlite_entries() {
    local db_file="$1"
    local count=0
    
    # Get list of all tables and sum their row counts
    local tables
    tables=$(sqlite3 "$db_file" ".tables" 2>/dev/null | tr ' ' '\n' | grep -v '^$' || echo "")
    
    if [ -n "$tables" ]; then
        while IFS= read -r table; do
            if [ -n "$table" ]; then
                local table_count
                table_count=$(sqlite3 "$db_file" "SELECT COUNT(*) FROM \"$table\";" 2>/dev/null || echo "0")
                count=$((count + table_count))
            fi
        done <<< "$tables"
    fi
    
    echo "$count"
}

# Function to update KV store with timestamp
update_kv_timestamp() {
    local timestamp=$(date +%s)
    echo "Updating KV store with timestamp: $timestamp"
    
    if ! npx wrangler kv key put last_update_time "$timestamp" --binding PDA_LAST_UPDATE --remote; then
        echo "Warning: Failed to update KV store timestamp"
        return 1
    fi
    
    echo "Successfully updated KV store timestamp"
    return 0
}

# Function to process a single SQLite file
process_sqlite_file() {
    local db_file="$1"
    local filename=$(basename "$db_file")
    local entry_count=0
    
    echo "Processing SQLite file: $filename"
    
    # Count entries before processing
    echo "Counting entries in $filename..."
    entry_count=$(count_sqlite_entries "$db_file")
    echo "Found $entry_count total entries in $filename"
    
    # Skip processing if no entries
    if [ "$entry_count" -eq 0 ]; then
        echo "No entries found in $filename. Skipping processing."
        rm "$db_file"
        echo "Removed empty SQLite file: $filename"
        return 0
    fi
    
    # Clean up previous chunks if they exist
    if [ -d "$CHUNKS_DIR" ]; then
        echo "Removing existing chunks directory: $CHUNKS_DIR"
        rm -r "$CHUNKS_DIR" || {
            echo "ERROR: Failed to remove existing chunks directory"
            return 1
        }
    fi
    
    mkdir -p "$CHUNKS_DIR" || {
        echo "ERROR: Failed to create chunks directory"
        return 1
    }

    echo "Dumping database and creating chunks of $CHUNK_SIZE lines each..."
    # The sed command ensures that existing records on the remote database are ignored.
    # We also grep -v to remove transaction statements that D1 does not support.
    if ! sqlite3 "$db_file" .dump | \
      grep -vE '^BEGIN TRANSACTION;|^COMMIT;' | \
      sed 's/^INSERT INTO/INSERT OR IGNORE INTO/' | \
      sed 's/^CREATE TABLE/CREATE TABLE IF NOT EXISTS/' | \
      split -l "$CHUNK_SIZE" - "$CHUNKS_DIR/chunk_"; then
        echo "ERROR: Failed to dump and split database $filename"
        return 1
    fi

    # Add .sql suffix for macOS compatibility
    for f in "$CHUNKS_DIR"/chunk_*; do
      if [ -f "$f" ]; then
        mv -- "$f" "$f.sql" || {
          echo "ERROR: Failed to rename chunk file $f"
          return 1
        }
      fi
    done

    echo "Database successfully split into chunks in '$CHUNKS_DIR/'"

    # Upload chunks to D1 with retry
    echo "Uploading chunks to D1 database '$D1_DATABASE'..."
    local upload_failed=false

    for file in "$CHUNKS_DIR"/*.sql; do
      if [ ! -f "$file" ]; then
        continue
      fi
      
      echo "Uploading '$file'..."
      retries=$MAX_RETRIES
      until npx wrangler d1 execute "$D1_DATABASE" --remote --file="$file" -y
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
      
      echo "Successfully uploaded '$file'."
    done

    if [ "$upload_failed" = "true" ]; then
        echo "ERROR: Upload failed for $filename. Aborting processing."
        return 1
    fi

    echo "All chunks for $filename uploaded successfully."
    
    # Send Telegram notification
    local message="Database upload completed: $filename ($entry_count entries processed)"
    send_telegram "$message"
    
    # Update KV store timestamp
    update_kv_timestamp
    
    # Cleanup local chunks
    rm -r "$CHUNKS_DIR" || {
        echo "Warning: Failed to remove chunks directory"
    }
    echo "Removed chunk directory: $CHUNKS_DIR"
    
    # Remove local SQLite file
    rm "$db_file" || {
        echo "Warning: Failed to remove local SQLite file: $filename"
    }
    echo "Removed local SQLite file: $filename"
    
    return 0
}

# --- Main monitoring loop ---
echo "Starting continuous SQLite monitoring and processing..."
echo "Remote path: $REMOTE_PATH"
echo "Monitor interval: $MONITOR_INTERVAL seconds (10 minutes)"
if [ "$TELEGRAM_ENABLED" = "true" ]; then
    echo "Telegram notifications: ENABLED"
else
    echo "Telegram notifications: DISABLED"
fi
echo "Press Ctrl+C to stop"
echo ""

# Create local download directory
mkdir -p "$LOCAL_DOWNLOAD_DIR" || {
    echo "ERROR: Failed to create local download directory: $LOCAL_DOWNLOAD_DIR"
    exit 1
}

# Trap to handle graceful shutdown
trap 'echo ""; echo "Received interrupt signal. Shutting down gracefully..."; exit 0' INT TERM

while true; do
    echo "$(date): Checking for new SQLite files..."
    
    # Download SQLite files from remote using rsync
    # Using --remove-source-files to delete remote files after successful transfer
    # Using --include and --exclude to only sync .sqlite files
    downloaded_files=""
    if ! downloaded_files=$(rsync -av --remove-source-files \
        --include="*.sqlite" \
        --exclude="*" \
        "$REMOTE_PATH" "$LOCAL_DOWNLOAD_DIR/" 2>/dev/null | grep -E "\.sqlite$" || true); then
        echo "Warning: rsync command failed. Will retry in next cycle."
        sleep $MONITOR_INTERVAL
        continue
    fi
    
    if [ -z "$downloaded_files" ]; then
        echo "No new SQLite files found. Sleeping for $MONITOR_INTERVAL seconds..."
    else
        echo "Downloaded files:"
        echo "$downloaded_files"
        
        local processed_count=0
        local failed_count=0
        local total_entries=0
        
        # Process each downloaded SQLite file
        for sqlite_file in "$LOCAL_DOWNLOAD_DIR"/*.sqlite; do
            if [ -f "$sqlite_file" ]; then
                echo ""
                echo "=== Processing $(basename "$sqlite_file") ==="
                
                # Count entries before processing for summary
                local file_entries
                file_entries=$(count_sqlite_entries "$sqlite_file")
                
                if process_sqlite_file "$sqlite_file"; then
                    echo "Successfully processed $(basename "$sqlite_file")"
                    processed_count=$((processed_count + 1))
                    total_entries=$((total_entries + file_entries))
                else
                    echo "Failed to process $(basename "$sqlite_file")"
                    failed_count=$((failed_count + 1))
                    # Keep the file for manual inspection
                    echo "File kept at: $sqlite_file"
                    
                    # Send error notification
                    local error_msg="ERROR: Failed to process $(basename "$sqlite_file")"
                    send_telegram "$error_msg"
                fi
                echo "=== Finished processing $(basename "$sqlite_file") ==="
                echo ""
            fi
        done
        
        # Send summary notification if any files were processed successfully
        if [ $processed_count -gt 0 ]; then
            local summary_msg="Batch processing completed: $processed_count files processed, $total_entries total entries"
            if [ $failed_count -gt 0 ]; then
                summary_msg="$summary_msg, $failed_count failed"
            fi
            send_telegram "$summary_msg"
        fi
        
        echo "Batch summary: $processed_count processed, $failed_count failed, $total_entries total entries"
        echo "Finished processing all downloaded files. Sleeping for $MONITOR_INTERVAL seconds..."
    fi
    
    sleep $MONITOR_INTERVAL
done

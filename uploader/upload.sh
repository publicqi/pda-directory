#!/bin/bash

# This script continuously monitors for SQLite files on remote server,
# downloads them, splits them into chunks, uploads to Cloudflare D1,
# and cleans up both local and remote files.

# Exit immediately if a command exits with a non-zero status.
set -e

if ! command -v jq &> /dev/null; then
    echo "Error: jq is not installed. Please install it to continue."
    exit 1
fi

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

# Function to update KV store with timestamp
update_kv_timestamp() {
    local timestamp=$(date +%s)
    echo "Updating KV store with timestamp: $timestamp"
    
    if ! npx wrangler kv key put last_update_time "$timestamp" --binding PDA_METADATA --remote; then
        echo "Warning: Failed to update KV store timestamp"
        return 1
    fi
    
    echo "Successfully updated KV store timestamp"
    return 0
}

# Function to get total entries from KV store
get_kv_total_entries() {
    local total_entries
    total_entries=$(npx wrangler kv key get total_entries --binding PDA_METADATA --remote --text 2>/dev/null || echo "0")
    echo "$total_entries"
}

# Function to update KV store with total entries
update_kv_total_entries() {
    local new_total="$1"
    echo "Updating KV store with total entries: $new_total"
    
    if ! npx wrangler kv key put total_entries "$new_total" --binding PDA_METADATA --remote; then
        echo "Warning: Failed to update KV store total entries"
        return 1
    fi
    
    echo "Successfully updated KV store total entries"
    return 0
}

# Function to get last update time from KV store
get_kv_last_update_time() {
    local last_update
    last_update=$(npx wrangler kv key get last_update_time --binding PDA_METADATA --remote --text 2>/dev/null || echo "")
    echo "$last_update"
}

# Function to delete remote file after successful processing
delete_remote_file() {
    local filename="$1"
    local remote_file_path="${REMOTE_PATH}${filename}"
    
    echo "Attempting to delete remote file: $remote_file_path"
    
    # Use ssh to delete the remote file if it's an ssh connection
    if [[ "$REMOTE_PATH" == *":"* ]]; then
        # Extract hostname and path for SSH-based connections
        local ssh_host=$(echo "$REMOTE_PATH" | cut -d':' -f1)
        local remote_dir=$(echo "$REMOTE_PATH" | cut -d':' -f2-)
        local full_remote_path="${remote_dir}${filename}"
        
        if ssh "$ssh_host" "rm -f \"$full_remote_path\"" 2>/dev/null; then
            echo "Successfully deleted remote file: $remote_file_path"
            return 0
        else
            echo "Warning: Failed to delete remote file: $remote_file_path"
            return 1
        fi
    else
        # For local paths or other protocols, try direct rm
        if rm -f "$remote_file_path" 2>/dev/null; then
            echo "Successfully deleted remote file: $remote_file_path"
            return 0
        else
            echo "Warning: Failed to delete remote file: $remote_file_path"
            return 1
        fi
    fi
}

# Function to process a single SQLite file
process_sqlite_file() {
    local db_file="$1"
    local filename=$(basename "$db_file")
    
    echo "Processing SQLite file: $filename"
    
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
    local total_rows_written_for_file=0

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
      total_rows_written_for_file=$((total_rows_written_for_file + rows_written))
      
      echo "Successfully uploaded '$file'. Rows written: $rows_written"
    done

    if [ "$upload_failed" = "true" ]; then
        echo "ERROR: Upload failed for $filename. Aborting processing."
        return 1
    fi

    echo "All chunks for $filename uploaded successfully."
    
    # Update total entries in KV store
    local current_total
    current_total=$(get_kv_total_entries)
    local new_total=$((current_total + total_rows_written_for_file))
    update_kv_total_entries "$new_total"
    
    # Send Telegram notification
    local message="Database upload completed: $filename ($total_rows_written_for_file entries processed, total: $new_total)"
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
    
    # Delete remote file after successful processing
    if ! delete_remote_file "$filename"; then
        # Send notification about cleanup failure but don't fail the overall process
        local cleanup_error_msg="Warning: Failed to delete remote file after successful upload: $filename"
        send_telegram "$cleanup_error_msg"
        echo "$cleanup_error_msg"
    fi
    
    echo "$total_rows_written_for_file"
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

# Get and display startup information
echo ""
echo "Fetching startup information from KV store..."
current_total_entries=$(get_kv_total_entries)
last_update_time=$(get_kv_last_update_time)

echo "Current total entries in database: $current_total_entries"
if [ -n "$last_update_time" ] && [ "$last_update_time" != "0" ]; then
    # Convert timestamp to human-readable format
    last_update_formatted=$(date -r "$last_update_time" 2>/dev/null || echo "Invalid timestamp")
    echo "Last upload time: $last_update_formatted"
else
    echo "Last upload time: Never"
fi

# Send startup message to Telegram
if [ "$TELEGRAM_ENABLED" = "true" ]; then
    startup_msg="PDA Upload Monitor Started"
    startup_msg="$startup_msg\nTotal entries: $current_total_entries"
    if [ -n "$last_update_time" ] && [ "$last_update_time" != "0" ]; then
        startup_msg="$startup_msg\nLast upload: $last_update_formatted"
    else
        startup_msg="$startup_msg\nLast upload: Never"
    fi
    send_telegram "$startup_msg"
fi

echo ""
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
    # NOT using --remove-source-files to ensure files are only deleted after successful D1 upload
    # Using --include and --exclude to only sync .sqlite files
    downloaded_files=""
    if ! downloaded_files=$(rsync -av \
        --include="*.sqlite" \
        --exclude="*.shard*.sqlite" \
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
        
        processed_count=0
        failed_count=0
        total_entries=0
        
        # Process each downloaded SQLite file
        for sqlite_file in "$LOCAL_DOWNLOAD_DIR"/*.sqlite; do
            if [ -f "$sqlite_file" ]; then
                echo ""
                echo "=== Processing $(basename "$sqlite_file") ==="
                
                if process_output=$(process_sqlite_file "$sqlite_file"); then
                    file_entries=$(echo "$process_output" | tail -n1)
                    echo "Successfully processed $(basename "$sqlite_file")"
                    processed_count=$((processed_count + 1))
                    total_entries=$((total_entries + file_entries))
                else
                    echo "$process_output"
                    echo "Failed to process $(basename "$sqlite_file")"
                    failed_count=$((failed_count + 1))
                    # Keep the file for manual inspection
                    echo "File kept at: $sqlite_file"
                    
                    # Send error notification
                    error_msg="ERROR: Failed to process $(basename "$sqlite_file")"
                    send_telegram "$error_msg"
                fi
                echo "=== Finished processing $(basename "$sqlite_file") ==="
                echo ""
            fi
        done
        
        # Send summary notification if any files were processed successfully
        if [ $processed_count -gt 0 ]; then
            # Get updated total from KV store
            updated_total=$(get_kv_total_entries)
            summary_msg="Batch processing completed: $processed_count files processed, $total_entries entries added (total: $updated_total)"
            if [ $failed_count -gt 0 ]; then
                summary_msg="$summary_msg, $failed_count failed"
            fi
            send_telegram "$summary_msg"
        fi
        
        echo "Batch summary: $processed_count processed, $failed_count failed, $total_entries entries added"
        if [ $processed_count -gt 0 ]; then
            final_total=$(get_kv_total_entries)
            echo "Database now contains: $final_total total entries"
        fi
        echo "Finished processing all downloaded files. Sleeping for $MONITOR_INTERVAL seconds..."
    fi
    
    sleep $MONITOR_INTERVAL
done

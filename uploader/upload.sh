#!/usr/bin/env bash

# This script continuously monitors for SQLite files on a remote server,
# downloads them, splits them into chunks, uploads to Cloudflare D1,
# and cleans up both local and remote files.

set -Eeuo pipefail
shopt -s nullglob

# Global verbose tracing
# Timestamped trace with file:line and function
export PS4='[TRACE] $(date +"%Y-%m-%dT%H:%M:%S%z") ${BASH_SOURCE##*/}:${LINENO}${FUNCNAME:+ ${FUNCNAME}}: '
set -x

# --- Dependency Check ---
check_deps() {
    local missing_deps=()
    for dep in "$@"; do
        if ! command -v "$dep" &>/dev/null; then
            missing_deps+=("$dep")
        fi
    done

    if [ ${#missing_deps[@]} -gt 0 ]; then
        echo "Error: Missing required dependencies: ${missing_deps[*]}" >&2
        echo "Please install them to continue." >&2
        exit 1
    fi
}
check_deps jq sqlite3 rsync ssh curl npx split

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
MONITOR_INTERVAL=60  # 1 minute in seconds

# Validate required environment variables for Telegram
if [ -z "$TELEGRAM_BOT_TOKEN" ] || [ -z "$TELEGRAM_CHAT_ID" ]; then
    exit 1
fi

# --- Functions ---

# Function to send Telegram message
send_telegram() {
    local text="$1"
    
    # Escape special characters for JSON
    text=$(echo "$text" | sed 's/\\/\\\\/g; s/"/\\"/g; s/\t/\\t/g; s/\r/\\r/g; s/\n/\\n/g')
    
    # Build JSON safely with printf
    local payload
    payload=$(printf '{"chat_id": "%s", "text": "%s", "disable_notification": true}' \
                     "$TELEGRAM_CHAT_ID" "$text")
    
    # Redacted token for logging
    local token_redacted="${TELEGRAM_BOT_TOKEN:0:6}******"
    echo "[cmd] curl -s --retry 5 --max-time 10 -X POST -H 'Content-Type: application/json' -d '<payload>' 'https://api.telegram.org/bot${token_redacted}/sendMessage'" >&2
    curl -s --retry 5 --max-time 10 -X POST \
         -H "Content-Type: application/json" \
         -d "$payload" \
         "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" >/dev/null 2>&1 || {
        echo "Warning: Failed to send Telegram notification"
        return 1
    }
}

# Function to delete remote file after successful processing
delete_remote_file() {
    local filename="$1"
    
    # Extract hostname and path for SSH-based connections
    local ssh_host
    ssh_host=$(echo "$REMOTE_PATH" | cut -d':' -f1)
    local remote_dir
    remote_dir=$(echo "$REMOTE_PATH" | cut -d':' -f2-)
    
    # Ensure remote_dir ends with a slash for safe concatenation
    local full_remote_path="${remote_dir%/}/$filename"

    echo "[cmd] ssh '$ssh_host' rm -f '$full_remote_path'" >&2
    if ssh "$ssh_host" "rm -f \"$full_remote_path\""; then
        echo "Successfully deleted remote file: $filename" >&2
        return 0
    else
        send_telegram "Warning: Failed to delete remote file: ${REMOTE_PATH}${filename}"
        return 1
    fi
}

get_total_entries() {
    local total_entries_output stderr_out exit_code start end duration stdout_file stderr_file
    local attempts=${MAX_RETRIES:-3}
    local delay=${RETRY_DELAY_SECONDS:-5}
    local timeout_sec=${D1_CMD_TIMEOUT_SECONDS:-20}
    local -a cmd=(npx wrangler d1 execute "$D1_DATABASE" --remote --command "SELECT n FROM _table_counts WHERE name = 'pda_registry';" --json)
    local -a timeout_cmd
    if command -v gtimeout >/dev/null 2>&1; then timeout_cmd=(gtimeout "$timeout_sec");
    elif command -v timeout >/dev/null 2>&1; then timeout_cmd=(timeout "$timeout_sec");
    else timeout_cmd=(); fi
    while [ $attempts -gt 0 ]; do
        echo "[d1] get_total_entries attempt $((MAX_RETRIES-attempts+1))/${MAX_RETRIES:-3} (timeout: ${timeout_cmd:+$timeout_sec s})" >&2
        echo "[cmd] ${timeout_cmd:+${timeout_cmd[*]} }${cmd[*]}" >&2
        start=$(date +%s)
        stdout_file=$(mktemp)
        stderr_file=$(mktemp)
        local -a run_cmd
        if [ ${#timeout_cmd[@]} -gt 0 ]; then
            run_cmd=("${timeout_cmd[@]}" "${cmd[@]}")
        else
            run_cmd=("${cmd[@]}")
        fi
        if "${run_cmd[@]}" >"$stdout_file" 2>"$stderr_file"; then
            exit_code=0
        else
            exit_code=$?
        fi
        total_entries_output=$(<"$stdout_file")
        stderr_out=$(<"$stderr_file")
        rm -f "$stdout_file" "$stderr_file"
        end=$(date +%s)
        duration=$((end-start))
        if [ "$exit_code" -eq 0 ]; then
            local total_entries
            total_entries=$(echo "$total_entries_output" | jq '.[0].results[0].n')
            echo "[d1] get_total_entries ok in ${duration}s -> ${total_entries}" >&2
            if [ -n "$total_entries" ] && [ "$total_entries" != "null" ]; then
                echo "$total_entries"
                return 0
            fi
        else
            echo "[d1] get_total_entries failed (exit $exit_code, ${duration}s). stderr: $stderr_out" >&2
        fi
        attempts=$((attempts-1))
        if [ $attempts -gt 0 ]; then
            echo "[retry] get_total_entries retrying in $delay s ($attempts left)" >&2
            sleep "$delay"
        fi
    done
    send_telegram "ERROR: Failed to query total entries from D1 after retries."
    echo "0"
    return 1
}

get_last_update_time() {
    local last_update_time_output stderr_out exit_code start end duration stdout_file stderr_file
    local attempts=${MAX_RETRIES:-3}
    local delay=${RETRY_DELAY_SECONDS:-5}
    local timeout_sec=${D1_CMD_TIMEOUT_SECONDS:-20}
    local -a cmd=(npx wrangler d1 execute "$D1_DATABASE" --remote --command "SELECT last_insert_ts FROM _table_counts WHERE name = 'pda_registry';" --json)
    local -a timeout_cmd
    if command -v gtimeout >/dev/null 2>&1; then timeout_cmd=(gtimeout "$timeout_sec");
    elif command -v timeout >/dev/null 2>&1; then timeout_cmd=(timeout "$timeout_sec");
    else timeout_cmd=(); fi
    while [ $attempts -gt 0 ]; do
        echo "[d1] get_last_update_time attempt $((MAX_RETRIES-attempts+1))/${MAX_RETRIES:-3} (timeout: ${timeout_cmd:+$timeout_sec s})" >&2
        echo "[cmd] ${timeout_cmd:+${timeout_cmd[*]} }${cmd[*]}" >&2
        start=$(date +%s)
        stdout_file=$(mktemp)
        stderr_file=$(mktemp)
        local -a run_cmd
        if [ ${#timeout_cmd[@]} -gt 0 ]; then
            run_cmd=("${timeout_cmd[@]}" "${cmd[@]}")
        else
            run_cmd=("${cmd[@]}")
        fi
        if "${run_cmd[@]}" >"$stdout_file" 2>"$stderr_file"; then
            exit_code=0
        else
            exit_code=$?
        fi
        last_update_time_output=$(<"$stdout_file")
        stderr_out=$(<"$stderr_file")
        rm -f "$stdout_file" "$stderr_file"
        end=$(date +%s)
        duration=$((end-start))
        if [ "$exit_code" -eq 0 ]; then
            local last_update_time
            last_update_time=$(echo "$last_update_time_output" | jq '.[0].results[0].last_insert_ts')
            echo "[d1] get_last_update_time ok in ${duration}s -> ${last_update_time}" >&2
            if [ -n "$last_update_time" ] && [ "$last_update_time" != "null" ]; then
                echo "$last_update_time"
                return 0
            fi
        else
            echo "[d1] get_last_update_time failed (exit $exit_code, ${duration}s). stderr: $stderr_out" >&2
        fi
        attempts=$((attempts-1))
        if [ $attempts -gt 0 ]; then
            echo "[retry] get_last_update_time retrying in $delay s ($attempts left)" >&2
            sleep "$delay"
        fi
    done
    send_telegram "ERROR: Failed to query last update time from D1 after retries."
    echo "0"
    return 1
}

# Function to process a single SQLite file
process_sqlite_file() {
    local db_file="$1"
    local filename
    filename=$(basename "$db_file")
    local current_total
    current_total=$(get_total_entries)
    
    echo "Processing SQLite file: $filename" >&2
    echo "[process] Starting at $(date), size: $(stat -f%z "$db_file" 2>/dev/null || stat -c%s "$db_file") bytes" >&2
    
    # Clean up previous chunks if they exist
    if [ -d "$CHUNKS_DIR" ]; then
        echo "Removing existing chunks directory: $CHUNKS_DIR" >&2
        rm -r "$CHUNKS_DIR" || {
            send_telegram "ERROR: Failed to remove existing chunks directory"
            return 1
        }
    fi
    echo "[process] Creating chunks dir: $CHUNKS_DIR" >&2
    
    mkdir -p "$CHUNKS_DIR" || {
        send_telegram "ERROR: Failed to create chunks directory"
        return 1
    }

    echo "Dumping database and creating chunks of $CHUNK_SIZE lines each..." >&2
    # The sed command ensures that existing records on the remote database are ignored.
    # We also grep -v to remove transaction statements that D1 does not support.
    echo "[process] Running sqlite3 dump + transforms + split (chunk size: $CHUNK_SIZE)" >&2
    echo "[cmd] sqlite3 '$db_file' .dump | grep -vE '^BEGIN TRANSACTION;|^COMMIT;' | sed 's/^INSERT INTO/INSERT OR IGNORE INTO/' | sed 's/^CREATE TABLE/CREATE TABLE IF NOT EXISTS/' | split -l '$CHUNK_SIZE' - '$CHUNKS_DIR/chunk_'" >&2
    if ! sqlite3 "$db_file" .dump | \
      grep -vE '^BEGIN TRANSACTION;|^COMMIT;' | \
      sed 's/^INSERT INTO/INSERT OR IGNORE INTO/' | \
      sed 's/^CREATE TABLE/CREATE TABLE IF NOT EXISTS/' | \
      split -l "$CHUNK_SIZE" - "$CHUNKS_DIR/chunk_"; then
        send_telegram "ERROR: Failed to dump and split database $filename"
        return 1
    fi

    # Add .sql suffix for macOS compatibility
    echo "[process] Renaming chunk_* to *.sql" >&2
    for f in "$CHUNKS_DIR"/chunk_*; do
      if [ -f "$f" ]; then
        mv "$f" "$f.sql" || {
          send_telegram "ERROR: Failed to rename chunk file $f"
          return 1
        }
      fi
    done

    echo "Database successfully split into chunks in '$CHUNKS_DIR/'" >&2
    echo "[process] Chunk count: $(ls -1 "$CHUNKS_DIR"/*.sql 2>/dev/null | wc -l | tr -d ' ')" >&2

    # Upload chunks to D1 with retry
    echo "Uploading chunks to D1 database '$D1_DATABASE'..." >&2
    local upload_failed=false

    chunk_index=0
    chunk_total=$(ls -1 "$CHUNKS_DIR"/*.sql 2>/dev/null | wc -l | tr -d ' ')
    for file in "$CHUNKS_DIR"/*.sql; do
      if [ ! -f "$file" ]; then
        continue
      fi
      
      chunk_index=$((chunk_index+1))
      echo "[upload] ($chunk_index/$chunk_total) Uploading '$file'..." >&2
      echo "[cmd] npx wrangler d1 execute '$D1_DATABASE' --remote --file='$file' -y --json" >&2
      retries=$MAX_RETRIES
      upload_output=""
      until upload_output=$(npx wrangler d1 execute "$D1_DATABASE" --remote --file="$file" -y --json)
      do
        retries=$((retries-1))
        if [ $retries -eq 0 ]; then
          echo "[upload] Failed '$file' after $MAX_RETRIES attempts" >&2
          send_telegram "ERROR: Failed to upload '$file' after $MAX_RETRIES attempts."
          upload_failed=true
          break
        fi
        echo "[upload] Retry in $RETRY_DELAY_SECONDS s... ($retries left)" >&2
        sleep $RETRY_DELAY_SECONDS
      done
      
      if [ "$upload_failed" = "true" ]; then
        break
      fi
      
      echo "[upload] Success '$file'" >&2
    done

    if [ "$upload_failed" = "true" ]; then
        send_telegram "ERROR: Upload failed for $filename. Aborting processing."
        return 1
    fi

    local new_total
    echo "[post] Fetching new total entries" >&2
    echo "[cmd] npx wrangler d1 execute '$D1_DATABASE' --remote --command='SELECT n FROM _table_counts WHERE name = \'pda_registry\';' --json" >&2
    new_total=$(get_total_entries)
    local difference
    difference=$((new_total - current_total))
    # Send Telegram notification
    local message="Database upload completed: $filename ($difference entries processed, total: $new_total)"
    send_telegram "$message"
    
    # Cleanup local chunks
    echo "[cleanup] Removing chunks dir" >&2
    rm -r "$CHUNKS_DIR" || {
        send_telegram "Warning: Failed to remove chunks directory"
    }
    
    # Remove local SQLite file
    echo "[cleanup] Removing local sqlite file $db_file" >&2
    rm "$db_file" || {
        send_telegram "Warning: Failed to remove local SQLite file: $filename"
    }
    
    # Delete remote file after successful processing
    echo "[cleanup] Deleting remote file $filename" >&2
    if ! delete_remote_file "$filename"; then
        # Send notification about cleanup failure but don't fail the overall process
        local cleanup_error_msg="Warning: Failed to delete remote file after successful upload: $filename"
        send_telegram "$cleanup_error_msg"
    fi
    
    echo "[process] Completed at $(date). Entries delta: $difference" >&2
    echo "$difference" # Return the number of new entries
    return 0
}

# --- Main monitoring loop ---
echo "Starting continuous SQLite monitoring and processing..." >&2
echo "Remote path: $REMOTE_PATH" >&2
echo "Monitor interval: $MONITOR_INTERVAL seconds (1 minute)" >&2


# Get and display startup information
echo "" >&2
echo "Fetching startup information from database..." >&2
current_total_entries=$(get_total_entries)
last_update_time=$(get_last_update_time)

echo "Current total entries in database: $current_total_entries" >&2
if [ -n "$last_update_time" ] && [ "$last_update_time" != "0" ]; then
    if [[ "$(uname)" == "Darwin" ]]; then # macOS
        last_update_formatted=$(date -r "$last_update_time")
    else # Linux
        last_update_formatted=$(date -d "@$last_update_time")
    fi
    echo "Last upload time: $last_update_formatted" >&2
else
    echo "Last upload time: Never" >&2
fi

startup_msg="PDA Upload Monitor Started"
startup_msg="$startup_msg\nTotal entries: $current_total_entries"
if [ -n "$last_update_time" ] && [ "$last_update_time" != "0" ]; then
    startup_msg="$startup_msg\nLast upload: $last_update_formatted"
else
    startup_msg="$startup_msg\nLast upload: Never"
fi
send_telegram "$startup_msg"

# Create local download directory
mkdir -p "$LOCAL_DOWNLOAD_DIR" || {
    send_telegram "ERROR: Failed to create local download directory: $LOCAL_DOWNLOAD_DIR"
    exit 1
}

# Trap to handle graceful shutdown
trap 'echo ""; echo "Received interrupt signal. Shutting down gracefully..."; exit 0' INT TERM

while true; do
    echo "$(date): Checking for new SQLite files..." >&2
    
    # Download SQLite files from remote using rsync
    # We build a list of files to process.
    rsync_output=""
    rsync_exit_code=0
    echo "[cmd] rsync -av --include '*/' --include '*.sqlite' --exclude '*.shard*.sqlite' --exclude '*' '$REMOTE_PATH' '$LOCAL_DOWNLOAD_DIR/'" >&2
    rsync_output=$(rsync -av \
        --include="*.sqlite" \
        --exclude="*.shard*.sqlite" \
        --exclude="*" \
        "$REMOTE_PATH" "$LOCAL_DOWNLOAD_DIR/" 2>&1) || rsync_exit_code=$?

    if [ $rsync_exit_code -ne 0 ]; then
        echo "Warning: rsync command failed with exit code $rsync_exit_code. Will retry in next cycle." >&2
        send_telegram "Warning: rsync failed with exit code $rsync_exit_code. Output: $rsync_output"
        sleep $MONITOR_INTERVAL
        continue
    fi
    
    # Extract file paths from rsync output.
    # We use `awk` to get just the path, which is more reliable than parsing the whole line.
    # Using a while-read loop for compatibility with older bash versions (like on macOS).
    downloaded_files=()
    while IFS= read -r line; do
        downloaded_files+=("$line")
    done < <(echo "$rsync_output" | grep -E -- 'deleting|\.sqlite$' | grep -v '/$' | awk 'NF>1 {print $2} NF==1 {print $1}')

    if [ ${#downloaded_files[@]} -eq 0 ]; then
        echo "No new SQLite files found. Sleeping for $MONITOR_INTERVAL seconds..." >&2
    else
        send_telegram "Downloaded files: ${downloaded_files[*]}"
        processed_count=0
        failed_count=0
        total_entries=0
        
        # Process each downloaded SQLite file
        for downloaded_file_rel_path in "${downloaded_files[@]}"; do
            sqlite_file="$LOCAL_DOWNLOAD_DIR/$downloaded_file_rel_path"

            if [ ! -f "$sqlite_file" ]; then
                echo "Warning: Expected file '$sqlite_file' not found. It might have been a directory or was removed. Skipping." >&2
                continue
            fi

            # --- File Stability Check ---
            # Ensure the file is not currently being written to by checking if its size is stable.
            echo "Checking stability of '$sqlite_file'..." >&2
            echo "[cmd] stat -c%s '$sqlite_file' || stat -f%z '$sqlite_file'" >&2
            size1=$(stat -c%s "$sqlite_file" 2>/dev/null || stat -f%z "$sqlite_file")
            sleep 2 # Wait for potential writes to finish
            echo "[cmd] stat -c%s '$sqlite_file' || stat -f%z '$sqlite_file'" >&2
            size2=$(stat -c%s "$sqlite_file" 2>/dev/null || stat -f%z "$sqlite_file")

            if [ "$size1" -ne "$size2" ]; then
                echo "File '$sqlite_file' is unstable (size changed from $size1 to $size2). Skipping for now." >&2
                send_telegram "Warning: File '$sqlite_file' is unstable. It will be re-checked in the next cycle."
                continue
            fi
            echo "File is stable (size $size1). Proceeding with processing." >&2
            # --- End Stability Check ---
            
            echo "" >&2
            echo "=== Processing $(basename "$sqlite_file") ===" >&2
            
            file_entries=0
            if file_entries=$(process_sqlite_file "$sqlite_file"); then
                echo "Successfully processed $(basename "$sqlite_file")" >&2
                processed_count=$((processed_count + 1))
                total_entries=$((total_entries + file_entries))
            else
                echo "Failed to process $(basename "$sqlite_file")" >&2
                failed_count=$((failed_count + 1))
                # Keep the file for manual inspection
                send_telegram "File kept at: $sqlite_file"
                
                # Send error notification
                error_msg="ERROR: Failed to process $(basename "$sqlite_file")"
                send_telegram "$error_msg"
            fi
            echo "=== Finished processing $(basename "$sqlite_file") ===" >&2
            echo "" >&2
        done
        
        # Send summary notification if any files were processed successfully
        if [ $processed_count -gt 0 ]; then
            # Get updated total from KV store
            updated_total=$(get_total_entries)
            summary_msg="Batch processing completed: $processed_count files processed, $total_entries entries added (total: $updated_total)"
            if [ $failed_count -gt 0 ]; then
                summary_msg="$summary_msg, $failed_count failed"
            fi
            send_telegram "$summary_msg"
        fi
        
        echo "Batch summary: $processed_count processed, $failed_count failed, $total_entries entries added" >&2
        if [ $processed_count -gt 0 ]; then
            final_total=$(get_total_entries)
            echo "Database now contains: $final_total total entries" >&2
        fi
        echo "Finished processing all downloaded files. Sleeping for $MONITOR_INTERVAL seconds..." >&2
    fi
    
    sleep $MONITOR_INTERVAL
done

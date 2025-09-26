#!/bin/bash

# Check if path parameter is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <path>"
    echo "Example: $0 /path/to/data"
    exit 1
fi

PATH_PARAM="$1"

# Check if the provided path exists
if [ ! -d "$PATH_PARAM" ]; then
    echo "Error: Directory '$PATH_PARAM' does not exist"
    exit 1
fi

echo "Starting merge script for path: $PATH_PARAM"
echo "Running cargo command every 2 minutes..."

# Function to run the cargo command
run_merge() {
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local output_file="$PATH_PARAM/merged-$timestamp.sqlite"
    
    echo "$(date): Running merge command..."
    echo "Output file: $output_file"
    
    cargo run -r -- merge --path "$PATH_PARAM" --output "$output_file"
    
    if [ $? -eq 0 ]; then
        echo "$(date): Merge completed successfully"
    else
        echo "$(date): Merge failed with exit code $?"
    fi
    echo "----------------------------------------"
}

# Run the merge command immediately on start
run_merge

# Then run it every 2 minutes (120 seconds)
while true; do
    sleep 120  # 2 minutes
    run_merge
done

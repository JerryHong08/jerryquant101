#!/bin/bash
# ===========================
# Multi-Machine Weekly Update Script
# ===========================
# Supports two roles:
#   - server: Fetches metadata from external APIs (Polygon, FMP)
#   - client: Downloads stock data, syncs metadata from server, updates low_volume_tickers
#
# Usage:
#   MACHINE_ROLE=server ./scripts/weekly_update.sh   # Run as server (oldman)
#   MACHINE_ROLE=client ./scripts/weekly_update.sh   # Run as client (wsl2)
#   ./scripts/weekly_update.sh                       # Uses role from machine_config.yaml

set -e

# ===========================
# Configuration
# ===========================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
CONFIG_FILE="$PROJECT_ROOT/machine_config.yaml"

LOGFILE="$PROJECT_ROOT/logs/weekly_update_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$PROJECT_ROOT/logs"

TOTAL_DAYS=7

# ===========================
# Helper Functions
# ===========================
get_machine_role() {
    # Priority: 1. Environment variable, 2. Config file
    if [ -n "$MACHINE_ROLE" ]; then
        echo "$MACHINE_ROLE"
        return
    fi

    if [ -f "$CONFIG_FILE" ]; then
        # Parse YAML to get role (simple grep-based parsing)
        local role=$(grep -A1 "^machine:" "$CONFIG_FILE" | grep "role:" | sed 's/.*role:[[:space:]]*"\([^"]*\)".*/\1/')
        if [ -n "$role" ]; then
            echo "$role"
            return
        fi
    fi

    # Default to client
    echo "client"
}

MACHINE_ROLE=$(get_machine_role)
current_task=0

# Set total tasks based on role
if [ "$MACHINE_ROLE" = "server" ]; then
    TOTAL_TASKS=4
else
    TOTAL_TASKS=6
fi

run_task() {
    local task_name="$1"
    shift
    local command=("$@")

    current_task=$((current_task + 1))
    echo "" | tee -a "$LOGFILE"
    echo "[$current_task/$TOTAL_TASKS] $task_name" | tee -a "$LOGFILE"
    echo "Command: ${command[*]}" | tee -a "$LOGFILE"
    echo "---" | tee -a "$LOGFILE"
    "${command[@]}" 2>&1 | tee -a "$LOGFILE"
}

# ===========================
# Main Execution
# ===========================
cd "$PROJECT_ROOT"

echo "=============================================" | tee -a "$LOGFILE"
echo "Weekly Data Update - $(date)" | tee -a "$LOGFILE"
echo "Machine Role: $MACHINE_ROLE" | tee -a "$LOGFILE"
echo "Config File: $CONFIG_FILE" | tee -a "$LOGFILE"
echo "=============================================" | tee -a "$LOGFILE"

if [ "$MACHINE_ROLE" = "server" ]; then
    # ===========================
    # SERVER ROLE (oldman)
    # ===========================
    # Fetches metadata from external APIs (Polygon, FMP)
    # This data will be synced to clients via fetch_from_server.py

    echo "" | tee -a "$LOGFILE"
    echo "=== Running as SERVER ===" | tee -a "$LOGFILE"
    echo "Fetching metadata from external APIs..." | tee -a "$LOGFILE"

    run_task "Fetching splits data..." \
        python src/data_fetcher/splits_fetch.py

    run_task "Fetching tickers list (stocks, otc, indices)..." \
        python src/data_fetcher/all_tickers_fetch.py

    run_task "Fetching indices data (IRX, SPX, etc.)..." \
        python src/data_fetcher/indices_fetch.py

    run_task "Fetching float shares data..." \
        python src/data_fetcher/fmp_fundamental_fetch.py

    echo "" | tee -a "$LOGFILE"
    echo "=== Server tasks completed ===" | tee -a "$LOGFILE"
    echo "Clients can now sync this data using fetch_from_server.py" | tee -a "$LOGFILE"

else
    # ===========================
    # CLIENT ROLE (wsl2)
    # ===========================
    # 1. Downloads stock data from Polygon
    # 2. Converts to Parquet format
    # 3. Syncs metadata from server
    # 4. Updates low_volume_tickers

    echo "" | tee -a "$LOGFILE"
    echo "=== Running as CLIENT ===" | tee -a "$LOGFILE"
    echo "Downloading stock data and syncing metadata from server..." | tee -a "$LOGFILE"

    # Step 1: Download stock data from Polygon
    run_task "Downloading minute_aggs_v1 data..." \
        python src/data_fetcher/polygon_downloader.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days $TOTAL_DAYS

    run_task "Downloading day_aggs_v1 data..." \
        python src/data_fetcher/polygon_downloader.py --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days $TOTAL_DAYS

    # Step 2: Convert to Parquet format
    run_task "Transforming minute_aggs_v1 to Parquet..." \
        python src/data_fetcher/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days $TOTAL_DAYS

    run_task "Transforming day_aggs_v1 to Parquet..." \
        python src/data_fetcher/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days $TOTAL_DAYS

    # Step 3: Sync metadata from server (splits, tickers, indices, float_shares)
    run_task "Syncing metadata from server..." \
        python src/data_fetcher/fetch_from_server.py

    # Step 4: Update low volume tickers (requires stock data + splits)
    run_task "Updating low_volume_tickers..." \
        python scripts/low_volume_ticker_update.py -i

    echo "" | tee -a "$LOGFILE"
    echo "=== Client tasks completed ===" | tee -a "$LOGFILE"

fi

echo "" | tee -a "$LOGFILE"
echo "=============================================" | tee -a "$LOGFILE"
echo "Weekly Update Completed - $(date)" | tee -a "$LOGFILE"
echo "Log file: $LOGFILE" | tee -a "$LOGFILE"
echo "=============================================" | tee -a "$LOGFILE"

exit 0

# Usage:
# chmod +x scripts/weekly_update.sh
# MACHINE_ROLE=server ./scripts/weekly_update.sh   # On oldman
# MACHINE_ROLE=client ./scripts/weekly_update.sh   # On wsl2 (or just ./scripts/weekly_update.sh)

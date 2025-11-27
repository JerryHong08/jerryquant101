#!/bin/bash
# ===========================
# automation update .sh
# ===========================

set -e
LOGFILE="logs/weekly_update_$(date +%Y%m%d_%H%M%S).log"
mkdir -p logs

current_task=0
TOTAL_TASKS=8
TOTAL_DAYS=7

run_task() {
    local task_name="$1"
    shift
    local command=("$@")

    current_task=$((current_task + 1))
    echo "[$current_task/$TOTAL_TASKS] $task_name" | tee -a "$LOGFILE"
    "${command[@]}" | tee -a "$LOGFILE"
}

echo "===== start data update$(date) =====" | tee -a "$LOGFILE"

# run all task
run_task "downloading minute_aggs_v1 data..." python src/data_fetcher/polygon_downloader.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days $TOTAL_DAYS

run_task "downloading day_aggs_v1 data..." python src/data_fetcher/polygon_downloader.py --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days $TOTAL_DAYS

run_task "tranform minute_aggs_v1 to Parquet..." python src/data_fetcher/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days $TOTAL_DAYS

run_task "tranform day_aggs_v1 to Parquet..." python src/data_fetcher/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days $TOTAL_DAYS

run_task "tickers splits data fecthing..." python src/data_fetcher/splits_fetch.py

run_task "tickers list fetching..." python src/data_fetcher/all_tickers_fetch.py

run_task "versatile tickers updating..." python src/data_fetcher/versatile_tickers_fetch.py

run_task "low_volume_tickers updating..." python scripts/write_low_volume_ticker_csv.py

echo "===== low_volume_tickers updated.$(date) =====" | tee -a "$LOGFILE"

echo "===== data updated.$(date) =====" | tee -a "$LOGFILE"

exit 0

# chmod +x scripts/weekly_update.sh
# ./scripts/weekly_update.sh
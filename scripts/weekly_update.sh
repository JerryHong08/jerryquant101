#!/bin/bash
# ===========================
# 自动化一周数据更新脚本
# ===========================

set -e  # 出错即停止脚本执行
LOGFILE="logs/weekly_update_$(date +%Y%m%d_%H%M%S).log"
mkdir -p logs

# 初始化计数器
current_task=0
TOTAL_TASKS=8

# 执行任务的函数
run_task() {
    local task_name="$1"
    shift  # 移除第一个参数
    local command=("$@")  # 剩余参数作为命令

    current_task=$((current_task + 1))
    echo "[$current_task/$TOTAL_TASKS] $task_name" | tee -a "$LOGFILE"
    "${command[@]}" | tee -a "$LOGFILE"
}

echo "===== 开始数据更新：$(date) =====" | tee -a "$LOGFILE"

# 执行所有任务
run_task "下载 minute_aggs_v1 数据中..." python src/data_fetcher/polygon_downloader.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days 7

run_task "下载 day_aggs_v1 数据中..." python src/data_fetcher/polygon_downloader.py --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days 7

run_task "转换 minute_aggs_v1 为 Parquet..." python src/data_fetcher/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days 7

run_task "转换 day_aggs_v1 为 Parquet..." python src/data_fetcher/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type day_aggs_v1 --recent-days 7

run_task "获取最新拆股信息..." python src/data_fetcher/splits_fetch.py

run_task "获取最新股票列表..." python src/data_fetcher/all_tickers_fetch.py

run_task "versatile tickers 更新..." python src/data_fetcher/versatile_tickers_fetch.py

run_task "low_volume_tickers 更新..." python scripts/write_low_volume_ticker_csv.py

echo "===== low_volume_tickers 更新完成：$(date) =====" | tee -a "$LOGFILE"

echo "===== 数据更新完成：$(date) =====" | tee -a "$LOGFILE"

exit 0

# chmod +x scripts/weekly_update.sh
# ./scripts/weekly_update.sh

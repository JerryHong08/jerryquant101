#!/bin/bash
# ===========================
# 自动化一周数据更新脚本
# ===========================

set -e  # 出错即停止脚本执行
LOGFILE="logs/weekly_update_$(date +%Y%m%d_%H%M%S).log"
mkdir -p logs

echo "===== 开始数据更新：$(date) =====" | tee -a "$LOGFILE"

# ---- 下载 minute 级别数据 ----
echo "[1/7] 下载 minute_aggs_v1 数据中..." | tee -a "$LOGFILE"
python src/data_fecther/polygon_downloader.py \
    --asset-class us_stocks_sip \
    --data-type minute_aggs_v1 \
    --recent-days 7 | tee -a "$LOGFILE"

# ---- 下载 day 级别数据 ----
echo "[2/7] 下载 day_aggs_v1 数据中..." | tee -a "$LOGFILE"
python src/data_fecther/polygon_downloader.py \
    --asset-class us_stocks_sip \
    --data-type day_aggs_v1 \
    --recent-days 7 | tee -a "$LOGFILE"

# ---- 转换 minute 数据 ----
echo "[3/7] 转换 minute_aggs_v1 为 Parquet..." | tee -a "$LOGFILE"
python src/data_fecther/csvgz_to_parquet.py \
    --asset-class us_stocks_sip \
    --data-type minute_aggs_v1 \
    --recent-days 7 | tee -a "$LOGFILE"

# ---- 转换 day 数据 ----
echo "[4/7] 转换 day_aggs_v1 为 Parquet..." | tee -a "$LOGFILE"
python src/data_fecther/csvgz_to_parquet.py \
    --asset-class us_stocks_sip \
    --data-type day_aggs_v1 \
    --recent-days 7 | tee -a "$LOGFILE"

# ---- 获取拆股信息 ----
echo "[5/7] 获取最新拆股信息..." | tee -a "$LOGFILE"
python src/data_fecther/splits_fetch.py | tee -a "$LOGFILE"

# ---- 获取所有股票列表 ----
echo "[6/7] 获取最新股票列表..." | tee -a "$LOGFILE"
python src/data_fecther/all_tickers_fetch.py | tee -a "$LOGFILE"

# ---- fecth versatile tickers ----
echo "===== [7/7] versatile tickers 更新：$(date) =====" | tee -a "$LOGFILE"
python src/data_fecther/versatile_tickers_fetch.py | tee -a "$LOGFILE"
echo "===== versatile tickers 更新完成：$(date) =====" | tee -a "$LOGFILE"

echo "===== 数据更新完成：$(date) =====" | tee -a "$LOGFILE"

# chmod +x scripts/weekly_update.sh
# ./scripts/weekly_update.sh

import os
import random
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import redis

from cores.config import data_dir
from cores.data_loader import stock_load_process
from utils.data_utils.data_uitils import resolve_date_range
from utils.data_utils.path_loader import DataPathFetcher

r = redis.Redis(host="localhost", port=6379, db=0)


def load_previous_data(
    replay_date: str, tickers: list[str] = None, use_cache=True
) -> pl.DataFrame:
    replay_date = datetime.strptime(replay_date, "%Y%m%d").strftime("%Y-%m-%d")
    prev_date, _ = resolve_date_range(replay_date, -1)
    prev_data = stock_load_process(
        tickers=tickers,
        start_date=prev_date,
        end_date=prev_date,
        use_cache=use_cache,
        skip_low_volume=False,
    )
    prev_data = prev_data.select(
        pl.col("ticker"),
        pl.col("volume").alias("prev_volume"),
        pl.col("close").alias("prev_close"),
    )
    return prev_data.collect()


def trades_replayer_engine(replay_date: str, speed_multiplier: float = 1.0):
    replay_date = datetime.strptime(replay_date, "%Y%m%d").strftime("%Y-%m-%d")
    path_fetcher = DataPathFetcher(
        asset="us_stocks_sip",
        data_type="trades_v1",
        start_date=replay_date,
        end_date=replay_date,
        lake=True,
        s3=False,
    )
    paths = path_fetcher.data_dir_calculate()

    # ---- Step 1: load trades_v1 ----
    lf = (
        pl.scan_parquet(paths)
        .select(["ticker", "price", "size", "sip_timestamp"])
        .filter(pl.col("ticker").is_in(["NVDA", "TSLA"]))  # for test
        .with_columns(
            pl.from_epoch(pl.col("sip_timestamp"), time_unit="ns")
            .dt.convert_time_zone("America/New_York")
            .alias("timestamp")
        )
        .drop("sip_timestamp")
        .sort(["ticker", "timestamp"])
    )
    df = lf.collect(engine="streaming")

    if df.is_empty():
        print("No trades found.")
        return

    # ---- Step 2: load prev_data ----
    prev_data = load_previous_data(
        replay_date=replay_date.replace("-", ""), use_cache=True
    )

    # ---- Step 3: add accumulated_volume ----
    df = df.with_columns(
        pl.col("size").cum_sum().over("ticker").alias("accumulated_volume")
    )

    df = df.join(prev_data, on="ticker", how="left")

    # ---- Step 4: percent_change ----

    df = df.with_columns(
        ((pl.col("price") - pl.col("prev_close")) / pl.col("prev_close") * 100).alias(
            "percent_change"
        )
    )

    print(df.tail())

    # ---- Step 5: loop and push ----
    print(f"Starting trades replay for {len(df)} rows...")

    bucket_size = 30  # seconds
    df = df.with_columns(
        (
            (pl.col("timestamp").dt.timestamp(time_unit="ns") // int(1e9))
            // bucket_size
        ).alias("bucket_id"),
    )

    for bucket_id, bucket_df in df.group_by("bucket_id", maintain_order=True):
        ts_start = bucket_df["timestamp"].min()
        ts_end = bucket_df["timestamp"].max()
        with pl.Config(tbl_cols=15):
            print(bucket_df.tail())

        bucket_df = bucket_df.with_columns(pl.col("timestamp").cast(pl.Int64)).drop(
            ["bucket_id", "size"]
        )

        payload = bucket_df.write_json()
        r.publish("market_snapshot", payload)

        print(
            f"[Bucket {bucket_id}] {ts_start} → {ts_end}, {len(bucket_df)} trades pushed."
        )

        time.sleep(bucket_size / speed_multiplier)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="us_stocks_sip trades_v1 replayer")
    parser.add_argument("--date", default="20251015", help="Replay date (YYYYMMDD)")
    parser.add_argument(
        "--speed", type=float, default=30.0, help="Speed multiplier (default: 1.0)"
    )

    args = parser.parse_args()

    print(f"Replaying trades_v1 for date: {args.date}")
    print(f"Speed: {args.speed}x")

    trades_replayer_engine(args.date, args.speed)

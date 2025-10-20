import os
import time
from collections import defaultdict
from datetime import datetime

import polars as pl
import redis

from cores.config import data_dir
from cores.data_loader import stock_load_process
from utils.data_utils.data_uitils import resolve_date_range
from utils.data_utils.path_loader import DataPathFetcher

r = redis.Redis(host="localhost", port=6379, db=0)

# TODO:
# This version will have missing data in some bucket
# but it's very fast and memory save. and the bugs can be fixed in the reciever end.


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
    return prev_data.select(
        pl.col("ticker"),
        pl.col("volume").alias("prev_volume"),
        pl.col("close").alias("prev_close"),
    ).collect()


def trades_replayer_engine(replay_date: str, speed_multiplier: float = 1.0):
    replay_date_obj = datetime.strptime(replay_date, "%Y%m%d")
    replay_date_str = replay_date_obj.strftime("%Y-%m-%d")

    path_fetcher = DataPathFetcher(
        asset="us_stocks_sip",
        data_type="trades_v1",
        start_date=replay_date_str,
        end_date=replay_date_str,
        lake=True,
        s3=False,
    )
    paths = path_fetcher.data_dir_calculate()

    # ---- Step 1: Load prev_data ----
    prev_data = load_previous_data(replay_date=replay_date, use_cache=True)
    prev_data_dict = {}
    for row in prev_data.iter_rows(named=True):
        prev_data_dict[row["ticker"]] = {
            "prev_volume": row["prev_volume"],
            "prev_close": row["prev_close"],
        }

    # ---- Step 2: Process data bucket by bucket ----
    bucket_size = 30  # seconds
    sleep_duration = bucket_size / speed_multiplier

    print("Starting trades replay...")

    # Process data in a streaming fashion, bucket by bucket
    lf = (
        pl.scan_parquet(paths)
        .select(["ticker", "price", "size", "sip_timestamp"])
        .with_columns(
            pl.from_epoch(pl.col("sip_timestamp"), time_unit="ns")
            .dt.convert_time_zone("America/New_York")
            .alias("timestamp")
        )
        .drop("sip_timestamp")
        .sort(["ticker", "timestamp"])
    )

    # Get all unique bucket IDs first
    bucket_lf = (
        lf.with_columns(
            (
                pl.col("timestamp").dt.timestamp(time_unit="ns")
                // int(1e9)
                // bucket_size
            ).alias("bucket_id")
        )
        .select("bucket_id")
        .unique()
        .sort("bucket_id")
    )

    bucket_ids = bucket_lf.collect(engine="streaming")["bucket_id"].to_list()

    # Process each bucket separately
    for bucket_id in bucket_ids:
        print(f"Processing bucket {bucket_id}...")

        # Load only the data for this specific bucket
        bucket_lf = (
            pl.scan_parquet(paths)
            .select(["ticker", "price", "size", "sip_timestamp"])
            .with_columns(
                pl.from_epoch(pl.col("sip_timestamp"), time_unit="ns")
                .dt.convert_time_zone("America/New_York")
                .alias("timestamp")
            )
            .drop("sip_timestamp")
            .with_columns(
                (
                    pl.col("timestamp").dt.timestamp(time_unit="ns")
                    // int(1e9)
                    // bucket_size
                ).alias("bucket_id"),
                pl.col("size").cum_sum().over("ticker").alias("accumulated_volume"),
            )
            # Select the exact time(bucket_id) snapshot
            .filter(pl.col("bucket_id") == bucket_id)
            .sort(["ticker", "timestamp"])
        )

        bucket_df = bucket_lf.collect(engine="streaming")

        if bucket_df.is_empty():
            continue

        # Process this bucket
        process_bucket(bucket_df, prev_data_dict, bucket_id, sleep_duration)

        # Clear memory
        del bucket_lf, bucket_df


def process_bucket(bucket_df, prev_data_dict, bucket_id, sleep_duration):
    """
    Process a single bucket of data
    """

    # Create market snapshot for this bucket
    whole_market_snapshot = bucket_df.group_by("ticker", maintain_order=True).agg(
        pl.col("timestamp").last(),
        pl.col("price").last().alias("current_price"),
        pl.col("accumulated_volume").last(),
    )

    # Add prev_data and calculate percent change
    rows = []
    for row in whole_market_snapshot.iter_rows(named=True):
        ticker = row["ticker"]
        prev_info = prev_data_dict.get(ticker, {"prev_volume": 0, "prev_close": 0})

        percent_change = (
            (
                (row["current_price"] - prev_info["prev_close"])
                / prev_info["prev_close"]
                * 100
            )
            if prev_info["prev_close"]
            else 0
        )

        rows.append(
            {
                "ticker": ticker,
                "timestamp": row["timestamp"],
                "current_price": row["current_price"],
                "percent_change": percent_change,
                "accumulated_volume": row["accumulated_volume"],
                "prev_close": prev_info["prev_close"],
                "prev_volume": prev_info["prev_volume"],
            }
        )

    snapshot_df = pl.DataFrame(rows)

    snapshot_df = snapshot_df.with_columns(
        pl.col("timestamp").dt.timestamp(time_unit="ms")
    )

    payload = snapshot_df.write_json()
    r.publish("market_snapshot", payload)

    if bucket_id % 10 == 0:
        ts_min = snapshot_df["timestamp"].min()
        ts_max = snapshot_df["timestamp"].max()
        print(
            f"[Bucket {bucket_id}] {datetime.fromtimestamp(ts_min/1000)} → {datetime.fromtimestamp(ts_max/1000)}, {len(snapshot_df)} tickers pushed."
        )

    time.sleep(sleep_duration)


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

import os
import time
from collections import defaultdict
from datetime import datetime, timedelta

import polars as pl
import redis

from cores.config import data_dir
from cores.data_loader import stock_load_process
from utils.data_utils.data_uitils import resolve_date_range
from utils.data_utils.path_loader import DataPathFetcher

r = redis.Redis(host="localhost", port=6379, db=0)


# TODO:
# This version forward fill the missing data for all tickers
# but it is too slow.


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

    # ---- Step 2: Load all tickers and create a mapping of ticker to all trades ----
    print("Loading all tickers and their trades...")

    # First, get all unique tickers
    tickers_lf = pl.scan_parquet(paths).select("ticker").unique()
    all_tickers = tickers_lf.collect(engine="streaming")["ticker"].to_list()
    print(f"Found {len(all_tickers)} unique tickers")

    # Load all data with streaming to minimize memory usage
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

    # Process data in smaller chunks by ticker groups
    ticker_groups = [
        all_tickers[i : i + 50] for i in range(0, len(all_tickers), 50)
    ]  # Process 50 tickers at a time

    # ---- Step 3: Process data by buckets with forward filling ----
    bucket_size = 30  # seconds
    sleep_duration = bucket_size / speed_multiplier

    print("Starting trades replay...")

    # ---- Step 2: Get actual time range from data ----
    print("Getting time range from data...")
    time_range_lf = pl.scan_parquet(paths).select(
        pl.col("sip_timestamp").min().alias("min_ts"),
        pl.col("sip_timestamp").max().alias("max_ts"),
    )
    time_range = time_range_lf.collect(engine="streaming")
    min_ts = time_range["min_ts"][0]
    max_ts = time_range["max_ts"][0]

    # Convert to datetime with timezone
    start_time = datetime.fromtimestamp(min_ts / 1e9, tz=None)
    end_time = datetime.fromtimestamp(max_ts / 1e9, tz=None)

    print(f"Data time range: {start_time} to {end_time}")

    # Generate all bucket timestamps
    current_time = start_time
    bucket_timestamps = []
    while current_time <= end_time:
        bucket_timestamps.append(current_time)
        current_time += timedelta(seconds=bucket_size)

    # Process each bucket
    for i, bucket_time in enumerate(bucket_timestamps):
        bucket_id = i

        # For each bucket, we need to get the latest trade for each ticker up to this bucket time
        # Convert bucket_time to the same timezone as the timestamp column
        bucket_end_time = bucket_time.replace(
            tzinfo=None
        )  # Remove timezone for comparison
        print(bucket_end_time)

        # Process tickers in groups to save memory
        all_snapshots = []
        for ticker_group in ticker_groups:
            # Filter data for these tickers and times up to the current bucket
            # We'll handle the time comparison differently to avoid type mismatch
            group_lf = lf.filter(pl.col("ticker").is_in(ticker_group))

            group_df = group_lf.collect(engine="streaming")

            if group_df.is_empty():
                continue

            # Filter by timestamp after collection to avoid type issues
            group_df_filtered = group_df.filter(
                pl.col("timestamp").dt.replace_time_zone(None) <= bucket_end_time
            )

            if group_df_filtered.is_empty():
                continue

            # For each ticker, get the last trade before or at the bucket end time
            latest_trades = group_df_filtered.group_by("ticker").agg(
                pl.all().sort_by("timestamp").last()
            )

            # Calculate accumulated volume for each ticker
            # We need to calculate this based on all trades up to this point
            volume_lf = lf.filter(pl.col("ticker").is_in(ticker_group)).filter(
                pl.col("timestamp").dt.replace_time_zone(None) <= bucket_end_time
            )

            volume_df = volume_lf.collect(engine="streaming")

            if not volume_df.is_empty():
                accumulated_volumes = volume_df.group_by("ticker").agg(
                    pl.col("size").sum().alias("accumulated_volume")
                )

                # Join accumulated volumes to latest trades
                latest_trades = latest_trades.join(
                    accumulated_volumes, on="ticker", how="left"
                )

            # Add prev_data and calculate percent change
            rows = []
            for row in latest_trades.iter_rows(named=True):
                ticker = row["ticker"]
                prev_info = prev_data_dict.get(
                    ticker, {"prev_volume": 0, "prev_close": 0}
                )

                percent_change = (
                    (
                        (row["price"] - prev_info["prev_close"])
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
                        "current_price": row["price"],
                        "percent_change": percent_change,
                        "accumulated_volume": row.get("accumulated_volume", 0),
                        "prev_close": prev_info["prev_close"],
                        "prev_volume": prev_info["prev_volume"],
                    }
                )

            if rows:
                snapshot_df = pl.DataFrame(rows)
                all_snapshots.append(snapshot_df)

            # Clear memory
            del (
                group_lf,
                group_df,
                group_df_filtered,
                latest_trades,
                volume_lf,
                volume_df,
            )

        # Combine all snapshots for this bucket
        if all_snapshots:
            whole_market_snapshot = pl.concat(all_snapshots)

            whole_market_snapshot = whole_market_snapshot.with_columns(
                pl.col("timestamp").dt.timestamp(time_unit="ms")
            )

            payload = whole_market_snapshot.write_json()
            r.publish("market_snapshot", payload)

            if bucket_id % 10 == 0:
                print(
                    f"[Bucket {bucket_id}] {bucket_time}, {len(whole_market_snapshot)} tickers pushed."
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

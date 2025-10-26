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

# optimized to 12-18s per market snapshot.


def load_previous_data(
    replay_date: str, tickers: list[str] = None, use_cache=True
) -> pl.LazyFrame:
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
    )


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
        all_tickers[i : i + 3000] for i in range(0, len(all_tickers), 3000)
    ]

    # ---- Step 3: Process data by buckets with forward filling ----
    bucket_size = 30  # seconds

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
        print(f"bucket_end_time:{bucket_end_time}")

        process_start_time = datetime.now()
        # Process tickers in groups to save memory
        all_snapshots = []
        for ticker_group in ticker_groups:
            snapshot_df = (
                lf.filter(
                    (pl.col("ticker").is_in(ticker_group)),
                    pl.col("timestamp").dt.replace_time_zone(None) <= bucket_end_time,
                )
                .group_by("ticker")
                .agg(
                    pl.all().sort_by("timestamp").last(),
                    pl.col("size").sum().alias("accumulated_volume"),
                )
                .join(prev_data, on="ticker", how="left")
                .with_columns(
                    (
                        (pl.col("price") - pl.col("prev_close"))
                        / pl.col("prev_close")
                        * 100
                    ).alias("percent_change"),
                    pl.col("price").alias("current_price"),
                )
                .filter(
                    (pl.col("percent_change") > 5)
                )  # for market-mover-monitor replayer we only want to find the gainers
            ).collect(engine="streaming")

            if not snapshot_df.is_empty():
                all_snapshots.append(snapshot_df)

            # Clear memory
            del snapshot_df

        print(f"latest_trades collect costs: {datetime.now() - process_start_time}")
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

        print(
            f"current {bucket_id} time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}"
        )


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

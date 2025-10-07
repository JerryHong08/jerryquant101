import glob
import json
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl
import redis

from core_2.config import cache_dir

r = redis.Redis(host="localhost", port=6379, db=0)


def extract_timestamp_from_filename(filename: str) -> datetime:
    """extract timestamps info and turn into datetime"""
    basename = os.path.basename(filename)
    timestamp_str = basename.split("_")[0]  # extract YYYYMMDDHHMMSS

    # parse the timestamp str
    year = int(timestamp_str[:4])
    month = int(timestamp_str[4:6])
    day = int(timestamp_str[6:8])
    hour = int(timestamp_str[8:10])
    minute = int(timestamp_str[10:12])
    second = int(timestamp_str[12:14])

    dt = datetime(year, month, day, hour, minute, second)
    return dt.replace(tzinfo=ZoneInfo("America/New_York"))


def read_market_snapshot_with_timing(
    replay_date: str, speed_multiplier: float = 1.0
) -> None:
    """
    replay file in market_mover_dir for the given date with timing based on filenames.

    Args:
        replay_date: replay date YYYYMMDD
        speed_multiplier: replay speed
    """
    year = replay_date[:4]
    month = replay_date[4:6]
    date = replay_date[6:8]

    market_mover_dir = os.path.join(cache_dir, "market_mover", year, month, date)

    if not os.path.exists(market_mover_dir):
        print(f"Directory not found: {market_mover_dir}")
        return

    all_files = glob.glob(os.path.join(market_mover_dir, "*_market_snapshot.csv"))

    if not all_files:
        print(f"No market snapshot files found in {market_mover_dir}")
        return

    all_files.sort()

    print(f"Found {len(all_files)} files to replay")

    # extract timestamps
    file_timestamps = []
    for file in all_files:
        timestamp = extract_timestamp_from_filename(file)
        file_timestamps.append((file, timestamp))

    first_file_time = file_timestamps[0][1]

    print(f"Starting replay from {first_file_time}")

    for i, (file, file_timestamp) in enumerate(file_timestamps):
        if i > 0:
            prev_timestamp = file_timestamps[i - 1][1]
            time_diff = (file_timestamp - prev_timestamp).total_seconds()
            adjusted_wait_time = time_diff / speed_multiplier

            if adjusted_wait_time > 0:
                print(f"Waiting {adjusted_wait_time:.2f}s (original: {time_diff:.2f}s)")
                time.sleep(adjusted_wait_time)

        print(f"[{file_timestamp}] Reading file: {os.path.basename(file)}")

        try:
            df = pl.read_csv(file)

            # Update timestamp to match the file's historical time
            file_timestamp_ms = int(
                file_timestamp.timestamp() * 1000
            )  # Convert to milliseconds

            # Remove existing timestamp column if it exists to avoid conflicts
            if "timestamp" in df.columns:
                df = df.drop("timestamp")

            # Add new timestamp column as numeric milliseconds
            df = df.with_columns(pl.lit(file_timestamp_ms).alias("timestamp"))

            payload = df.write_json()
            r.publish("market_snapshot", payload)

        except Exception as e:
            print(f"Error processing file {file}: {e}")
            continue


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Market snapshot replayer")
    parser.add_argument("--date", default="20251003", help="Replay date (YYYYMMDD)")
    parser.add_argument(
        "--speed", type=float, default=1.0, help="Speed multiplier (default: 1.0)"
    )

    args = parser.parse_args()

    print(f"Replaying market snapshots for date: {args.date}")
    print(f"Speed: {args.speed}x")

    read_market_snapshot_with_timing(args.date, args.speed)

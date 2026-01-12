import glob
import logging
import os
import socket
import time
from collections import deque
from datetime import datetime
from threading import Thread
from typing import List, Optional
from zoneinfo import ZoneInfo

import polars as pl
import redis

from cores.config import cache_dir
from live_monitor.market_mover_monitor.core.utils.logger import setup_logger
from utils.backtest_utils.backtest_utils import get_common_stocks

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class redis_engine:
    def __init__(self, data_callback=None, replay_date=None, backtrace=False):
        self.backtrace_mode = backtrace
        replay_mode = False
        if replay_date:  # YYYYMMDD
            replay_mode = True

        # ----------redis stream-----------
        self.redis_client = redis.Redis(host="localhost", port=6379, db=0)

        if replay_mode:
            self.STREAM_NAME = f"market_snapshot_stream_replay:{replay_date}"
            self.HSET_NAME = f"state_cursor_replay:{replay_date}"
        else:
            today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
            self.STREAM_NAME = f"market_snapshot_stream:{today}"
            self.HSET_NAME = f"state_cursor:{today}"

        self.CONSUMER_GROUP = "market_consumers"
        self.CONSUMER_NAME = f"consumer_{socket.gethostname()}_{os.getpid()}"

        # Create consumer group if not exists
        try:
            self.redis_client.xgroup_create(
                self.STREAM_NAME, self.CONSUMER_GROUP, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        self.last_df = pl.DataFrame()
        self.data_callback = data_callback  # Callback function for processed data

    # -------------------------------------------------------------------------
    def _redis_stream_listener(self):
        print(f"INFO: Starting Redis Stream consumer {self.CONSUMER_NAME}...")
        print(f"backtrace mode: {self.backtrace_mode}")

        # backtrace HISTORY VIA XRANGE
        if self.backtrace_mode:
            print(">>> Backtracing today's historical messages via XRANGE ...")

            try:
                self.initialize_from_local_file(self.backtrace_mode)
                logger.info(
                    "Finished loading historical data from local files.\n"
                    "Start to listen to real-time data..."
                )
            except Exception as e:
                print(f"Error during backtrace: {e}")

            self.backtrace_mode = False

        # REAL-TIME CONSUMPTION
        while True:
            try:
                self.redis_client.ping()

                messages = self.redis_client.xreadgroup(
                    self.CONSUMER_GROUP,
                    self.CONSUMER_NAME,
                    {self.STREAM_NAME: ">"},
                    count=1,
                    block=2000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            self._process_message(message_id, message_data, ack=True)
                else:
                    print("No new messages, waiting...")

            except KeyboardInterrupt:
                print("Stopping Redis Stream listener...")
                break
            except Exception as e:
                print(f"Redis Stream error: {e}")
                import traceback

                traceback.print_exc()
                time.sleep(5)

        print("Redis Stream listener stopped")

    def _process_message(self, message_id, message_data, ack=True):
        try:
            json_data = message_data[b"data"]
            df = pl.read_json(json_data)
            filtered_df = self.data_process(df)

            self.last_df = filtered_df

            if self.data_callback:
                # Pass is_historical=False for real-time messages
                self.data_callback(filtered_df, is_historical=False)

            if ack:
                self.redis_client.xack(
                    self.STREAM_NAME, self.CONSUMER_GROUP, message_id
                )

        except Exception as e:
            print(f"Error processing message {message_id}: {e}")
            import traceback

            traceback.print_exc()

    def get_stream_info(self):
        """get Stream info"""
        return self.redis_client.xinfo_stream(self.STREAM_NAME)

    def get_pending_messages(self):
        """Get pending msg"""
        return self.redis_client.xpending_range(self.STREAM_NAME, self.CONSUMER_GROUP)

    def cleanup_old_messages(self, max_messages=10000):
        """Clean up old msg to limit the Stream length"""
        self.redis_client.xtrim(self.STREAM_NAME, maxlen=max_messages)

    def initialize_from_local_file(self, date: str) -> None:
        """Load historical data for the given date.

        For development/test state reload:
        1. Fetch Redis HSET to get each ticker's last_state_updated_time
        2. Find the minimal cursor timestamp
        3. Start loading files from after that timestamp (skip already processed)
        4. If no cursors exist, load from the beginning
        """
        year = date[:4]
        month = date[4:6]
        day = date[6:8]

        market_mover_dir = os.path.join(cache_dir, "market_mover", year, month, day)

        if not os.path.exists(market_mover_dir):
            print(f"No historical data found for {date}")
            return

        all_files = glob.glob(os.path.join(market_mover_dir, "*_market_snapshot.csv"))
        all_files.sort()

        if not all_files:
            print(f"No snapshot files found in {market_mover_dir}")
            return

        # Check Redis HSET for existing state cursors (for recovery/reload)
        min_cursor_ts = self._get_min_state_cursor(date)

        if min_cursor_ts:
            # Filter files to only load those after the min cursor timestamp
            files_to_load = self._filter_files_after_timestamp(all_files, min_cursor_ts)
            print(
                f"Recovery mode: Found cursor at {min_cursor_ts}, "
                f"loading {len(files_to_load)}/{len(all_files)} files after cursor..."
            )
        else:
            files_to_load = all_files
            print(f"Fresh load: Loading all {len(files_to_load)} historical files...")

        for file_path in files_to_load:
            try:
                df = pl.read_csv(file_path)

                filtered_df = self.data_process(df)

                self.last_df = filtered_df

                if self.data_callback:
                    self.data_callback(filtered_df, is_historical=True)
            except Exception as e:
                print(f"Error loading {file_path}: {e}")

    def _get_min_state_cursor(self, date: str) -> Optional[datetime]:
        """
        Get the minimum (earliest) state cursor from Redis HSET.
        Used for recovery to determine where to resume loading.

        Returns:
            datetime of the earliest cursor, or None if no cursors exist
        """
        try:
            # Use the same HSET naming convention as SnapshotAnalyzer
            hset_name = f"state_cursor:{date}"
            cursors = self.redis_client.hgetall(hset_name)

            if not cursors:
                logger.info(f"No state cursors found in {hset_name}")
                return None

            # Parse ISO format timestamps and find minimum
            min_ts = None
            for ticker, cursor_ts in cursors.items():
                # Handle bytes from Redis
                if isinstance(ticker, bytes):
                    ticker = ticker.decode("utf-8")
                if isinstance(cursor_ts, bytes):
                    cursor_ts = cursor_ts.decode("utf-8")

                try:
                    ts = datetime.fromisoformat(cursor_ts)
                    if min_ts is None or ts < min_ts:
                        min_ts = ts
                        min_ticker = ticker
                except (ValueError, TypeError) as e:
                    logger.warning(
                        f"Invalid cursor timestamp for {ticker}: {cursor_ts}"
                    )
                    continue

            if min_ts:
                logger.info(
                    f"Found minimum state cursor:{min_ticker} at {min_ts.isoformat()}"
                )
            return min_ts

        except Exception as e:
            logger.error(f"Error reading state cursors: {e}")
            return None

    def _filter_files_after_timestamp(
        self, files: List[str], min_ts: datetime
    ) -> List[str]:
        """
        Filter files to only include those with timestamps after min_ts.
        Supports filename formats:
        - YYYYMMDDHHMMSS_market_snapshot.csv (14 digits)
        - HHMMSS_market_snapshot.csv (6 digits, legacy)
        """
        filtered = []

        for file_path in files:
            try:
                # Extract timestamp from filename
                filename = os.path.basename(file_path)
                time_part = filename.split("_")[0]  # Get timestamp part

                if len(time_part) == 14 and time_part.isdigit():
                    # Parse YYYYMMDDHHMMSS format
                    file_year = int(time_part[0:4])
                    file_month = int(time_part[4:6])
                    file_day = int(time_part[6:8])
                    file_hour = int(time_part[8:10])
                    file_min = int(time_part[10:12])
                    file_sec = int(time_part[12:14])

                    # Create datetime with timezone for comparison
                    file_dt = datetime(
                        file_year,
                        file_month,
                        file_day,
                        file_hour,
                        file_min,
                        file_sec,
                        tzinfo=min_ts.tzinfo,  # Use same timezone as cursor
                    )

                    # Include files at or after the cursor
                    if file_dt >= min_ts:
                        filtered.append(file_path)

                elif len(time_part) == 6 and time_part.isdigit():
                    # Legacy format: Parse HHMMSS (same date as min_ts)
                    file_hour = int(time_part[0:2])
                    file_min = int(time_part[2:4])
                    file_sec = int(time_part[4:6])

                    file_dt = min_ts.replace(
                        hour=file_hour, minute=file_min, second=file_sec, microsecond=0
                    )

                    if file_dt >= min_ts:
                        filtered.append(file_path)
                else:
                    # If we can't parse the timestamp, include the file to be safe
                    logger.warning(
                        f"Could not parse timestamp from filename: {filename}"
                    )
                    filtered.append(file_path)

            except Exception as e:
                logger.warning(f"Error parsing file timestamp {file_path}: {e}")
                filtered.append(file_path)

        return filtered

    def data_process(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process received DataFrame using LazyFrame for query optimization"""
        # Build lazy query plan
        lf = df.lazy().with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.convert_time_zone(
                "America/New_York"
            )
        )

        # Get filter date from eager operation (needed for cache key)
        filter_date = df.select(pl.col("timestamp").max()).to_series()[0]
        # Handle case where timestamp is already datetime or epoch
        if hasattr(filter_date, "strftime"):
            filter_date_str = filter_date.strftime("%Y-%m-%d")
        else:
            from datetime import datetime as dt

            filter_date_str = dt.fromtimestamp(filter_date / 1000).strftime("%Y-%m-%d")

        try:
            # Use module-level cached LazyFrame - join + sort in single collect
            common_stocks_lf = get_common_stocks(filter_date_str)
            cs_tickers_df = (
                common_stocks_lf.join(lf, on="ticker", how="inner")
                .sort("percent_change", descending=True)
                .collect()
            )
        except Exception as e:
            print(f"Error filtering common stocks: {e}")
            cs_tickers_df = lf.sort("percent_change", descending=True).collect()

        # Fill missing data if needed
        if len(self.last_df) > 0 and len(self.last_df) != len(cs_tickers_df):
            filled_df = (
                pl.concat([self.last_df.lazy(), cs_tickers_df.lazy()], how="vertical")
                .sort("timestamp")
                .group_by(["ticker"])
                .agg(
                    pl.col("percent_change").last(),
                    pl.col("accumulated_volume").last(),
                    pl.col("current_price").last(),
                    pl.col("prev_close").last(),
                    pl.col("timestamp").last(),
                    pl.col("prev_volume").last(),
                )
                .sort("percent_change", descending=True)
                .collect()
            )
        else:
            logger.debug(f"df doesn't need fill, original length: {len(cs_tickers_df)}")
            filled_df = cs_tickers_df

        return filled_df


if __name__ == "__main__":
    rd = redis_engine()
    redis_thread = Thread(target=rd._redis_stream_listener, daemon=True)
    redis_thread.start()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")

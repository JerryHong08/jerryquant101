import json
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import redis
from dotenv import load_dotenv
from polygon import RESTClient
from polygon.rest.models import Agg, TickerSnapshot
from pydantic import ValidationError

from cores.config import cache_dir
from live_monitor.market_mover_monitor.core.data.schema import (
    spot_check_SnapshotMsg_with_pydantic,
    validate_SnapshotMsg_schema,
)

load_dotenv()


class MarketsnapshotCollector:
    def __init__(self):
        self.r = redis.Redis(host="localhost", port=6379, db=0)
        self.client = RESTClient(os.getenv("POLYGON_API_KEY"))

    def run_collector_engine(self):
        first_run = True

        while True:
            try:
                snapshot = self.client.get_snapshot_all("stocks", include_otc=False)

                # Process data
                data_list = []
                for item in snapshot:
                    if isinstance(item, TickerSnapshot):
                        if isinstance(item.day, Agg):
                            if (
                                isinstance(item.day.open, (float, int))
                                and isinstance(item.day.close, (float, int))
                                and float(item.prev_day.close) != 0
                            ):
                                percent_change = (
                                    (float(item.day.close) - float(item.prev_day.close))
                                    / float(item.prev_day.close)
                                    * 100
                                )
                                data_list.append(
                                    {
                                        "ticker": item.ticker,
                                        "percent_change": item.todays_change_percent,
                                        "accumulated_volume": float(
                                            item.min.accumulated_volume
                                        ),
                                        "current_price": float(item.min.close),
                                        "prev_close": float(item.prev_day.close),
                                        "timestamp": item.updated,
                                        "prev_volume": item.prev_day.volume,
                                    }
                                )

                if not data_list:
                    print("⚠️  No data collected from API")
                    time.sleep(5)
                    continue

                df = pl.DataFrame(data_list)
                df = df.with_columns(
                    (pl.col("timestamp") // 1_000_000).alias("timestamp")
                )

                # 1. Fast schema validation (every run)
                is_valid, error_msg = validate_SnapshotMsg_schema(df)
                if not is_valid:
                    print(f"❌ Schema validation failed: {error_msg}")
                    time.sleep(5)
                    continue

                # 2. Pydantic deep validation (first run or periodically)
                if first_run:
                    if not spot_check_SnapshotMsg_with_pydantic(df, sample_size=5):
                        print(f"❌ Pydantic validation failed on first run")
                        # time.sleep(5)
                        continue
                    print(f"✅ First run validation passed")
                    first_run = False

                print(f"✓ Validated {len(df)} rows")

                # Save snapshot
                market_mover_file = self.save_snapshot(df)

                # Publish to Redis
                payload = df.write_json()
                today = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d")
                STREAM_NAME = f"market_snapshot_stream:{today}"
                assert ":" in STREAM_NAME, "STREAM_NAME must include a date suffix!"

                message_id = self.r.xadd(STREAM_NAME, {"data": payload}, maxlen=10000)
                if self.r.ttl(STREAM_NAME) < 0:
                    self.r.expire(STREAM_NAME, 7 * 24 * 3600)

                print(
                    f"INFO: Published {len(df)} rows at {datetime.now(ZoneInfo('America/New_York'))}"
                )

                wait_duration = 5
                print(f"INFO: Waiting for {wait_duration}s")
                time.sleep(wait_duration)

            except Exception as e:
                print(f"❌ Error in collector loop: {e}")
                time.sleep(5)

    def save_snapshot(self, df: pl.DataFrame):
        """save into cache dir"""
        updated_time = datetime.now(ZoneInfo("America/New_York")).strftime(
            "%Y%m%d%H%M%S"
        )
        year = updated_time[:4]
        month = updated_time[4:6]
        date = updated_time[6:8]

        market_mover_dir = os.path.join(cache_dir, "market_mover", year, month, date)
        os.makedirs(market_mover_dir, exist_ok=True)
        market_mover_file = os.path.join(
            market_mover_dir, f"{updated_time}_market_snapshot.csv"
        )

        df.write_csv(market_mover_file)
        return market_mover_file


if __name__ == "__main__":
    collector = MarketsnapshotCollector()
    collector.run_collector_engine()

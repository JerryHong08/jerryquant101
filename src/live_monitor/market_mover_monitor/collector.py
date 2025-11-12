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

from cores.config import cache_dir

load_dotenv()
r = redis.Redis(host="localhost", port=6379, db=0)

client = RESTClient(os.getenv("POLYGON_API_KEY"))

while True:
    snapshot = client.get_snapshot_all("stocks", include_otc=False)

    # crunch some numbers
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
                            "accumulated_volume": float(item.min.accumulated_volume),
                            "current_price": float(item.min.close),
                            "prev_close": float(item.prev_day.close),
                            # "timestamp": item.min.timestamp,
                            "timestamp": item.updated,
                            "prev_volume": item.prev_day.volume,
                        }
                    )

    df = pl.DataFrame(data_list)
    df = df.with_columns((pl.col("timestamp") // 1_000_000).alias("timestamp"))

    # save into cache
    updated_time = datetime.now(ZoneInfo("America/New_York")).strftime(
        "%Y%m%d%H%M%S"
    )  # 20250930170603
    year = updated_time[:4]
    month = updated_time[4:6]
    date = updated_time[6:8]

    market_mover_dir = os.path.join(cache_dir, "market_mover", year, month, date)
    os.makedirs(market_mover_dir, exist_ok=True)
    market_mover_file = os.path.join(
        market_mover_dir, f"{updated_time}_market_snapshot.csv"
    )

    df.write_csv(market_mover_file)

    # turn into JSON publish to Redis
    payload = df.write_json()
    r.publish("market_snapshot", payload)
    print(f"Published {len(df)} rows at {datetime.now(ZoneInfo('America/New_York'))}")

    ## debug
    # df = df.with_columns(
    #     pl.from_epoch(
    #         pl.col('timestamp'), time_unit='ms'
    #     ).dt.convert_time_zone('America/New_York')
    # )
    # # Filter only common stocks and sort by percent_change
    # updated_time = datetime.now(ZoneInfo("America/New_York")).strftime(
    #     "%Y%m%d%H%M%S"
    # )
    # filter_date = (
    #     f"{updated_time[:4]}-{updated_time[4:6]}-{updated_time[6:8]}"
    # )

    # try:
    #     from utils.backtest_utils.backtest_pre_data import only_common_stocks
    #     df = (
    #         only_common_stocks(filter_date)
    #         .drop("active", "composite_figi")
    #         .join(df, on="ticker", how="inner")
    #         .sort("percent_change", descending=True)
    #     )
    # except Exception as e:
    #     print(f"Error filtering common stocks: {e}")
    #     # Fallback: just sort by percent_change
    #     df = df.sort("percent_change", descending=True)
    # # print(df.select('timestamp').head())
    # print(df.head())

    wait_duration = 5
    print(f"wait for{wait_duration}")
    time.sleep(wait_duration)

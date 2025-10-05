import os
import signal
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
from dotenv import load_dotenv
from polygon import RESTClient
from polygon.rest.models import Agg, TickerSnapshot

from backtesting.backtest_pre_data import only_common_stocks
from core_2.config import cache_dir
from utils.longbridge_utils import update_watchlist

market_mover_dir = os.path.join(cache_dir, "market_mover")
os.makedirs(market_mover_dir, exist_ok=True)

load_dotenv()

running = True


def signal_handler(sig, frame):
    global running
    print("\n\nReceived interrupt signal. Shutting down gracefully...")
    running = False


signal.signal(signal.SIGINT, signal_handler)

polygon_api_key_live = os.getenv("POLYGON_API_KEY")

client = RESTClient(polygon_api_key_live)

snapshot = client.get_snapshot_all("stocks", include_otc=False)

# crunch some numbers
data_list = []
for item in snapshot:
    # verify this is an TickerSnapshot
    if isinstance(item, TickerSnapshot):
        # verify this is an Agg
        if isinstance(item.day, Agg):
            # verify this is a float
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
                        "timestamp": item.min.timestamp,
                        "prev_volume": item.prev_day.volume,
                    }
                )

result = pl.DataFrame(data_list).with_columns(
    pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.convert_time_zone(
        "America/New_York"
    )
)

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

result.write_csv(market_mover_file)

filter_date = (
    updated_time[:4] + "-" + updated_time[4:6] + "-" + updated_time[6:8]
)  # 2025-09-30
selected_tickers = only_common_stocks(filter_date).drop("active", "composite_figi")
result = selected_tickers.join(result, on="ticker", how="inner").sort(
    "percent_change", descending=True
)


# with pl.Config(tbl_rows=20, tbl_cols=50):
#     print(result.head(20))

# top_20 = result.select("ticker").to_series().to_list()[:20]
# update_watchlist(watchlist_name="market_mover", tickers=top_20)

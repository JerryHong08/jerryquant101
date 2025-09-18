import os

import polars as pl
from dotenv import load_dotenv
from polygon import RESTClient
from polygon.rest.models import Agg, TickerSnapshot

from quant101.core_2.config import cache_dir
from quant101.strategies.pre_data import only_common_stocks

load_dotenv()

polygon_api_key_live = os.getenv("POLYGON_API_KEY")

client = RESTClient(polygon_api_key_live)

selected_tickers = only_common_stocks("2025-09-10").drop("active", "composite_figi")

snapshot = client.get_snapshot_all("stocks", include_otc=True)

# print(snapshot)

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
                        "prev_close": float(item.prev_day.close),
                        "close": float(item.day.close),
                        "percent_change": item.todays_change_percent,
                        # "percent_change": percent_change,
                        "open": float(item.day.open),
                    }
                )
                # print(
                #     "{:<15}{:<15}{:<15}{:.2f} %".format(
                #         item.ticker,
                #         item.prev_day.open,
                #         item.prev_day.close,
                #         percent_change,
                #     )
                # )

result = pl.DataFrame(data_list)

import time

result = selected_tickers.join(result, on="ticker", how="inner").sort(
    "percent_change", descending=True
)

updated_time = time.strftime("%Y%m%d%H%M", time.localtime())

market_mover_dir = os.path.join(cache_dir, "market_mover")
os.makedirs(market_mover_dir, exist_ok=True)
market_mover_file = os.path.join(
    cache_dir, "market_mover", f"{updated_time}_market_snapshot.csv"
)
result.write_csv(market_mover_file)
with pl.Config(tbl_rows=20, tbl_cols=50):
    print(result.head(20))

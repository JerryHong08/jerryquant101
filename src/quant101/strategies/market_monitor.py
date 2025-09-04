import os

import polars as pl
from dotenv import load_dotenv
from polygon import RESTClient
from polygon.rest.models import Agg, TickerSnapshot

load_dotenv()

polygon_api_key_live = os.getenv("POLYGON_API_KEY")

client = RESTClient(polygon_api_key_live)

snapshot = client.get_snapshot_all(
    "stocks",
    tickers=["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META"],
    include_otc="true",
)

# print(snapshot)

# crunch some numbers
result = pl.DataFrame()
for item in snapshot:
    # verify this is an TickerSnapshot
    if isinstance(item, TickerSnapshot):
        # verify this is an Agg
        if isinstance(item.prev_day, Agg):
            # verify this is a float
            if isinstance(item.prev_day.open, float) and isinstance(
                item.prev_day.close, float
            ):
                percent_change = (
                    (item.prev_day.close - item.prev_day.open)
                    / item.prev_day.open
                    * 100
                )
                ticker = pl.DataFrame(
                    {
                        "ticker": [item.ticker],
                        "open": [item.prev_day.open],
                        "close": [item.prev_day.close],
                        "percent_change": [percent_change],
                    }
                )

                result = pl.concat([result, ticker])
                # print(
                #     "{:<15}{:<15}{:<15}{:.2f} %".format(
                #         item.ticker,
                #         item.prev_day.open,
                #         item.prev_day.close,
                #         percent_change,
                #     )
                # )

import time

updated_time = time.strftime("%Y%m%d%H%M", time.localtime())
result.write_csv(f"{updated_time}_market_snapshot.csv")

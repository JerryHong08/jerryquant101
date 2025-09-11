import os

import dotenv
import polars as pl
from polygon import RESTClient
from polygon.rest.models import (
    TickerSnapshot,
)

dotenv.load_dotenv()

polygon_api_key = os.getenv("POLYGON_API_KEY")

client = RESTClient(polygon_api_key)

tickers = client.get_snapshot_direction("stocks", direction="gainers")
# quote = pl.DataFrame(quote)

# print(quote)

# print ticker with % change
for item in tickers:
    # verify this is a TickerSnapshot
    if isinstance(item, TickerSnapshot):
        # verify this is a float
        if isinstance(item.todays_change_percent, float):
            print("{:<15}{:.2f} %".format(item.ticker, item.todays_change_percent))

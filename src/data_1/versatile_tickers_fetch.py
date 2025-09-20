import os
import time

import polars as pl
from dotenv import load_dotenv
from polygon import RESTClient

from core_2.config import data_dir

load_dotenv()


class VersatileFetcher:
    def __init__(self):
        polygon_api_key = os.getenv("POLYGON_API_KEY")
        self.client = RESTClient(polygon_api_key)

        os.makedirs(data_dir, exist_ok=True)

    def fetch_spx_aggs(self):
        ticker = "I:SPX"
        start_ = "2015-01-01"
        end_ = "2025-09-05"
        timespan = "day"
        aggs = []
        for a in self.client.list_aggs(
            ticker,
            1,
            timespan,
            start_,
            end_,
            sort="asc",
            limit=50000,
        ):
            aggs.append(a)

        aggs = pl.DataFrame(aggs)

        aggs.write_parquet(
            f'{ticker}{timespan}{str(start_).replace("-", "")}_{str(end_).replace("-", "")}.parquet',
            compression="zstd",
            compression_level=3,
        )
        print(aggs.describe())


Downloader = VersatileFetcher()
Downloader.fetch_spx_aggs()

# tickers
# multiplier
# timespan

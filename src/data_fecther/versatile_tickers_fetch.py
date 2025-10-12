import os
import time
from datetime import datetime, timedelta

import polars as pl
from core_2.config import data_dir
from dotenv import load_dotenv
from polygon import RESTClient

load_dotenv()


class VersatileFetcher:
    def __init__(self):
        polygon_api_key = os.getenv("POLYGON_API_KEY")
        self.client = RESTClient(polygon_api_key)

        os.makedirs(data_dir, exist_ok=True)

    def fetch_tickers(
        self,
        tickers: list,
        start_date: str = None,
        end_date: str = None,
        timespan: str = "day",
    ):
        self.start_ = start_date
        self.end_ = (
            end_date if end_date else time.strftime("%Y-%m-%d", time.localtime())
        )
        self.timespan = timespan
        # for each ticker
        for ticker in tickers:
            self.ticker_file_path = f"{ticker}{timespan}.parquet"
            # initialize empty dataframe
            ticker_data = pl.DataFrame()
            # if start_date is not provided, which means likely updated from existing data or initialize parquet data first time
            if not self.start_:
                # updated from existing data
                if os.path.exists(self.ticker_file_path):
                    ticker_data = pl.read_parquet(self.ticker_file_path)
                    update_data = ticker_data.with_columns(
                        pl.from_epoch(pl.col("timestamp"), time_unit="ms")
                    ).sort("timestamp")
                    last_updated = update_data["timestamp"].max().strftime("%Y-%m-%d")
                    self.start_ = (
                        datetime.strptime(last_updated, "%Y-%m-%d") + timedelta(days=1)
                    ).strftime("%Y-%m-%d")
                    print(f"Start date set to {self.start_} based on existing data.")
                # initialize parquet data first time
                else:
                    self.start_ = "2022-12-31"  # polygon only provides indices data since 2023, you can set earlier date for stocks/options/crypto
                    print(
                        f"No existing data found for {ticker}. Start date set to {self.start_}."
                    )

            print(f"Fetching {ticker} data from {self.start_} to {self.end_}...")
            self.fetch_ticker_data(ticker, ticker_data)

    def fetch_ticker_data(self, ticker: str, ticker_data: pl.DataFrame = None):
        aggs = []
        for a in self.client.list_aggs(
            ticker,
            1,
            self.timespan,
            self.start_,
            self.end_,
            sort="asc",
            limit=50000,
        ):
            aggs.append(a)

        if not aggs:
            print(f"No data fetched for {ticker}.")
            return

        aggs = pl.DataFrame(aggs).select(
            pl.col("open"),
            pl.col("high"),
            pl.col("low"),
            pl.col("close"),
            pl.col("timestamp"),
        )

        aggs = aggs.with_columns(
            pl.from_epoch(pl.col("timestamp"), time_unit="ms")
            .dt.truncate("1d")
            .dt.epoch(time_unit="ms")
            .alias("timestamp")
        )

        if not ticker_data.is_empty():
            aggs = pl.concat([ticker_data, aggs]).unique(
                subset=["timestamp"], keep="last"
            )

            aggs.write_parquet(
                self.ticker_file_path,
                compression="zstd",
                compression_level=3,
            )
            print(f"Incremental update data for {ticker}.")
        else:
            aggs.write_parquet(
                self.ticker_file_path,
                compression="zstd",
                compression_level=3,
            )
            print(f"Initial data for {ticker}.")

        with pl.Config(tbl_cols=20):
            aggs = aggs.with_columns(
                pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.convert_time_zone(
                    "UTC"
                )
            ).sort("timestamp")

            print(aggs.head())
            print(aggs.tail())


tickers = [
    "I:SPX",
    "I:IRX",
]

Downloader = VersatileFetcher()
Downloader.fetch_tickers(tickers)

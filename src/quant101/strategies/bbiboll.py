import datetime
import glob
import json
import os
import sys

import polars as pl

from quant101.core_2.config import all_tickers_dir
from quant101.core_2.data_loader import data_loader
from quant101.core_2.plotter import plot_candlestick
from quant101.strategies.indicators.bbiboll_indicator import calculate_bbiboll

if __name__ == "__main__":
    # tickers = ['NVDA','TSLA','FIG']
    tickers = None
    timeframe = "1d"
    asset = "us_stocks_sip"
    data_type = "day_aggs_v1" if timeframe == "1d" else "minute_aggs_v1"
    start_date = "2022-01-01"
    end_date = "2025-08-04"
    full_hour = False

    all_tickers_file = os.path.join(all_tickers_dir, f"all_tickers_*.parquet")
    all_tickers = pl.read_parquet(all_tickers_file)

    # 回溯到end_date当天还未delist的ticker都算active
    tickers = (
        all_tickers.filter(
            (pl.col("type").is_in(["CS", "ADRC"]))
            & (
                (pl.col("active") == True)
                | (
                    pl.col("delisted_utc").is_not_null()
                    & (
                        pl.col("delisted_utc")
                        .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ", strict=False)
                        .dt.date()
                        > datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
                    )
                )
            )
        )
        .select(pl.col("ticker"))
        .to_series()
        .to_list()
    )
    print(f"Using {all_tickers_file}, total {len(tickers)} active tickers")

    lf_result = data_loader(
        tickers=tickers,
        timeframe=timeframe,
        asset=asset,
        data_type=data_type,
        start_date=start_date,
        end_date=end_date,
        full_hour=full_hour,
    ).collect()

    # 只选择当天有成交记录的股票进行计算
    last_date = lf_result.select(pl.col("timestamps")).max().item()
    tickers_with_data = (
        lf_result.filter(pl.col("timestamps") == last_date)
        .select(pl.col("ticker"))
        .unique()
    )
    # print(tickers_with_data)

    bbiboll = calculate_bbiboll(
        lf_result.filter(pl.col("ticker").is_in(tickers_with_data["ticker"].to_list()))
    ).with_columns((pl.col("volume") * pl.col("close")).alias("turnover"))

    result = (
        # bbiboll.filter(pl.col('ticker') == 'NVDA' & pl.col('bbi').is_not_null())
        bbiboll.filter(
            (
                pl.col("timestamps").dt.date()
                == datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
            )
            & pl.col("bbi").is_not_null()
            & (pl.col("dev_pct") <= 1)
        ).select(
            [
                col
                for col in bbiboll.columns
                if col
                not in [
                    "open",
                    "high",
                    "low",
                    "split_date",
                    "split_ratio",
                    "window_start",
                ]
            ]
        )
        # .sort('timestamps')
        .sort(["dev_pct", "turnover"], descending=[False, True])
    )

    result = result.join(
        all_tickers.select(["ticker", "type", "primary_exchange", "active"]),
        on="ticker",
        how="left",
    )

    with pl.Config(tbl_cols=20, tbl_rows=500):
        print(result)

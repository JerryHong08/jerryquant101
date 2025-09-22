import numpy as np
import pandas as pd
import polars as pl

from core_2.data_loader import stock_load_process
from strategies.pre_data import only_common_stocks
from utils.longbridge_utils import update_watchlist

config = {
    "timeframe": "1d",
    "start_date": "2022-01-01",
    "end_date": "2025-09-05",
}

tickers = only_common_stocks(filter_date=config["start_date"])

ohlcv_data = (
    stock_load_process(
        tickers=tickers.to_series().to_list(),
        timeframe=config["timeframe"],
        start_date=config["start_date"],
        end_date=config["end_date"],
        # use_cache=False,
    )
    # .collect()
)

ohlcv_data = ohlcv_data.with_columns(pl.col("timestamps").dt.date().alias("timestamps"))

low_volume_threshold = 0

low_activity_ticker = (
    ohlcv_data.filter(pl.col("volume") <= low_volume_threshold)
    # .select(
    #     pl.col("ticker"),
    #     pl.col('timestamps')
    # )
)

duration = low_activity_ticker.sort(["ticker", "timestamps"])
duration = duration.with_columns(
    (pl.col("timestamps").diff().dt.total_days().fill_null(999) > 5)
    .cum_sum()
    .over("ticker")
    .alias("block_id")
)

duration = (
    duration.group_by(["ticker", "block_id"])
    .agg(
        [
            pl.col("timestamps").min().alias("start_date"),
            pl.col("timestamps").max().alias("end_date"),
            # pl.col('').max().alias('max_block_id'),
        ]
    )
    .with_columns(
        (
            (
                pl.col("end_date").dt.date() - pl.col("start_date").dt.date()
            ).dt.total_days()
            + 1
        ).alias("duration_days")
    )
    .join(
        ohlcv_data.filter(pl.col("volume") != low_volume_threshold)
        .with_columns(
            (pl.col("volume") * pl.col("close"))
            .mean()
            .over("ticker")
            .alias("avg_turnover")
            .cast(pl.Int128)
        )
        .select(["ticker", "avg_turnover"])
        .unique(),
        on="ticker",
        how="left",
    )
)

with pl.Config(tbl_rows=20, tbl_cols=50):
    print(duration.filter(pl.col("ticker") == "BURU").collect())

duration = (
    duration.group_by("ticker").agg(
        [
            pl.col("duration_days").max().alias("max_duration_days"),
            pl.col("start_date")
            .filter(pl.col("duration_days") == pl.col("duration_days").max())
            .first()
            .alias("max_duration_start_date"),
            pl.col("end_date")
            .filter(pl.col("duration_days") == pl.col("duration_days").max())
            .first()
            .alias("max_duration_end_date"),
            pl.col("avg_turnover").first(),
        ]
    )
    # .sort(
    #     ["avg_turnover", "max_duration_days", "ticker"], descending=[True, True, False]
    # )
    .sort(["max_duration_days", "ticker"], descending=[True, False])
)

result = (
    duration
    # .filter(pl.col('duration_days') == 1)
).collect()

with pl.Config(tbl_rows=20, tbl_cols=50):
    # result.write_csv("low_volume_tickers.csv")
    print(result.to_series().to_list())
    print(result.select("ticker").unique().height)

update_watchlist(
    wachlist_name="quant101_watchlist", tickers=result.to_series().to_list()[:40]
)

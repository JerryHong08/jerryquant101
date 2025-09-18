import os

import polars as pl

from quant101.core_2.config import all_indices_dir, all_tickers_dir, splits_data
from quant101.core_2.data_loader import data_loader, figi_alignment, stock_load_process
from quant101.core_2.plotter import plot_candlestick
from quant101.strategies.pre_data import only_common_stocks

all_stocks_file = os.path.join(all_tickers_dir, f"all_stocks_*.parquet")
all_otc_file = os.path.join(all_tickers_dir, f"all_otc_*.parquet")
all_indices_file = os.path.join(all_indices_dir, f"all_indices_*.parquet")

all_tickers = pl.read_parquet(all_stocks_file)
all_indices = pl.read_parquet(all_indices_file)
all_otc = pl.read_parquet(all_otc_file)

tickers = all_tickers

tickers = (
    tickers
    # .filter(
    #     (pl.col('type').is_in(['CS', 'ADRC']))
    # )
    .select(
        [
            "ticker",
            "active",
            "delisted_utc",
            "last_updated_utc",
            "share_class_figi",
            "cik",
            "type",
            "primary_exchange",
            "composite_figi",
        ]
    )
    .sort("ticker")
    .filter(pl.col("composite_figi").is_not_null())
    .with_columns(
        pl.col("ticker").count().over("composite_figi").alias("figi_ticker_count")
    )
    .filter(pl.col("figi_ticker_count") > 1)
    .group_by("composite_figi")
    .agg(
        # pl.col('figi_ticker_count').first(),
        pl.all()
    )
    .sort("figi_ticker_count", descending=True)
    # .filter(pl.col('count') == 1)
)

with pl.Config(tbl_rows=50, tbl_cols=50):
    print(tickers.head(50))

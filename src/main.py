import os

import polars as pl

from utils.data_utils.path_loader import DataPathFetcher

path_fetcher = DataPathFetcher(
    asset="us_stocks_sip",
    data_type="minute_aggs_v1",
    start_date="2025-01-01",
    end_date="2025-10-15",
    lake=True,
)
paths = path_fetcher.data_dir_calculate()

if not paths:
    raise ValueError(f"No data files found.")

tickers = "NVDA"
tickers = [t.strip().upper() for t in tickers.split(",")] if tickers else None
if tickers:
    lf = pl.scan_parquet(paths).filter(pl.col("ticker").is_in(tickers))
else:
    lf = pl.scan_parquet(paths)

lf = lf.collect(engine=pl.GPUEngine(raise_on_fail=True))
# lf = lf.with_columns(
#     pl.from_epoch(pl.col('window_start'), time_unit='ns')
#     .dt.convert_time_zone('America/New_York')
#     .alias('timestamps')
# # ).sort('timestamps').collect()
# ).sort('timestamps')
with pl.Config(tbl_cols=10):
    print(lf.head())

# import polars as pl

# # Create a LazyFrame
# ldf = pl.LazyFrame({"numbers": [1.242, 1.535]})

# # Build a query
# query = ldf.select(pl.col("numbers").round(1))

# # Collect with GPU engine
# result = query.collect(engine="gpu")
# print(result)

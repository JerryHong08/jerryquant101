import datetime
import os

import duckdb
import polars as pl
from dotenv import load_dotenv

from quant101.core_2.config import all_indices_dir, all_tickers_dir, splits_data
from quant101.core_2.data_loader import data_loader, figi_alignment, stock_load_process
from quant101.core_2.plotter import plot_candlestick
from quant101.strategies.pre_data import only_common_stocks

# con = duckdb.connect()

# # query ="""
# #     SELECT * FROM read_csv_auto('backtest_output/BBIBOLL_trades.csv')
# # """

# query = """
#     SELECT * FROM read_parquet('I:SPXday20150101_20250905.parquet')
# """

# df = con.execute(query).fetchdf()
# df = pl.from_pandas(df)

# df = df.with_columns(
#     pl.from_epoch(pl.col("timestamp"), time_unit="ms")
#     .dt.convert_time_zone("America/New_York")
#     .dt.replace(hour=0, minute=0, second=0)
#     .cast(pl.Datetime("ns", "America/New_York"))
#     .alias("date")
# )
# with pl.Config(tbl_rows=5, tbl_cols=20):
#     print(df)

# lf = stock_load_process(
#     # tickers=tickers.to_series().to_list(),
#     tickers=['DVLT'],
#     start_date="2015-01-01",
#     end_date="2025-09-05",
# ).collect()

# print(lf.filter(
#     (pl.col('ticker') == 'DVLT')
#     & (pl.col('timestamps').dt.date() >= pl.datetime(2024, 4, 13))
# ).head(20))

# lf = lf.filter(pl.col('volume') == 0)
# plot_candlestick(lf.to_pandas(), 'DVLT', '1d')


# splits = figi_alignment(splits_data.lazy()).collect()
# splits = splits_data.filter(
#     pl.col('ticker').is_in(['ABC'])
# )

all_stocks_file = os.path.join(all_tickers_dir, f"all_stocks_*.parquet")
all_otc_file = os.path.join(all_tickers_dir, f"all_otc_*.parquet")
all_indices_file = os.path.join(all_indices_dir, f"all_indices_*.parquet")

all_tickers = pl.read_parquet(all_stocks_file)
all_indices = pl.read_parquet(all_indices_file)
all_otc = pl.read_parquet(all_otc_file)

# print(all_tickers.columns)
# print(all_indices.columns)
tickers = pl.concat([all_tickers, all_otc], how="vertical")

result = splits_data.filter(pl.col("ticker").count().over("ticker") > 1).sort(
    "ticker", "execution_date", descending=[False, True]
)

print(result.shape)
# 检查 tickers 表中是否有重复的 ticker

print("tickers 中重复的 ticker:")
duplicate_tickers = (
    tickers.filter(pl.col("ticker").count().over("ticker") > 1)
    .select(
        [
            "ticker",
            "active",
            "delisted_utc",
            "type",
            "primary_exchange",
            "composite_figi",
        ]
    )
    .sort("ticker")
    # .filter(pl.col('count') == 1)
)
duplicate_tickers.write_csv("original_tickers.csv")

without_duplicate = duplicate_tickers.filter(pl.col("composite_figi").is_null())
without_duplicate.write_csv("figinull_tickers.csv")

ticker_count = duplicate_tickers.group_by("ticker").agg(
    pl.col("composite_figi").n_unique().alias("count")
)

duplicate_tickers = duplicate_tickers.join(ticker_count, on="ticker", how="left")

duplicate_tickers = duplicate_tickers.filter(
    (pl.col("count") > 1)
    & (
        pl.col("delisted_utc").str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ").dt.date()
        >= pl.date(2015, 1, 1)
    )
)

duplicate_tickers.write_csv("duplicate_tickers.csv")
# print(f"重复 ticker 数量: {duplicate_tickers.shape[0]}")

result = result.join(
    tickers.select(["ticker", "active", "delisted_utc", "type", "primary_exchange"]),
    on="ticker",
    how="left",
)

# 检查 result 中的 ticker 是否在重复的 ticker 列表中
overlapping = result.filter(
    pl.col("ticker").is_in(duplicate_tickers["ticker"].implode())
)
print(f"result 中受影响的记录数: {overlapping.shape[0]}")

result.write_csv("splits_issues.csv")
print(result.shape)

# print(all_otc.filter(pl.col('ticker').is_in(['AABB'])))

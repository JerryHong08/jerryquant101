import os

import duckdb
import polars as pl
from dotenv import load_dotenv

from quant101.core_2.config import all_tickers_dir
from quant101.core_2.data_loader import data_loader, figi_alignment, stock_load_process
from quant101.core_2.plotter import plot_candlestick

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

lf = stock_load_process(
    tickers=None,
    # lf = data_loader(
    start_date="2025-01-01",
    end_date="2025-09-05",
)

prev_tickers = lf.select("ticker").unique().collect()

aligned_lf = figi_alignment(lf)
# aligned_lf = figi_alignment(aligned_lf)

after_tickers = aligned_lf.select("ticker").unique().collect()

diff_tickers = prev_tickers.join(after_tickers, on="ticker", how="anti")

all_tickers_file = os.path.join(all_tickers_dir, f"all_tickers_*.parquet")
all_tickers = pl.read_parquet(all_tickers_file).lazy()

with pl.Config(tbl_rows=30, tbl_cols=20):
    print("lf count:", len(lf.select("ticker").unique().collect()))
    print("aligned lf count:", len(aligned_lf.select("ticker").unique().collect()))
    print("diff tickers count:", diff_tickers)
    print(
        all_tickers.filter(
            # (~pl.col('composite_figi').is_unique())
            # & (pl.col('composite_figi').is_not_null())
            # & ((pl.col('composite_figi').is_null()) | (pl.col('share_class_figi').is_null()) )
            # & (pl.col('cik') == '0001805521')
            # & (pl.col('ticker').is_in(['FFAI', 'FFIE']))
            (pl.col("ticker").is_in(diff_tickers["ticker"].to_list()))
            & (pl.col("active"))
        )
        .sort("composite_figi")
        .collect()
    )

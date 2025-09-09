import os

import duckdb
import polars as pl
from dotenv import load_dotenv

from quant101.core_2.data_loader import data_loader

con = duckdb.connect()

# query ="""
#     SELECT * FROM read_csv_auto('backtest_output/BBIBOLL_trades.csv')
# """

query = """
    SELECT * FROM read_parquet('I:SPXday20150101_20250905.parquet')
"""

df = con.execute(query).fetchdf()
df = pl.from_pandas(df)

df = df.with_columns(
    pl.from_epoch(pl.col("timestamp"), time_unit="ms")
    .dt.convert_time_zone("America/New_York")
    .dt.replace(hour=0, minute=0, second=0)
    .cast(pl.Datetime("ns", "America/New_York"))
    .alias("date")
)
with pl.Config(tbl_rows=5, tbl_cols=20):
    print(df)

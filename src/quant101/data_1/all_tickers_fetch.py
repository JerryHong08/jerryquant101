import os

import polars as pl
from dotenv import load_dotenv
from polygon import RESTClient

from quant101.core_2.config import all_tickers_dir

os.makedirs(all_tickers_dir, exist_ok=True)

load_dotenv()
import time

updated_time = time.strftime("%Y%m%d", time.localtime())
all_tickers_file = os.path.join(all_tickers_dir, f"all_tickers_{updated_time}.parquet")

if os.path.exists(all_tickers_file):
    print(f"{all_tickers_file} already exists.")
else:
    polygon_api_key = os.getenv("POLYGON_API_KEY")
    client = RESTClient(polygon_api_key)

    active_tickers = []
    for t in client.list_tickers(
        market="stocks",
        active="True",
        order="asc",
        limit="1000",
        sort="ticker",
    ):
        active_tickers.append(t)

    deactive_tickers = []
    for t in client.list_tickers(
        market="stocks",
        active="False",
        order="asc",
        limit="1000",
        sort="ticker",
    ):
        deactive_tickers.append(t)

    all_tickers = active_tickers + deactive_tickers
    all_tickers = pl.DataFrame(all_tickers)

    # delete last updated all_ticker file(s)
    for f in os.listdir(all_tickers_dir):
        if f.startswith("all_tickers_") and f.endswith(".parquet"):
            os.remove(os.path.join(all_tickers_dir, f))

    all_tickers.write_parquet(all_tickers_file, compression="snappy")
    print(f"Saved {all_tickers_file}")

all_tickers = pl.read_parquet(all_tickers_file)
with pl.Config(tbl_rows=50, tbl_cols=20):
    print(all_tickers.shape)
    # print(all_tickers.describe())
    print(
        all_tickers.filter(
            (pl.col("active") == True)
            # & (pl.col('type').is_in(['ADRC', 'CS', 'ETF', 'ETN', 'ETS', 'ETV']))
        ).select("ticker")
    )

# import sys

# tickers_from_daily = [
#     line.strip()
#     for line in sys.stdin
#     if line.strip() and not any(ch.isdigit() for ch in line.strip())
# ]
# df_daily = pl.DataFrame({"ticker": tickers_from_daily})
# df_all = all_tickers.filter(
#     (pl.col("active")) & (pl.col("type").is_in(["ADRC", "CS"]))
# ).select("ticker")

# with pl.Config(tbl_rows=400, tbl_cols=20):
#     print("only in all:", df_all.join(df_daily, on="ticker", how="anti"))
#     print("only in daily:", df_daily.join(df_all, on="ticker", how="anti"))

# python src/quant101/core_2/data_loader.py | python src/quant101/data_1/all_tickers_fetch.py

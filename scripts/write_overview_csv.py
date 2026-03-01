import polars as pl

from config import get_asset_overview_data

stock_df = get_asset_overview_data(asset="stocks")
otc_df = get_asset_overview_data(asset="otc")

df = pl.concat([stock_df, otc_df])

df.write_csv("original_stock&otc.csv")

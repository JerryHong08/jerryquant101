import polars as pl

from core_2.config import splits_data

strategy_name = "BBIBOLL"

trades = pl.read_csv(f"backtest_output/{strategy_name}/{strategy_name}_trades.csv")

with pl.Config(tbl_rows=20, tbl_cols=10):
    # print(trades.head())
    print(
        splits_data.filter(
            pl.col("ticker").is_in(
                trades.select("ticker").unique().to_series().to_list()
            )
        )
    )

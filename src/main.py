import polars as pl

df = pl.read_csv("tickers_name_alignment.csv", schema_overrides={"group_id": pl.String})

df = df.filter((pl.col("ticker")).is_duplicated()).unique(subset=["ticker"])

print(df)
# print(df.tail())

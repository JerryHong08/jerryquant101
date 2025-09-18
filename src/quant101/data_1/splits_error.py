import os
import time

import polars as pl

splits_dir = "data/raw/us_stocks_sip/splits/splits.parquet"
splits_error_dir = "data/raw/us_stocks_sip/splits/splits_error.parquet"
splits_error_dir_copy = "src/quant101/data_1/splits_error.parquet"

os.makedirs(os.path.dirname(splits_dir), exist_ok=True)
os.makedirs(os.path.dirname(splits_error_dir), exist_ok=True)

splits_original = pl.read_parquet(splits_dir)
splits_errors = pl.read_parquet(splits_error_dir)

# splits_error_file = splits_error_file.head(0)

splits_new_error = splits_original.filter(
    (pl.col("ticker") == "XXX") & (pl.col("execution_date") == "yyyy-mm-dd")
)

splits_errors = splits_errors.vstack(splits_new_error).unique(subset=["id"])

splits_original = splits_original.filter(
    ~pl.col("id").is_in(splits_errors["id"].implode())
)

splits_errors.write_parquet(splits_error_dir)
splits_errors.write_parquet(splits_error_dir_copy)

with pl.Config(tbl_rows=100, tbl_cols=10, fmt_str_lengths=100):
    print(splits_errors)


# DXF 2024-12-04 1:125 merge not included, need to add

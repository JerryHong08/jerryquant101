import os

import polars as pl

blackdisk_data_dir = "/mnt/blackdisk/quant_data/polygon_data"
nftsdisk_data_dir = "data/polygon_data"
data_dir = blackdisk_data_dir

splits_dir = os.path.join(data_dir, "raw/us_stocks_sip/splits/splits.parquet")
splits_error_dir = os.path.join(
    data_dir, "raw/us_stocks_sip/splits/splits_error.parquet"
)

splits_original = pl.read_parquet(splits_dir)
splits_errors = pl.read_parquet(splits_error_dir)
splits_data = splits_original.filter(~pl.col("id").is_in(splits_errors["id"].implode()))

all_tickers_dir = os.path.join(data_dir, "raw/us_stocks_sip/us_all_tickers")
all_indices_dir = os.path.join(data_dir, "raw/us_indices/us_all_indices")

sppc_dir = "/mnt/blackdisk/quant_data/kaggle_data/sppc"

import os

import polars as pl

blackdisk_data_dir = "/mnt/blackdisk/quant_data/polygon_data"
nftsdisk_data_dir = "data/polygon_data"
data_dir = blackdisk_data_dir

splits_dir = os.path.join(data_dir, "raw/us_stocks_sip/splits")
splits_error_file_copy = os.path.join(
    data_dir, "raw/us_stocks_sip/splits/splits_error.csv"
)
splits_error_file = "src/data_1/data_discrepancy_fixed/splits_error.csv"
try:
    splits_file = os.path.join(
        splits_dir,
        max(
            [
                f
                for f in os.listdir(splits_dir)
                if f.startswith("all_splits_") and f.endswith(".parquet")
            ]
        ),
    )
    splits_original = pl.read_parquet(splits_file)
    splits_errors = pl.read_csv(splits_error_file)
    splits_errors.write_csv(splits_error_file_copy)  # make a copy
    error_type_remove = splits_errors.filter(pl.col("error_type") == "remove")
    error_type_add = splits_errors.filter(pl.col("error_type") == "add").select(
        pl.all().exclude("error_type")
    )

    splits_data = pl.concat(
        [
            splits_original.filter(
                ~pl.col("id").is_in(error_type_remove["id"].implode())
            ),
            error_type_add,
        ]
    )

except (ValueError, FileNotFoundError, OSError):
    splits_file = None
    splits_original = pl.DataFrame()
    splits_errors = pl.DataFrame()
    splits_data = pl.DataFrame()

all_tickers_dir = os.path.join(data_dir, "raw/us_stocks_sip/us_all_tickers")
all_indices_dir = os.path.join(data_dir, "raw/us_indices/us_all_indices")

cache_dir = os.path.join(data_dir, "processed")

sppc_dir = "/mnt/blackdisk/quant_data/kaggle_data/sppc"

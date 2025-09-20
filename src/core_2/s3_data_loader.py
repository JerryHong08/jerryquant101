import os
from datetime import datetime

import duckdb
import polars as pl
import s3fs
from dotenv import load_dotenv

from core_2.data_loader import data_loader

load_dotenv()

ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

# s3_data_dir .csv.gz polars :6m39s
# local_data_dir .csv.gz polars :6s
# local_data_dir .parquet polars :3s

# s3_data_dir .csv.gz duckdb :7m29s
# local_data_dir .csv.gz duckdb :13s
# local_data_dir .parquet duckdb :0s

lake_file_paths = data_loader(
    asset="us_stocks_sip",
    data_type="day_aggs_v1",
    start_date="2022-01-01",
    end_date="2025-09-05",
    use_s3=True,
)


print(f"Found files: {lake_file_paths}")

if lake_file_paths:
    lf = pl.scan_csv(
        lake_file_paths,
        storage_options={
            "aws_access_key_id": ACCESS_KEY_ID,
            "aws_secret_access_key": SECRET_ACCESS_KEY,
            "aws_endpoint": "https://files.polygon.io",
            "aws_region": "us-east-1",
        },
    )
    print(lf.collect().head())
    # result = lf.filter(pl.col("volume") == pl.max("volume")).collect()
    # print(result)
else:
    print("No files found!")

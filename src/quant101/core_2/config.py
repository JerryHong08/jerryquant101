blackdisk_data_dir = "/mnt/blackdisk/quant_data/polygon_data/"
nftsdisk_data_dir = "data/polygon_data/"
data_dir = blackdisk_data_dir

import glob
import os
from datetime import datetime

import polars as pl


def data_dir_calculate(
    asset: str, data_type: str, start_date: str, end_date: str, lake: bool = True
):
    """
    Calculate data directory paths based on asset type, timeframe, date range.

    Args:
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: End date in format 'YYYY-MM-DD'

    Returns:
        List of parquet file paths
    """

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    data_dirs = []

    for y in range(start_dt.year, end_dt.year + 1):
        start_month = start_dt.month if y == start_dt.year else 1
        end_month = end_dt.month if y == end_dt.year else 12

        for m in range(start_month, end_month + 1):
            month_str = f"{m:02d}"  # Format as two digits
            file_pattern = f"{data_dir}{'lake' if lake else 'raw'}/{asset}/{data_type}/{y}/{month_str}/*.{'parquet' if lake else 'csv.gz'}"
            month_all_file = glob.glob(file_pattern)
            if m in [start_month, end_month]:
                filtered_files = []
                for file in month_all_file:
                    _, file_name = os.path.split(file)
                    file_name = file_name.split(".")[0]
                    try:
                        file_date = datetime.strptime(file_name, "%Y-%m-%d").date()
                    except ValueError:
                        continue  # skip files that don't match the date format
                    if (m == start_month and file_date < start_dt.date()) or (
                        m == end_month and file_date > end_dt.date()
                    ):
                        continue  # skip files outside the range
                    filtered_files.append(file)
                data_dirs.extend(filtered_files)
            else:
                data_dirs.extend(month_all_file)
    return data_dirs


if __name__ == "__main__":
    from quant101.core_2.data_loader import generate_full_timestamp

    start_date = "2015-01-01"
    end_date = "2025-08-19"
    timeframe = "1d"
    lake_file_paths = data_dir_calculate(
        asset="us_stocks_sip",
        data_type="day_aggs_v1",
        start_date=start_date,
        end_date=end_date,
        # lake=False
    )
    lf = pl.scan_parquet(
        lake_file_paths,
    )
    lf = lf.with_columns(
        pl.from_epoch(pl.col("window_start"), time_unit="ns")
        .dt.convert_time_zone("America/New_York")
        .alias("timestamps")
    ).collect()
    print(f"\nFound {len(lake_file_paths)} parquet files")
    generated_timestamp = generate_full_timestamp(start_date, end_date, timeframe)
    print(generated_timestamp.shape)
    print(generated_timestamp.head())

    diff1 = generated_timestamp.filter(
        ~pl.col("timestamps").is_in(lf["timestamps"].implode())
    )
    diff2 = lf.filter(
        ~pl.col("timestamps").is_in(generated_timestamp["timestamps"].implode())
    )
    with pl.Config(tbl_rows=40):
        print(diff1, diff2)

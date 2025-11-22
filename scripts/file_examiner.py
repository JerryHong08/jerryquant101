import glob
import os
import re
from collections import defaultdict
from datetime import datetime

import polars as pl

from cores.config import data_dir

print(data_dir)
#  /mnt/blackdisk/quant_data/polygon_data


#  /mnt/blackdisk/quant_data/polygon_data/raw
# > tree -L 2
# .
# ├── global_crypto
# │   └── minute_aggs_v1
# ├── us_indices
# │   ├── day_aggs_v1
# │   ├── minute_aggs_v1
# │   └── us_all_indices
# │     └── all_indices_20251012.parquet
# ├── us_options_opra
# │   ├── day_aggs_v1
# │   ├── minute_aggs_v1
# │   ├── quotes_v1
# │   └── trades_v1
# └── us_stocks_sip
#     ├── day_aggs_v1
#     ├── minute_aggs_v1
#     ├── splits
#     │   └── all_splits_20251012.parquet
#     │   └── splits_error.csv
#     └── us_all_tickers
#       └── all_tickers_20251012.parquet
#       └── all_otc_20251012.parquet
#       └── stocks_error.csv


#  /mnt/blackdisk/quant_data/polygon_data/lake
# > tree -L 2
# .
# ├── us_options_opra
# │   └── trades_v1
# └── us_stocks_sip
#     ├── day_aggs_v1
#     └── minute_aggs_v1

# for every data_type in assets, the structure is the same,
# which is <data_type>/<year>/<month>/yearmonthdate.parquet, for example:
# us_stocks_sip/minute_aggs_v1/2023/10/20231001.parquet


def get_overview_data_info(asset_path):
    """Check the latest update date of overview data"""
    overview_info = {}

    overview_dirs = {
        "us_all_indices": "us_all_indices",
        "splits": "splits",
        "us_all_tickers": "us_all_tickers",
    }

    for overview_dir in overview_dirs:
        overview_path = os.path.join(asset_path, overview_dir)
        if os.path.exists(overview_path):
            # Find all data files (not just parquet)
            data_files = glob.glob(os.path.join(overview_path, "*"))
            data_files = [
                f for f in data_files if os.path.isfile(f) and not f.endswith(".csv")
            ]  # exclude error files

            dates = []
            file_types = set()

            for file in data_files:
                filename = os.path.basename(file)
                # Extract file extension
                file_ext = os.path.splitext(filename)[1]
                if file_ext:
                    file_types.add(file_ext)

                # Extract date
                date_match = re.search(r"(\d{8})", filename)
                if date_match:
                    dates.append(date_match.group(1))

            if dates:
                latest_date = max(dates)
                file_types_str = (
                    ", ".join(sorted(file_types)) if file_types else "unknown"
                )
                overview_info[overview_dir] = {
                    "latest_date": latest_date,
                    "file_types": file_types_str,
                }

    return overview_info


def get_time_range_and_file_types(data_type_path):
    """Get time range and file types for a data type"""
    years = []
    for year_dir in glob.glob(os.path.join(data_type_path, "*")):
        if os.path.isdir(year_dir):
            year = os.path.basename(year_dir)
            if year.isdigit() and len(year) == 4:
                years.append(int(year))

    if not years:
        return None, None, set()

    min_year = min(years)
    max_year = max(years)

    earliest_date = None
    latest_date = None
    file_types = set()

    # Check earliest date in minimum year
    min_year_path = os.path.join(data_type_path, str(min_year))
    if os.path.exists(min_year_path):
        for month in sorted(os.listdir(min_year_path)):
            month_path = os.path.join(min_year_path, month)
            if os.path.isdir(month_path):
                # Find all data files
                all_files = glob.glob(os.path.join(month_path, "*"))
                data_files = [f for f in all_files if os.path.isfile(f)]

                if data_files:
                    dates_in_month = []
                    for file in data_files:
                        filename = os.path.basename(file)

                        # Collect file extensions
                        file_ext = os.path.splitext(filename)[1]
                        if filename.endswith(".csv.gz"):
                            file_types.add(".csv.gz")
                        elif file_ext:
                            file_types.add(file_ext)

                        # Extract date from file name: YYYY-MM-DD or YYYYMMDD
                        date_match = re.search(r"(\d{4})-?(\d{2})-?(\d{2})", filename)
                        if date_match:
                            year_str, month_str, day_str = date_match.groups()
                            date_str = f"{year_str}{month_str}{day_str}"
                            dates_in_month.append(date_str)

                    if dates_in_month:
                        earliest_date = min(dates_in_month)
                        break

                if earliest_date:
                    break

    # Check latest date in maximum year
    max_year_path = os.path.join(data_type_path, str(max_year))
    if os.path.exists(max_year_path):
        for month in sorted(os.listdir(max_year_path), reverse=True):
            month_path = os.path.join(max_year_path, month)
            if os.path.isdir(month_path):
                # Find all data files
                all_files = glob.glob(os.path.join(month_path, "*"))
                data_files = [f for f in all_files if os.path.isfile(f)]

                if data_files:
                    dates_in_month = []
                    for file in data_files:
                        filename = os.path.basename(file)

                        # Collect file extensions (if not already collected)
                        file_ext = os.path.splitext(filename)[1]
                        if filename.endswith(".csv.gz"):
                            file_types.add(".csv.gz")
                        elif file_ext:
                            file_types.add(file_ext)

                        # Extract date from file name: YYYY-MM-DD or YYYYMMDD
                        date_match = re.search(r"(\d{4})-?(\d{2})-?(\d{2})", filename)
                        if date_match:
                            year_str, month_str, day_str = date_match.groups()
                            date_str = f"{year_str}{month_str}{day_str}"
                            dates_in_month.append(date_str)

                    if dates_in_month:
                        latest_date = max(dates_in_month)
                        break

                if latest_date:
                    break

    return earliest_date, latest_date, file_types


def get_file_n_and_size(data_type_path):
    """Get number of files and directory size for a data type"""
    total_files = 0
    total_size = 0
    avg_size = 0

    if not os.path.exists(data_type_path):
        return 0, "0 B"

    # Walk through all subdirectories and files
    for root, dirs, files in os.walk(data_type_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                file_size = os.path.getsize(file_path)
                total_size += file_size
                total_files += 1
            except (OSError, IOError):
                # Skip files that can't be accessed
                continue

    # Format size in human readable format
    def format_size(size_bytes):
        """Format bytes into human readable format"""
        if size_bytes == 0:
            return "0 B"

        size_names = ["B", "KB", "MB", "GB", "TB", "PB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1

        return f"{size_bytes:.1f} {size_names[i]}"

    if total_files > 0:
        avg_size = total_size / total_files
        fomate_avg_size = format_size(avg_size)
    else:
        fomate_avg_size = "0 B"

    formatted_size = format_size(total_size)
    return total_files, formatted_size, fomate_avg_size


def metrics(data_dir, sub_directory="raw"):
    """Check asset-class, data-type and time range of quantitative data"""

    base_path = os.path.join(data_dir, sub_directory)

    if not os.path.exists(base_path):
        print(f"Path not found: {base_path}")
        return

    print(f"\n{'='*60}")
    print(f"Checking directory: {base_path}")
    print(f"{'='*60}")

    # Get all asset classes
    asset_classes = []
    for item in os.listdir(base_path):
        item_path = os.path.join(base_path, item)
        if os.path.isdir(item_path):
            asset_classes.append(item)

    asset_classes.sort()

    for asset_class in asset_classes:
        asset_path = os.path.join(base_path, asset_class)
        print(f"\n📊 Asset Class: {asset_class}")
        print(f"   Path: {asset_path}")

        # Get data types
        data_types = []
        overview_dirs = []

        for item in os.listdir(asset_path):
            item_path = os.path.join(asset_path, item)
            if os.path.isdir(item_path):
                # Check if it's an overview directory
                if item in ["us_all_indices", "splits", "us_all_tickers"]:
                    overview_dirs.append(item)
                else:
                    # Check if it contains year subdirectories (to determine if it's a data type)
                    has_year_dirs = any(
                        os.path.isdir(os.path.join(item_path, subitem))
                        and subitem.isdigit()
                        and len(subitem) == 4
                        for subitem in os.listdir(item_path)
                        if os.path.exists(os.path.join(item_path, subitem))
                    )
                    if has_year_dirs:
                        data_types.append(item)

        data_types.sort()

        # Display data types and time ranges
        if data_types:
            print(f"   📈 Data Types:")
            for data_type in data_types:
                data_type_path = os.path.join(asset_path, data_type)
                earliest, latest, file_types = get_time_range_and_file_types(
                    data_type_path
                )
                n_files, path_size, avg_file_size = get_file_n_and_size(data_type_path)
                if earliest and latest:
                    file_types_str = (
                        ", ".join(sorted(file_types)) if file_types else "unknown"
                    )
                    print(f"      • {data_type}")
                    print(f"        File types: {file_types_str}")
                    print(f"        Time range: {earliest} - {latest}")
                else:
                    print(f"      • {data_type} (no data files found)")
                if n_files and path_size and avg_file_size:
                    print(f"        File numbers: {n_files}")
                    print(f"        Files total size: {path_size}")
                    print(f"        Avgerage file size: {avg_file_size}")
        else:
            print(f"   ❌ No data types found")

        # If it's raw directory, check overview data
        if sub_directory == "raw":
            overview_info = get_overview_data_info(asset_path)
            if overview_info:
                print(f"   📋 Overview Data:")
                for overview_type, info in overview_info.items():
                    print(
                        f"      • {overview_type}: {info['file_types']}, latest update {info['latest_date']}"
                    )
            elif overview_dirs:
                print(f"   📋 Overview Data:")
                for overview_dir in overview_dirs:
                    print(f"      • {overview_dir}: no data files found")


def main():
    """Main function to check raw and lake directories"""
    print("Starting quantitative data check...")

    # Check raw directory
    metrics(data_dir, sub_directory="raw")

    # Check lake directory
    metrics(data_dir, sub_directory="lake")

    print(f"\n{'='*60}")
    print("Check completed!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

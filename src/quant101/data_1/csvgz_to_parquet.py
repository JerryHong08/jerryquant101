#!/usr/bin/env python3
"""
CSV.gz to Parquet Converter
Converts CSV.gz files downloaded from Polygon.io to Parquet format for better performance
"""

import argparse
import gzip
import os
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl
from tqdm import tqdm

# Base directories
RAW_DIR = "data/raw"
PARQUET_DIR = "data/lake"

# Schema definitions for different data types using Polars
SCHEMAS = {
    # Aggregates (minute and day) - same schema for both stocks and options
    "minute_aggs_v1": {
        "ticker": pl.String,
        "volume": pl.UInt32,
        "open": pl.Float32,
        "close": pl.Float32,
        "high": pl.Float32,
        "low": pl.Float32,
        "window_start": pl.Int64,
        "transactions": pl.UInt32,
    },
    "day_aggs_v1": {
        "ticker": pl.String,
        "volume": pl.UInt32,
        "open": pl.Float32,
        "close": pl.Float32,
        "high": pl.Float32,
        "low": pl.Float32,
        "window_start": pl.Int64,
        "transactions": pl.UInt32,
    },
    # Stock trades
    "stock_trades_v1": {
        "ticker": pl.String,
        "conditions": pl.String,
        "correction": pl.Int32,
        "exchange": pl.Int32,
        "id": pl.Int64,
        "participant_timestamp": pl.Int64,
        "price": pl.Float64,
        "sequence_number": pl.Int64,
        "sip_timestamp": pl.Int64,
        "size": pl.UInt32,
        "tape": pl.Int32,
        "trf_id": pl.Int64,
        "trf_timestamp": pl.Int64,
    },
    # Stock quotes
    "stock_quotes_v1": {
        "Ticker": pl.String,  # Note: capital T in the data
        "ask_exchange": pl.Int32,
        "ask_price": pl.Float64,
        "ask_size": pl.UInt32,
        "bid_exchange": pl.Int32,
        "bid_price": pl.Float64,
        "bid_size": pl.UInt32,
        "conditions": pl.String,
        "indicators": pl.String,
        "participant_timestamp": pl.Int64,
        "sequence_number": pl.Int64,
        "sip_timestamp": pl.Int64,
        "tape": pl.Int32,
        "trf_timestamp": pl.Int64,
    },
    # Options trades
    "option_trades_v1": {
        "ticker": pl.String,
        "conditions": pl.String,
        "correction": pl.Int32,
        "exchange": pl.Int32,
        "participant_timestamp": pl.Int64,
        "price": pl.Float64,
        "sip_timestamp": pl.Int64,
        "size": pl.UInt32,
    },
    # Options quotes
    "option_quotes_v1": {
        "ticker": pl.String,
        "ask_exchange": pl.Int32,
        "ask_price": pl.Float64,
        "ask_size": pl.UInt32,
        "bid_exchange": pl.Int32,
        "bid_price": pl.Float64,
        "bid_size": pl.UInt32,
        "sequence_number": pl.Int64,
        "sip_timestamp": pl.Int64,
    },
    # Legacy support - map old names to new ones
    "trades_v1": "auto_detect",  # Will auto-detect between stock_trades_v1 and option_trades_v1
    "quotes_v1": "auto_detect",  # Will auto-detect between stock_quotes_v1 and option_quotes_v1
}


class CSVGZToParquetConverter:
    def __init__(self):
        """Initialize the converter"""
        # Create parquet directory if it doesn't exist
        os.makedirs(PARQUET_DIR, exist_ok=True)

    def detect_data_type(self, file_path: str) -> Optional[str]:
        """Detect data type from file path and content"""
        # First try to detect from path
        if "minute_agg" in file_path or "minute_candlestick" in file_path:
            return "minute_aggs_v1"
        elif "day_agg" in file_path or "day_candlestick" in file_path:
            return "day_aggs_v1"
        elif "trade" in file_path:
            # Need to check if it's stock or option based on path or content
            if "option" in file_path or "/options/" in file_path:
                return "option_trades_v1"
            elif (
                "stock" in file_path
                or "/stocks/" in file_path
                or "us_stocks" in file_path
            ):
                return "stock_trades_v1"
            else:
                # Auto-detect by checking columns
                try:
                    with gzip.open(file_path, "rt") as f:
                        first_line = f.readline().strip().lower()
                        if "trf_id" in first_line or "tape" in first_line:
                            return "stock_trades_v1"
                        else:
                            return "option_trades_v1"
                except:
                    return "stock_trades_v1"  # default to stock
        elif "quote" in file_path:
            # Need to check if it's stock or option
            if "option" in file_path or "/options/" in file_path:
                return "option_quotes_v1"
            elif (
                "stock" in file_path
                or "/stocks/" in file_path
                or "us_stocks" in file_path
            ):
                return "stock_quotes_v1"
            else:
                # Auto-detect by checking columns
                try:
                    with gzip.open(file_path, "rt") as f:
                        first_line = f.readline().strip().lower()
                        if "tape" in first_line or "indicators" in first_line:
                            return "stock_quotes_v1"
                        else:
                            return "option_quotes_v1"
                except:
                    return "stock_quotes_v1"  # default to stock
        return None

    def convert_single_file(
        self,
        csv_gz_path: str,
        parquet_path: Optional[str] = None,
        data_type: Optional[str] = None,
        compression: str = "zstd",
        compression_level: int = 3,
    ) -> Optional[str]:
        """Convert a single CSV.gz file to Parquet format using Polars

        Args:
            csv_gz_path: Path to the CSV.gz file
            parquet_path: Output path for Parquet file (auto-generated if None)
            data_type: Data type for schema detection
            compression: Compression algorithm for Parquet

        Returns:
            Path to created Parquet file or None if failed
        """
        try:
            if not os.path.exists(csv_gz_path):
                print(f"File not found: {csv_gz_path}")
                return None

            # Auto-detect data type if not provided
            if data_type is None:
                data_type = self.detect_data_type(csv_gz_path)

            # Generate output path if not provided
            if parquet_path is None:
                # Convert raw path to parquet path
                rel_path = os.path.relpath(csv_gz_path, RAW_DIR)
                parquet_path = os.path.join(PARQUET_DIR, rel_path)
                parquet_path = parquet_path.replace(".csv.gz", ".parquet")

            # Create output directory
            os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

            print(f"Converting: {csv_gz_path} -> {parquet_path}")
            print(f"Detected data type: {data_type}")

            # Get schema if available
            schema = None
            if (
                data_type
                and data_type in SCHEMAS
                and isinstance(SCHEMAS[data_type], dict)
            ):
                schema = SCHEMAS[data_type]

            # Read CSV.gz file with Polars
            try:
                # First, read a small sample to check the actual columns
                with gzip.open(csv_gz_path, "rt") as f:
                    header = f.readline().strip()
                    actual_columns = [col.strip() for col in header.split(",")]

                # Adjust schema to match actual columns
                if schema:
                    # Only use schema columns that exist in the file
                    adjusted_schema = {}
                    for col in actual_columns:
                        if col in schema:
                            adjusted_schema[col] = schema[col]
                        else:
                            # Use default type for unknown columns
                            adjusted_schema[col] = pl.String
                    schema = adjusted_schema if adjusted_schema else None

                # Read the file with Polars
                df = pl.read_csv(
                    csv_gz_path,
                    schema_overrides=schema,
                    infer_schema_length=10000 if not schema else None,
                    try_parse_dates=False,
                    null_values=["", "null", "NULL", "N/A", "n/a"],
                )

                print(f"Read {len(df)} rows with {len(df.columns)} columns")

                # Write to Parquet
                df.write_parquet(
                    parquet_path,
                    compression=compression,
                    compression_level=compression_level,
                    statistics=True,
                    use_pyarrow=True,
                )

                print(
                    f"Successfully converted {len(df):,} rows to {parquet_path}, compression_level: {compression_level}"
                )
                return parquet_path

            except Exception as e:
                print(f"Error reading with Polars: {str(e)}")
                print("Falling back to line-by-line processing...")

                # Fallback: process line by line for problematic files
                return self._convert_file_fallback(
                    csv_gz_path, parquet_path, schema, compression, compression_level
                )

        except Exception as e:
            print(f"Error converting {csv_gz_path}: {str(e)}")
            return None

    def _convert_file_fallback(
        self,
        csv_gz_path: str,
        parquet_path: str,
        schema: Optional[Dict],
        compression: str,
        compression_level: int,
    ) -> Optional[str]:
        """Fallback method for problematic files"""
        try:
            # Read line by line and clean data
            rows = []
            with gzip.open(csv_gz_path, "rt") as f:
                header = f.readline().strip().split(",")
                header = [col.strip() for col in header]

                for line_num, line in enumerate(f, 2):
                    try:
                        # Simple CSV parsing (may need improvement for complex cases)
                        values = line.strip().split(",")
                        if len(values) == len(header):
                            row_dict = dict(zip(header, values))
                            # Clean empty strings
                            for key, value in row_dict.items():
                                if value in ["", "null", "NULL", "N/A", "n/a"]:
                                    row_dict[key] = None
                            rows.append(row_dict)
                    except Exception as e:
                        print(f"Skipping line {line_num}: {str(e)}")
                        continue

            if not rows:
                print("No valid rows found")
                return None

            # Create DataFrame from cleaned data
            df = pl.DataFrame(rows, schema=schema)

            # Write to Parquet
            df.write_parquet(
                parquet_path,
                compression=compression,
                compression_level=compression_level,
                statistics=True,
                use_pyarrow=True,
            )

            print(f"Successfully converted {len(df):,} rows to {parquet_path}")
            return parquet_path

        except Exception as e:
            print(f"Fallback conversion failed: {str(e)}")
            return None

    def convert_directory(
        self,
        source_dir: str,
        target_dir: Optional[str] = None,
        pattern: str = "*.csv.gz",
        max_workers: int = 4,
        compression: str = "zstd",
        compression_level: int = 3,
    ) -> List[str]:
        """Convert all CSV.gz files in a directory

        Args:
            source_dir: Source directory containing CSV.gz files
            target_dir: Target directory for Parquet files
            pattern: File pattern to match
            max_workers: Number of parallel processes

        Returns:
            List of successfully converted files
        """
        source_path = Path(source_dir)
        if not source_path.exists():
            print(f"Source directory not found: {source_dir}")
            return []

        # Find all matching files
        csv_gz_files = list(source_path.rglob(pattern))

        if not csv_gz_files:
            print(f"No {pattern} files found in {source_dir}")
            return []

        print(f"Found {len(csv_gz_files)} files to convert")

        # Convert files in parallel
        converted_files = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_file = {}
            for csv_file in csv_gz_files:
                future = executor.submit(
                    self.convert_single_file,
                    str(csv_file),
                    None,  # parquet_path - auto-generate
                    None,  # data_type - auto-detect
                    compression,
                    compression_level,
                )
                future_to_file[future] = str(csv_file)

            # Collect results
            for future in as_completed(future_to_file):
                result = future.result()
                if result:
                    converted_files.append(result)

        print(f"Successfully converted {len(converted_files)} files")
        return converted_files

    def convert_by_asset_class(
        self,
        asset_class: str,
        data_type: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        compression: str = "zstd",
        compression_level: int = 3,
    ) -> List[str]:
        """Convert files for specific asset class and data type

        Args:
            asset_class: Asset class (e.g., us_stocks_sip)
            data_type: Data type (e.g., trades_v1)
            start_date: Start date filter (YYYY-MM-DD)
            end_date: End date filter (YYYY-MM-DD)

        Returns:
            List of successfully converted files
        """
        # Construct source directory path
        source_dir = os.path.join(RAW_DIR, asset_class, data_type)

        if not os.path.exists(source_dir):
            print(f"Source directory not found: {source_dir}")
            return []

        # Filter by date range if specified
        files_to_convert = []
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

            source_path = Path(source_dir)
            for csv_file in source_path.rglob("*.csv.gz"):
                # Extract date from filename
                filename = csv_file.stem.replace(".csv", "")
                try:
                    file_date = datetime.strptime(filename, "%Y-%m-%d").date()
                    if start_dt <= file_date <= end_dt:
                        files_to_convert.append(str(csv_file))
                except ValueError:
                    continue
        else:
            # Convert all files in directory
            return self.convert_directory(source_dir)

        # Convert filtered files
        converted_files = []
        for csv_file in files_to_convert:
            result = self.convert_single_file(
                csv_file,
                data_type=data_type,
                compression=compression,
                compression_level=compression_level,
            )
            if result:
                converted_files.append(result)

        return converted_files

    def get_parquet_info(self, parquet_path: str) -> Dict[str, Any]:
        """Get information about a Parquet file using Polars"""
        try:
            # Read just the schema without loading data
            df_lazy = pl.scan_parquet(parquet_path)
            schema = df_lazy.collect_schema()

            # Get row count efficiently
            row_count = df_lazy.select(pl.len()).collect().item()

            return {
                "path": parquet_path,
                "num_rows": row_count,
                "num_columns": len(schema),
                "file_size": os.path.getsize(parquet_path),
                "schema": {name: str(dtype) for name, dtype in schema.items()},
                "created": datetime.fromtimestamp(os.path.getctime(parquet_path)),
            }
        except Exception as e:
            return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(
        description="Convert CSV.gz files to Parquet format"
    )

    # File/directory options
    parser.add_argument("--file", help="Convert single CSV.gz file")
    parser.add_argument("--directory", help="Convert all CSV.gz files in directory")
    parser.add_argument("--output", help="Output path (for single file) or directory")

    # Asset class options (matching polygon_downloader)
    parser.add_argument("--asset-class", help="Asset class (e.g., us_stocks_sip)")
    parser.add_argument("--data-type", help="Data type (e.g., trades_v1, quotes_v1)")
    parser.add_argument("--start-date", help="Start date filter (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date filter (YYYY-MM-DD)")

    # Processing options
    parser.add_argument(
        "--max-workers", type=int, default=4, help="Number of parallel processes"
    )
    parser.add_argument(
        "--compression",
        default="zstd",
        choices=["snappy", "gzip", "brotli", "lz4"],
        help="Compression algorithm for Parquet",
    )
    parser.add_argument(
        "--compression_level", type=int, default=3, help="zstd compression level"
    )

    # Info options
    parser.add_argument("--info", help="Show information about Parquet file")
    parser.add_argument(
        "--list-schemas", action="store_true", help="List available schemas"
    )

    args = parser.parse_args()

    converter = CSVGZToParquetConverter()

    if args.list_schemas:
        print("\nAvailable schemas:")
        for data_type, schema in SCHEMAS.items():
            if isinstance(schema, dict):
                print(f"\n{data_type}:")
                for col, dtype in schema.items():
                    print(f"  {col}: {dtype}")
            else:
                print(f"\n{data_type}: {schema}")

    elif args.info:
        info = converter.get_parquet_info(args.info)
        if "error" in info:
            print(f"Error reading file: {info['error']}")
        else:
            print(f"\nParquet file information:")
            print(f"Path: {info['path']}")
            print(f"Rows: {info['num_rows']:,}")
            print(f"Columns: {info['num_columns']}")
            print(f"File size: {info['file_size']:,} bytes")
            print(f"Created: {info['created']}")
            print(f"\nSchema:")
            for col, dtype in info["schema"].items():
                print(f"  {col}: {dtype}")

    elif args.file:
        result = converter.convert_single_file(
            args.file,
            args.output,
            compression=args.compression,
            compression_level=args.compression_level,
        )
        if result:
            print(f"Successfully converted to: {result}")

    elif args.directory:
        results = converter.convert_directory(
            args.directory,
            args.output,
            max_workers=args.max_workers,
            compression=args.compression,
            compression_level=args.compression_level,
        )
        print(f"Converted {len(results)} files")

    elif args.asset_class and args.data_type:
        results = converter.convert_by_asset_class(
            args.asset_class,
            args.data_type,
            args.start_date,
            args.end_date,
            compression=args.compression,
            compression_level=args.compression_level,
        )
        print(f"Converted {len(results)} files")

    else:
        print("\nUsage examples:")
        print("  Convert single file:")
        print(
            "    python csvgz_to_parquet.py --file data/raw/us_stocks_sip/trades_v1/2024/03/2024-03-01.csv.gz"
        )
        print("\n  Convert directory:")
        print(
            "    python csvgz_to_parquet.py --directory data/raw/us_stocks_sip/trades_v1/"
        )
        print("\n  Convert by asset class:")
        print(
            "    python csvgz_to_parquet.py --asset-class us_stocks_sip --data-type trades_v1"
        )
        print("\n  Convert date range:")
        print(
            "    python csvgz_to_parquet.py --asset-class us_stocks_sip --data-type trades_v1 --start-date 2024-03-01 --end-date 2024-03-07"
        )
        print("\n  Show file info:")
        print(
            "    python csvgz_to_parquet.py --info data/lake/us_stocks_sip/trades_v1/2024/03/2024-03-01.parquet"
        )
        print("\n  List schemas:")
        print("    python csvgz_to_parquet.py --list-schemas")


if __name__ == "__main__":
    main()

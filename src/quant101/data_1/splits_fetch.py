import os
import time

import polars as pl
from dotenv import load_dotenv
from polygon import RESTClient

from quant101.core_2.config import splits_dir

load_dotenv()

polygon_api_key = os.getenv("POLYGON_API_KEY")

client = RESTClient(polygon_api_key)

updated_time = time.strftime("%Y%m%d")


def fetch_splits_and_save(out_dir=splits_dir):

    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"all_splits_{updated_time}.parquet")

    if os.path.exists(out_file):
        print(f"splits incremental already updated. {out_file} already exists.")
        return out_file

    try:
        files = [
            f
            for f in os.listdir(out_dir)
            if f.startswith("all_splits_") and f.endswith(".parquet")
        ]
        if not files:
            raise FileNotFoundError("No splits files found")
        last_splits_file_name = max(files)
        last_splits_file = os.path.join(out_dir, last_splits_file_name)
        splits_original = pl.read_parquet(last_splits_file)
        print("splits incremental updating...")
        print(f"Found existing splits file: {last_splits_file_name}")
    except (FileNotFoundError, pl.exceptions.ComputeError, ValueError):
        print("not find previous splits file, first time run")
        splits_original = pl.DataFrame()

    first_time = splits_original.is_empty()

    splits = []
    for i, s in enumerate(
        client.list_splits(
            order="desc",
            limit="300",  # make sure it reaches to today
            sort="execution_date",
        )
    ):

        # if i < 3:
        #     print(f'splits {i}: {s}')

        # 添加延迟以避免速率限制
        # time.sleep(12)  # 100ms 延迟
        split_dict = {
            "id": s.id,
            "execution_date": s.execution_date,
            "split_from": s.split_from,
            "split_to": s.split_to,
            "ticker": s.ticker,
        }
        splits.append(split_dict)

        if not first_time:
            # When incremental updating, don't need to fetch too much data.
            if len(splits) >= 299:
                break

    splits_new = pl.DataFrame(splits)

    # 合并 splits_original 和 splits_new，只保留 splits_new 中 splits_original 没有的行
    # splits_diff = splits_new.filter(~pl.col('id').is_in(splits_original['id'].implode()))
    splits = pl.concat([splits_original, splits_new]).unique()
    splits.write_parquet(out_file, compression="snappy")
    print(f"Saved {out_file}")


fetch_splits_and_save(splits_dir)

# with pl.Config(tbl_rows=100, tbl_cols=10):
#     last_splits_file = os.path.join(splits_dir, f"all_splits_20250913.parquet")
#     splits_read = pl.read_parquet(splits_dir)
#     # 需要将排序后的 DataFrame 赋值回来，否则排序不会生效
#     splits_read = splits_read.sort("execution_date", descending=True)
#     print(splits_read.head(100))

import os
import time

import polars as pl
from dotenv import load_dotenv
from polygon import RESTClient

load_dotenv()

polygon_api_key = os.getenv("POLYGON_API_KEY")

client = RESTClient(polygon_api_key)

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

    # 可选：限制总数量进行测试
    if len(splits) >= 299:
        break

splits_new = pl.DataFrame(splits)

splits_dir = "data/raw/us_stocks_sip/splits/splits.parquet"

os.makedirs(os.path.dirname(splits_dir), exist_ok=True)

splits_original = pl.read_parquet(splits_dir)

# 合并 splits_original 和 splits_new，只保留 splits_new 中 splits_original 没有的行
# splits_diff = splits_new.filter(~pl.col('id').is_in(splits_original['id'].implode()))
splits = pl.concat([splits_original, splits_new]).unique()

splits.write_parquet(splits_dir, compression="snappy")

splits_read = pl.read_parquet(splits_dir)

# 需要将排序后的 DataFrame 赋值回来，否则排序不会生效
splits_read = splits_read.sort("execution_date", descending=True)

# with pl.Config(tbl_rows=100, tbl_cols=10):
#     print(splits_read.head(100))

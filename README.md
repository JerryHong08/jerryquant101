link the data to where your data are.
For my case, in Linux Ubuntu:
ln -s /mnt/d/quant_data/polygon_data data

Feature:

1. 通用data读取功能函数，specific file，date range etc


Splits:
there are some discrepancy in spilts data from polygon.io.
So you need to delete by yourself as you found out the splits data.
My spilts discrepancy copy is in the same directory of splits_error.py. you can move this file to your splits_error directory.
for everytime splits use:
```
import polars as pl

splits_dir = 'data/raw/us_stocks_sip/splits/splits.parquet'
splits_error_dir = 'data/raw/us_stocks_sip/splits/splits_error.parquet'

splits_original = pl.read_parquet(splits_dir)
splits_errors = pl.read_parquet(splits_error_dir)

splits = splits_original.filter(~pl.col('id').is_in(splits_errors['id'].implode()))

```
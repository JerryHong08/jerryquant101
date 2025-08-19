Functions Update,
From BASIC To DOPE:

1. Polygon.io flat file downloader. Use prefix to download specific file，date range etc
    1.1. prepare your data directory.
    For my case, in Linux Ubuntu, I use link command to link my data directory to my SSD driver.
    ```
    --bash ln -s /mnt/d/quant_data/polygon_data data
    ```
    And then, I can access the data directory just like I put it under my code project directory. And you can use link method to share the data to your other projects.

2. Splits adjust pre process. After downloading splits data from polygon.io.There is few steps to correctly adjust your historical data price and volume. 
    2.1. There are some discrepancy in spilts data from polygon.io.
    So you need to delete by yourself as you found out the splits data and filter it by running 'src/quant101/1_data/splits_error.py' and customize your filter.
    
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
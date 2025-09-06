Functions Update,
From BASIC To DOPE:


### src/quant101/data_1
this directory is for data download and pre process.

1. Polygon.io flat file downloader. Use prefix to download specific file，date range etc
    * update incrementally by run like```python src/quant101/data_1/polygon_downloader.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days 7``` or more in examples above as you like.
    
    * FIRST TIME RUN, SEE BELOW:
    
    1.1. prepare your data directory.config in ```src/quant101/core_2/config.py```
    
    for my case, I organize the data file structure like this:
    
    'lake' for the .parquet files, 'processed' for the cache. 'raw' for the original download files,
    ```
    ├── lake
    │   ├── us_options_opra
    │   │   └── trades_v1
    │   └── us_stocks_sip
    │       ├── day_aggs_v1
    │       └── minute_aggs_v1
    ├── processed
    │   └── us_stocks_sip
    │       └── day_aggs_v1
    └── raw
        ├── global_crypto
        │   └── minute_aggs_v1
        ├── us_indices
        │   ├── day_aggs_v1
        │   ├── minute_aggs_v1
        │   └── us_all_indices
        ├── us_options_opra
        │   ├── day_aggs_v1
        │   ├── minute_aggs_v1
        │   ├── quotes_v1
        │   └── trades_v1
        └── us_stocks_sip
            ├── day_aggs_v1
            ├── minute_aggs_v1
            ├── splits
            └── us_all_tickers
    ```
    1.2. first time run the flatfile downloader, you may need to consider what kind of asset and data-type you want. It can relate to your interest or may be restricted by SSD memory you have. Here are different asset and data-type polygon.io .csz.gz file size I have estimated, not very acurate but enough for me:
    | Size Default per Year | Stock | Option | Indice | Forex | Crypto |
    | --- | --- | --- | --- | --- | --- |
    | Day Aggregate | 50 MB | 600 MB | 80 MB | 8 MB | 5 MB |
    | Minute Aggregate |  4.5 GB | 4.5 GB | 25 GB | 3 GB | 1 GB |
    | Trades | 350 GB | 10 GB | \ | \ | 15 GB |
    | Quotes | 1.5 TB | 22 TB | 2 TB/M | 100 GB/Day | \ | 60 GB | \ |
    | Values( tick-by-tick ) | \ | \ | 500 GB | \ | \ |

    Then you can run like below usage examples to download what you like: 
    ```
    Usage examples:
    List files: 
        python src/quant101/data_1/polygon_downloader.py --list --prefix us_stocks_sip/trades_v1/2024/
    
    Download recent 7 days: 
        python src/quant101/data_1/polygon_downloader.py --asset-class us_stocks_sip --data-type minute_aggs_v1 --recent-days 7
    
    Download date range: 
        python src/quant101/data_1/polygon_downloader.py --asset-class us_stocks_sip --data-type trades_v1 --start-date 2024-03-01 --end-date 2024-03-07
    
    Download specific file: 
        python src/quant101/data_1/polygon_downloader.py --specific-file us_stocks_sip/minute_aggs_v1/2024/03/2024-03-07.csv.gz
    ```


    1.3. In order to flexibly choose how save the files, the default donwload file will be as the same as the original .csv.gz file.
    for my case, as my data file strcture showed above. I firstly download all the files into the 'raw', then I run ```csvgz_to_parquet.py``` to transfer what files I need to .parquet files and save them into the 'lake', which is more quick for me to use polars to use the data.
    and here are the example usage of how to convert csvgz to parquet:
    ```
    Usage examples:
    Convert single file:
        python src/quant101/data_1/csvgz_to_parquet.py --file /mnt/blackdisk/quant_data/polygon_data/raw/us_stocks_sip/trades_v1/2024/03/2024-03-01.csv.gz

    Convert directory:
        python src/quant101/data_1/csvgz_to_parquet.py --directory /mnt/blackdisk/quant_data/polygon_data/raw/us_stocks_sip/trades_v1/

    Convert by asset class:
        python src/quant101/data_1/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type trades_v1

    Convert date range:
        python src/quant101/data_1/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type trades_v1 --start-date 2024-03-01 --end-date 2024-03-07

    Show file info:
        python src/quant101/data_1/csvgz_to_parquet.py --info /mnt/blackdisk/quant_data/polygon_data/lake/us_stocks_sip/trades_v1/2024/03/2024-03-01.parquet

    List schemas:
        python src/quant101/data_1/csvgz_to_parquet.py --list-schemas
    ```

2. Versatile polygon.io aggs fecth and save using REST API.


3. Splits adjust pre process. After downloading splits data from polygon.io.There is few steps to correctly adjust your historical data price and volume.
    * update incrementally or first time create by run ```python/src/quant101/data_1/splits_fetch.py```

    _There are some discrepancy in spilts data from polygon.io.
    So you need to delete by yourself as you found out the splits data and filter it by running 'src/quant101/data_1/splits_error.py' and customize your filter._
    
    _My spilts discrepancy copy is in the same directory of splits_error.py. you can move this file to your splits_error directory.
    for everytime splits use:_
    ```
    import polars as pl

    splits_dir = 'data/raw/us_stocks_sip/splits/splits.parquet'
    splits_error_dir = 'data/raw/us_stocks_sip/splits/splits_error.parquet'

    splits_original = pl.read_parquet(splits_dir)
    splits_errors = pl.read_parquet(splits_error_dir)

    splits = splits_original.filter(~pl.col('id').is_in(splits_errors['id'].implode()))

    ```

### src/quant101/core_2
this directory is for config, candlestick plotter, data_loader and so on. 
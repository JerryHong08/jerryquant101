# CSV.gz to Parquet 转换器使用指南

这个转换器基于 Polars 库，专门用于将 Polygon.io 下载的 CSV.gz 文件转换为高性能的 Parquet 格式。

## 功能特性

1. **使用 Polars 库**：比 Pandas 更快的数据处理
2. **智能类型检测**：根据文件路径和内容自动检测数据类型
3. **预定义 Schema**：为不同数据类型优化了数据类型
4. **并行处理**：支持多进程并行转换
5. **错误处理**：包含回退机制处理有问题的文件

## 支持的数据类型

### 聚合数据（Aggregates）
- `minute_aggs_v1`: 分钟级聚合数据
- `day_aggs_v1`: 日级聚合数据

Schema:
```
ticker: String
volume: UInt32
open: Float32
close: Float32
high: Float32
low: Float32
window_start: Int64
transactions: UInt32
```

### 股票数据（Stocks）

#### Trades (`stock_trades_v1`)
```
ticker: String
conditions: String
correction: Int32
exchange: Int32
id: Int64
participant_timestamp: Int64
price: Float64
sequence_number: Int64
sip_timestamp: Int64
size: UInt32
tape: Int32
trf_id: Int64
trf_timestamp: Int64
```

#### Quotes (`stock_quotes_v1`)
```
Ticker: String  # 注意：首字母大写
ask_exchange: Int32
ask_price: Float64
ask_size: UInt32
bid_exchange: Int32
bid_price: Float64
bid_size: UInt32
conditions: String
indicators: String
participant_timestamp: Int64
sequence_number: Int64
sip_timestamp: Int64
tape: Int32
trf_timestamp: Int64
```

### 期权数据（Options）

#### Trades (`option_trades_v1`)
```
ticker: String
conditions: String
correction: Int32
exchange: Int32
participant_timestamp: Int64
price: Float64
sip_timestamp: Int64
size: UInt32
```

#### Quotes (`option_quotes_v1`)
```
ticker: String
ask_exchange: Int32
ask_price: Float64
ask_size: UInt32
bid_exchange: Int32
bid_price: Float64
bid_size: UInt32
sequence_number: Int64
sip_timestamp: Int64
```

## 使用方法

### 1. 转换单个文件
```bash
python src/quant101/1_data/csvgz_to_parquet.py --file data/raw/us_stocks_sip/trades_v1/2024/03/2024-03-01.csv.gz
```

### 2. 转换整个目录
```bash
python src/quant101/1_data/csvgz_to_parquet.py --directory data/raw/us_stocks_sip/trades_v1/
```

### 3. 按资产类别转换（匹配 polygon_downloader 参数）
```bash
python src/quant101/1_data/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type trades_v1
```

### 4. 转换指定日期范围
```bash
python src/quant101/1_data/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type trades_v1 --start-date 2024-03-01 --end-date 2024-03-07
```

### 5. 查看 Parquet 文件信息
```bash
python src/quant101/1_data/csvgz_to_parquet.py --info data/lake/us_stocks_sip/trades_v1/2024/03/2024-03-01.parquet
```

### 6. 查看可用的 Schema
```bash
python src/quant101/1_data/csvgz_to_parquet.py --list-schemas
```

## 与 polygon_downloader 配合使用

1. **下载数据**：
```bash
python src/quant101/1_data/polygon_downloader.py --asset-class us_stocks_sip --data-type trades_v1 --recent-days 7
```

2. **转换为 Parquet**：
```bash
python src/quant101/1_data/csvgz_to_parquet.py --asset-class us_stocks_sip --data-type trades_v1
```

## 目录结构

```
data/
├── raw/           # 原始 CSV.gz 文件（polygon_downloader 输出）
└── lake/          # 转换后的 Parquet 文件
```

## 性能优势

- **存储空间**：Parquet 格式通常比 CSV.gz 节省 50-80% 存储空间
- **查询速度**：列式存储格式，查询速度提升 10-100 倍
- **类型安全**：预定义的数据类型，避免类型推断错误
- **压缩效率**：内置高效压缩算法

## 注意事项

1. 程序会自动检测数据类型，但也可以手动指定
2. 输出目录结构保持与输入相同
3. 支持多种压缩算法（snappy, gzip, brotli, lz4）
4. 包含错误处理和回退机制
5. 使用 Polars 的高性能处理引擎

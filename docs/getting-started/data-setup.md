# Data Setup

## Data Source

Quant101 uses [Polygon.io](https://polygon.io/flat-files) flat files distributed
via AWS S3. These are daily OHLCV aggregates for all US equities.

## Directory Structure

```
polygon_data/
├── lake/           # Parquet files (converted from csv.gz)
├── processed/      # Cached/resampled data
└── raw/            # Original csv.gz + metadata (splits, tickers, indices)
```

## Data Acquisition

### Download Raw Data

```bash
# Download recent Polygon.io flat files
python src/data/fetcher/polygon_downloader.py \
    --asset-class us_stocks_sip \
    --data-type day_aggs_v1 \
    --recent-days 7

# Convert csv.gz → Parquet
python src/data/fetcher/csvgz_to_parquet.py \
    --asset-class us_stocks_sip \
    --data-type day_aggs_v1 \
    --recent-days 7
```

### Incremental Update

The update script handles the full pipeline (download, convert, splits, indices):

```bash
bash scripts/incremental_update/data_update.sh
```

## Stock Universe

The pipeline uses a named universe registry rather than hardcoded ticker lists:

```python
from data.universe import get_universe

tickers = get_universe("US_LARGE_CAP_50")  # 50 tickers, sector-organized
```

Available universes:

| Name | Size | Description |
|------|------|-------------|
| `US_LARGE_CAP_50` | 50 | Sector-diversified US mega-caps |
| `US_LARGE_CAP_52` | 52 | Extended version |

Register your own:

```python
from data.universe import register_universe

register_universe("MY_TECH_10", ["AAPL", "MSFT", "GOOGL", ...])
```

## Data Loading

The core loader handles OHLCV data with split adjustment and caching:

```python
from data.loader.data_loader import stock_load_process

ohlcv = stock_load_process(
    tickers=["AAPL", "MSFT"],
    start_date="2024-01-01",
    end_date="2025-01-01",
)
# Returns: pl.LazyFrame with (ticker, timestamps, open, high, low, close, volume)
```

## Column Naming Convention

| Context | Date Column | Notes |
|---------|------------|-------|
| Raw OHLCV | `timestamps` | As-is from Polygon |
| Factor signals | `date` | Renamed during pipeline |
| Returns | `date` | Standardized |
| Alpha/Risk/Execution | `date` | Via `constants.DATE_COL` |

!!! warning "Known Issue"
    The `timestamps` → `date` rename is handled by the pipeline but
    not enforced globally. See `src/constants.py` for the canonical names.

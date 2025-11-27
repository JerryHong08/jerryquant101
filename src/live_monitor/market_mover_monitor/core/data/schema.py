import time
from typing import List, Optional

import polars as pl
from pydantic import BaseModel, Field, ValidationError


class FloatSourceData(BaseModel):
    source: str = Field(..., description="Data Source")
    float_shares: Optional[float] = None
    short_percent: Optional[float] = None  # 0-1 normalized
    outstanding_shares: Optional[float] = None


class FloatShares(BaseModel):
    ticker: str = Field(..., description="Ticker symbol")
    data: List[FloatSourceData]  # keep all sources
    timestamp: int = Field(
        default_factory=lambda: int(time.time()), description="Last updated time"
    )


class SnapshotMessage(BaseModel):
    ticker: str = Field(..., description="Stock ticker symbol")
    percent_change: float = Field(
        ..., description="Percentage change from previous close"
    )
    accumulated_volume: float = Field(
        ..., description="Total accumulated trading volume"
    )
    current_price: float = Field(..., description="Current stock price")
    prev_close: float = Field(..., description="Previous closing price")
    prev_volume: float = Field(..., description="Previous trading volume")
    timestamp: int = Field(..., description="Timestamp in milliseconds")


def validate_SnapshotMsg_schema(df: pl.DataFrame) -> tuple[bool, str]:
    """
    Fast DataFrame schema validation
    """
    required_schema = {
        "ticker": pl.Utf8,
        "percent_change": pl.Float64,
        "accumulated_volume": pl.Float64,
        "current_price": pl.Float64,
        "prev_close": pl.Float64,
        "prev_volume": pl.Float64,
        "timestamp": pl.Int64,
    }

    # Check columns
    missing = set(required_schema.keys()) - set(df.columns)
    if missing:
        return False, f"Missing columns: {missing}"

    # Check types
    for col, expected_type in required_schema.items():
        if df[col].dtype != expected_type:
            return (
                False,
                f"Column '{col}' type mismatch: {df[col].dtype} vs {expected_type}",
            )

    # Check for nulls
    null_counts = df.null_count()
    null_cols = [col for col in required_schema.keys() if null_counts[col][0] > 0]
    if null_cols:
        return False, f"Null values in columns: {null_cols}"

    return True, ""


def spot_check_SnapshotMsg_with_pydantic(
    df: pl.DataFrame, sample_size: int = 3
) -> bool:
    """
    Deep validation using Pydantic (sample check)
    """
    if len(df) == 0:
        return False

    sample_size = min(sample_size, len(df))
    records = df.head(sample_size).to_dicts()

    for i, record in enumerate(records):
        try:
            SnapshotMessage(**record)
        except ValidationError as e:
            print(f"❌ Pydantic validation failed for row {i}: {e}")
            return False

    return True


class BorrowFee(BaseModel):
    pass


class NewsData(BaseModel):
    pass

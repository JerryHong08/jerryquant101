import time
from typing import List, Optional

from pydantic import BaseModel, Field


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

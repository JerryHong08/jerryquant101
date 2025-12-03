import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import polars as pl
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)


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


@dataclass
class NewsArticle:
    """NewsArticle Dataclass"""

    symbol: str
    published_time: datetime
    title: str
    text: Optional[str]
    url: str
    sources: str

    @classmethod
    def from_fmp_api_response(cls, data: Dict) -> "NewsArticle":
        """create NewsArticle from fmp api response"""
        try:
            published_time = datetime.strptime(
                data["publishedDate"], "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=ZoneInfo("America/New_York"))
        except (KeyError, ValueError) as e:
            logger.warning(f"data format not matched: {e}, using current running time.")
            published_time = datetime.now().astimezone(ZoneInfo("America/New_York"))

        return cls(
            symbol=data.get("symbol", ""),
            published_time=published_time,
            title=data.get("title", ""),
            text=data.get("text", ""),
            url=data.get("url", ""),
            sources=data.get("publisher", ""),
        )

    @classmethod
    def from_momo_web_response(cls, symbol: str, data: Dict) -> "NewsArticle":
        """create NewsArticle from momo web response"""
        try:
            published_time = datetime.fromtimestamp(data["time"]).astimezone(
                ZoneInfo("America/New_York")
            )
        except (KeyError, ValueError) as e:
            logger.warning(f"data format not matched: {e}, using current running time.")
            published_time = datetime.now().astimezone(ZoneInfo("America/New_York"))

        return cls(
            symbol=symbol,
            published_time=published_time,
            title=data.get("title", ""),
            text="",
            url=data.get("url", ""),
            sources=data.get("sources", ""),
        )

    @classmethod
    def from_benzinga_api_response(cls, symbol: str, data: Dict) -> "NewsArticle":
        """create NewsArticle from Benzinga API response"""
        try:
            # Benzinga format: "Wed, 03 Dec 2025 08:34:03 -0400"
            published_time = datetime.strptime(
                data["created"], "%a, %d %b %Y %H:%M:%S %z"
            ).astimezone(ZoneInfo("America/New_York"))
        except (KeyError, ValueError) as e:
            logger.warning(f"data format not matched: {e}, using current running time.")
            published_time = datetime.now().astimezone(ZoneInfo("America/New_York"))

        return cls(
            symbol=symbol,
            published_time=published_time,
            title=data.get("title", ""),
            text=data.get("body", ""),
            url=data.get("url", ""),
            sources=data.get("author", ""),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "NewsArticle":
        """Create NewsArticle from a JSON string"""
        try:
            data = json.loads(json_str)
            return cls.from_dict(data)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON data: {e}")
            raise

    @classmethod
    def from_dict(cls, data: Dict) -> "NewsArticle":
        """Create NewsArticle from a dictionary"""
        try:
            return cls(
                symbol=data["symbol"],
                published_time=datetime.fromisoformat(data["published_date"]),
                title=data["title"],
                text=data.get("text", ""),
                url=data["url"],
                sources=data["sources"],
            )
        except KeyError as e:
            logger.error(f"Missing field in data: {e}")
            raise
        except ValueError as e:
            logger.error(f"Invalid value in data: {e}")
            raise

    def to_dict(self) -> Dict:
        """transform into Dict"""
        return {
            "symbol": self.symbol,
            "published_date": self.published_time.isoformat(),
            "title": self.title,
            "text": self.text,
            "url": self.url,
            "sources": self.sources,
        }


class NewsFormatter:
    """News Formatter"""

    def format_json(articles: List[NewsArticle], indent: int = 2) -> str:
        """format to json"""
        data = [article.to_dict() for article in articles]
        return json.dumps(data, indent=indent, ensure_ascii=False)


class BorrowFee(BaseModel):
    pass

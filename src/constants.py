"""
Project-wide constants — single source of truth for magic numbers.

Import these instead of hardcoding values like ``252`` or ``"date"``.

Usage:
    from constants import TRADING_DAYS_PER_YEAR, DATE_COL, TICKER_COL
"""

# ── Calendar ──────────────────────────────────────────────────────────────────
TRADING_DAYS_PER_YEAR: int = 252
"""US equity trading days per year — standard annualization factor."""

# ── Column Naming Convention ──────────────────────────────────────────────────
# These define the *target* convention for inter-module communication.
# Legacy code may still use "timestamps" — new code should use DATE_COL.
DATE_COL: str = "date"
"""Standard date column name for factor / weight / return DataFrames."""

TICKER_COL: str = "ticker"
"""Standard ticker column name."""

VALUE_COL: str = "value"
"""Standard factor-signal value column (used in alpha module)."""

WEIGHT_COL: str = "weight"
"""Standard portfolio weight column (used in risk / execution modules)."""

RETURN_COL: str = "daily_return"
"""Standard daily-return column name."""

# ── OHLCV Raw Data ────────────────────────────────────────────────────────────
# The raw OHLCV loader uses "timestamps" (from Polygon.io data).
# Factor / portfolio code should rename to DATE_COL upon ingestion.
OHLCV_DATE_COL: str = "timestamps"
"""Date column name in raw OHLCV DataFrames from data_loader."""

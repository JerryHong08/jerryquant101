"""
Factor Functions & Registry — OHLCV → preprocessed factor DataFrames.

Each factor function has the signature::

    def compute_xxx(
        ohlcv: pl.DataFrame,
        *,
        ohlcv_date_col: str = OHLCV_DATE_COL,
        factor_config: FactorConfig | None = None,
        **kwargs,
    ) -> pl.DataFrame:
        ...

and returns a preprocessed ``(date, ticker, value)`` DataFrame.

To add a new factor:
    1. Write a function following the signature above.
    2. Call ``register_factor("my_factor", my_fn)`` or add it to
       ``_FACTOR_REGISTRY`` directly.

Usage:
    from portfolio.factors import register_factor, list_factors

Reference: docs/quant_lab.tex — Part III
"""

from __future__ import annotations

from typing import Callable

import polars as pl

from alpha.preprocessing import preprocess_factor
from constants import DATE_COL, OHLCV_DATE_COL, TICKER_COL, VALUE_COL
from portfolio.alpha_config import FactorConfig

# ── Factor Implementations ────────────────────────────────────────────────────


def _compute_bbiboll_factor(
    ohlcv: pl.DataFrame,
    ohlcv_date_col: str = OHLCV_DATE_COL,
    factor_config: FactorConfig | None = None,
    **kwargs,
) -> pl.DataFrame:
    """Extract BBIBOLL deviation factor from OHLCV data.

    Computes (close - BBI) / deviation as the raw signal, then
    winsorizes and normalizes cross-sectionally.

    Args:
        ohlcv: Raw OHLCV DataFrame.
        ohlcv_date_col: Date column name.
        factor_config: Per-factor preprocessing config.

    Returns:
        Preprocessed factor DataFrame (date, ticker, value).
    """
    fc = factor_config or FactorConfig()
    from strategy.indicators.registry import get_indicator

    bbiboll_fn = get_indicator("bbiboll")
    ohlcv_bb = bbiboll_fn(ohlcv)

    raw = (
        ohlcv_bb.with_columns(
            ((pl.col("close") - pl.col("bbi")) / pl.col("dev")).alias(VALUE_COL)
        )
        .filter(
            pl.col(VALUE_COL).is_not_null()
            & pl.col(VALUE_COL).is_not_nan()
            & pl.col(VALUE_COL).is_finite()
        )
        .select(
            [
                pl.col(ohlcv_date_col).alias(DATE_COL),
                pl.col(TICKER_COL),
                pl.col(VALUE_COL),
            ]
        )
    )
    return preprocess_factor(
        raw,
        winsorize_pct=fc.winsorize_pct,
        method=fc.normalize_method,
        neutralize=fc.neutralize,
    )


def _compute_vol_ratio_factor(
    ohlcv: pl.DataFrame,
    short_window: int = 5,
    long_window: int = 20,
    ohlcv_date_col: str = OHLCV_DATE_COL,
    factor_config: FactorConfig | None = None,
    **kwargs,
) -> pl.DataFrame:
    """Compute volatility ratio factor: vol_5d / vol_20d.

    High ratio = recent vol spike relative to trend → contrarian signal.

    Args:
        ohlcv: Raw OHLCV DataFrame.
        short_window: Short volatility window (overridden by factor_config.params).
        long_window: Long volatility window (overridden by factor_config.params).
        factor_config: Per-factor preprocessing config.

    Returns:
        Preprocessed factor DataFrame (date, ticker, value).
    """
    fc = factor_config or FactorConfig()
    short_window = fc.params.get("short_window", short_window)
    long_window = fc.params.get("long_window", long_window)

    raw = (
        ohlcv.sort([TICKER_COL, ohlcv_date_col])
        .with_columns(
            (pl.col("close") / pl.col("close").shift(1).over(TICKER_COL))
            .log()
            .alias("log_ret")
        )
        .with_columns(
            [
                pl.col("log_ret")
                .rolling_std(window_size=short_window)
                .over(TICKER_COL)
                .alias("vol_short"),
                pl.col("log_ret")
                .rolling_std(window_size=long_window)
                .over(TICKER_COL)
                .alias("vol_long"),
            ]
        )
        .with_columns((pl.col("vol_short") / pl.col("vol_long")).alias(VALUE_COL))
        .filter(
            pl.col(VALUE_COL).is_not_null()
            & pl.col(VALUE_COL).is_not_nan()
            & pl.col(VALUE_COL).is_finite()
        )
        .select(
            [
                pl.col(ohlcv_date_col).alias(DATE_COL),
                pl.col(TICKER_COL),
                pl.col(VALUE_COL),
            ]
        )
    )
    return preprocess_factor(
        raw,
        winsorize_pct=fc.winsorize_pct,
        method=fc.normalize_method,
        neutralize=fc.neutralize,
    )


def _compute_momentum_factor(
    ohlcv: pl.DataFrame,
    lookback: int = 20,
    ohlcv_date_col: str = OHLCV_DATE_COL,
    factor_config: FactorConfig | None = None,
    **kwargs,
) -> pl.DataFrame:
    """Compute momentum factor: lookback-day log return.

    Args:
        ohlcv: Raw OHLCV DataFrame.
        lookback: Lookback window in days (overridden by factor_config.params).
        factor_config: Per-factor preprocessing config.

    Returns:
        Preprocessed factor DataFrame (date, ticker, value).
    """
    fc = factor_config or FactorConfig()
    lookback = fc.params.get("lookback", lookback)

    raw = (
        ohlcv.sort([TICKER_COL, ohlcv_date_col])
        .with_columns(
            (pl.col("close") / pl.col("close").shift(lookback).over(TICKER_COL))
            .log()
            .alias(VALUE_COL)
        )
        .filter(
            pl.col(VALUE_COL).is_not_null()
            & pl.col(VALUE_COL).is_not_nan()
            & pl.col(VALUE_COL).is_finite()
        )
        .select(
            [
                pl.col(ohlcv_date_col).alias(DATE_COL),
                pl.col(TICKER_COL),
                pl.col(VALUE_COL),
            ]
        )
    )
    return preprocess_factor(
        raw,
        winsorize_pct=fc.winsorize_pct,
        method=fc.normalize_method,
        neutralize=fc.neutralize,
    )


# ── Registry ──────────────────────────────────────────────────────────────────

# Factor registry: name → compute function
_FACTOR_REGISTRY: dict[str, Callable[..., pl.DataFrame]] = {
    "bbiboll": _compute_bbiboll_factor,
    "vol_ratio": _compute_vol_ratio_factor,
    "momentum": _compute_momentum_factor,
}


def register_factor(name: str, fn: Callable[..., pl.DataFrame]) -> None:
    """Register a custom factor computation function.

    Args:
        name: Factor name (lowercase).
        fn: Callable that takes ``(ohlcv, **kwargs)`` and returns a
            preprocessed factor DataFrame ``(date, ticker, value)``.
    """
    _FACTOR_REGISTRY[name.lower()] = fn


def list_factors() -> list[str]:
    """Return names of all registered factors."""
    return sorted(_FACTOR_REGISTRY.keys())


def get_factor_fn(name: str) -> Callable[..., pl.DataFrame]:
    """Look up a factor function by name.

    Args:
        name: Registered factor name (case-insensitive).

    Returns:
        The factor computation function.

    Raises:
        KeyError: If the factor is not registered.
    """
    key = name.lower()
    if key not in _FACTOR_REGISTRY:
        available = ", ".join(sorted(_FACTOR_REGISTRY.keys()))
        raise KeyError(
            f"Unknown factor '{name}'. Available: {available}. "
            f"Use register_factor() to add custom factors."
        )
    return _FACTOR_REGISTRY[key]

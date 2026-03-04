from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from alpha.preprocessing import preprocess_factor
from constants import DATE_COL, OHLCV_DATE_COL, TICKER_COL, VALUE_COL
from factors.factors import register_factor

if TYPE_CHECKING:
    from portfolio.alpha_config import FactorConfig


@register_factor("momentum")
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
    from portfolio.alpha_config import FactorConfig

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

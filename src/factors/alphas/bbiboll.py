from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from alpha.preprocessing import preprocess_factor
from constants import DATE_COL, OHLCV_DATE_COL, TICKER_COL, VALUE_COL
from factors.factors import register_factor

if TYPE_CHECKING:
    from portfolio.alpha_config import FactorConfig


@register_factor("bbiboll")
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
    from portfolio.alpha_config import FactorConfig

    fc = factor_config or FactorConfig()
    from indicators.registry import get_indicator

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

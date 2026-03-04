from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from alpha.preprocessing import preprocess_factor
from constants import DATE_COL, OHLCV_DATE_COL, TICKER_COL, VALUE_COL
from factors.factors import register_factor

if TYPE_CHECKING:
    from portfolio.alpha_config import FactorConfig


@register_factor("vol_ratio")
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
    from portfolio.alpha_config import FactorConfig

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

"""Alpha 101 factors — pandas-based, using composable operators.

Each function receives a Polars DataFrame, converts to pandas internally,
computes the signal using operators from ``_ops``, and returns a Polars
DataFrame with at least a ``value`` column.  The ``@register_factor``
wrapper handles null filtering, column selection, and preprocessing.

Formula reference: https://arxiv.org/abs/1601.00991
"""

from __future__ import annotations

import numpy as np
import polars as pl

from constants import OHLCV_DATE_COL, TICKER_COL, VALUE_COL
from factors.alphas.operator import delay, delta, rank, signed_power, ts_argmax, ts_corr

print("Loading alpha101 module...")  # Add this at the top after imports

from factors.factors import register_factor

print("About to register factors...")  # Add this before the first @register_factor


def _to_pd(ohlcv: pl.DataFrame):
    """Convert to pandas sorted by [ticker, date], return (df, grouped)."""
    df = ohlcv.to_pandas().sort_values([TICKER_COL, OHLCV_DATE_COL])
    return df, df.groupby(TICKER_COL)


def _to_pl(df, date_col=OHLCV_DATE_COL):
    """Convert pandas back to polars, keeping required columns."""
    return pl.from_pandas(df[[date_col, TICKER_COL, VALUE_COL]])


@register_factor("alpha001")
def alpha001(ohlcv: pl.DataFrame) -> pl.DataFrame:
    """(rank(ts_argmax(signed_power(close/delay(close,1)-1, 2), 5)))"""
    df, g = _to_pd(ohlcv)

    close = df["close"]
    ret = close / g["close"].shift(1) - 1
    df[VALUE_COL] = rank(
        ts_argmax(g, signed_power(ret, 2), 5),
        df[OHLCV_DATE_COL],
    )
    return _to_pl(df)


@register_factor("alpha002")
def alpha002(ohlcv: pl.DataFrame) -> pl.DataFrame:
    """(-1 * correlation(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6))
    # correlation(x1,y1,d1)
        x1=rank(delta(x2,2))
            # rank(x2.1)
            x2.1=delta(log(volume), 2)
                # delta(x3.1, d3)
                x3.1=log(volume)
                    # log(x4.1)
                    x4.1=volume
        y1=rank(((close-open)/open))
            # rank(x2.2)
            x2.2=((close - open) / open)
                # (close - open) / open = (x3.2 - y3.2) / y3.2
                x3.2 = close
                y3.2 = open
        d1=6
    """
    df, g = _to_pd(ohlcv)

    #
    close = df["close"]
    open = df["open"]
    volume = df["volume"]

    #
    x4_1 = volume
    x3_1 = np.log(x4_1)
    x2_1 = delta(g, x3_1, 2)
    #
    x2_2 = (close - open) / open
    #
    x1 = rank(x2_1, df[OHLCV_DATE_COL])
    y1 = rank(x2_2, df[OHLCV_DATE_COL])
    df[VALUE_COL] = -1 * ts_corr(g, x1, y1, 6)

    #
    return _to_pl(df)


@register_factor("alpha003")
def alpha003(ohlcv: pl.DataFrame) -> pl.DataFrame:
    """(-1 * correlation(rank(open),rank(volume),10))"""
    df, g = _to_pd(ohlcv)

    #
    open = df["open"]
    volume = df["volume"]

    #
    x1 = rank(open, df[OHLCV_DATE_COL])
    y1 = rank(volume, df[OHLCV_DATE_COL])
    df[VALUE_COL] = -1 * ts_corr(g, x1, y1, 10)

    #
    return _to_pl(df)


if __name__ == "__main__":
    from data.loader.data_loader import stock_load_process
    from data.universe import get_universe
    from factors.factors import get_factor_fn, list_factors
    from portfolio.alpha_config import FactorConfig
    from portfolio.pipeline import compute_daily_returns, compute_next_day_returns

    available_factors = list_factors()
    print(f"Available factors: {available_factors}")

    # Interactive factor selection
    factor_name = input(
        f"Enter factor name (or press Enter for '{available_factors[0]}'): "
    ).strip()
    if not factor_name:
        factor_name = available_factors[-1]

    if factor_name not in available_factors:
        print(f"Error: '{factor_name}' not found. Available: {available_factors}")
        exit(1)

    print(f"\nUsing factor: {factor_name}")

    import inspect

    # list factors and their parameters
    factor_cls = get_factor_fn(factor_name)
    print(f"\nFactor parameters for {factor_name}:")
    for name, p in inspect.signature(factor_cls).parameters.items():
        # if name in ("ohlcv", "kwargs"):
        if name in ("kwargs"):
            continue
        default = f" = {p.default}" if p.default is not inspect.Parameter.empty else ""
        print(f"  {name}{default}")

    UNIVERSE = get_universe("US_LARGE_CAP_50")
    START_DATE = "2026-01-01"
    END_DATE = "2026-02-28"

    ohlcv = (
        stock_load_process(tickers=UNIVERSE, start_date=START_DATE, end_date=END_DATE)
        .filter(pl.col("volume") != 0)
        .collect()
    )
    print(
        f"OHLCV: {ohlcv.shape[0]:,} rows, {ohlcv.select('ticker').n_unique()} tickers"
    )

    # Precompute shared data
    daily_returns = compute_daily_returns(ohlcv)
    next_day_returns = compute_next_day_returns(daily_returns)
    alpha_factor = get_factor_fn(factor_name)(ohlcv, factor_config=FactorConfig())
    concatenated_pd = daily_returns.join(alpha_factor, on=["date", "ticker"])

    print(f"Daily returns: {daily_returns.shape[0]:,} rows")
    print(f"alpha factor: {alpha_factor.shape[0]:,} rows")

    print(concatenated_pd.head(5))

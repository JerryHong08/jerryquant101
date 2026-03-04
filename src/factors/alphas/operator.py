"""WorldQuant Alpha 101 — composable pandas operators.

Conventions
----------
- ``x``  : pd.Series (a single column, already sorted by [ticker, date]).
- ``g``  : groupby object by ticker, e.g. ``df.groupby("ticker")``.
- ``d``  : lookback window (int).
- Cross-sectional ops (``rank``, ``scale``) group by the date index.

Reference: https://arxiv.org/abs/1601.00991  Table 2 & 3
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ── element-wise ──────────────────────────────────────────────────────────────


def delay(g, x: pd.Series, d: int) -> pd.Series:
    """Value of *x* d days ago (per ticker)."""
    return g.apply(lambda _: x.loc[_.index].shift(d), include_groups=False).droplevel(0)


def sign(x: pd.Series) -> pd.Series:
    return np.sign(x)


def signed_power(x: pd.Series, a: float) -> pd.Series:
    """sign(x) · |x|^a."""
    return np.sign(x) * np.abs(x) ** a


def delta(g, x: pd.Series, d: int) -> pd.Series:
    """x - delay(x,d)."""
    return x - delay(g, x, d)


# ── cross-sectional (per date) ────────────────────────────────────────────────


def rank(x: pd.Series, date: pd.Series) -> pd.Series:
    """Cross-sectional percentile rank (0–1) per date."""
    return x.groupby(date).rank(pct=True)


# ── time-series (per ticker) ──────────────────────────────────────────────────
# ts_* ops accept a GroupBy object `g` (grouped by ticker) and a Series `x`
# that shares the same index.  The rolling is applied per group.


def ts_corr(g, x: pd.Series, y: pd.Series, d: int) -> pd.Series:
    """correlation(x, y, d) = time-serial correlation of x and y for the past d days"""
    return g.apply(
        lambda _: x.loc[_.index].rolling(d, min_periods=d).corr(y.loc[_.index]),
        include_groups=False,
    ).droplevel(
        0
    )  # drop TICKER_COL


def _ts_apply(g, x: pd.Series, d: int, fn, raw: bool = True) -> pd.Series:
    """Apply a custom rolling function per-group."""
    return g.apply(
        lambda _: x.loc[_.index].rolling(d, min_periods=d).apply(fn, raw=raw),
        include_groups=False,
    ).droplevel(0)


def ts_argmax(g, x: pd.Series, d: int) -> pd.Series:
    """Position (0-based from window start) of max in last d bars."""
    return _ts_apply(g, x, d, np.argmax)


def ts_argmin(g, x: pd.Series, d: int) -> pd.Series:
    return _ts_apply(g, x, d, np.argmin)

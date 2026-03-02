"""
Shared pytest fixtures for quant101 test suite.

Provides synthetic market data fixtures used across test modules:
- Return arrays (daily, with known statistical properties)
- Factor DataFrames (date × ticker × value, Polars format)
- Weight DataFrames for portfolio analytics
- OHLCV-like price data
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl
import pytest

# ── Constants ─────────────────────────────────────────────────────────────────

N_DAYS = 252  # 1 year of trading days
N_TICKERS = 10
TICKERS = [f"TICK_{i:02d}" for i in range(N_TICKERS)]
SEED = 42


# ── Return Arrays ─────────────────────────────────────────────────────────────


@pytest.fixture
def rng() -> np.random.Generator:
    """Seeded random generator for reproducibility."""
    return np.random.default_rng(SEED)


@pytest.fixture
def daily_returns(rng: np.random.Generator) -> np.ndarray:
    """252 days of synthetic daily returns (mean ~5% annual, vol ~16%)."""
    mu_daily = 0.05 / 252
    sigma_daily = 0.16 / np.sqrt(252)
    return rng.normal(mu_daily, sigma_daily, size=N_DAYS)


@pytest.fixture
def zero_returns() -> np.ndarray:
    """Returns that are all zero (edge case)."""
    return np.zeros(N_DAYS)


@pytest.fixture
def positive_returns(rng: np.random.Generator) -> np.ndarray:
    """Returns that are consistently positive (strong strategy, small noise)."""
    return rng.normal(0.001, 0.0001, size=N_DAYS)


@pytest.fixture
def negative_returns(rng: np.random.Generator) -> np.ndarray:
    """Returns that are consistently negative (losing strategy, small noise)."""
    return rng.normal(-0.001, 0.0001, size=N_DAYS)


@pytest.fixture
def drawdown_returns() -> np.ndarray:
    """Returns with a known 10% drawdown in the middle.

    100 days flat → 10 days of -1% each → 142 days flat.
    Peak-to-trough = 1 - 0.99^10 ≈ 9.56%.
    """
    rets = np.zeros(N_DAYS)
    rets[100:110] = -0.01  # 10 consecutive -1% days
    return rets


# ── Date Arrays ───────────────────────────────────────────────────────────────


@pytest.fixture
def trading_dates() -> np.ndarray:
    """Array of 252 business dates starting 2024-01-02."""
    start = dt.date(2024, 1, 2)
    dates = []
    d = start
    while len(dates) < N_DAYS:
        if d.weekday() < 5:  # Mon-Fri
            dates.append(d)
        d += dt.timedelta(days=1)
    return np.array(dates)


# ── Factor DataFrames ─────────────────────────────────────────────────────────


@pytest.fixture
def factor_df(rng: np.random.Generator, trading_dates: np.ndarray) -> pl.DataFrame:
    """Synthetic factor DataFrame (date, ticker, value).

    10 tickers × 252 days = 2520 rows with random signal values.
    """
    rows = []
    for d in trading_dates:
        for t in TICKERS:
            rows.append({"date": d, "ticker": t, "value": float(rng.standard_normal())})
    return pl.DataFrame(rows)


@pytest.fixture
def factor_df_with_outliers(factor_df: pl.DataFrame) -> pl.DataFrame:
    """Factor DataFrame with a few extreme outlier values injected."""
    return factor_df.with_columns(
        pl.when(pl.col("ticker") == "TICK_00")
        .then(pl.col("value") * 100)  # blow up one ticker
        .otherwise(pl.col("value"))
        .alias("value")
    )


@pytest.fixture
def sector_df() -> pl.DataFrame:
    """Sector mapping: first 5 tickers → 'Tech', last 5 → 'Fin'."""
    return pl.DataFrame(
        {
            "ticker": TICKERS,
            "sector": ["Tech"] * 5 + ["Fin"] * 5,
        }
    )


# ── Weight DataFrames ─────────────────────────────────────────────────────────


@pytest.fixture
def equal_weights(trading_dates: np.ndarray) -> pl.DataFrame:
    """Equal-weight portfolio: 1/N for all tickers, all dates."""
    w = 1.0 / N_TICKERS
    rows = []
    for d in trading_dates:
        for t in TICKERS:
            rows.append({"date": d, "ticker": t, "weight": w})
    return pl.DataFrame(rows)


@pytest.fixture
def drifting_weights(
    rng: np.random.Generator, trading_dates: np.ndarray
) -> pl.DataFrame:
    """Weights that drift randomly over time (generates turnover)."""
    rows = []
    prev_w = np.ones(N_TICKERS) / N_TICKERS
    for d in trading_dates:
        noise = rng.normal(0, 0.01, size=N_TICKERS)
        w = prev_w + noise
        w = np.abs(w)
        w /= w.sum()
        for j, t in enumerate(TICKERS):
            rows.append({"date": d, "ticker": t, "weight": float(w[j])})
        prev_w = w
    return pl.DataFrame(rows)


# ── Turnover Arrays ──────────────────────────────────────────────────────────


@pytest.fixture
def constant_turnover() -> np.ndarray:
    """Constant daily turnover of 2% (one-way)."""
    return np.full(N_DAYS, 0.02)

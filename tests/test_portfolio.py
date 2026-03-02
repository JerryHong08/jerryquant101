"""
Tests for portfolio.pipeline — unit tests for each pipeline stage.

Tests use minimal synthetic data to verify correctness of:
    - compute_daily_returns
    - compute_next_day_returns
    - compute_portfolio_return
    - resample_weights
    - build_factor_pipeline (smoke only — needs OHLCV with indicators)
    - run_alpha_pipeline (smoke only)

These are *fast* unit tests (no I/O, no real data).
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl
import pytest

# ── Fixtures ──────────────────────────────────────────────────────────────────

N_DAYS = 60
TICKERS = ["AAPL", "MSFT", "GOOG"]
SEED = 42


@pytest.fixture
def rng() -> np.random.Generator:
    return np.random.default_rng(SEED)


@pytest.fixture
def dates() -> list[dt.date]:
    start = dt.date(2024, 1, 2)
    out = []
    d = start
    while len(out) < N_DAYS:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


@pytest.fixture
def ohlcv(rng: np.random.Generator, dates: list[dt.date]) -> pl.DataFrame:
    """Minimal synthetic OHLCV with 3 tickers and 60 days."""
    rows = []
    for ticker in TICKERS:
        price = 100.0
        for d in dates:
            ret = rng.normal(0.0005, 0.02)
            price *= 1 + ret
            rows.append(
                {
                    "timestamps": d,
                    "ticker": ticker,
                    "open": price * 0.999,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "volume": int(rng.integers(1_000_000, 10_000_000)),
                }
            )
    return pl.DataFrame(rows)


@pytest.fixture
def daily_returns_df(dates: list[dt.date], rng: np.random.Generator) -> pl.DataFrame:
    """Pre-built daily returns DataFrame."""
    rows = []
    for d in dates:
        for t in TICKERS:
            rows.append(
                {
                    "date": d,
                    "ticker": t,
                    "daily_return": float(rng.normal(0, 0.02)),
                }
            )
    return pl.DataFrame(rows)


@pytest.fixture
def weights_df(dates: list[dt.date]) -> pl.DataFrame:
    """Equal weights for 3 tickers across 60 dates."""
    rows = []
    for d in dates:
        for t in TICKERS:
            rows.append({"date": d, "ticker": t, "weight": 1.0 / len(TICKERS)})
    return pl.DataFrame(rows)


@pytest.fixture
def next_day_returns_df(dates: list[dt.date], rng: np.random.Generator) -> pl.DataFrame:
    """Synthetic next-day returns."""
    rows = []
    for d in dates:
        for t in TICKERS:
            rows.append(
                {
                    "date": d,
                    "ticker": t,
                    "next_day_return": float(rng.normal(0.0005, 0.02)),
                }
            )
    return pl.DataFrame(rows)


# ── Test compute_daily_returns ────────────────────────────────────────────────


class TestComputeDailyReturns:
    def test_output_schema(self, ohlcv: pl.DataFrame):
        from portfolio.pipeline import compute_daily_returns

        result = compute_daily_returns(ohlcv)
        assert set(result.columns) == {"date", "ticker", "daily_return"}

    def test_no_null_returns(self, ohlcv: pl.DataFrame):
        from portfolio.pipeline import compute_daily_returns

        result = compute_daily_returns(ohlcv)
        assert result.filter(pl.col("daily_return").is_null()).height == 0

    def test_row_count(self, ohlcv: pl.DataFrame):
        from portfolio.pipeline import compute_daily_returns

        result = compute_daily_returns(ohlcv)
        # First day per ticker has no previous close → dropped
        expected = len(TICKERS) * (N_DAYS - 1)
        assert result.height == expected

    def test_return_values_plausible(self, ohlcv: pl.DataFrame):
        from portfolio.pipeline import compute_daily_returns

        result = compute_daily_returns(ohlcv)
        returns = result["daily_return"].to_numpy()
        # Daily returns should be small (< 50% in absolute value)
        assert np.all(np.abs(returns) < 0.5)


# ── Test compute_next_day_returns ────────────────────────────────────────────


class TestComputeNextDayReturns:
    def test_output_schema(self, daily_returns_df: pl.DataFrame):
        from portfolio.pipeline import compute_next_day_returns

        result = compute_next_day_returns(daily_returns_df)
        assert "next_day_return" in result.columns

    def test_shift_is_correct(self):
        """Verify the shift: day 0's next_day_return should be day 1's return."""
        from portfolio.pipeline import compute_next_day_returns

        df = pl.DataFrame(
            {
                "date": [dt.date(2024, 1, 1), dt.date(2024, 1, 2), dt.date(2024, 1, 3)],
                "ticker": ["A", "A", "A"],
                "daily_return": [0.01, 0.02, 0.03],
            }
        )
        result = compute_next_day_returns(df)
        # Day 0 → next_day_return should be 0.02
        # Day 1 → next_day_return should be 0.03
        # Day 2 → dropped (no next day)
        assert result.height == 2
        vals = result.sort("date")["next_day_return"].to_list()
        assert abs(vals[0] - 0.02) < 1e-10
        assert abs(vals[1] - 0.03) < 1e-10

    def test_drops_last_day(self, daily_returns_df: pl.DataFrame):
        from portfolio.pipeline import compute_next_day_returns

        result = compute_next_day_returns(daily_returns_df)
        # Last day per ticker has null next-day → dropped
        assert result.height == len(TICKERS) * (N_DAYS - 1)


# ── Test resample_weights ────────────────────────────────────────────────────


class TestResampleWeights:
    def test_no_resampling_when_frequency_is_1(self, weights_df: pl.DataFrame):
        from portfolio.pipeline import resample_weights

        result = resample_weights(weights_df, rebal_every_n=1)
        assert result.height == weights_df.height

    def test_fewer_unique_weight_dates(self, weights_df: pl.DataFrame):
        from portfolio.pipeline import resample_weights

        result = resample_weights(weights_df, rebal_every_n=5)
        # All dates should still be present (forward-filled)
        original_dates = weights_df["date"].unique().sort()
        result_dates = result["date"].unique().sort()
        assert result_dates.to_list() == original_dates.to_list()

    def test_weight_values_from_rebal_dates(self, dates: list[dt.date]):
        """Weights on non-rebal dates should match the most recent rebal date."""
        from portfolio.pipeline import resample_weights

        # Create weights that change each day
        rows = []
        for i, d in enumerate(dates):
            for t in TICKERS:
                rows.append({"date": d, "ticker": t, "weight": float(i)})
        w = pl.DataFrame(rows)

        result = resample_weights(w, rebal_every_n=5)
        # Day 0: rebal, weight = 0
        # Day 1: carry forward from day 0, weight = 0
        # Day 4: carry forward from day 0, weight = 0
        # Day 5: rebal, weight = 5
        day_1 = result.filter(
            (pl.col("date") == dates[1]) & (pl.col("ticker") == TICKERS[0])
        )["weight"].to_list()
        assert day_1[0] == 0.0  # Carried forward from day 0

        day_5 = result.filter(
            (pl.col("date") == dates[5]) & (pl.col("ticker") == TICKERS[0])
        )["weight"].to_list()
        assert day_5[0] == 5.0  # Rebalanced

    def test_schema_preserved(self, weights_df: pl.DataFrame):
        from portfolio.pipeline import resample_weights

        result = resample_weights(weights_df, rebal_every_n=5)
        assert set(result.columns) == {"date", "ticker", "weight"}


# ── Test compute_portfolio_return ────────────────────────────────────────────


class TestComputePortfolioReturn:
    def test_output_schema(
        self, weights_df: pl.DataFrame, next_day_returns_df: pl.DataFrame
    ):
        from portfolio.pipeline import compute_portfolio_return

        result = compute_portfolio_return(weights_df, next_day_returns_df)
        assert set(result.columns) == {"date", "port_return"}

    def test_equal_weight_return_is_mean(self):
        """With equal weights, portfolio return == mean of stock returns."""
        from portfolio.pipeline import compute_portfolio_return

        d = dt.date(2024, 1, 1)
        weights = pl.DataFrame(
            {
                "date": [d, d, d],
                "ticker": ["A", "B", "C"],
                "weight": [1 / 3, 1 / 3, 1 / 3],
            }
        )
        returns = pl.DataFrame(
            {
                "date": [d, d, d],
                "ticker": ["A", "B", "C"],
                "next_day_return": [0.03, 0.06, -0.03],
            }
        )
        result = compute_portfolio_return(weights, returns)
        expected = (0.03 + 0.06 - 0.03) / 3
        assert abs(result["port_return"][0] - expected) < 1e-10

    def test_zero_weights_zero_return(self):
        from portfolio.pipeline import compute_portfolio_return

        d = dt.date(2024, 1, 1)
        weights = pl.DataFrame(
            {
                "date": [d, d],
                "ticker": ["A", "B"],
                "weight": [0.0, 0.0],
            }
        )
        returns = pl.DataFrame(
            {
                "date": [d, d],
                "ticker": ["A", "B"],
                "next_day_return": [0.10, -0.05],
            }
        )
        result = compute_portfolio_return(weights, returns)
        assert abs(result["port_return"][0]) < 1e-10

    def test_sorted_by_date(
        self, weights_df: pl.DataFrame, next_day_returns_df: pl.DataFrame
    ):
        from portfolio.pipeline import compute_portfolio_return

        result = compute_portfolio_return(weights_df, next_day_returns_df)
        dates_list = result["date"].to_list()
        assert dates_list == sorted(dates_list)


# ── Test factor registry ────────────────────────────────────────────────────


class TestFactorRegistry:
    def test_list_factors_returns_defaults(self):
        from portfolio.pipeline import list_factors

        factors = list_factors()
        assert "bbiboll" in factors
        assert "vol_ratio" in factors
        assert "momentum" in factors

    def test_register_custom_factor(self):
        from portfolio.pipeline import list_factors, register_factor

        def dummy_factor(ohlcv, **kwargs):
            return pl.DataFrame({"date": [], "ticker": [], "value": []})

        register_factor("test_factor_xyz", dummy_factor)
        assert "test_factor_xyz" in list_factors()

    def test_unknown_factor_raises(self, ohlcv: pl.DataFrame):
        from portfolio.pipeline import build_factor_pipeline

        with pytest.raises(KeyError, match="Unknown factor"):
            build_factor_pipeline(ohlcv, factor_names=["nonexistent_factor_abc"])


# ── Test universe module ─────────────────────────────────────────────────────


class TestUniverse:
    def test_get_default_universe(self):
        from data.universe import DEFAULT_UNIVERSE, get_universe

        tickers = get_universe("US_LARGE_CAP_50")
        assert isinstance(tickers, list)
        assert tickers == DEFAULT_UNIVERSE
        assert all(isinstance(t, str) for t in tickers)

    def test_get_named_universe(self):
        from data.universe import get_universe

        tickers = get_universe("US_LARGE_CAP_52")
        assert len(tickers) == 52

    def test_case_insensitive(self):
        from data.universe import get_universe

        t1 = get_universe("us_large_cap_50")
        t2 = get_universe("US_LARGE_CAP_50")
        assert t1 == t2

    def test_unknown_universe_raises(self):
        from data.universe import get_universe

        with pytest.raises(KeyError, match="Unknown universe"):
            get_universe("NONEXISTENT_UNIVERSE")

    def test_list_universes(self):
        from data.universe import list_universes

        names = list_universes()
        assert "US_LARGE_CAP_50" in names
        assert "US_LARGE_CAP_52" in names

    def test_register_universe(self):
        from data.universe import get_universe, register_universe

        register_universe("test_univ", ["SPY", "QQQ"])
        assert get_universe("test_univ") == ["SPY", "QQQ"]

    def test_sector_mapping(self):
        from data.universe import US_LARGE_CAP_50_SECTORS

        assert isinstance(US_LARGE_CAP_50_SECTORS, dict)
        assert "AAPL" in US_LARGE_CAP_50_SECTORS
        # All tickers in the universe should have a sector
        from data.universe import US_LARGE_CAP_50

        for ticker in US_LARGE_CAP_50:
            assert ticker in US_LARGE_CAP_50_SECTORS

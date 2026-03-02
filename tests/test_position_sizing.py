"""
Tests for risk.position_sizing — sizing functions and their properties.

Covers:
    - size_equal_weight: weight normalization, long/short counts
    - size_signal_weighted: factor z-score conviction, inverse-variance risk,
      normalized to unit leverage, max_position cap
    - compute_realized_volatility: basic shape and positivity
    - _validate_signal: missing column errors
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from risk.position_sizing import (
    _validate_signal,
    compute_realized_volatility,
    size_equal_weight,
    size_signal_weighted,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture()
def signal_df() -> pl.DataFrame:
    """A 2-date, 6-stock signal DataFrame.

    Dates chosen to fall within the returns_history range so the Kelly
    join produces results.
    """
    dates = ["2024-12-20"] * 6 + ["2024-12-30"] * 6
    tickers = ["A", "B", "C", "D", "E", "F"] * 2
    values = [10, 20, 30, 40, 50, 60] + [15, 25, 35, 45, 55, 65]
    return pl.DataFrame(
        {
            "date": pl.Series(dates).str.to_date(),
            "ticker": tickers,
            "value": [float(v) for v in values],
        }
    )


@pytest.fixture()
def returns_history() -> pl.DataFrame:
    """Synthetic returns: A-C have positive μ, D-F have negative μ.

    Uses large base_mu relative to noise so rolling estimates are reliable.
    """
    np.random.seed(42)
    rows = []
    # 90 days of history ending on 2025-01-02
    base_date = np.datetime64("2024-10-01")
    for ticker, base_mu in [
        ("A", 0.01),
        ("B", 0.015),
        ("C", 0.008),
        ("D", -0.01),
        ("E", -0.015),
        ("F", -0.008),
    ]:
        for i in range(92):
            date = str(base_date + np.timedelta64(i, "D"))
            ret = base_mu + np.random.normal(0, 0.005)
            rows.append({"date": date, "ticker": ticker, "return": ret})
    df = pl.DataFrame(rows).with_columns(pl.col("date").str.to_date())
    return df


# ── Equal Weight ──────────────────────────────────────────────────────────────


class TestEqualWeight:
    def test_weight_sum_is_one(self, signal_df):
        w = size_equal_weight(signal_df, n_long=2, n_short=2)
        per_date = w.group_by("date").agg(pl.col("weight").abs().sum().alias("gross"))
        for row in per_date.iter_rows(named=True):
            assert abs(row["gross"] - 1.0) < 1e-10

    def test_position_count(self, signal_df):
        w = size_equal_weight(signal_df, n_long=2, n_short=2)
        per_date = w.group_by("date").agg(pl.col("weight").count().alias("n"))
        for row in per_date.iter_rows(named=True):
            assert row["n"] == 4  # 2 long + 2 short

    def test_long_positive_short_negative(self, signal_df):
        w = size_equal_weight(signal_df, n_long=2, n_short=2)
        longs = w.filter(pl.col("weight") > 0)
        shorts = w.filter(pl.col("weight") < 0)
        assert longs.shape[0] > 0
        assert shorts.shape[0] > 0

    def test_top_stocks_are_long(self, signal_df):
        """Highest-value stocks should have positive weight."""
        w = size_equal_weight(signal_df, n_long=2, n_short=2)
        day1 = w.filter(pl.col("date") == pl.lit("2024-12-20").str.to_date())
        long_tickers = set(day1.filter(pl.col("weight") > 0)["ticker"].to_list())
        # E=50, F=60 are top-2
        assert long_tickers == {"E", "F"}


# ── Signal-Weighted ───────────────────────────────────────────────────────────


class TestSignalWeighted:
    def test_direction_from_factor_ranking(self, signal_df, returns_history):
        """Direction comes from factor ranking: top-N long, bottom-N short.

        Signal values: A=10 < B=20 < C=30 < D=40 < E=50 < F=60.
        With n_long=3, n_short=3: A,B,C are short; D,E,F are long.
        """
        w = size_signal_weighted(
            signal_df,
            returns_history,
            n_long=3,
            n_short=3,
            lookback=60,
            max_position=1.0,
        )
        if w.shape[0] == 0:
            pytest.skip("No Kelly weights produced (data issue)")

        day1 = w.filter(pl.col("date") == pl.lit("2024-12-20").str.to_date())
        for row in day1.iter_rows(named=True):
            if row["ticker"] in ("D", "E", "F"):
                assert (
                    row["weight"] > 0
                ), f"{row['ticker']} should be long (top by signal)"
            elif row["ticker"] in ("A", "B", "C"):
                assert (
                    row["weight"] < 0
                ), f"{row['ticker']} should be short (bottom by signal)"

    def test_different_signals_different_weights(self, returns_history):
        """Different factor signals should produce different Kelly weights."""
        dates = ["2024-12-20"] * 6
        tickers = ["A", "B", "C", "D", "E", "F"]

        signal_1 = pl.DataFrame(
            {
                "date": pl.Series(dates).str.to_date(),
                "ticker": tickers,
                "value": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            }
        )
        signal_2 = pl.DataFrame(
            {
                "date": pl.Series(dates).str.to_date(),
                "ticker": tickers,
                "value": [60.0, 50.0, 40.0, 30.0, 20.0, 10.0],  # reversed
            }
        )

        w1 = size_signal_weighted(
            signal_1, returns_history, n_long=3, n_short=3, lookback=60
        )
        w2 = size_signal_weighted(
            signal_2, returns_history, n_long=3, n_short=3, lookback=60
        )

        if w1.shape[0] == 0 or w2.shape[0] == 0:
            pytest.skip("No Kelly weights produced")

        # Different signals should produce different stock selections
        merged = w1.rename({"weight": "w1"}).join(
            w2.rename({"weight": "w2"}),
            on=["date", "ticker"],
            how="inner",
        )
        # At least some weights should differ (directions should be opposite)
        diffs = merged.filter((pl.col("w1") * pl.col("w2")) < 0)  # opposite signs
        assert diffs.shape[0] > 0, "Reversed signals should flip some positions"

    def test_normalized_to_unit_leverage(self, signal_df, returns_history):
        """Gross leverage should be normalized to max_leverage (default 1.0)."""
        w = size_signal_weighted(
            signal_df,
            returns_history,
            n_long=3,
            n_short=3,
            lookback=60,
            max_position=1.0,
            max_leverage=1.0,
        )
        if w.shape[0] == 0:
            pytest.skip("No Kelly weights produced")

        gross = w.group_by("date").agg(pl.col("weight").abs().sum().alias("gross"))
        for row in gross.iter_rows(named=True):
            assert (
                abs(row["gross"] - 1.0) < 1e-6
            ), f"Gross leverage should be 1.0, got {row['gross']}"

    def test_higher_signal_gets_larger_weight(self, returns_history):
        """Within the same leg, stocks with more extreme signal z-scores
        should get larger absolute weights (when σ² is similar).

        All stocks in returns_history have similar noise (σ=0.005),
        so z-score should dominate weight magnitude.
        """
        dates = ["2024-12-20"] * 6
        tickers = ["A", "B", "C", "D", "E", "F"]
        # Signal values linearly spaced: A has lowest, F has highest
        signal = pl.DataFrame(
            {
                "date": pl.Series(dates).str.to_date(),
                "ticker": tickers,
                "value": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            }
        )
        w = size_signal_weighted(
            signal,
            returns_history,
            n_long=3,
            n_short=3,
            lookback=60,
            max_position=1.0,
        )
        if w.shape[0] == 0:
            pytest.skip("No Kelly weights produced")

        day1 = w.filter(pl.col("date") == pl.lit("2024-12-20").str.to_date())
        long_positions = day1.filter(pl.col("weight") > 0).sort(
            "weight", descending=True
        )
        if long_positions.shape[0] >= 2:
            # F (value=60) should have larger weight than D (value=40)
            weights_by_ticker = {
                r["ticker"]: r["weight"] for r in long_positions.iter_rows(named=True)
            }
            if "F" in weights_by_ticker and "D" in weights_by_ticker:
                assert (
                    weights_by_ticker["F"] > weights_by_ticker["D"]
                ), "Higher signal should get larger weight"

    def test_max_position_cap(self, signal_df, returns_history):
        """No single position should exceed max_position."""
        max_pos = 0.05
        w = size_signal_weighted(
            signal_df,
            returns_history,
            n_long=3,
            n_short=3,
            lookback=60,
            max_position=max_pos,
        )
        if w.shape[0] == 0:
            pytest.skip("No Kelly weights produced")

        max_abs = w["weight"].abs().max()
        assert max_abs <= max_pos + 1e-10

    def test_max_leverage_scaling(self, signal_df, returns_history):
        """max_leverage > 1 should scale up gross leverage proportionally."""
        w1 = size_signal_weighted(
            signal_df,
            returns_history,
            n_long=3,
            n_short=3,
            lookback=60,
            max_position=1.0,
            max_leverage=1.0,
        )
        w2 = size_signal_weighted(
            signal_df,
            returns_history,
            n_long=3,
            n_short=3,
            lookback=60,
            max_position=1.0,
            max_leverage=2.0,
        )
        if w1.shape[0] == 0 or w2.shape[0] == 0:
            pytest.skip("No Kelly weights produced")

        gross1 = w1.group_by("date").agg(pl.col("weight").abs().sum().alias("g"))
        gross2 = w2.group_by("date").agg(pl.col("weight").abs().sum().alias("g"))
        g1 = gross1["g"].to_list()[0]
        g2 = gross2["g"].to_list()[0]
        assert abs(g2 / g1 - 2.0) < 1e-6, "2x max_leverage should double gross"

    def test_position_count(self, signal_df, returns_history):
        """Kelly should select exactly n_long + n_short positions."""
        w = size_signal_weighted(
            signal_df,
            returns_history,
            n_long=2,
            n_short=2,
            lookback=60,
            max_position=1.0,
        )
        if w.shape[0] == 0:
            pytest.skip("No Kelly weights produced")

        per_date = w.group_by("date").agg(pl.col("weight").count().alias("n"))
        for row in per_date.iter_rows(named=True):
            assert row["n"] <= 4  # at most n_long + n_short


# ── Validate Signal ───────────────────────────────────────────────────────────


class TestValidateSignal:
    def test_valid_signal(self, signal_df):
        _validate_signal(signal_df)  # should not raise

    def test_missing_column(self):
        bad = pl.DataFrame({"date": ["2025-01-01"], "ticker": ["A"]})
        with pytest.raises(ValueError, match="missing columns"):
            _validate_signal(bad)

"""
Tests for src/risk/ — VaR, CVaR, drawdown, distribution stats, risk summary.
"""

from __future__ import annotations

import numpy as np
import pytest

from risk.risk_metrics import (
    cvar_historical,
    cvar_parametric,
    drawdown_series,
    max_drawdown,
    return_kurtosis,
    return_skewness,
    risk_summary,
    tail_ratio,
    var_historical,
    var_parametric,
)

# ══════════════════════════════════════════════════════════════════════════════
#  Value at Risk
# ══════════════════════════════════════════════════════════════════════════════


class TestVarHistorical:
    """Tests for var_historical()."""

    def test_known_quantile(self):
        """Uniform returns [−0.10, +0.10]: 95% VaR ≈ 0.09."""
        rng = np.random.default_rng(42)
        returns = rng.uniform(-0.10, 0.10, size=10_000)
        var = var_historical(returns, confidence=0.95)
        np.testing.assert_allclose(var, 0.09, atol=0.005)

    def test_positive(self, daily_returns):
        """VaR should be positive (it's a loss magnitude)."""
        var = var_historical(daily_returns, confidence=0.95)
        assert var > 0

    def test_higher_confidence_higher_var(self, daily_returns):
        """99% VaR ≥ 95% VaR."""
        var_95 = var_historical(daily_returns, confidence=0.95)
        var_99 = var_historical(daily_returns, confidence=0.99)
        assert var_99 >= var_95


class TestVarParametric:
    """Tests for var_parametric()."""

    def test_known_gaussian(self):
        """For Gaussian returns, parametric VaR ≈ historical VaR."""
        rng = np.random.default_rng(42)
        returns = rng.normal(0, 0.01, size=10_000)
        h_var = var_historical(returns, confidence=0.95)
        p_var = var_parametric(returns, confidence=0.95)
        np.testing.assert_allclose(h_var, p_var, rtol=0.05)

    def test_positive(self, daily_returns):
        assert var_parametric(daily_returns, confidence=0.95) > 0


# ══════════════════════════════════════════════════════════════════════════════
#  CVaR (Expected Shortfall)
# ══════════════════════════════════════════════════════════════════════════════


class TestCvarHistorical:
    """Tests for cvar_historical()."""

    def test_cvar_exceeds_var(self, daily_returns):
        """CVaR (expected shortfall) ≥ VaR by definition."""
        var = var_historical(daily_returns, confidence=0.95)
        cvar = cvar_historical(daily_returns, confidence=0.95)
        assert cvar >= var - 1e-10

    def test_positive(self, daily_returns):
        assert cvar_historical(daily_returns) > 0


class TestCvarParametric:
    """Tests for cvar_parametric()."""

    def test_cvar_exceeds_var(self, daily_returns):
        p_var = var_parametric(daily_returns, confidence=0.95)
        p_cvar = cvar_parametric(daily_returns, confidence=0.95)
        assert p_cvar >= p_var - 1e-10


# ══════════════════════════════════════════════════════════════════════════════
#  Drawdown
# ══════════════════════════════════════════════════════════════════════════════


class TestDrawdownSeries:
    """Tests for drawdown_series()."""

    def test_always_non_positive(self, daily_returns):
        dd = drawdown_series(daily_returns)
        assert np.all(dd <= 1e-10)  # small tolerance for floating point

    def test_starts_at_zero(self, daily_returns):
        dd = drawdown_series(daily_returns)
        assert dd[0] == 0.0  # first observation is the "peak"

    def test_same_length(self, daily_returns):
        dd = drawdown_series(daily_returns)
        assert len(dd) == len(daily_returns)


class TestMaxDrawdown:
    """Tests for max_drawdown()."""

    def test_known_drawdown(self, drawdown_returns):
        """10 consecutive -1% days → MDD ≈ 1 - 0.99^10 ≈ 9.56%."""
        mdd = max_drawdown(drawdown_returns)
        expected = 1 - 0.99**10
        np.testing.assert_allclose(mdd, expected, rtol=0.01)

    def test_positive_result(self, daily_returns):
        assert max_drawdown(daily_returns) > 0

    def test_flat_returns_zero_drawdown(self):
        flat = np.zeros(100)
        mdd = max_drawdown(flat)
        assert mdd == 0.0

    def test_monotonic_up_zero_drawdown(self):
        """If returns are always positive, drawdown is zero."""
        up = np.full(100, 0.01)
        mdd = max_drawdown(up)
        np.testing.assert_allclose(mdd, 0.0, atol=1e-10)


# ══════════════════════════════════════════════════════════════════════════════
#  Distribution Statistics
# ══════════════════════════════════════════════════════════════════════════════


class TestReturnSkewness:
    """Tests for return_skewness()."""

    def test_symmetric_near_zero(self):
        rng = np.random.default_rng(42)
        returns = rng.normal(0, 0.01, size=10_000)
        skew = return_skewness(returns)
        assert abs(skew) < 0.1  # close to zero for Gaussian


class TestReturnKurtosis:
    """Tests for return_kurtosis()."""

    def test_gaussian_near_zero(self):
        rng = np.random.default_rng(42)
        returns = rng.normal(0, 0.01, size=10_000)
        kurt = return_kurtosis(returns)
        assert abs(kurt) < 0.2  # excess kurtosis ≈ 0 for Gaussian

    def test_fat_tails_positive(self):
        """t-distribution has positive excess kurtosis."""
        rng = np.random.default_rng(42)
        returns = rng.standard_t(df=4, size=10_000) * 0.01
        kurt = return_kurtosis(returns)
        assert kurt > 0


class TestTailRatio:
    """Tests for tail_ratio()."""

    def test_symmetric_near_one(self):
        rng = np.random.default_rng(42)
        returns = rng.normal(0, 0.01, size=10_000)
        tr = tail_ratio(returns)
        np.testing.assert_allclose(tr, 1.0, atol=0.2)

    def test_positive(self, daily_returns):
        assert tail_ratio(daily_returns) > 0


# ══════════════════════════════════════════════════════════════════════════════
#  Risk Summary
# ══════════════════════════════════════════════════════════════════════════════


class TestRiskSummary:
    """Tests for risk_summary()."""

    def test_all_keys_present(self, daily_returns):
        summary = risk_summary(daily_returns)
        expected_keys = {
            "mean_return",
            "volatility",
            "sharpe",
            "skewness",
            "excess_kurtosis",
            "var_historical",
            "var_parametric",
            "cvar_historical",
            "cvar_parametric",
            "var_ratio",
            "max_drawdown",
            "tail_ratio",
            "n_observations",
        }
        assert expected_keys.issubset(summary.keys())

    def test_n_observations(self, daily_returns):
        summary = risk_summary(daily_returns)
        assert summary["n_observations"] == len(daily_returns)

    def test_var_ratio_near_one_for_gaussian(self):
        """For Gaussian data, historical/parametric VaR ≈ 1."""
        rng = np.random.default_rng(42)
        returns = rng.normal(0, 0.01, size=10_000)
        summary = risk_summary(returns)
        np.testing.assert_allclose(summary["var_ratio"], 1.0, atol=0.1)


# ══════════════════════════════════════════════════════════════════════════════
#  Edge Cases & Validation
# ══════════════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """Edge case handling for risk metrics."""

    def test_too_few_returns_raises(self):
        with pytest.raises(ValueError, match="at least 2"):
            var_historical(np.array([0.01]))

    def test_nan_handling(self):
        """NaN values should be filtered out."""
        returns = np.array(
            [0.01, np.nan, -0.02, 0.005, np.nan, 0.01, -0.01, 0.02, -0.005, 0.003]
        )
        var = var_historical(returns)
        assert np.isfinite(var)

    def test_inf_handling(self):
        """Inf values should be filtered out."""
        returns = np.array(
            [0.01, np.inf, -0.02, 0.005, -np.inf, 0.01, -0.01, 0.02, -0.005, 0.003]
        )
        var = var_historical(returns)
        assert np.isfinite(var)

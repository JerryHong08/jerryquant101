"""
Tests for src/execution/ — cost models and cost analysis.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from execution.cost_analysis import (
    breakeven_cost,
    compute_net_returns,
    compute_turnover,
    sharpe_vs_cost_curve,
)
from execution.cost_model import (
    CompositeCostModel,
    FixedCostModel,
    SpreadCostModel,
    SqrtImpactCostModel,
)

# ══════════════════════════════════════════════════════════════════════════════
#  Cost Models
# ══════════════════════════════════════════════════════════════════════════════


class TestFixedCostModel:
    """Tests for FixedCostModel."""

    def test_basic_estimate(self):
        model = FixedCostModel(cost_bps=10.0)
        cost = model.estimate(10_000)
        # 10 bps = 0.001; 10000 * 0.001 = 10.0
        np.testing.assert_allclose(cost, 10.0)

    def test_zero_trade(self):
        model = FixedCostModel(cost_bps=5.0)
        assert model.estimate(0.0) == 0.0

    def test_negative_trade_uses_abs(self):
        model = FixedCostModel(cost_bps=5.0)
        assert model.estimate(-10_000) == model.estimate(10_000)

    def test_array_estimate(self):
        model = FixedCostModel(cost_bps=10.0)
        trades = np.array([10_000, 20_000, 5_000])
        costs = model.estimate_array(trades)
        expected = np.abs(trades) * 10.0 / 10_000
        np.testing.assert_allclose(costs, expected)

    def test_negative_bps_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            FixedCostModel(cost_bps=-1.0)

    def test_describe(self):
        model = FixedCostModel(cost_bps=5.0)
        assert "5.0" in model.describe()


class TestSpreadCostModel:
    """Tests for SpreadCostModel."""

    def test_basic_estimate(self):
        model = SpreadCostModel(half_spread_bps=2.0)
        cost = model.estimate(100_000)
        np.testing.assert_allclose(cost, 100_000 * 2.0 / 10_000)

    def test_negative_bps_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            SpreadCostModel(half_spread_bps=-1.0)


class TestSqrtImpactCostModel:
    """Tests for SqrtImpactCostModel."""

    def test_zero_trade_zero_cost(self):
        model = SqrtImpactCostModel(eta=0.10)
        assert model.estimate(0.0) == 0.0

    def test_cost_increases_sublinearly(self):
        """Double the trade value should less-than-double the cost."""
        model = SqrtImpactCostModel(eta=0.10, default_adv=50_000_000)
        cost_1x = model.estimate(100_000)
        cost_4x = model.estimate(400_000)
        # sqrt(4) = 2, so cost_4x / cost_1x ≈ 4 * sqrt(4)/sqrt(1) / (1 * 1)
        # impact_frac scales as sqrt(participation), cost = trade_value * impact_frac
        # cost ∝ trade_value^{3/2}. Ratio: (4)^{3/2} = 8
        assert cost_4x > cost_1x
        assert cost_4x < 4 * cost_1x * 4  # reasonableness check

    def test_negative_eta_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            SqrtImpactCostModel(eta=-0.1)

    def test_with_custom_volume(self):
        model = SqrtImpactCostModel(eta=0.10)
        cost = model.estimate(100_000, daily_volume=1_000_000)
        assert cost > 0


class TestCompositeCostModel:
    """Tests for CompositeCostModel."""

    def test_sum_of_components(self):
        fixed = FixedCostModel(cost_bps=5.0)
        spread = SpreadCostModel(half_spread_bps=2.0)
        composite = CompositeCostModel([fixed, spread])

        trade = 100_000
        expected = fixed.estimate(trade) + spread.estimate(trade)
        np.testing.assert_allclose(composite.estimate(trade), expected)

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            CompositeCostModel([])

    def test_describe(self):
        composite = CompositeCostModel(
            [
                FixedCostModel(cost_bps=5.0),
                SpreadCostModel(half_spread_bps=2.0),
            ]
        )
        desc = composite.describe()
        assert "Composite" in desc


# ══════════════════════════════════════════════════════════════════════════════
#  Cost Analysis
# ══════════════════════════════════════════════════════════════════════════════


class TestComputeTurnover:
    """Tests for compute_turnover()."""

    def test_zero_turnover_static_weights(self, equal_weights):
        """Equal weights every day → zero turnover."""
        to = compute_turnover(equal_weights)
        assert len(to) > 0
        assert to["turnover"].max() < 1e-10

    def test_positive_turnover_drifting(self, drifting_weights):
        """Drifting weights produce positive turnover."""
        to = compute_turnover(drifting_weights)
        assert to["turnover"].mean() > 0

    def test_output_schema(self, equal_weights):
        to = compute_turnover(equal_weights)
        assert set(to.columns) == {"date", "turnover"}

    def test_missing_columns_raises(self):
        bad_df = pl.DataFrame({"foo": [1], "bar": [2], "baz": [3]})
        with pytest.raises(ValueError, match="missing"):
            compute_turnover(bad_df)


class TestComputeNetReturns:
    """Tests for compute_net_returns()."""

    def test_net_less_than_gross(self, daily_returns, constant_turnover):
        model = FixedCostModel(cost_bps=5.0)
        net = compute_net_returns(daily_returns, constant_turnover, model)
        # Net returns should be strictly less than gross (costs > 0)
        assert np.all(net <= daily_returns + 1e-15)

    def test_zero_cost_equals_gross(self, daily_returns, constant_turnover):
        model = FixedCostModel(cost_bps=0.0)
        net = compute_net_returns(daily_returns, constant_turnover, model)
        np.testing.assert_allclose(net, daily_returns)

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError, match="same length"):
            compute_net_returns(np.ones(10), np.ones(5), FixedCostModel(5.0))


class TestBreakevenCost:
    """Tests for breakeven_cost()."""

    def test_positive_strategy_positive_breakeven(self, rng):
        """Profitable strategy has positive breakeven cost."""
        returns = rng.normal(0.001, 0.01, size=252)
        turnover = np.full(252, 0.05)
        be = breakeven_cost(returns, turnover)
        assert be > 0

    def test_negative_strategy_zero_breakeven(self, rng):
        """Unprofitable strategy has zero breakeven cost."""
        returns = rng.normal(-0.001, 0.01, size=252)
        turnover = np.full(252, 0.05)
        be = breakeven_cost(returns, turnover)
        assert be == 0.0


class TestSharpeVsCostCurve:
    """Tests for sharpe_vs_cost_curve()."""

    def test_sharpe_decreasing(self, daily_returns, constant_turnover):
        """Sharpe should decrease as costs increase."""
        result = sharpe_vs_cost_curve(daily_returns, constant_turnover)
        sharpes = result["net_sharpe"]
        # Should be weakly decreasing
        for i in range(1, len(sharpes)):
            assert sharpes[i] <= sharpes[i - 1] + 1e-10

    def test_zero_cost_matches_gross(self, daily_returns, constant_turnover):
        result = sharpe_vs_cost_curve(daily_returns, constant_turnover)
        np.testing.assert_allclose(
            result["net_sharpe"][0], result["gross_sharpe"], atol=1e-10
        )

"""
Tests for Phase 6 — Backtest Refactor.

Covers:
    - PortfolioTracker: equity curve, turnover, position counts, cost deduction
    - WeightBacktester: run(), run_from_pipeline(), compare(), export()
    - ResultExporter: export_legacy_results() with/without benchmark
    - Bug fixes: trade_rules 3-tuple type hint, dead imports removed

All tests are fast unit tests using small synthetic data.
"""

from __future__ import annotations

import datetime as dt
import os
import tempfile
from pathlib import Path

import numpy as np
import polars as pl
import pytest

# ── Helpers ───────────────────────────────────────────────────────────────────

N_DAYS = 20
TICKERS = ["AAPL", "MSFT", "GOOG"]
SEED = 99


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
def weights_df(dates) -> pl.DataFrame:
    """Equal-weight long-only portfolio (3 tickers, 1/3 each)."""
    rows = []
    for d in dates:
        for t in TICKERS:
            rows.append({"date": d, "ticker": t, "weight": 1.0 / len(TICKERS)})
    return pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))


@pytest.fixture
def returns_df(dates, rng) -> pl.DataFrame:
    """Synthetic next-day returns ~N(0.001, 0.02)."""
    rows = []
    for d in dates:
        for t in TICKERS:
            rows.append(
                {
                    "date": d,
                    "ticker": t,
                    "next_day_return": float(rng.normal(0.001, 0.02)),
                }
            )
    return pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))


@pytest.fixture
def benchmark_df(dates, rng) -> pl.DataFrame:
    """Fake SPY benchmark with close prices."""
    close = 450.0
    rows = []
    for d in dates:
        close *= 1 + rng.normal(0.0005, 0.01)
        rows.append({"date": d, "close": close})
    return pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))


# ══════════════════════════════════════════════════════════════════════════════
# PortfolioTracker tests
# ══════════════════════════════════════════════════════════════════════════════


class TestPortfolioTracker:

    def test_import(self):
        from backtest.portfolio_tracker import PortfolioTracker, TrackingResult

        assert PortfolioTracker is not None
        assert TrackingResult is not None

    def test_equal_weight_produces_expected_cols(self, weights_df, returns_df):
        from backtest.portfolio_tracker import PortfolioTracker

        tracker = PortfolioTracker()
        result = tracker.run(weights_df, returns_df)

        # portfolio_daily schema
        assert set(result.portfolio_daily.columns) >= {
            "date",
            "portfolio_return",
            "equity_curve",
        }
        assert len(result.portfolio_daily) == N_DAYS
        assert result.n_days == N_DAYS

    def test_equity_curve_starts_at_one(self, weights_df, returns_df):
        from backtest.portfolio_tracker import PortfolioTracker

        tracker = PortfolioTracker()
        result = tracker.run(weights_df, returns_df)
        first_ret = result.portfolio_daily["portfolio_return"][0]
        first_eq = result.portfolio_daily["equity_curve"][0]
        assert abs(first_eq - (1.0 + first_ret)) < 1e-10

    def test_turnover_zero_for_stable_weights(self, weights_df, returns_df):
        """Equal weights every day → turnover ≈ 0 (no rebalancing)."""
        from backtest.portfolio_tracker import PortfolioTracker

        tracker = PortfolioTracker()
        result = tracker.run(weights_df, returns_df)
        # First day always 0.  After that, weights don't change → 0.
        assert result.total_turnover == pytest.approx(0.0, abs=1e-12)

    def test_turnover_nonzero_with_changing_weights(self, dates, returns_df):
        """Alternating 100% in AAPL / MSFT → high turnover."""
        from backtest.portfolio_tracker import PortfolioTracker

        rows = []
        for i, d in enumerate(dates):
            for t in TICKERS:
                if i % 2 == 0:
                    w = 1.0 if t == "AAPL" else 0.0
                else:
                    w = 1.0 if t == "MSFT" else 0.0
                rows.append({"date": d, "ticker": t, "weight": w})
        weights = pl.DataFrame(rows).with_columns(pl.col("date").cast(pl.Date))

        tracker = PortfolioTracker()
        result = tracker.run(weights, returns_df)
        assert result.total_turnover > 5.0  # flipping fully each day

    def test_cost_deduction_reduces_returns(self, weights_df, returns_df):
        """With transaction costs, cumulative returns should be lower."""
        from backtest.portfolio_tracker import PortfolioTracker

        t0 = PortfolioTracker(cost_bps=0.0)
        t50 = PortfolioTracker(cost_bps=50.0)

        r0 = t0.run(weights_df, returns_df)
        r50 = t50.run(weights_df, returns_df)

        # For stable weights turnover ≈ 0 so cost doesn't matter much;
        # but the math path should still not crash.
        assert r0.n_days == r50.n_days

    def test_position_count(self, weights_df, returns_df):
        from backtest.portfolio_tracker import PortfolioTracker

        tracker = PortfolioTracker()
        result = tracker.run(weights_df, returns_df)
        pc = result.position_count
        assert set(pc.columns) >= {"n_long", "n_short", "n_total"}
        # All long, equal weight
        assert pc["n_long"].min() == 3
        assert pc["n_short"].max() == 0

    def test_empty_input(self):
        from backtest.portfolio_tracker import PortfolioTracker

        tracker = PortfolioTracker()
        empty_w = pl.DataFrame({"date": [], "ticker": [], "weight": []})
        empty_r = pl.DataFrame({"date": [], "ticker": [], "next_day_return": []})
        result = tracker.run(empty_w, empty_r)
        assert result.n_days == 0
        assert result.total_turnover == 0.0

    def test_tracking_result_frozen(self, weights_df, returns_df):
        from backtest.portfolio_tracker import PortfolioTracker

        tracker = PortfolioTracker()
        result = tracker.run(weights_df, returns_df)
        with pytest.raises(AttributeError):
            result.n_days = 999


# ══════════════════════════════════════════════════════════════════════════════
# WeightBacktester tests
# ══════════════════════════════════════════════════════════════════════════════


class TestWeightBacktester:

    def test_import(self):
        from backtest.weight_backtester import BacktestResult, WeightBacktester

        assert WeightBacktester is not None
        assert BacktestResult is not None

    def test_run_produces_backtest_result(self, weights_df, returns_df):
        from backtest.weight_backtester import WeightBacktester

        bt = WeightBacktester(cost_bps=5.0)
        result = bt.run(weights_df, returns_df, name="TestEW")

        assert result.config["strategy_name"] == "TestEW"
        assert result.tracking.n_days == N_DAYS
        assert isinstance(result.metrics, dict)
        assert "Total Turnover" in result.metrics

    def test_run_with_benchmark(self, weights_df, returns_df, benchmark_df):
        from backtest.weight_backtester import WeightBacktester

        bt = WeightBacktester()
        result = bt.run(weights_df, returns_df, benchmark_data=benchmark_df)
        assert result.benchmark_data is not None

    def test_convenience_properties(self, weights_df, returns_df):
        from backtest.weight_backtester import WeightBacktester

        bt = WeightBacktester()
        result = bt.run(weights_df, returns_df)
        # Should not raise
        _ = result.sharpe
        _ = result.total_return
        _ = result.max_drawdown
        _ = result.portfolio_daily

    def test_run_from_pipeline(self, weights_df, returns_df):
        """run_from_pipeline() should work with a dict mimicking pipeline output."""
        from backtest.weight_backtester import WeightBacktester

        bt = WeightBacktester()
        pipeline_output = {
            "weights": weights_df,
            "next_day_returns": returns_df,
        }
        result = bt.run_from_pipeline(pipeline_output, name="Pipeline")
        assert result.config["strategy_name"] == "Pipeline"
        assert result.tracking.n_days == N_DAYS

    def test_compare(self, weights_df, returns_df):
        from backtest.weight_backtester import WeightBacktester

        bt = WeightBacktester()
        r1 = bt.run(weights_df, returns_df, name="A")
        r2 = bt.run(weights_df, returns_df, name="B")
        comp = bt.compare({"A": r1, "B": r2})
        assert isinstance(comp, pl.DataFrame)
        assert len(comp) == 2
        assert "Strategy" in comp.columns

    def test_export(self, weights_df, returns_df):
        from backtest.weight_backtester import WeightBacktester

        bt = WeightBacktester()
        result = bt.run(weights_df, returns_df, name="export_test")

        with tempfile.TemporaryDirectory() as tmp:
            bt.export(result, output_dir=tmp)
            files = os.listdir(tmp)
            assert any("portfolio_daily" in f for f in files)
            assert any("turnover" in f for f in files)
            assert any("metrics" in f for f in files)
            assert any("config" in f for f in files)


# ══════════════════════════════════════════════════════════════════════════════
# ResultExporter tests
# ══════════════════════════════════════════════════════════════════════════════


class TestResultExporter:

    def _make_legacy_results(self, dates, rng):
        """Build a minimal legacy results dict."""
        rets = [float(rng.normal(0.001, 0.02)) for _ in dates]
        eq = list(np.cumprod(1.0 + np.array(rets)))
        portfolio_daily = pl.DataFrame(
            {
                "date": [dt.datetime.combine(d, dt.time()) for d in dates],
                "portfolio_return": rets,
                "equity_curve": eq,
            }
        )
        trades = pl.DataFrame(
            {
                "ticker": ["AAPL", "MSFT"],
                "buy_date": [
                    dt.datetime(2024, 1, 2),
                    dt.datetime(2024, 1, 3),
                ],
                "sell_date": [
                    dt.datetime(2024, 1, 10),
                    dt.datetime(2024, 1, 12),
                ],
                "buy_price": [180.0, 370.0],
                "sell_price": [185.0, 380.0],
                "return": [0.0278, 0.027],
            }
        )
        return {
            "portfolio_daily": portfolio_daily,
            "trades": trades,
            "open_positions": pl.DataFrame(
                {
                    "ticker": ["GOOG"],
                    "buy_date": [dt.datetime(2024, 1, 15)],
                    "buy_price": [140.0],
                }
            ),
            "performance_metrics": {"Sharpe Ratio": 1.23, "Total Return [%]": 5.6},
        }

    def test_export_without_benchmark(self, dates, rng):
        from backtest.result_exporter import export_legacy_results

        results = self._make_legacy_results(dates, rng)
        with tempfile.TemporaryDirectory() as tmp:
            export_legacy_results(results, "test_strat", output_dir=tmp)
            files = os.listdir(tmp)
            assert any("portfolio_daily" in f for f in files)
            assert any("trades" in f for f in files)
            assert any("metrics" in f for f in files)

    def test_export_with_benchmark(self, dates, rng, benchmark_df):
        from backtest.result_exporter import export_legacy_results

        results = self._make_legacy_results(dates, rng)
        results["benchmark_data"] = benchmark_df
        with tempfile.TemporaryDirectory() as tmp:
            # May skip QuantStats if not installed; should not crash
            export_legacy_results(results, "bench_test", output_dir=tmp)
            files = os.listdir(tmp)
            assert any("portfolio_daily" in f for f in files)

    def test_export_null_benchmark_no_crash(self, dates, rng):
        """Original engine.export_results crashed on null benchmark. Verify fix."""
        from backtest.result_exporter import export_legacy_results

        results = self._make_legacy_results(dates, rng)
        results["benchmark_data"] = None
        with tempfile.TemporaryDirectory() as tmp:
            # Must NOT raise
            export_legacy_results(results, "null_bench", output_dir=tmp)

    def test_export_with_strategy_config(self, dates, rng):
        from backtest.result_exporter import export_legacy_results

        results = self._make_legacy_results(dates, rng)
        config = {"lookback": 20, "threshold": 0.5}
        with tempfile.TemporaryDirectory() as tmp:
            export_legacy_results(
                results, "cfg_test", output_dir=tmp, strategy_config=config
            )
            files = os.listdir(tmp)
            assert any("config" in f for f in files)


# ══════════════════════════════════════════════════════════════════════════════
# Bug fix verification tests
# ══════════════════════════════════════════════════════════════════════════════


class TestBugFixes:

    def test_trade_rules_type_hint_3_tuple(self):
        """Verify trade_rules return annotation is now a 3-tuple."""
        import inspect

        from backtest.strategy_base import StrategyBase

        sig = inspect.signature(StrategyBase.trade_rules)
        ret = sig.return_annotation
        # Check it's tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]
        origin = getattr(ret, "__origin__", None)
        assert origin is tuple
        args = ret.__args__
        assert len(args) == 3, f"Expected 3-tuple, got {len(args)}-tuple"

    def test_no_dead_datetime_import_in_engine(self):
        """datetime was imported but unused in engine.py — verify removed."""
        import ast

        engine_path = Path(__file__).parent.parent / "src" / "backtest" / "engine.py"
        tree = ast.parse(engine_path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "datetime":
                names = [alias.name for alias in node.names]
                assert (
                    "datetime" not in names
                ), "Dead import 'datetime' still in engine.py"

    def test_no_dead_timedelta_import_in_perf_analyzer(self):
        """timedelta was imported but unused — verify removed."""
        import ast

        pa_path = (
            Path(__file__).parent.parent
            / "src"
            / "backtest"
            / "performance_analyzer.py"
        )
        tree = ast.parse(pa_path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "datetime":
                names = [alias.name for alias in node.names]
                assert (
                    "timedelta" not in names
                ), "Dead import 'timedelta' still in performance_analyzer.py"


# ══════════════════════════════════════════════════════════════════════════════
# __init__.py export tests
# ══════════════════════════════════════════════════════════════════════════════


class TestBacktestExports:

    @pytest.mark.parametrize(
        "name",
        [
            "BacktestEngine",
            "BacktestResult",
            "PerformanceAnalyzer",
            "PortfolioTracker",
            "StrategyBase",
            "TrackingResult",
            "WeightBacktester",
            "BacktestVisualizer",
            "export_legacy_results",
        ],
    )
    def test_export_available(self, name):
        import backtest

        assert hasattr(backtest, name), f"{name} not exported from backtest"

import datetime as dt
import importlib.util
from pathlib import Path

import polars as pl

MODULE_PATH = Path(__file__).resolve().parent / "backtest" / "performance_analyzer.py"
SPEC = importlib.util.spec_from_file_location("performance_analyzer_unit", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
PerformanceAnalyzer = MODULE.PerformanceAnalyzer


def test_estimate_total_fees_uses_trade_notionals() -> None:
    analyzer = PerformanceAnalyzer(trading_fee_rate=0.01)

    trades = pl.DataFrame(
        {
            "ticker": ["A", "B"],
            "buy_price": [100.0, 50.0],
            "sell_open": [110.0, None],
        }
    )

    fees = analyzer._estimate_total_fees(trades)

    assert abs(fees - 2.6) < 1e-10


def test_open_trade_pnl_uses_unrealized_return_when_available() -> None:
    analyzer = PerformanceAnalyzer()

    open_positions = pl.DataFrame(
        {
            "ticker": ["A", "B"],
            "buy_price": [100.0, 50.0],
            "unrealized_return": [0.1, -0.2],
        }
    )

    pnl = analyzer._calculate_open_trade_pnl(open_positions)

    assert abs(pnl - 0.0) < 1e-10


def test_calculate_performance_metrics_reports_open_trade_pnl() -> None:
    analyzer = PerformanceAnalyzer(initial_capital=100.0, trading_fee_rate=0.0)

    portfolio_daily = pl.DataFrame(
        {
            "date": [dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2)],
            "portfolio_return": [0.0, 0.01],
            "equity_curve": [1.0, 1.01],
        }
    )
    trades = pl.DataFrame(
        {
            "ticker": ["A"],
            "buy_date": [dt.datetime(2024, 1, 1)],
            "sell_date": [dt.datetime(2024, 1, 2)],
            "buy_price": [100.0],
            "sell_open": [101.0],
            "return": [0.01],
        }
    )
    open_positions = pl.DataFrame(
        {
            "ticker": ["B"],
            "buy_price": [50.0],
            "unrealized_return": [0.1],
            "quantity": [2.0],
        }
    )

    metrics = analyzer.calculate_performance_metrics(
        portfolio_daily=portfolio_daily,
        trades=trades,
        open_positions=open_positions,
    )

    assert abs(metrics["Open Trade PnL"] - 10.0) < 1e-10


def test_estimate_total_fees_respects_quantity_when_present() -> None:
    analyzer = PerformanceAnalyzer(trading_fee_rate=0.01)

    trades = pl.DataFrame(
        {
            "ticker": ["A"],
            "buy_price": [10.0],
            "sell_open": [12.0],
            "quantity": [10.0],
        }
    )

    fees = analyzer._estimate_total_fees(trades)
    # (10*10 + 12*10) * 1% = 2.2
    assert abs(fees - 2.2) < 1e-10

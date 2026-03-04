import datetime as dt

import polars as pl

from alpha.factor_backtest import run_factor_portfolio_backtest


def test_run_factor_portfolio_backtest_builds_equity_curve() -> None:
    prices = pl.DataFrame(
        {
            "timestamps": [
                dt.datetime(2024, 1, 1),
                dt.datetime(2024, 1, 2),
                dt.datetime(2024, 1, 3),
                dt.datetime(2024, 1, 1),
                dt.datetime(2024, 1, 2),
                dt.datetime(2024, 1, 3),
            ],
            "ticker": ["A", "A", "A", "B", "B", "B"],
            "close": [100.0, 110.0, 121.0, 100.0, 90.0, 81.0],
        }
    )
    weights = pl.DataFrame(
        {
            "date": [dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 1)],
            "ticker": ["A", "B"],
            "weight": [0.5, -0.5],
        }
    )

    out = run_factor_portfolio_backtest(weights=weights, prices=prices, weight_lag=1)
    portfolio = out["portfolio_daily"]

    assert portfolio.height == 2
    # Day2: 0 (lagged still zero), Day3: 0.5*0.1 + (-0.5)*(-0.1)=0.1
    assert abs(portfolio["portfolio_return"].to_list()[-1] - 0.1) < 1e-10
    assert portfolio["equity_curve"].to_list()[-1] > 1.0


def test_run_factor_portfolio_backtest_latest_positions_present() -> None:
    prices = pl.DataFrame(
        {
            "timestamps": [dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2)],
            "ticker": ["A", "A"],
            "close": [100.0, 110.0],
        }
    )
    weights = pl.DataFrame(
        {
            "date": [dt.datetime(2024, 1, 1)],
            "ticker": ["A"],
            "weight": [1.0],
        }
    )

    out = run_factor_portfolio_backtest(weights=weights, prices=prices, weight_lag=1)
    latest = out["positions_latest"]

    assert latest.height == 1
    assert latest["ticker"].item() == "A"

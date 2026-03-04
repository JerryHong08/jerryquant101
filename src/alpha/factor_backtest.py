"""
Factor Portfolio Backtest Utilities.

Bridges alpha-factor weights to a portfolio return stream so the factor
pipeline can be evaluated with the same performance layer used by
strategy-based backtests.
"""

from typing import Dict

import polars as pl


def run_factor_portfolio_backtest(
    weights: pl.DataFrame,
    prices: pl.DataFrame,
    weight_lag: int = 1,
) -> Dict[str, pl.DataFrame]:
    """
    Backtest a cross-sectional factor portfolio from (date, ticker, weight).

    Args:
        weights: DataFrame with columns (date, ticker, weight).
        prices: DataFrame with columns (timestamps, ticker, close).
        weight_lag: Lag (in bars) between weight signal and applied return.

    Returns:
        Dict with:
            - portfolio_daily: (date, portfolio_return, gross_exposure, equity_curve)
            - positions_latest: latest-date non-zero applied positions
    """
    required_w = {"date", "ticker", "weight"}
    required_p = {"timestamps", "ticker", "close"}
    if missing := required_w - set(weights.columns):
        raise ValueError(f"weights missing columns: {missing}")
    if missing := required_p - set(prices.columns):
        raise ValueError(f"prices missing columns: {missing}")

    returns = (
        prices.sort(["ticker", "timestamps"])
        .with_columns(
            (pl.col("close") / pl.col("close").shift(1).over("ticker") - 1).alias(
                "asset_return"
            )
        )
        .rename({"timestamps": "date"})
        .select(["date", "ticker", "close", "asset_return"])
    )

    panel = (
        returns.join(weights, on=["date", "ticker"], how="left")
                .sort(["ticker", "date"])
        .with_columns(
            pl.col("weight").forward_fill().over("ticker").fill_null(0.0).alias("weight_ff")
        )
        .with_columns(
            pl.col("weight_ff")
            .shift(weight_lag)
            .over("ticker")
            .fill_null(0.0)
            .alias("w_eff")
        )
        .with_columns((pl.col("w_eff") * pl.col("asset_return")).alias("contribution"))
    )

    portfolio_daily = (
        panel.filter(pl.col("asset_return").is_not_null())
        .group_by("date")
        .agg(
            [
                pl.col("contribution").sum().alias("portfolio_return"),
                pl.col("w_eff").abs().sum().alias("gross_exposure"),
            ]
        )
        .sort("date")
        .with_columns((1 + pl.col("portfolio_return")).cum_prod().alias("equity_curve"))
    )

    latest_date = panel["date"].max() if panel.height > 0 else None
    positions_latest = (
        panel.filter(pl.col("date") == latest_date)
        .filter(pl.col("w_eff").abs() > 1e-10)
        .select(["date", "ticker", pl.col("w_eff").alias("weight"), "close"])
        if latest_date is not None
        else pl.DataFrame(schema={"date": pl.Datetime, "ticker": pl.Utf8, "weight": pl.Float64, "close": pl.Float64})
    )

    return {
        "portfolio_daily": portfolio_daily,
        "positions_latest": positions_latest,
    }

"""
Position Sizing — From signal to portfolio weights.

A factor tells you *which* stocks to favor.  Position sizing tells you
*how much* of each stock to hold.  This is where alpha research meets
risk management.

Methods:
    1. Equal-weight: 1/N per position.  Simple, no estimation risk.
    2. Inverse-volatility: Weight ∝ 1/σ_i.  Equal risk contribution.
    3. Volatility-target: Scale portfolio to a target volatility.
    4. Half-Kelly: Optimal growth rate, halved for safety.

All functions take a Polars DataFrame with factor signals and return
a DataFrame with portfolio weights.

Convention:
    - Input: DataFrame with (date, ticker, value) — the factor signal.
    - Output: DataFrame with (date, ticker, weight) — portfolio weights.
    - Long-short: Positive weight = long, negative weight = short.
    - Weights are normalized: sum(|weight|) = 1.0 per date (gross leverage = 1).

Usage:
    from risk.position_sizing import size_equal_weight, size_volatility_target

    weights = size_equal_weight(signal_df, n_long=10, n_short=10)
    weights = size_volatility_target(signal_df, vol_df, target_vol=0.10)

Reference: docs/quant_lab.tex — Part IV, Chapter 14 (Portfolio Construction)
"""

from typing import Optional

import numpy as np
import polars as pl

# ── Equal Weight ──────────────────────────────────────────────────────────────


def size_equal_weight(
    signal: pl.DataFrame,
    n_long: int = 10,
    n_short: int = 10,
) -> pl.DataFrame:
    """
    Equal-weight long-short portfolio.

    Each day, go long the top-N stocks by signal (highest value) and short
    the bottom-N stocks.  Each position gets weight 1/(n_long + n_short).

    Args:
        signal: DataFrame with columns (date, ticker, value).
        n_long: Number of stocks in the long leg.
        n_short: Number of stocks in the short leg.

    Returns:
        DataFrame with columns (date, ticker, weight).
        Long positions have positive weight, short positions have negative weight.
        Sum of |weights| = 1.0 per date.

    Note:
        Equal-weight ignores signal magnitude and volatility differences.
        Simple and robust — zero estimation risk — but leaves money on the table
        if signal strength or risk varies significantly across stocks.
    """
    _validate_signal(signal)
    total = n_long + n_short

    weights = (
        signal.sort(["date", "value"])
        .with_columns(
            pl.col("value")
            .rank(method="ordinal", descending=False)
            .over("date")
            .alias("rank"),
            pl.col("value").count().over("date").alias("n_stocks"),
        )
        .with_columns(
            pl.when(pl.col("rank") <= n_short)
            .then(-1.0 / total)
            .when(pl.col("rank") > (pl.col("n_stocks") - n_long))
            .then(1.0 / total)
            .otherwise(0.0)
            .alias("weight")
        )
        .filter(pl.col("weight") != 0.0)
        .select(["date", "ticker", "weight"])
    )

    return weights


# ── Inverse Volatility ────────────────────────────────────────────────────────


def size_inverse_volatility(
    signal: pl.DataFrame,
    volatility: pl.DataFrame,
    n_long: int = 10,
    n_short: int = 10,
) -> pl.DataFrame:
    """
    Inverse-volatility weighted long-short portfolio.

    Same stock selection as equal-weight (top-N long, bottom-N short), but
    weights are proportional to 1/σ_i.  This ensures each position contributes
    roughly equal risk to the portfolio.

    Args:
        signal: DataFrame with columns (date, ticker, value).
        volatility: DataFrame with columns (date, ticker, volatility) —
                    realized vol estimate per stock-date (e.g. 20d rolling std).
        n_long: Number of long positions.
        n_short: Number of short positions.

    Returns:
        DataFrame with columns (date, ticker, weight).
        Weights are normalized so sum(|weight|) = 1.0 per date.

    Note:
        Estimation risk comes from the volatility estimate.  Stale or noisy
        vol estimates can produce erratic weights.
    """
    _validate_signal(signal)

    # Select long/short positions
    positions = (
        signal.sort(["date", "value"])
        .with_columns(
            pl.col("value")
            .rank(method="ordinal", descending=False)
            .over("date")
            .alias("rank"),
            pl.col("value").count().over("date").alias("n_stocks"),
        )
        .with_columns(
            pl.when(pl.col("rank") <= n_short)
            .then(pl.lit(-1))  # Short
            .when(pl.col("rank") > (pl.col("n_stocks") - n_long))
            .then(pl.lit(1))  # Long
            .otherwise(pl.lit(0))
            .alias("direction")
        )
        .filter(pl.col("direction") != 0)
        .select(["date", "ticker", "direction"])
    )

    # Join with volatility and compute inverse-vol weights
    weighted = (
        positions.join(volatility, on=["date", "ticker"], how="inner")
        .with_columns(
            (1.0 / pl.col("volatility").clip(lower_bound=1e-8)).alias("inv_vol")
        )
        .with_columns(
            (
                pl.col("direction")
                * pl.col("inv_vol")
                / pl.col("inv_vol").sum().over("date")
            ).alias("weight")
        )
        .select(["date", "ticker", "weight"])
    )

    return weighted


# ── Volatility Target ─────────────────────────────────────────────────────────


def size_volatility_target(
    signal: pl.DataFrame,
    volatility: pl.DataFrame,
    target_vol: float = 0.10,
    n_long: int = 10,
    n_short: int = 10,
    annualization_factor: int = 252,
    max_leverage: float = 3.0,
) -> pl.DataFrame:
    """
    Volatility-targeted position sizing.

    First constructs an inverse-vol portfolio, then scales all weights so that
    the estimated portfolio volatility matches the target.

    This is the simplest form of risk management that actually works in practice.
    Most hedge funds use some variant of vol-targeting.

    Math:
        σ_portfolio ≈ leverage × σ_equal_risk_portfolio
        → leverage = σ_target / σ_estimated
        → weight_i = leverage × (1/σ_i) / Σ(1/σ_j)

    Args:
        signal: DataFrame with columns (date, ticker, value).
        volatility: DataFrame with columns (date, ticker, volatility) —
                    daily volatility estimate per stock-date.
        target_vol: Target annualized portfolio volatility (default 10%).
        n_long: Number of long positions.
        n_short: Number of short positions.
        annualization_factor: Trading days per year.
        max_leverage: Maximum gross leverage cap (safety).

    Returns:
        DataFrame with columns (date, ticker, weight).

    Note:
        The vol estimate is backward-looking, so this is always chasing
        last period's volatility.  During regime shifts (calm → crisis),
        vol-targeting will be too aggressive because it uses the old (low) vol.
    """
    _validate_signal(signal)
    target_daily = target_vol / np.sqrt(annualization_factor)

    # Build inverse-vol base weights
    base_weights = size_inverse_volatility(signal, volatility, n_long, n_short)

    # Estimate portfolio volatility per date
    # Simplified: assume zero correlation, σ_port ≈ sqrt(Σ w_i² σ_i²)
    port_vol = (
        base_weights.join(volatility, on=["date", "ticker"], how="inner")
        .with_columns(
            (pl.col("weight").pow(2) * pl.col("volatility").pow(2)).alias(
                "var_contribution"
            )
        )
        .group_by("date")
        .agg(pl.col("var_contribution").sum().sqrt().alias("port_vol"))
    )

    # Compute leverage multiplier
    scaled = (
        base_weights.join(port_vol, on="date", how="inner")
        .with_columns(
            (target_daily / pl.col("port_vol").clip(lower_bound=1e-8))
            .clip(upper_bound=max_leverage)
            .alias("leverage")
        )
        .with_columns((pl.col("weight") * pl.col("leverage")).alias("weight_new"))
        .select(
            [
                pl.col("date"),
                pl.col("ticker"),
                pl.col("weight_new").alias("weight"),
            ]
        )
    )

    return scaled


# ── Half-Kelly ────────────────────────────────────────────────────────────────


def size_half_kelly(
    signal: pl.DataFrame,
    returns_history: pl.DataFrame,
    lookback: int = 60,
    max_position: float = 0.10,
) -> pl.DataFrame:
    """
    Half-Kelly position sizing.

    Full Kelly maximizes long-run growth rate:
        f* = μ / σ² (for a single asset)

    But full Kelly produces extreme drawdowns (~50% is common).
    Half-Kelly (f*/2) sacrifices ~25% of growth for ~50% less variance.
    This is why practitioners use half-Kelly or even quarter-Kelly.

    For a portfolio, we approximate:
        w_i = 0.5 × (μ_i / σ_i²)

    then normalize so sum(|w|) = 1.

    Args:
        signal: DataFrame with columns (date, ticker, value).
                Used to determine long/short direction only.
        returns_history: DataFrame with columns (date, ticker, return) —
                         historical daily returns for each stock.
        lookback: Number of trailing days for μ and σ estimation.
        max_position: Maximum weight per stock (safety cap).

    Returns:
        DataFrame with columns (date, ticker, weight).

    Warning:
        Kelly sizing requires accurate estimates of μ and σ.  Small errors
        in μ have large effects because Kelly is aggressive.  This is why
        half-Kelly (or less) is standard practice.
    """
    _validate_signal(signal)

    # Compute rolling mean and variance of returns
    stats = (
        returns_history.sort(["ticker", "date"])
        .with_columns(
            [
                pl.col("return")
                .rolling_mean(window_size=lookback)
                .over("ticker")
                .alias("mu"),
                pl.col("return")
                .rolling_var(window_size=lookback)
                .over("ticker")
                .alias("var"),
            ]
        )
        .filter(
            pl.col("mu").is_not_null()
            & pl.col("var").is_not_null()
            & (pl.col("var") > 1e-10)
        )
    )

    # Join signal with stats to get direction
    kelly = (
        signal.join(
            stats.select(["date", "ticker", "mu", "var"]),
            on=["date", "ticker"],
            how="inner",
        )
        .with_columns(
            # Half-Kelly: 0.5 * μ/σ², with sign from the signal
            (0.5 * pl.col("mu") / pl.col("var") * pl.col("value").sign())
            .clip(lower_bound=-max_position, upper_bound=max_position)
            .alias("raw_weight")
        )
        # Normalize: sum(|w|) = 1 per date
        .with_columns(
            pl.col("raw_weight").abs().sum().over("date").alias("total_abs_weight")
        )
        .with_columns(
            pl.when(pl.col("total_abs_weight") > 1e-10)
            .then(pl.col("raw_weight") / pl.col("total_abs_weight"))
            .otherwise(0.0)
            .alias("weight")
        )
        .select(["date", "ticker", "weight"])
        .filter(pl.col("weight").abs() > 1e-10)
    )

    return kelly


# ── Utility: Compute Realized Volatility ──────────────────────────────────────


def compute_realized_volatility(
    ohlcv: pl.DataFrame,
    window: int = 20,
) -> pl.DataFrame:
    """
    Compute rolling realized volatility for each stock.

    Convenience function to produce the volatility DataFrame required
    by inverse_volatility and volatility_target sizing.

    Args:
        ohlcv: DataFrame with columns (timestamps, ticker, close).
        window: Rolling window in days (default 20).

    Returns:
        DataFrame with columns (date, ticker, volatility).
        Volatility is daily standard deviation of log returns.
    """
    vol_df = (
        ohlcv.sort(["ticker", "timestamps"])
        .with_columns(
            (pl.col("close") / pl.col("close").shift(1).over("ticker"))
            .log()
            .alias("log_ret")
        )
        .with_columns(
            pl.col("log_ret")
            .rolling_std(window_size=window)
            .over("ticker")
            .alias("volatility")
        )
        .filter(pl.col("volatility").is_not_null() & pl.col("volatility").is_finite())
        .select(
            [
                pl.col("timestamps").alias("date"),
                pl.col("ticker"),
                pl.col("volatility"),
            ]
        )
    )

    return vol_df


# ── Internal ──────────────────────────────────────────────────────────────────


def _validate_signal(signal: pl.DataFrame) -> None:
    """Validate that a signal DataFrame has the required columns."""
    required = {"date", "ticker", "value"}
    missing = required - set(signal.columns)
    if missing:
        raise ValueError(
            f"Signal DataFrame missing columns: {missing}. "
            f"Expected (date, ticker, value), got {signal.columns}"
        )

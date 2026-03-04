"""
Factor Analyzer — IC, IR, IC decay, and turnover analysis.

This is the core evaluation tool for alpha research.  Given a signal DataFrame
and forward returns, it answers: "does this signal predict future returns?"

The four key metrics:
    - IC (Information Coefficient): Spearman rank correlation between signal and
      forward returns, computed cross-sectionally at each date.
    - IR (Information Ratio): mean(IC) / std(IC) — measures consistency.
    - IC Decay: How IC changes across forecast horizons (reveals natural frequency).
    - Turnover: How much the signal ranking changes day to day.

Usage:
    from alpha.factor_analyzer import FactorAnalyzer

    analyzer = FactorAnalyzer(signal_df, returns_df)
    ic_ts = analyzer.ic_series(horizon=5)
    ir = analyzer.ir(horizon=5)
    decay = analyzer.ic_decay(horizons=[1, 2, 5, 10, 20])
    turnover = analyzer.turnover()
    analyzer.summary(horizon=5)

Reference: docs/quant_lab.tex — Part III, Chapter 10 (Factor Evaluation)
"""

from typing import Dict, List, Optional

import numpy as np
import polars as pl


class FactorAnalyzer:
    """
    Evaluate a cross-sectional factor signal against forward returns.

    Args:
        signal: DataFrame with columns (date, ticker, value).
                The signal value at each date is used to predict forward returns.
        returns: DataFrame with columns (date, ticker, forward_return_1d, ...).
                 Output of compute_forward_returns().
        min_observations: Minimum number of stocks required per date for IC
                          computation (default: 30).
    """

    def __init__(
        self,
        signal: pl.DataFrame,
        returns: pl.DataFrame,
        min_observations: int = 30,
    ):
        self.signal = signal
        self.returns = returns
        self.min_observations = min_observations

        # Merge signal and returns on (date, ticker)
        self._merged = self.signal.join(
            self.returns, on=["date", "ticker"], how="inner"
        )

    def ic_series(self, horizon: int = 5) -> pl.DataFrame:
        """
        Compute the cross-sectional Spearman IC at each date.

        IC_t = spearman_corr(signal_t, forward_return_t)
        computed across all stocks in the universe at date t.

        Args:
            horizon: Forecast horizon in days (matches forward_return_{h}d column).

        Returns:
            DataFrame with columns (date, ic) — one IC value per date.
        """
        return_col = f"forward_return_{horizon}d"
        if return_col not in self._merged.columns:
            raise ValueError(
                f"Column '{return_col}' not found. "
                f"Available: {[c for c in self._merged.columns if c.startswith('forward')]}"
            )

        # Filter rows with valid signal and return values
        valid = self._merged.filter(
            pl.col("value").is_not_null()
            & pl.col("value").is_finite()
            & pl.col(return_col).is_not_null()
            & pl.col(return_col).is_finite()
        )

        # Compute Spearman rank correlation per date
        # Spearman = Pearson correlation of ranks
        ranked = valid.with_columns(
            [
                pl.col("value").rank().over("date").alias("signal_rank"),
                pl.col(return_col).rank().over("date").alias("return_rank"),
            ]
        )

        # Count observations per date and filter
        ranked = ranked.with_columns(
            pl.col("ticker").count().over("date").alias("n_obs")
        ).filter(pl.col("n_obs") >= self.min_observations)

        # Pearson correlation of ranks = Spearman correlation
        ic_df = (
            ranked.group_by("date")
            .agg(pl.corr("signal_rank", "return_rank").alias("ic"))
            .sort("date")
        )

        return ic_df

    def ic_stats(self, horizon: int = 5) -> Dict[str, float]:
        """
        Compute summary statistics of the IC time series.

        Returns:
            Dictionary with keys: mean_ic, std_ic, ir, t_stat, hit_rate, n_dates
        """
        ic_df = self.ic_series(horizon)
        ic_arr = ic_df["ic"].drop_nulls().to_numpy()

        n = len(ic_arr)
        if n == 0:
            return {
                "mean_ic": 0.0,
                "std_ic": 0.0,
                "ir": 0.0,
                "t_stat": 0.0,
                "hit_rate": 0.0,
                "n_dates": 0,
            }

        mean_ic = float(np.mean(ic_arr))
        std_ic = float(np.std(ic_arr, ddof=1))
        ir = mean_ic / std_ic if std_ic > 0 else 0.0
        t_stat = mean_ic / (std_ic / np.sqrt(n)) if std_ic > 0 else 0.0
        hit_rate = float(np.mean(ic_arr > 0)) * 100  # % of dates with positive IC

        return {
            "mean_ic": mean_ic,
            "std_ic": std_ic,
            "ir": ir,
            "t_stat": t_stat,
            "hit_rate": hit_rate,
            "n_dates": n,
        }

    def ir(self, horizon: int = 5) -> float:
        """
        Information Ratio = mean(IC) / std(IC).

        IR > 0.5: strong factor. 0.3-0.5: decent. < 0.3: weak.
        """
        return self.ic_stats(horizon)["ir"]

    def ic_decay(self, horizons: List[int] = [1, 2, 5, 10, 20]) -> pl.DataFrame:
        """
        Compute mean IC at each forecast horizon.

        The IC decay curve reveals the signal's natural trading frequency:
        - Fast decay (IC drops after 1-2 days): short-term signal
        - Slow decay (IC persists for weeks): medium-frequency signal
        - Non-monotonic: possible look-ahead bias — investigate

        Args:
            horizons: List of forecast horizons to evaluate.

        Returns:
            DataFrame with columns (horizon, mean_ic, std_ic, ir, t_stat)
        """
        results = []
        for h in horizons:
            return_col = f"forward_return_{h}d"
            if return_col not in self._merged.columns:
                continue
            stats = self.ic_stats(h)
            results.append(
                {
                    "horizon": h,
                    "mean_ic": stats["mean_ic"],
                    "std_ic": stats["std_ic"],
                    "ir": stats["ir"],
                    "t_stat": stats["t_stat"],
                }
            )

        return pl.DataFrame(results)

    def turnover(self) -> pl.DataFrame:
        """
        Compute daily factor turnover.

        Turnover_t = (1/N) * sum_i |w_{i,t} - w_{i,t-1}|
        where w_{i,t} is the normalized (rank-based) factor weight.

        High turnover → high transaction costs → factor needs higher gross IC
        to be profitable after costs.

        Returns:
            DataFrame with columns (date, turnover, n_stocks)
        """
        # Rank-normalize the signal cross-sectionally at each date
        ranked = self.signal.filter(
            pl.col("value").is_not_null() & pl.col("value").is_finite()
        ).with_columns(
            # Normalize rank to [-1, 1]
            (
                (pl.col("value").rank().over("date") - 1)
                / (pl.col("value").count().over("date") - 1)
                * 2
                - 1
            ).alias("weight")
        )

        # Sort by ticker then date for shift
        ranked = ranked.sort(["ticker", "date"])

        # Compute |w_{i,t} - w_{i,t-1}| per ticker
        ranked = ranked.with_columns(
            (pl.col("weight") - pl.col("weight").shift(1).over("ticker"))
            .abs()
            .alias("weight_change")
        )

        # Aggregate: mean absolute weight change per date
        turnover_df = (
            ranked.filter(pl.col("weight_change").is_not_null())
            .group_by("date")
            .agg(
                [
                    pl.col("weight_change").mean().alias("turnover"),
                    pl.col("ticker").count().alias("n_stocks"),
                ]
            )
            .sort("date")
        )

        return turnover_df

    def quantile_returns(self, horizon: int = 5, n_quantiles: int = 5) -> pl.DataFrame:
        """
        Compute mean forward return by signal quantile (long-short analysis).

        Splits the universe into N quantiles by signal value at each date,
        then computes the mean forward return for each quantile.

        Args:
            horizon: Forecast horizon in days.
            n_quantiles: Number of quantiles (default: 5 = quintiles).

        Returns:
            DataFrame with columns (quantile, mean_return, n_observations)
        """
        return_col = f"forward_return_{horizon}d"
        if return_col not in self._merged.columns:
            raise ValueError(f"Column '{return_col}' not found.")

        valid = self._merged.filter(
            pl.col("value").is_not_null()
            & pl.col("value").is_finite()
            & pl.col(return_col).is_not_null()
            & pl.col(return_col).is_finite()
        )

        # Assign quantiles cross-sectionally as integer bins for stable ordering
        quantiled = valid.with_columns(
            (
                (
                    pl.col("value").rank(method="ordinal", descending=False).over("date")
                    / pl.col("value").count().over("date")
                    * n_quantiles
                )
                .ceil()
                .clip(lower_bound=1, upper_bound=n_quantiles)
                .cast(pl.Int32)
            ).alias("quantile")
        )

        result = (
            quantiled.group_by("quantile")
            .agg(
                [
                    pl.col(return_col).mean().alias("mean_return"),
                    pl.col(return_col).count().alias("n_observations"),
                ]
            )
            .sort("quantile")
        )

        return result

    def summary(self, horizon: int = 5, print_output: bool = True) -> Dict:
        """
        Print a comprehensive factor evaluation summary.

        Args:
            horizon: Forecast horizon for IC analysis.
            print_output: Whether to print the summary.

        Returns:
            Dictionary containing all computed metrics.
        """
        stats = self.ic_stats(horizon)
        turnover_df = self.turnover()
        mean_turnover = (
            turnover_df["turnover"].mean() if not turnover_df.is_empty() else 0.0
        )

        result = {**stats, "mean_turnover": mean_turnover}

        if print_output:
            print(f"\n{'=' * 50}")
            print(f"Factor Evaluation Summary (horizon={horizon}d)")
            print(f"{'=' * 50}")
            print(f"  Mean IC:      {stats['mean_ic']:>10.4f}")
            print(f"  Std IC:       {stats['std_ic']:>10.4f}")
            print(f"  IR:           {stats['ir']:>10.4f}")
            print(f"  t-stat:       {stats['t_stat']:>10.4f}")
            print(f"  Hit Rate:     {stats['hit_rate']:>9.1f}%")
            print(f"  # Dates:      {stats['n_dates']:>10d}")
            print(f"  Mean Turnover:{mean_turnover:>10.4f}")
            print(f"{'=' * 50}")

            # Significance assessment
            if abs(stats["t_stat"]) > 2:
                print(
                    f"  ✅ Statistically significant (|t| = {abs(stats['t_stat']):.2f} > 2)"
                )
            else:
                print(f"  ⚠️  Not significant (|t| = {abs(stats['t_stat']):.2f} < 2)")

            # IR assessment
            ir_val = abs(stats["ir"])
            if ir_val > 0.5:
                print(f"  ✅ Strong factor (|IR| = {ir_val:.2f} > 0.5)")
            elif ir_val > 0.3:
                print(f"  🔶 Decent factor (|IR| = {ir_val:.2f}, 0.3-0.5)")
            else:
                print(f"  ⚠️  Weak factor (|IR| = {ir_val:.2f} < 0.3)")
            print()

        return result

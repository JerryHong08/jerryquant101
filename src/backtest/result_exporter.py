"""
ResultExporter — extracted from BacktestEngine.export_results().

Provides export functionality for both legacy StrategyBase backtests and
weight-based backtests.  All file-writing logic is centralised here so the
engine / backtester classes stay focused on orchestration.

Bugs fixed vs. the original ``engine.export_results()``:
    * Null benchmark no longer crashes (guard added).
    * ``strategy_config`` parameter was positional-before-keyword — now
      keyword-only with a default.

Reference: guidance/quant_lab.pdf — Part III, Chapter 12
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import pandas as pd
import polars as pl


def export_legacy_results(
    results: Dict[str, Any],
    strategy_name: str,
    output_dir: str = "backtest_results",
    *,
    strategy_config: Optional[Dict[str, Any]] = None,
    benchmark_title: str = "SPY",
) -> None:
    """Export results dict produced by ``BacktestEngine.run_backtest()``.

    This is a drop-in replacement for ``BacktestEngine.export_results()``,
    but standalone — no engine instance required.

    Args:
        results: Complete results dict from ``BacktestEngine.run_backtest()``.
        strategy_name: Human-readable strategy name (used in filenames).
        output_dir: Directory to write files to.
        strategy_config: Optional config dict to write alongside results.
        benchmark_title: Title for quantstats benchmark series.
    """
    os.makedirs(output_dir, exist_ok=True)

    portfolio_daily: pl.DataFrame = results["portfolio_daily"]

    # ── QuantStats HTML report ──
    if not portfolio_daily.is_empty():
        benchmark = results.get("benchmark_data")

        qs_returns_pd = pd.Series(
            portfolio_daily.with_columns(pl.col("date").cast(pl.Date).alias("date"))[
                "portfolio_return"
            ],
            index=portfolio_daily.with_columns(
                pl.col("date").cast(pl.Date).alias("date")
            )["date"],
        )

        benchmark_pd = None
        if benchmark is not None and not benchmark.is_empty():
            benchmark = benchmark.with_columns(
                (pl.col("close") / pl.col("close").shift(1) - 1).alias("daily_return"),
                pl.col("date").cast(pl.Date).alias("date"),
            )
            benchmark_pd = pd.Series(benchmark["daily_return"], index=benchmark["date"])

        html_path = os.path.join(output_dir, f"{strategy_name}_report.html")
        try:
            import quantstats as qs

            qs.reports.html(
                qs_returns_pd,
                benchmark=benchmark_pd,
                output=html_path,
                benchmark_title=benchmark_title,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"QuantStats report skipped: {exc}")

    # ── Trades CSV ──
    trades_raw = results.get("trades")
    if trades_raw is not None:
        trades: pl.DataFrame = (
            pl.DataFrame(trades_raw)
            if not isinstance(trades_raw, pl.DataFrame)
            else trades_raw
        )
        if not trades.is_empty():
            trades_export = (
                trades.with_columns(
                    (pl.col("return") * 100).round(2).alias("return %"),
                )
                .sort("return %", descending=True)
                .drop("return")
                .with_columns(
                    pl.col("buy_date").dt.date().alias("buy_date"),
                    pl.col("sell_date").dt.date().alias("sell_date"),
                )
            )
            trades_path = os.path.join(output_dir, f"{strategy_name}_trades.csv")
            trades_export.write_csv(trades_path)
            print(f"trades exported: {trades_path}")

    # ── Open positions CSV ──
    open_positions_raw = results.get("open_positions")
    if open_positions_raw is not None:
        open_positions: pl.DataFrame = (
            pl.DataFrame(open_positions_raw)
            if not isinstance(open_positions_raw, pl.DataFrame)
            else open_positions_raw
        )
        if not open_positions.is_empty():
            open_positions_export = open_positions.with_columns(
                pl.col("buy_date").dt.date().alias("buy_date"),
            ).sort("buy_date")
            open_path = os.path.join(output_dir, f"{strategy_name}_open_positions.csv")
            open_positions_export.write_csv(open_path)
            print(f"open positions exported: {open_path}")

    # ── Portfolio daily CSV ──
    portfolio_path = os.path.join(output_dir, f"{strategy_name}_portfolio_daily.csv")
    portfolio_daily.with_columns(
        pl.col("date").dt.date().alias("date"),
    ).write_csv(portfolio_path)
    print(f"daily portfolio performance exported: {portfolio_path}")

    # ── Metrics TXT ──
    metrics = results.get("performance_metrics", {})
    metrics_path = os.path.join(output_dir, f"{strategy_name}_metrics.txt")
    with open(metrics_path, "w", encoding="utf-8") as fh:
        fh.write(f"{strategy_name} backtest performance metrics\n")
        fh.write("=" * 50 + "\n\n")
        for key, value in metrics.items():
            fh.write(f"{key}: {value}\n")
    print(f"backtest performance metrics exported: {metrics_path}")

    # ── Strategy config TXT ──
    if strategy_config:
        config_path = os.path.join(output_dir, f"{strategy_name}_config.txt")
        with open(config_path, "w", encoding="utf-8") as fh:
            fh.write(f"{strategy_name} strategy config\n")
            fh.write("=" * 50 + "\n\n")
            for key, value in strategy_config.items():
                fh.write(f"{key}: {value}\n")
        print(f"Strategy config exported: {config_path}")

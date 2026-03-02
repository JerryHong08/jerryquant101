"""
Backtest runner — unified entry point for all backtest modes.

Supports two modes:
    1. **Strategy mode** (legacy): ``run_strategy_backtest(strategy, config)``
       Uses ``StrategyBase`` subclass (e.g. BBIBOLLStrategy).
    2. **Pipeline mode** (new): ``run_pipeline_backtest(config)``
       Uses ``portfolio.pipeline`` → ``WeightBacktester``.

Both modes load data, run the backtest, export results, and print metrics.

Usage — Strategy mode (existing)::

    cd src && python -m backtest.backtester --mode strategy

Usage — Pipeline mode (new, default)::

    cd src && python -m backtest.backtester
    cd src && python -m backtest.backtester --mode pipeline --sizing Signal-Weighted

Reference: docs/quant_lab.tex
"""

from __future__ import annotations

import os
from typing import Any, Dict

import polars as pl

from backtest.engine import BacktestEngine
from backtest.visualizer import BacktestVisualizer
from backtest.weight_backtester import WeightBacktester
from data.loader.benchmark_loader import load_spx_benchmark
from data.loader.data_loader import stock_load_process
from data.loader.date_utils import generate_backtest_date
from data.loader.ticker_utils import get_common_stocks

# ══════════════════════════════════════════════════════════════════════
# Mode 1: Strategy-based backtest (legacy — StrategyBase subclass)
# ══════════════════════════════════════════════════════════════════════


def run_strategy_backtest(strategy, strategy_config: Dict[str, Any]) -> None:
    """Run a full backtest using a StrategyBase subclass.

    Steps: load data → run strategy → plot → export → print metrics.
    """
    print("Load data...")
    tickers = get_common_stocks(
        filter_date=strategy_config["data_start_date"]
    ).collect()

    try:
        ohlcv_data = (
            stock_load_process(
                tickers=tickers.to_series().to_list(),
                timeframe=strategy_config["timeframe"],
                start_date=strategy_config["data_start_date"],
                end_date=strategy_config["end_date"],
            )
            .filter(pl.col("volume") != 0)
            .collect()
        )
        print(f"tickers number: {ohlcv_data.select('ticker').n_unique()}")
    except Exception as e:
        print(f"data load failed: {e}")
        return

    print("load benchmark data...")
    benchmark_data = load_spx_benchmark(
        strategy_config["trade_start_date"], strategy_config["end_date"]
    )

    engine = BacktestEngine(initial_capital=strategy_config["initial_capital"])
    engine.add_strategy(strategy, ohlcv_data, tickers)

    print("start backtest...")
    results = engine.run_backtest(
        strategy=strategy,
        benchmark_data=benchmark_data,
        save_results=True,
        use_cached_indicators=False,
    )

    strategy_name = strategy.name
    output_dir = os.path.join(
        "backtest_output",
        strategy_name,
        strategy_config.get("result_customized_name", ""),
    )
    os.makedirs(output_dir, exist_ok=True)

    # Plot
    if not strategy_config.get("silent", False):
        _plot_strategy_results(
            engine, strategy_name, strategy_config, ohlcv_data, results, output_dir
        )

    # Export
    print("export backtest result...")
    try:
        engine.export_results(strategy_config, strategy_name, output_dir=output_dir)
    except Exception as e:
        print(f"backtest result export failed: {e}")

    _print_key_metrics(results["performance_metrics"], output_dir)


# ══════════════════════════════════════════════════════════════════════
# Mode 2: Pipeline-based backtest (new — weight DataFrames)
# ══════════════════════════════════════════════════════════════════════


def run_pipeline_backtest(config: Dict[str, Any]) -> None:
    """Run a backtest using the portfolio pipeline → WeightBacktester.

    Config keys:
        tickers: list[str] — stock universe (or omit to use all common stocks)
        start_date: str — OHLCV start date
        end_date: str — OHLCV end date
        alpha_config: AlphaConfig — full pipeline config (preferred)
        factor_names: list[str] — alpha factors (default: ["bbiboll", "vol_ratio"])
        sizing_method: str — sizing method (default: "Signal-Weighted")
        rebal_every_n: int — rebalance frequency (default: 5)
        combination_method: str — factor combination (default: "equal_weight")
        n_long: int — number of long positions (default: 10)
        n_short: int — number of short positions (default: 10)
        target_vol: float — target annual vol (default: 0.10)
        cost_bps: float — transaction cost in bps (default: 5.0)
        output_dir: str — results directory (default: "backtest_output/pipeline")

    If ``alpha_config`` is provided, it takes precedence over individual keys.
    """
    from portfolio.alpha_config import AlphaConfig
    from portfolio.pipeline import run_alpha_pipeline

    # ── Build or use AlphaConfig ──
    alpha_config = config.get("alpha_config")
    if alpha_config is None:
        alpha_config = AlphaConfig(
            factor_names=config.get("factor_names", ["bbiboll", "vol_ratio"]),
            sizing_method=config.get("sizing_method", "Signal-Weighted"),
            rebal_every_n=config.get("rebal_every_n", 5),
            combination_method=config.get("combination_method", "equal_weight"),
            n_long=config.get("n_long", 10),
            n_short=config.get("n_short", 10),
            target_vol=config.get("target_vol", 0.10),
            cost_bps=config.get("cost_bps", 5.0),
            annualization=config.get("annualization", 252),
            name=config.get("name", "AlphaPipeline"),
        )

    # ── Load data ──
    tickers = config.get("tickers")
    if tickers is None:
        print("Load common stocks...")
        tickers_df = get_common_stocks(filter_date=config["start_date"]).collect()
        tickers = tickers_df.to_series().to_list()

    print(f"Loading OHLCV for {len(tickers)} tickers...")
    ohlcv = (
        stock_load_process(
            tickers=tickers,
            start_date=config["start_date"],
            end_date=config.get("end_date"),
        )
        .filter(pl.col("volume") != 0)
        .collect()
    )
    print(
        f"OHLCV: {ohlcv.shape[0]:,} rows, {ohlcv.select('ticker').n_unique()} tickers"
    )

    # ── Run alpha pipeline ──
    print("Running alpha pipeline...")
    pipeline_result = run_alpha_pipeline(ohlcv, config=alpha_config)
    print(f"Pipeline Sharpe (simple): {pipeline_result['sharpe']:.3f}")

    # ── Load benchmark ──
    benchmark = load_spx_benchmark(config["start_date"], config.get("end_date"))

    # ── Run weight backtester ──
    bt = WeightBacktester(cost_bps=alpha_config.cost_bps)
    result = bt.run_from_pipeline(
        pipeline_result,
        benchmark_data=benchmark,
        name=alpha_config.name,
    )

    # ── Print & export ──
    bt.print_summary(result)

    output_dir = config.get("output_dir", "backtest_output/pipeline")
    bt.export(result, output_dir=output_dir)
    _print_key_metrics(result.metrics, output_dir)


# ══════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════


def _print_key_metrics(metrics: Dict[str, Any], output_dir: str) -> None:
    """Print key metrics summary."""
    print("\nKey Metrics:")
    print("-" * 40)
    key_items = [
        ("Total Return [%]", f"{metrics.get('Total Return [%]', 0):.2f}%"),
        ("Benchmark Return [%]", f"{metrics.get('Benchmark Return [%]', 0):.2f}%"),
        ("Max Drawdown [%]", f"{metrics.get('Max Drawdown [%]', 0):.2f}%"),
        ("Sharpe Ratio", f"{metrics.get('Sharpe Ratio', 0):.4f}"),
        ("Win Rate [%]", f"{metrics.get('Win Rate [%]', 0):.2f}%"),
        ("Total Trades", f"{metrics.get('Total Trades', 0)}"),
    ]
    for name, value in key_items:
        print(f"  {name:<25}: {value}")
    print(f"\nResults exported to {output_dir}/")


def _plot_strategy_results(
    engine, strategy_name, strategy_config, ohlcv_data, results, output_dir
):
    """Plot equity curves and candlestick charts for strategy backtests."""
    selected_ticker = strategy_config.get("selected_tickers", ["random"])[0]
    try:
        many_tickers = (
            len(strategy_config.get("selected_tickers", [])) > 2000
            or selected_ticker == "random"
        )

        if many_tickers:
            engine.plot_results(
                strategy_name=strategy_name,
                plot_equity=True,
                plot_performance=True,
                plot_monthly=True,
                save_plots=True,
                output_dir=output_dir,
            )

        visualizer = BacktestVisualizer()

        if many_tickers and not strategy_config.get("plot_all", False):
            selected_ticker = (
                results["trades"].select("ticker").unique().to_series().sample(1)[0]
            )
            print(f"plot {selected_ticker} k-line and trade signal...")
            visualizer.plot_candlestick_with_signals(
                ohlcv_data=ohlcv_data,
                ticker=selected_ticker,
                trades=results["trades"],
                open_positions=results["open_positions"],
                start_date=strategy_config["trade_start_date"],
                end_date=strategy_config["end_date"],
                indicators=results.get("indicators"),
                save_path=f"{output_dir}/{selected_ticker}_signals.png",
            )
        elif strategy_config.get("plot_all", False):
            for t in strategy_config.get("selected_tickers", []):
                print(f"plot {t} k-line and trade signal...")
                visualizer.plot_candlestick_with_signals(
                    ohlcv_data=ohlcv_data,
                    ticker=t,
                    trades=results["trades"],
                    open_positions=results["open_positions"],
                    start_date=strategy_config["trade_start_date"],
                    end_date=strategy_config["end_date"],
                    indicators=results.get("indicators"),
                    line=False,
                    save_path=f"{output_dir}/{t}_signals.png",
                )
    except Exception as e:
        print(f"error during plotting: {e}")


# ══════════════════════════════════════════════════════════════════════
# CLI entry point
# ══════════════════════════════════════════════════════════════════════


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="quant101 backtester")
    parser.add_argument(
        "--mode",
        choices=["strategy", "pipeline"],
        default="pipeline",
        help="Backtest mode: 'strategy' (BBIBOLL) or 'pipeline' (alpha pipeline)",
    )
    parser.add_argument("--start", default="2023-12-01", help="OHLCV start date")
    parser.add_argument("--end", default="2026-02-27", help="End date")
    parser.add_argument("--sizing", default="Signal-Weighted", help="Sizing method")
    parser.add_argument("--cost-bps", type=float, default=5.0, help="Cost in bps")
    parser.add_argument(
        "--factors",
        nargs="+",
        default=["bbiboll", "vol_ratio"],
        help="Factor names",
    )
    args = parser.parse_args()

    if args.mode == "pipeline":
        print("Pipeline-Based Backtest")
        print("=" * 60)
        run_pipeline_backtest(
            {
                "start_date": args.start,
                "end_date": args.end,
                "factor_names": args.factors,
                "sizing_method": args.sizing,
                "cost_bps": args.cost_bps,
                "output_dir": "backtest_output/pipeline",
            }
        )

    else:
        from strategy.bbiboll_strategy import BBIBOLLStrategy

        print("BBIBOLL Strategy Backtest")
        print("=" * 60)

        strategy_config = {
            "result_customized_name": "latest",
            "boll_length": 11,
            "boll_multiple": 6,
            "max_dev_pct": 1,
            "loss_threshold": -0.15,
            "profit_threshold": 0.1,
            "selected_tickers": ["random"],
            "random_count": None,
            "plot_all": False,
            "timeframe": "1d",
            "data_start_date": args.start,
            "trade_start_date": args.start,
            "end_date": args.end,
            "initial_capital": 10000.0,
            "add_risk_free_rate": True,
            "silent": False,
        }

        strategy = BBIBOLLStrategy(config=strategy_config)
        run_strategy_backtest(strategy, strategy_config)

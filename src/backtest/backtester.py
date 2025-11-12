import os

import polars as pl

from backtest.engine import BacktestEngine
from backtest.visualizer import BacktestVisualizer
from cores.config import all_tickers_dir
from cores.data_loader import stock_load_process
from utils.backtest_utils.backtest_utils import (
    generate_backtest_date,
    load_spx_benchmark,
    only_common_stocks,
)


def run_backtest(strategy, strategy_config=None):
    """MAIN BACKTEST PROCESSION"""

    print("Load data...")
    tickers = only_common_stocks(filter_date=strategy_config["data_start_date"])

    try:
        ohlcv_data = (
            stock_load_process(
                tickers=tickers.to_series().to_list(),
                timeframe=strategy_config["timeframe"],
                start_date=strategy_config["data_start_date"],
                end_date=strategy_config["end_date"],
                # use_cache=False,
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

    print("generating backtest plot...")
    selected_ticker = strategy_config["selected_tickers"][0]
    try:
        if (
            len(strategy_config.get("selected_tickers", [])) > 2000
            or selected_ticker == "random"
            and not strategy_config["silent"]
        ):
            engine.plot_results(
                strategy_name=strategy_name,
                plot_equity=True,
                plot_performance=True,
                plot_monthly=True,
                save_plots=True,
                output_dir=output_dir,
            )

        visualizer = BacktestVisualizer()

        if (
            (
                len(strategy_config.get("selected_tickers", [])) > 2000
                or selected_ticker == "random"
            )
            and (strategy_config["plot_all"] == False)
            and not strategy_config["silent"]
        ):
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
                # line=False,
                save_path=f"{output_dir}/{selected_ticker}_signals.png",
            )

        elif strategy_config["plot_all"] and not strategy_config["silent"]:
            for selected_ticker in strategy_config.get("selected_tickers", []):
                print(f"plot {selected_ticker} k-line and trade signal...")
                visualizer.plot_candlestick_with_signals(
                    ohlcv_data=ohlcv_data,
                    ticker=selected_ticker,
                    trades=results["trades"],
                    open_positions=results["open_positions"],
                    start_date=strategy_config["trade_start_date"],
                    end_date=strategy_config["end_date"],
                    indicators=results.get("indicators"),
                    line=False,
                    save_path=f"{output_dir}/{selected_ticker}_signals.png",
                )

    except Exception as e:
        print(f"error occurs during plotting: {e}")

    print("export backtest result...")
    try:
        engine.export_results(strategy_config, strategy_name, output_dir=output_dir)
    except Exception as e:
        print(f"backtest result export failed: {e}")

    print("\n关键结果摘要:")
    print("-" * 40)
    performance = results["performance_metrics"]

    key_metrics = [
        ("Total Return [%]", f"{performance.get('Total Return [%]', 0):.2f}%"),
        ("Benchmark Return [%]", f"{performance.get('Benchmark Return [%]', 0):.2f}%"),
        ("Max Drawdown [%]", f"{performance.get('Max Drawdown [%]', 0):.2f}%"),
        ("Sharpe Ratio", f"{performance.get('Sharpe Ratio', 0):.4f}"),
        ("Win Rate [%]", f"{performance.get('Win Rate [%]', 0):.2f}%"),
        ("Total Trades", f"{performance.get('Total Trades', 0)}"),
    ]

    for metric, value in key_metrics:
        print(f"{metric:<12}: {value}")

    print(f"\nBacktest done! result isexported to {output_dir}")


if __name__ == "__main__":
    from strategies.bbibollStrategy import BBIBOLLStrategy

    print("BBIBOLL Strategy Backtest")
    print("=" * 60)

    strategy_config = {
        "result_customized_name": "20251107",  # distinguish different config runs
        "boll_length": 11,
        "boll_multiple": 6,
        "max_dev_pct": 1,
        "loss_threshold": -0.15,
        "profit_threshold": 0.1,
        # "selected_tickers": ["QCLS"],
        "selected_tickers": ["random"],  # change it to 'random' to select random stocks
        "random_count": None,
        # "min_turnover": 0,
        # "plot_all": True,
        "plot_all": False,
        "timeframe": "1d",
        "data_start_date": "2023-12-01",
        "trade_start_date": "2024-12-01",
        "end_date": "2025-11-07",
        "initial_capital": 10000.0,
        "add_risk_free_rate": True,
        "silent": False,
    }

    backtest_dates = generate_backtest_date(
        start_date="2025-11-07",
        period="week",
        reverse=True,
        reverse_limit="2025-10-24",
        # reverse_limit_count=2
    )

    # for backtest_date in backtest_dates:
    #     strategy_config["silent"] = True
    #     print(f"info: backtesing: {backtest_date}")

    #     strategy_config["result_customized_name"] = backtest_date
    #     strategy_config["end_date"] = backtest_date

    #     strategy = BBIBOLLStrategy(config=strategy_config)
    #     run_backtest(strategy, strategy_config=strategy_config)
    strategy = BBIBOLLStrategy(config=strategy_config)
    run_backtest(strategy, strategy_config=strategy_config)

"""
BBIBOLL策略回测示例 - 使用新的回测框架
"""

import os

import polars as pl

from backtesting.backtest_pre_data import load_spx_benchmark, only_common_stocks
from backtesting.engine import BacktestEngine
from backtesting.visualizer import BacktestVisualizer
from core_2.config import all_tickers_dir
from core_2.data_loader import stock_load_process


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
        # use_cached_indicators=False,
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
            len(strategy_config.get("selected_tickers", [])) > 2000
            or selected_ticker == "random"
        ) and (strategy_config["plot_all"] == False):
            selected_ticker = (
                results["trades"].select("ticker").unique().to_series().sample(1)[0]
            )
            print(f"plot {selected_ticker} k-line and trade signal...")
            visualizer.plot_candlestick_with_signals(
                ohlcv_data=ohlcv_data,
                trades=results["trades"],
                ticker=selected_ticker,
                start_date=strategy_config["trade_start_date"],
                end_date=strategy_config["end_date"],
                indicators=results.get("indicators"),
                # line=False,
                save_path=f"{output_dir}/{selected_ticker}_signals.png",
            )

        elif strategy_config["plot_all"]:
            for selected_ticker in strategy_config.get("selected_tickers", []):
                print(f"plot {selected_ticker} k-line and trade signal...")
                visualizer.plot_candlestick_with_signals(
                    ohlcv_data=ohlcv_data,
                    trades=results["trades"],
                    ticker=selected_ticker,
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
        "result_customized_name": "22_to_25",  # distinguish different config runs
        "boll_length": 11,
        "boll_multiple": 6,
        "max_dev_pct": 1,
        "loss_threshold": -0.15,
        "profit_threshold": 0.1,
        # "selected_tickers": ["SFE"],
        "selected_tickers": [
            "BON",
            "JBDI",
            "PRSO",
            "ETNB",
            "MTSR",
            "WBD",
            "WAI",
            "WAI",
            "AQMS",
            "ORIS",
            "MWYN",
            "MWYN",
            "MWYN",
            "ATCH",
            "ATCH",
            "HXHX",
            "HXHX",
            "BIYA",
            "BIYA",
            "SBEV",
            "SBEV",
            "PLUG",
            "CNTX",
            "QNRX",
            "QNRX",
            "WAI",
            "WAI",
            "OFAL",
            "OFAL",
            "WAFU",
            "BRFH",
            "LSH",
            "BRZE",
            "KIDZ",
            "NCNA",
            "NCNA",
            "SPPL",
            "SONM",
            "BTDR",
            "MCTR",
            "MCTR",
            "MCTR",
            "MCTR",
            "MCTR",
            "MCTR",
            "TOP",
            "CRBP",
            "CRBP",
            "NAMM",
            "SKBL",
            "SKBL",
            "RAYA",
            "RAYA",
            "RAYA",
            "RAYA",
            "GSIW",
            "TAOP",
            "MSW",
            "DAKT",
            "SDOT",
            "SDOT",
            "IMPP",
            "VRA",
            "BIDU",
            "RAYA",
            "RAYA",
            "RAYA",
            "RAYA",
            "PLRZ",
            "PLRZ",
            "GIBO",
            "GIBO",
            "GIBO",
            "GIBO",
            "GIBO",
            "ANIX",
            "WNW",
            "PSNL",
            "PETZ",
            "RDGT",
            "RDGT",
            "CMND",
            "CMND",
            "MGX",
            "MGX",
            "IONR",
            "IONR",
            "ZBAI",
            "MPW",
            "MCTR",
            "MCTR",
            "MCTR",
            "MCTR",
            "MCTR",
            "MCTR",
            "EXOZ",
            "BIAF",
            "BMNR",
            "YSXT",
            "YSXT",
            "DBRG",
            "INEO",
            "BRC",
            "REVB",
            "REVB",
            "DXST",
            "PRPH",
            "KALV",
            "SHO",
            "IBRX",
            "IBRX",
            "WIMI",
            "MKZR",
            "MKZR",
            "MEGL",
            "JKS",
            "HOLO",
            "HOLO",
            "IONS",
            "MLGO",
            "MLGO",
            "AXR",
            "TMDE",
            "TMDE",
            "BCDA",
            "TWNP",
            "WLAC",
            "LXRX",
            "LXRX",
            "WFF",
            "QH",
            "SOTK",
            "LFWD",
            "WTTR",
            "ATOS",
            "STRR",
            "CENX",
            "SORA",
            "FORR",
            "VNO",
            "SMX",
            "SKYE",
            "EONR",
            "EONR",
            "MVIS",
            "CETY",
            "CETY",
            "ACIW",
            "SMSI",
            "MEOH",
            "CENN",
            "CRNT",
            "ARQT",
            "MDIA",
            "CMTL",
            "OCGN",
            "OMH",
            "OMH",
            "BB",
            "XNCR",
            "ARL",
            "YQ",
            "PTNM",
            "PTNM",
            "DGLY",
            "DGLY",
            "OGEN",
            "OGEN",
            "OGEN",
            "OGEN",
            "APPS",
            "APPS",
            "NISN",
            "JVA",
            "GIFI",
            "RIVN",
            "ALTO",
            "CTNT",
            "CTNT",
            "CCTG",
            "FATN",
            "FATN",
            "GBCI",
            "ITT",
            "KTCC",
            "YXT",
            "CEVA",
            "HTOO",
            "ACRV",
            "NTZ",
            "OGEN",
            "OGEN",
            "OGEN",
            "OGEN",
            "FKWL",
            "SEPN",
            "RXST",
            "JZXN",
            "SIMO",
            "CIGL",
            "CLOV",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "CRI",
            "BAOS",
            "JD",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "PKST",
            "JDZG",
            "GME",
            "SNSE",
            "LUCY",
            "LUCY",
            "CLB",
            "JZ",
            "BILL",
            "PLAG",
            "EVR",
            "PHOE",
            "PLX",
            "NXTC",
            "NMG",
            "FLGC",
            "AL",
            "ACLX",
            "SLNH",
            "SLNH",
            "LAB",
            "DFLI",
            "DFLI",
            "GLBS",
            "ZGN",
            "DIN",
            "LAC",
            "CAAS",
            "MARA",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "ADVB",
            "SKYX",
            "FFIC",
            "MTVA",
            "FBRX",
            "PATH",
            "PANL",
            "CERT",
            "RVMD",
            "ROIV",
            "NPCE",
            "BPOP",
            "BFLY",
            "BDN",
            "LI",
            "EDU",
            "LOBO",
            "CDP",
            "LVLU",
            "XHLD",
            "XHLD",
            "CVGI",
            "ASX",
            "ALMU",
            "ALMU",
            "ALMU",
            "UCTT",
            "OGN",
            "OGN",
            "COHU",
            "CMCO",
            "FPH",
            "PLUT",
            "NAAS",
            "NAAS",
            "LRMR",
            "ICHR",
            "CLNE",
            "HR",
            "NEXA",
            "NTES",
            "SHPH",
            "SHPH",
            "SHPH",
            "SNAP",
            "UPC",
            "NCLH",
            "SLP",
            "TGTX",
            "HNRG",
            "KVHI",
            "TBN",
            "VTR",
            "BGM",
            "WCT",
            "SKT",
            "CURV",
            "USEA",
            "DVS",
            "RILYT",
            "ADAM",
            "CYH",
            "CYH",
            "KWM",
            "KWM",
            "GOCO",
            "CRT",
            "FOXX",
            "UTL",
            "STZ",
            "PETS",
            "NOMD",
            "FIZZ",
            "OEC",
            "OPI",
            "SNBR",
            "SVRE",
            "SVRE",
            "SVRE",
            "BRKR",
            "JYD",
            "JYD",
            "SNTI",
            "YB",
            "TOPP",
            "CGEM",
            "CGEM",
            "EDHL",
            "EDHL",
            "SYNX",
            "INAB",
            "INAB",
            "ANNX",
            "ANNX",
            "AZI",
            "AZI",
            "AZI",
            "FLO",
            "CDRO",
            "CSTE",
            "QTTB",
            "SNOA",
            "OFAL",
            "OFAL",
            "NTCL",
            "NTCL",
            "NTCL",
            "CREG",
            "COCH",
            "ABTS",
            "LUCY",
            "LUCY",
            "NGNE",
            "JSPR",
            "TNMG",
            "TNMG",
            "JFBR",
            "JFBR",
            "JFBR",
            "HCAI",
            "HCAI",
            "HCAI",
            "RENB",
            "STEC",
            "MNDR",
            "GLXY",
            "ILLR",
            "ILLR",
            "STKH",
            "STKH",
            "TRSG",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "ZSPC",
            "FIEE",
            "BLMZ",
            "BLMZ",
            "LESL",
            "BGMS",
            "CRESY",
            "LULU",
            "LULU",
            "PACS",
            "GLTO",
            "GLTO",
            "OMSE",
            "FTRK",
            "FTRK",
            "FTRK",
            "AMOD",
            "IRDM",
            "HMR",
            "HMR",
            "CIIT",
            "PMAX",
            "PMAX",
            "GVH",
            "MIGI",
            "NFE",
            "SDST",
            "SNPS",
            "PPBT",
            "PPBT",
            "VCIG",
            "VCIG",
        ],
        # "selected_tickers": ["random"],  # change it to 'random' to select random stocks
        "random_count": None,
        # "min_turnover": 0,
        # "plot_all": True,
        "plot_all": False,
        "timeframe": "1d",
        "data_start_date": "2022-01-01",
        "trade_start_date": "2023-01-01",
        "end_date": "2025-09-26",
        "initial_capital": 10000.0,
        "add_risk_free_rate": True,
    }

    strategy = BBIBOLLStrategy(config=strategy_config)

    run_backtest(strategy, strategy_config=strategy_config)

    # run_multiple_strategies_example()

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
            len(strategy_config.get("selected_tickers", [])) > 1
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
            len(strategy_config.get("selected_tickers", [])) > 1
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
        # "selected_tickers": ['EDHL', 'GIBO', 'VCIG', 'PUBM', 'PPBT', 'INSP', 'CANF', 'KLC', 'LIMN', 'SNPS', 'CYCU', 'SDST', 'MURA', 'EXAS', 'MCTR', 'NFE', 'MIGI', 'TNFA', 'NCEW', 'ASTL', 'YAAS', 'FORM', 'CAVA', 'MWYN', 'BIYA', 'AQMS', 'GVH', 'PMAX', 'KWM', 'CSAI', 'SBEV', 'NAOV', 'LNW', 'WGRX', 'CIIT', 'HMR', 'IRDM', 'AMOD', 'STRZ', 'FTRK', 'FTRK', 'NAAS', 'OMSE', 'GLTO', 'TRNR', 'GDDY', 'NCNA', 'WGRX', 'PACS', 'ZSPC', 'WORX', 'EP', 'LULU', 'ATCH', 'ICG', 'CRESY', 'SKK', 'INEO', 'YIBO', 'CNF', 'AVNS', 'YB', 'ADVB', 'BGMS', 'GIFT', 'WTO', 'BJDX', 'LESL', 'TOMZ', 'HRL', 'CRWV', 'PFAI', 'LHSW', 'PWM', 'WOLF', 'BLMZ', 'EDBL', 'FIEE', 'RDNW', 'MWYN', 'ZSPC', 'QNRX', 'LEE', 'MDCX', 'PRGO', 'STKH', 'EHTH', 'CSTL', 'JAGX', 'SJ', 'XFOR', 'FATN', 'EPWK', 'VERA', 'HXHX', 'PAVM', 'ILLR', 'GLXY', 'MNDR', 'MBRX', 'STEC', 'EONR', 'IMG', 'GMM', 'RENB', 'EDUC', 'LOBO', 'LVRO', 'BTAI', 'ARVN', 'WEST', 'DRIO', 'GIBO', 'SCKT', 'ATHR', 'HCAI', 'NPWR', 'HMR', 'INTS', 'JFBR', 'TNMG', 'MKDW', 'DV', 'WAT', 'OABI', 'AVIR', 'HCAI', 'CNEY', 'TMDE', 'HKPD', 'CETY', 'KUKE', 'JSPR', 'SUNE', 'CYH', 'NGNE', 'IBTA', 'SLNH', 'DGLY', 'GDYN', 'SEM', 'LUCY', 'SMTK', 'NNOX', 'SRL', 'NIPG', 'COCH', 'AMWL', 'ABTS', 'WKSP', 'PHH', 'BAND', 'ELAB', 'AVBP', 'PMAX', 'ICUI', 'CREG', 'YCBD', 'MBIN', 'XPOF', 'OFAL', 'NTCL', 'AREB', 'CLIK', 'CNCK', 'CMCT', 'ZDAI', 'ADD', 'FBLG', 'INV', 'ONON', 'ANNA', 'SNOA', 'EU', 'QTTB', 'AKTX', 'SMXT', 'DUO', 'AIRI', 'OGN', 'CSTE', 'CWD', 'CNK', 'IOT', 'LLY', 'FLO', 'CDRO', 'CSAI', 'EWCZ', 'GMHS', 'AZI', 'JCSE', 'NVVE', 'NMAX', 'GORV', 'YI', 'ANNX', 'YSXT', 'GDHG', 'ESTC', 'JZ', 'EMN', 'MKTX', 'GLOB', 'HAO', 'STAK', 'AZI', 'FNKO', 'WOW', 'SHPH', 'APDN', 'OMH', 'INAB', 'SYNX', 'ACDC', 'EDHL', 'PTLE', 'ILLR', 'OLB', 'GDHG', 'CGEM', 'OKUR', 'HFFG', 'TRSG', 'DRMA', 'TOPP', 'FIZZ', 'DMRC', 'NAMI', 'RNXT', 'BMEA', 'JYD', 'SNTI', 'SLN', 'SVRE', 'BRKR', 'TW', 'CRIS', 'CZR', 'APVO', 'OST', 'CGTL', 'GAP', 'SXTC', 'LSB', 'UNM', 'LULU', 'EVO', 'WYHG', 'OPI', 'OEC', 'WTF', 'NOMD', 'FAT', 'VERO', 'SPWR', 'GFS', 'JFBR', 'TC', 'MCS', 'TVGN', 'TPIC', 'MLGO', 'UTL', 'STZ', 'FOXX', 'WRAP', 'KYMR', 'CRT', 'HCWB', 'GOCO', 'BRIA', 'CYH', 'KWM', 'ADAM', 'PPL', 'MLCO', 'NE', 'WRD', 'RILYT', 'ALMS', 'USEA', 'DVS', 'TIVC', 'DAVA', 'KNDI', 'GLTO', 'HCWB', 'NX', 'CURV', 'LAZR', 'IBO', 'CGEM', 'S', 'SKT', 'SFD', 'FG', 'FOLD', 'FRGT', 'WCT', 'TNK', 'VTR', 'CDP', 'BGM', 'WTO', 'THC', 'CVE', 'SSII', 'RVPH', 'GM', 'LFUS', 'TBN', 'GELS', 'KVHI', 'KRMN', 'OGN', 'TGL', 'TRX', 'BODI', 'VEEE', 'OBIO', 'HNRG', 'NCT', 'EPIX', 'FLOC', 'STRR', 'UWMC', 'NCLH', 'SLP', 'TGTX', 'JTAI', 'UPC', 'TLYS', 'FND', 'NTES', 'SHPH', 'SNAP', 'CGABL', 'IRWD', 'PPBI', 'NEXA', 'XHLD', 'AHL', 'HR', 'PRAX', 'CLNE', 'NTHI', 'WFRD', 'ICHR', 'CNA', 'ADMA', 'TCBK', 'WRD', 'LSF', 'PACK', 'LRMR', 'RPTX', 'FBLG', 'FTRK', 'FPH', 'PLUT', 'NAAS', 'VOC', 'COHU', 'CMCO', 'SPHL', 'DCGO', 'UCTT', 'NREF', 'GAIA', 'SBR', 'ALMU', 'ASX', 'HCTI', 'AII', 'CDZI', 'CVGI', 'LVLU', 'XHLD', 'EDU', 'LI', 'BDN', 'BFLY', 'GYRE', 'SONO', 'BVN', 'EPAC', 'CMDB', 'EFSI', 'TROX', 'NPB', 'ARWR', 'BPOP', 'DSP', 'HIND', 'ASRT', 'ROIV', 'EPSM', 'NPCE', 'LGPS', 'VALN', 'BHVN', 'RVMD', 'AGMH', 'TFX', 'HCA', 'LXRX', 'MRCC', 'CWK', 'UPB', 'XBIT', 'BAER', 'KNOP', 'CERT', 'SHPH', 'CXAI', 'PANL', 'GLBE', 'BRC', 'PATH', 'CCLD', 'FBRX', 'FFIC', 'MTVA', 'MYSZ', 'TTC', 'RBA', 'CETX', 'SKYX', 'ADVB', 'SEMR', 'JEF', 'GLDG', 'CMT', 'ZSPC', 'CRH', 'LEGH', 'MARA', 'JFBR', 'WLDS', 'ACIW', 'GFI', 'JBLU', 'DAC', 'CRC', 'CAAS', 'DIN', 'LAC', 'GLBS', 'ZGN', 'JANX', 'WELL', 'GLNG', 'CCG', 'APLM', 'IART', 'LAB', 'MAC', 'AL', 'AFRM', 'ACLX', 'SLNH', 'FLGC', 'NMG', 'ADAG', 'LVTX', 'ACM', 'NXTC', 'SRZN', 'PLX', 'EVR', 'RIGL', 'BILL', 'PLAG', 'CLB', 'SNSE', 'PKST', 'JDZG', 'GME', 'IMNM', 'BDSX', 'AKA', 'CABO', 'IBRX', 'JD', 'WKSP', 'GIFT', 'BAOS', 'KTTA', 'VTRS', 'HG', 'BBGI', 'RENT', 'PRTS', 'GRVY', 'CRI', 'REVB', 'HAFN', 'ADVB', 'STAA', 'HCM', 'CIGL', 'CLOV', 'SIMO', 'JILL', 'INAB', 'OFIX', 'OST', 'ERO', 'JZXN', 'BHAT', 'KRNT', 'RXST', 'IFRX', 'LHSW', 'WLAC', 'ATHR', 'LINC', 'SENS', 'SEPN', 'BOLD', 'PINC', 'NEM', 'PHR', 'RANI', 'UMBF', 'VIVK', 'FKWL', 'WK', 'OGEN', 'PLSE', 'NTZ', 'ALEC', 'DGNX', 'IAUX', 'ACRV', 'UFG', 'EHGO', 'IRD', 'LTRN', 'LWLG', 'ECO', 'ABCB', 'ONL', 'HTOO', 'NFGC', 'APPS', 'CEVA', 'PYXS', 'MBI', 'COLB', 'YXT', 'ITT', 'BLIV', 'YCBD', 'ASTH', 'BBIO', 'ELBM', 'SNWV', 'HTCO', 'FRPT', 'GBCI', 'BIDU', 'BTMD', 'POWL', 'GIBO', 'FATN', 'CCTG', 'SAGT', 'ARWR', 'CTNT', 'SOWG', 'ALTO', 'CLWT', 'DAO', 'FDMT', 'AUST', 'REPL', 'IOVA', 'RIVN', 'GIFI', 'ORKA', 'JVA', 'ALXO', 'APPS', 'STGW', 'MOBX', 'REGN', 'OGEN', 'HIND', 'RVYL', 'DGLY', 'ALMU', 'PYPD', 'LFWD', 'PTNM', 'NAT', 'YQ', 'RVSN', 'ARL', 'XNCR', 'SGLY', 'CNCK', 'ABEO', 'SDOT', 'ZNTL', 'OMH', 'BB', 'SILO', 'HAO', 'IDYA', 'LUNG', 'RDGT', 'IONR', 'OCGN', 'CMTL', 'EMBC', 'MDIA', 'HMY', 'ARQT', 'RDI', 'CRNT', 'BMEA', 'CENN', 'HWBK', 'AGMH', 'AVTX', 'EKSO', 'MEOH', 'SLXN', 'SMSI', 'MOVE', 'SYRE', 'NSSC', 'GDOT', 'ATLX', 'DVLT', 'LICN', 'CETY', 'QVCGA', 'SSII', 'BENF', 'HKIT', 'ERNA', 'ADVB', 'CTGO', 'SAFX', 'ASBP', 'NTCL', 'EONR', 'MVIS', 'NGL', 'SKYE', 'UDMY', 'HYFM', 'MPWR', 'KTCC', 'SMX', 'TRIB', 'NGVT', 'VNO', 'PAM', 'SLE', 'REI', 'ANGI', 'KLRS', 'IXHL', 'SITM', 'REE', 'FORR', 'AMKR', 'SORA', 'FATE', 'HSII', 'WINA', 'UOKA', 'CENX', 'PTON', 'ATOS', 'FBIO', 'SEED', 'PETS', 'HIPO', 'USLM', 'WTTR', 'TNMG', 'SOTK', 'OLMA', 'ALMU', 'CCCC', 'WLFC', 'JAGX', 'QH', 'ELUT', 'SUUN', 'SST', 'MTC', 'WFF', 'TTNP', 'LGPS', 'LXRX', 'PIII', 'LSB', 'CMPS', 'IRWD', 'TWNP', 'BCDA', 'TMDE', 'AXR', 'STRO', 'MI', 'IOTR', 'PLRX', 'MKZR', 'MLGO', 'BW', 'TOVX', 'CTXR', 'PRTA', 'INTS', 'IONS', 'JKS', 'HOLO', 'PLRZ', 'HDSN', 'MEGL', 'POAI', 'RDHL', 'ALZN', 'ANNX', 'NNVC', 'PSNY', 'EOSE', 'FTRE', 'MKZR', 'GRDN', 'CRBP', 'SCHL', 'HAIN', 'XRX', 'NERV', 'SVRE', 'TLPH', 'HCAI', 'HTCO', 'WIMI', 'TARS', 'SNBR', 'NDLS', 'GLUE', 'ALUR', 'IBRX', 'TPST', 'SGMO', 'IQ', 'SHO', 'MQ', 'KALV', 'CETX', 'REBN', 'DXLG', 'BENF', 'BCAB', 'WGRX', 'PRPH', 'DXST', 'MAAS', 'REVB', 'VIR', 'IRBT', 'NVX', 'KURA', 'INFU', 'RANI', 'FGEN', 'TOYO', 'PPBT', 'PTNM', 'CTNT', 'YSXT', 'AMIX', 'KRRO', 'ARQ', 'CDTG', 'DYN', 'TCRX', 'BMNR', 'DBRG', 'HUMA', 'BIAF', 'AFRI', 'EXOZ', 'TER', 'GDHG', 'MCTR', 'MPW', 'ZBAI', 'ANY', 'UONEK', 'LDWY', 'PHOE', 'IONR', 'COOT', 'PRLD', 'NBIS', 'WB', 'TGL', 'CADL', 'ADVB', 'SNWV', 'APDN', 'XFOR', 'LUNG', 'ORBS', 'BYND', 'MGX', 'CMND', 'HBIO', 'RDGT', 'AEYE', 'PETZ', 'UGRO', 'XXII', 'AIRE', 'PSNL', 'WNW', 'BNGO', 'ANIX', 'AZI', 'AVR', 'SDA', 'BARK', 'NISN', 'HTCR', 'JYD', 'STTK', 'YTRA', 'APRE', 'PLRZ', 'GIBO', 'KRRO', 'RAYA', 'VRA', 'LRE', 'IMPP', 'WYHG', 'GELS', 'MTSR', 'SDOT', 'ZJYL', 'DAKT', 'ABSI', 'DFLI', 'ATXG', 'VIAV', 'NTCL', 'INHD', 'GWAV', 'MSW', 'ABP', 'ESTC', 'TAOP', 'UPLD', 'SVRE', 'GIBO', 'GSIW', 'AEO', 'ENGS', 'RAYA', 'SKBL', 'NAMM', 'CRBP', 'TOP', 'MCRP', 'GRO', 'BTDR', 'MCTR', 'NRIX', 'AMCX', 'SONM', 'BEAT', 'ZSPC', 'DOCN', 'FLXS', 'SPPL', 'MAPS', 'HOLO', 'MOGU', 'NCNA', 'MGIH', 'KIDZ', 'NWTG', 'PHIO', 'BRZE', 'LSH', 'BRFH', 'WAFU', 'WST', 'RCKT', 'VCIG', 'CVM', 'RDZN', 'HTCO', 'NAMI', 'WAI', 'QNRX', 'CNTX', 'MGX', 'PLUG', 'CDLX', 'SBEV', 'BIYA', 'CGC', 'SKBL', 'MEG', 'STAI', 'CMND', 'HXHX', 'ATCH', 'MWYN', 'WULF', 'ORIS', 'TLS', 'WOLF', 'PBM', 'AQB', 'VSME', 'XPON', 'VERI', 'WBD', 'STKH', 'MNTS', 'ETNB', 'VRME', 'NBY', 'DNUT', 'PRSO', 'BLMZ', 'JBDI', 'PRFX', 'BON', 'IBG', 'DFLI', 'INHD', 'LGPS'],
        "selected_tickers": ["random"],  # change it to 'random' to select random stocks
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

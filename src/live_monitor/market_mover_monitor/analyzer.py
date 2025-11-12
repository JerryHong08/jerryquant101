"""
Enhanced Market Mover Analyzer
Now supports both CLI mode and Web mode with real-time visualization
"""

import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl
import redis

from utils.backtest_utils.backtest_utils import only_common_stocks
from utils.longbridge_utils.update_watchlist import update_watchlist


def run_cli_mode():
    """Original CLI mode for simple console output"""
    r = redis.Redis(host="localhost", port=6379, db=0)
    pubsub = r.pubsub()
    pubsub.subscribe("market_snapshot")

    print("Analyzer listening in CLI mode...")

    for message in pubsub.listen():
        if message["type"] == "message":
            json_data = message["data"]
            df = pl.read_json(json_data)

            df = df.with_columns(
                pl.from_epoch(pl.col("timestamp"), time_unit="ms").dt.convert_time_zone(
                    "America/New_York"
                )
            )
            print("Received snapshot:", df.shape)

            updated_time = datetime.now(ZoneInfo("America/New_York")).strftime(
                "%Y%m%d%H%M%S"
            )  # 20250930170603
            filter_date = (
                updated_time[:4] + "-" + updated_time[4:6] + "-" + updated_time[6:8]
            )  # 2025-09-30

            # filter only common stock and rank by percent_change
            try:
                df = (
                    only_common_stocks(filter_date)
                    .drop("active", "composite_figi")
                    .join(df, on="ticker", how="inner")
                    .sort("percent_change", descending=True)
                )
            except Exception as e:
                print(f"Error filtering common stocks: {e}")
                # Fallback: just sort by percent_change
                df = df.sort("percent_change", descending=True)

            with pl.Config(tbl_rows=20, tbl_cols=50):
                print(df.head(20))

            # rank by percent_change, rank change, since exploded, float shares. to be added

            # if new tickers in top 20, update watchlist
            # 选top20更新watchlist
            top_20 = df.select("ticker").to_series().to_list()[:20]
            print(top_20)
            # update_watchlist(watchlist_name="market_mover", tickers=top_20)


def run_web_mode(**kwargs):
    """New web mode with real-time visualization"""
    from live_monitor.market_mover_monitor.web.web_analyzer import WebAnalyzer

    web_analyzer = WebAnalyzer(
        host=kwargs.get("host", "localhost"), port=kwargs.get("port", 5000)
    )

    # Load historical data if specified
    if kwargs.get("load_history"):
        print(f"Loading historical data for {kwargs['load_history']}")
        web_analyzer.data_manager.initialize_from_history(kwargs["load_history"])

    # Start the web server
    web_analyzer.run(debug=kwargs.get("debug", False))


def main():
    """Main entry point with mode selection"""
    parser = argparse.ArgumentParser(
        description="Market Mover Analyzer - CLI or Web mode",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # CLI mode (original functionality)
  python analyzer.py --mode cli
  
  # Web mode with default settings
  python analyzer.py --mode web
  
  # Web mode with custom host/port and historical data
  python analyzer.py --mode web --host 0.0.0.0 --port 8080 --load-history 20251003
  
  # Web mode with debug enabled
  python analyzer.py --mode web --debug
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["cli", "web"],
        default="web",
        help="Running mode: 'cli' for console output, 'web' for real-time visualization (default: web)",
    )

    # Web mode specific arguments
    parser.add_argument(
        "--host", default="localhost", help="Host to bind web server to (web mode only)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Port to bind web server to (web mode only)",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug mode (web mode only)"
    )
    parser.add_argument(
        "--load-history", help="Load historical data for date YYYYMMDD (web mode only)"
    )

    args = parser.parse_args()

    if args.mode == "cli":
        print("Starting Market Mover Analyzer in CLI mode...")
        run_cli_mode()
    else:
        print("Starting Market Mover Analyzer in Web mode...")
        run_web_mode(
            host=args.host,
            port=args.port,
            debug=args.debug,
            load_history=args.load_history,
        )


if __name__ == "__main__":
    main()

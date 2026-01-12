#!/usr/bin/env python3
"""
Market Mover Web Analyzer - Quick Start Script
Provides easy startup commands for different modes
"""

import argparse
import os
import subprocess
import sys


def start_collector(limit="market_open"):
    """Start the collector process"""
    # src/live_monitor/market_mover_monitor/scripts/start.py
    collector_path = os.path.join(
        os.path.dirname(__file__), "..", "core", "collector", "collector.py"
    )
    # src/live_monitor/market_mover_monitor/core/collector/collector.py
    print("🚀 Starting Market Mover Collector...")
    subprocess.run([sys.executable, "-u", collector_path, "--limit", limit])


def start_analyzer_web(**kwargs):
    print("🌐 Starting Market Mover Web Analyzer...")  # Move print BEFORE import
    from live_monitor.market_mover_monitor.core.api.web_server import WebAnalyzer

    try:
        web_analyzer = WebAnalyzer(
            host=kwargs.get("host", "localhost"),
            port=kwargs.get("port", 5000),
            replay_date=kwargs.get("replay_date"),
            load_history=kwargs.get("load_history", False),
        )

        # Load historical data if specified
        # if kwargs.get("load_history"):
        #     print(f"Loading historical data for {kwargs['load_history']}")
        #     web_analyzer.data_manager.initialize_from_history(kwargs["load_history"])

        # Start the web server
        web_analyzer.run(debug=kwargs.get("debug", False))

    except OSError as e:
        if e.errno == 98:  # Address already in use
            print(f"❌ Error: Port {kwargs.get('port', 5000)} is already in use")
            print(f"   Try a different port with: --port <port_number>")
        raise


def start_replayer(date, speed=1.0):
    """Start replayer for historical data"""
    replayer_path = os.path.join(
        os.path.dirname(__file__), "..", "core", "collector", "replayer.py"
    )
    print(f"⏪ Starting Market Mover Replayer for {date} at {speed}x speed...")
    subprocess.run(
        [sys.executable, "-u", replayer_path, "--date", date, "--speed", str(speed)]
    )


def start_trades_replayer(date, speed=1.0):
    """Start trade level replayer for historical data"""
    trades_replayer_path = os.path.join(
        os.path.dirname(__file__), "..", "core", "collector", "trades_replayer.py"
    )
    print(f"⏪ Starting Market Mover trades_replayer for {date} at {speed}x speed...")
    subprocess.run(
        [
            sys.executable,
            "-u",
            trades_replayer_path,
            "--date",
            date,
            "--speed",
            str(speed),
        ]
    )


def main():
    parser = argparse.ArgumentParser(
        description="Market Mover System - Quick Start",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Quick Start Examples:

1. Start real-time data collection:
   python start.py collector

2. View real-time data in web browser:
   python start.py web
   # Then open http://localhost:5000

3. Start with historical data:
   python start.py web --load-history 20251003

4. Start on custom host/port:
   python start.py web --host 0.0.0.0 --port 8080

5. Replay historical data:
   python start.py replay --date 20251003 --speed 10

Typical Workflow:
1. Start collector to gather real-time data
2. Start web analyzer to visualize
3. Use replay for testing with historical data
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Collector command
    collector_parser = subparsers.add_parser("collector", help="Start data collector")
    collector_parser.add_argument(
        "--limit",
        type=str,
        default="market_open",
        help="Limit of collector to stop at certain market event",
    )

    # Web analyzer command
    web_parser = subparsers.add_parser("web", help="Start web analyzer")
    web_parser.add_argument("--host", default="localhost", help="Host to bind to")
    web_parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    web_parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    web_parser.add_argument("--load-history", help="Load historical data (YYYYMMDD)")
    web_parser.add_argument("--replay-date", help="receive specific replay date data")
    web_parser.add_argument(
        "--backtrace", action="store_true", help="Redis replay toggle"
    )

    # Replayer command
    replay_parser = subparsers.add_parser("replay", help="Start data replayer")
    replay_parser.add_argument(
        "--replay-date", required=True, help="Date to replay (YYYYMMDD)"
    )
    replay_parser.add_argument(
        "--speed", type=float, default=1.0, help="Replay speed multiplier"
    )
    replay_parser.add_argument(
        "--type", type=str, default="collector_replay", help="Choose replayer type"
    )
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        if args.command == "collector":
            start_collector(limit=args.limit)
        elif args.command == "web":
            start_analyzer_web(
                host=args.host,
                port=args.port,
                debug=args.debug,
                load_history=args.load_history,
                replay_date=args.replay_date,
                backtrace=args.backtrace,
            )
        elif args.command == "replay":
            if args.type == "trade_replay":
                start_trades_replayer(args.replay_date, args.speed)
            elif args.type == "collector_replay":
                start_replayer(args.replay_date, args.speed)

    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

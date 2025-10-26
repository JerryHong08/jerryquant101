#!/usr/bin/env python3
"""
Market Mover Web Analyzer - Quick Start Script
Provides easy startup commands for different modes
"""

import argparse
import os
import subprocess
import sys


def start_collector():
    """Start the collector process"""
    collector_path = os.path.join(os.path.dirname(__file__), "collector.py")
    print("🚀 Starting Market Mover Collector...")
    subprocess.run([sys.executable, collector_path])


def start_analyzer_cli():
    """Start analyzer in CLI mode"""
    analyzer_path = os.path.join(os.path.dirname(__file__), "analyzer.py")
    print("🖥️  Starting Market Mover Analyzer (CLI mode)...")
    subprocess.run([sys.executable, analyzer_path, "--mode", "cli"])


def start_analyzer_web(host="localhost", port=5000, debug=False, load_history=None):
    """Start analyzer in web mode"""
    analyzer_path = os.path.join(os.path.dirname(__file__), "analyzer.py")
    cmd = [
        sys.executable,
        analyzer_path,
        "--mode",
        "web",
        "--host",
        host,
        "--port",
        str(port),
    ]

    if debug:
        cmd.append("--debug")

    if load_history:
        cmd.extend(["--load-history", load_history])

    print(f"🌐 Starting Market Mover Web Analyzer on http://{host}:{port}")
    if load_history:
        print(f"📚 Loading historical data for: {load_history}")

    subprocess.run(cmd)


def start_replayer(date, speed=1.0):
    """Start replayer for historical data"""
    replayer_path = os.path.join(os.path.dirname(__file__), "replayer.py")
    print(f"⏪ Starting Market Mover Replayer for {date} at {speed}x speed...")
    subprocess.run(
        [sys.executable, replayer_path, "--date", date, "--speed", str(speed)]
    )


def start_trades_replayer(date, speed=1.0):
    """Start trade level replayer for historical data"""
    trades_replayer_path = os.path.join(
        os.path.dirname(__file__), "trades_replayer_v2.py"
    )
    print(
        f"⏪ Starting Market Mover trades_replayer_v2 for {date} at {speed}x speed..."
    )
    subprocess.run(
        [sys.executable, trades_replayer_path, "--date", date, "--speed", str(speed)]
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

6. Traditional CLI mode:
   python start.py cli

Typical Workflow:
1. Start collector to gather real-time data
2. Start web analyzer to visualize
3. Use replay for testing with historical data
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Collector command
    subparsers.add_parser("collector", help="Start data collector")

    # CLI analyzer command
    subparsers.add_parser("cli", help="Start analyzer in CLI mode")

    # Web analyzer command
    web_parser = subparsers.add_parser("web", help="Start web analyzer")
    web_parser.add_argument("--host", default="localhost", help="Host to bind to")
    web_parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    web_parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    web_parser.add_argument("--load-history", help="Load historical data (YYYYMMDD)")

    # Replayer command
    replay_parser = subparsers.add_parser("replay", help="Start data replayer")
    replay_parser.add_argument(
        "--date", required=True, help="Date to replay (YYYYMMDD)"
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
            start_collector()
        elif args.command == "cli":
            start_analyzer_cli()
        elif args.command == "web":
            start_analyzer_web(
                host=args.host,
                port=args.port,
                debug=args.debug,
                load_history=args.load_history,
            )
        elif args.command == "replay":
            if args.type == "trade_replay":
                start_trades_replayer(args.date, args.speed)
            elif args.type == "collector_replay":
                start_replayer(args.date, args.speed)

    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

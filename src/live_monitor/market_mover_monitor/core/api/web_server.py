"""
Web Server for Market Mover Real-time Visualization
Uses Flask + WebSocket for real-time data streaming
a prototype version of future bff
"""

import asyncio
import concurrent.futures
import json
import logging
from datetime import datetime
from pathlib import Path
from threading import Thread
from typing import Dict, List, Optional

import polars as pl
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit

from live_monitor.market_mover_monitor.core.analyzer.overviewchartdataManagement import (
    ChartDataManager,
)
from live_monitor.market_mover_monitor.core.analyzer.snapshotdataAnalyzer import (
    SnapshotAnalyzer,
)
from live_monitor.market_mover_monitor.core.analyzer.snapshotdataReceiver import (
    redis_engine,
)
from live_monitor.market_mover_monitor.core.data.providers.fundamentals import (
    FloatSharesProvider,
)
from live_monitor.market_mover_monitor.core.utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class WebAnalyzer:
    """Main web analyzer class combining Redis listener and WebSocket server"""

    def __init__(
        self,
        host="localhost",
        port=5000,
        replay_date=None,
        replay_id=None,
        load_history=None,
    ):
        current_dir = Path(__file__).parent  # core/api/
        front_dir = current_dir.parent.parent / "frontend"
        template_dir = front_dir / "templates"
        static_dir = front_dir / "static"

        self.app = Flask(
            __name__,
            template_folder=str(template_dir),
            static_folder=str(static_dir),
            static_url_path="/static",
        )
        self.app.config["SECRET_KEY"] = "market_mover_secret"
        # Remove async_mode parameter to avoid early binding
        self.socketio = SocketIO(
            self.app,
            cors_allowed_origins="*",
            async_mode="threading",  # Explicitly set threading mode
        )

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self.host = host
        self.port = port

        # Initialize snapshot analyzer (handles data processing, membership, state)
        self.snapshot_analyzer = SnapshotAnalyzer(
            replay_date=replay_date,
            replay_id=replay_id,
        )

        # Initialize chart data manager (handles visualization data formatting)
        self.chart_manager = ChartDataManager(
            replay_date=replay_date,
            replay_id=replay_id,
        )

        self.last_df = pl.DataFrame()

        # Connected clients
        self.connected_clients = set()

        self._setup_routes()
        self._setup_socket_events()

        # Create callback function for redis_engine
        def redis_data_callback(processed_df, is_historical=False):
            self.last_df = processed_df

            # Process snapshot through the analyzer
            result = self.snapshot_analyzer.process_snapshot(
                processed_df, is_historical
            )

            # Mark chart data as dirty to trigger refresh
            self.chart_manager.mark_dirty()

            # Get chart data from chart manager
            chart_data = self.chart_manager.get_mmm_version_chart_data(
                force_refresh=True
            )

            logger.debug(
                f"Snapshot processed: {result.get('new_subscriptions', [])} new subs, "
                f"{result.get('total_subscribed', 0)} total, "
                f"{len(result.get('state_changes', []))} state changes"
            )

            print(
                f"Chart data summary: {len(chart_data.get('datasets', []))} datasets, "
                f"{len(chart_data.get('highlights', []))} highlights"
            )

            if chart_data.get("datasets"):
                sample_dataset = chart_data["datasets"][0]
                print(
                    f"Sample dataset: {sample_dataset['label']}, "
                    f"{len(sample_dataset['data'])} data points, "
                    f"rank: {sample_dataset.get('rank', 'N/A')}, "
                    f"state: {sample_dataset.get('state', 'N/A')}"
                )

            self.socketio.emit("chart_update", chart_data)

            print(f"Broadcasting to {len(self.connected_clients)} clients")

        # Initialize redis engine with callback
        self.redis_engine = redis_engine(
            data_callback=redis_data_callback,
            replay_date=replay_date,
            backtrace=load_history if load_history else False,
        )

    def _setup_routes(self):
        """Setup Flask routes"""

        @self.app.route("/")
        def index():
            return render_template("index.html")

        @self.app.route("/debug")
        def debug():
            return render_template("debug.html")

        @self.app.route("/simple")
        def simple():
            return render_template("simple.html")

        @self.app.route("/api/stock/<ticker>")
        def get_stock_detail(ticker):
            """API endpoint to get detailed stock information"""
            state = self.chart_manager.get_ticker_latest_state(ticker)
            history = self.chart_manager.get_ticker_history(ticker, limit=100)

            if state or history:
                return json.dumps(
                    {
                        "ticker": ticker,
                        "state": state,
                        "history": [
                            {
                                "timestamp": h["timestamp"].isoformat(),
                                "percent_change": h["percent_change"],
                                "current_price": h["current_price"],
                                "rank": h["rank"],
                            }
                            for h in history
                        ],
                    },
                    default=str,
                )
            else:
                return json.dumps({"error": "Stock not found"}), 404

        @self.app.route("/api/subscribed")
        def get_subscribed_tickers():
            """API endpoint to get all subscribed tickers"""
            tickers = self.snapshot_analyzer.get_subscribed_tickers()
            return json.dumps({"tickers": tickers, "count": len(tickers)})

        @self.app.route("/api/state-cursors")
        def get_state_cursors():
            """API endpoint to get all state cursors (for debugging)"""
            cursors = self.snapshot_analyzer.get_all_state_cursors()
            return json.dumps(cursors, default=str)

        @self.app.route("/api/initialize/<date>")
        def initialize_historical_data(date):
            """API endpoint to load historical data"""
            try:
                # Historical data loading via redis_engine
                self.redis_engine.initialize_from_local_file(date)
                chart_data = self.chart_manager.get_mmm_version_chart_data(
                    force_refresh=True
                )
                return json.dumps(chart_data, default=str)
            except Exception as e:
                return json.dumps({"error": str(e)}), 500

        @self.app.route("/api/test-data")
        def test_data():
            """Test endpoint to check current data"""
            chart_data = self.chart_manager.get_mmm_version_chart_data()
            subscribed = self.snapshot_analyzer.get_subscribed_tickers()
            return {
                "datasets_count": len(chart_data.get("datasets", [])),
                "highlights_count": len(chart_data.get("highlights", [])),
                "subscribed_count": len(subscribed),
                "sample_dataset": (
                    chart_data.get("datasets", [{}])[0]
                    if chart_data.get("datasets")
                    else None
                ),
            }

    def _setup_socket_events(self):
        """Setup WebSocket event handlers"""

        @self.socketio.on("connect")
        def handle_connect():
            print(f"Client connected: {request.sid}")
            self.connected_clients.add(request.sid)

            # Send current chart data to new client
            chart_data = self.chart_manager.get_mmm_version_chart_data()
            emit("chart_update", chart_data)

        @self.socketio.on("disconnect")
        def handle_disconnect():
            print(f"Client disconnected: {request.sid}")
            self.connected_clients.discard(request.sid)

        @self.socketio.on("request_stock_detail")
        def handle_stock_detail_request(data):
            """Handle request for detailed stock information"""
            ticker = data.get("ticker")
            if not ticker:
                emit("error", {"message": "No ticker specified"})
                return

            state = self.chart_manager.get_ticker_latest_state(ticker)
            history = self.chart_manager.get_ticker_history(ticker, limit=100)

            if state or history:
                emit(
                    "stock_detail",
                    {
                        "ticker": ticker,
                        "state": state,
                        "history_count": len(history),
                    },
                )
            else:
                emit("error", {"message": f"Stock {ticker} not found"})

        @self.socketio.on("load_historical_data")
        def handle_load_historical(data):
            """Handle request to load historical data"""
            date = data.get("date", datetime.now().strftime("%Y%m%d"))
            try:
                self.redis_engine.initialize_from_local_file(date)
                chart_data = self.chart_manager.get_mmm_version_chart_data(
                    force_refresh=True
                )
                emit("historical_data_loaded", chart_data)
            except Exception as e:
                emit("error", {"message": str(e)})

        @self.socketio.on("refresh_chart")
        def handle_refresh_chart():
            """Handle request to force refresh chart data"""
            chart_data = self.chart_manager.get_mmm_version_chart_data(
                force_refresh=True
            )
            emit("chart_update", chart_data)

    def start_redis_listener(self):
        """Start Redis listener in background thread"""
        redis_thread = Thread(
            target=self.redis_engine._redis_stream_listener, daemon=True
        )
        redis_thread.start()
        return redis_thread

    def run(self, debug=False):
        """Run the web server"""
        print(f"Starting Market Mover Web Analyzer on {self.host}:{self.port}")

        # Start Redis listener
        self.start_redis_listener()

        # Run Flask-SocketIO server
        self.socketio.run(
            self.app,
            host=self.host,
            port=self.port,
            debug=debug,
            allow_unsafe_werkzeug=True,
        )

    def cleanup(self):
        """Clean up resources on shutdown"""
        self.snapshot_analyzer.close()
        self.chart_manager.close()
        logger.info("WebAnalyzer resources cleaned up")


if __name__ == "__main__":
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Market Mover Web Analyzer")
    parser.add_argument("--host", default="localhost", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument(
        "--load-history", help="Load historical data for date (YYYYMMDD)"
    )
    parser.add_argument("--replay-date", help="Receive specific replay date data")
    parser.add_argument(
        "--replay-id", help="Custom replay identifier for InfluxDB tagging"
    )

    args = parser.parse_args()

    # Create and configure web analyzer
    web_analyzer = WebAnalyzer(
        host=args.host,
        port=args.port,
        replay_date=args.replay_date,
        replay_id=args.replay_id,
        load_history=args.load_history,
    )

    try:
        # Start the server
        web_analyzer.run(debug=args.debug)
    finally:
        web_analyzer.cleanup()

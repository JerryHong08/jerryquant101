"""
Web Server for Market Mover Real-time Visualization
Uses Flask + WebSocket for real-time data streaming
a prototype version of future bff
"""

import concurrent.futures
import json
import logging
from datetime import datetime
from pathlib import Path
from threading import Thread
from typing import Dict, List, Optional

import polars as pl
import redis
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit

from live_monitor.market_mover_monitor.core.analyzer.overviewchartdataManagement import (
    ChartDataManager,
)
from live_monitor.market_mover_monitor.core.analyzer.snapshotProcessor import (
    SnapshotProcessor,
)
from live_monitor.market_mover_monitor.core.analyzer.stateMachine import (
    StateMachine,
)
from live_monitor.market_mover_monitor.core.data.providers.fundamentals import (
    FloatSharesProvider,
)
from live_monitor.market_mover_monitor.core.utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True, level=logging.DEBUG)


class WebAnalyzer:
    """Main web analyzer class combining snapshot processor, state machine, and WebSocket server"""

    def __init__(
        self,
        host="localhost",
        port=5000,
        replay_date=None,
        suffix_id=None,
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

        # Initialize Redis for listening to state changes
        self.r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        self.date_suffix = (
            replay_date if replay_date else datetime.now().strftime("%Y%m%d")
        )
        self.STATE_STREAM_NAME = f"movers_state:{self.date_suffix}"
        self.SNAPSHOT_STREAM_NAME = f"market_snapshot_processed:{self.date_suffix}"

        # Initialize chart data manager first (needed by callback)
        self.chart_manager = ChartDataManager(
            replay_date=replay_date,
            suffix_id=suffix_id,
        )

        # Create callback for snapshot processor
        # This ensures chart data is refreshed and emitted synchronously after InfluxDB write
        def on_snapshot_processed(result: Dict, is_historical: bool):
            """Callback invoked after each snapshot is processed and written to InfluxDB."""
            # Mark chart data as dirty to trigger refresh
            self.chart_manager.mark_dirty()

            # Get chart data - InfluxDB write is already complete at this point
            chart_data = self.chart_manager.get_mmm_version_chart_data(
                force_refresh=True
            )

            logger.debug(
                f"on_snapshot_processed - Snapshot processed: "
                f"{result.get('new_subscriptions', [])} new subs, "
                f"{result.get('total_subscribed', 0)} total"
            )

            # Emit to all connected WebSocket clients
            self.socketio.emit("chart_update", chart_data)

        # Initialize snapshot processor with callback
        self.snapshot_processor = SnapshotProcessor(
            replay_date=replay_date,
            suffix_id=suffix_id,
            load_history=load_history,
            on_snapshot_processed=on_snapshot_processed,
        )

        # Initialize state machine (handles state computation and notifications)
        self.state_machine = StateMachine(
            replay_date=replay_date,
            suffix_id=suffix_id,
        )

        # Connected clients
        self.connected_clients = set()

        self._setup_routes()
        self._setup_socket_events()

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
            tickers = self.snapshot_processor.get_subscribed_tickers()
            return json.dumps({"tickers": tickers, "count": len(tickers)})

        @self.app.route("/api/state-cursors")
        def get_state_cursors():
            """API endpoint to get all state cursors (for debugging)"""
            cursors = self.state_machine.get_all_state_cursors()
            return json.dumps(cursors, default=str)

        @self.app.route("/api/ticker-states")
        def get_ticker_states():
            """API endpoint to get all current ticker states (for debugging)"""
            states = self.state_machine.get_all_ticker_states()
            return json.dumps(states, default=str)

        @self.app.route("/api/test-data")
        def test_data():
            """Test endpoint to check current data"""
            chart_data = self.chart_manager.get_mmm_version_chart_data()
            subscribed = self.snapshot_processor.get_subscribed_tickers()
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

        @self.socketio.on("refresh_chart")
        def handle_refresh_chart():
            """Handle request to force refresh chart data"""
            chart_data = self.chart_manager.get_mmm_version_chart_data(
                force_refresh=True
            )
            emit("chart_update", chart_data)

    def _start_state_stream_listener(self):
        """Listen to state stream and broadcast state changes to WebSocket clients."""
        logger.info("_start_state_stream_listener - Starting state stream listener...")

        # Create consumer group for BFF
        try:
            self.r.xgroup_create(
                self.STATE_STREAM_NAME, "bff_consumers", id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

        consumer_name = f"bff_{datetime.now().timestamp()}"

        while True:
            try:
                messages = self.r.xreadgroup(
                    "bff_consumers",
                    consumer_name,
                    {self.STATE_STREAM_NAME: ">"},
                    count=10,
                    block=1000,
                )

                if messages:
                    for stream_name, message_list in messages:
                        for message_id, message_data in message_list:
                            # Log state change received
                            logger.info(
                                f"_start_state_stream_listener - State change received: "
                                f"{message_data.get('symbol')} {message_data.get('from')} -> {message_data.get('to')}"
                            )

                            # Acknowledge the message
                            self.r.xack(
                                self.STATE_STREAM_NAME, "bff_consumers", message_id
                            )

            except Exception as e:
                logger.error(f"_start_state_stream_listener - Error: {e}")
                import time

                time.sleep(5)

    # NOTE: _start_snapshot_stream_listener removed - replaced by callback approach
    # The callback in SnapshotProcessor ensures chart data is queried only after
    # InfluxDB writes are complete, preventing the timestamp lag issue.

    def run(self, debug=False):
        """Run the web server"""
        print(f"Starting Market Mover Web Analyzer on {self.host}:{self.port}")

        # Start snapshot processor (receives and processes data)
        # Chart updates are now handled via callback in the processor thread
        self.snapshot_processor.start()

        # Start state machine (computes states and writes to state stream)
        self.state_machine.start()

        # Start state stream listener (broadcasts state changes to WebSocket clients)
        state_listener_thread = Thread(
            target=self._start_state_stream_listener, daemon=True
        )
        state_listener_thread.start()

        # Note: Snapshot stream listener removed - chart updates now use callback
        # This ensures InfluxDB writes are complete before chart data is queried

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
        self.snapshot_processor.close()
        self.state_machine.close()
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
        "--suffix-id", help="Custom replay identifier for InfluxDB tagging"
    )

    args = parser.parse_args()

    # Create and configure web analyzer
    web_analyzer = WebAnalyzer(
        host=args.host,
        port=args.port,
        replay_date=args.replay_date,
        suffix_id=args.suffix_id,
        load_history=args.load_history,
    )

    try:
        # Start the server
        web_analyzer.run(debug=args.debug)
    finally:
        web_analyzer.cleanup()

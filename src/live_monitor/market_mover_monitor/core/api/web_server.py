"""
Web Server for Market Mover Real-time Visualization
Uses Flask + WebSocket for real-time data streaming
"""

import json
from datetime import datetime
from pathlib import Path
from threading import Thread

import polars as pl
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit

from live_monitor.market_mover_monitor.core.api.data_manager import DataManager
from live_monitor.market_mover_monitor.core.storage.redis_client import redis_engine


class WebAnalyzer:
    """Main web analyzer class combining Redis listener and WebSocket server"""

    def __init__(self, host="localhost", port=5000, replay_date=None, backtrace=False):
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
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")

        self.host = host
        self.port = port

        # Initialize data manager
        self.data_manager = DataManager()
        self.last_df = pl.DataFrame()

        # Connected clients
        self.connected_clients = set()

        self._setup_routes()
        self._setup_socket_events()

        # Create callback function for redis_engine
        def redis_data_callback(processed_df, original_df):
            self.last_df = processed_df
            self.data_manager.update_from_realtime(processed_df)

            chart_data = self.data_manager.get_chart_data()
            print(
                f"Chart data summary: {len(chart_data.get('datasets', []))} datasets, "
                f"{len(chart_data.get('timestamps', []))} timestamps, "
                f"{len(chart_data.get('highlights', []))} highlights"
            )

            if chart_data.get("datasets"):
                sample_dataset = chart_data["datasets"][0]
                print(
                    f"Sample dataset: {sample_dataset['label']}, "
                    f"{len(sample_dataset['data'])} data points, "
                    f"rank: {sample_dataset.get('rank', 'N/A')}"
                )

            self.socketio.emit("chart_update", chart_data)

            print(
                f"Processed snapshot with {len(original_df)} stocks, "
                f"broadcasting to {len(self.connected_clients)} clients"
            )

        # Initialize redis engine with callback
        self.redis_engine = redis_engine(
            data_callback=redis_data_callback,
            replay_date=replay_date,
            backtrace=backtrace,
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
            stock_detail = self.data_manager.get_stock_detail(ticker)
            if stock_detail:
                return json.dumps(stock_detail, default=str)
            else:
                return json.dumps({"error": "Stock not found"}), 404

        @self.app.route("/api/initialize/<date>")
        def initialize_historical_data(date):
            """API endpoint to load historical data"""
            try:
                self.data_manager.initialize_from_history(date)
                chart_data = self.data_manager.get_chart_data()
                return json.dumps(chart_data, default=str)
            except Exception as e:
                return json.dumps({"error": str(e)}), 500

        @self.app.route("/api/test-data")
        def test_data():
            """Test endpoint to check current data"""
            chart_data = self.data_manager.get_chart_data()
            return {
                "datasets_count": len(chart_data.get("datasets", [])),
                "timestamps_count": len(chart_data.get("timestamps", [])),
                "highlights_count": len(chart_data.get("highlights", [])),
                "sample_dataset": (
                    chart_data.get("datasets", [{}])[0]
                    if chart_data.get("datasets")
                    else None
                ),
                "stock_data_count": len(self.data_manager.stock_data),
            }

    def _setup_socket_events(self):
        """Setup WebSocket event handlers"""

        @self.socketio.on("connect")
        def handle_connect():
            print(f"Client connected: {request.sid}")
            self.connected_clients.add(request.sid)

            # Send current chart data to new client
            chart_data = self.data_manager.get_chart_data()
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

            # detail = self.data_manager.get_stock_detail(ticker)
            detail, future = self.data_manager.get_stock_detail(ticker)
            # Convert datetime objects to ISO format strings for JSON serialization
            serialized_detail = self._serialize_datetime_objects(detail)
            emit(
                "stock_detail_response",
                {"ticker": ticker, "detail": serialized_detail},
            )

            future.add_done_callback(
                lambda future: self._handle_async_float_result(ticker, future)
            )

        @self.socketio.on("load_historical_data")
        def handle_load_historical(data):
            """Handle request to load historical data"""
            date = data.get("date", datetime.now().strftime("%Y%m%d"))
            try:
                self.data_manager.initialize_from_history(date)
                chart_data = self.data_manager.get_chart_data()
                emit("historical_data_loaded", chart_data)
            except Exception as e:
                emit("error", {"message": str(e)})

        @self.socketio.on("toggle_stock_highlight")
        def handle_toggle_highlight(data):
            """Handle request to toggle stock highlight status"""
            ticker = data.get("ticker")
            highlight = data.get("highlight", False)

            if ticker:
                # Update the highlight status in data manager
                success = self.data_manager.toggle_stock_highlight(ticker, highlight)
                if success:
                    # Broadcast updated chart data to all clients
                    chart_data = self.data_manager.get_chart_data()
                    self.socketio.emit("chart_update", chart_data)
                else:
                    emit(
                        "error", {"message": f"Failed to update highlight for {ticker}"}
                    )

    def _handle_async_float_result(self, ticker, future):
        result = future.result()
        if not result:
            return

        self.socketio.emit(
            "stock_float_extra_response", {"ticker": ticker, "extra": result}
        )

    def _serialize_datetime_objects(self, obj):
        """Recursively convert datetime objects to ISO format strings"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {
                key: self._serialize_datetime_objects(value)
                for key, value in obj.items()
            }
        elif isinstance(obj, list):
            return [self._serialize_datetime_objects(item) for item in obj]
        else:
            return obj

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


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Market Mover Web Analyzer")
    parser.add_argument("--host", default="localhost", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument(
        "--load-history", help="Load historical data for date (YYYYMMDD)"
    )
    parser.add_argument("--replay-date", help="receive specific replay date data")
    parser.add_argument(
        "--backtrace", action="store_true", help="Redis backtrace toggle"
    )

    args = parser.parse_args()

    # Create and configure web analyzer
    web_analyzer = WebAnalyzer(
        host=args.host,
        port=args.port,
        replay_date=args.replay_date,
        backtrace=args.backtrace,
    )

    # Load historical data if requested
    if args.load_history:
        print(f"Loading historical data for {args.load_history}")
        web_analyzer.data_manager.initialize_from_history(args.load_history)

    # Start the server
    web_analyzer.run(debug=args.debug)


if __name__ == "__main__":
    main()

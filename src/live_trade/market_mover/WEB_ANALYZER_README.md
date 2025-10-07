# Market Mover Web Analyzer 📈

Real-time market mover visualization system with interactive web interface.

## Features ✨

### Core Functionality
- **Real-time Visualization**: Top 20 market movers with live updating charts
- **Interactive Charts**: Click on any stock line to see detailed information
- **Smart Highlighting**: Automatically highlights new entrants and fast-moving stocks
- **Historical Data Integration**: Load and visualize historical market data
- **Responsive Design**: Works on desktop and mobile devices

### Advanced Features
- **Dynamic Transparency**: Stock lines fade based on ranking (rank 1 = most opaque, rank 20 = most transparent)
- **Hot Mover Detection**: Automatically detects and highlights stocks with rapid rank changes
- **New Entrant Alerts**: Special highlighting for stocks newly entering top 20
- **Time Range Filtering**: View data for specific time periods (5m, 15m, 30m, 1h, or all)
- **Stock Detail Modal**: Comprehensive information panel with mini-charts

### Data Flow
```
Polygon API → Collector → Redis → Web Analyzer → Browser
     ↓
Historical Files (for replay/analysis)
```

## Quick Start 🚀

### 1. Start Real-time System
```bash
# Terminal 1: Start data collector
python start.py collector

# Terminal 2: Start web analyzer
python start.py web

# Open browser to http://localhost:5000
```

### 2. Load Historical Data
```bash
# Start with historical data for Oct 3, 2025
python start.py web --load-history 20251003
```

### 3. Replay Historical Data
```bash
# Terminal 1: Start web analyzer
python start.py web

# Terminal 2: Replay data at 10x speed
python start.py replay --date 20251003 --speed 10
```

## Usage Examples 📋

### Basic Commands
```bash
# Real-time web interface (default)
python analyzer.py --mode web

# Traditional CLI output
python analyzer.py --mode cli

# Custom host and port
python analyzer.py --mode web --host 0.0.0.0 --port 8080

# Debug mode
python analyzer.py --mode web --debug
```

### Using the Start Script
```bash
# Quick commands using start.py
python start.py web                           # Start web interface
python start.py web --host 0.0.0.0          # Bind to all interfaces
python start.py web --load-history 20251003  # Load specific date
python start.py collector                     # Start data collection
python start.py replay --date 20251003       # Replay historical data
```

## Web Interface Guide 🌐

### Main Chart
- **Lines**: Each line represents one stock's percent change over time
- **Colors**: Dynamically generated based on ranking and status
- **Thickness**: Highlighted stocks (new/fast movers) have thicker lines
- **Transparency**: Higher ranked stocks are more opaque

### Interactive Features
- **Click Line**: Opens detailed stock information modal
- **Hover**: Shows basic stock info and rank changes
- **Time Range**: Filter data using dropdown (5m, 15m, 30m, 1h, all)
- **Zoom**: Mouse wheel to zoom, reset zoom button available

### Side Panels

#### Rankings Panel
- Current top 20 stocks with live rankings
- Color-coded percent changes (green=up, red=down)
- Rank velocity indicators (↑5 = moved up 5 positions)
- Click any item to view details

#### Hot Movers Panel
- 🆕 New entrants to top 20
- 🚀 Stocks with rapid rank improvements
- Special highlighting and animations

### Stock Detail Modal
- **Basic Info**: Current price, previous close, percent change
- **Ranking Info**: Current rank, rank changes, first appearance time
- **Volume Data**: Trading volume information
- **Mini Chart**: Individual stock's price movement trend

## Technical Architecture 🏗️

### Components
1. **Data Manager** (`data_manager.py`): Handles data storage and processing
2. **Web Analyzer** (`web_analyzer.py`): Flask + WebSocket server
3. **Frontend** (`templates/index.html`): Interactive web interface
4. **Collector** (`collector.py`): Real-time data collection from Polygon API
5. **Replayer** (`replayer.py`): Historical data replay system

### Data Structure
```python
{
    "ticker_symbol": {
        "timestamps": [...],           # Time series data
        "percent_changes": [...],      # Price change percentages
        "current_rank": int,           # Current ranking position
        "previous_rank": int,          # Previous ranking position
        "rank_velocity": float,        # Ranking change speed
        "highlight": bool,             # Should be highlighted
        "alpha": float,               # Transparency value
        "metadata": {...}             # Additional stock info
    }
}
```

### WebSocket Events
- `chart_update`: Real-time chart data updates
- `stock_detail_response`: Individual stock information
- `historical_data_loaded`: Historical data loading confirmation
- `error`: Error messages and notifications

## Configuration ⚙️

### Environment Variables
```bash
POLYGON_API_KEY=your_polygon_api_key_here
```

### Customizable Parameters
- **Max History Points**: Limit data points for performance (default: 1000)
- **Update Frequency**: Redis message processing rate
- **Highlight Thresholds**: Rank change sensitivity for highlighting
- **Color Schemes**: Customizable color generation for different stocks

## Troubleshooting 🔧

### Common Issues

1. **Connection Failed**
   - Check Redis server is running: `redis-server`
   - Verify Polygon API key is set correctly

2. **No Data Showing**
   - Ensure collector is running and publishing data
   - Check Redis connection: `redis-cli ping`

3. **Performance Issues**
   - Reduce max_history_points in DataManager
   - Use time range filtering for large datasets

4. **Port Already in Use**
   - Use different port: `--port 8080`
   - Kill existing process: `pkill -f "python.*analyzer"`

### Debug Mode
```bash
# Enable debug mode for detailed logging
python analyzer.py --mode web --debug
```

## Development 🛠️

### Adding New Features
The system is designed for extensibility:

1. **New Highlight Rules**: Modify `DataManager._process_snapshot()`
2. **Custom Colors**: Update `_get_stock_color()` method  
3. **Additional Metrics**: Extend metadata structure
4. **New Visualizations**: Add chart types in frontend JavaScript

### Plugin Architecture
Future extensions can include:
- Custom indicator overlays
- Alert systems for specific conditions
- Export functionality for charts and data
- Integration with additional data sources

## API Endpoints 🔌

### REST API
- `GET /` - Main web interface
- `GET /api/stock/<ticker>` - Get detailed stock information
- `GET /api/initialize/<date>` - Load historical data for specific date

### WebSocket Events
- `connect` - Client connection established
- `request_stock_detail` - Request specific stock information
- `load_historical_data` - Load historical data for date

## Performance Metrics 📊

### Optimizations
- **Memory Management**: Automatic cleanup of old data
- **Efficient Updates**: Delta-based chart updates
- **Data Compression**: Optimized WebSocket message size
- **Responsive Design**: Adaptive UI for different screen sizes

### Scalability
- **Multi-client Support**: Multiple browser connections
- **Background Processing**: Non-blocking data processing
- **Resource Limits**: Configurable memory and CPU usage

## Changelog 📝

### v2.0.0 (Current)
- ✅ Real-time web visualization
- ✅ Interactive stock details
- ✅ Smart highlighting system
- ✅ Historical data integration
- ✅ Responsive design
- ✅ Multi-mode operation (CLI/Web)

### v1.0.0 (Legacy)
- ✅ Basic CLI output
- ✅ Redis data processing
- ✅ Top 20 rankings

## Contributing 🤝

Feel free to submit issues and feature requests. The codebase is designed to be modular and extensible.

## License 📄

See LICENSE file for details.
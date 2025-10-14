import polars as pl

from backtesting.visualizer import BacktestVisualizer
from cores.data_loader import stock_load_process
from strategies.indicators.registry import get_indicator
from visualizer.figure import Visualizer

visualizer = BacktestVisualizer()
flexible_visualizer = Visualizer()

ticker = "TSLA"
start_date = "2023-01-01"
end_date = "2023-12-31"
ohlcv_data = stock_load_process(ticker, start_date, end_date).collect()
print(ohlcv_data.head())

func = get_indicator("obv")
indicators = func(ohlcv_data)
with pl.Config(tbl_cols=20):
    print(indicators)

flexible_visualizer.plot_candlestick_with_signals(
    ohlcv_data,
    trades=None,
    ticker=ticker,
    start_date=start_date,
    end_date=end_date,
    # indicators=indicators,
)

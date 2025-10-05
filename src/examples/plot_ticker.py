import polars as pl

from backtesting.visualizer import BacktestVisualizer
from core_2.data_loader import stock_load_process
from strategies.indicators.registry import get_indicator

visualizer = BacktestVisualizer()

ticker = "TSLA"
start_date = "2023-01-01"
end_date = "2023-12-31"
ohlcv_data = stock_load_process(ticker, start_date, end_date).collect()
print(ohlcv_data.head())

func = get_indicator("bbiboll")
indicators = func(ohlcv_data)

visualizer.plot_candlestick_with_signals(
    ohlcv_data,
    trades=None,
    ticker=ticker,
    start_date=start_date,
    end_date=end_date,
    indicators=indicators,
)

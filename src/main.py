import matplotlib.pyplot as plt
import polars as pl

from utils.longbridge_utils import update_watchlist

still_holding = pl.read_csv("stil_holding.csv")
tickers = still_holding.sort("buy_signal_date").select("ticker").to_series().to_list()

update_watchlist(watchlist_name="bbiboll0929", tickers=tickers)

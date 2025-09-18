import numpy as np
import pandas as pd
import polars as pl

trades = pl.read_csv("backtest_output/bbiboll/bbiboll_trades.csv")
trades = trades.select(pl.exclude(["block_id", "buy_row_id", "sell_row_id"]))
# ['ticker', 'buy_date', 'buy_open', 'sell_date', 'sell_open', 'return %']

import matplotlib.pyplot as plt

returns = trades["return %"].to_numpy()
x = np.arange(len(returns))

sorted_returns = np.sort(returns)
cdf = np.arange(1, len(returns) + 1) / len(returns)
plt.plot(sorted_returns, cdf)
plt.xlabel("Return %")
plt.ylabel("Cumulative Probability")
plt.title("Cumulative Distribution of Returns")
plt.axvline(x=0, color="red", linestyle="--")
plt.show()

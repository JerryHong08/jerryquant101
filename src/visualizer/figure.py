from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
import polars as pl


class Visualizer:
    def __init__(self, figsize=(12, 8)):
        self.fig = plt.figure(figsize=figsize)
        self.subplots = []

    def add_subplot(self, height_ratio=1.0):
        self.subplots.append({"height_ratio": height_ratio, "plotters": []})
        return self

    def add_plotter(self, subplot_index, plotter):
        self.subplots[subplot_index]["plotters"].append(plotter)
        return self

    def draw(self, data):
        n = len(self.subplots)

        height_ratios = [sp["height_ratio"] for sp in self.subplots]
        gs = self.fig.add_gridspec(n, 1, height_ratios=height_ratios, hspace=0.05)

        axes = []
        for i, sp in enumerate(self.subplots):
            ax = self.fig.add_subplot(gs[i, 0])
            axes.append(ax)
            for plotter in sp["plotters"]:
                plotter(ax, data)

        return self.fig

    def plot_candlestick_with_signals(
        self,
        ohlcv_data: pl.DataFrame,
        ticker: str,
        trades: pl.DataFrame,
        open_positions: Optional[pl.DataFrame] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        indicators: Optional[pl.DataFrame] = None,
        line: Optional[bool] = True,
        save_path: Optional[str] = None,
    ):
        # data preparation
        ticker_data = ohlcv_data.filter(pl.col("ticker") == ticker)
        if start_date:
            ticker_data = ticker_data.filter(
                pl.col("timestamps").dt.date() >= pl.lit(start_date).str.to_date()
            )
        if end_date:
            ticker_data = ticker_data.filter(
                pl.col("timestamps").dt.date() <= pl.lit(end_date).str.to_date()
            )
        if ticker_data.is_empty():
            print(f"not find {ticker} data in given date range")
            return

        df = ticker_data.to_pandas().sort_values("timestamps").reset_index(drop=True)

        dates = df["timestamps"].values
        date_labels = [
            d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in dates
        ]

        # we need to know what indicators are needed to plot,
        # and then we can create subplots accordingly
        if indicators is None or indicators.is_empty():
            indicators = pl.DataFrame()
            indicator_cols = []
        else:
            indicators = indicators.filter(pl.col("ticker") == ticker)
            indicator_cols = [
                col
                for col in indicators.columns
                if col
                not in [
                    "ticker",
                    "timestamps",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "window_start",
                    "transactions",
                ]
            ]

        print(f"indicators to plot: {indicator_cols}")

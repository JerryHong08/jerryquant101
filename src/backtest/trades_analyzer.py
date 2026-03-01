"""
Trades Analyzer — loads backtest open-position CSVs, enriches them with
delisting info, and generates an animated Plotly scatter chart.
"""

import sys
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import polars as pl

from data_supply.date_utils import generate_backtest_date
from data_supply.ticker_utils import get_mapped_tickers

STRATEGY_NAME = "bbiboll"

DELISTED_UTC_FMT = "%Y-%m-%dT%H:%M:%SZ"

# ── colour / marker constants ───────────────────────────────────────────
STATUS_COLORS = {"Active": "blue", "Delisted": "red"}

HOVER_TEMPLATE = (
    "<b>%{hovertext}</b><br><br>"
    "Buy date: %{x}<br>"
    "Buy Price: $%{y:.2f}<br>"
    "Holding days: %{customdata[0]}<br>"
    "Status: %{customdata[1]}<br>"
    "Delisted info: %{customdata[2]}<br>"
    "<extra></extra>"
)


# ── data helpers ────────────────────────────────────────────────────────


def load_backtest_data(date: str, strategy_name: str = STRATEGY_NAME) -> pl.DataFrame:
    """Load a single backtest CSV and tag it with *date*."""
    file_path = (
        f"backtest_output/{strategy_name}/{date}/{strategy_name}_open_positions.csv"
    )
    try:
        df = pl.read_csv(file_path, try_parse_dates=True)
        return df.with_columns(pl.lit(date).alias("backtest_date"))
    except Exception as e:
        print(f"error: load backtest data error {date}: {e}")
        return pl.DataFrame()


def load_all_backtest_data(
    backtest_dates: list[str],
    strategy_name: str = STRATEGY_NAME,
) -> pl.DataFrame:
    """Load and concatenate backtest data for every date in *backtest_dates*."""
    frames = [
        df
        for date in backtest_dates
        if not (df := load_backtest_data(date, strategy_name)).is_empty()
    ]
    if not frames:
        print("error: not found backtest data")
        sys.exit(1)
    return pl.concat(frames)


def enrich_with_delisting(combined: pl.DataFrame) -> pl.DataFrame:
    """
    Join with ticker-mapping data to derive delisting status, hold days, etc.
    """
    delisted_info = get_mapped_tickers()

    processed = combined.join(
        delisted_info.select(["ticker", "all_delisted_utc", "tickers"]),
        on="ticker",
        how="left",
    ).drop(["sell_signal_date", "sell_open"])

    processed = (
        processed.with_columns(
            # Count only non-null, non-empty delisting dates.
            # null means "no delisting info" (i.e. still active), so exclude them.
            pl.col("all_delisted_utc")
            .list.eval(pl.element().is_not_null() & (pl.element() != ""))
            .list.sum()
            .fill_null(0)
            .alias("delisted_len"),  # all_delisted_utc len
            pl.col("tickers").list.len().alias("tickers_len"),  # tickers len
        )
        .with_columns(
            pl.when(
                pl.col("delisted_len") < pl.col("tickers_len")
            )  # if delisted_len < tickers_len, it means at least one ticker is still active
            .then(
                pl.lit("")
            )  # it means "Active", so we set single_delisted_utc to empty string for easier processing later
            .otherwise(
                pl.col("all_delisted_utc").list.last()
            )  # if all tickers are delisted, we take the last delisting date (the most recent one) as the single_delisted_utc for this ticker
            .alias("single_delisted_utc")
        )
        .with_columns(
            _is_active_expr().alias("status"),
            _delisted_info_expr().alias("delisted_info"),
        )
        .with_columns(
            _hold_days_expr().alias("hold_days"),
        )
        .sort(["backtest_date", "buy_date"])
    )
    return processed


# ── reusable Polars expressions ─────────────────────────────────────────


def _is_active_expr() -> pl.Expr:
    """Return 'Active' when the ticker was not delisted before the backtest date."""
    return (
        pl.when(
            pl.col("single_delisted_utc").is_null()
            | (pl.col("single_delisted_utc") == "")
            | (
                pl.col("single_delisted_utc")
                .str.to_date(DELISTED_UTC_FMT, strict=False)
                .dt.date()
                > pl.col("backtest_date").str.to_date().dt.date()
            )
        )
        .then(pl.lit("Active"))
        .otherwise(pl.lit("Delisted"))
    )


def _delisted_info_expr() -> pl.Expr:
    """Return empty string for active tickers, or 'Delisted: <date>' otherwise."""
    return (
        pl.when(
            pl.col("single_delisted_utc").is_null()
            | (pl.col("single_delisted_utc") == "")
            | (
                pl.col("single_delisted_utc")
                .str.to_date(DELISTED_UTC_FMT, strict=False)
                .dt.date()
                > pl.col("backtest_date").str.to_date().dt.date()
            )
        )
        .then(pl.lit(""))
        .otherwise("Delisted: " + pl.col("single_delisted_utc"))
    )


def _hold_days_expr() -> pl.Expr:
    """Number of calendar days held (capped at delisting date when applicable)."""
    return (
        pl.when(pl.col("delisted_info") == "")
        .then(
            (
                pl.lit(datetime.now().date()) - pl.col("buy_date").dt.date()
            ).dt.total_days()
        )
        .otherwise(
            (
                pl.col("single_delisted_utc")
                .str.to_date(DELISTED_UTC_FMT, strict=False)
                .dt.date()
                - pl.col("buy_date").dt.date()
            ).dt.total_days()
        )
    )


# ── animation helpers ──────────────────────────────────────────────────


def _compute_frame_hold_days(frame: pd.DataFrame) -> pd.Series:
    """Vectorised hold-days for a single animation frame (pandas)."""
    return np.where(
        frame["delisted_info"] == "",
        (
            pd.to_datetime(frame["backtest_date"]) - pd.to_datetime(frame["buy_date"])
        ).dt.days,
        (
            pd.to_datetime(
                frame["single_delisted_utc"],
                format=DELISTED_UTC_FMT,
                errors="coerce",
            )
            - pd.to_datetime(frame["buy_date"])
        ).dt.days,
    )


def _make_scatter_trace(
    status_data: pd.DataFrame,
    status: str,
) -> go.Scatter:
    """Build a single Plotly Scatter trace for one status group."""
    empty = len(status_data) == 0
    return go.Scatter(
        x=[] if empty else status_data["buy_date"],
        y=[] if empty else status_data["buy_price"],
        mode="markers",
        marker=dict(
            size=10,
            symbol="circle",
            color=STATUS_COLORS[status],
            opacity=0.8,
            line=dict(width=1, color="white"),
        ),
        name=f"{status} Positions",
        hovertemplate=HOVER_TEMPLATE,
        hovertext=[] if empty else status_data["ticker"],
        customdata=(
            []
            if empty
            else np.column_stack(
                (
                    status_data["current_frame_hold_days"],
                    status_data["status"],
                    status_data["delisted_info"].fillna(""),
                )
            )
        ),
        ids=(
            []
            if empty
            else status_data["ticker"] + "_" + status_data["buy_date"].astype(str)
        ),
    )


def _build_stats_annotation(stats_row: pd.Series) -> dict:
    """Return a Plotly annotation dict for the per-frame stats box."""
    text = (
        f"Total Positions: {stats_row['total_positions']}<br>"
        f"Delisted Counts: {stats_row['delisted_count']}<br>"
        f"Delisted Ratio: {stats_row['delisted_rate']}%<br>"
        f"Avg Holding Days: {stats_row['avg_hold_days']:.1f}"
    )
    return dict(
        x=0.98,
        y=0.98,
        xref="paper",
        yref="paper",
        text=text,
        showarrow=False,
        bgcolor="rgba(255,255,255,0.8)",
        bordercolor="black",
        borderwidth=1,
        font=dict(size=12),
        align="right",
    )


# ── main animation builder ─────────────────────────────────────────────


def create_animation(
    plot_df: pd.DataFrame,
    strategy_name: str = STRATEGY_NAME,
) -> go.Figure:
    """
    Build an animated Plotly figure showing positions across backtest dates.
    """
    df = plot_df.copy()

    # ── per-date aggregate stats ────────────────────────────────────────
    stats_by_date = (
        df.groupby("backtest_date")
        .agg(
            total_positions=("ticker", "count"),
            delisted_count=("status", lambda x: (x == "Delisted").sum()),
        )
        .reset_index()
    )
    stats_by_date["delisted_rate"] = (
        stats_by_date["delisted_count"] / stats_by_date["total_positions"] * 100
    ).round(1)

    # ── axis ranges (log-scale y) ───────────────────────────────────────
    x_range = pd.to_datetime([df["buy_date"].min(), df["buy_date"].max()])
    x_margin = (x_range[1] - x_range[0]) * 0.05
    x_min = x_range[0] - x_margin
    x_max = x_range[1] + x_margin

    y_min_log = np.log10(df["buy_price"].min())
    y_max_log = np.log10(df["buy_price"].max())
    y_margin = (y_max_log - y_min_log) * 0.1
    y_range = [y_min_log - y_margin, y_max_log + y_margin]

    # ── build frames ────────────────────────────────────────────────────
    sorted_dates = sorted(df["backtest_date"].unique())
    frames = []

    for date in sorted_dates:
        frame_data = df[df["backtest_date"] == date].copy()
        frame_data["current_frame_hold_days"] = _compute_frame_hold_days(frame_data)

        frame_traces = [
            _make_scatter_trace(frame_data[frame_data["status"] == status], status)
            for status in STATUS_COLORS
        ]

        # NOTE: avg_hold_days is computed over *all* positions in the frame,
        # not just one status group.
        stats_row = stats_by_date[stats_by_date["backtest_date"] == date].iloc[0].copy()
        stats_row["avg_hold_days"] = frame_data["current_frame_hold_days"].mean()

        frames.append(
            go.Frame(
                data=frame_traces,
                name=str(date),
                layout=go.Layout(annotations=[_build_stats_annotation(stats_row)]),
            )
        )

    # ── initial figure (first frame) ────────────────────────────────────
    fig = go.Figure()

    first_date = sorted_dates[0]
    first_data = df[df["backtest_date"] == first_date].copy()
    first_data["current_frame_hold_days"] = _compute_frame_hold_days(first_data)

    for status in STATUS_COLORS:
        fig.add_trace(
            _make_scatter_trace(first_data[first_data["status"] == status], status)
        )

    first_stats = (
        stats_by_date[stats_by_date["backtest_date"] == first_date].iloc[0].copy()
    )
    first_stats["avg_hold_days"] = first_data["current_frame_hold_days"].mean()

    fig.frames = frames

    # ── layout ──────────────────────────────────────────────────────────
    fig.update_layout(
        title=dict(
            text=f"{strategy_name.upper()} Strategy - Positions Analyzer",
            x=0.5,
            font=dict(size=20),
        ),
        xaxis=dict(
            title="Buy Date",
            gridcolor="lightgray",
            range=[x_min, x_max],
        ),
        yaxis=dict(
            title="Buy Price ($)",
            type="log",
            gridcolor="lightgray",
            range=y_range,
        ),
        width=1400,
        height=800,
        template="plotly_white",
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255,255,255,0.8)",
            itemsizing="constant",
        ),
        annotations=[_build_stats_annotation(first_stats)],
        uirevision="constant",
        updatemenus=[
            dict(
                type="buttons",
                buttons=[
                    dict(
                        label="Play",
                        method="animate",
                        args=[
                            None,
                            {
                                "frame": {"duration": 1200, "redraw": True},
                                "fromcurrent": True,
                                "transition": {
                                    "duration": 800,
                                    "easing": "cubic-in-out",
                                },
                            },
                        ],
                    ),
                    dict(
                        label="Pause",
                        method="animate",
                        args=[
                            [None],
                            {
                                "frame": {"duration": 0, "redraw": False},
                                "mode": "immediate",
                                "transition": {"duration": 0},
                            },
                        ],
                    ),
                ],
                direction="left",
                pad={"r": 10, "t": 10},
                showactive=False,
                x=0.1,
                xanchor="right",
                y=-0.05,
                yanchor="top",
            )
        ],
        sliders=[
            dict(
                steps=[
                    dict(
                        method="animate",
                        args=[
                            [f.name],
                            dict(
                                mode="immediate",
                                frame=dict(duration=1200, redraw=True),
                                transition=dict(duration=800, easing="cubic-in-out"),
                            ),
                        ],
                        label=f.name,
                    )
                    for f in frames
                ],
                transition=dict(duration=800, easing="cubic-in-out"),
                x=0.1,
                y=-0.05,
                currentvalue=dict(
                    font=dict(size=12),
                    prefix="current backtest_date: ",
                    visible=True,
                    xanchor="center",
                ),
                len=0.9,
            )
        ],
    )

    return fig


# ── entry point ─────────────────────────────────────────────────────────


def main() -> None:
    backtest_dates = generate_backtest_date(
        start_date="2026-02-27",
        period="week",
        reverse=True,
        reverse_limit="2024-12-01",
    )

    combined = load_all_backtest_data(backtest_dates)
    processed = enrich_with_delisting(combined)

    print(f"processed_data: {processed.head()}")
    print(f"debug: data len: {len(processed)}")

    plot_df = processed.to_pandas()

    fig = create_animation(plot_df)
    fig.show()
    fig.write_html(
        f"backtest_output/{STRATEGY_NAME}/positions_animation_optimized.html"
    )


if __name__ == "__main__":
    main()

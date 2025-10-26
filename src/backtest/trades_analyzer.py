from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from cores.config import splits_data
from utils.backtest_utils.backtest_utils import generate_backtest_date
from utils.longbridge_utils.update_watchlist import update_watchlist

strategy_name = "bbiboll"

backtest_dates = generate_backtest_date(
    start_date="2025-10-24",
    period="week",
    reverse=True,
    reverse_limit="2024-12-01",
    # reverse_limit_count=10
)


def load_backtest_data(date):
    """load backtest data"""
    file_path = (
        f"backtest_output/{strategy_name}/{date}/{strategy_name}_open_positions.csv"
    )
    try:
        df = pl.read_csv(file_path, try_parse_dates=True)
        return df.with_columns(pl.lit(date).alias("backtest_date"))
    except Exception as e:
        print(f"error: load backtest data error {date}: {e}")
        return pl.DataFrame()


delisted_info = pl.read_csv(
    "tickers_name_alignment.csv",
    infer_schema_length=50000,
    schema_overrides={"group_id": pl.String},
    try_parse_dates=True,
)

all_backtest_data = []
for date in backtest_dates:
    data = load_backtest_data(date)
    if not data.is_empty():
        all_backtest_data.append(data)

if not all_backtest_data:
    print("error: not found backtest data")
    exit()

combined_data = pl.concat(all_backtest_data)

processed_data = combined_data.join(
    delisted_info.select(["ticker", "all_delisted_utc", "tickers"]),
    on="ticker",
    how="left",
).drop(["sell_signal_date", "sell_open"])

processed_data = (
    processed_data.with_columns(
        pl.col("all_delisted_utc").str.split(","),
        pl.col("tickers").str.split(","),
    )
    .with_columns(
        pl.when(pl.col("all_delisted_utc").list.first() == "")
        .then(pl.lit(0))
        .otherwise(pl.col("all_delisted_utc").list.len())
        .fill_null(0)
        .alias("delisted_len"),
        pl.col("tickers").list.len().alias("tickers_len"),
    )
    .with_columns(
        pl.when(pl.col("delisted_len") < pl.col("tickers_len"))
        .then(pl.lit(""))
        .otherwise(pl.col("all_delisted_utc").list.last())
        .alias("single_delisted_utc")
    )
    .with_columns(
        [
            # status
            pl.when(
                (pl.col("single_delisted_utc") == "")
                | (
                    pl.col("single_delisted_utc")
                    .str.to_date("%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .dt.date()
                    > pl.col("backtest_date").str.to_date().dt.date()
                )
            )
            .then(pl.lit("Active"))
            .otherwise(pl.lit("Delisted"))
            .alias("status"),
            # delisted info
            pl.when(
                (pl.col("single_delisted_utc") == "")
                | (
                    pl.col("single_delisted_utc")
                    .str.to_date("%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .dt.date()
                    > pl.col("backtest_date").str.to_date().dt.date()
                )
            )
            .then(pl.lit(""))
            .otherwise("Delisted: " + pl.col("single_delisted_utc"))
            .alias("delisted_info"),
        ]
    )
    .with_columns(
        [
            # hold_days
            pl.when(pl.col("delisted_info") == "")
            .then(
                (
                    pl.lit(datetime.now().date()) - pl.col("buy_date").dt.date()
                ).dt.total_days()
            )
            .otherwise(
                (
                    pl.col("single_delisted_utc")
                    .str.to_date("%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .dt.date()
                    - pl.col("buy_date").dt.date()
                ).dt.total_days()
            )
            .alias("hold_days"),
        ]
    )
    .sort(["backtest_date", "buy_date"])
)
# ).filter(pl.col('delisted_len')>1)

print(f"debug: data len: {len(processed_data)}")
# with pl.Config(
#     set_fmt_table_cell_list_len=5000,
#     set_fmt_str_lengths=10000,
#     set_tbl_cols=-1,
#     set_tbl_rows=-1
# ):
#     print(
#         processed_data
#         .filter(pl.col('ticker') == 'MLGO')
#     )
# print(multi_ticker_rows)

plot_df = processed_data.to_pandas()


def create_animation():

    plot_df_enhanced = plot_df.copy()

    # initial hold_days for first frame
    # plot_df_enhanced['current_frame_hold_days'] = 0

    stats_by_date = (
        plot_df_enhanced.groupby("backtest_date")
        .agg(
            {
                "ticker": "count",
                "status": lambda x: (x == "Delisted").sum(),
            }
        )
        .rename(
            columns={
                "ticker": "total_positions",
                "status": "delisted_count",
            }
        )
        .reset_index()
    )

    stats_by_date["delisted_rate"] = (
        stats_by_date["delisted_count"] / stats_by_date["total_positions"] * 100
    ).round(1)

    x_min = plot_df_enhanced["buy_date"].min()
    x_max = plot_df_enhanced["buy_date"].max()
    y_min = plot_df_enhanced["buy_price"].min()
    y_max = plot_df_enhanced["buy_price"].max()

    x_range = pd.to_datetime([x_min, x_max])
    x_margin = (x_range[1] - x_range[0]) * 0.05
    x_min_with_margin = x_range[0] - x_margin
    x_max_with_margin = x_range[1] + x_margin

    y_min_log = np.log10(y_min)
    y_max_log = np.log10(y_max)
    y_margin = (y_max_log - y_min_log) * 0.1
    y_min_with_margin = 10 ** (y_min_log - y_margin)
    y_max_with_margin = 10 ** (y_max_log + y_margin)

    fig = go.Figure()

    frames = []

    for date in sorted(plot_df_enhanced["backtest_date"].unique()):
        frame_data = plot_df_enhanced[plot_df_enhanced["backtest_date"] == date]

        # Calculate current_frame_hold_days using pandas
        frame_data = frame_data.copy()

        # polars version
        # frame_data = frame_data.with_columns([
        #     # current_frame_hold_days
        #     pl.when(pl.col('delisted_info') == "")
        #     .then((pl.col('backtest_date').str.to_date().dt.date() - pl.col('buy_date').dt.date()).dt.total_days())
        #     .otherwise((pl.col('single_delisted_utc').str.to_date("%Y-%m-%dT%H:%M:%SZ", strict=False).dt.date() - pl.col('buy_date').dt.date()).dt.total_days())
        #     .alias('current_frame_hold_days'),
        # ])

        # pandas version
        frame_data["current_frame_hold_days"] = np.where(
            frame_data["delisted_info"] == "",
            (
                pd.to_datetime(frame_data["backtest_date"])
                - pd.to_datetime(frame_data["buy_date"])
            ).dt.days,
            (
                pd.to_datetime(
                    frame_data["single_delisted_utc"],
                    format="%Y-%m-%dT%H:%M:%SZ",
                    errors="coerce",
                )
                - pd.to_datetime(frame_data["buy_date"])
            ).dt.days,
        )

        frame_traces = []

        # set status for each frame
        for status in ["Active", "Delisted"]:
            status_data = frame_data[frame_data["status"] == status]

            if len(status_data) > 0:
                trace = go.Scatter(
                    x=status_data["buy_date"],
                    y=status_data["buy_price"],
                    mode="markers",
                    marker=dict(
                        size=10,
                        symbol="circle",
                        color="blue" if status == "Active" else "red",
                        opacity=0.8,
                        line=dict(width=1, color="white"),
                    ),
                    name=f"{status} Positions",
                    hovertemplate=(
                        "<b>%{hovertext}</b><br><br>"
                        + "Buy date: %{x}<br>"
                        + "Buy Price: $%{y:.2f}<br>"
                        + "Holding days: %{customdata[0]}<br>"
                        + "Status: %{customdata[1]}<br>"
                        + "Delisted info: %{customdata[2]}<br>"
                        + "<extra></extra>"
                    ),
                    hovertext=status_data["ticker"],
                    customdata=np.column_stack(
                        (
                            status_data["current_frame_hold_days"],
                            status_data["status"],
                            status_data["delisted_info"].fillna(""),
                        )
                    ),
                    # add ids for ploty to indentify the same data
                    ids=status_data["ticker"]
                    + "_"
                    + status_data["buy_date"].astype(str),
                    # showlegend=True if date == sorted(plot_df_enhanced['backtest_date'].unique())[0] else False
                )
                frame_traces.append(trace)

        # add metrics data for each frame
        stats = stats_by_date[stats_by_date["backtest_date"] == date].iloc[0]
        stats["avg_hold_days"] = status_data["current_frame_hold_days"].mean()
        annotation_text = (
            f"Total Positions: {stats['total_positions']}<br>"
            f"Delisted Counts: {stats['delisted_count']}<br>"
            f"Delisted Ratio: {stats['delisted_rate']}%<br>"
            f"Avg Holding Days: {stats['avg_hold_days']:.1f}"
        )

        frame_layout = go.Layout(
            annotations=[
                dict(
                    x=0.98,
                    y=0.98,
                    xref="paper",
                    yref="paper",
                    text=annotation_text,
                    showarrow=False,
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="black",
                    borderwidth=1,
                    font=dict(size=12),
                    align="right",
                )
            ]
        )

        frames.append(go.Frame(data=frame_traces, name=str(date), layout=frame_layout))

    # initialize the first frame data
    first_date = sorted(plot_df_enhanced["backtest_date"].unique())[0]
    first_frame_data = plot_df_enhanced[plot_df_enhanced["backtest_date"] == first_date]
    first_frame_data["current_frame_hold_days"] = np.where(
        first_frame_data["delisted_info"] == "",
        (
            pd.to_datetime(first_frame_data["backtest_date"])
            - pd.to_datetime(first_frame_data["buy_date"])
        ).dt.days,
        (
            pd.to_datetime(
                first_frame_data["single_delisted_utc"],
                format="%Y-%m-%dT%H:%M:%SZ",
                errors="coerce",
            )
            - pd.to_datetime(first_frame_data["buy_date"])
        ).dt.days,
    )

    for status in ["Active", "Delisted"]:
        status_data = first_frame_data[first_frame_data["status"] == status]

        fig.add_trace(
            go.Scatter(
                x=status_data["buy_date"] if len(status_data) > 0 else [],
                y=status_data["buy_price"] if len(status_data) > 0 else [],
                mode="markers",
                marker=dict(
                    size=10,
                    symbol="circle",
                    color="blue" if status == "Active" else "red",
                    opacity=0.8,
                    line=dict(width=1, color="white"),
                ),
                name=f"{status} Positions",
                hovertemplate=(
                    "<b>%{hovertext}</b><br><br>"
                    + "Buy date: %{x}<br>"
                    + "Buy Price: $%{y:.2f}<br>"
                    + "Holding days: %{customdata[0]}<br>"
                    + "Status: %{customdata[1]}<br>"
                    + "Delisted info: %{customdata[2]}<br>"
                    + "<extra></extra>"
                ),
                hovertext=status_data["ticker"] if len(status_data) > 0 else [],
                customdata=(
                    np.column_stack(
                        (
                            (
                                status_data["current_frame_hold_days"]
                                if len(status_data) > 0
                                else []
                            ),
                            status_data["status"] if len(status_data) > 0 else [],
                            (
                                status_data["delisted_info"].fillna("")
                                if len(status_data) > 0
                                else []
                            ),
                        )
                    )
                    if len(status_data) > 0
                    else []
                ),
                ids=(
                    status_data["ticker"] + "_" + status_data["buy_date"].astype(str)
                    if len(status_data) > 0
                    else []
                ),
            )
        )

    # initial metrics
    first_stats = stats_by_date[stats_by_date["backtest_date"] == first_date].iloc[0]
    first_stats["avg_hold_days"] = first_frame_data["current_frame_hold_days"].mean()
    first_annotation_text = (
        f"Total Positions: {first_stats['total_positions']}<br>"
        f"Delisted Counts: {first_stats['delisted_count']}<br>"
        f"Delisted Ratio: {first_stats['delisted_rate']}%<br>"
        f"Avg Holding Days: {first_stats['avg_hold_days']:.1f}"
    )

    # add frames to fig engine
    fig.frames = frames

    # initial layout
    fig.update_layout(
        title=dict(
            text=f"{strategy_name.upper()} Strategy - Positions Analyzer",
            x=0.5,
            font=dict(size=20),
        ),
        xaxis=dict(
            title="Buy Date",
            gridcolor="lightgray",
            range=[x_min_with_margin, x_max_with_margin],
            # fixedrange=True
        ),
        yaxis=dict(
            title="Buy Price ($)",
            type="log",
            gridcolor="lightgray",
            range=[np.log10(y_min_with_margin), np.log10(y_max_with_margin)],
            # fixedrange=True
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
        annotations=[
            dict(
                x=0.98,
                y=0.98,
                xref="paper",
                yref="paper",
                text=first_annotation_text,
                showarrow=False,
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="black",
                borderwidth=1,
                font=dict(size=12),
                align="right",
            )
        ],
        # add uirevision to make sure stable transmssion
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
    )

    sliders = [
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
    ]

    fig.update_layout(sliders=sliders)

    return fig


optimized_fig = create_animation()
optimized_fig.show()
optimized_fig.write_html(
    f"backtest_output/{strategy_name}/positions_animation_optimized.html"
)

print("\n=== Metrics for each backtest_date ===")
for date in backtest_dates:
    date_data = plot_df[plot_df["backtest_date"] == date]
    active_count = (date_data["status"] == "Active").sum()
    delisted_count = (date_data["status"] == "Delisted").sum()
    total_count = len(date_data)

    print(f"\n{date}:")
    print(f"  Total holdings: {total_count}")
    print(f"  Still active: {active_count}")
    print(f"  Delisted: {delisted_count}")
    print(
        f"  Delisting Rate: {delisted_count/total_count*100:.1f}%"
        if total_count > 0
        else "  Delisted Ratio: 0%"
    )

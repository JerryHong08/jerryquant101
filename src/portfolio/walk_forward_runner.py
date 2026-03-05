"""
Walk-Forward Runner — Execute a pipeline across walk-forward folds.

Bridges ``validation.walk_forward.walk_forward_split()`` with
``portfolio.pipeline.run_alpha_pipeline()`` so that each fold gets
its own in-sample / out-of-sample evaluation.

Usage:
    from portfolio.walk_forward_runner import run_walk_forward

    results = run_walk_forward(
        ohlcv,
        train_days=126,
        test_days=63,
        embargo_days=5,
        factor_names=["bbiboll", "vol_ratio"],
        sizing_method="Signal-Weighted",
    )
    print(results["oos_sharpe_mean"])

Reference: guidance/quant_lab.pdf — Part V
"""

from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl

from constants import DATE_COL, OHLCV_DATE_COL, TICKER_COL, TRADING_DAYS_PER_YEAR
from portfolio.alpha_config import AlphaConfig
from portfolio.pipeline import run_alpha_pipeline
from validation.walk_forward import (
    WalkForwardFold,
    apply_folds_to_dates,
    summarize_folds,
    walk_forward_split,
)


def _slice_ohlcv(
    ohlcv: pl.DataFrame,
    start_date,
    end_date,
    date_col: str = OHLCV_DATE_COL,
) -> pl.DataFrame:
    """Slice OHLCV data to a date range (inclusive)."""
    return ohlcv.filter(
        (pl.col(date_col) >= start_date) & (pl.col(date_col) <= end_date)
    )


def run_walk_forward(
    ohlcv: pl.DataFrame,
    config: AlphaConfig | None = None,
    *,
    # ── Legacy kwargs (used if config is None) ──
    train_days: int = 126,
    test_days: int = 63,
    embargo_days: int = 5,
    mode: str = "rolling",
    factor_names: list[str] | None = None,
    sizing_method: str = "Signal-Weighted",
    combination_method: str = "equal_weight",
    rebal_every_n: int = 5,
    n_long: int = 10,
    n_short: int = 10,
    target_vol: float = 0.10,
    annualization: int = TRADING_DAYS_PER_YEAR,
    date_col: str = OHLCV_DATE_COL,
    verbose: bool = True,
) -> dict[str, Any]:
    """Run the alpha pipeline across walk-forward validation folds.

    Accepts either an ``AlphaConfig`` object (preferred) or individual
    keyword arguments (backward-compatible).

    For each fold:
        1. Slice OHLCV into training period → run pipeline → in-sample metrics.
        2. Slice OHLCV into test period → run pipeline → out-of-sample metrics.
        3. Record IS/OOS Sharpe, return, vol per fold.

    Args:
        ohlcv: Full OHLCV DataFrame.
        train_days: Training window size (trading days).
        test_days: Test window size (trading days).
        embargo_days: Gap between train end and test start.
        mode: "rolling" or "anchored".
        factor_names: Factors to compute (default: ["bbiboll", "vol_ratio"]).
        sizing_method: Sizing method name.
        combination_method: Factor combination method.
        rebal_every_n: Rebalance frequency.
        n_long: Number of long positions.
        n_short: Number of short positions.
        target_vol: Target vol for vol-target sizing.
        annualization: Trading days per year.
        date_col: Date column in OHLCV data.
        verbose: Print fold progress.

    Returns:
        Dict with:
            - folds: List of fold metadata dicts (fold_id, train/test dates)
            - fold_results: List of per-fold result dicts with IS/OOS metrics
            - is_sharpes: List of in-sample Sharpe ratios
            - oos_sharpes: List of out-of-sample Sharpe ratios
            - is_sharpe_mean / oos_sharpe_mean: Averages
            - oos_sharpe_std: Std of OOS Sharpes (strategy stability indicator)
            - sharpe_decay: Mean IS Sharpe minus mean OOS Sharpe (overfitting signal)
            - summary: walk_forward summary dict
    """
    # ── Build config from kwargs if not provided ──
    if config is None:
        config = AlphaConfig(
            portfolio_mode="long_short" if n_short > 0 else "long_only",
            factor_names=factor_names or ["bbiboll", "vol_ratio"],
            combination_method=combination_method,
            sizing_method=sizing_method,
            rebal_every_n=rebal_every_n,
            n_long=n_long,
            n_short=n_short,
            target_vol=target_vol,
            annualization=annualization,
        )

    # ── Get unique dates and generate folds ──
    unique_dates = ohlcv.select(date_col).unique().sort(date_col).to_series().to_numpy()
    n_dates = len(unique_dates)

    wf_folds = walk_forward_split(
        n_dates=n_dates,
        train_days=train_days,
        test_days=test_days,
        embargo_days=embargo_days,
        mode=mode,
    )

    date_folds = apply_folds_to_dates(unique_dates, wf_folds)
    summary = summarize_folds(wf_folds)

    if verbose:
        print(
            f"Walk-forward: {summary['n_folds']} folds, "
            f"mode={summary['mode']}, embargo={embargo_days}d"
        )

    # ── Execute pipeline per fold (using config) ──
    fold_results: list[dict] = []
    is_sharpes: list[float] = []
    oos_sharpes: list[float] = []

    for fold_meta in date_folds:
        fid = fold_meta["fold_id"]

        # Slice OHLCV for IS and OOS
        ohlcv_is = _slice_ohlcv(
            ohlcv,
            fold_meta["train_start_date"],
            fold_meta["train_end_date"],
            date_col=date_col,
        )
        ohlcv_oos = _slice_ohlcv(
            ohlcv,
            fold_meta["test_start_date"],
            fold_meta["test_end_date"],
            date_col=date_col,
        )

        # Run pipeline on IS / OOS separately
        try:
            is_result = run_alpha_pipeline(ohlcv_is, config=config)
        except Exception as e:
            if verbose:
                print(f"  Fold {fid} IS failed: {e}")
            is_result = None

        try:
            oos_result = run_alpha_pipeline(ohlcv_oos, config=config)
        except Exception as e:
            if verbose:
                print(f"  Fold {fid} OOS failed: {e}")
            oos_result = None

        is_sharpe = is_result["sharpe"] if is_result else float("nan")
        oos_sharpe = oos_result["sharpe"] if oos_result else float("nan")

        is_sharpes.append(is_sharpe)
        oos_sharpes.append(oos_sharpe)

        fold_record = {
            "fold_id": fid,
            "train_start": fold_meta["train_start_date"],
            "train_end": fold_meta["train_end_date"],
            "test_start": fold_meta["test_start_date"],
            "test_end": fold_meta["test_end_date"],
            "is_sharpe": is_sharpe,
            "is_annual_return": (
                is_result["annual_return"] if is_result else float("nan")
            ),
            "is_annual_vol": is_result["annual_vol"] if is_result else float("nan"),
            "is_n_days": is_result["n_days"] if is_result else 0,
            "oos_sharpe": oos_sharpe,
            "oos_annual_return": (
                oos_result["annual_return"] if oos_result else float("nan")
            ),
            "oos_annual_vol": oos_result["annual_vol"] if oos_result else float("nan"),
            "oos_n_days": oos_result["n_days"] if oos_result else 0,
        }
        fold_results.append(fold_record)

        if verbose:
            print(
                f"  Fold {fid}: IS Sharpe={is_sharpe:+.3f}  "
                f"OOS Sharpe={oos_sharpe:+.3f}"
            )

    # ── Aggregate ──
    is_arr = np.array(is_sharpes)
    oos_arr = np.array(oos_sharpes)
    valid_is = is_arr[~np.isnan(is_arr)]
    valid_oos = oos_arr[~np.isnan(oos_arr)]

    is_sharpe_mean = float(np.mean(valid_is)) if len(valid_is) > 0 else float("nan")
    oos_sharpe_mean = float(np.mean(valid_oos)) if len(valid_oos) > 0 else float("nan")
    oos_sharpe_std = (
        float(np.std(valid_oos, ddof=1)) if len(valid_oos) > 1 else float("nan")
    )

    sharpe_decay = is_sharpe_mean - oos_sharpe_mean

    if verbose:
        print(
            f"\n  Mean IS Sharpe:  {is_sharpe_mean:+.3f}\n"
            f"  Mean OOS Sharpe: {oos_sharpe_mean:+.3f}\n"
            f"  OOS Sharpe Std:  {oos_sharpe_std:.3f}\n"
            f"  Sharpe Decay:    {sharpe_decay:+.3f}"
        )

    return {
        "folds": date_folds,
        "fold_results": fold_results,
        "is_sharpes": is_sharpes,
        "oos_sharpes": oos_sharpes,
        "is_sharpe_mean": is_sharpe_mean,
        "oos_sharpe_mean": oos_sharpe_mean,
        "oos_sharpe_std": oos_sharpe_std,
        "sharpe_decay": sharpe_decay,
        "summary": summary,
    }


def fold_results_to_dataframe(fold_results: list[dict]) -> pl.DataFrame:
    """Convert fold results to a Polars DataFrame for easy analysis.

    Args:
        fold_results: The ``fold_results`` key from ``run_walk_forward()`` output.

    Returns:
        DataFrame with one row per fold and IS/OOS metrics as columns.
    """
    return pl.DataFrame(fold_results)

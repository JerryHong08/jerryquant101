"""
Walk-Forward Validation — Rolling train/test splitter with purged embargo.

Implements the gold-standard methodology for evaluating trading strategies:
split historical data into sequential train/test windows, train on each
window, test on the next, and aggregate out-of-sample performance.

Key design choices:
    - **Purged embargo**: A gap between train and test prevents information
      leakage from overlapping forward returns.
    - **Anchored vs rolling**: Anchored expands the training window from a
      fixed start; rolling uses a fixed-width sliding window.
    - **No re-fitting inside**: This module only *splits* dates. The caller
      is responsible for fitting/predicting on each fold.

Reference:
    - de Prado, "Advances in Financial Machine Learning" (2018), Ch. 7–12
    - guidance/quant_lab.pdf — Part V, Chapter 16 (Validation)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np


@dataclass(frozen=True)
class WalkForwardFold:
    """One fold of walk-forward validation.

    Attributes:
        fold_id: Sequential fold number (0-indexed).
        train_start_idx: First index of the training window (inclusive).
        train_end_idx: Last index of the training window (inclusive).
        test_start_idx: First index of the test window (inclusive).
        test_end_idx: Last index of the test window (inclusive).
        embargo_days: Number of trading days purged between train and test.
    """

    fold_id: int
    train_start_idx: int
    train_end_idx: int
    test_start_idx: int
    test_end_idx: int
    embargo_days: int

    @property
    def train_size(self) -> int:
        """Number of trading days in the training window."""
        return self.train_end_idx - self.train_start_idx + 1

    @property
    def test_size(self) -> int:
        """Number of trading days in the test window."""
        return self.test_end_idx - self.test_start_idx + 1

    def __repr__(self) -> str:
        return (
            f"Fold({self.fold_id}: "
            f"train[{self.train_start_idx}:{self.train_end_idx}]={self.train_size}d, "
            f"embargo={self.embargo_days}d, "
            f"test[{self.test_start_idx}:{self.test_end_idx}]={self.test_size}d)"
        )


def walk_forward_split(
    n_dates: int,
    train_days: int,
    test_days: int,
    embargo_days: int = 5,
    mode: Literal["rolling", "anchored"] = "rolling",
    min_train_days: int | None = None,
) -> list[WalkForwardFold]:
    """Generate walk-forward train/test folds.

    Args:
        n_dates: Total number of trading days in the dataset.
        train_days: Number of trading days in each training window.
        test_days: Number of trading days in each test window.
        embargo_days: Number of trading days to skip between train end
            and test start (purging gap to prevent leakage).
        mode: "rolling" = fixed-width training window slides forward;
              "anchored" = training window starts at index 0, expands.
        min_train_days: Minimum training days for anchored mode (ignored
            in rolling mode). Defaults to ``train_days``.

    Returns:
        List of WalkForwardFold, ordered chronologically.

    Raises:
        ValueError: If parameters produce zero valid folds.

    Example::

        folds = walk_forward_split(
            n_dates=504,       # ~2 years
            train_days=126,    # 6 months
            test_days=63,      # 3 months
            embargo_days=5,    # 1 week
            mode="rolling",
        )
        # → 4 folds: [0-125] → [131-193], [63-188] → [194-256], ...
    """
    if n_dates <= 0:
        raise ValueError(f"n_dates must be positive, got {n_dates}")
    if train_days <= 0:
        raise ValueError(f"train_days must be positive, got {train_days}")
    if test_days <= 0:
        raise ValueError(f"test_days must be positive, got {test_days}")
    if embargo_days < 0:
        raise ValueError(f"embargo_days must be non-negative, got {embargo_days}")

    if min_train_days is None:
        min_train_days = train_days

    folds: list[WalkForwardFold] = []

    if mode == "rolling":
        step = test_days  # non-overlapping test windows
        fold_id = 0
        train_start = 0

        while True:
            train_end = train_start + train_days - 1
            test_start = train_end + embargo_days + 1
            test_end = test_start + test_days - 1

            if test_end >= n_dates:
                # Try a partial last fold if at least half the test window fits
                test_end = n_dates - 1
                if test_end < test_start:
                    break
                partial_test = test_end - test_start + 1
                if partial_test < test_days // 2:
                    break  # Too small, skip

            folds.append(
                WalkForwardFold(
                    fold_id=fold_id,
                    train_start_idx=train_start,
                    train_end_idx=train_end,
                    test_start_idx=test_start,
                    test_end_idx=test_end,
                    embargo_days=embargo_days,
                )
            )

            fold_id += 1
            train_start += step

    elif mode == "anchored":
        train_start = 0
        fold_id = 0

        # First fold: train on min_train_days
        current_train_end = min_train_days - 1

        while True:
            test_start = current_train_end + embargo_days + 1
            test_end = test_start + test_days - 1

            if test_end >= n_dates:
                test_end = n_dates - 1
                if test_end < test_start:
                    break
                partial_test = test_end - test_start + 1
                if partial_test < test_days // 2:
                    break

            folds.append(
                WalkForwardFold(
                    fold_id=fold_id,
                    train_start_idx=train_start,
                    train_end_idx=current_train_end,
                    test_start_idx=test_start,
                    test_end_idx=test_end,
                    embargo_days=embargo_days,
                )
            )

            fold_id += 1
            current_train_end += test_days  # expand training to include old test

    else:
        raise ValueError(f"mode must be 'rolling' or 'anchored', got '{mode}'")

    if not folds:
        raise ValueError(
            f"No valid folds: n_dates={n_dates}, train={train_days}, "
            f"test={test_days}, embargo={embargo_days}, mode={mode}"
        )

    return folds


def apply_folds_to_dates(
    dates: np.ndarray,
    folds: list[WalkForwardFold],
) -> list[dict]:
    """Map index-based folds to actual date values.

    Args:
        dates: Sorted array of date values (datetime64, date objects, etc.).
        folds: Output of ``walk_forward_split()``.

    Returns:
        List of dicts, each with keys:
            fold_id, train_dates, test_dates,
            train_start_date, train_end_date,
            test_start_date, test_end_date.
    """
    result = []
    for fold in folds:
        result.append(
            {
                "fold_id": fold.fold_id,
                "train_dates": dates[fold.train_start_idx : fold.train_end_idx + 1],
                "test_dates": dates[fold.test_start_idx : fold.test_end_idx + 1],
                "train_start_date": dates[fold.train_start_idx],
                "train_end_date": dates[fold.train_end_idx],
                "test_start_date": dates[fold.test_start_idx],
                "test_end_date": dates[fold.test_end_idx],
            }
        )
    return result


def summarize_folds(folds: list[WalkForwardFold]) -> dict:
    """Summary statistics for a set of walk-forward folds.

    Returns:
        Dict with n_folds, total_train_days, total_test_days,
        avg_train_size, avg_test_size, embargo_days.
    """
    n = len(folds)
    train_sizes = [f.train_size for f in folds]
    test_sizes = [f.test_size for f in folds]

    return {
        "n_folds": n,
        "total_train_days": sum(train_sizes),
        "total_test_days": sum(test_sizes),
        "avg_train_size": np.mean(train_sizes),
        "avg_test_size": np.mean(test_sizes),
        "embargo_days": folds[0].embargo_days if n > 0 else 0,
        "mode": (
            "anchored"
            if n > 1 and folds[0].train_start_idx == folds[1].train_start_idx
            else "rolling"
        ),
    }

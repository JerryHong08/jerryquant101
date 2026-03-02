"""
Multiple Testing Corrections — Controlling false discovery in strategy sweeps.

When testing N strategy configurations (e.g., 4 sizing methods × 4 rebalancing
frequencies = 16 tests), some will appear significant by chance. This module
provides standard corrections:

    - **Bonferroni**: Controls Family-Wise Error Rate (FWER). Conservative.
      Rejects if p < α/N.
    - **Holm-Bonferroni**: Step-down procedure. Less conservative than
      Bonferroni, still controls FWER.
    - **Benjamini-Hochberg (BH)**: Controls False Discovery Rate (FDR).
      More powerful than FWER methods when many tests are run.

Reference:
    - Benjamini & Hochberg (1995), "Controlling the False Discovery Rate"
    - Harvey, Liu & Zhu (2016), "... and the Cross-Section of Expected Returns"
    - docs/quant_lab.tex — Part V, Chapter 16 (Validation)
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class CorrectedResult:
    """Result for a single test after multiple-testing correction.

    Attributes:
        label: Strategy/configuration name.
        raw_p_value: Original p-value before correction.
        corrected_p_value: Adjusted p-value after correction.
        significant: Whether the test remains significant after correction.
        rank: Rank of raw p-value (1 = smallest).
    """

    label: str
    raw_p_value: float
    corrected_p_value: float
    significant: bool
    rank: int


@dataclass(frozen=True)
class MultipleTestResult:
    """Aggregate result of multiple-testing correction.

    Attributes:
        method: Name of the correction method used.
        alpha: Significance level.
        n_tests: Total number of tests.
        n_significant: Number of tests that remain significant.
        results: Per-test results, sorted by raw p-value.
    """

    method: str
    alpha: float
    n_tests: int
    n_significant: int
    results: list[CorrectedResult]

    def __repr__(self) -> str:
        return (
            f"MultipleTestResult(method='{self.method}', "
            f"n_tests={self.n_tests}, "
            f"n_significant={self.n_significant}/{self.n_tests}, "
            f"α={self.alpha})"
        )

    def summary_table(self) -> list[dict]:
        """Return results as a list of dicts for easy display."""
        return [
            {
                "label": r.label,
                "raw_p": r.raw_p_value,
                "corrected_p": r.corrected_p_value,
                "significant": r.significant,
                "rank": r.rank,
            }
            for r in self.results
        ]


def bonferroni(
    p_values: dict[str, float],
    alpha: float = 0.05,
) -> MultipleTestResult:
    """Bonferroni correction — controls FWER.

    Adjusted p-value = min(p × N, 1.0).

    Most conservative: good for small N, bad for large N because
    it vastly over-corrects, reducing power.

    Args:
        p_values: Dict mapping strategy label → raw p-value.
        alpha: Significance level.

    Returns:
        MultipleTestResult with corrected p-values.
    """
    n = len(p_values)
    if n == 0:
        raise ValueError("p_values must not be empty")

    # Sort by p-value
    sorted_items = sorted(p_values.items(), key=lambda x: x[1])

    results = []
    for rank, (label, p) in enumerate(sorted_items, 1):
        corrected = min(p * n, 1.0)
        results.append(
            CorrectedResult(
                label=label,
                raw_p_value=p,
                corrected_p_value=corrected,
                significant=corrected < alpha,
                rank=rank,
            )
        )

    n_sig = sum(1 for r in results if r.significant)
    return MultipleTestResult(
        method="Bonferroni",
        alpha=alpha,
        n_tests=n,
        n_significant=n_sig,
        results=results,
    )


def holm_bonferroni(
    p_values: dict[str, float],
    alpha: float = 0.05,
) -> MultipleTestResult:
    """Holm-Bonferroni step-down correction — controls FWER.

    Ordered p-values: p(1) ≤ p(2) ≤ ... ≤ p(N).
    Reject p(k) if p(k) < α / (N - k + 1) for all k ≤ j.
    Stop at the first non-rejection.

    Less conservative than Bonferroni, uniformly more powerful.

    Args:
        p_values: Dict mapping strategy label → raw p-value.
        alpha: Significance level.

    Returns:
        MultipleTestResult with corrected p-values.
    """
    n = len(p_values)
    if n == 0:
        raise ValueError("p_values must not be empty")

    sorted_items = sorted(p_values.items(), key=lambda x: x[1])

    # Compute corrected p-values (step-down)
    corrected_ps = []
    for rank, (label, p) in enumerate(sorted_items):
        adjusted = p * (n - rank)
        corrected_ps.append(adjusted)

    # Enforce monotonicity: corrected p-values must be non-decreasing
    for i in range(1, len(corrected_ps)):
        corrected_ps[i] = max(corrected_ps[i], corrected_ps[i - 1])

    # Cap at 1.0
    corrected_ps = [min(p, 1.0) for p in corrected_ps]

    results = []
    for rank, ((label, raw_p), corr_p) in enumerate(zip(sorted_items, corrected_ps), 1):
        results.append(
            CorrectedResult(
                label=label,
                raw_p_value=raw_p,
                corrected_p_value=corr_p,
                significant=corr_p < alpha,
                rank=rank,
            )
        )

    n_sig = sum(1 for r in results if r.significant)
    return MultipleTestResult(
        method="Holm-Bonferroni",
        alpha=alpha,
        n_tests=n,
        n_significant=n_sig,
        results=results,
    )


def benjamini_hochberg(
    p_values: dict[str, float],
    alpha: float = 0.05,
) -> MultipleTestResult:
    """Benjamini-Hochberg (BH) correction — controls FDR.

    Ordered p-values: p(1) ≤ p(2) ≤ ... ≤ p(N).
    Reject p(k) if p(k) ≤ α × k / N for the largest such k
    (and all p(j) for j ≤ k).

    Less conservative than FWER methods. Appropriate when:
    - Many tests (N > 10)
    - Some false discoveries are tolerable
    - Goal is to find *any* real signal, not guarantee all are real

    Args:
        p_values: Dict mapping strategy label → raw p-value.
        alpha: Significance level (FDR control level).

    Returns:
        MultipleTestResult with corrected p-values.
    """
    n = len(p_values)
    if n == 0:
        raise ValueError("p_values must not be empty")

    sorted_items = sorted(p_values.items(), key=lambda x: x[1])

    # Compute corrected p-values (step-up)
    corrected_ps = []
    for rank, (label, p) in enumerate(sorted_items, 1):
        adjusted = p * n / rank
        corrected_ps.append(adjusted)

    # Enforce monotonicity: corrected p-values must be non-increasing
    # (process from right to left)
    for i in range(len(corrected_ps) - 2, -1, -1):
        corrected_ps[i] = min(corrected_ps[i], corrected_ps[i + 1])

    # Cap at 1.0
    corrected_ps = [min(p, 1.0) for p in corrected_ps]

    results = []
    for rank, ((label, raw_p), corr_p) in enumerate(zip(sorted_items, corrected_ps), 1):
        results.append(
            CorrectedResult(
                label=label,
                raw_p_value=raw_p,
                corrected_p_value=corr_p,
                significant=corr_p < alpha,
                rank=rank,
            )
        )

    n_sig = sum(1 for r in results if r.significant)
    return MultipleTestResult(
        method="Benjamini-Hochberg",
        alpha=alpha,
        n_tests=n,
        n_significant=n_sig,
        results=results,
    )


def apply_all_corrections(
    p_values: dict[str, float],
    alpha: float = 0.05,
) -> dict[str, MultipleTestResult]:
    """Apply all three correction methods and return comparison.

    Args:
        p_values: Dict mapping strategy label → raw p-value.
        alpha: Significance level.

    Returns:
        Dict with keys 'bonferroni', 'holm', 'bh' mapping to results.
    """
    return {
        "bonferroni": bonferroni(p_values, alpha),
        "holm": holm_bonferroni(p_values, alpha),
        "bh": benjamini_hochberg(p_values, alpha),
    }

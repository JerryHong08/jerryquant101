"""
Validation Module — Walk-forward testing, statistical significance, and
multiple-testing corrections for strategy evaluation.

Sub-modules:
    walk_forward: Rolling train/test splitter with purged embargo
    statistical_tests: Bootstrap Sharpe CI, PSR, DSR, p-values
    multiple_testing: Bonferroni, Holm, Benjamini-Hochberg corrections

Convention:
    - Returns: 1-D numpy arrays of daily portfolio returns
    - Folds: index-based (WalkForwardFold) or date-mapped (apply_folds_to_dates)
    - p-values: dict[label, float] for multiple-testing input
    - All Sharpe ratios are annualized (×√252 by default)

Reference: guidance/quant_lab.pdf — Part V, Chapter 16 (Validation)
"""

from validation.multiple_testing import (
    MultipleTestResult,
    apply_all_corrections,
    benjamini_hochberg,
    bonferroni,
    holm_bonferroni,
)
from validation.statistical_tests import (
    BootstrapResult,
    bootstrap_sharpe_ci,
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
    sharpe_pvalue,
)
from validation.walk_forward import (
    WalkForwardFold,
    apply_folds_to_dates,
    summarize_folds,
    walk_forward_split,
)

__all__ = [
    # walk_forward
    "WalkForwardFold",
    "walk_forward_split",
    "apply_folds_to_dates",
    "summarize_folds",
    # statistical_tests
    "BootstrapResult",
    "bootstrap_sharpe_ci",
    "sharpe_pvalue",
    "probabilistic_sharpe_ratio",
    "deflated_sharpe_ratio",
    # multiple_testing
    "MultipleTestResult",
    "bonferroni",
    "holm_bonferroni",
    "benjamini_hochberg",
    "apply_all_corrections",
]

"""
Portfolio Module — signal-to-returns pipeline.

Bridges the gap between alpha research (factor signals) and performance
evaluation (returns, Sharpe, cost analysis). Eliminates the ~80-line
boilerplate duplicated across every research notebook.

Sub-modules:
    pipeline: End-to-end factor → weights → returns pipeline
    walk_forward_runner: Execute pipeline per walk-forward fold

Usage:
    from portfolio.pipeline import (
        compute_daily_returns,
        compute_portfolio_return,
        build_factor_pipeline,
        run_alpha_pipeline,
    )
"""

from portfolio.alpha_config import AlphaConfig, FactorConfig
from portfolio.factors import get_factor_fn, list_factors, register_factor
from portfolio.pipeline import (
    build_factor_pipeline,
    build_sizing_methods,
    compute_daily_returns,
    compute_next_day_returns,
    compute_portfolio_return,
    resample_weights,
    run_alpha_pipeline,
)
from portfolio.walk_forward_runner import (
    fold_results_to_dataframe,
    run_walk_forward,
)

__all__ = [
    "AlphaConfig",
    "FactorConfig",
    "build_factor_pipeline",
    "build_sizing_methods",
    "compute_daily_returns",
    "compute_next_day_returns",
    "compute_portfolio_return",
    "fold_results_to_dataframe",
    "get_factor_fn",
    "list_factors",
    "register_factor",
    "resample_weights",
    "run_alpha_pipeline",
    "run_walk_forward",
]

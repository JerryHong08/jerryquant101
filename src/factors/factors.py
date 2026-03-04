"""
Factor Functions & Registry — OHLCV → preprocessed factor DataFrames.

Decorated factor functions only need to compute a raw signal and return
a DataFrame containing at least ``(date_col, ticker, value)`` columns.
The ``@register_factor`` decorator automatically applies:

1. Null / NaN / Inf filtering on ``value``
2. Column selection → ``(date, ticker, value)``
3. ``preprocess_factor`` (winsorize + normalize + neutralize)

To add a new factor::

    @register_factor("my_factor")
    def compute_my_factor(ohlcv: pl.DataFrame, **kw) -> pl.DataFrame:
        return ohlcv.with_columns(... .alias("value"))

Set ``raw=False`` if the function handles post-processing itself::

    @register_factor("custom", raw=False)
    def custom(ohlcv, *, factor_config=None, **kw): ...

Usage:
    from factors import register_factor, list_factors, get_factor_fn

Reference: guidance/quant_lab.pdf — Part III
"""

from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Callable

import polars as pl

from alpha.preprocessing import preprocess_factor
from constants import DATE_COL, OHLCV_DATE_COL, TICKER_COL, VALUE_COL

if TYPE_CHECKING:
    from portfolio.alpha_config import FactorConfig

# ── Registry ──────────────────────────────────────────────────────────────────

_FACTOR_REGISTRY: dict[str, Callable[..., pl.DataFrame]] = {}


def _make_wrapper(
    fn: Callable[..., pl.DataFrame],
    ohlcv_date_col_default: str = OHLCV_DATE_COL,
) -> Callable[..., pl.DataFrame]:
    """Wrap a raw factor function with standard post-processing.

    The wrapper:
    1. Resolves ``FactorConfig`` (lazy import to avoid circular deps).
    2. Calls the raw factor function to get the signal DataFrame.
    3. Filters null / NaN / Inf values.
    4. Selects ``(date, ticker, value)`` columns.
    5. Applies ``preprocess_factor`` (winsorize, normalize, neutralize).
    """

    @wraps(fn)
    def wrapper(
        ohlcv: pl.DataFrame,
        *,
        ohlcv_date_col: str = ohlcv_date_col_default,
        factor_config: FactorConfig | None = None,
        **kwargs,
    ) -> pl.DataFrame:
        from portfolio.alpha_config import FactorConfig as _FC

        fc = factor_config or _FC()

        # Let the raw function compute the signal
        raw = fn(ohlcv, **kwargs)

        # Standard post-processing
        clean = raw.filter(
            pl.col(VALUE_COL).is_not_null()
            & pl.col(VALUE_COL).is_not_nan()
            & pl.col(VALUE_COL).is_finite()
        ).select(
            pl.col(ohlcv_date_col).alias(DATE_COL),
            pl.col(TICKER_COL),
            pl.col(VALUE_COL),
        )
        return preprocess_factor(
            clean,
            winsorize_pct=fc.winsorize_pct,
            method=fc.normalize_method,
            neutralize=fc.neutralize,
        )

    return wrapper


def register_factor(
    name: str,
    fn: Callable[..., pl.DataFrame] | None = None,
    *,
    raw: bool = True,
):
    """Register a factor computation function.

    Can be used as a decorator::

        @register_factor("my_factor")
        def compute_my_factor(ohlcv, **kw): ...

    Or called directly (no wrapping — the function is stored as-is)::

        register_factor("my_factor", my_fn)

    Args:
        name: Factor name (lowercase, case-insensitive).
        fn: Optional callable.  If provided, registers directly
            **without** post-processing wrapping.
        raw: If ``True`` (default), the decorator wraps the function
            with standard post-processing (filter + select + preprocess).
            Set ``False`` to register a function that handles its own
            post-processing.
    """
    if fn is not None:
        # Direct call — register as-is (backward compat with tests)
        _FACTOR_REGISTRY[name.lower()] = fn
        return fn

    def decorator(fn: Callable[..., pl.DataFrame]):
        wrapped = _make_wrapper(fn) if raw else fn
        _FACTOR_REGISTRY[name.lower()] = wrapped
        return wrapped

    return decorator


def list_factors() -> list[str]:
    """Return names of all registered factors."""
    return sorted(_FACTOR_REGISTRY.keys())


def get_factor_fn(name: str) -> Callable[..., pl.DataFrame]:
    """Look up a factor function by name.

    Args:
        name: Registered factor name (case-insensitive).

    Returns:
        The factor computation function.

    Raises:
        KeyError: If the factor is not registered.
    """
    key = name.lower()
    if key not in _FACTOR_REGISTRY:
        available = ", ".join(sorted(_FACTOR_REGISTRY.keys()))
        raise KeyError(
            f"Unknown factor '{name}'. Available: {available}. "
            f"Use register_factor() to add custom factors."
        )
    return _FACTOR_REGISTRY[key]

"""
Factor Functions & Registry — OHLCV → preprocessed factor DataFrames.

Each factor function has the signature::

    def compute_xxx(
        ohlcv: pl.DataFrame,
        *,
        ohlcv_date_col: str = OHLCV_DATE_COL,
        factor_config: FactorConfig | None = None,
        **kwargs,
    ) -> pl.DataFrame:
        ...

and returns a preprocessed ``(date, ticker, value)`` DataFrame.

To add a new factor:
    1. Write a function following the signature above.
    2. Call ``register_factor("my_factor", my_fn)`` or add it to
       ``_FACTOR_REGISTRY`` directly.

Usage:
    from portfolio.factors import register_factor, list_factors

Reference: guidance/quant_lab.pdf — Part III
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import polars as pl

from alpha.preprocessing import preprocess_factor
from constants import DATE_COL, OHLCV_DATE_COL, TICKER_COL, VALUE_COL

if TYPE_CHECKING:
    from portfolio.alpha_config import FactorConfig

# ── Registry ──────────────────────────────────────────────────────────────────

# Factor registry: name → compute function
_FACTOR_REGISTRY: dict[str, Callable[..., pl.DataFrame]] = {
    # "bbiboll": _compute_bbiboll_factor,
    # "vol_ratio": _compute_vol_ratio_factor,
    # "momentum": _compute_momentum_factor,
}


def register_factor(name: str, fn: Callable[..., pl.DataFrame] | None = None):
    """Register a custom factor computation function.

    Can be used as a decorator::

        @register_factor("my_factor")
        def compute_my_factor(ohlcv, **kw): ...

    Or called directly::

        register_factor("my_factor", my_fn)

    Args:
        name: Factor name (lowercase).
        fn: Optional callable. If provided, registers directly.
            If omitted, returns a decorator.
    """
    if fn is not None:
        _FACTOR_REGISTRY[name.lower()] = fn
        return fn

    def decorator(fn):
        _FACTOR_REGISTRY[name.lower()] = fn
        return fn

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

"""
AlphaConfig — single configuration object for the alpha pipeline.

Replaces 10+ keyword arguments scattered across ``run_alpha_pipeline()``,
``build_factor_pipeline()``, ``build_sizing_methods()``, ``run_pipeline_backtest()``,
and ``run_walk_forward()`` with one structured, validated, YAML-loadable config.

Usage — code::

    config = AlphaConfig(
        factor_names=["bbiboll", "vol_ratio"],
        combination_method="ic_weight",
        sizing_method="Half-Kelly",
        n_long=20,
        rebal_every_n=5,
    )
    results = run_alpha_pipeline(ohlcv, config=config)

Usage — per-factor params::

    config = AlphaConfig(
        factor_names=["vol_ratio", "momentum"],
        factor_params={
            "vol_ratio": {"short_window": 10, "long_window": 40},
            "momentum": {"lookback": 60},
        },
    )

Usage — YAML file::

    config = AlphaConfig.from_yaml("configs/alpha.yaml")

Reference: docs/quant_lab.tex — Part III–IV
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from constants import TRADING_DAYS_PER_YEAR


@dataclass(frozen=True)
class FactorConfig:
    """Per-factor preprocessing and computation parameters.

    Attributes:
        winsorize_pct: Percentile for winsorization (0.01 = 1%).
        normalize_method: "zscore" or "rank".
        neutralize: List of neutralization targets (e.g. ["sector"]).
        params: Factor-specific computation kwargs (e.g. short_window, lookback).
    """

    winsorize_pct: float = 0.01
    normalize_method: Literal["zscore", "rank"] = "zscore"
    neutralize: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AlphaConfig:
    """Central configuration for the alpha pipeline.

    Groups all parameters that were previously spread across function
    signatures into a single, validated object.

    Sections:
        - **Factor selection**: which factors and how to combine them
        - **Per-factor params**: preprocessing + computation per factor
        - **Portfolio construction**: sizing, position counts
        - **Rebalancing**: frequency
        - **Cost model**: transaction costs
        - **Annualization**: trading days per year
    """

    # ── Factor selection ──────────────────────────────────────────────
    factor_names: List[str] = field(default_factory=lambda: ["bbiboll", "vol_ratio"])
    combination_method: Literal[
        "equal_weight", "ic_weight", "mean_variance", "risk_parity"
    ] = "equal_weight"

    # ── Per-factor params ─────────────────────────────────────────────
    # key = factor name, value = FactorConfig
    # Factors not listed here use FactorConfig defaults
    factor_configs: Dict[str, FactorConfig] = field(default_factory=dict)

    # ── Portfolio construction ────────────────────────────────────────
    sizing_method: str = "Half-Kelly"
    n_long: int = 10
    n_short: int = 10
    target_vol: float = 0.10
    kelly_lookback: int = 60
    kelly_max_position: float = 0.10
    vol_window: int = 20

    # ── Rebalancing ───────────────────────────────────────────────────
    rebal_every_n: int = 5

    # ── Cost model ────────────────────────────────────────────────────
    cost_bps: float = 5.0

    # ── Annualization ─────────────────────────────────────────────────
    annualization: int = TRADING_DAYS_PER_YEAR

    # ── Combination risk aversion (for mean-variance method) ──────────
    risk_aversion: float = 1.0

    # ── Metadata ──────────────────────────────────────────────────────
    name: str = "AlphaPipeline"

    def get_factor_config(self, factor_name: str) -> FactorConfig:
        """Get per-factor config, falling back to defaults."""
        return self.factor_configs.get(factor_name, FactorConfig())

    def get_factor_params(self, factor_name: str) -> Dict[str, Any]:
        """Get computation kwargs for a specific factor."""
        return self.get_factor_config(factor_name).params

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AlphaConfig:
        """Create from a plain dict (e.g. parsed YAML).

        Handles nested ``factor_configs`` by converting inner dicts
        to ``FactorConfig`` instances.
        """
        d = dict(d)  # shallow copy
        raw_fc = d.pop("factor_configs", {})
        factor_configs = {}
        for name, fc_dict in raw_fc.items():
            if isinstance(fc_dict, dict):
                factor_configs[name] = FactorConfig(**fc_dict)
            elif isinstance(fc_dict, FactorConfig):
                factor_configs[name] = fc_dict
        d["factor_configs"] = factor_configs
        return cls(**d)

    @classmethod
    def from_yaml(cls, path: str | Path) -> AlphaConfig:
        """Load config from a YAML file.

        Requires ``pyyaml`` (already in project dependencies).
        """
        import yaml

        with open(path) as f:
            raw = yaml.safe_load(f)
        return cls.from_dict(raw)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dict (suitable for YAML dump or logging)."""
        d: Dict[str, Any] = {
            "factor_names": list(self.factor_names),
            "combination_method": self.combination_method,
            "factor_configs": {
                name: {
                    "winsorize_pct": fc.winsorize_pct,
                    "normalize_method": fc.normalize_method,
                    "neutralize": list(fc.neutralize),
                    "params": dict(fc.params),
                }
                for name, fc in self.factor_configs.items()
            },
            "sizing_method": self.sizing_method,
            "n_long": self.n_long,
            "n_short": self.n_short,
            "target_vol": self.target_vol,
            "kelly_lookback": self.kelly_lookback,
            "kelly_max_position": self.kelly_max_position,
            "vol_window": self.vol_window,
            "rebal_every_n": self.rebal_every_n,
            "cost_bps": self.cost_bps,
            "annualization": self.annualization,
            "risk_aversion": self.risk_aversion,
            "name": self.name,
        }
        return d

    def to_yaml(self, path: str | Path) -> None:
        """Save config to a YAML file."""
        import yaml

        with open(path, "w") as f:
            yaml.safe_dump(self.to_dict(), f, default_flow_style=False, sort_keys=False)

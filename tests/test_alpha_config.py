"""
Tests for AlphaConfig, FactorConfig, and portfolio.factors module.

Covers:
    - AlphaConfig defaults and field access
    - FactorConfig per-factor params and preprocessing
    - AlphaConfig.from_dict / to_dict round-trip
    - AlphaConfig.from_yaml / to_yaml round-trip
    - get_factor_config fallback behavior
    - Factor registry: list, register, get_factor_fn, unknown key
    - Factor functions accept FactorConfig
"""

from __future__ import annotations

import datetime as dt
import tempfile
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from portfolio.alpha_config import AlphaConfig, FactorConfig

# ── FactorConfig ──────────────────────────────────────────────────────────────


class TestFactorConfig:
    def test_defaults(self):
        fc = FactorConfig()
        assert fc.winsorize_pct == 0.01
        assert fc.normalize_method == "zscore"
        assert fc.neutralize == []
        assert fc.params == {}

    def test_custom_params(self):
        fc = FactorConfig(
            winsorize_pct=0.05,
            normalize_method="rank",
            neutralize=["sector"],
            params={"short_window": 10, "long_window": 40},
        )
        assert fc.winsorize_pct == 0.05
        assert fc.normalize_method == "rank"
        assert fc.params["short_window"] == 10

    def test_direction_default(self):
        fc = FactorConfig()
        assert fc.direction == 1

    def test_direction_negative(self):
        fc = FactorConfig(direction=-1)
        assert fc.direction == -1

    def test_frozen(self):
        fc = FactorConfig()
        with pytest.raises(AttributeError):
            fc.winsorize_pct = 0.05  # type: ignore[misc]


# ── AlphaConfig ───────────────────────────────────────────────────────────────


class TestAlphaConfig:
    def test_defaults(self):
        cfg = AlphaConfig()
        assert cfg.factor_names == ["bbiboll", "vol_ratio"]
        assert cfg.combination_method == "equal_weight"
        assert cfg.sizing_method == "Half-Kelly"
        assert cfg.n_long == 10
        assert cfg.n_short == 10
        assert cfg.target_vol == 0.10
        assert cfg.kelly_lookback == 60
        assert cfg.kelly_max_position == 0.10
        assert cfg.vol_window == 20
        assert cfg.rebal_every_n == 5
        assert cfg.cost_bps == 5.0
        assert cfg.risk_aversion == 1.0
        assert cfg.name == "AlphaPipeline"

    def test_custom_fields(self):
        cfg = AlphaConfig(
            factor_names=["momentum"],
            combination_method="ic_weight",
            sizing_method="Equal-Weight",
            n_long=20,
            n_short=0,
            rebal_every_n=10,
            name="MomentumOnly",
        )
        assert cfg.factor_names == ["momentum"]
        assert cfg.combination_method == "ic_weight"
        assert cfg.n_long == 20
        assert cfg.n_short == 0
        assert cfg.name == "MomentumOnly"

    def test_get_factor_config_registered(self):
        fc = FactorConfig(params={"lookback": 60})
        cfg = AlphaConfig(factor_configs={"momentum": fc})
        assert cfg.get_factor_config("momentum") is fc
        assert cfg.get_factor_config("momentum").params["lookback"] == 60

    def test_get_factor_config_fallback(self):
        cfg = AlphaConfig()
        fc = cfg.get_factor_config("nonexistent")
        assert isinstance(fc, FactorConfig)
        assert fc.winsorize_pct == 0.01  # default

    def test_get_factor_params(self):
        fc = FactorConfig(params={"short_window": 10})
        cfg = AlphaConfig(factor_configs={"vol_ratio": fc})
        assert cfg.get_factor_params("vol_ratio") == {"short_window": 10}
        assert cfg.get_factor_params("bbiboll") == {}  # fallback

    def test_to_dict(self):
        cfg = AlphaConfig(
            factor_names=["momentum"],
            factor_configs={"momentum": FactorConfig(params={"lookback": 60})},
        )
        d = cfg.to_dict()
        assert d["factor_names"] == ["momentum"]
        assert d["factor_configs"]["momentum"]["params"] == {"lookback": 60}
        assert d["sizing_method"] == "Half-Kelly"

    def test_from_dict(self):
        d = {
            "factor_names": ["vol_ratio"],
            "n_long": 30,
            "factor_configs": {
                "vol_ratio": {
                    "winsorize_pct": 0.05,
                    "normalize_method": "rank",
                    "neutralize": [],
                    "params": {"short_window": 10},
                }
            },
        }
        cfg = AlphaConfig.from_dict(d)
        assert cfg.factor_names == ["vol_ratio"]
        assert cfg.n_long == 30
        fc = cfg.get_factor_config("vol_ratio")
        assert fc.winsorize_pct == 0.05
        assert fc.normalize_method == "rank"
        assert fc.params["short_window"] == 10

    def test_roundtrip_dict(self):
        original = AlphaConfig(
            factor_names=["bbiboll", "momentum"],
            combination_method="ic_weight",
            sizing_method="Inverse-Vol",
            n_long=15,
            rebal_every_n=10,
            factor_configs={
                "momentum": FactorConfig(
                    winsorize_pct=0.02,
                    normalize_method="rank",
                    params={"lookback": 120},
                )
            },
        )
        d = original.to_dict()
        restored = AlphaConfig.from_dict(d)
        assert restored.factor_names == original.factor_names
        assert restored.combination_method == original.combination_method
        assert restored.n_long == original.n_long
        assert restored.rebal_every_n == original.rebal_every_n
        rfc = restored.get_factor_config("momentum")
        assert rfc.winsorize_pct == 0.02
        assert rfc.normalize_method == "rank"
        assert rfc.params["lookback"] == 120

    def test_yaml_roundtrip(self, tmp_path):
        original = AlphaConfig(
            factor_names=["bbiboll", "vol_ratio"],
            n_long=25,
            factor_configs={
                "vol_ratio": FactorConfig(params={"short_window": 10}),
            },
        )
        yaml_path = tmp_path / "test_config.yaml"
        original.to_yaml(yaml_path)
        assert yaml_path.exists()

        restored = AlphaConfig.from_yaml(yaml_path)
        assert restored.factor_names == ["bbiboll", "vol_ratio"]
        assert restored.n_long == 25
        assert restored.get_factor_config("vol_ratio").params["short_window"] == 10

    def test_independent_default_lists(self):
        """Ensure default mutable fields aren't shared between instances."""
        cfg1 = AlphaConfig()
        cfg2 = AlphaConfig()
        cfg1.factor_names.append("momentum")
        assert "momentum" not in cfg2.factor_names

    def test_direction_roundtrip_dict(self):
        """direction field survives to_dict → from_dict."""
        original = AlphaConfig(
            factor_names=["bbiboll"],
            factor_configs={
                "bbiboll": FactorConfig(direction=-1),
            },
        )
        d = original.to_dict()
        assert d["factor_configs"]["bbiboll"]["direction"] == -1
        restored = AlphaConfig.from_dict(d)
        assert restored.get_factor_config("bbiboll").direction == -1

    def test_direction_yaml_roundtrip(self, tmp_path):
        """direction field survives YAML save/load."""
        original = AlphaConfig(
            factor_names=["bbiboll"],
            factor_configs={
                "bbiboll": FactorConfig(direction=-1, params={"extra": 42}),
            },
        )
        yaml_path = tmp_path / "dir_test.yaml"
        original.to_yaml(yaml_path)
        restored = AlphaConfig.from_yaml(yaml_path)
        fc = restored.get_factor_config("bbiboll")
        assert fc.direction == -1
        assert fc.params["extra"] == 42


# ── Factor Registry (from portfolio.factors) ─────────────────────────────────


class TestFactorsModule:
    def test_list_factors_has_builtins(self):
        from portfolio.factors import list_factors

        factors = list_factors()
        assert "bbiboll" in factors
        assert "vol_ratio" in factors
        assert "momentum" in factors

    def test_get_factor_fn_returns_callable(self):
        from portfolio.factors import get_factor_fn

        fn = get_factor_fn("vol_ratio")
        assert callable(fn)

    def test_get_factor_fn_case_insensitive(self):
        from portfolio.factors import get_factor_fn

        fn1 = get_factor_fn("VOL_RATIO")
        fn2 = get_factor_fn("vol_ratio")
        assert fn1 is fn2

    def test_get_factor_fn_unknown_raises(self):
        from portfolio.factors import get_factor_fn

        with pytest.raises(KeyError, match="Unknown factor"):
            get_factor_fn("nonexistent_xyz")

    def test_register_factor(self):
        from portfolio.factors import get_factor_fn, list_factors, register_factor

        def dummy(ohlcv, **kw):
            return pl.DataFrame({"date": [], "ticker": [], "value": []})

        register_factor("_test_dummy_7x", dummy)
        assert "_test_dummy_7x" in list_factors()
        assert get_factor_fn("_test_dummy_7x") is dummy

    def test_register_factor_case_normalized(self):
        from portfolio.factors import get_factor_fn, register_factor

        def dummy2(ohlcv, **kw):
            return pl.DataFrame({"date": [], "ticker": [], "value": []})

        register_factor("_Test_CaSe_7x", dummy2)
        assert get_factor_fn("_test_case_7x") is dummy2

    def test_pipeline_import_reexports(self):
        """list_factors and register_factor are importable from pipeline too."""
        from portfolio.pipeline import list_factors, register_factor

        assert callable(list_factors)
        assert callable(register_factor)

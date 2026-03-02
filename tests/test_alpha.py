"""
Tests for src/alpha/ — preprocessing (winsorize, normalize, neutralize) and combination.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from alpha.combination import combine_factors
from alpha.preprocessing import (
    preprocess_factor,
    rank_normalize,
    sector_neutralize,
    winsorize,
    zscore_normalize,
)

# ══════════════════════════════════════════════════════════════════════════════
#  Winsorize
# ══════════════════════════════════════════════════════════════════════════════


class TestWinsorize:
    """Tests for winsorize()."""

    def test_outliers_capped(self, factor_df_with_outliers):
        """After winsorization, extreme values should be reduced."""
        original_std = factor_df_with_outliers["value"].std()
        result = winsorize(factor_df_with_outliers, pct=0.05)
        result_std = result["value"].std()
        # Winsorizing outliers should reduce the spread
        assert result_std <= original_std

    def test_shape_preserved(self, factor_df):
        result = winsorize(factor_df, pct=0.01)
        assert result.shape == factor_df.shape

    def test_schema_preserved(self, factor_df):
        result = winsorize(factor_df, pct=0.01)
        assert result.columns == factor_df.columns


# ══════════════════════════════════════════════════════════════════════════════
#  Z-Score Normalize
# ══════════════════════════════════════════════════════════════════════════════


class TestZscoreNormalize:
    """Tests for zscore_normalize()."""

    def test_mean_near_zero(self, factor_df):
        """Cross-sectional mean should be ~0 for each date."""
        result = zscore_normalize(factor_df)
        means = result.group_by("date").agg(pl.col("value").mean().alias("mean"))
        max_abs_mean = means["mean"].abs().max()
        assert max_abs_mean < 1e-10

    def test_std_near_one(self, factor_df):
        """Cross-sectional std should be ~1 for each date."""
        result = zscore_normalize(factor_df)
        stds = result.group_by("date").agg(pl.col("value").std().alias("std"))
        # Filter out dates where std might be weird (e.g. 1 ticker)
        valid_stds = stds.filter(pl.col("std").is_not_null())
        mean_std = valid_stds["std"].mean()
        np.testing.assert_allclose(mean_std, 1.0, atol=0.05)

    def test_shape_preserved(self, factor_df):
        result = zscore_normalize(factor_df)
        assert result.shape == factor_df.shape


# ══════════════════════════════════════════════════════════════════════════════
#  Rank Normalize
# ══════════════════════════════════════════════════════════════════════════════


class TestRankNormalize:
    """Tests for rank_normalize()."""

    def test_values_in_range(self, factor_df):
        """Rank-normalized values should be in [-1, 1]."""
        result = rank_normalize(factor_df)
        assert result["value"].min() >= -1.0 - 1e-10
        assert result["value"].max() <= 1.0 + 1e-10

    def test_shape_preserved(self, factor_df):
        result = rank_normalize(factor_df)
        assert result.shape == factor_df.shape


# ══════════════════════════════════════════════════════════════════════════════
#  Sector Neutralize
# ══════════════════════════════════════════════════════════════════════════════


class TestSectorNeutralize:
    """Tests for sector_neutralize()."""

    def test_sector_mean_near_zero(self, factor_df, sector_df):
        """After neutralization, each (date, sector) group should have mean ≈ 0."""
        neutralized = sector_neutralize(factor_df, sector_df)
        # Re-join sectors for checking
        with_sectors = neutralized.join(sector_df, on="ticker", how="left")
        means = with_sectors.group_by(["date", "sector"]).agg(
            pl.col("value").mean().alias("mean")
        )
        max_abs_mean = means["mean"].abs().max()
        assert max_abs_mean < 1e-10

    def test_shape_preserved(self, factor_df, sector_df):
        result = sector_neutralize(factor_df, sector_df)
        assert result.shape == factor_df.shape


# ══════════════════════════════════════════════════════════════════════════════
#  Preprocess Factor (Full Pipeline)
# ══════════════════════════════════════════════════════════════════════════════


class TestPreprocessFactor:
    """Tests for preprocess_factor()."""

    def test_zscore_pipeline(self, factor_df):
        result = preprocess_factor(factor_df, method="zscore")
        means = result.group_by("date").agg(pl.col("value").mean().alias("mean"))
        assert means["mean"].abs().max() < 1e-10

    def test_rank_pipeline(self, factor_df):
        result = preprocess_factor(factor_df, method="rank")
        assert result["value"].min() >= -1.0 - 1e-10
        assert result["value"].max() <= 1.0 + 1e-10

    def test_with_sector_neutralization(self, factor_df, sector_df):
        result = preprocess_factor(
            factor_df,
            sectors=sector_df,
            neutralize=["sector"],
            method="zscore",
        )
        assert result.shape[0] == factor_df.shape[0]

    def test_invalid_method_raises(self, factor_df):
        with pytest.raises(ValueError, match="Unknown method"):
            preprocess_factor(factor_df, method="bad")

    def test_sector_without_df_raises(self, factor_df):
        with pytest.raises(ValueError, match="required"):
            preprocess_factor(factor_df, neutralize=["sector"])

    def test_nan_rows_removed(self, factor_df):
        """Rows with NaN/Inf values are dropped."""
        with_nans = factor_df.with_columns(
            pl.when(pl.col("ticker") == "TICK_00")
            .then(None)
            .otherwise(pl.col("value"))
            .alias("value")
        )
        result = preprocess_factor(with_nans, method="zscore")
        # All TICK_00 rows should be gone
        assert result.filter(pl.col("ticker") == "TICK_00").height == 0


# ══════════════════════════════════════════════════════════════════════════════
#  Factor Combination
# ══════════════════════════════════════════════════════════════════════════════


class TestCombineFactors:
    """Tests for combine_factors()."""

    def test_equal_weight_is_mean(self, factor_df, rng, trading_dates):
        """Equal-weight combination of 2 factors = their mean."""
        # Create a second independent factor
        rows = []
        for d in trading_dates:
            for t in [
                "TICK_00",
                "TICK_01",
                "TICK_02",
                "TICK_03",
                "TICK_04",
                "TICK_05",
                "TICK_06",
                "TICK_07",
                "TICK_08",
                "TICK_09",
            ]:
                rows.append(
                    {"date": d, "ticker": t, "value": float(rng.standard_normal())}
                )
        factor_b = pl.DataFrame(rows)

        composite = combine_factors([factor_df, factor_b], method="equal_weight")
        assert "value" in composite.columns
        assert composite.height > 0

        # Check that composite ≈ 0.5*A + 0.5*B for a sample row
        d0 = trading_dates[0]
        t0 = "TICK_00"
        val_a = factor_df.filter((pl.col("date") == d0) & (pl.col("ticker") == t0))[
            "value"
        ][0]
        val_b = factor_b.filter((pl.col("date") == d0) & (pl.col("ticker") == t0))[
            "value"
        ][0]
        val_c = composite.filter((pl.col("date") == d0) & (pl.col("ticker") == t0))[
            "value"
        ][0]
        np.testing.assert_allclose(val_c, 0.5 * val_a + 0.5 * val_b, atol=1e-10)

    def test_single_factor_passthrough(self, factor_df):
        """Single factor → returned as-is."""
        result = combine_factors([factor_df])
        assert result.equals(factor_df)

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="At least one"):
            combine_factors([])

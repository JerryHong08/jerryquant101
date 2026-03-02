"""
Tests for src/validation/ — walk-forward splits, statistical tests, multiple testing.
"""

from __future__ import annotations

import numpy as np
import pytest

from validation.multiple_testing import (
    apply_all_corrections,
    benjamini_hochberg,
    bonferroni,
    holm_bonferroni,
)
from validation.statistical_tests import (
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

# ══════════════════════════════════════════════════════════════════════════════
#  Walk-Forward Split
# ══════════════════════════════════════════════════════════════════════════════


class TestWalkForwardSplit:
    """Tests for walk_forward_split()."""

    def test_rolling_basic(self):
        """Rolling mode produces expected number of folds."""
        folds = walk_forward_split(
            n_dates=504, train_days=126, test_days=63, embargo_days=5, mode="rolling"
        )
        assert len(folds) >= 3
        assert all(isinstance(f, WalkForwardFold) for f in folds)

    def test_rolling_no_overlap(self):
        """Test windows in rolling mode do not overlap."""
        folds = walk_forward_split(
            n_dates=504, train_days=126, test_days=63, embargo_days=5, mode="rolling"
        )
        for i in range(len(folds) - 1):
            # Current test must end before next test starts
            assert folds[i].test_end_idx < folds[i + 1].test_start_idx

    def test_embargo_gap_respected(self):
        """Embargo creates a gap between train end and test start."""
        embargo = 10
        folds = walk_forward_split(
            n_dates=504,
            train_days=126,
            test_days=63,
            embargo_days=embargo,
            mode="rolling",
        )
        for fold in folds:
            gap = fold.test_start_idx - fold.train_end_idx - 1
            assert gap == embargo, f"Fold {fold.fold_id}: gap={gap}, expected {embargo}"

    def test_anchored_expanding_train(self):
        """Anchored mode: all folds start at index 0, training window expands."""
        folds = walk_forward_split(
            n_dates=504, train_days=126, test_days=63, embargo_days=5, mode="anchored"
        )
        assert len(folds) >= 2
        for fold in folds:
            assert fold.train_start_idx == 0, "Anchored folds must start at index 0"
        # Training size should be non-decreasing
        for i in range(1, len(folds)):
            assert folds[i].train_size >= folds[i - 1].train_size

    def test_fold_sizes(self):
        """Train and test sizes match requested parameters (except possibly last fold)."""
        folds = walk_forward_split(
            n_dates=504, train_days=126, test_days=63, embargo_days=5, mode="rolling"
        )
        for fold in folds[:-1]:  # Skip last (may be partial)
            assert fold.train_size == 126
            assert fold.test_size == 63

    def test_invalid_inputs(self):
        """Invalid parameters raise ValueError."""
        with pytest.raises(ValueError, match="positive"):
            walk_forward_split(n_dates=0, train_days=126, test_days=63)
        with pytest.raises(ValueError, match="positive"):
            walk_forward_split(n_dates=504, train_days=0, test_days=63)
        with pytest.raises(ValueError, match="non-negative"):
            walk_forward_split(
                n_dates=504, train_days=126, test_days=63, embargo_days=-1
            )

    def test_too_few_dates_raises(self):
        """Raise ValueError if dates are too few for even one fold."""
        with pytest.raises(ValueError, match="No valid folds"):
            walk_forward_split(n_dates=50, train_days=126, test_days=63)

    def test_invalid_mode(self):
        """Invalid mode raises ValueError."""
        with pytest.raises(ValueError, match="mode"):
            walk_forward_split(n_dates=504, train_days=126, test_days=63, mode="bad")


class TestApplyFoldsToDates:
    """Tests for apply_folds_to_dates()."""

    def test_date_mapping(self, trading_dates):
        n = len(trading_dates)
        folds = walk_forward_split(
            n_dates=n, train_days=63, test_days=21, embargo_days=5
        )
        date_folds = apply_folds_to_dates(trading_dates, folds)

        assert len(date_folds) == len(folds)
        for df_fold, fold in zip(date_folds, folds):
            assert df_fold["fold_id"] == fold.fold_id
            assert len(df_fold["train_dates"]) == fold.train_size
            assert len(df_fold["test_dates"]) == fold.test_size


class TestSummarizeFolds:
    """Tests for summarize_folds()."""

    def test_summary_keys(self):
        folds = walk_forward_split(n_dates=504, train_days=126, test_days=63)
        summary = summarize_folds(folds)
        assert "n_folds" in summary
        assert "avg_train_size" in summary
        assert "avg_test_size" in summary
        assert summary["n_folds"] == len(folds)


# ══════════════════════════════════════════════════════════════════════════════
#  Statistical Tests
# ══════════════════════════════════════════════════════════════════════════════


class TestBootstrapSharpeCI:
    """Tests for bootstrap_sharpe_ci()."""

    def test_ci_contains_point_estimate(self, daily_returns):
        result = bootstrap_sharpe_ci(daily_returns, n_bootstrap=500, seed=42)
        assert result.ci_lower <= result.point_estimate <= result.ci_upper

    def test_ci_gets_narrower_with_more_data(self, rng):
        """More data → narrower CI."""
        short = rng.normal(0.0002, 0.01, size=50)
        long = rng.normal(0.0002, 0.01, size=500)

        ci_short = bootstrap_sharpe_ci(short, n_bootstrap=500, seed=42)
        ci_long = bootstrap_sharpe_ci(long, n_bootstrap=500, seed=42)

        assert ci_long.ci_width < ci_short.ci_width

    def test_positive_returns_positive_sharpe(self):
        rng = np.random.default_rng(42)
        returns = rng.normal(0.001, 0.0001, size=252)
        result = bootstrap_sharpe_ci(returns, n_bootstrap=200, seed=42)
        assert result.point_estimate > 0

    def test_too_few_returns_raises(self):
        with pytest.raises(ValueError, match="at least 10"):
            bootstrap_sharpe_ci(np.array([0.01, 0.02, 0.03]))

    def test_bootstrap_distribution_length(self, daily_returns):
        n_boot = 300
        result = bootstrap_sharpe_ci(daily_returns, n_bootstrap=n_boot, seed=42)
        assert len(result.bootstrap_distribution) == n_boot


class TestSharpePvalue:
    """Tests for sharpe_pvalue()."""

    def test_strong_positive_returns(self):
        """Consistently positive returns should yield low p-value."""
        rng = np.random.default_rng(42)
        returns = rng.normal(0.002, 0.005, size=252)  # strong signal with noise
        result = sharpe_pvalue(returns)
        assert result["p_value"] < 0.05
        assert result["sharpe"] > 0

    def test_zero_mean_returns(self, rng):
        """Zero-mean returns should yield high p-value (not significant)."""
        returns = rng.normal(0, 0.01, size=252)
        result = sharpe_pvalue(returns)
        # Not guaranteed but highly likely for zero-mean
        assert result["p_value"] > 0.01

    def test_output_keys(self, daily_returns):
        result = sharpe_pvalue(daily_returns)
        for key in ["sharpe", "se", "t_stat", "p_value", "n_obs"]:
            assert key in result

    def test_too_few_returns(self):
        result = sharpe_pvalue(np.array([0.01, 0.02]))
        assert result["p_value"] == 1.0


class TestProbabilisticSharpeRatio:
    """Tests for probabilistic_sharpe_ratio()."""

    def test_strong_returns_high_psr(self):
        rng = np.random.default_rng(42)
        returns = rng.normal(0.002, 0.005, size=252)
        result = probabilistic_sharpe_ratio(returns, benchmark_sr=0.0)
        assert result["psr"] > 0.95

    def test_benchmark_above_sharpe(self, daily_returns):
        """If benchmark is much higher than actual Sharpe, PSR should be low."""
        result = probabilistic_sharpe_ratio(daily_returns, benchmark_sr=5.0)
        assert result["psr"] < 0.5


class TestDeflatedSharpeRatio:
    """Tests for deflated_sharpe_ratio()."""

    def test_more_trials_lower_dsr(self):
        """DSR decreases as number of trials increases."""
        rng = np.random.default_rng(42)
        returns = rng.normal(0.001, 0.005, size=252)
        dsr_1 = deflated_sharpe_ratio(returns, n_trials=1)
        dsr_16 = deflated_sharpe_ratio(returns, n_trials=16)
        dsr_100 = deflated_sharpe_ratio(returns, n_trials=100)
        assert dsr_1["dsr"] >= dsr_16["dsr"] >= dsr_100["dsr"]

    def test_single_trial_equals_psr(self):
        """With 1 trial, DSR = PSR with benchmark_sr=0."""
        rng = np.random.default_rng(42)
        returns = rng.normal(0.001, 0.005, size=252)
        dsr = deflated_sharpe_ratio(returns, n_trials=1)
        psr = probabilistic_sharpe_ratio(returns, benchmark_sr=0.0)
        np.testing.assert_allclose(dsr["dsr"], psr["psr"], atol=1e-6)

    def test_invalid_n_trials(self):
        with pytest.raises(ValueError, match="n_trials"):
            deflated_sharpe_ratio(np.array([0.01] * 50), n_trials=0)


# ══════════════════════════════════════════════════════════════════════════════
#  Multiple Testing Corrections
# ══════════════════════════════════════════════════════════════════════════════


KNOWN_PVALUES = {
    "Strategy_A": 0.001,
    "Strategy_B": 0.01,
    "Strategy_C": 0.03,
    "Strategy_D": 0.10,
    "Strategy_E": 0.50,
}


class TestBonferroni:
    """Tests for bonferroni()."""

    def test_known_correction(self):
        """Bonferroni: corrected_p = p * N."""
        result = bonferroni(KNOWN_PVALUES, alpha=0.05)
        assert result.n_tests == 5
        # Strategy_A: 0.001 * 5 = 0.005 < 0.05 → significant
        sig_labels = {r.label for r in result.results if r.significant}
        assert "Strategy_A" in sig_labels
        # Strategy_D: 0.10 * 5 = 0.50 ≥ 0.05 → not significant
        assert "Strategy_D" not in sig_labels

    def test_corrected_pvalues_capped(self):
        """Corrected p-values should never exceed 1.0."""
        result = bonferroni(KNOWN_PVALUES, alpha=0.05)
        for r in result.results:
            assert r.corrected_p_value <= 1.0

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="not be empty"):
            bonferroni({})


class TestHolmBonferroni:
    """Tests for holm_bonferroni()."""

    def test_less_conservative_than_bonferroni(self):
        """Holm should reject at least as many hypotheses as Bonferroni."""
        bf = bonferroni(KNOWN_PVALUES)
        hb = holm_bonferroni(KNOWN_PVALUES)
        assert hb.n_significant >= bf.n_significant

    def test_monotonic_corrected_pvalues(self):
        """Corrected p-values should be non-decreasing."""
        result = holm_bonferroni(KNOWN_PVALUES)
        cps = [r.corrected_p_value for r in result.results]
        for i in range(1, len(cps)):
            assert cps[i] >= cps[i - 1] - 1e-12

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="not be empty"):
            holm_bonferroni({})


class TestBenjaminiHochberg:
    """Tests for benjamini_hochberg()."""

    def test_most_powerful(self):
        """BH should reject at least as many as Holm-Bonferroni (usually more)."""
        hb = holm_bonferroni(KNOWN_PVALUES)
        bh = benjamini_hochberg(KNOWN_PVALUES)
        assert bh.n_significant >= hb.n_significant

    def test_corrected_pvalues_capped(self):
        result = benjamini_hochberg(KNOWN_PVALUES)
        for r in result.results:
            assert 0 <= r.corrected_p_value <= 1.0

    def test_single_test(self):
        """With one test, no correction is needed."""
        result = benjamini_hochberg({"only": 0.04}, alpha=0.05)
        assert result.n_significant == 1
        np.testing.assert_allclose(result.results[0].corrected_p_value, 0.04)

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="not be empty"):
            benjamini_hochberg({})


class TestApplyAllCorrections:
    """Tests for apply_all_corrections()."""

    def test_returns_all_methods(self):
        results = apply_all_corrections(KNOWN_PVALUES)
        assert set(results.keys()) == {"bonferroni", "holm", "bh"}
        for key, result in results.items():
            assert result.n_tests == 5

    def test_power_ordering(self):
        """Bonferroni ≤ Holm ≤ BH in number of rejections."""
        results = apply_all_corrections(KNOWN_PVALUES)
        assert results["bonferroni"].n_significant <= results["holm"].n_significant
        assert results["holm"].n_significant <= results["bh"].n_significant

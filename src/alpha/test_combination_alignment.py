import polars as pl

from alpha.combination import combine_factors
from alpha.factor_analyzer import FactorAnalyzer


def test_combine_factors_mean_variance_aligns_ic_by_date() -> None:
    factors = [
        pl.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "ticker": ["A", "A"],
                "value": [1.0, 2.0],
            }
        ),
        pl.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "ticker": ["A", "A"],
                "value": [0.5, 1.5],
            }
        ),
    ]

    ic_1 = pl.DataFrame({"date": [1, 2, 3], "ic": [0.1, 0.2, 0.3]})
    ic_2 = pl.DataFrame({"date": [2, 3, 4], "ic": [0.5, 0.6, 0.7]})

    out = combine_factors(
        factors=factors,
        method="mean_variance",
        ic_series_list=[ic_1, ic_2],
    )

    assert out.height == 2
    assert "value" in out.columns


def test_quantile_returns_sorted_numerically_for_many_bins() -> None:
    signal = pl.DataFrame(
        {
            "date": ["2024-01-01"] * 12,
            "ticker": [f"T{i}" for i in range(12)],
            "value": list(range(12)),
        }
    )
    returns = pl.DataFrame(
        {
            "date": ["2024-01-01"] * 12,
            "ticker": [f"T{i}" for i in range(12)],
            "forward_return_1d": [0.01] * 12,
        }
    )

    analyzer = FactorAnalyzer(signal=signal, returns=returns, min_observations=1)
    out = analyzer.quantile_returns(horizon=1, n_quantiles=12)

    assert out["quantile"].to_list() == sorted(out["quantile"].to_list())
    assert out["quantile"].dtype == pl.Int32

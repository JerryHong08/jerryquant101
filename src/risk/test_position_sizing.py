import polars as pl

from risk.position_sizing import size_quantile_threshold


def test_size_quantile_threshold_equal_weight_normalizes_gross_one() -> None:
    signal = pl.DataFrame(
        {
            "date": ["2024-01-01"] * 10,
            "ticker": [f"T{i}" for i in range(10)],
            "value": list(range(10)),
        }
    )

    weights = size_quantile_threshold(
        signal,
        long_quantile=0.8,
        short_quantile=0.2,
        weight_by_signal_strength=False,
    )

    assert weights.height == 4
    gross = weights.select(pl.col("weight").abs().sum()).item()
    assert abs(gross - 1.0) < 1e-10


def test_size_quantile_threshold_signal_strength_overweights_extremes() -> None:
    signal = pl.DataFrame(
        {
            "date": ["2024-01-01"] * 5,
            "ticker": ["A", "B", "C", "D", "E"],
            "value": [-10.0, -1.0, 0.0, 1.0, 8.0],
        }
    )

    weights = size_quantile_threshold(
        signal,
        long_quantile=0.75,
        short_quantile=0.25,
        weight_by_signal_strength=True,
    )

    a_weight = weights.filter(pl.col("ticker") == "A").select("weight").item()
    b_weight = weights.filter(pl.col("ticker") == "B").select("weight").item()
    e_weight = weights.filter(pl.col("ticker") == "E").select("weight").item()

    assert abs(a_weight) > abs(b_weight)
    assert e_weight > 0


def test_size_quantile_threshold_supports_long_only_mode() -> None:
    signal = pl.DataFrame(
        {
            "date": ["2024-01-01"] * 6,
            "ticker": ["A", "B", "C", "D", "E", "F"],
            "value": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        }
    )

    weights = size_quantile_threshold(
        signal,
        long_quantile=0.8,
        short_quantile=None,
        weight_by_signal_strength=False,
    )

    assert weights.select((pl.col("weight") > 0).all()).item()
    gross = weights.select(pl.col("weight").abs().sum()).item()
    assert abs(gross - 1.0) < 1e-10

# Quant101

A structured quantitative research laboratory for US equity alpha.

Built with [Polars](https://pola.rs/), backtested with a custom engine,
and documented as a 59-page learning journal in LaTeX.

---

## What This Project Is

| | |
|---|---|
| **Data** | Polygon.io flat files → Parquet, split-adjusted, 50-stock US large-cap universe |
| **Alpha** | Factor signals → IC/IR evaluation → preprocessing → combination |
| **Risk** | VaR/CVaR, drawdown, position sizing (EW, inverse-vol, vol-target, signal-weighted) |
| **Execution** | Transaction cost models (fixed, spread, sqrt-impact), breakeven analysis |
| **Validation** | Walk-forward, bootstrap Sharpe CI, PSR/DSR, multiple-testing corrections |
| **Pipeline** | `AlphaConfig` → factor registry → sizing → rebalancing → backtest, all in one call |

**201 tests** · **64 source files** · **12.8K lines** · **7 research notebooks**

---

## Quick Links

<div class="grid cards" markdown>

- :material-rocket-launch: **[Quick Start](getting-started/quickstart.md)**

    Get up and running in 5 minutes

- :material-file-tree: **[Architecture](architecture/overview.md)**

    How the modules fit together

- :material-flask: **[Research Guide](research/factor-workflow.md)**

    End-to-end factor research workflow

- :material-lightbulb: **[Lessons Learned](lessons/quant-notes.md)**

    Key insights from building this lab

</div>

---

## Current Status

**v1.0.0** — All research infrastructure complete (Phases 0–7).
The lab is built; now it needs experiments that produce real alpha signal.

See the [Changelog](changelog.md) for version history,
or the [LaTeX encyclopedia](https://github.com/JerryHong08/jerryquant101/blob/master/guidance/quant_lab.pdf)
for the full learning journal.

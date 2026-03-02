# Quick Start

Get Quant101 running locally in 5 minutes.

## Prerequisites

- Python 3.12+
- [Poetry](https://python-poetry.org/) for dependency management
- Polygon.io flat file data (or any OHLCV source in Parquet)

## Installation

```bash
git clone git@github.com:JerryHong08/jerryquant101.git
cd jerryquant101
poetry install
```

## Configuration

Copy the example config and edit your data paths:

```bash
cp basic_config.yaml.example basic_config.yaml
```

Key fields in `basic_config.yaml`:

```yaml
data:
  data_dir: /path/to/your/polygon_data    # Root of your data directory

update:
  mode: standalone                         # standalone | server | client
```

## Run the Test Suite

```bash
python -m pytest --tb=short -q
# Expected: 201 passed
```

## Run a Pipeline Backtest

The fastest way to see the system work end-to-end:

```bash
cd src && python -m backtest.backtester --mode pipeline
```

This runs the default alpha pipeline (BBIBOLL factor, equal-weight sizing,
daily rebalancing) on the US Large Cap 50 universe and prints a Sharpe ratio.

## Run a Strategy Backtest

For the legacy trade-level BBIBOLL backtest:

```bash
cd src && python -m backtest.backtester --mode strategy
```

## Explore Notebooks

The research notebooks are the best way to understand each module:

| Notebook | What It Shows |
|----------|--------------|
| `pipeline_demo.ipynb` | Full pipeline: OHLCV → factor → weights → Sharpe |
| `factor_diagnostics.ipynb` | Per-factor IC/IR, L/S returns, direction check |
| `risk_analysis.ipynb` | VaR, CVaR, drawdown, 4 sizing methods compared |
| `cost_analysis.ipynb` | Transaction costs, breakeven analysis, rebalancing frequency |
| `validation.ipynb` | Walk-forward, bootstrap CI, PSR/DSR, multiple testing |
| `alpha_research.ipynb` | End-to-end BBIBOLL factor analysis |
| `alpha_iteration.ipynb` | Multi-factor: STR, Vol Ratio, IC correlation |

## Next Steps

- Read the [Architecture Overview](../architecture/overview.md) to understand how modules connect
- Follow the [Factor Research Workflow](../research/factor-workflow.md) to build your own alpha
- Review the [Lessons Learned](../lessons/quant-notes.md) to avoid common pitfalls

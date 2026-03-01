# Quant Lab Assistant — Skill File

> **Read this file at the start of every session.**
> It defines your role, responsibilities, and working conventions.
> It does NOT track progress — see `README.md` for the roadmap and `CHANGELOG.md` for history.

---

## Your Role

You are the **Quant Lab Assistant** for Jerry's `quant101` project.

**Mission**: Help Jerry upgrade this project from a beginner backtesting experiment
into a structured learning project that takes him from beginner to job-market-ready
quantitative researcher & trader, targeting v1.0.0.

---

## How You Work

1. **Build incrementally.** Each new module must work with the existing codebase without breaking anything. Run AST checks or tests after every batch of edits.
2. **Follow the LaTeX.** Every module maps to a chapter in `docs/quant_lab.tex`. Code follows the methodology: Motivation → Concepts → Implementation → Experiment → Reflection.
3. **Validate with notebooks.** After building a new `src/` module, create a research notebook in `notebooks/` to exercise it end-to-end with real data.
4. **Update the roadmap, not this file.** When you complete work, update `README.md` (roadmap checkboxes) and `CHANGELOG.md` (version history). Only update this file if conventions or architecture fundamentally change.
5. **The LaTeX is Jerry's encyclopedia.** `docs/quant_lab.tex` is written *for Jerry* to reflect on and learn from. When new modules reveal insights, propose additions to the LaTeX — but Jerry decides what goes in.

---

## Document Responsibilities

| Document | Purpose | Update when |
|----------|---------|-------------|
| **SKILL.md** (this file) | Directive for AI assistant — conventions, architecture, key references | Conventions or architecture fundamentally change |
| **README.md** | Project overview, roadmap, quick start — the public face | Every meaningful code change (check off items, add new phases) |
| **CHANGELOG.md** | Version history with conventional commits | Every version bump |
| **docs/quant_lab.tex** | Learning encyclopedia for Jerry — theory, experiments, reflections | When concepts solidify or new experiments yield insights |

---

## Project Identity

| Key | Value |
|-----|-------|
| Name | quant101 |
| Language | Python 3.12+ |
| DataFrame library | Polars (never pandas unless a dependency requires it) |
| Data source | Polygon.io (Massive) flat files (S3) — US equities |
| Build tool | Poetry (`pyproject.toml`) |
| Version control | Commitizen (`cz_conventional_commits`) |
| Config | `basic_config.yaml` (gitignored, template at `.example`) |
| LaTeX compiler | XeLaTeX at `/mnt/blackdisk/texlive/bin/x86_64-linux/xelatex` |

---

## Conventions

### Code Style
- **Language**: English only — code, comments, docstrings, prints
- **Naming**: `snake_case` for files/functions, `PascalCase` for classes
- **Imports**: `data.loader.xxx`, `data.fetcher.xxx`, `strategy.xxx`, `backtest.xxx`, `alpha.xxx`
- **Config**: Machine-specific paths via `basic_config.yaml`, code reads via `src/config.py`
- **Testing**: AST parse check after every batch of edits; notebook validation for new modules
- **Commits**: Conventional commits via commitizen (`feat:`, `fix:`, `refactor:`, `docs:`)

### Data Conventions
- **Factor signal**: `pl.DataFrame` with columns `(date, ticker, value)` — 3-column long format
- **Forward returns**: `pl.DataFrame` with columns `(date, ticker, forward_return_1d, ..., forward_return_Nd)`
- **OHLCV**: `stock_load_process()` returns `pl.LazyFrame` with `(ticker, timestamps, open, high, low, close, volume, ...)`
- **Timestamps column**: Raw data uses `timestamps`; factor/returns DataFrames rename to `date`

### Module Pattern
When building a new `src/` module (e.g., `src/risk/`):
1. Create the subpackage with `__init__.py` that re-exports the public API
2. Write implementation files with full docstrings (numpy-style)
3. Create a validation notebook in `notebooks/` that exercises the module end-to-end
4. Update `README.md` roadmap checkboxes
5. Propose relevant additions to `docs/quant_lab.tex` if experiments yield insights

---

## Architecture Reference

```
src/
├── config.py                  # Central config — data paths, asset loaders, lazy getters
├── alpha/                     # Factor research — signals, evaluation, preprocessing, combination
├── data/
│   ├── fetcher/               # Data acquisition — Polygon.io S3, FMP, yfinance, rsync
│   └── loader/                # Data loading — OHLCV, split adjustment, resampling, caching
├── backtest/                  # Backtesting — engine, strategy base, performance analyzer, visualizer
├── strategy/                  # Trading strategies & indicator registry
│   └── indicators/            # Technical indicators (BBIBOLL, OBV, etc.)
├── visualizer/                # Standalone charting
├── longport/                  # Longport broker integration
├── examples/                  # Usage examples
└── utils/                     # Logger, shared utilities
```

**Target modules** (not yet built — see README.md roadmap):
- `src/risk/` — VaR, CVaR, position sizing, portfolio construction
- `src/execution/` — Transaction cost modeling
- `src/ml/` — Feature engineering, time-series validation, tree models

---

## Key Files Lookup

| When you need... | Read... |
|---|---|
| Data paths, asset loaders | `src/config.py` |
| OHLCV loading API | `src/data/loader/data_loader.py` — `stock_load_process()` |
| Ticker universe, FIGI | `src/data/loader/ticker_utils.py` |
| Strategy pattern | `src/strategy/bbiboll_strategy.py` |
| Indicator registration | `src/strategy/indicators/registry.py` |
| Backtest orchestration | `src/backtest/backtester.py` |
| Performance metrics | `src/backtest/performance_analyzer.py` |
| Factor evaluation (IC/IR) | `src/alpha/factor_analyzer.py` |
| Factor preprocessing | `src/alpha/preprocessing.py` |
| Factor combination | `src/alpha/combination.py` |
| Forward returns | `src/alpha/forward_returns.py` |
| Full learning methodology | `docs/quant_lab.tex` |
| Roadmap & current state | `README.md` |
| Version history | `CHANGELOG.md` |

---

## LaTeX ↔ Code Mapping

| LaTeX Chapter | Code Module |
|---|---|
| Part I: Data Engineering (Ch 1–3) | `src/data/` |
| Part II: Strategy & Backtesting (Ch 4–7) | `src/strategy/`, `src/backtest/` |
| Part III: Alpha Research (Ch 8–11) | `src/alpha/` |
| Part IV: Portfolio, Risk & Execution (Ch 12–14) | `src/risk/`, `src/execution/` |
| Part V: Machine Learning (Ch 15–17) | `src/ml/` |

# Quant Notes

Key insights discovered during development and study.
Each entry captures the date, the question, the finding, and why it matters.

---

## Entry 1 — Individual Factor IR Is Weak by Design
**Date:** 2026-03-02

**Question:** All four factors (BBIBOLL, STR, VPD, Vol Ratio) showed |IR| in the
0.05–0.12 range. Are these signals too weak to be useful?

**Finding:** No — this is *normal*. Practical IR ranges:

| Range | Interpretation |
|-------|---------------|
| \|IR\| < 0.05 | Noise, not tradeable |
| 0.05 ≤ \|IR\| < 0.15 | Weak but real — typical for individual factors |
| 0.15 ≤ \|IR\| < 0.30 | Solid single factor |
| \|IR\| ≥ 0.50 | Suspicious — likely overfitted or in-sample |

**Why it matters:** The Fundamental Law of Active Management:

$$\text{IR}_{\text{portfolio}} \approx \text{IR}_{\text{single}} \times \sqrt{N}$$

Combining 10 orthogonal factors with IR = 0.10 gives portfolio IR ≈ 0.32.
The game is not finding one brilliant factor; it is combining many
weak-but-orthogonal signals.

---

## Entry 2 — Anti-Correlation Requires Aligned IC Signs
**Date:** 2026-03-02

**Question:** BBIBOLL and STR have strongly anti-correlated daily IC (ρ = −0.80).
Anti-correlation should be great for diversification — why did equal-weight
combination *dilute* the signal instead?

**Finding:** Anti-correlation in the IC *series* means the two factors
"take turns being right." But whether that helps depends on **direction**:

- **Same IC sign + anti-correlated → great.** Both predict the same direction.
  On days factor A is strong, B is weak — but they still point the same way.
  Mean IC preserved, variance drops, IR rises.

- **Opposite IC sign + anti-correlated → cancellation.** BBIBOLL has IC = −0.022,
  STR has IC = +0.013. When BBIBOLL fires strongly (IC ≈ −0.05), anti-correlation
  means STR also fires strongly — but in the *opposite* direction (IC ≈ +0.04).
  Average: (−0.05 + 0.04)/2 ≈ −0.005. The signals cancel.

**Experimental evidence:**

- BBIBOLL + Vol Ratio (ρ = 0.02, same IC sign): composite |IR| = 0.136 > 0.122 (best individual). ✅
- BBIBOLL + STR (ρ = −0.80, opposite IC sign): composite IR diluted. ❌

!!! quote "Interview One-Liner"
    *"Anti-correlated ICs reduce tracking error, but only improve IR if the
    factors agree on direction. Opposite IC signs turn anti-correlation from
    a hedge into a cancellation."*

---

## Entry 3 — Strategy-Based vs. Alpha-Pipeline Backtesting
**Date:** 2026-03-02

**Question:** Two backtest paths exist. What is the fundamental difference?

**Finding:** They embody two distinct mental models:

| Dimension | Strategy ("I Trade") | Pipeline ("I Allocate") |
|-----------|---------------------|------------------------|
| Unit of analysis | Individual trade | Portfolio weight |
| State | Entry price, holding flag | None (recomputed fresh) |
| Decision | Buy/sell *this* stock? | Allocate across *all* stocks? |
| Output | Trade list + P&L | Weight matrix + return series |
| Rebalance | Signal-triggered | Calendar-based |
| Typical role | Trader / execution | Researcher / PM |

**Why it matters:** Most quant interviews and institutional workflows assume
the **pipeline worldview**. The strategy approach is valuable for execution
analysis, but the pipeline is closer to how firms like Citadel, Two Sigma,
and AQR structure research.

The bridge is `WeightBacktester`: it accepts pipeline output (weights)
and produces the equity curve that connects "I allocate" to concrete metrics.

---

## Entry 4 — Kelly Criterion Is a Leverage Tool, Not an Allocation Tool
**Date:** 2026-03-02

The most important lesson from Phase 7. See the full writeup in
[Position Sizing → The Kelly Lesson](../research/position-sizing.md#the-kelly-lesson).

**Summary:**

- Kelly solves *"how much total capital to deploy"*, not *"how to split
  capital across N stocks"*
- Using scalar $\mu_i / \sigma_i^2$ for cross-sectional allocation ignores
  correlations and concentrates into low-vol names
- For BBIBOLL (mean-reversion), this systematically under-weights the
  highest-alpha stocks (which are high-vol by nature)
- Ablation study: signal-weighted sizing cost **0.68 Sharpe** vs equal-weight
- Renamed from "Half-Kelly" to "Signal-Weighted" for honesty

!!! quote "Interview One-Liner"
    *"Kelly tells you how much to bet, not how to split the bet. Using
    scalar Kelly for cross-sectional allocation ignores correlations and
    concentrates into low-vol names — exactly the wrong thing for a
    volatility-driven alpha."*

# Position Sizing

How portfolio weights are determined given a factor signal.

## The Four Methods

Quant101 provides four sizing methods, all producing a weight DataFrame
with columns `(date, ticker, weight)`:

### Equal Weight

$$w_i = \begin{cases} +1/N & \text{if stock } i \text{ is in the long leg} \\ -1/N & \text{if stock } i \text{ is in the short leg} \end{cases}$$

The baseline. No information beyond rank ordering is used.

### Inverse Volatility

$$w_i \propto \frac{1}{\sigma_i}$$

Lower-volatility stocks get higher weight. Risk-parity inspired — equalizes
risk contribution rather than capital contribution.

### Volatility Target

$$w_i = \frac{\sigma_{\text{target}}}{\sigma_i}$$

Each position is sized so its dollar volatility matches a target. Useful
when you want a specific portfolio volatility profile.

### Signal-Weighted

$$w_i \propto \frac{\text{direction}_i \times |z_i|}{\sigma_i^2}$$

where:

- $z_i$ = cross-sectional z-score of the factor signal (conviction)
- $\sigma_i^2$ = rolling variance of historical returns (risk)
- Normalized to $\sum|w_i| = \text{max\_leverage}$ (default 1.0)

High-conviction, low-risk stocks get larger positions.

---

## The Kelly Lesson

!!! failure "Critical Bug: Why 'Half-Kelly' Was Wrong"
    This method was originally called "Half-Kelly sizing." It took three
    successive bug fixes and an ablation study to understand why that
    name (and mental model) was fundamentally misleading.

### What Happened

We implemented $w_i \propto \mu_i / \sigma_i^2$ (classic Kelly) for
cross-sectional portfolio allocation. Results:

| Config | Sharpe |
|--------|--------|
| Equal-Weight + Daily | **+0.814** (baseline) |
| Equal-Weight + Weekly | +0.417 |
| Signal-Weighted + Daily | +0.137 |
| Signal-Weighted + Weekly | −0.393 |

Signal-weighted sizing cost **0.68 Sharpe** compared to equal-weight —
the single largest source of degradation.

### The Three Bugs

1. **Normalization destroyed Kelly's answer.** Normalizing $\sum|w| = 1$
   discards the leverage signal, which is the whole point of Kelly.

2. **$\mu_i$ from returns ignored the factor.** Historical mean return over
   60 days is pure noise (SNR ≈ 0.02). All configs produced identical
   Sharpe = 0.343 regardless of factor chosen.

3. **Historical $\mu$ is stealth momentum.** A 60-day rolling mean injects
   momentum bias that conflicts with a mean-reversion signal like BBIBOLL.

### Root Cause

Kelly criterion solves: *"What fraction of my bankroll should I wager
on a bet with known edge $\mu$ and variance $\sigma^2$?"*

$$f^* = \frac{\mu}{\sigma^2}$$

This is for a **single repeated bet with independent outcomes**. We
misused it for **cross-sectional allocation across correlated assets**.

| | Kelly's Problem | Our Misuse |
|---|---|---|
| **Question** | How much total capital to deploy? | How to split capital across N stocks? |
| **Input** | Edge and variance of one bet | Cross-sectional factor signal |
| **Output** | Portfolio leverage $f^*$ | Relative stock weights |
| **Regime** | Independent repeated bets | Correlated concurrent positions |

The correct multi-asset generalization is **multivariate Kelly**:

$$\mathbf{f}^* = \Sigma^{-1} \boldsymbol{\mu}$$

where $\Sigma$ is the covariance matrix. Our per-stock $\mu_i / \sigma_i^2$
ignores correlations entirely.

### Why $z / \sigma^2$ Hurts Mean-Reversion

For BBIBOLL, the strongest alpha comes from **high-volatility** stocks
that have deviated far from their bands. But $w_i \propto z_i / \sigma_i^2$
penalizes large $\sigma_i$ quadratically. The stocks with the highest
conviction ($|z_i|$) also have the highest $\sigma_i$ — the penalty
overwhelms the signal.

### Resolution

| Action | Status |
|--------|--------|
| Renamed `size_half_kelly` → `size_signal_weighted` | ✅ |
| Docstring states "Not true Kelly" | ✅ |
| Formula kept as conviction × inverse-variance | ✅ |
| True Kelly (portfolio leverage) deferred to future module | Planned |

### Takeaway

!!! quote "Interview One-Liner"
    *"Kelly tells you how much to bet, not how to split the bet.
    Using scalar Kelly for cross-sectional allocation ignores correlations
    and concentrates into low-vol names — exactly the wrong thing for a
    volatility-driven alpha."*

Full version: [docs/latex/quant_lab.tex — Entry 4](https://github.com/JerryHong08/jerryquant101/blob/master/guidance/quant_lab.pdf).

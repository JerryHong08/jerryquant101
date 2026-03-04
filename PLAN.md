# Backtest/Alpha/Risk Hardening Plan (Recursive)

Execution loop:
1. read PLAN.md,
2. complete next pending batch,
3. run focused checks,
4. update PLAN.md,
5. repeat as long as possible.

## Progress Rules
- [ ] pending
- [~] in progress
- [x] done

## Batch B (completed earlier)
- [x] Fee calculation moved from end-equity scaling to notional-based estimation.
- [x] Open-position mark-to-market fields added.
- [x] Open-position PnL wired into performance metrics.
- [x] Unit tests added for fee and open PnL logic.
- [x] README backtest quick-start command fixed.

## Batch C (bridge for factor pipeline)
- [x] Add factor-portfolio backtest utility to bridge alpha weights -> portfolio return stream.
- [x] Export bridge API from `alpha.__init__`.
- [x] Add tests for equity-curve generation and latest positions output.

## Batch 4 (robustness follow-up)
- [x] Add quantity-aware notional support in fee estimation.
- [x] Add quantity-aware open-trade PnL support.
- [x] Fix `BBIBOLLStrategy.trade_rules` return type annotation to 3-tuple.
- [x] Extend tests for quantity-sensitive fee/PnL behavior.

## Batch 5 (next queued)
- [ ] Build transaction-cost model module (`fixed + spread + impact`) and integrate into both strategy and factor paths.
- [ ] Add shared output schema for strategy-based and factor-based backtest reports.

## Batch 6 (next queued)
- [ ] Add open-position lifecycle invariants and regression tests.
- [ ] Add walk-forward scaffold (rolling split + embargo hooks).

## Recursive Loop Result (current session)
Completed Batch C and Batch 4; queued Batch 5/6 for next implementation cycle.

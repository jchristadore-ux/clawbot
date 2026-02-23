# Live Readiness Report

## Verdict

**Not ready to go live** (real-money trading) in its current state.

The bot has solid paper-trading mechanics and several safety gates, but it lacks core production controls required for live deployment.

## What is good already

- Environment-driven risk controls exist (daily loss cap, cooldown, max trades/day, edge thresholds).
- A kill-switch flag is available.
- Multiple market data fallback paths exist (CLOB -> Gamma -> synthetic).
- Basic persistence and trade journaling are present.

## Blocking issues before live launch

1. **No true live execution path**
   - `LIVE_MODE` only tightens parameters; there is no authenticated order placement function.
   - Result: switching on `LIVE_MODE=true` does not execute real orders.

2. **No test suite**
   - There are no unit/integration tests validating core math, risk gates, or state transitions.
   - Result: regressions in PnL/accounting and risk checks can ship unnoticed.

3. **Single-file architecture without isolation boundaries**
   - Data access, strategy logic, risk checks, and orchestration are all in one script.
   - Result: hard to test, review, and safely evolve under production pressure.

4. **No idempotency/duplicate-run protection**
   - If scheduler retries the same window, the bot can process the same cycle twice.
   - Result: duplicate trades are possible.

5. **No explicit health/alerting integration**
   - Logs print to stdout, but there is no alert hook for repeated fetch failures, DB failures, or stale markets.
   - Result: operational issues may go unnoticed until after losses.

6. **No deployment runbook / go-live checklist in repo**
   - No documented preflight checks, rollback steps, environment baseline, or incident response flow.

## Recommended minimum launch bar

1. Implement a real live execution module (signed order placement + order status reconciliation).
2. Add tests for:
   - probability and edge math,
   - enter/exit transitions,
   - realized/unrealized PnL,
   - risk gates and kill-switch behavior.
3. Add candle/window idempotency key in DB and enforce one decision per interval.
4. Split code into modules (`data`, `strategy`, `risk`, `execution`, `state`) and keep `bot.py` as thin orchestration.
5. Add alerting (e.g., webhook) for repeated failures and risk-limit hits.
6. Add `GO_LIVE_CHECKLIST.md` and `RUNBOOK.md`.

## Fast confidence checks run in this review

- Python bytecode compile succeeded.
- No tests discovered in repo.
- Dependency footprint is minimal (`requests`, `psycopg2-binary`) with no pinned versions.

## Launch recommendation

Keep running in paper mode while implementing the blocking items above. Reassess after tests and a real execution/reconciliation path are in place.

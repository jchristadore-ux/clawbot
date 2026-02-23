# RUNBOOK (GitHub + Railway)

## Normal operation
- GitHub is source-of-truth; Railway auto-deploys from selected branch.
- Railway scheduler/cron triggers `python bot.py` every 5 minutes.
- Bot reserves an idempotency key in `processed_intervals`; duplicates are skipped.
- Bot fetches Kraken closes and Polymarket marks, computes edge/signal, applies risk gates.
- In live mode, bot places signed order and reconciles final status.

## Incident: repeated data fetch failures
1. Check network/API availability.
2. Confirm webhook alerts are arriving.
3. Temporarily set `KILL_SWITCH=true` while investigating.

## Incident: order placement or reconciliation failures
1. Validate `LIVE_EXEC_*` credentials and endpoint paths.
2. Check exchange status page and API health.
3. Keep `KILL_SWITCH=true` until reconciliation is stable.

## Incident: risk-limit churn
1. Review `MAX_DAILY_LOSS`, `COOLDOWN_MINUTES`, `MAX_TRADES_PER_DAY`.
2. Review signal thresholds (`EDGE_ENTER`, `EDGE_EXIT`, live overrides).
3. Reduce `LIVE_TRADE_SIZE` before re-enabling.

## Rollback
- Set `KILL_SWITCH=true` immediately.
- Set `LIVE_MODE=false` to return to paper execution path.
- Keep scheduler active to continue monitoring data quality without placing live orders.


## Deployment process (GitHub -> Railway)
1. Open PR in GitHub and merge to deployment branch.
2. Confirm Railway auto-deploy started from that commit.
3. Validate logs and health before enabling live trading changes.

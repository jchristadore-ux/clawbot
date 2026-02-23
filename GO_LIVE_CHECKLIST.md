# GO_LIVE_CHECKLIST (GitHub + Railway)

## Pre-deploy
- [ ] Confirm GitHub default branch contains latest bot code and tests.
- [ ] Railway project is connected to the correct GitHub repo/branch.
- [ ] Set `DATABASE_URL` (or verify Railway Postgres injection).
- [ ] Set live execution credentials: `LIVE_EXEC_BASE_URL`, `LIVE_EXEC_API_KEY`, `LIVE_EXEC_API_SECRET`, `LIVE_EXEC_API_PASSPHRASE`.
- [ ] Set `ALERT_WEBHOOK_URL`.
- [ ] Verify `LIVE_MODE=true` and `KILL_SWITCH=true` for smoke test.
- [ ] Run tests locally or in CI: `python -m unittest discover -s tests -v`.

## Dry run
- [ ] Trigger one Railway deploy from GitHub and verify startup command `python bot.py`.
- [ ] Run one cycle with `LIVE_MODE=false` and inspect Railway logs.
- [ ] Verify interval idempotency (`processed_intervals` gets one row per candle).
- [ ] Verify risk gate alerts fire when blocked.

## Live cutover
- [ ] Disable kill switch (`KILL_SWITCH=false`) only after confirming alert delivery.
- [ ] Confirm order placement and reconciliation statuses are logged.
- [ ] Verify first live order remains within configured `LIVE_TRADE_SIZE`.

## Post-deploy
- [ ] Monitor consecutive failures and webhook alerts for first 24h.
- [ ] Review `paper_trades`/live order logs and risk blocks.
- [ ] Re-enable `KILL_SWITCH=true` if repeated reconciliation failures occur.

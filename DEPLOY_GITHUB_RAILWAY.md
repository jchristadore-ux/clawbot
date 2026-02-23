# Deploy Clawbot with GitHub + Railway + Postgres

This guide is for your exact setup:
- **Code in GitHub**
- **Deploys from Railway**
- **Database in Railway Postgres**

---

## 1) Push code to GitHub

1. Create/update your repository on GitHub.
2. Ensure these files are present on your default branch:
   - `bot.py`
   - `requirements.txt`
   - `clawbot/` package
3. Commit and push.

Railway will pull from this GitHub repo directly.

---

## 2) Create Railway project and connect GitHub

1. In Railway: **New Project** → **Deploy from GitHub repo**.
2. Select your repo and branch.
3. Railway creates a service from your Python code.

---

## 3) Add Railway Postgres

1. In the same Railway project: **New** → **Database** → **PostgreSQL**.
2. Railway injects connection vars into project environment.
3. Confirm `DATABASE_URL` is available to your bot service.

> `clawbot/state.py` requires `DATABASE_URL` at startup.

---

## 4) Configure bot service start command

In Railway service settings:
- **Start Command**: `python bot.py`

If needed, set **Root Directory** to repo root.

---

## 5) Set required environment variables

In Railway service → **Variables**, set at minimum:

### Core
- `DATABASE_URL` (usually injected by Railway Postgres)
- `LIVE_MODE=false` (for initial paper validation)
- `KILL_SWITCH=true` (safe default)

### Strategy / Risk (recommended starting values)
- `START_BALANCE=1000`
- `TRADE_SIZE=25`
- `EDGE_ENTER=0.08`
- `EDGE_EXIT=0.04`
- `MAX_DAILY_LOSS=3`
- `COOLDOWN_MINUTES=20`
- `MAX_TRADES_PER_DAY=8`

### Alerts (recommended)
- `ALERT_WEBHOOK_URL=<your webhook>`
- `ALERT_CONSECUTIVE_FAILURE_THRESHOLD=3`

### Live execution (only when you’re ready)
- `LIVE_EXEC_BASE_URL=...`
- `LIVE_EXEC_ORDER_PATH=/orders`
- `LIVE_EXEC_ORDER_STATUS_PATH=/orders/{order_id}`
- `LIVE_EXEC_API_KEY=...`
- `LIVE_EXEC_API_SECRET=...`
- `LIVE_EXEC_API_PASSPHRASE=...`

---

## 6) Run cadence on Railway

This bot is candle-based (5m). Use one of:

### Option A: Railway Cron/Job style schedule (preferred)
- Run `python bot.py` every 5 minutes.

### Option B: Always-on worker
- Keep service running with external scheduler trigger strategy.

Because the bot uses `processed_intervals` idempotency, duplicate runs in same window are skipped.

---

## 7) Validate paper mode first

1. Keep `LIVE_MODE=false`.
2. Run/schedule the service.
3. Check logs for:
   - interval key generation
   - no DB errors
   - signal/action lines
4. Confirm database tables created:
   - `paper_state`
   - `paper_trades`
   - `processed_intervals`

---

## 8) Live cutover sequence

1. Set `KILL_SWITCH=true`, `LIVE_MODE=true`.
2. Run one cycle and verify:
   - signed order code path is reachable
   - credentials load correctly
   - no crash on reconciliation
3. Set `KILL_SWITCH=false` only after alerts are confirmed.
4. Start with minimal `LIVE_TRADE_SIZE`.

---

## 9) Ongoing operations

- Monitor Railway logs and alert webhook.
- Watch risk gate block messages.
- If anything degrades:
  - set `KILL_SWITCH=true`
  - optionally set `LIVE_MODE=false`

---

## 10) GitHub workflow recommendations

- Protect `main` with PR review.
- Require tests before merge (`python -m unittest discover -s tests -v`).
- Tag releases used for production deploys.


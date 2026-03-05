# CLAUDE.md — ClawBot Codebase Guide

This file provides AI assistants with a comprehensive understanding of the ClawBot repository: its structure, conventions, workflows, and operational context.

---

## Overview

**ClawBot** (internally called **Johnny5**) is a production algorithmic trading bot for Kalshi BTC 15-minute binary options markets (`KXBTC15M`). It uses a Black-Scholes-inspired pricing model to detect mispriced options and trades them when an edge exists above a dynamic threshold.

- **Language**: Python (primary), TypeScript/Bun (auth gateway)
- **Deployment target**: Railway (auto-deploys on push to master)
- **Database**: PostgreSQL (via `DATABASE_URL`)
- **Current version**: `JOHNNY5_KALSHI_BTC15M_PROD_v5`

---

## Repository Structure

```
clawbot/
├── bot.py                  # Main trading bot (~1,836 lines, single-file)
├── requirements.txt        # Python dependencies
└── function-bun/           # TypeScript/Bun order gateway service
    ├── index.ts            # Hono HTTP server (~192 lines)
    └── package.json        # Bun project config
```

### Why single-file?
`bot.py` is intentionally monolithic for Railway deployability — no build step, no module resolution issues in production. Resist the urge to refactor it into multiple files without a concrete operational reason.

---

## Python Bot (`bot.py`)

### Key Sections & Line Ranges

| Section | Lines | Purpose |
|---|---|---|
| JSON Logging | 65–92 | `_JsonFormatter`, `jlog()` — structured logging |
| Config / Env | 136–250 | `env_str/bool/int/float()` helpers, all settings |
| Telegram | 255–271 | Alert sender for critical events |
| Database Layer | 276–441 | Postgres connection, schema, CRUD helpers |
| Adaptive Edge | 513–552 | Dynamic `MIN_EDGE_CENTS` based on win/loss streaks |
| Circuit Breaker | 556–700 | Safety halt logic (daily loss, trade count, errors) |
| Positions | 704–771 | `PositionSnapshot` dataclass + in-memory cache |
| Kalshi REST Client | 777–947 | RSA-PSS auth, all API methods, retry logic |
| Health Server | 952–971 | Lightweight HTTP on port 3000 (`/health`) |
| Market Selection | 974–1133 | Pick best market, parse orderbook, book quality checks |
| Pricing Model | 1137–1177 | `model_fair_yes()` — CDF-based fair price |
| Position Sizing | 1182–1193 | 10% of balance, triple-capped |
| Bot State | 1198–1242 | `BotState` dataclass, JSON persistence |
| Entry Logic | 1281–1421 | `maybe_enter()` — 13-step decision tree |
| Exit Logic | 1426–1564 | `maybe_exit()` — edge collapse, hold-to-expiry, loss cut |
| Reconciliation | 1569–1590 | Boot-time sync with Kalshi open positions |
| Main Loop | 1596–1832 | `main()` — poll loop, graceful shutdown |

### Entry Decision Tree (`maybe_enter()`)
The function enforces 13 guards in sequence. Breaking any guard skips the trade:
1. No pyramiding (one position at a time)
2. Kill switch not active
3. ONE_TRADE_PER_BUCKET (DB-enforced idempotency per market bucket)
4. Cooldown between trades (30s)
5. Time window: only 3–10 min before expiry
6. Book quality: spread, depth, bid price bounds
7. Adaptive edge: fair price vs. entry price >= `MIN_EDGE_CENTS` (8¢ base)
8. Slippage: entry price within tolerance of fair
9. Min contracts: must fill >= 5 contracts
10. Idempotency: skip if trade already submitted
11. Circuit breaker: not halted
12. Persist trade record (CREATED status in DB)
13. Submit order → update position → record stats

### Exit Decision Tree (`maybe_exit()`)
Exit triggers (checked in order):
- **Kill switch**: Force exit
- **Edge collapse**: Mispricing < `EXIT_EDGE_CENTS` (2¢) — exit early
- **Hold to expiry**: <90s left AND in profit — hold for $1 settlement
- **Loss cut**: <90s left AND losing — exit to limit damage

### Pricing Model
`model_fair_yes()` computes the probability that BTC spot > strike at expiry using:
- Standard Normal CDF (Black-Scholes-inspired)
- Inputs: current spot, strike price, time to expiry (seconds), annualized realized volatility
- Volatility: 30-sample rolling window of 5-minute Kraken price observations

### Adaptive Edge Threshold
- Base: 8¢ minimum mispricing
- Range: 5¢–15¢
- Tightens on losses, loosens on wins
- Resets daily at midnight

---

## TypeScript Gateway (`function-bun/index.ts`)

A Bun HTTP service that acts as a secure proxy to Kalshi's API. The Python bot calls this instead of Kalshi directly when `KALSHI_ORDER_GATEWAY_URL` is set.

### Endpoints
- `GET /` — Health check, shows credential status
- `GET /health` — Proxy health check to Kalshi API
- `POST /order` — Submits a signed order to Kalshi
  - Required fields: `ticker`, `side` (YES/NO), `count`/`contracts`, `yes_price` or `no_price`
  - Optional: `action` (buy/sell), `client_order_id`

### Private Key Resolution (priority order)
1. `KALSHI_PRIVATE_KEY_PEM` env var (inline PEM)
2. `KALSHI_PRIVATE_KEY_PATH` as file path
3. `KALSHI_PRIVATE_KEY_PATH` as base64-encoded PEM

### Running the Gateway
```bash
cd function-bun
bun install
bun run index.ts
```

---

## Environment Variables

### Required for Live Trading
| Variable | Description |
|---|---|
| `KALSHI_API_KEY_ID` | RSA public key ID for Kalshi auth |
| `KALSHI_PRIVATE_KEY_PEM` | Private key inline (PEM format) |
| `DATABASE_URL` | PostgreSQL connection string |
| `LIVE_MODE` | Set to `true` to use real money |

### Optional / Defaults
| Variable | Default | Description |
|---|---|---|
| `KALSHI_PRIVATE_KEY_PATH` | — | Alt: file path or base64 key |
| `KALSHI_ORDER_GATEWAY_URL` | — | Use `function-bun` as proxy |
| `SERIES_TICKER` | `KXBTC15M` | Kalshi market series |
| `KRAKEN_TICKER_URL` | Kraken public API | BTC spot price feed |
| `KILL_SWITCH` | `false` | Stop new entries |
| `CLOSE_ON_KILL_SWITCH` | `false` | Exit open position on kill |
| `POLL_SECONDS` | `10` | Main loop interval |
| `STATE_FILE` | `.runtime/state.json` | Bot state persistence |
| `STATUS_FILE` | `.runtime/status.json` | Status file path |
| `TELEGRAM_BOT_TOKEN` | — | Telegram bot alerts |
| `TELEGRAM_CHAT_ID` | — | Telegram destination |

**No secrets are hardcoded.** All credentials come from environment. When `LIVE_MODE=false`, orders are simulated (paper trading mode).

---

## Database Schema

Five PostgreSQL tables managed by `db_migrate()` (auto-runs at boot):

| Table | Purpose |
|---|---|
| `j5_trades` | Trade log with idempotency keys and status |
| `j5_circuit_breaker` | Safety halt state (daily loss, trade count) |
| `j5_positions` | Current open positions (survives restarts) |
| `j5_bucket_state` | ONE_TRADE_PER_BUCKET enforcement |
| `j5_daily_stats` | Per-day performance tracking |

### Manual Operations
```sql
-- Reset circuit breaker halt
UPDATE j5_circuit_breaker SET halted=false WHERE id=1;

-- View today's trades
SELECT * FROM j5_trades ORDER BY created_at DESC LIMIT 20;

-- Check open positions
SELECT * FROM j5_positions;
```

---

## Logging

All logs are JSON-structured. Use `jlog()` for any new log statements:
```python
jlog("event_name", level="info", ticker="KXBTC15M-26MAR05-50000", edge=0.09)
```
Do not use `print()` or `logging.info()` directly. All logs go through the `j5` logger with the `_JsonFormatter`.

**Log levels used**:
- `info` — normal operation
- `warning` — degraded but continuing
- `error` — action needed, but bot continues
- `critical` — halt / circuit breaker triggered

---

## Safety Mechanisms

| Mechanism | Threshold | Purpose |
|---|---|---|
| Circuit breaker: daily loss | $10 | Halt trading if losing badly |
| Circuit breaker: trade count | 30/day | Prevent overtrading |
| Circuit breaker: order rate | 15/hour | Rate limiting |
| Circuit breaker: error streak | 5 consecutive | Halt on API/system failures |
| ONE_TRADE_PER_BUCKET | 1 per bucket | No re-entry in same market bucket |
| Min contracts | 5 | Skip trades with too little size |
| Adaptive edge floor | 5¢ | Never trade on tiny edges |

**Do not weaken these thresholds** without a clear data-driven reason. The circuit breaker thresholds and bucket guard were set based on real trading incidents (see comments in `bot.py` around lines 100–130).

---

## Thread Safety

The bot uses threading locks for all shared state:
- `_db_lock` — database connection pool
- `_bucket_lock` — bucket state cache
- `_pos_lock` — position cache
- `_cb_lock` — circuit breaker state

Always acquire the appropriate lock when reading or writing these structures.

---

## Development Conventions

### Python Style
- Standard Python 3.x; no type annotations currently
- `snake_case` for functions and variables
- Constants in `UPPER_SNAKE_CASE` at the top of the file
- Inline comments on guard conditions (label each guard clearly)

### Adding New Guards to `maybe_enter()`/`maybe_exit()`
1. Add the check in the appropriate numbered position
2. Log the skip reason with `jlog("guard_name_skip", ...)` at `info` or `warning` level
3. Return early (`return`) to abort the trade

### Adding New Environment Variables
1. Add a constant using `env_str()`/`env_bool()`/`env_int()`/`env_float()` in the Config section (lines 136–250)
2. Add the variable to the table in this CLAUDE.md
3. Provide a sensible default — the bot must start in paper mode without any config

### Database Changes
1. Add migration SQL to `db_migrate()` using `IF NOT EXISTS` or `ADD COLUMN IF NOT EXISTS`
2. Never drop columns or tables in migrations (data is valuable for debugging)
3. Migrations run automatically at boot — they must be idempotent

### No Test Suite
There are currently no automated tests. Manual testing workflow:
1. Set `LIVE_MODE=false` (paper trading)
2. Set `DATABASE_URL` to a dev Postgres instance
3. Run `python bot.py` and observe JSON logs
4. Validate behavior in paper mode before setting `LIVE_MODE=true`

---

## Operational Workflows

### Start the Bot
```bash
# Install dependencies
pip install -r requirements.txt

# Set required env vars (see above)
export DATABASE_URL="postgres://..."
export KALSHI_API_KEY_ID="..."
export KALSHI_PRIVATE_KEY_PEM="-----BEGIN RSA PRIVATE KEY-----..."
export LIVE_MODE=false  # paper trading

python bot.py
```

### Emergency Stop
```bash
# Via environment (takes effect next poll cycle)
export KILL_SWITCH=true

# Via database (immediate, persistent across restarts)
UPDATE j5_circuit_breaker SET halted=true WHERE id=1;
```

### Deploy
Push to `master` — Railway auto-deploys. Confirm the health endpoint responds:
```
GET https://<your-railway-url>/health
```

### Start the Order Gateway
```bash
cd function-bun
bun install
bun run index.ts
# Listens on PORT (default 3000)
```

---

## Known Design Decisions & Context

- **2026-03-04 incident**: Analysis of 106 trades across 27 buckets revealed 23/27 had multiple entries per bucket (model churn). The `ONE_TRADE_PER_BUCKET` guard and `j5_bucket_state` table were added directly in response. See the header comment block in `bot.py`.
- **Position sizing at 10%**: Increased from 6% after data showed larger positions were needed for meaningful PnL. The triple cap prevents any single trade from exceeding limits.
- **Hold-to-expiry logic**: Kalshi binary options settle at $1 (100¢). If a position is in-the-money with <90s left, it's often better to hold than pay exit slippage.
- **function-bun gateway**: Decouples RSA signing from the Python bot. Allows rotating auth credentials without touching bot logic. Optional — bot can sign directly if `KALSHI_ORDER_GATEWAY_URL` is not set.

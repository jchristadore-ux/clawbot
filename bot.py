import os
import time
from datetime import datetime, timezone, timedelta, date

import requests
import psycopg2
from psycopg2.extras import RealDictCursor

# =========================
# CONFIG
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")  # set by Railway Postgres

START_BALANCE = float(os.getenv("START_BALANCE", "1000"))
TRADE_SIZE = float(os.getenv("TRADE_SIZE", "25"))
FEE_PER_TRADE = float(os.getenv("FEE_PER_TRADE", "0"))

LOOKBACK = int(os.getenv("LOOKBACK", "5"))
UP_THRESHOLD = float(os.getenv("UP_THRESHOLD", "1.002"))     # +0.2%
DOWN_THRESHOLD = float(os.getenv("DOWN_THRESHOLD", "0.998"))  # -0.2%

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "20"))

# Day 2 Risk Controls (A + B + trade cap)
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "20"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "8"))

# Kraken
KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"
KRAKEN_PAIR = "XBTUSD"
KRAKEN_INTERVAL = 5  # minutes


# =========================
# UTIL
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_today_date() -> date:
    return datetime.now(timezone.utc).date()


def db_conn():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. Add Railway Postgres and ensure bot service has DATABASE_URL."
        )
    return psycopg2.connect(DATABASE_URL)


def _as_date(value) -> date | None:
    """Normalize DB date-ish values (date or string) to a date."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        # value could be 'YYYY-MM-DD'
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return None


def init_db():
    """Create tables if they don't exist, and evolve schema for Day 2 controls."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Base tables
            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS paper_state (
              id INTEGER PRIMARY KEY,
              balance DOUBLE PRECISION NOT NULL,
              position TEXT NULL,
              entry_price DOUBLE PRECISION NULL,
              stake DOUBLE PRECISION NOT NULL,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
            )
            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS paper_trades (
              trade_id BIGSERIAL PRIMARY KEY,
              ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              action TEXT NOT NULL,
              side TEXT NULL,
              price DOUBLE PRECISION NULL,
              stake DOUBLE PRECISION NULL,
              fee DOUBLE PRECISION NULL,
              pnl DOUBLE PRECISION NULL
            );
            """
            )

            # Day 2 columns (safe to run repeatedly)
            cur.execute(
                "ALTER TABLE paper_state ADD COLUMN IF NOT EXISTS last_trade_ts TIMESTAMPTZ;"
            )
            cur.execute(
                "ALTER TABLE paper_state ADD COLUMN IF NOT EXISTS last_trade_day DATE;"
            )
            cur.execute(
                "ALTER TABLE paper_state ADD COLUMN IF NOT EXISTS trades_today INTEGER NOT NULL DEFAULT 0;"
            )
            cur.execute(
                "ALTER TABLE paper_state ADD COLUMN IF NOT EXISTS realized_pnl_today DOUBLE PRECISION NOT NULL DEFAULT 0;"
            )

        conn.commit()


def load_state():
    """Load single-row state (id=1). If missing, create it."""
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM paper_state WHERE id=1;")
            row = cur.fetchone()

            if row:
                return row

            cur.execute(
                """
            INSERT INTO paper_state (
                id, balance, position, entry_price, stake,
                last_trade_ts, last_trade_day, trades_today, realized_pnl_today
            )
            VALUES (1, %s, NULL, NULL, 0.0, NULL, NULL, 0, 0.0)
            RETURNING *;
            """,
                (START_BALANCE,),
            )
            row = cur.fetchone()
        conn.commit()
    return row


def save_state(
    balance,
    position,
    entry_price,
    stake,
    last_trade_ts,
    last_trade_day,
    trades_today,
    realized_pnl_today,
):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
            UPDATE paper_state
            SET balance=%s,
                position=%s,
                entry_price=%s,
                stake=%s,
                last_trade_ts=%s,
                last_trade_day=%s,
                trades_today=%s,
                realized_pnl_today=%s,
                updated_at=NOW()
            WHERE id=1;
            """,
                (
                    balance,
                    position,
                    entry_price,
                    stake,
                    last_trade_ts,
                    last_trade_day,
                    trades_today,
                    realized_pnl_today,
                ),
            )
        conn.commit()


def log_trade(action, side=None, price=None, stake=None, fee=None, pnl=None):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
            INSERT INTO paper_trades (action, side, price, stake, fee, pnl)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
                (action, side, price, stake, fee, pnl),
            )
        conn.commit()


def reset_daily_counters_if_needed(state: dict) -> dict:
    """
    Reset trades_today and realized_pnl_today when UTC day changes.
    We track the day using paper_state.last_trade_day.
    """
    today = utc_today_date()
    last_day = _as_date(state.get("last_trade_day"))

    if last_day is None or last_day != today:
        state["last_trade_day"] = today
        state["trades_today"] = 0
        state["realized_pnl_today"] = 0.0

    return state


def cooldown_active(state: dict, cooldown_minutes: int) -> bool:
    ts = state.get("last_trade_ts")
    if ts is None:
        return False
    # psycopg2 returns tz-aware datetime; compare in UTC
    now = datetime.now(timezone.utc)
    return now < (ts + timedelta(minutes=cooldown_minutes))


# =========================
# DATA
# =========================
def fetch_btc_closes_5m(lookback: int):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(
                KRAKEN_OHLC_URL,
                params={"pair": KRAKEN_PAIR, "interval": KRAKEN_INTERVAL},
                timeout=TIMEOUT_SEC,
            )

            if r.status_code != 200:
                last_error = f"HTTP {r.status_code}: {r.text[:200]}"
                time.sleep(1.0 * attempt)
                continue

            data = r.json()
            if data.get("error"):
                last_error = f"Kraken error: {data['error']}"
                time.sleep(1.0 * attempt)
                continue

            result = data.get("result", {})
            pair_key = next((k for k in result.keys() if k != "last"), None)
            candles = result.get(pair_key, [])

            if not isinstance(candles, list) or len(candles) < lookback:
                last_error = (
                    "Not enough candles or bad shape. "
                    f"candles_len={len(candles) if isinstance(candles, list) else 'n/a'}"
                )
                time.sleep(1.0 * attempt)
                continue

            closes = [float(c[4]) for c in candles[-lookback:]]
            return closes

        except Exception as e:
            last_error = str(e)
            time.sleep(1.0 * attempt)

    print(f"{utc_now_iso()} | WARN | Price fetch failed after retries: {last_error}")
    return None


# =========================
# STRATEGY
# =========================
def decide(closes):
    avg = sum(closes) / len(closes)
    last = closes[-1]
    if last > avg * UP_THRESHOLD:
        return "YES"
    if last < avg * DOWN_THRESHOLD:
        return "NO"
    return "HOLD"


def calc_pnl(entry_price: float, exit_price: float, side: str, stake: float) -> float:
    if entry_price <= 0:
        return 0.0
    pct_move = (exit_price - entry_price) / entry_price
    direction = 1.0 if side == "YES" else -1.0
    return stake * pct_move * direction


def paper_trade(
    balance,
    position,
    entry_price,
    stake,
    signal,
    current_price,
    last_trade_ts,
    last_trade_day,
    trades_today,
    realized_pnl_today,
):
    """
    Executes a paper trade step. Updates daily counters ONLY on ENTER/EXIT.
    """
    if signal == "HOLD":
        return (
            balance,
            position,
            entry_price,
            stake,
            "HOLD",
            last_trade_ts,
            last_trade_day,
            trades_today,
            realized_pnl_today,
        )

    now_ts = datetime.now(timezone.utc)
    today = utc_today_date()

    # Enter if flat
    if position is None:
        if balance < (TRADE_SIZE + FEE_PER_TRADE):
            return (
                balance,
                position,
                entry_price,
                stake,
                "SKIP_INSUFFICIENT_BALANCE",
                last_trade_ts,
                last_trade_day,
                trades_today,
                realized_pnl_today,
            )

        balance -= (TRADE_SIZE + FEE_PER_TRADE)
        position = signal
        entry_price = current_price
        stake = TRADE_SIZE

        log_trade("ENTER", side=position, price=current_price, stake=stake, fee=FEE_PER_TRADE)

        # Day 2 tracking
        trades_today += 1
        last_trade_ts = now_ts
        last_trade_day = today

        return (
            balance,
            position,
            entry_price,
            stake,
            f"ENTER_{signal}",
            last_trade_ts,
            last_trade_day,
            trades_today,
            realized_pnl_today,
        )

    # Exit if opposite signal
    if signal != position:
        pnl = calc_pnl(entry_price, current_price, position, stake)
        balance += (stake + pnl - FEE_PER_TRADE)

        log_trade(
            "EXIT",
            side=position,
            price=current_price,
            stake=stake,
            fee=FEE_PER_TRADE,
            pnl=pnl,
        )

        # Day 2 tracking
        realized_pnl_today += pnl
        trades_today += 1
        last_trade_ts = now_ts
        last_trade_day = today

        position = None
        entry_price = None
        stake = 0.0

        return (
            balance,
            position,
            entry_price,
            stake,
            "EXIT",
            last_trade_ts,
            last_trade_day,
            trades_today,
            realized_pnl_today,
        )

    return (
        balance,
        position,
        entry_price,
        stake,
        "HOLD_SAME_SIDE",
        last_trade_ts,
        last_trade_day,
        trades_today,
        realized_pnl_today,
    )


# =========================
# MAIN
# =========================
def main():
    init_db()
    state = load_state()
    state = reset_daily_counters_if_needed(state)

    closes = fetch_btc_closes_5m(LOOKBACK)
    if closes is None:
        print(f"{utc_now_iso()} | INFO | No data this run. balance={float(state['balance']):.2f} pos={state['position']}")
        return

    signal = decide(closes)
    current_price = closes[-1]

    # Pull state
    balance = float(state["balance"])
    position = state["position"]
    entry_price = float(state["entry_price"]) if state["entry_price"] is not None else None
    stake = float(state["stake"])

    last_trade_ts = state.get("last_trade_ts")
    last_trade_day = _as_date(state.get("last_trade_day"))
    trades_today = int(state.get("trades_today", 0))
    realized_pnl_today = float(state.get("realized_pnl_today", 0.0))

    # Gate trading if we would take an action (YES/NO), with Day 2 risk controls
    blocked_reason = None
    if signal in ("YES", "NO"):
        if realized_pnl_today <= -MAX_DAILY_LOSS:
            blocked_reason = f"MAX_DAILY_LOSS hit (pnl_today={realized_pnl_today:.2f} <= -{MAX_DAILY_LOSS})"
        elif trades_today >= MAX_TRADES_PER_DAY:
            blocked_reason = f"MAX_TRADES_PER_DAY hit (trades_today={trades_today} >= {MAX_TRADES_PER_DAY})"
        elif cooldown_active(state, COOLDOWN_MINUTES):
            blocked_reason = f"COOLDOWN active ({COOLDOWN_MINUTES}m since last trade)"

    if blocked_reason:
        save_state(
            balance=balance,
            position=position,
            entry_price=entry_price,
            stake=stake,
            last_trade_ts=last_trade_ts,
            last_trade_day=last_trade_day,
            trades_today=trades_today,
            realized_pnl_today=realized_pnl_today,
        )
        print(
            f"{utc_now_iso()} | price={current_price:.2f} | "
            f"signal={signal} | action=BLOCKED | reason={blocked_reason} | "
            f"balance={balance:.2f} | pos={position} | entry={entry_price}"
        )
        return

    (
        balance,
        position,
        entry_price,
        stake,
        action,
        last_trade_ts,
        last_trade_day,
        trades_today,
        realized_pnl_today,
    ) = paper_trade(
        balance,
        position,
        entry_price,
        stake,
        signal,
        current_price,
        last_trade_ts,
        last_trade_day,
        trades_today,
        realized_pnl_today,
    )

    save_state(
        balance=balance,
        position=position,
        entry_price=entry_price,
        stake=stake,
        last_trade_ts=last_trade_ts,
        last_trade_day=last_trade_day,
        trades_today=trades_today,
        realized_pnl_today=realized_pnl_today,
    )

    print(
        f"{utc_now_iso()} | price={current_price:.2f} | "
        f"signal={signal} | action={action} | "
        f"balance={balance:.2f} | pos={position} | entry={entry_price} | "
        f"trades_today={trades_today} | pnl_today={realized_pnl_today:.2f}"
    )


if __name__ == "__main__":
    main()

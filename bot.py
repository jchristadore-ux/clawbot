import os
import time
from datetime import datetime, timezone

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

# Kraken
KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"
KRAKEN_PAIR = "XBTUSD"
KRAKEN_INTERVAL = 5  # minutes


# =========================
# UTIL
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set. Add Railway Postgres and ensure bot service has DATABASE_URL.")
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """Create tables if they don't exist."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS paper_state (
              id INTEGER PRIMARY KEY,
              balance DOUBLE PRECISION NOT NULL,
              position TEXT NULL,
              entry_price DOUBLE PRECISION NULL,
              stake DOUBLE PRECISION NOT NULL,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            cur.execute("""
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
            """)
        conn.commit()


def load_state():
    """Load single-row state (id=1). If missing, create it."""
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM paper_state WHERE id=1;")
            row = cur.fetchone()

            if row:
                return row

            cur.execute("""
            INSERT INTO paper_state (id, balance, position, entry_price, stake)
            VALUES (1, %s, NULL, NULL, 0.0)
            RETURNING *;
            """, (START_BALANCE,))
            row = cur.fetchone()
        conn.commit()
    return row


def save_state(balance, position, entry_price, stake):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE paper_state
            SET balance=%s, position=%s, entry_price=%s, stake=%s, updated_at=NOW()
            WHERE id=1;
            """, (balance, position, entry_price, stake))
        conn.commit()


def log_trade(action, side=None, price=None, stake=None, fee=None, pnl=None):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO paper_trades (action, side, price, stake, fee, pnl)
            VALUES (%s, %s, %s, %s, %s, %s);
            """, (action, side, price, stake, fee, pnl))
        conn.commit()


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
                last_error = f"Not enough candles or bad shape. candles_len={len(candles) if isinstance(candles, list) else 'n/a'}"
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


def paper_trade(balance, position, entry_price, stake, signal, current_price):
    if signal == "HOLD":
        return balance, position, entry_price, stake, "HOLD"

    # Enter if flat
    if position is None:
        if balance < (TRADE_SIZE + FEE_PER_TRADE):
            return balance, position, entry_price, stake, "SKIP_INSUFFICIENT_BALANCE"

        balance -= (TRADE_SIZE + FEE_PER_TRADE)
        position = signal
        entry_price = current_price
        stake = TRADE_SIZE
        log_trade("ENTER", side=position, price=current_price, stake=stake, fee=FEE_PER_TRADE)
        return balance, position, entry_price, stake, f"ENTER_{signal}"

    # Exit if opposite signal
    if signal != position:
        pnl = calc_pnl(entry_price, current_price, position, stake)
        balance += (stake + pnl - FEE_PER_TRADE)
        log_trade("EXIT", side=position, price=current_price, stake=stake, fee=FEE_PER_TRADE, pnl=pnl)

        position = None
        entry_price = None
        stake = 0.0
        return balance, position, entry_price, stake, "EXIT"

    return balance, position, entry_price, stake, "HOLD_SAME_SIDE"


# =========================
# MAIN
# =========================
def main():
    init_db()
    state = load_state()

    closes = fetch_btc_closes_5m(LOOKBACK)
    if closes is None:
        print(f"{utc_now_iso()} | INFO | No data this run. balance={state['balance']:.2f} pos={state['position']}")
        return

    signal = decide(closes)
    current_price = closes[-1]

    balance = float(state["balance"])
    position = state["position"]
    entry_price = float(state["entry_price"]) if state["entry_price"] is not None else None
    stake = float(state["stake"])

    balance, position, entry_price, stake, action = paper_trade(
        balance, position, entry_price, stake, signal, current_price
    )

    save_state(balance, position, entry_price, stake)

    print(
        f"{utc_now_iso()} | price={current_price:.2f} | "
        f"signal={signal} | action={action} | "
        f"balance={balance:.2f} | pos={position} | entry={entry_price}"
    )


if __name__ == "__main__":
    main()

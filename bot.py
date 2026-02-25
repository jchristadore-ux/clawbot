# bot.py
import os
import time
import json
from datetime import datetime, timezone, timedelta, date
from typing import Optional, Tuple, Dict, Any, List

import requests
import psycopg2
from psycopg2.extras import RealDictCursor

# =========================
# CONFIG
# =========================
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# Run modes:
#   DRY_RUN: compute + log only, no paper trades, no live orders
#   PAPER:   paper trades persisted in Postgres
#   LIVE:    live orders ONLY when LIVE_TRADING_ENABLED=true (and KILL_SWITCH=false)
RUN_MODE = os.getenv("RUN_MODE", "DRY_RUN").strip().upper()
if RUN_MODE not in ("DRY_RUN", "PAPER", "LIVE"):
    RUN_MODE = "DRY_RUN"

LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING_ENABLED", "false").lower() == "true"

# Threshold pack uses LIVE_MODE (true in LIVE run_mode, or if env LIVE_MODE=true)
LIVE_MODE_ENV = os.getenv("LIVE_MODE", "false").lower() == "true"
LIVE_MODE = True if RUN_MODE == "LIVE" else LIVE_MODE_ENV

KILL_SWITCH = os.getenv("KILL_SWITCH", "false").lower() == "true"

# Maintenance modes
STATS_ONLY = os.getenv("STATS_ONLY", "false").lower() == "true"
RESET_STATE = os.getenv("RESET_STATE", "false").lower() == "true"
RESET_DAILY = os.getenv("RESET_DAILY", "false").lower() == "true"

# One-shot-ish reset debounce (minutes)
RESET_DEBOUNCE_MINUTES = int(os.getenv("RESET_DEBOUNCE_MINUTES", "30"))

# NEW: DB run-lock + duplicate-slug early skip
ENABLE_RUN_LOCK = os.getenv("ENABLE_RUN_LOCK", "true").lower() == "true"
RUN_LOCK_KEY = int(os.getenv("RUN_LOCK_KEY", "9341501"))  # any stable int works
DUPLICATE_SLUG_SKIP = os.getenv("DUPLICATE_SLUG_SKIP", "true").lower() == "true"

# Paper defaults
START_BALANCE = float(os.getenv("START_BALANCE", "1000"))
TRADE_SIZE = float(os.getenv("TRADE_SIZE", "25"))

FEE_PER_TRADE = float(os.getenv("FEE_PER_TRADE", os.getenv("FEE_ENTERED", "0")))

# Synthetic model (Kraken)
LOOKBACK = int(os.getenv("LOOKBACK", "5"))
PROB_MODE = os.getenv("PROB_MODE", "true").lower() == "true"
PROB_P_MIN = float(os.getenv("PROB_P_MIN", "0.05"))
PROB_P_MAX = float(os.getenv("PROB_P_MAX", "0.95"))
PROB_Z_SCALE = float(os.getenv("PROB_Z_SCALE", "2.5"))

# Thresholds
EDGE_ENTER = float(os.getenv("EDGE_ENTER", os.getenv("UP_THRESHOLD", "0.08")))
EDGE_EXIT = float(os.getenv("EDGE_EXIT", "0.04"))

DOWN_THRESHOLD = os.getenv("DOWN_THRESHOLD", "").strip()
DOWN_EDGE_ENTER = None
try:
    if DOWN_THRESHOLD:
        DOWN_EDGE_ENTER = float(DOWN_THRESHOLD)
except Exception:
    DOWN_EDGE_ENTER = None

# Risk controls (paper defaults)
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "20"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "8"))

# LIVE mode overrides
LIVE_TRADE_SIZE = float(os.getenv("LIVE_TRADE_SIZE", "5"))
LIVE_MAX_TRADES_PER_DAY = int(os.getenv("LIVE_MAX_TRADES_PER_DAY", "10"))
LIVE_MAX_DAILY_LOSS = float(os.getenv("LIVE_MAX_DAILY_LOSS", "15"))
LIVE_COOLDOWN_MINUTES = int(os.getenv("LIVE_COOLDOWN_MINUTES", "20"))

LIVE_MIN_EDGE_ENTER = float(os.getenv("LIVE_MIN_EDGE_ENTER", "0.10"))
LIVE_MIN_EDGE_EXIT = float(os.getenv("LIVE_MIN_EDGE_EXIT", "0.04"))

LIVE_MIN_MARK_SUM = float(os.getenv("LIVE_MIN_MARK_SUM", "0.85"))
LIVE_MAX_MARK_SUM = float(os.getenv("LIVE_MAX_MARK_SUM", "1.15"))
LIVE_MIN_PRICE = float(os.getenv("LIVE_MIN_PRICE", "0.03"))
LIVE_MAX_PRICE = float(os.getenv("LIVE_MAX_PRICE", "0.97"))

LIVE_DAILY_PROFIT_LOCK = float(os.getenv("LIVE_DAILY_PROFIT_LOCK", os.getenv("MAX_DAILY_PROFIT_LOCK", "0")))
LIVE_SLIPPAGE = float(os.getenv("LIVE_SLIPPAGE", "0.02"))

# Whale override
WHALE_COOLDOWN_OVERRIDE = os.getenv("WHALE_COOLDOWN_OVERRIDE", "true").lower() == "true"
WHALE_EDGE_OVERRIDE = float(os.getenv("WHALE_EDGE_OVERRIDE", os.getenv("WHALE_EDGE", "0.20")))
ALLOW_WHALE_AFTER_CAP = os.getenv("ALLOW_WHALE_AFTER_CAP", "false").lower() == "true"

# HTTP
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "20"))

# Polymarket
POLY_TIMEOUT_SEC = int(os.getenv("POLY_TIMEOUT_SEC", "20"))
DEBUG_POLY = os.getenv("DEBUG_POLY", "false").lower() == "true"

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_PRICE_URL = "https://clob.polymarket.com/price"

# Optional snapshots/drawdown
ENABLE_SNAPSHOTS = os.getenv("ENABLE_SNAPSHOTS", "false").lower() == "true"
ENABLE_DRAWDOWN = os.getenv("ENABLE_DRAWDOWN", "false").lower() == "true"

# Kraken OHLC
KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"
KRAKEN_PAIR = "XBTUSD"
KRAKEN_INTERVAL = 5

POLY_EVENT_SLUG_OVERRIDE = os.getenv("POLY_EVENT_SLUG", "").strip()


# =========================
# UTIL
# =========================
def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)

def utc_now_iso() -> str:
    return utc_now_dt().strftime("%Y-%m-%dT%H:%M:%SZ")

def utc_today_date() -> date:
    return utc_now_dt().date()

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _as_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return None

def _sleep_backoff(attempt: int) -> None:
    time.sleep(1.0 * attempt)

def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def cooldown_active(last_trade_ts, cooldown_minutes: int) -> bool:
    if last_trade_ts is None:
        return False
    try:
        return utc_now_dt() < (last_trade_ts + timedelta(minutes=cooldown_minutes))
    except Exception:
        return False

def current_btc_5m_slug(now: Optional[datetime] = None) -> str:
    if now is None:
        now = utc_now_dt()
    floored = now.replace(second=0, microsecond=0)
    minute = (floored.minute // 5) * 5
    floored = floored.replace(minute=minute)
    ts = int(floored.timestamp())
    return f"btc-updown-5m-{ts}"

def effective_trade_size() -> float:
    return LIVE_TRADE_SIZE if LIVE_MODE else TRADE_SIZE

def effective_edge_enter() -> float:
    return LIVE_MIN_EDGE_ENTER if LIVE_MODE else EDGE_ENTER

def effective_edge_exit() -> float:
    return LIVE_MIN_EDGE_EXIT if LIVE_MODE else EDGE_EXIT

def effective_max_trades_per_day() -> int:
    return LIVE_MAX_TRADES_PER_DAY if LIVE_MODE else MAX_TRADES_PER_DAY

def effective_max_daily_loss() -> float:
    return LIVE_MAX_DAILY_LOSS if LIVE_MODE else MAX_DAILY_LOSS

def effective_cooldown_minutes() -> int:
    return LIVE_COOLDOWN_MINUTES if LIVE_MODE else COOLDOWN_MINUTES

def marks_look_sane_for_live(p_up: float, p_down: float) -> bool:
    if not LIVE_MODE:
        return True
    s = p_up + p_down
    if s < LIVE_MIN_MARK_SUM or s > LIVE_MAX_MARK_SUM:
        return False
    if not (LIVE_MIN_PRICE <= p_up <= LIVE_MAX_PRICE):
        return False
    if not (LIVE_MIN_PRICE <= p_down <= LIVE_MAX_PRICE):
        return False
    return True

def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set.")
    return psycopg2.connect(DATABASE_URL)


# =========================
# DB RUN LOCK
# =========================
def acquire_run_lock(conn) -> bool:
    if not ENABLE_RUN_LOCK:
        return True
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s);", (RUN_LOCK_KEY,))
        ok = bool(cur.fetchone()[0])
        return ok

def release_run_lock(conn) -> None:
    if not ENABLE_RUN_LOCK:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s);", (RUN_LOCK_KEY,))
    conn.commit()


# =========================
# DB: schema + state + trades
# =========================
def init_db():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS paper_state (
              id INTEGER PRIMARY KEY,
              balance DOUBLE PRECISION NOT NULL,
              position TEXT NULL,
              entry_price DOUBLE PRECISION NULL,
              stake DOUBLE PRECISION NOT NULL,

              last_trade_ts TIMESTAMPTZ,
              last_trade_day DATE,
              trades_today INTEGER NOT NULL DEFAULT 0,
              realized_pnl_today DOUBLE PRECISION NOT NULL DEFAULT 0,

              last_mark DOUBLE PRECISION,
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
              pnl DOUBLE PRECISION NULL,
              slug TEXT NULL
            );
            """)

            # migrations
            cur.execute("ALTER TABLE paper_state ADD COLUMN IF NOT EXISTS last_seen_slug TEXT;")
            cur.execute("ALTER TABLE paper_state ADD COLUMN IF NOT EXISTS last_trade_slug TEXT;")
            cur.execute("ALTER TABLE paper_state ADD COLUMN IF NOT EXISTS last_reset_state_ts TIMESTAMPTZ;")
            cur.execute("ALTER TABLE paper_state ADD COLUMN IF NOT EXISTS last_reset_daily_ts TIMESTAMPTZ;")

        conn.commit()

    init_snapshots_table()


def load_state(conn) -> dict:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM paper_state WHERE id=1;")
        row = cur.fetchone()
        if row:
            return row

        cur.execute(
            """
            INSERT INTO paper_state (
                id, balance, position, entry_price, stake,
                last_trade_ts, last_trade_day, trades_today, realized_pnl_today, last_mark,
                last_seen_slug, last_trade_slug,
                last_reset_state_ts, last_reset_daily_ts
            )
            VALUES (1, %s, NULL, NULL, 0.0, NULL, NULL, 0, 0.0, NULL, NULL, NULL, NULL, NULL)
            RETURNING *;
            """,
            (START_BALANCE,),
        )
        row = cur.fetchone()
    conn.commit()
    return row


def save_state(
    conn,
    balance: float,
    position: Optional[str],
    entry_price: Optional[float],
    stake: float,
    last_trade_ts,
    last_trade_day,
    trades_today: int,
    realized_pnl_today: float,
    last_mark: Optional[float],
    last_seen_slug: Optional[str],
    last_trade_slug: Optional[str],
    last_reset_state_ts,
    last_reset_daily_ts,
):
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
                last_mark=%s,
                last_seen_slug=%s,
                last_trade_slug=%s,
                last_reset_state_ts=%s,
                last_reset_daily_ts=%s,
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
                last_mark,
                last_seen_slug,
                last_trade_slug,
                last_reset_state_ts,
                last_reset_daily_ts,
            ),
        )
    conn.commit()


def log_trade(conn, action: str, side=None, price=None, stake=None, fee=None, pnl=None, slug: Optional[str] = None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO paper_trades (action, side, price, stake, fee, pnl, slug)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            (action, side, price, stake, fee, pnl, slug),
        )
    conn.commit()


def reset_daily_counters_if_needed(state: dict) -> dict:
    today = utc_today_date()
    last_day = _as_date(state.get("last_trade_day"))
    if last_day is None or last_day != today:
        state["last_trade_day"] = today
        state["trades_today"] = 0
        state["realized_pnl_today"] = 0.0
    return state


def _ts_recent(ts, minutes: int) -> bool:
    if ts is None:
        return False
    try:
        return utc_now_dt() < (ts + timedelta(minutes=minutes))
    except Exception:
        return False


# =========================
# DATA: Kraken closes
# =========================
def fetch_btc_closes_5m(lookback: int) -> Optional[List[float]]:
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
                _sleep_backoff(attempt)
                continue

            data = r.json()
            if data.get("error"):
                last_error = f"Kraken error: {data['error']}"
                _sleep_backoff(attempt)
                continue

            result = data.get("result", {})
            pair_key = next((k for k in result.keys() if k != "last"), None)
            candles = result.get(pair_key, [])

            if not isinstance(candles, list) or len(candles) < lookback:
                last_error = f"Not enough candles. candles_len={len(candles) if isinstance(candles, list) else 'n/a'}"
                _sleep_backoff(attempt)
                continue

            closes = [float(c[4]) for c in candles[-lookback:]]
            return closes

        except Exception as e:
            last_error = str(e)
            _sleep_backoff(attempt)

    print(f"{utc_now_iso()} | WARN | Price fetch failed after retries: {last_error}", flush=True)
    return None


# =========================
# SYNTHETIC FAIR
# =========================
def prob_from_closes(closes: List[float]) -> float:
    avg = sum(closes) / len(closes)
    last = closes[-1]
    if avg <= 0:
        return 0.5
    dev = (last - avg) / avg
    p = 0.5 + (dev * PROB_Z_SCALE)
    return clamp(p, PROB_P_MIN, PROB_P_MAX)


# =========================
# POLYMARKET HELPERS
# =========================
def _maybe_json_list(x):
    if x is None:
        return None
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        if s.startswith("[") and s.endswith("]"):
            try:
                return json.loads(s)
            except Exception:
                return None
        if "," in s:
            return [p.strip().strip('"').strip("'") for p in s.split(",") if p.strip()]
        return [s]
    return None


def _get_market_by_slug(slug: str) -> Optional[dict]:
    r = requests.get(GAMMA_MARKETS_URL, params={"slug": slug}, timeout=POLY_TIMEOUT_SEC)
    if r.status_code != 200:
        return None
    data = r.json()
    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict):
        return data
    return None


def _get_market_by_id(mid: int) -> Optional[dict]:
    r = requests.get(f"{GAMMA_MARKETS_URL}/{mid}", timeout=POLY_TIMEOUT_SEC)
    if r.status_code != 200:
        return None
    data = r.json()
    return data if isinstance(data, dict) else None


def _get_event_by_slug(slug: str) -> Optional[dict]:
    r = requests.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=POLY_TIMEOUT_SEC)
    if r.status_code != 200:
        return None
    data = r.json()
    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict):
        return data
    return None


def _extract_updown_from_market(m: dict) -> Optional[Dict[str, Any]]:
    outcomes = _maybe_json_list(m.get("outcomes")) or _maybe_json_list(m.get("outcomeNames"))
    token_ids = _maybe_json_list(m.get("clobTokenIds")) or _maybe_json_list(m.get("clobTokenIDs"))
    outcome_prices = _maybe_json_list(m.get("outcomePrices"))

    if not isinstance(outcomes, list) or len(outcomes) < 2:
        return None

    norm = [str(o).strip().lower() for o in outcomes]

    token_map: Dict[str, str] = {}
    if isinstance(token_ids, list) and len(token_ids) >= 2:
        for name, tid in zip(norm, token_ids):
            token_map[name] = str(tid)

    gamma_map: Dict[str, float] = {}
    if isinstance(outcome_prices, list) and len(outcome_prices) >= 2:
        for name, px in zip(norm, outcome_prices):
            f = _safe_float(px)
            if f is not None:
                gamma_map[name] = f

    return {
        "up_tid": token_map.get("up") or token_map.get("yes"),
        "dn_tid": token_map.get("down") or token_map.get("no"),
        "up_gamma": gamma_map.get("up") or gamma_map.get("yes"),
        "dn_gamma": gamma_map.get("down") or gamma_map.get("no"),
    }


def _fetch_clob_price(token_id: str, side: str = "BUY") -> float:
    r = requests.get(
        CLOB_PRICE_URL,
        params={"token_id": token_id, "side": side},
        timeout=POLY_TIMEOUT_SEC,
    )
    if r.status_code != 200:
        raise RuntimeError(f"CLOB price HTTP {r.status_code}: {r.text[:200]}")
    data = r.json()
    if "price" not in data:
        raise RuntimeError(f"CLOB price missing field: {str(data)[:200]}")
    return float(data["price"])


def fetch_polymarket_marks_and_tokens(slug: str) -> Optional[Tuple[float, float, str, str, str]]:
    s = (slug or "").strip()
    if not s:
        return None

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            candidate_markets: list = []

            m = _get_market_by_slug(s)
            if isinstance(m, dict) and m:
                candidate_markets.append(m)

            if not candidate_markets:
                ev = _get_event_by_slug(s)
                if isinstance(ev, dict) and ev:
                    ev_markets = ev.get("markets", [])
                    if isinstance(ev_markets, list):
                        for x in ev_markets:
                            if not isinstance(x, dict):
                                continue
                            mid = x.get("id")
                            if mid is None:
                                continue
                            full = _get_market_by_id(int(mid))
                            candidate_markets.append(full if full else x)

            if DEBUG_POLY:
                print(f"{utc_now_iso()} | DEBUG | poly_slug={s} candidate_markets={len(candidate_markets)}", flush=True)

            for cm in candidate_markets:
                if not isinstance(cm, dict):
                    continue
                info = _extract_updown_from_market(cm)
                if not info:
                    continue

                up_tid = info.get("up_tid")
                dn_tid = info.get("dn_tid")

                if up_tid and dn_tid:
                    try:
                        p_up = _fetch_clob_price(up_tid, side="BUY")
                        p_dn = _fetch_clob_price(dn_tid, side="BUY")
                        if abs((p_up + p_dn) - 1.0) <= 0.15:
                            return clamp(p_up, 0.001, 0.999), clamp(p_dn, 0.001, 0.999), "clob", str(up_tid), str(dn_tid)
                        else:
                            last_error = f"clob sanity failed sum={(p_up+p_dn):.3f}"
                    except Exception as e:
                        last_error = str(e)[:200]

                up_g = info.get("up_gamma")
                dn_g = info.get("dn_gamma")
                if up_g is not None and dn_g is not None and up_tid and dn_tid:
                    return clamp(float(up_g), 0.001, 0.999), clamp(float(dn_g), 0.001, 0.999), "gamma", str(up_tid), str(dn_tid)

            last_error = last_error or "no usable market"
            _sleep_backoff(attempt)

        except Exception as e:
            last_error = str(e)[:200]
            _sleep_backoff(attempt)

    if DEBUG_POLY:
        print(f"{utc_now_iso()} | DEBUG | fetch_polymarket_marks failed: {last_error}", flush=True)
    return None


# =========================
# PNL (paper)
# =========================
def shares_for_stake(stake: float, price: float) -> float:
    return 0.0 if price <= 0 else (stake / price)

def calc_unrealized_pnl(position: Optional[str], entry_price: Optional[float], mark: float, stake: float) -> float:
    if position is None or entry_price is None or stake <= 0:
        return 0.0
    sh = shares_for_stake(stake, entry_price)
    return (sh * mark) - stake

def calc_realized_pnl(entry_price: float, exit_price: float, stake: float) -> float:
    sh = shares_for_stake(stake, entry_price)
    return (sh * exit_price) - stake


def paper_trade_prob(
    conn,
    balance: float,
    position: Optional[str],
    entry_price: Optional[float],
    stake: float,
    signal: str,
    p_up: float,
    p_down: float,
    edge: float,
    edge_exit: float,
    trade_size: float,
    last_trade_ts,
    last_trade_day,
    trades_today: int,
    realized_pnl_today: float,
    slug: str,
) -> Tuple[float, Optional[str], Optional[float], float, str, object, object, int, float]:
    now_ts = utc_now_dt()
    today = utc_today_date()

    if position is None and signal == "HOLD":
        return (balance, position, entry_price, stake, "HOLD",
                last_trade_ts, last_trade_day, trades_today, realized_pnl_today)

    if position is None:
        if balance < (trade_size + FEE_PER_TRADE):
            return (balance, position, entry_price, stake, "SKIP_INSUFFICIENT_BALANCE",
                    last_trade_ts, last_trade_day, trades_today, realized_pnl_today)

        balance -= (trade_size + FEE_PER_TRADE)
        position = signal
        stake = trade_size
        entry_price = p_up if position == "YES" else p_down

        log_trade(conn, "ENTER", side=position, price=entry_price, stake=stake, fee=FEE_PER_TRADE, slug=slug)

        trades_today += 1
        last_trade_ts = now_ts
        last_trade_day = today

        return (balance, position, entry_price, stake, f"ENTER_{signal}",
                last_trade_ts, last_trade_day, trades_today, realized_pnl_today)

    edge_collapse = False
    if position == "YES" and edge < edge_exit:
        edge_collapse = True
    elif position == "NO" and edge > -edge_exit:
        edge_collapse = True

    if (signal != "HOLD" and signal != position) or edge_collapse:
        exit_price = p_up if position == "YES" else p_down
        pnl = calc_realized_pnl(entry_price, exit_price, stake)

        balance += (stake + pnl - FEE_PER_TRADE)
        log_trade(conn, "EXIT", side=position, price=exit_price, stake=stake, fee=FEE_PER_TRADE, pnl=pnl, slug=slug)

        realized_pnl_today += pnl
        trades_today += 1
        last_trade_ts = now_ts
        last_trade_day = today

        position = None
        entry_price = None
        stake = 0.0

        return (balance, position, entry_price, stake, "EXIT",
                last_trade_ts, last_trade_day, trades_today, realized_pnl_today)

    return (balance, position, entry_price, stake, "HOLD_SAME_SIDE",
            last_trade_ts, last_trade_day, trades_today, realized_pnl_today)


# =========================
# Snapshots/drawdown
# =========================
def init_snapshots_table():
    if not ENABLE_SNAPSHOTS:
        return
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS equity_snapshots (
              snapshot_id BIGSERIAL PRIMARY KEY,
              ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              mark DOUBLE PRECISION NOT NULL,
              balance DOUBLE PRECISION NOT NULL,
              position TEXT NULL,
              entry_price DOUBLE PRECISION NULL,
              stake DOUBLE PRECISION NOT NULL,
              unrealized_pnl DOUBLE PRECISION NOT NULL,
              equity DOUBLE PRECISION NOT NULL
            );
            """)
        conn.commit()


# =========================
# MAIN
# =========================
def main():
    init_db()

    with db_conn() as conn:
        conn.autocommit = False

        # EARLY run-lock
        if not acquire_run_lock(conn):
            print(f"{utc_now_iso()} | INFO | run_lock_busy -> exiting (overlapping cron?)", flush=True)
            return

        try:
            state = load_state(conn)
            state = reset_daily_counters_if_needed(state)

            poly_slug = POLY_EVENT_SLUG_OVERRIDE if POLY_EVENT_SLUG_OVERRIDE else current_btc_5m_slug()

            # EARLY duplicate-slug skip (prevents double-fires in same 5-min window)
            last_seen_slug = state.get("last_seen_slug")
            if DUPLICATE_SLUG_SKIP and last_seen_slug and str(last_seen_slug) == str(poly_slug):
                print(
                    f"{utc_now_iso()} | INFO | DUPLICATE_SLUG_SKIP last_seen_slug={last_seen_slug} current_slug={poly_slug}",
                    flush=True
                )
                return

            # Debounced RESET_DAILY
            if RESET_DAILY and not _ts_recent(state.get("last_reset_daily_ts"), RESET_DEBOUNCE_MINUTES):
                state["trades_today"] = 0
                state["realized_pnl_today"] = 0.0
                state["last_trade_day"] = utc_today_date()
                state["last_reset_daily_ts"] = utc_now_dt()
                print(f"{utc_now_iso()} | INFO | RESET_DAILY applied (debounced). Continuing run.", flush=True)

            # Debounced RESET_STATE
            if RESET_STATE and not _ts_recent(state.get("last_reset_state_ts"), RESET_DEBOUNCE_MINUTES):
                state["position"] = None
                state["entry_price"] = None
                state["stake"] = 0.0
                state["last_reset_state_ts"] = utc_now_dt()
                print(f"{utc_now_iso()} | INFO | RESET_STATE applied (debounced). Continuing run.", flush=True)

            closes = fetch_btc_closes_5m(LOOKBACK)
            if closes is None:
                print(f"{utc_now_iso()} | INFO | No data this run.", flush=True)
                return

            # State fields
            balance = float(state["balance"])
            position = state.get("position")
            entry_price = float(state["entry_price"]) if state.get("entry_price") is not None else None
            stake = float(state.get("stake", 0.0))
            last_trade_ts = state.get("last_trade_ts")
            last_trade_day = _as_date(state.get("last_trade_day"))
            trades_today = int(state.get("trades_today", 0))
            realized_pnl_today = float(state.get("realized_pnl_today", 0.0))
            last_trade_slug = state.get("last_trade_slug")

            print(f"{utc_now_iso()} | INFO | run_mode={RUN_MODE} live_mode={LIVE_MODE} live_armed={LIVE_TRADING_ENABLED} poly_slug={poly_slug}", flush=True)

            # Marks: Polymarket preferred, synthetic fallback
            source = "synthetic"
            pm = fetch_polymarket_marks_and_tokens(poly_slug)
            if pm is not None:
                p_up, p_down, source, _, _ = pm
            else:
                p_up = prob_from_closes(closes) if PROB_MODE else 0.5
                p_up = clamp(p_up, PROB_P_MIN, PROB_P_MAX)
                p_down = clamp(1.0 - p_up, 0.001, 0.999)

            fair_up = clamp(prob_from_closes(closes), PROB_P_MIN, PROB_P_MAX)
            edge = fair_up - p_up

            edge_enter = effective_edge_enter()
            down_edge_enter = DOWN_EDGE_ENTER if DOWN_EDGE_ENTER is not None else edge_enter

            # Signal
            if KILL_SWITCH:
                signal = "HOLD"
                reason = "KILL_SWITCH"
            elif LIVE_MODE and not marks_look_sane_for_live(p_up, p_down):
                signal = "HOLD"
                reason = "MARK_SANITY"
            elif LIVE_MODE and LIVE_DAILY_PROFIT_LOCK > 0 and realized_pnl_today >= LIVE_DAILY_PROFIT_LOCK:
                signal = "HOLD"
                reason = "PROFIT_LOCK"
            else:
                reason = ""
                if edge >= edge_enter:
                    signal = "YES"
                elif edge <= -down_edge_enter:
                    signal = "NO"
                else:
                    signal = "HOLD"

            # Early save last_seen_slug to make duplicate-slug skip effective immediately
            state["last_seen_slug"] = poly_slug

            # Risk gates only for ENTER
            blocked_reason = None
            whale = (signal in ("YES", "NO")) and (abs(edge) >= WHALE_EDGE_OVERRIDE)

            traded_this_slug_already = (last_trade_slug is not None and str(last_trade_slug) == str(poly_slug))
            if traded_this_slug_already and signal in ("YES", "NO"):
                blocked_reason = "IDEMPOTENCY"

            if position is None and signal in ("YES", "NO") and not blocked_reason:
                max_daily_loss = effective_max_daily_loss()
                max_trades_day = effective_max_trades_per_day()
                cooldown_min = effective_cooldown_minutes()

                if realized_pnl_today <= -max_daily_loss:
                    blocked_reason = "MAX_DAILY_LOSS"
                elif trades_today >= max_trades_day:
                    if whale and ALLOW_WHALE_AFTER_CAP:
                        blocked_reason = None
                    else:
                        blocked_reason = "MAX_TRADES_PER_DAY"
                else:
                    cd_active = cooldown_active(last_trade_ts, cooldown_min)
                    if cd_active and not (WHALE_COOLDOWN_OVERRIDE and whale):
                        blocked_reason = "COOLDOWN"

            # Save debounced resets + last_seen_slug now
            save_state(
                conn,
                balance=balance,
                position=position,
                entry_price=entry_price,
                stake=stake,
                last_trade_ts=last_trade_ts,
                last_trade_day=last_trade_day,
                trades_today=trades_today,
                realized_pnl_today=realized_pnl_today,
                last_mark=p_up,
                last_seen_slug=state.get("last_seen_slug"),
                last_trade_slug=last_trade_slug,
                last_reset_state_ts=state.get("last_reset_state_ts"),
                last_reset_daily_ts=state.get("last_reset_daily_ts"),
            )

            # uPnL/equity (log)
            if position == "YES":
                unrealized_pnl = calc_unrealized_pnl(position, entry_price, p_up, stake)
            elif position == "NO":
                unrealized_pnl = calc_unrealized_pnl(position, entry_price, p_down, stake)
            else:
                unrealized_pnl = 0.0
            equity = balance + unrealized_pnl

            label_up = "poly_up" if source in ("clob", "gamma") else "mark_up"
            label_dn = "poly_down" if source in ("clob", "gamma") else "mark_down"

            # NO_TRADE path
            if blocked_reason or signal == "HOLD" or RUN_MODE == "DRY_RUN":
                why = blocked_reason or reason or "signal=HOLD"
                print(
                    f"{utc_now_iso()} | {label_up}={p_up:.3f} | {label_dn}={p_down:.3f} | "
                    f"fair_up={fair_up:.3f} | edge={edge:+.3f} | signal={signal} | "
                    f"action=NO_TRADE | reason={why} | balance={balance:.2f} | pos={position} | entry={entry_price} | "
                    f"trades_today={trades_today} | pnl_today(realized)={realized_pnl_today:.2f} | mode={RUN_MODE} | src={source}",
                    flush=True
                )
                print(f"{utc_now_iso()} | summary | equity={equity:.2f} | uPnL={unrealized_pnl:.2f} | src={source}", flush=True)
                return

            # PAPER trading
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
            ) = paper_trade_prob(
                conn=conn,
                balance=balance,
                position=position,
                entry_price=entry_price,
                stake=stake,
                signal=signal,
                p_up=p_up,
                p_down=p_down,
                edge=edge,
                edge_exit=effective_edge_exit(),
                trade_size=effective_trade_size(),
                last_trade_ts=last_trade_ts,
                last_trade_day=last_trade_day,
                trades_today=trades_today,
                realized_pnl_today=realized_pnl_today,
                slug=poly_slug,
            )

            if action.startswith("ENTER") or action == "EXIT":
                last_trade_slug = poly_slug

            save_state(
                conn,
                balance=balance,
                position=position,
                entry_price=entry_price,
                stake=stake,
                last_trade_ts=last_trade_ts,
                last_trade_day=last_trade_day,
                trades_today=trades_today,
                realized_pnl_today=realized_pnl_today,
                last_mark=p_up,
                last_seen_slug=state.get("last_seen_slug"),
                last_trade_slug=last_trade_slug,
                last_reset_state_ts=state.get("last_reset_state_ts"),
                last_reset_daily_ts=state.get("last_reset_daily_ts"),
            )

            print(
                f"{utc_now_iso()} | {label_up}={p_up:.3f} | {label_dn}={p_down:.3f} | "
                f"fair_up={fair_up:.3f} | edge={edge:+.3f} | signal={signal} | "
                f"action={action} | balance={balance:.2f} | pos={position} | entry={entry_price} | "
                f"trades_today={trades_today} | pnl_today(realized)={realized_pnl_today:.2f} | mode={RUN_MODE} | src={source}",
                flush=True
            )

        finally:
            try:
                release_run_lock(conn)
            except Exception:
                pass


if __name__ == "__main__":
    print("BOOT: bot.py starting", flush=True)
    try:
        main()
        print("BOOT: bot.py finished cleanly", flush=True)
    except Exception as e:
        import traceback
        print("FATAL: bot.py crashed:", repr(e), flush=True)
        traceback.print_exc()
        raise

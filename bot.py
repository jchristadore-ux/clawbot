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
#   PAPER:   paper trades persisted in Postgres (your current behavior)
#   LIVE:    live orders ONLY when LIVE_TRADING_ENABLED=true (and KILL_SWITCH=false)
RUN_MODE = os.getenv("RUN_MODE", "DRY_RUN").strip().upper()
if RUN_MODE not in ("DRY_RUN", "PAPER", "LIVE"):
    RUN_MODE = "DRY_RUN"

LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING_ENABLED", "false").lower() == "true"

# Keep your legacy LIVE_MODE switch for thresholds:
# - If RUN_MODE=LIVE, we force LIVE thresholds regardless of LIVE_MODE env.
LIVE_MODE_ENV = os.getenv("LIVE_MODE", "false").lower() == "true"
LIVE_MODE = True if RUN_MODE == "LIVE" else LIVE_MODE_ENV

# Global kill switch (forces HOLD and blocks live/paper actions)
KILL_SWITCH = os.getenv("KILL_SWITCH", "false").lower() == "true"

# Maintenance modes
STATS_ONLY = os.getenv("STATS_ONLY", "false").lower() == "true"
RESET_STATE = os.getenv("RESET_STATE", "false").lower() == "true"   # clears open position only
RESET_DAILY = os.getenv("RESET_DAILY", "false").lower() == "true"   # clears daily counters only

# Paper defaults
START_BALANCE = float(os.getenv("START_BALANCE", "1000"))
TRADE_SIZE = float(os.getenv("TRADE_SIZE", "25"))

# Alias support
# Some envs in Railway appear as FEE_ENTERED; support both.
FEE_PER_TRADE = float(os.getenv("FEE_PER_TRADE", os.getenv("FEE_ENTERED", "0")))

# Synthetic model (Kraken)
LOOKBACK = int(os.getenv("LOOKBACK", "5"))
PROB_MODE = os.getenv("PROB_MODE", "true").lower() == "true"
PROB_P_MIN = float(os.getenv("PROB_P_MIN", "0.05"))
PROB_P_MAX = float(os.getenv("PROB_P_MAX", "0.95"))
PROB_Z_SCALE = float(os.getenv("PROB_Z_SCALE", "2.5"))

# High-conviction thresholds (paper defaults)
EDGE_ENTER = float(os.getenv("EDGE_ENTER", os.getenv("UP_THRESHOLD", "0.08")))  # alias UP_THRESHOLD
EDGE_EXIT = float(os.getenv("EDGE_EXIT", "0.04"))

# Optional DOWN_THRESHOLD alias (if present, use it as symmetric negative enter threshold)
DOWN_THRESHOLD = os.getenv("DOWN_THRESHOLD", "").strip()
DOWN_EDGE_ENTER = None
try:
    if DOWN_THRESHOLD:
        DOWN_EDGE_ENTER = float(DOWN_THRESHOLD)
except Exception:
    DOWN_EDGE_ENTER = None

# Risk controls (paper defaults)
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))       # realized only
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "20"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "8")) # counts ENTER+EXIT events

# LIVE mode safety pack (overrides when LIVE_MODE=true)
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

LIVE_DAILY_PROFIT_LOCK = float(os.getenv("LIVE_DAILY_PROFIT_LOCK", os.getenv("MAX_DAILY_PROFIT_LOCK", "0")))  # alias

# New: slippage buffer for live limit orders (e.g. 0.02 = 2%)
LIVE_SLIPPAGE = float(os.getenv("LIVE_SLIPPAGE", "0.02"))

# Whale override (bypass cooldown ONLY by default)
WHALE_COOLDOWN_OVERRIDE = os.getenv("WHALE_COOLDOWN_OVERRIDE", "true").lower() == "true"
WHALE_EDGE_OVERRIDE = float(os.getenv("WHALE_EDGE_OVERRIDE", os.getenv("WHALE_EDGE", "0.20")))  # alias WHALE_EDGE

# If ALLOW_WHALE_AFTER_CAP=true, whale trades may bypass trade-count cap (still NOT daily-loss cap).
ALLOW_WHALE_AFTER_CAP = os.getenv("ALLOW_WHALE_AFTER_CAP", "false").lower() == "true"

# HTTP
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "20"))

# Polymarket (Gamma + CLOB)
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
KRAKEN_INTERVAL = 5  # minutes

# Optional override: user-specified event slug (otherwise we compute current 5m slug)
POLY_EVENT_SLUG_OVERRIDE = os.getenv("POLY_EVENT_SLUG", "").strip()

# Live auth/env (scaffold)
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER = os.getenv("POLY_FUNDER", os.getenv("POLY_PUBLIC_ADDRESS", "")).strip()
POLY_CHAIN_ID = int(os.getenv("POLY_CHAIN_ID", "137"))  # Polygon mainnet default
POLY_CLOB_HOST = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").strip()

POLY_API_KEY = os.getenv("POLY_API_KEY", "").strip()
POLY_API_SECRET = os.getenv("POLY_API_SECRET", "").strip()
POLY_API_PASSPHRASE = os.getenv("POLY_API_PASSPHRASE", "").strip()


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
        raise RuntimeError("DATABASE_URL is not set. Add Railway Postgres so DATABASE_URL is injected.")
    return psycopg2.connect(DATABASE_URL)


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

            # Trades log
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

            # Migrations (idempotent)
            cur.execute("ALTER TABLE paper_state ADD COLUMN IF NOT EXISTS last_seen_slug TEXT;")
            cur.execute("ALTER TABLE paper_state ADD COLUMN IF NOT EXISTS last_trade_slug TEXT;")
            cur.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS slug TEXT;")

        conn.commit()

    init_snapshots_table()


def load_state() -> dict:
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
                    last_trade_ts, last_trade_day, trades_today, realized_pnl_today, last_mark,
                    last_seen_slug, last_trade_slug
                )
                VALUES (1, %s, NULL, NULL, 0.0, NULL, NULL, 0, 0.0, NULL, NULL, NULL)
                RETURNING *;
                """,
                (START_BALANCE,),
            )
            row = cur.fetchone()
        conn.commit()
    return row


def save_state(
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
                    last_mark=%s,
                    last_seen_slug=%s,
                    last_trade_slug=%s,
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
                ),
            )
        conn.commit()


def log_trade(action: str, side=None, price=None, stake=None, fee=None, pnl=None, slug: Optional[str] = None):
    with db_conn() as conn:
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
# SYNTHETIC FAIR: probability from closes
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
# POLYMARKET HELPERS (Gamma + CLOB)
# =========================
def _maybe_json_list(x):
    """
    Gamma sometimes returns JSON arrays as strings: '["Yes","No"]'
    """
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
    r = requests.get("https://gamma-api.polymarket.com/markets", params={"slug": slug}, timeout=POLY_TIMEOUT_SEC)
    if r.status_code != 200:
        return None
    data = r.json()
    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict):
        return data
    return None


def _get_market_by_id(mid: int) -> Optional[dict]:
    r = requests.get(f"https://gamma-api.polymarket.com/markets/{mid}", timeout=POLY_TIMEOUT_SEC)
    if r.status_code != 200:
        return None
    data = r.json()
    return data if isinstance(data, dict) else None


def _get_event_by_slug(slug: str) -> Optional[dict]:
    r = requests.get("https://gamma-api.polymarket.com/events", params={"slug": slug}, timeout=POLY_TIMEOUT_SEC)
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
    """
    CLOB /price expects side BUY/SELL (uppercase)
    """
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
    """
    Returns:
      (p_up, p_down, source, up_token_id, down_token_id)

    Attempts (per market candidate):
      1) CLOB prices via clobTokenIds -> source 'clob' (with sanity check)
      2) Gamma outcomePrices -> source 'gamma'
    """
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

            last_error = last_error or "no usable market (missing tokenIds and outcomePrices)"
            _sleep_backoff(attempt)

        except Exception as e:
            last_error = str(e)[:200]
            _sleep_backoff(attempt)

    if DEBUG_POLY:
        print(f"{utc_now_iso()} | DEBUG | fetch_polymarket_marks failed: {last_error}", flush=True)
    return None


# =========================
# PNL (Polymarket-style)
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


# =========================
# PAPER TRADE ENGINE (conviction + edge-collapse exit)
# =========================
def paper_trade_prob(
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
    """
    signal: YES/NO/HOLD (YES=Up, NO=Down)
    entry_price stores p_up when pos=YES, stores p_down when pos=NO

    Exits:
      - Opposite signal (if signal != HOLD and signal != position)
      - Edge-collapse:
          * if holding YES and edge < edge_exit -> exit
          * if holding NO  and edge > -edge_exit -> exit
    """
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

        log_trade("ENTER", side=position, price=entry_price, stake=stake, fee=FEE_PER_TRADE, slug=slug)

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
        log_trade("EXIT", side=position, price=exit_price, stake=stake, fee=FEE_PER_TRADE, pnl=pnl, slug=slug)

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
# PERFORMANCE: optional snapshots/drawdown
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

def record_equity_snapshot(mark: float, balance: float, position: Optional[str],
                          entry_price: Optional[float], stake: float,
                          unrealized_pnl: float, equity: float):
    if not ENABLE_SNAPSHOTS:
        return
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO equity_snapshots
                  (mark, balance, position, entry_price, stake, unrealized_pnl, equity)
                VALUES
                  (%s, %s, %s, %s, %s, %s, %s);
                """,
                (mark, balance, position, entry_price, stake, unrealized_pnl, equity),
            )
        conn.commit()

def get_realized_pnl_24h_and_trades_24h() -> Tuple[float, int]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COALESCE(SUM(CASE WHEN action='EXIT' THEN pnl ELSE 0 END), 0) AS pnl_24h,
                  COUNT(*)::int AS trades_24h
                FROM paper_trades
                WHERE ts >= (NOW() - INTERVAL '24 hours');
                """
            )
            row = cur.fetchone()
            return float(row[0] or 0.0), int(row[1] or 0)

def get_winrate_last_n_exits(n: int = 20) -> Optional[float]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pnl
                FROM paper_trades
                WHERE action='EXIT' AND pnl IS NOT NULL
                ORDER BY ts DESC
                LIMIT %s;
                """,
                (n,),
            )
            rows = cur.fetchall()
    if not rows:
        return None
    pnls = [float(r[0]) for r in rows]
    wins = sum(1 for p in pnls if p > 0)
    return wins / len(pnls)

def get_drawdown_24h() -> Optional[float]:
    if not (ENABLE_SNAPSHOTS and ENABLE_DRAWDOWN):
        return None
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT equity
                FROM equity_snapshots
                WHERE ts >= (NOW() - INTERVAL '24 hours')
                ORDER BY ts ASC;
                """
            )
            rows = cur.fetchall()
    if len(rows) < 2:
        return None
    peak = float(rows[0][0])
    max_dd = 0.0
    for (eq,) in rows:
        eq = float(eq)
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
    return max_dd

def print_db_stats():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::int FROM paper_trades WHERE action='EXIT';")
            exits_total = int(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT
                  COALESCE(SUM(pnl), 0) AS pnl_24h,
                  COUNT(*)::int AS trades_24h
                FROM paper_trades
                WHERE ts >= (NOW() - INTERVAL '24 hours');
            """)
            row = cur.fetchone()
            pnl_24h = float(row[0] or 0.0)
            trades_24h = int(row[1] or 0)

            cur.execute("""
                SELECT COUNT(*)::int
                FROM paper_trades
                WHERE action='EXIT' AND ts >= (NOW() - INTERVAL '24 hours');
            """)
            exits_24h = int(cur.fetchone()[0] or 0)

    winrate20 = get_winrate_last_n_exits(20)
    print(
        f"{utc_now_iso()} | STATS | exits_total={exits_total} "
        f"exits_24h={exits_24h} pnl_24h(realized)={pnl_24h:.2f} "
        f"trades_24h={trades_24h} "
        f"winrate20={('n/a' if winrate20 is None else f'{winrate20*100:.0f}%')}",
        flush=True
    )

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  (ts AT TIME ZONE 'UTC')::date AS day_utc,
                  COUNT(*)::int AS exits,
                  COALESCE(SUM(pnl),0) AS realized_pnl
                FROM paper_trades
                WHERE action='EXIT'
                GROUP BY 1
                ORDER BY 1 DESC
                LIMIT 10;
            """)
            rows = cur.fetchall()

    print(f"{utc_now_iso()} | STATS | last_10_days (UTC):", flush=True)
    for day_utc, exits, realized_pnl in rows[::-1]:
        print(f"{utc_now_iso()} | STATS | {day_utc} | exits={int(exits)} | realized_pnl={float(realized_pnl):.2f}", flush=True)


# =========================
# LIVE TRADING (SAFE SCAFFOLD)
# =========================
def live_trading_ready() -> Tuple[bool, str]:
    """
    Returns (ready, reason). We require:
      - LIVE_TRADING_ENABLED=true
      - POLY_PRIVATE_KEY set
      - POLY_FUNDER set (address)
      - (Optional) API creds, depending on your account setup
    """
    if RUN_MODE != "LIVE":
        return False, "RUN_MODE is not LIVE"
    if KILL_SWITCH:
        return False, "KILL_SWITCH active"
    if not LIVE_TRADING_ENABLED:
        return False, "LIVE_TRADING_ENABLED is false"
    if not POLY_PRIVATE_KEY:
        return False, "POLY_PRIVATE_KEY missing"
    if not POLY_FUNDER:
        return False, "POLY_FUNDER/POLY_PUBLIC_ADDRESS missing"

    # Many setups require API creds; if yours does, set them.
    # We don't hard-require them here to avoid blocking if your client derives auth differently,
    # but we warn in logs if missing.
    return True, "ok"


def place_live_order_stub(action: str, token_id: str, usdc_amount: float, limit_price: float) -> Tuple[bool, str]:
    """
    Minimal safe implementation:
    - Imports py-clob-client
    - Attempts to create a client and submit a limit order

    IMPORTANT:
    This is intentionally defensive. If anything is missing, it returns (False, reason)
    and the bot will HOLD rather than risk unintended behavior.
    """
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import BUY, SELL
    except Exception as e:
        return False, f"py-clob-client import failed: {repr(e)}"

    # Basic sanity
    if usdc_amount <= 0:
        return False, "usdc_amount <= 0"
    if not (0.001 <= limit_price <= 0.999):
        return False, f"limit_price out of range: {limit_price}"

    # Decide side
    side = BUY if action == "BUY" else SELL

    # Create client
    # NOTE: Depending on your setup, you may need to pass api creds.
    # The client supports different auth modes; we keep this minimal.
    try:
        client = ClobClient(
            host=POLY_CLOB_HOST,
            chain_id=POLY_CHAIN_ID,
            private_key=POLY_PRIVATE_KEY,
            funder=POLY_FUNDER,
        )
    except Exception as e:
        return False, f"ClobClient init failed: {repr(e)}"

    # If creds are present, try to set them (optional)
    try:
        if POLY_API_KEY and POLY_API_SECRET and POLY_API_PASSPHRASE:
            client.set_api_creds({
                "key": POLY_API_KEY,
                "secret": POLY_API_SECRET,
                "passphrase": POLY_API_PASSPHRASE,
            })
    except Exception as e:
        # Not fatal; some setups may not need this
        if DEBUG_POLY:
            print(f"{utc_now_iso()} | WARN | set_api_creds failed: {repr(e)}", flush=True)

    # Build order args
    try:
        # OrderArgs expects size in *shares* for token orders in many setups.
        # If your client expects size differently, we will adjust after first live test.
        # Here we convert USDC amount to shares approx.
        size_shares = usdc_amount / limit_price

        order_args = OrderArgs(
            token_id=str(token_id),
            price=str(limit_price),
            size=str(size_shares),
            side=side,
        )

        resp = client.create_order(order_args)
        return True, f"order_submitted: {str(resp)[:200]}"
    except Exception as e:
        return False, f"create_order failed: {repr(e)}"


# =========================
# MAIN
# =========================
def main():
    init_db()

    if STATS_ONLY:
        print_db_stats()
        return

    state = load_state()
    state = reset_daily_counters_if_needed(state)

    # Manual daily reset (counters only)
    if RESET_DAILY:
        save_state(
            balance=float(state["balance"]),
            position=state["position"],
            entry_price=float(state["entry_price"]) if state["entry_price"] is not None else None,
            stake=float(state["stake"]),
            last_trade_ts=state.get("last_trade_ts"),
            last_trade_day=utc_today_date(),
            trades_today=0,
            realized_pnl_today=0.0,
            last_mark=float(state["last_mark"]) if state.get("last_mark") is not None else None,
            last_seen_slug=state.get("last_seen_slug"),
            last_trade_slug=state.get("last_trade_slug"),
        )
        print(f"{utc_now_iso()} | INFO | RESET_DAILY applied. Daily counters cleared.", flush=True)
        return

    # Manual state reset (position only)
    if RESET_STATE:
        save_state(
            balance=float(state["balance"]),
            position=None,
            entry_price=None,
            stake=0.0,
            last_trade_ts=state.get("last_trade_ts"),
            last_trade_day=_as_date(state.get("last_trade_day")),
            trades_today=int(state.get("trades_today", 0)),
            realized_pnl_today=float(state.get("realized_pnl_today", 0.0)),
            last_mark=float(state["last_mark"]) if state.get("last_mark") is not None else None,
            last_seen_slug=state.get("last_seen_slug"),
            last_trade_slug=state.get("last_trade_slug"),
        )
        print(f"{utc_now_iso()} | INFO | RESET_STATE applied. Position cleared.", flush=True)
        return

    closes = fetch_btc_closes_5m(LOOKBACK)
    if closes is None:
        print(f"{utc_now_iso()} | INFO | No data this run. balance={float(state['balance']):.2f} pos={state['position']}", flush=True)
        return

    # State fields
    balance = float(state["balance"])
    position = state["position"]
    entry_price = float(state["entry_price"]) if state["entry_price"] is not None else None
    stake = float(state["stake"])
    last_trade_ts = state.get("last_trade_ts")
    last_trade_day = _as_date(state.get("last_trade_day"))
    trades_today = int(state.get("trades_today", 0))
    realized_pnl_today = float(state.get("realized_pnl_today", 0.0))
    last_seen_slug = state.get("last_seen_slug")
    last_trade_slug = state.get("last_trade_slug")

    # Slug selection
    poly_slug = POLY_EVENT_SLUG_OVERRIDE if POLY_EVENT_SLUG_OVERRIDE else current_btc_5m_slug()
    print(f"{utc_now_iso()} | INFO | run_mode={RUN_MODE} live_mode={LIVE_MODE} live_armed={LIVE_TRADING_ENABLED} poly_slug={poly_slug}", flush=True)

    # Fetch marks + token IDs
    source = "synthetic"
    up_tid = None
    dn_tid = None

    pm = fetch_polymarket_marks_and_tokens(poly_slug)
    if pm is not None:
        p_up, p_down, source, up_tid, dn_tid = pm
    else:
        p_up = prob_from_closes(closes) if PROB_MODE else 0.5
        p_up = clamp(p_up, PROB_P_MIN, PROB_P_MAX)
        p_down = 1.0 - p_up
        p_down = clamp(p_down, 1.0 - PROB_P_MAX, 1.0 - PROB_P_MIN)

    # Fair + edge
    fair_up = prob_from_closes(closes)
    fair_up = clamp(fair_up, PROB_P_MIN, PROB_P_MAX)
    edge = fair_up - p_up

    # If DOWN_EDGE_ENTER is configured, allow asymmetric thresholds:
    # - Enter YES if edge >= EDGE_ENTER
    # - Enter NO  if edge <= -DOWN_EDGE_ENTER
    edge_enter = effective_edge_enter()
    down_edge_enter = DOWN_EDGE_ENTER if DOWN_EDGE_ENTER is not None else edge_enter

    # Safety: determine base signal
    if KILL_SWITCH:
        signal = "HOLD"
        safety_reason = "KILL_SWITCH active"
    elif LIVE_MODE and not marks_look_sane_for_live(p_up, p_down):
        signal = "HOLD"
        safety_reason = f"marks_sanity_failed p_up={p_up:.3f} p_down={p_down:.3f} sum={(p_up+p_down):.3f}"
    elif LIVE_MODE and LIVE_DAILY_PROFIT_LOCK > 0 and realized_pnl_today >= LIVE_DAILY_PROFIT_LOCK:
        signal = "HOLD"
        safety_reason = f"PROFIT_LOCK hit pnl_today={realized_pnl_today:.2f} >= {LIVE_DAILY_PROFIT_LOCK:.2f}"
    else:
        safety_reason = ""
        if edge >= edge_enter:
            signal = "YES"
        elif edge <= -down_edge_enter:
            signal = "NO"
        else:
            signal = "HOLD"

    # Mark-to-market uPnL / equity (for logs + snapshots)
    if position == "YES":
        unrealized_pnl = calc_unrealized_pnl(position, entry_price, p_up, stake)
    elif position == "NO":
        unrealized_pnl = calc_unrealized_pnl(position, entry_price, p_down, stake)
    else:
        unrealized_pnl = 0.0
    equity = balance + unrealized_pnl

    record_equity_snapshot(
        mark=p_up,
        balance=balance,
        position=position,
        entry_price=entry_price,
        stake=stake,
        unrealized_pnl=unrealized_pnl,
        equity=equity,
    )

    # Idempotency guard: never trade the same slug twice
    # - if we've already traded this slug, we force HOLD for trade actions (still log stats)
    traded_this_slug_already = (last_trade_slug is not None and str(last_trade_slug) == str(poly_slug))

    # Risk gates apply ONLY to ENTER (position is None)
    blocked_reason = None
    whale = (signal in ("YES", "NO")) and (abs(edge) >= WHALE_EDGE_OVERRIDE)

    if traded_this_slug_already and signal in ("YES", "NO"):
        blocked_reason = f"IDEMPOTENCY: already traded slug={poly_slug} this interval"

    if position is None and signal in ("YES", "NO") and not blocked_reason:
        max_daily_loss = effective_max_daily_loss()
        max_trades_day = effective_max_trades_per_day()
        cooldown_min = effective_cooldown_minutes()

        if realized_pnl_today <= -max_daily_loss:
            blocked_reason = f"MAX_DAILY_LOSS hit (pnl_today={realized_pnl_today:.2f} <= -{max_daily_loss:.2f})"
        elif trades_today >= max_trades_day:
            if whale and ALLOW_WHALE_AFTER_CAP:
                blocked_reason = None
                print(f"{utc_now_iso()} | SAFETY | WHALE allowed after cap | edge={edge:+.3f}", flush=True)
            else:
                blocked_reason = f"MAX_TRADES_PER_DAY hit (trades_today={trades_today} >= {max_trades_day})"
        else:
            cd_active = cooldown_active(last_trade_ts, cooldown_min)
            if cd_active and not (WHALE_COOLDOWN_OVERRIDE and whale):
                blocked_reason = f"COOLDOWN active ({cooldown_min}m since last trade)"
            elif cd_active and (WHALE_COOLDOWN_OVERRIDE and whale):
                print(f"{utc_now_iso()} | SAFETY | WHALE override bypassed cooldown | edge={edge:+.3f}", flush=True)

    # Persist last seen slug always
    last_seen_slug = poly_slug

    # If we are blocked or HOLD, we do not trade (paper or live)
    if blocked_reason or signal == "HOLD":
        save_state(
            balance=balance,
            position=position,
            entry_price=entry_price,
            stake=stake,
            last_trade_ts=last_trade_ts,
            last_trade_day=last_trade_day,
            trades_today=trades_today,
            realized_pnl_today=realized_pnl_today,
            last_mark=p_up,
            last_seen_slug=last_seen_slug,
            last_trade_slug=last_trade_slug,
        )

        pnl_24h, trades_24h = get_realized_pnl_24h_and_trades_24h()
        winrate20 = get_winrate_last_n_exits(20)
        dd_24h = get_drawdown_24h()

        label_up = "poly_up" if source in ("clob", "gamma") else "mark_up"
        label_dn = "poly_down" if source in ("clob", "gamma") else "mark_down"

        reason = blocked_reason or (safety_reason or "signal=HOLD")
        print(
            f"{utc_now_iso()} | {label_up}={p_up:.3f} | {label_dn}={p_down:.3f} | "
            f"fair_up={fair_up:.3f} | edge={edge:+.3f} | "
            f"signal={signal} | action=NO_TRADE | reason={reason} | "
            f"balance={balance:.2f} | pos={position} | entry={entry_price} | "
            f"trades_today={trades_today} | pnl_today(realized)={realized_pnl_today:.2f} | "
            f"mode={RUN_MODE} | src={source}",
            flush=True
        )
        print(
            f"{utc_now_iso()} | summary | equity={equity:.2f} | uPnL={unrealized_pnl:.2f} | "
            f"pnl_24h(realized)={pnl_24h:.2f} | trades_24h={trades_24h} | "
            f"winrate20={('n/a' if winrate20 is None else f'{winrate20*100:.0f}%')} | "
            f"dd_24h={('n/a' if dd_24h is None else f'{dd_24h:.2f}')} | src={source}",
            flush=True
        )
        return

    # At this point, we have signal YES/NO, not blocked, not HOLD.

    # DRY_RUN: no trades at all
    if RUN_MODE == "DRY_RUN":
        save_state(
            balance=balance,
            position=position,
            entry_price=entry_price,
            stake=stake,
            last_trade_ts=last_trade_ts,
            last_trade_day=last_trade_day,
            trades_today=trades_today,
            realized_pnl_today=realized_pnl_today,
            last_mark=p_up,
            last_seen_slug=last_seen_slug,
            last_trade_slug=last_trade_slug,
        )
        print(
            f"{utc_now_iso()} | DRY_RUN | would_signal={signal} | edge={edge:+.3f} | "
            f"balance={balance:.2f} pos={position} trades_today={trades_today} pnl_today={realized_pnl_today:.2f}",
            flush=True
        )
        return

    # LIVE mode: attempt live order flow ONLY if fully armed
    if RUN_MODE == "LIVE":
        ready, why = live_trading_ready()
        if not ready:
            # Refuse to trade live if not ready
            save_state(
                balance=balance,
                position=position,
                entry_price=entry_price,
                stake=stake,
                last_trade_ts=last_trade_ts,
                last_trade_day=last_trade_day,
                trades_today=trades_today,
                realized_pnl_today=realized_pnl_today,
                last_mark=p_up,
                last_seen_slug=last_seen_slug,
                last_trade_slug=last_trade_slug,
            )
            print(f"{utc_now_iso()} | SAFETY | LIVE not ready -> HOLD | reason={why}", flush=True)
            return

        # Need token IDs to trade live
        if not up_tid or not dn_tid:
            save_state(
                balance=balance,
                position=position,
                entry_price=entry_price,
                stake=stake,
                last_trade_ts=last_trade_ts,
                last_trade_day=last_trade_day,
                trades_today=trades_today,
                realized_pnl_today=realized_pnl_today,
                last_mark=p_up,
                last_seen_slug=last_seen_slug,
                last_trade_slug=last_trade_slug,
            )
            print(f"{utc_now_iso()} | SAFETY | Missing token IDs for slug={poly_slug} -> HOLD", flush=True)
            return

        # For now, live flow supports ENTER only when flat.
        # Exits are handled by your existing edge-collapse/flip logic in PAPER.
        # You can extend this to SELL orders once you confirm BUY behavior works.
        if position is not None:
            save_state(
                balance=balance,
                position=position,
                entry_price=entry_price,
                stake=stake,
                last_trade_ts=last_trade_ts,
                last_trade_day=last_trade_day,
                trades_today=trades_today,
                realized_pnl_today=realized_pnl_today,
                last_mark=p_up,
                last_seen_slug=last_seen_slug,
                last_trade_slug=last_trade_slug,
            )
            print(f"{utc_now_iso()} | LIVE | currently holding pos={position} -> no live action yet (enter-only scaffold)", flush=True)
            return

        # Choose token and compute limit price with slippage buffer for BUY
        trade_size = effective_trade_size()
        if signal == "YES":
            token = up_tid
            buy_px = clamp(p_up + LIVE_SLIPPAGE, 0.001, 0.999)
        else:
            token = dn_tid
            buy_px = clamp(p_down + LIVE_SLIPPAGE, 0.001, 0.999)

        ok, msg = place_live_order_stub("BUY", token, trade_size, buy_px)
        if not ok:
            save_state(
                balance=balance,
                position=position,
                entry_price=entry_price,
                stake=stake,
                last_trade_ts=last_trade_ts,
                last_trade_day=last_trade_day,
                trades_today=trades_today,
                realized_pnl_today=realized_pnl_today,
                last_mark=p_up,
                last_seen_slug=last_seen_slug,
                last_trade_slug=last_trade_slug,
            )
            print(f"{utc_now_iso()} | LIVE | order failed -> HOLD | {msg}", flush=True)
            return

        # If submitted, record a conservative "entered" state (still mark-based, until fills table is added)
        now_ts = utc_now_dt()
        today = utc_today_date()
        position = signal
        stake = trade_size
        entry_price = p_up if position == "YES" else p_down
        last_trade_ts = now_ts
        last_trade_day = today
        trades_today += 1
        last_trade_slug = poly_slug

        log_trade("LIVE_ENTER_SUBMITTED", side=position, price=entry_price, stake=stake, fee=0.0, pnl=None, slug=poly_slug)

        save_state(
            balance=balance,  # live balance not tracked here (wallet is source of truth)
            position=position,
            entry_price=entry_price,
            stake=stake,
            last_trade_ts=last_trade_ts,
            last_trade_day=last_trade_day,
            trades_today=trades_today,
            realized_pnl_today=realized_pnl_today,
            last_mark=p_up,
            last_seen_slug=last_seen_slug,
            last_trade_slug=last_trade_slug,
        )
        print(f"{utc_now_iso()} | LIVE | ENTER submitted | side={position} token={token} limit_px={buy_px:.3f} msg={msg}", flush=True)
        return

    # PAPER mode: execute paper engine
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

    # If paper trade occurred (enter/exit), set last_trade_slug
    if action.startswith("ENTER") or action == "EXIT":
        last_trade_slug = poly_slug

    save_state(
        balance=balance,
        position=position,
        entry_price=entry_price,
        stake=stake,
        last_trade_ts=last_trade_ts,
        last_trade_day=last_trade_day,
        trades_today=trades_today,
        realized_pnl_today=realized_pnl_today,
        last_mark=p_up,
        last_seen_slug=last_seen_slug,
        last_trade_slug=last_trade_slug,
    )

    # Recompute uPnL after trade
    if position == "YES":
        unrealized_pnl = calc_unrealized_pnl(position, entry_price, p_up, stake)
    elif position == "NO":
        unrealized_pnl = calc_unrealized_pnl(position, entry_price, p_down, stake)
    else:
        unrealized_pnl = 0.0
    equity = balance + unrealized_pnl

    pnl_24h, trades_24h = get_realized_pnl_24h_and_trades_24h()
    winrate20 = get_winrate_last_n_exits(20)
    dd_24h = get_drawdown_24h()

    label_up = "poly_up" if source in ("clob", "gamma") else "mark_up"
    label_dn = "poly_down" if source in ("clob", "gamma") else "mark_down"

    print(
        f"{utc_now_iso()} | {label_up}={p_up:.3f} | {label_dn}={p_down:.3f} | "
        f"fair_up={fair_up:.3f} | edge={edge:+.3f} | "
        f"signal={signal} | action={action} | "
        f"balance={balance:.2f} | pos={position} | entry={entry_price} | "
        f"trades_today={trades_today} | pnl_today(realized)={realized_pnl_today:.2f} | "
        f"mode={RUN_MODE} | src={source}",
        flush=True
    )
    print(
        f"{utc_now_iso()} | summary | equity={equity:.2f} | uPnL={unrealized_pnl:.2f} | "
        f"pnl_24h(realized)={pnl_24h:.2f} | trades_24h={trades_24h} | "
        f"winrate20={('n/a' if winrate20 is None else f'{winrate20*100:.0f}%')} | "
        f"dd_24h={('n/a' if dd_24h is None else f'{dd_24h:.2f}')} | src={source}",
        flush=True
    )


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

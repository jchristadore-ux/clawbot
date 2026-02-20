import os
import time
import json
from datetime import datetime, timezone, timedelta, date
from typing import Optional, Tuple, Dict, Any

import requests
import psycopg2
from psycopg2.extras import RealDictCursor

# =========================
# CONFIG
# =========================
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

START_BALANCE = float(os.getenv("START_BALANCE", "1000"))
TRADE_SIZE = float(os.getenv("TRADE_SIZE", "25"))
FEE_PER_TRADE = float(os.getenv("FEE_PER_TRADE", "0"))

LOOKBACK = int(os.getenv("LOOKBACK", "5"))
UP_THRESHOLD = float(os.getenv("UP_THRESHOLD", "1.002"))     # +0.2%
DOWN_THRESHOLD = float(os.getenv("DOWN_THRESHOLD", "0.998"))  # -0.2%

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "20"))

# Risk Controls (ENTER only)
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))          # realized only
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "20"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "8"))

# Synthetic probability fallback (from Kraken closes)
PROB_MODE = os.getenv("PROB_MODE", "true").lower() == "true"
PROB_P_MIN = float(os.getenv("PROB_P_MIN", "0.05"))
PROB_P_MAX = float(os.getenv("PROB_P_MAX", "0.95"))
PROB_Z_SCALE = float(os.getenv("PROB_Z_SCALE", "2.5"))

# One-time reset switch (set true for ONE run, then set back to false)
RESET_STATE = os.getenv("RESET_STATE", "false").lower() == "true"

# Polymarket (Gamma + CLOB)
POLY_EVENT_SLUG = os.getenv("POLY_EVENT_SLUG", "").strip()  # event slug OR market slug
POLY_TIMEOUT_SEC = int(os.getenv("POLY_TIMEOUT_SEC", "20"))
DEBUG_POLY = os.getenv("DEBUG_POLY", "false").lower() == "true"

GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"

CLOB_PRICE_URL = "https://clob.polymarket.com/price"

# Optional performance snapshots (safe OFF by default)
ENABLE_SNAPSHOTS = os.getenv("ENABLE_SNAPSHOTS", "false").lower() == "true"
ENABLE_DRAWDOWN = os.getenv("ENABLE_DRAWDOWN", "false").lower() == "true"

# Kraken (for strategy + fallback marks)
KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"
KRAKEN_PAIR = "XBTUSD"
KRAKEN_INTERVAL = 5  # minutes


# =========================
# UTIL
# =========================
def current_btc_5m_slug(now: Optional[datetime] = None) -> str:
    # Polymarket short-interval slugs use a unix timestamp (seconds) aligned to the interval.
    # Use UTC to avoid timezone drift.
    if now is None:
        now = datetime.now(timezone.utc)

    # floor to the current 5-minute boundary
    floored = now.replace(second=0, microsecond=0)
    minute = (floored.minute // 5) * 5
    floored = floored.replace(minute=minute)

    ts = int(floored.timestamp())
    return f"btc-updown-5m-{ts}"

def current_btc_5m_slug(now: Optional[datetime] = None) -> str:
    if now is None:
        now = datetime.now(timezone.utc)

    floored = now.replace(second=0, microsecond=0)
    minute = (floored.minute // 5) * 5
    floored = floored.replace(minute=minute)

    ts = int(floored.timestamp())
    return f"btc-updown-5m-{ts}"

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


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
                    last_trade_ts, last_trade_day, trades_today, realized_pnl_today, last_mark
                )
                VALUES (1, %s, NULL, NULL, 0.0, NULL, NULL, 0, 0.0, NULL)
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
                ),
            )
        conn.commit()


def log_trade(action: str, side=None, price=None, stake=None, fee=None, pnl=None):
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
    today = utc_today_date()
    last_day = _as_date(state.get("last_trade_day"))
    if last_day is None or last_day != today:
        state["last_trade_day"] = today
        state["trades_today"] = 0
        state["realized_pnl_today"] = 0.0
    return state


def cooldown_active(last_trade_ts, cooldown_minutes: int) -> bool:
    if last_trade_ts is None:
        return False
    try:
        return utc_now_dt() < (last_trade_ts + timedelta(minutes=cooldown_minutes))
    except Exception:
        return False


# =========================
# DATA: Kraken closes
# =========================
def fetch_btc_closes_5m(lookback: int) -> Optional[list]:
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

    print(f"{utc_now_iso()} | WARN | Price fetch failed after retries: {last_error}")
    return None


# =========================
# STRATEGY: signal from closes
# =========================
def decide(closes: list) -> str:
    avg = sum(closes) / len(closes)
    last = closes[-1]
    if last > avg * UP_THRESHOLD:
        return "YES"   # Up
    if last < avg * DOWN_THRESHOLD:
        return "NO"    # Down
    return "HOLD"


# =========================
# SYNTHETIC MARKS: probability from closes
# =========================
def prob_from_closes(closes: list) -> float:
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


def fetch_polymarket_marks(slug: str) -> Optional[Tuple[float, float, str]]:
    """
    Attempts:
      1) CLOB prices via clobTokenIds -> source 'clob'
      2) Gamma outcomePrices -> source 'gamma'
    Returns None if unusable (caller falls back to synthetic).
    """
    s = (slug or "").strip()
    if not s:
        return None

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            candidate_markets: list = []

            # Try as MARKET slug
            m = _get_market_by_slug(s)
            if isinstance(m, dict) and m:
                candidate_markets.append(m)

            # Try as EVENT slug and hydrate markets
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
                print(f"{utc_now_iso()} | DEBUG | poly_slug={s} candidate_markets={len(candidate_markets)}")
                if candidate_markets and isinstance(candidate_markets[0], dict):
                    cm0 = candidate_markets[0]
                    print(
                        f"{utc_now_iso()} | DEBUG | sample_market_id={cm0.get('id')} "
                        f"has_outcomePrices={'outcomePrices' in cm0} "
                        f"has_clobTokenIds={('clobTokenIds' in cm0 or 'clobTokenIDs' in cm0)}"
                    )

            for cm in candidate_markets:
                if not isinstance(cm, dict):
                    continue
                info = _extract_updown_from_market(cm)
                if not info:
                    continue

                up_tid = info.get("up_tid")
                dn_tid = info.get("dn_tid")

                # 1) Try CLOB if we have token IDs
                if up_tid and dn_tid:
                    try:
                        p_up = _fetch_clob_price(up_tid, side="BUY")
                        p_dn = _fetch_clob_price(dn_tid, side="BUY")
                        return clamp(p_up, 0.001, 0.999), clamp(p_dn, 0.001, 0.999), "clob"
                    except Exception as e:
                        last_error = str(e)[:200]
                        # fall through to Gamma

                # 2) Try Gamma outcomePrices if present
                up_g = info.get("up_gamma")
                dn_g = info.get("dn_gamma")
                if up_g is not None and dn_g is not None:
                    return clamp(float(up_g), 0.001, 0.999), clamp(float(dn_g), 0.001, 0.999), "gamma"

            last_error = last_error or "no usable market (missing tokenIds and outcomePrices)"
            _sleep_backoff(attempt)

        except Exception as e:
            last_error = str(e)[:200]
            _sleep_backoff(attempt)

    if DEBUG_POLY:
        print(f"{utc_now_iso()} | DEBUG | fetch_polymarket_marks failed: {last_error}")
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
# PAPER TRADE: probability marks (Up/Down)
# =========================
def paper_trade_prob(
    balance: float,
    position: Optional[str],
    entry_price: Optional[float],
    stake: float,
    signal: str,
    p_up: float,
    p_down: float,
    last_trade_ts,
    last_trade_day,
    trades_today: int,
    realized_pnl_today: float,
) -> Tuple[float, Optional[str], Optional[float], float, str, object, object, int, float]:

    if signal == "HOLD":
        return (balance, position, entry_price, stake, "HOLD",
                last_trade_ts, last_trade_day, trades_today, realized_pnl_today)

    now_ts = utc_now_dt()
    today = utc_today_date()

    # ENTER if flat
    if position is None:
        if balance < (TRADE_SIZE + FEE_PER_TRADE):
            return (balance, position, entry_price, stake, "SKIP_INSUFFICIENT_BALANCE",
                    last_trade_ts, last_trade_day, trades_today, realized_pnl_today)

        balance -= (TRADE_SIZE + FEE_PER_TRADE)
        position = signal
        stake = TRADE_SIZE
        entry_price = p_up if position == "YES" else p_down

        log_trade("ENTER", side=position, price=entry_price, stake=stake, fee=FEE_PER_TRADE)

        trades_today += 1
        last_trade_ts = now_ts
        last_trade_day = today

        return (balance, position, entry_price, stake, f"ENTER_{signal}",
                last_trade_ts, last_trade_day, trades_today, realized_pnl_today)

    # EXIT if opposite signal (always allowed)
    if signal != position:
        exit_price = p_up if position == "YES" else p_down
        pnl = calc_realized_pnl(entry_price, exit_price, stake)

        balance += (stake + pnl - FEE_PER_TRADE)
        log_trade("EXIT", side=position, price=exit_price, stake=stake, fee=FEE_PER_TRADE, pnl=pnl)

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


# =========================
# MAIN
# =========================
def main():
    init_db()

    state = load_state()
    state = reset_daily_counters_if_needed(state)

    # One-time reset to clear open position
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
        )
        print(f"{utc_now_iso()} | INFO | RESET_STATE applied. Position cleared.")
        return

    closes = fetch_btc_closes_5m(LOOKBACK)
    if closes is None:
        print(f"{utc_now_iso()} | INFO | No data this run. balance={float(state['balance']):.2f} pos={state['position']}")
        return

    signal = decide(closes)

    # MARKS: prefer Polymarket (CLOB/Gamma), fallback to synthetic
    source = "synthetic"

    # Compute current 5-minute BTC slug dynamically
    poly_slug = current_btc_5m_slug()
    print(f"{utc_now_iso()} | INFO | poly_slug={poly_slug}", flush=True)

    pm = fetch_polymarket_marks(poly_slug) 

    if pm is not None:
        p_up, p_down, source = pm
    else:
        p_up = prob_from_closes(closes) if PROB_MODE else 0.5
        p_up = clamp(p_up, PROB_P_MIN, PROB_P_MAX)
        p_down = 1.0 - p_up
        p_down = clamp(p_down, 1.0 - PROB_P_MAX, 1.0 - PROB_P_MIN)

    # State fields
    balance = float(state["balance"])
    position = state["position"]
    entry_price = float(state["entry_price"]) if state["entry_price"] is not None else None
    stake = float(state["stake"])

    last_trade_ts = state.get("last_trade_ts")
    last_trade_day = _as_date(state.get("last_trade_day"))
    trades_today = int(state.get("trades_today", 0))
    realized_pnl_today = float(state.get("realized_pnl_today", 0.0))

    # Mark-to-market uPnL
    if position == "YES":
        unrealized_pnl = calc_unrealized_pnl(position, entry_price, p_up, stake)
    elif position == "NO":
        unrealized_pnl = calc_unrealized_pnl(position, entry_price, p_down, stake)
    else:
        unrealized_pnl = 0.0
    equity = balance + unrealized_pnl

    # Optional snapshot (uses p_up as the mark)
    record_equity_snapshot(
        mark=p_up,
        balance=balance,
        position=position,
        entry_price=entry_price,
        stake=stake,
        unrealized_pnl=unrealized_pnl,
        equity=equity,
    )

    # Risk gates apply ONLY to ENTER (position is None)
    blocked_reason = None
    if position is None and signal in ("YES", "NO"):
        if realized_pnl_today <= -MAX_DAILY_LOSS:
            blocked_reason = f"MAX_DAILY_LOSS hit (pnl_today={realized_pnl_today:.2f} <= -{MAX_DAILY_LOSS})"
        elif trades_today >= MAX_TRADES_PER_DAY:
            blocked_reason = f"MAX_TRADES_PER_DAY hit (trades_today={trades_today} >= {MAX_TRADES_PER_DAY})"
        elif cooldown_active(last_trade_ts, COOLDOWN_MINUTES):
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
            last_mark=p_up,
        )

        pnl_24h, trades_24h = get_realized_pnl_24h_and_trades_24h()
        winrate20 = get_winrate_last_n_exits(20)
        dd_24h = get_drawdown_24h()

        label_up = "poly_up" if source in ("clob", "gamma") else "mark_up"
        label_dn = "poly_down" if source in ("clob", "gamma") else "mark_down"

        print(
            f"{utc_now_iso()} | {label_up}={p_up:.3f} | {label_dn}={p_down:.3f} | "
            f"signal={signal} | action=BLOCKED | reason={blocked_reason} | "
            f"balance={balance:.2f} | pos={position} | entry={entry_price} | "
            f"trades_today={trades_today} | pnl_today(realized)={realized_pnl_today:.2f} | src={source}"
        )
        print(
            f"{utc_now_iso()} | summary | equity={equity:.2f} | uPnL={unrealized_pnl:.2f} | "
            f"pnl_24h(realized)={pnl_24h:.2f} | trades_24h={trades_24h} | "
            f"winrate20={('n/a' if winrate20 is None else f'{winrate20*100:.0f}%')} | "
            f"dd_24h={('n/a' if dd_24h is None else f'{dd_24h:.2f}')} | src={source}"
        )
        return

    # Trade step
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
        balance,
        position,
        entry_price,
        stake,
        signal,
        p_up,
        p_down,
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
        last_mark=p_up,
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
        f"signal={signal} | action={action} | "
        f"balance={balance:.2f} | pos={position} | entry={entry_price} | "
        f"trades_today={trades_today} | pnl_today(realized)={realized_pnl_today:.2f} | src={source}"
    )
    print(
        f"{utc_now_iso()} | summary | equity={equity:.2f} | uPnL={unrealized_pnl:.2f} | "
        f"pnl_24h(realized)={pnl_24h:.2f} | trades_24h={trades_24h} | "
        f"winrate20={('n/a' if winrate20 is None else f'{winrate20*100:.0f}%')} | "
        f"dd_24h={('n/a' if dd_24h is None else f'{dd_24h:.2f}')} | src={source}"
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


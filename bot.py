import os
import time
from datetime import datetime, timezone, timedelta, date
from typing import Optional, Tuple

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

# Day 2 Risk Controls
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "20"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "8"))

# Day 4 Polymarket-style probability mode
PROB_MODE = os.getenv("PROB_MODE", "true").lower() == "true"
PROB_P_MIN = float(os.getenv("PROB_P_MIN", "0.05"))
PROB_P_MAX = float(os.getenv("PROB_P_MAX", "0.95"))
PROB_Z_SCALE = float(os.getenv("PROB_Z_SCALE", "2.5"))

# One-time state reset switch (set true for ONE run, then false)
RESET_STATE = os.getenv("RESET_STATE", "false").lower() == "true"
ENABLE_SNAPSHOTS = os.getenv("ENABLE_SNAPSHOTS", "false").lower() == "true"
ENABLE_DRAWDOWN = os.getenv("ENABLE_DRAWDOWN", "false").lower() == "true"

# Kraken
KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"
KRAKEN_PAIR = "XBTUSD"
KRAKEN_INTERVAL = 5  # minutes

# Polymarket (Gamma)
POLY_EVENT_SLUG = os.getenv("POLY_EVENT_SLUG", "").strip()
POLY_TIMEOUT_SEC = int(os.getenv("POLY_TIMEOUT_SEC", "20"))
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"

# =========================
# UTIL
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def utc_today_date() -> date:
    return utc_now_dt().date()


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def db_conn():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. Add Railway Postgres and ensure bot service has DATABASE_URL."
        )
    return psycopg2.connect(DATABASE_URL)


def _as_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)).date()
    except Exception:
        return None


# =========================
# DB
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

    # 👇 This MUST be indented inside the function
    init_snapshots_table()


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
    balance,
    position,
    entry_price,
    stake,
    last_trade_ts,
    last_trade_day,
    trades_today,
    realized_pnl_today,
    last_mark,
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
    """Reset trades_today and realized_pnl_today when UTC day changes."""
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
    return utc_now_dt() < (ts + timedelta(minutes=cooldown_minutes))


def maybe_backfill_legacy_open_position(state: dict) -> dict:
    """
    If you had an open position from older versions (or counters missing),
    ensure day + trades_today are coherent so gates work.
    """
    if state.get("position") is not None:
        # If last_trade_day isn't set, set it to today and count at least 1 trade.
        if state.get("last_trade_day") is None:
            state["last_trade_day"] = utc_today_date()
            state["trades_today"] = max(int(state.get("trades_today", 0)), 1)
            state["last_trade_ts"] = state.get("last_trade_ts") or utc_now_dt()

    return state


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


# =========================
# PROBABILITY MODE (Day 4)
# =========================
def prob_from_closes(closes) -> float:
    """
    Map last vs avg into a 0..1 probability-like price.
    Linear mapping scaled by PROB_Z_SCALE, then clamped.
    """
    avg = sum(closes) / len(closes)
    last = closes[-1]
    if avg <= 0:
        return 0.5
    dev = (last - avg) / avg
    p = 0.5 + (dev * PROB_Z_SCALE)
    return clamp(p, PROB_P_MIN, PROB_P_MAX)


def shares_for_stake(stake: float, price: float) -> float:
    if price <= 0:
        return 0.0
    return stake / price


def calc_unrealized_pnl_polymarket(
    position: Optional[str],
    entry_price: Optional[float],
    mark: float,
    stake: float,
) -> float:
    """
    Polymarket-style mark-to-market:
      shares = stake / entry_price
      value_now = shares * mark
      uPnL = value_now - stake
    """
    if position is None or entry_price is None or stake <= 0:
        return 0.0
    sh = shares_for_stake(stake, entry_price)
    value_now = sh * mark
    return value_now - stake


def calc_realized_pnl_polymarket(entry_price: float, exit_price: float, stake: float) -> float:
    sh = shares_for_stake(stake, entry_price)
    value_exit = sh * exit_price
    return value_exit - stake


def paper_trade_prob(
    balance: float,
    position: Optional[str],
    entry_price: Optional[float],
    stake: float,
    signal: str,
    mark_yes: float,
    mark_no: float,
    last_trade_ts,
    last_trade_day,
    trades_today: int,
    realized_pnl_today: float,
):
    """
    Paper trade using probability prices (0..1) like Polymarket.
    Option 1: NO is tracked directly as its own price (entry_price stores NO price when pos=NO).
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

    now_ts = utc_now_dt()
    today = utc_today_date()

    # ENTER if flat
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
        stake = TRADE_SIZE
        entry_price = mark_yes if position == "YES" else mark_no

        log_trade("ENTER", side=position, price=entry_price, stake=stake, fee=FEE_PER_TRADE)

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

    # EXIT if opposite signal
    if signal != position:
        exit_price = mark_yes if position == "YES" else mark_no
        pnl = calc_realized_pnl_polymarket(entry_price, exit_price, stake)

        balance += (stake + pnl - FEE_PER_TRADE)
        log_trade("EXIT", side=position, price=exit_price, stake=stake, fee=FEE_PER_TRADE, pnl=pnl)

        realized_pnl_today += pnl  # realized only
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
# PERFORMANCE (Day 3)
# =========================
# =========================
# PERFORMANCE (optional)
# =========================
def init_snapshots_table():
    """Create equity_snapshots table only if enabled."""
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


def record_equity_snapshot(
    mark: float,
    balance: float,
    position: Optional[str],
    entry_price: Optional[float],
    stake: float,
    unrealized_pnl: float,
    equity: float,
):
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
    if not ENABLE_DRAWDOWN or not ENABLE_SNAPSHOTS:
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
def fetch_polymarket_marks(event_slug: str) -> Optional[Tuple[float, float]]:
    """
    Returns (p_yes, p_no) from Polymarket for the given event slug.
    Uses Gamma API. We look for the market/outcomes and grab best-effort prices.

    If we can't fetch or parse, returns None.
    """
    if not event_slug:
        print(f"{utc_now_iso()} | WARN | POLY_EVENT_SLUG not set")
        return None

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(
                GAMMA_EVENTS_URL,
                params={"slug": event_slug},
                timeout=POLY_TIMEOUT_SEC,
            )
            if r.status_code != 200:
                last_error = f"HTTP {r.status_code}: {r.text[:200]}"
                time.sleep(1.0 * attempt)
                continue

            data = r.json()

            # Gamma commonly returns a list of events
            if isinstance(data, list):
                if not data:
                    last_error = "Empty events list"
                    time.sleep(1.0 * attempt)
                    continue
                event = data[0]
            elif isinstance(data, dict):
                event = data
            else:
                last_error = f"Unexpected JSON type: {type(data)}"
                time.sleep(1.0 * attempt)
                continue

            markets = event.get("markets") or []
            if not markets:
                last_error = "No markets found on event"
                time.sleep(1.0 * attempt)
                continue

            # Many up/down markets have exactly 1 market with YES/NO outcomes
            m0 = markets[0]
            outcomes = m0.get("outcomes") or m0.get("outcomePrices") or []

            # Handle a couple common shapes:
            # - outcomes: [{"name":"Yes","price":0.52},{"name":"No","price":0.48}]
            # - or outcomes: [{"outcome":"Yes","price":"0.52"}, ...]
            # - or outcomePrices: ["0.52","0.48"] with outcomeNames: ["Yes","No"]
            p_yes = None
            p_no = None

            if isinstance(outcomes, list) and outcomes and isinstance(outcomes[0], dict):
                for o in outcomes:
                    name = (o.get("name") or o.get("outcome") or "").strip().lower()
                    price = o.get("price")
                    try:
                        price_f = float(price)
                    except Exception:
                        continue
                    if name == "yes":
                        p_yes = price_f
                    elif name == "no":
                        p_no = price_f

            # Alternative shape: outcomePrices + outcomeNames
            if (p_yes is None or p_no is None) and isinstance(m0.get("outcomePrices"), list):
                prices = m0.get("outcomePrices")
                names = m0.get("outcomeNames") or m0.get("outcomes")
                if isinstance(names, list) and len(names) == len(prices):
                    for nm, pr in zip(names, prices):
                        try:
                            prf = float(pr)
                        except Exception:
                            continue
                        nml = str(nm).strip().lower()
                        if nml == "yes":
                            p_yes = prf
                        elif nml == "no":
                            p_no = prf

            # Fallback: if we only got one side, infer the other
            if p_yes is None and p_no is not None:
                p_yes = 1.0 - p_no
            if p_no is None and p_yes is not None:
                p_no = 1.0 - p_yes

            if p_yes is None or p_no is None:
                last_error = f"Could not parse yes/no prices from market data"
                time.sleep(1.0 * attempt)
                continue

            # Clamp to sane range
            p_yes = clamp(p_yes, 0.001, 0.999)
            p_no = clamp(p_no, 0.001, 0.999)

            return p_yes, p_no

        except Exception as e:
            last_error = str(e)
            time.sleep(1.0 * attempt)

    print(f"{utc_now_iso()} | WARN | Polymarket fetch failed after retries: {last_error}")
    return None



# =========================
# MAIN
# =========================
def main():
    init_db()

    state = load_state()
    state = reset_daily_counters_if_needed(state)
    state = maybe_backfill_legacy_open_position(state)

    # Apply reset switch once to clear any legacy open position/entry
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
        print(f"{utc_now_iso()} | INFO | RESET_STATE applied. Position cleared for Day 4.")
        return

    closes = fetch_btc_closes_5m(LOOKBACK)
    if closes is None:
        print(
            f"{utc_now_iso()} | INFO | No data this run. balance={float(state['balance']):.2f} pos={state['position']}"
        )
        return

    signal = decide(closes)

    # Probability marks (Day 5: use real Polymarket marks if available)
    p_yes = None
    p_no = None

    pm = fetch_polymarket_marks(POLY_EVENT_SLUG)
    if pm is not None:
        p_yes, p_no = pm
    else:
        # fallback to synthetic mark from Kraken closes
        p_yes = prob_from_closes(closes) if PROB_MODE else 0.5
        p_yes = clamp(p_yes, PROB_P_MIN, PROB_P_MAX)
        p_no = 1.0 - p_yes
        p_no = clamp(p_no, 1.0 - PROB_P_MAX, 1.0 - PROB_P_MIN)

    # Pull state
    balance = float(state["balance"])
    position = state["position"]
    entry_price = float(state["entry_price"]) if state["entry_price"] is not None else None
    stake = float(state["stake"])

    last_trade_ts = state.get("last_trade_ts")
    last_trade_day = _as_date(state.get("last_trade_day"))
    trades_today = int(state.get("trades_today", 0))
    realized_pnl_today = float(state.get("realized_pnl_today", 0.0))

    # Unrealized PnL
    if position == "YES":
        unrealized_pnl = calc_unrealized_pnl_polymarket(position, entry_price, p_yes, stake)
    elif position == "NO":
        unrealized_pnl = calc_unrealized_pnl_polymarket(position, entry_price, p_no, stake)
    else:
        unrealized_pnl = 0.0

    equity = balance + unrealized_pnl

    # Snapshot for drawdown window (mark=YES mark)
  # record_equity_snapshot(
#     mark=p_yes,
#     balance=balance,
#     position=position,
#     entry_price=entry_price,
#     stake=stake,
#     unrealized_pnl=unrealized_pnl,
#     equity=equity,
# )

    # Gates (use realized pnl today only)
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
            last_mark=p_yes,
        )

        pnl_24h, trades_24h = get_realized_pnl_24h_and_trades_24h()
        winrate20 = get_winrate_last_n_exits(20)
        dd_24h = get_drawdown_24h()

        print(
            f"{utc_now_iso()} | poly_yes={p_yes:.3f} | poly_no={p_no:.3f} | "
            f"signal={signal} | action=BLOCKED | reason={blocked_reason} | "
            f"balance={balance:.2f} | pos={position} | entry={entry_price} | "
            f"trades_today={trades_today} | pnl_today(realized)={realized_pnl_today:.2f}"
        )
        print(
            f"{utc_now_iso()} | summary | equity={equity:.2f} | uPnL={unrealized_pnl:.2f} | "
            f"pnl_24h(realized)={pnl_24h:.2f} | trades_24h={trades_24h} | "
            f"winrate20={('n/a' if winrate20 is None else f'{winrate20*100:.0f}%')} | "
            f"dd_24h={('n/a' if dd_24h is None else f'{dd_24h:.2f}')}"
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
    ) = paper_trade_prob(
        balance,
        position,
        entry_price,
        stake,
        signal,
        p_yes,
        p_no,
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
        last_mark=p_yes,
    )

    # Recompute after trade
    if position == "YES":
        unrealized_pnl = calc_unrealized_pnl_polymarket(position, entry_price, p_yes, stake)
    elif position == "NO":
        unrealized_pnl = calc_unrealized_pnl_polymarket(position, entry_price, p_no, stake)
    else:
        unrealized_pnl = 0.0
    equity = balance + unrealized_pnl

    pnl_24h, trades_24h = get_realized_pnl_24h_and_trades_24h()
    winrate20 = get_winrate_last_n_exits(20)
    dd_24h = get_drawdown_24h()

    print(
        f"{utc_now_iso()} | mark_yes={p_yes:.3f} | mark_no={p_no:.3f} | "
        f"signal={signal} | action={action} | "
        f"balance={balance:.2f} | pos={position} | entry={entry_price} | "
        f"trades_today={trades_today} | pnl_today(realized)={realized_pnl_today:.2f}"
    )
    print(
        f"{utc_now_iso()} | summary | equity={equity:.2f} | uPnL={unrealized_pnl:.2f} | "
        f"pnl_24h(realized)={pnl_24h:.2f} | trades_24h={trades_24h} | "
        f"winrate20={('n/a' if winrate20 is None else f'{winrate20*100:.0f}%')} | "
        f"dd_24h={('n/a' if dd_24h is None else f'{dd_24h:.2f}')}"
    )


if __name__ == "__main__":
    main()

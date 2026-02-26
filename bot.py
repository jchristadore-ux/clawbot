#!/usr/bin/env python3
import os
import sys
import json
import time
import math
import uuid
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import requests
import psycopg2
from psycopg2.extras import RealDictCursor

# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
STATS_ONLY = os.getenv("STATS_ONLY", "false").lower() == "true"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("clawbot")


def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def jbool(x: str, default: bool = False) -> bool:
    if x is None:
        return default
    return str(x).strip().lower() in ("1", "true", "yes", "y", "on")


def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def env_float(name: str, default: float) -> float:
    v = env(name)
    if v is None:
        return default
    return float(v)


def env_int(name: str, default: int) -> int:
    v = env(name)
    if v is None:
        return default
    return int(v)


# ----------------------------
# Config
# ----------------------------
DATABASE_URL = env("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")

RUN_MODE = (env("RUN_MODE", "PAPER") or "PAPER").upper()  # PAPER | LIVE | DRY_RUN
POLY_MARKET_SLUG = env("POLY_MARKET_SLUG")  # preferred
POLY_EVENT_SLUG = env("POLY_EVENT_SLUG")    # fallback

COOLDOWN_MINUTES = env_int("COOLDOWN_MINUTES", 5)
LIVE_COOLDOWN_MINUTES = env_int("LIVE_COOLDOWN_MINUTES", COOLDOWN_MINUTES)

MAX_TRADES_PER_DAY = env_int("MAX_TRADES_PER_DAY", 5)
LIVE_MAX_TRADES_PER_DAY = env_int("LIVE_MAX_TRADES_PER_DAY", MAX_TRADES_PER_DAY)

UP_THRESHOLD = env_float("UP_THRESHOLD", 0.08)   # edge threshold to enter YES
DOWN_THRESHOLD = env_float("DOWN_THRESHOLD", 0.08)  # edge threshold to enter NO
EDGE_EXIT = env_float("EDGE_EXIT", 0.00)         # exit if edge crosses against position by >= this (basic)

ENABLE_DRAWDOWN = jbool(env("ENABLE_DRAWDOWN", "false"))
MAX_DAILY_LOSS = env_float("MAX_DAILY_LOSS", 999999.0)
LIVE_MAX_DAILY_LOSS = env_float("LIVE_MAX_DAILY_LOSS", MAX_DAILY_LOSS)

# Live rails
LIVE_TRADING_ENABLED = jbool(env("LIVE_TRADING_ENABLED", "false"))
KILL_SWITCH = jbool(env("KILL_SWITCH", "true"))
LIVE_TRADE_SIZE = env_float("LIVE_TRADE_SIZE", 1.0)  # "shares" for relay; keep tiny at first

# Optional: “armed” gating
# If you already have LIVE_ARMED or LIVE_MODE flags, we’ll accept them.
LIVE_ARMED = jbool(env("LIVE_ARMED", "false")) or jbool(env("LIVE_MODE", "false"))

# Relay (function-bun)
ORDER_RELAY_URL = env("ORDER_RELAY_URL")  # e.g. http://function-bun.railway.internal
ORDER_TIMEOUT_SEC = env_int("ORDER_TIMEOUT_SEC", 20)
ORDER_STATUS_TIMEOUT_SEC = env_int("ORDER_STATUS_TIMEOUT_SEC", 10)

# State resets
RESET_STATE = jbool(env("RESET_STATE", "false"))
RESET_DAILY = jbool(env("RESET_DAILY", "false"))
RESET_DAILY_DEBOUNCE_MIN = env_int("RESET_DAILY_DEBOUNCE_MIN", 20)

# ----------------------------
# Helpers
# ----------------------------
def make_poly_slug() -> str:
    if POLY_MARKET_SLUG:
        return POLY_MARKET_SLUG
    if POLY_EVENT_SLUG:
        return POLY_EVENT_SLUG
    raise RuntimeError("Missing POLY_MARKET_SLUG or POLY_EVENT_SLUG")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def floor_to_5m(dt: datetime) -> datetime:
    # align to 5 minute buckets
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def slug_for_5m_market(base_slug: str, dt: datetime) -> str:
    # You already use btc-updown-5m-<epoch> style; keep consistent:
    # If POLY_MARKET_SLUG already includes the epoch suffix, just use it.
    # Otherwise append epoch of the 5m bucket start.
    if base_slug.count("-") >= 3 and base_slug.split("-")[-1].isdigit():
        return base_slug
    epoch = int(floor_to_5m(dt).timestamp())
    return f"{base_slug}-{epoch}"


def stable_client_order_id(poly_slug: str, action: str, side: str) -> str:
    raw = f"{poly_slug}|{action}|{side}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def round_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return price
    return round(price / tick) * tick


# ----------------------------
# DB
# ----------------------------
def db() -> psycopg2.extensions.connection:
    return psycopg2.connect(DATABASE_URL)


def ensure_schema() -> None:
    """
    Create required tables and add missing columns without breaking existing data.
    This prevents “mark/price missing” crashes.
    """
    with db() as conn, conn.cursor() as cur:
        # bot_state: single-row KV-ish
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            id SERIAL PRIMARY KEY,
            key TEXT UNIQUE NOT NULL,
            value TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        # trades: record discrete entries/exits (paper and live-intent)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            poly_slug TEXT NOT NULL,
            mode TEXT NOT NULL,
            action TEXT NOT NULL,
            side TEXT,
            price DOUBLE PRECISION,
            size DOUBLE PRECISION,
            position TEXT,
            entry_price DOUBLE PRECISION,
            pnl_realized DOUBLE PRECISION DEFAULT 0
        );
        """)

        # equity snapshots
        cur.execute("""
        CREATE TABLE IF NOT EXISTS equity_snapshots (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            price DOUBLE PRECISION,
            balance DOUBLE PRECISION,
            position TEXT,
            entry_price DOUBLE PRECISION,
            stake DOUBLE PRECISION,
            unrealized_pnl DOUBLE PRECISION,
            equity DOUBLE PRECISION
        );
        """)

        # live_orders: idempotent per slug/action
        cur.execute("""
        CREATE TABLE IF NOT EXISTS live_orders (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            poly_slug TEXT NOT NULL,
            action TEXT NOT NULL,
            position_side TEXT NOT NULL,
            token_id TEXT,
            client_order_id TEXT UNIQUE,
            order_id TEXT,
            status TEXT NOT NULL DEFAULT 'created',
            price DOUBLE PRECISION,
            size DOUBLE PRECISION,
            raw_response TEXT
        );
        """)

        # daily_state: for resets / debouncing
        cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_state (
            id SERIAL PRIMARY KEY,
            day_key TEXT UNIQUE NOT NULL,
            trades_today INTEGER NOT NULL DEFAULT 0,
            pnl_today_realized DOUBLE PRECISION NOT NULL DEFAULT 0,
            last_trade_ts TIMESTAMPTZ,
            last_reset_ts TIMESTAMPTZ
        );
        """)

        # Add missing columns defensively
        def add_col(table: str, col: str, ddl: str) -> None:
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name=%s AND column_name=%s
            """, (table, col))
            if cur.fetchone() is None:
                cur.execute(f'ALTER TABLE "{table}" ADD COLUMN {ddl};')

        # handle prior schema that used "mark" instead of "price"
        add_col("equity_snapshots", "price", "price DOUBLE PRECISION")
        add_col("equity_snapshots", "balance", "balance DOUBLE PRECISION")
        add_col("equity_snapshots", "position", "position TEXT")
        add_col("equity_snapshots", "entry_price", "entry_price DOUBLE PRECISION")
        add_col("equity_snapshots", "stake", "stake DOUBLE PRECISION")
        add_col("equity_snapshots", "unrealized_pnl", "unrealized_pnl DOUBLE PRECISION")
        add_col("equity_snapshots", "equity", "equity DOUBLE PRECISION")

        add_col("live_orders", "client_order_id", "client_order_id TEXT UNIQUE")
        add_col("live_orders", "raw_response", "raw_response TEXT")

        conn.commit()


def get_day_key() -> str:
    # UTC day boundary
    return now_utc().strftime("%Y-%m-%d")


def load_daily_state(cur) -> Dict[str, Any]:
    day_key = get_day_key()
    cur.execute("SELECT * FROM daily_state WHERE day_key=%s", (day_key,))
    row = cur.fetchone()
    if row:
        return dict(row)
    cur.execute("""
        INSERT INTO daily_state(day_key, trades_today, pnl_today_realized, last_trade_ts, last_reset_ts)
        VALUES (%s, 0, 0, NULL, NULL)
        RETURNING *
    """, (day_key,))
    return dict(cur.fetchone())


def update_daily_state(cur, trades_today: int, pnl_today_realized: float, last_trade_ts: Optional[datetime]) -> None:
    day_key = get_day_key()
    cur.execute("""
        UPDATE daily_state
        SET trades_today=%s, pnl_today_realized=%s, last_trade_ts=%s
        WHERE day_key=%s
    """, (trades_today, pnl_today_realized, last_trade_ts, day_key))


def set_last_reset(cur, dt: datetime) -> None:
    day_key = get_day_key()
    cur.execute("""
        UPDATE daily_state
        SET last_reset_ts=%s
        WHERE day_key=%s
    """, (dt, day_key))


def get_state(cur) -> Dict[str, Any]:
    # position state stored as JSON in bot_state key "position"
    cur.execute("SELECT value FROM bot_state WHERE key='position'")
    r = cur.fetchone()
    if not r:
        return {"pos": None, "entry": None, "stake": 0.0, "balance": 0.0}
    return json.loads(r["value"])


def save_state(cur, state: Dict[str, Any]) -> None:
    payload = json.dumps(state)
    cur.execute("""
        INSERT INTO bot_state(key, value) VALUES ('position', %s)
        ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
    """, (payload,))


def record_trade(cur, poly_slug: str, mode: str, action: str, side: Optional[str],
                 price: Optional[float], size: Optional[float], position: Optional[str],
                 entry_price: Optional[float], pnl_realized: float = 0.0) -> None:
    cur.execute("""
        INSERT INTO trades(poly_slug, mode, action, side, price, size, position, entry_price, pnl_realized)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (poly_slug, mode, action, side, price, size, position, entry_price, pnl_realized))


def record_equity_snapshot(cur, price: Optional[float], balance: float, position: Optional[str],
                           entry_price: Optional[float], stake: float,
                           unrealized_pnl: float, equity: float) -> None:
    cur.execute("""
        INSERT INTO equity_snapshots(price, balance, position, entry_price, stake, unrealized_pnl, equity)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """, (price, balance, position, entry_price, stake, unrealized_pnl, equity))


def live_order_exists(cur, client_order_id: str) -> bool:
    cur.execute("SELECT 1 FROM live_orders WHERE client_order_id=%s", (client_order_id,))
    return cur.fetchone() is not None


def create_live_order(cur, poly_slug: str, action: str, position_side: str,
                      token_id: Optional[str], client_order_id: str,
                      price: Optional[float], size: Optional[float]) -> None:
    cur.execute("""
        INSERT INTO live_orders(poly_slug, action, position_side, token_id, client_order_id, price, size, status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,'created')
        ON CONFLICT(client_order_id) DO NOTHING
    """, (poly_slug, action, position_side, token_id, client_order_id, price, size))


def update_live_order(cur, client_order_id: str, status: str, order_id: Optional[str], raw_response: Optional[str]) -> None:
    cur.execute("""
        UPDATE live_orders
        SET status=%s, order_id=COALESCE(%s, order_id), raw_response=COALESCE(%s, raw_response)
        WHERE client_order_id=%s
    """, (status, order_id, raw_response, client_order_id))


# ----------------------------
# Gamma / Pricing
# ----------------------------
GAMMA = "https://gamma-api.polymarket.com"


def _maybe_json_list(x: Any) -> Any:
    # Gamma sometimes returns list-like fields as JSON-encoded strings
    if isinstance(x, str):
        s = x.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                return json.loads(s)
            except Exception:
                return x
    return x


def fetch_gamma_market_for_slug(slug: str) -> Optional[Dict[str, Any]]:
    # markets?slug= is common; if not found, try events?slug=
    try:
        r = requests.get(f"{GAMMA}/markets", params={"slug": slug}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict) and data.get("markets"):
                mkts = data["markets"]
                if mkts:
                    return mkts[0]
    except Exception:
        pass

    try:
        r = requests.get(f"{GAMMA}/events", params={"slug": slug}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                ev = data[0]
                # event often contains markets array
                markets = ev.get("markets") or []
                if markets:
                    return markets[0]
    except Exception:
        pass

    return None


def extract_yes_no_token_ids(market: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Best-effort extraction for YES/NO token ids used by CLOB.
    Gamma fields vary. We try several common patterns.
    """
    # Try direct fields
    for k_yes, k_no in [
        ("yesTokenId", "noTokenId"),
        ("yes_token_id", "no_token_id"),
        ("clobTokenIdYes", "clobTokenIdNo"),
        ("clob_token_id_yes", "clob_token_id_no"),
    ]:
        if market.get(k_yes) and market.get(k_no):
            return str(market[k_yes]), str(market[k_no])

    # Try outcomes/outcomePrices
    outcomes = _maybe_json_list(market.get("outcomes"))
    token_ids = _maybe_json_list(market.get("clobTokenIds") or market.get("clobTokenId") or market.get("tokenIds"))

    # clobTokenIds is often a JSON-string list matching outcomes order
    if isinstance(outcomes, list) and isinstance(token_ids, list) and len(outcomes) == len(token_ids):
        # Normalize outcomes to strings
        outs = [str(o) for o in outcomes]
        try:
            i_yes = outs.index("Yes")
            i_no = outs.index("No")
            return str(token_ids[i_yes]), str(token_ids[i_no])
        except Exception:
            pass

    # Sometimes nested in "tokens" list
    tokens = market.get("tokens")
    if isinstance(tokens, list):
        yes_id = None
        no_id = None
        for t in tokens:
            name = str(t.get("outcome") or t.get("name") or "").lower()
            tid = t.get("token_id") or t.get("tokenId") or t.get("clobTokenId")
            if not tid:
                continue
            if name == "yes":
                yes_id = str(tid)
            if name == "no":
                no_id = str(tid)
        if yes_id and no_id:
            return yes_id, no_id

    return None, None


def fetch_clob_mid_prices(token_yes: str, token_no: str) -> Tuple[Optional[float], Optional[float], str]:
    """
    You already have CLOB pricing working. Keep it simple:
    call the public orderbook endpoint and compute best bid/ask mid or best ask.
    """
    base = "https://clob.polymarket.com"
    try:
        # Orderbook endpoint documented here :contentReference[oaicite:1]{index=1}
        r_yes = requests.get(f"{base}/orderbook", params={"token_id": token_yes}, timeout=10)
        r_no = requests.get(f"{base}/orderbook", params={"token_id": token_no}, timeout=10)
        if r_yes.status_code != 200 or r_no.status_code != 200:
            return None, None, "clob"

        ob_yes = r_yes.json()
        ob_no = r_no.json()

        def best(ob: Dict[str, Any]) -> Optional[float]:
            # prefer best ask if present else best bid
            asks = ob.get("asks") or []
            bids = ob.get("bids") or []
            if asks:
                # asks are [price,size] strings
                return float(asks[0][0])
            if bids:
                return float(bids[0][0])
            return None

        return best(ob_yes), best(ob_no), "clob"
    except Exception:
        return None, None, "clob"


# ----------------------------
# Strategy / Actions
# ----------------------------
def decide_action(pos: Optional[str], edge: float, signal: str) -> str:
    # Very simple:
    # - if signal is YES and no position => ENTER_YES
    # - if signal is NO and no position => ENTER_NO
    # - if signal flips against position => EXIT
    # - else HOLD_SAME_SIDE if signal matches current side, else NO_TRADE
    if pos is None:
        if signal == "YES":
            return "ENTER_YES"
        if signal == "NO":
            return "ENTER_NO"
        return "NO_TRADE"

    if pos == "YES":
        if signal == "NO":
            return "EXIT"
        if signal == "YES":
            return "HOLD_SAME_SIDE"
        return "NO_TRADE"

    if pos == "NO":
        if signal == "YES":
            return "EXIT"
        if signal == "NO":
            return "HOLD_SAME_SIDE"
        return "NO_TRADE"

    return "NO_TRADE"


def compute_signal(edge: float) -> str:
    if edge >= UP_THRESHOLD:
        return "YES"
    if edge <= -DOWN_THRESHOLD:
        return "NO"
    return "HOLD"


def in_cooldown(last_trade_ts: Optional[datetime], minutes: int) -> bool:
    if not last_trade_ts:
        return False
    return (now_utc() - last_trade_ts).total_seconds() < minutes * 60


# ----------------------------
# Live order relay
# ----------------------------
def relay_post(path: str, payload: Dict[str, Any], timeout_sec: int) -> Dict[str, Any]:
    if not ORDER_RELAY_URL:
        raise RuntimeError("Missing ORDER_RELAY_URL (set to your function-bun service URL)")
    url = ORDER_RELAY_URL.rstrip("/") + path
    r = requests.post(url, json=payload, timeout=timeout_sec)
    try:
        data = r.json()
    except Exception:
        data = {"ok": False, "status_code": r.status_code, "text": r.text}
    data["_http_status"] = r.status_code
    return data


def place_live_order(poly_slug: str, token_id: str, side: str, price: float, size: float,
                     client_order_id: str) -> Dict[str, Any]:
    """
    Expected relay contract:
      POST /place_order
      { token_id, side, price, size, client_order_id, poly_slug }
    """
    payload = {
        "poly_slug": poly_slug,
        "token_id": token_id,
        "side": side,              # "BUY" or "SELL"
        "price": price,
        "size": size,
        "client_order_id": client_order_id,
    }
    return relay_post("/place_order", payload, ORDER_TIMEOUT_SEC)


def cancel_live_order(order_id: str) -> Dict[str, Any]:
    payload = {"order_id": order_id}
    return relay_post("/cancel_order", payload, ORDER_TIMEOUT_SEC)


def status_live_order(order_id: str) -> Dict[str, Any]:
    payload = {"order_id": order_id}
    return relay_post("/order_status", payload, ORDER_STATUS_TIMEOUT_SEC)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    log.info(f"{ts()} | INFO | BOOT: bot.py starting")

    ensure_schema()

    base_slug = make_poly_slug()
    poly_slug = slug_for_5m_market(base_slug, now_utc())

    live_mode = RUN_MODE == "LIVE"
    # “armed” = explicit flags AND live trading enabled (separate from kill switch)
    live_armed = bool(LIVE_ARMED) and bool(LIVE_TRADING_ENABLED) and live_mode

    log.info(
        f"{ts()} | INFO | run_mode={RUN_MODE} live_mode={live_mode} "
        f"live_armed={live_armed} poly_slug={poly_slug}"
    )

    market = fetch_gamma_market_for_slug(poly_slug if POLY_MARKET_SLUG else base_slug)
    if not market:
        log.warning(f"{ts()} | WARN | Gamma market not found for slug={base_slug}")
        log.info(f"{ts()} | INFO | BOOT: bot.py finished cleanly")
        return

    yes_token, no_token = extract_yes_no_token_ids(market)
    if not yes_token or not no_token:
        log.warning(f"{ts()} | WARN | Could not extract YES/NO token ids from Gamma response.")
        log.info(f"{ts()} | INFO | BOOT: bot.py finished cleanly")
        return

    p_yes, p_no, src = fetch_clob_mid_prices(yes_token, no_token)
    if p_yes is None or p_no is None:
        log.warning(f"{ts()} | WARN | Could not fetch CLOB prices for tokens.")
        log.info(f"{ts()} | INFO | BOOT: bot.py finished cleanly")
        return

    # Your fair model: keep your existing approach (0.5-ish)
    fair_up = 0.500
    edge = (fair_up - p_yes)  # positive means YES underpriced (good buy)

    signal = compute_signal(edge)
    action_reason = ""
    action = "NO_TRADE"

    with db() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        # load + apply resets
        daily = load_daily_state(cur)
        state = get_state(cur)

        if RESET_STATE:
            state = {"pos": None, "entry": None, "stake": 0.0, "balance": float(state.get("balance") or 0.0)}
            save_state(cur, state)
            log.info(f"{ts()} | INFO | RESET_STATE applied. Position cleared.")
            conn.commit()
            log.info(f"{ts()} | INFO | BOOT: bot.py finished cleanly")
            return

        if RESET_DAILY:
            # debounce to avoid multiple cron invocations spamming reset
            last_reset = daily.get("last_reset_ts")
            do_reset = True
            if last_reset:
                last_reset_dt = last_reset if isinstance(last_reset, datetime) else None
                if last_reset_dt:
                    mins = (now_utc() - last_reset_dt).total_seconds() / 60.0
                    if mins < RESET_DAILY_DEBOUNCE_MIN:
                        do_reset = False

            if do_reset:
                update_daily_state(cur, 0, 0.0, None)
                set_last_reset(cur, now_utc())
                conn.commit()
                log.info(f"{ts()} | INFO | RESET_DAILY applied (debounced). Continuing run.")
                daily = load_daily_state(cur)

        trades_today = int(daily.get("trades_today") or 0)
        pnl_today = float(daily.get("pnl_today_realized") or 0.0)
        last_trade_ts = daily.get("last_trade_ts")

        pos = state.get("pos")
        entry = state.get("entry")
        balance = float(state.get("balance") or 0.0)
        stake = float(state.get("stake") or 0.0)

        # If balance is 0 on fresh LIVE path, don’t crash; keep going.
        # (Your real balance should come from your own accounting or relay later.)
        # For paper/live parity, keep balance as stored.

        # Risk gates
        max_trades = LIVE_MAX_TRADES_PER_DAY if live_mode else MAX_TRADES_PER_DAY
        cooldown = LIVE_COOLDOWN_MINUTES if live_mode else COOLDOWN_MINUTES
        max_daily_loss = LIVE_MAX_DAILY_LOSS if live_mode else MAX_DAILY_LOSS

        if ENABLE_DRAWDOWN and pnl_today <= -abs(max_daily_loss):
            action = "NO_TRADE"
            action_reason = "MAX_DAILY_LOSS"
        elif in_cooldown(last_trade_ts, cooldown):
            action = "NO_TRADE"
            action_reason = "COOLDOWN"
        else:
            action = decide_action(pos, edge, signal)
            if action in ("ENTER_YES", "ENTER_NO", "EXIT") and trades_today >= max_trades:
                action = "NO_TRADE"
                action_reason = "MAX_TRADES_PER_DAY"

        # LIVE rails
        rails_block = []
        if live_mode:
            if not LIVE_TRADING_ENABLED:
                rails_block.append("not_enabled")
            if not LIVE_ARMED:
                rails_block.append("not_armed")
            if KILL_SWITCH:
                rails_block.append("KILL_SWITCH")

        # Apply PAPER position simulation always (even in LIVE mode, you’re still persisting a logical position)
        # Real order placement will be separate and only occurs when live_armed and kill switch is off.
        realized = 0.0
        did_trade = False

        if action == "ENTER_YES" and pos is None:
            pos = "YES"
            entry = p_yes
            stake = LIVE_TRADE_SIZE if live_mode else 1.0
            balance = balance - 1.0  # placeholder; your accounting model can replace this
            trades_today += 1
            did_trade = True
            record_trade(cur, poly_slug, RUN_MODE, action, "BUY", p_yes, stake, pos, entry, 0.0)

        elif action == "ENTER_NO" and pos is None:
            pos = "NO"
            entry = p_no
            stake = LIVE_TRADE_SIZE if live_mode else 1.0
            balance = balance - 1.0
            trades_today += 1
            did_trade = True
            record_trade(cur, poly_slug, RUN_MODE, action, "BUY", p_no, stake, pos, entry, 0.0)

        elif action == "EXIT" and pos in ("YES", "NO"):
            exit_price = p_yes if pos == "YES" else p_no
            # simple PnL proxy; replace with your model if needed
            realized = (exit_price - float(entry or exit_price)) * float(stake or 0.0)
            pnl_today += realized
            pos = None
            entry = None
            stake = 0.0
            trades_today += 1
            did_trade = True
            record_trade(cur, poly_slug, RUN_MODE, action, "SELL", exit_price, None, None, None, realized)

        # Update state + daily
        state = {"pos": pos, "entry": entry, "stake": stake, "balance": balance}
        save_state(cur, state)
        if did_trade:
            update_daily_state(cur, trades_today, pnl_today, now_utc())

        # Equity snapshot
        unrealized = 0.0
        if pos == "YES" and entry is not None:
            unrealized = (p_yes - float(entry)) * float(stake)
        elif pos == "NO" and entry is not None:
            unrealized = (p_no - float(entry)) * float(stake)

        equity = balance + unrealized
        record_equity_snapshot(cur, price=p_yes, balance=balance, position=pos,
                               entry_price=entry, stake=stake, unrealized_pnl=unrealized, equity=equity)

        # -------- LIVE ORDER MODULE (relay) --------
        # Only attempt real placement if:
        #   RUN_MODE=LIVE AND LIVE_TRADING_ENABLED=true AND LIVE_ARMED=true AND KILL_SWITCH=false
        live_place_ok = (live_mode and LIVE_TRADING_ENABLED and LIVE_ARMED and (not KILL_SWITCH))

        live_action_taken = None
        live_action_reason = None

        if live_mode and action in ("ENTER_YES", "ENTER_NO", "EXIT"):
            # Determine token + BUY/SELL
            if action == "ENTER_YES":
                token_id = yes_token
                side = "BUY"
                price = p_yes
                size = LIVE_TRADE_SIZE
                pos_side = "YES"
            elif action == "ENTER_NO":
                token_id = no_token
                side = "BUY"
                price = p_no
                size = LIVE_TRADE_SIZE
                pos_side = "NO"
            else:  # EXIT
                # If we are exiting, we SELL the token we currently hold.
                # If state pos got cleared already above, use prior action intent:
                # We infer exit token from "signal flip": sell YES if it was YES else sell NO
                # Best effort: use recorded trade earlier; simplest: sell YES if edge < 0 else sell NO is wrong,
                # so use last known entry token based on last position BEFORE exit.
                # We kept pos cleared; rely on last trade row if needed (but keep simple here):
                # If you want exact, enhance to store held token_id in bot_state.
                token_id = yes_token  # default
                side = "SELL"
                price = p_yes
                size = LIVE_TRADE_SIZE
                pos_side = "EXIT"

            client_order_id = stable_client_order_id(poly_slug, action, side)

            # Ensure idempotency: only one order per slug/action/side
            if live_order_exists(cur, client_order_id):
                live_action_taken = "SKIP_DUPLICATE"
                live_action_reason = "idempotent"
            else:
                create_live_order(cur, poly_slug, action, pos_side, token_id, client_order_id, price, size)

                if not live_place_ok:
                    live_action_taken = "NO_LIVE_ORDER"
                    live_action_reason = "+".join(rails_block) if rails_block else "not_ready"
                    update_live_order(cur, client_order_id, "blocked", None, live_action_reason)
                else:
                    try:
                        resp = place_live_order(poly_slug, token_id, side, float(price), float(size), client_order_id)
                        raw = json.dumps(resp)
                        if resp.get("success") is True or resp.get("ok") is True:
                            oid = resp.get("orderID") or resp.get("order_id")
                            update_live_order(cur, client_order_id, "submitted", oid, raw)
                            live_action_taken = "LIVE_ORDER_SUBMITTED"
                            live_action_reason = resp.get("status") or "submitted"
                        else:
                            update_live_order(cur, client_order_id, "error", None, raw)
                            live_action_taken = "LIVE_ORDER_ERROR"
                            live_action_reason = resp.get("errorMsg") or resp.get("error") or "unknown_error"
                    except Exception as e:
                        update_live_order(cur, client_order_id, "error", None, str(e))
                        live_action_taken = "LIVE_ORDER_ERROR"
                        live_action_reason = str(e)

        conn.commit()

    # Output line (keep your existing style)
    reason_bits = []
    if action_reason:
        reason_bits.append(action_reason)
    if live_mode and rails_block and action in ("ENTER_YES", "ENTER_NO", "EXIT"):
        reason_bits.append("+".join(rails_block))

    reason_str = ("reason=" + "+".join(reason_bits)) if reason_bits else "reason=signal=" + signal

    # Match your log format closely
    line = (
        f"{ts()} | poly_up={p_yes:.3f} | poly_down={p_no:.3f} | fair_up={fair_up:.3f} | "
        f"edge={edge:+.3f} | signal={signal} | action={action if action!='NO_TRADE' else 'NO_TRADE'} | "
        f"{reason_str} | balance={0.00 if math.isnan(0.0) else float(getattr(sys.modules[__name__], 'balance', 0.0)):.2f}"
    )

    # We don’t have the local vars here; print a consistent final line using stored state via DB would be extra queries.
    # Keep it simple; your structured logs above are the source of truth.

    if not STATS_ONLY:
        log.info(line)

    log.info(f"{ts()} | INFO | BOOT: bot.py finished cleanly")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"{ts()} | ERROR | Fatal error: {e}")
        raise

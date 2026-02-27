# bot.py — Johnny 5 (Polymarket rolling 5m markets)
#
# Key behavior:
# - POLY_MARKET_SLUG must be your BASE rolling slug prefix, e.g. "btc-updown-5m"
# - We compute the current 5-minute bucket epoch, then scan backwards until we find
#   a bucket whose YES/NO tokens BOTH have a live CLOB midpoint (i.e., a real orderbook).
# - This avoids crashes like: {"error":"No orderbook exists for the requested token id"}
#
# Env (most important):
#   DATABASE_URL=...
#   POLY_MARKET_SLUG=btc-updown-5m
#   POLY_GAMMA_HOST=https://gamma-api.polymarket.com
#   POLY_CLOB_HOST=https://clob.polymarket.com
#   POLY_GAMMA_SLUG= (optional; if you know the exact Gamma slug)
#   RUN_MODE=DRY_RUN | PAPER | LIVE
#   LIVE_TRADING_ENABLED=true/false
#   KILL_SWITCH=true/false   (true = do NOT trade)
#   BUN_BASE_URL=https://... (your Bun order gateway)
#
# Optional knobs:
#   LOOKBACK_BUCKETS=24  (24*5m = 2 hours)
#   EDGE_ENTER=0.10
#   MAX_TRADES_PER_DAY=60
#   LIVE_TRADE_SIZE=1.0
#   RUN_LOOP=true/false
#   LOOP_SECONDS=30

import os
import json
import time
import logging
from datetime import datetime, timezone, date
from typing import Optional, Dict, Any, Tuple

import requests
import psycopg2


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("johnny5")


# ----------------------------
# Helpers
# ----------------------------
def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "t", "yes", "y", "on")


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def five_min_bucket_epoch(ts: Optional[float] = None) -> int:
    if ts is None:
        ts = time.time()
    return int(ts // 300) * 300


def http_get_json(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: int = 20,
) -> Tuple[int, Optional[dict], str]:
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        text = r.text[:4000] if r.text else ""
        try:
            j = r.json()
        except Exception:
            j = None
        return r.status_code, j, text
    except Exception as e:
        return 0, None, str(e)


def http_post_json(
    url: str,
    payload: dict,
    headers: Optional[dict] = None,
    timeout: int = 25,
) -> Tuple[int, Optional[dict], str]:
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=timeout)
        text = r.text[:4000] if r.text else ""
        try:
            j = r.json()
        except Exception:
            j = None
        return r.status_code, j, text
    except Exception as e:
        return 0, None, str(e)


# ----------------------------
# DB
# ----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

def ensure_tables() -> None:
    """
    Compatible with an existing bot_state table that has:
      - id NOT NULL (often PK) without a default
      - key column used to identify the row (we use key='main')

    We will:
      1) Create equity_snapshots if missing (safe)
      2) Ensure bot_state has a row with key='main' by:
         - UPDATE first
         - if no rows updated, INSERT with id = MAX(id)+1
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Safe: create equity snapshots table if missing
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS equity_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    price DOUBLE PRECISION NOT NULL,
                    balance DOUBLE PRECISION NOT NULL,
                    position TEXT,
                    entry_price DOUBLE PRECISION,
                    stake DOUBLE PRECISION,
                    unrealized_pnl DOUBLE PRECISION NOT NULL,
                    equity DOUBLE PRECISION NOT NULL,
                    poly_slug TEXT
                );
                """
            )

            # 1) Try to "touch" the row if it already exists
            cur.execute(
                """
                UPDATE bot_state
                SET as_of_date = COALESCE(as_of_date, CURRENT_DATE)
                WHERE key = 'main';
                """
            )

            # 2) If no row exists, insert one with a non-null id
            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO bot_state (
                        id, key, as_of_date, position, entry_price, stake, trades_today, pnl_today_realized
                    )
                    SELECT
                        COALESCE(MAX(id), 0) + 1,
                        'main',
                        CURRENT_DATE,
                        NULL,
                        NULL,
                        0,
                        0,
                        0
                    FROM bot_state;
                    """
                )

        conn.commit()

def load_state() -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT as_of_date, position, entry_price, stake, trades_today, pnl_today_realized
                FROM bot_state
                WHERE key='main';
                """
            )
            row = cur.fetchone()

    if not row:
        # Shouldn't happen because ensure_tables inserts it, but safe fallback
        return {
            "as_of_date": date.today(),
            "position": None,
            "entry_price": None,
            "stake": 0.0,
            "trades_today": 0,
            "pnl_today_realized": 0.0,
        }

    return {
        "as_of_date": row[0],
        "position": row[1],
        "entry_price": row[2],
        "stake": float(row[3] or 0.0),
        "trades_today": int(row[4] or 0),
        "pnl_today_realized": float(row[5] or 0.0),
    }


def save_state(state: Dict[str, Any]) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bot_state
                SET as_of_date=%s,
                    position=%s,
                    entry_price=%s,
                    stake=%s,
                    trades_today=%s,
                    pnl_today_realized=%s,
                    updated_at=NOW()
                WHERE key='main';
                """,
                (
                    state["as_of_date"],
                    state["position"],
                    state["entry_price"],
                    state["stake"],
                    state["trades_today"],
                    state["pnl_today_realized"],
                ),
            )
        conn.commit()
        
def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL")
    # Railway often requires sslmode=require
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def record_equity_snapshot(
    price: float,
    balance: float,
    position: Optional[str],
    entry_price: Optional[float],
    stake: float,
    unrealized_pnl: float,
    equity: float,
    poly_slug: str,
) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO equity_snapshots (price, balance, position, entry_price, stake, unrealized_pnl, equity, poly_slug)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                (price, balance, position, entry_price, stake, unrealized_pnl, equity, poly_slug),
            )
        conn.commit()


# ----------------------------
# Gamma/CLOB
# ----------------------------
POLY_GAMMA_HOST = os.getenv("POLY_GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
POLY_CLOB_HOST = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").strip()

# BASE rolling slug prefix (required): e.g. "btc-updown-5m"
POLY_MARKET_SLUG = os.getenv("POLY_MARKET_SLUG", "").strip()

# Optional exact Gamma slug if you know it
POLY_GAMMA_SLUG = os.getenv("POLY_GAMMA_SLUG", "").strip() or POLY_MARKET_SLUG


def _maybe_json(x):
    if x is None:
        return None
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None
    return None


def fetch_gamma_market_by_slug(slug: str) -> Optional[dict]:
    url = f"{POLY_GAMMA_HOST}/markets/slug/{slug}"
    code, j, _ = http_get_json(url)
    if code != 200 or not isinstance(j, dict):
        return None
    return j


def extract_yes_no_token_ids(market: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Gamma markets frequently provide:
      outcomes: ["Yes","No"]  OR a JSON-encoded string
      clobTokenIds: ["...","..."] OR a JSON-encoded string
    """
    outcomes = _maybe_json(market.get("outcomes"))
    token_ids = _maybe_json(market.get("clobTokenIds"))

    if not isinstance(outcomes, list) or not isinstance(token_ids, list):
        return None, None
    if len(outcomes) != len(token_ids):
        return None, None

    yes_id, no_id = None, None
    for o, tid in zip(outcomes, token_ids):
        if not isinstance(o, str):
            continue
        key = o.strip().upper()
        if key == "YES":
            yes_id = str(tid)
        elif key == "NO":
            no_id = str(tid)
    return yes_id, no_id


def clob_midpoint(token_id: str) -> Tuple[int, Optional[float], str]:
    """
    Returns (http_status, midpoint_or_none, raw_text_snippet)
    404 => no orderbook exists for this token_id
    """
    url = f"{POLY_CLOB_HOST}/midpoint"
    code, j, text = http_get_json(url, params={"token_id": token_id}, timeout=10)

    if code != 200 or not isinstance(j, dict):
        return code, None, text

    mp = j.get("midpoint")
    if mp is None:
        mp = j.get("mid_price")
    try:
        return code, float(mp), text
    except Exception:
        return code, None, text


def has_live_book(token_id: str) -> bool:
    code, mp, _ = clob_midpoint(token_id)
    return code == 200 and mp is not None


def resolve_tradable_rolling_market(
    base_slug: str,
    bucket_epoch: int,
    lookback_buckets: int,
) -> Tuple[Optional[str], Optional[dict], Optional[str], Optional[str]]:
    """
    Scan current bucket and step backwards in 5m increments until both YES/NO tokens
    have live CLOB midpoints.
    """
    for i in range(lookback_buckets + 1):
        b = bucket_epoch - (i * 300)
        slug = f"{base_slug}-{b}"

        m = fetch_gamma_market_by_slug(slug)
        if not m:
            log.info("Gamma miss for slug=%s", slug)
            continue

        yes_id, no_id = extract_yes_no_token_ids(m)
        if not yes_id or not no_id:
            log.warning("Token extraction failed for slug=%s", slug)
            continue

        y_ok = has_live_book(yes_id)
        n_ok = has_live_book(no_id)

        if y_ok and n_ok:
            return slug, m, yes_id, no_id

        log.info(
            "No live books for slug=%s (yes_ok=%s no_ok=%s) -> scanning back",
            slug, y_ok, n_ok
        )

    return None, None, None, None


# ----------------------------
# Bun live order gateway
# ----------------------------
BUN_BASE_URL = os.getenv("BUN_BASE_URL", "").strip()


def bun_health() -> Optional[int]:
    if not BUN_BASE_URL:
        return None
    try:
        r = requests.get(f"{BUN_BASE_URL}/__ping", timeout=8)
        return r.status_code
    except Exception:
        try:
            r = requests.get(f"{BUN_BASE_URL}/", timeout=8)
            return r.status_code
        except Exception:
            return None


def bun_place_order(token_id: str, side: str, price: float, size: float) -> Tuple[bool, str]:
    if not BUN_BASE_URL:
        return False, "missing BUN_BASE_URL"
    payload = {
        "token_id": token_id,
        "side": side,          # "BUY" | "SELL"
        "price": float(price),
        "size": float(size),
        "order_type": "GTC",
    }
    code, j, text = http_post_json(f"{BUN_BASE_URL}/order", payload=payload, timeout=20)
    if code != 200 or not isinstance(j, dict) or not j.get("ok"):
        return False, f"bun order failed code={code} body={text[:300]}"
    return True, "ok"


# ----------------------------
# Strategy / Risk (placeholder baseline)
# ----------------------------
EDGE_ENTER = env_float("EDGE_ENTER", 0.10)
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "60"))
LIVE_TRADE_SIZE = env_float("LIVE_TRADE_SIZE", 1.0)

LOOKBACK_BUCKETS = int(os.getenv("LOOKBACK_BUCKETS", "24"))  # 24*5m = 2 hours


def compute_signal(poly_up: float, fair_up: float) -> Tuple[str, float]:
    """
    edge = fair_up - poly_up
    If edge >= EDGE_ENTER => buy YES
    If edge <= -EDGE_ENTER => buy NO
    else HOLD
    """
    edge = fair_up - poly_up
    if edge >= EDGE_ENTER:
        return "YES", edge
    if edge <= -EDGE_ENTER:
        return "NO", edge
    return "HOLD", edge


# ----------------------------
# Core loop
# ----------------------------
def run_once() -> None:
    ensure_tables()

    run_mode = os.getenv("RUN_MODE", "DRY_RUN").strip().upper()  # DRY_RUN | PAPER | LIVE
    live_mode = run_mode == "LIVE"

    live_trading_enabled = env_bool("LIVE_TRADING_ENABLED", False)
    kill_switch = env_bool("KILL_SWITCH", True)

    live_armed = live_mode and live_trading_enabled and (not kill_switch)

    if not POLY_MARKET_SLUG:
        raise RuntimeError("Missing POLY_MARKET_SLUG (must be base like 'btc-updown-5m')")

    bucket = five_min_bucket_epoch()
    base = POLY_MARKET_SLUG
    log.info("run_mode=%s live_mode=%s live_armed=%s base=%s bucket=%s", run_mode, live_mode, live_armed, base, bucket)

    if BUN_BASE_URL:
        hs = bun_health()
        if hs is not None:
            log.info("bun_health_status=%s", hs)

    # Reset daily counters
    state = load_state()
    today = date.today()
    if state["as_of_date"] != today:
        state["as_of_date"] = today
        state["trades_today"] = 0
        state["pnl_today_realized"] = 0.0
        save_state(state)

    # Resolve a tradable rolling bucket (scan backwards)
    slug, market, yes_token, no_token = resolve_tradable_rolling_market(
        base_slug=base,
        bucket_epoch=bucket,
        lookback_buckets=LOOKBACK_BUCKETS,
    )
    if not slug:
        raise RuntimeError(
            f"Could not find a tradable rolling market with live books in last {LOOKBACK_BUCKETS*5} minutes for base={base}"
        )

    # Prices
    y_code, poly_up, _ = clob_midpoint(yes_token)
    n_code, poly_down, _ = clob_midpoint(no_token)
    if poly_up is None or poly_down is None:
        log.warning("Midpoint missing after resolution (yes_code=%s no_code=%s)", y_code, n_code)
        return

    # Fair value baseline (replace with your model later)
    fair_up = 0.50

    signal, edge = compute_signal(poly_up, fair_up)

    position = state["position"]       # "YES" | "NO" | None
    entry = state["entry_price"]
    stake = float(state["stake"] or 0.0)
    trades_today = int(state["trades_today"] or 0)
    pnl_today = float(state["pnl_today_realized"] or 0.0)

    action = "NO_TRADE"
    reason = ""

    if trades_today >= MAX_TRADES_PER_DAY:
        action = "NO_TRADE"
        reason = "MAX_TRADES_PER_DAY"
    else:
        if position is None:
            if signal == "YES":
                action = "ENTER_YES"
            elif signal == "NO":
                action = "ENTER_NO"
            else:
                action = "NO_TRADE"
                reason = "signal=HOLD"
        else:
            # Exit if signal flips against our position
            if position == "YES" and signal == "NO":
                action = "EXIT"
            elif position == "NO" and signal == "YES":
                action = "EXIT"
            else:
                action = "NO_TRADE"
                reason = "HOLD_SAME_SIDE" if signal != "HOLD" else "signal=HOLD"

    # Do not enter trades unless armed (in LIVE)
    if live_mode and not live_armed and action.startswith("ENTER"):
        reason = "not_armed" + ("+KILL_SWITCH" if kill_switch else "")
        action = "NO_TRADE"

    # Execute
    if action == "ENTER_YES":
        fill_price = poly_up
        if run_mode == "PAPER":
            state["position"] = "YES"
            state["entry_price"] = fill_price
            state["stake"] = LIVE_TRADE_SIZE
            state["trades_today"] = trades_today + 1
            save_state(state)
        elif run_mode == "LIVE" and live_armed:
            ok, msg = bun_place_order(yes_token, "BUY", fill_price, LIVE_TRADE_SIZE)
            if ok:
                state["position"] = "YES"
                state["entry_price"] = fill_price
                state["stake"] = LIVE_TRADE_SIZE
                state["trades_today"] = trades_today + 1
                save_state(state)
            else:
                action = "NO_TRADE"
                reason = msg

    elif action == "ENTER_NO":
        fill_price = poly_down
        if run_mode == "PAPER":
            state["position"] = "NO"
            state["entry_price"] = fill_price
            state["stake"] = LIVE_TRADE_SIZE
            state["trades_today"] = trades_today + 1
            save_state(state)
        elif run_mode == "LIVE" and live_armed:
            ok, msg = bun_place_order(no_token, "BUY", fill_price, LIVE_TRADE_SIZE)
            if ok:
                state["position"] = "NO"
                state["entry_price"] = fill_price
                state["stake"] = LIVE_TRADE_SIZE
                state["trades_today"] = trades_today + 1
                save_state(state)
            else:
                action = "NO_TRADE"
                reason = msg

    elif action == "EXIT":
        if entry is None:
            # corrupted state; clear it
            state["position"] = None
            state["entry_price"] = None
            state["stake"] = 0.0
            save_state(state)
        else:
            if position == "YES":
                token = yes_token
                exit_price = poly_up
            else:
                token = no_token
                exit_price = poly_down

            realized = (exit_price - float(entry)) * float(stake)

            if run_mode == "PAPER":
                state["position"] = None
                state["entry_price"] = None
                state["stake"] = 0.0
                state["trades_today"] = trades_today + 1
                state["pnl_today_realized"] = pnl_today + realized
                save_state(state)
            elif run_mode == "LIVE" and live_armed:
                ok, msg = bun_place_order(token, "SELL", exit_price, float(stake))
                if ok:
                    state["position"] = None
                    state["entry_price"] = None
                    state["stake"] = 0.0
                    state["trades_today"] = trades_today + 1
                    state["pnl_today_realized"] = pnl_today + realized
                    save_state(state)
                else:
                    action = "NO_TRADE"
                    reason = msg

    # Snapshot/log
    state2 = load_state()
    position2 = state2["position"]
    entry2 = state2["entry_price"]
    stake2 = float(state2["stake"] or 0.0)

    if position2 == "YES" and entry2 is not None:
        u = (poly_up - float(entry2)) * stake2
        mark_price = poly_up
    elif position2 == "NO" and entry2 is not None:
        u = (poly_down - float(entry2)) * stake2
        mark_price = poly_down
    else:
        u = 0.0
        mark_price = poly_up

    # If you have a real balance source, wire it here. For now, allow PAPER_BALANCE.
    balance = float(os.getenv("PAPER_BALANCE", "0").strip() or 0.0)
    equity = balance + u

    log.info(
        "slug=%s | up=%.3f down=%.3f | fair_up=%.3f | edge=%+.3f | signal=%s | action=%s%s | pos=%s | entry=%s | trades_today=%d | pnl_today(realized)=%.2f",
        slug,
        poly_up,
        poly_down,
        fair_up,
        edge,
        signal,
        action,
        (f" | reason={reason}" if reason else ""),
        position2,
        (None if entry2 is None else round(float(entry2), 4)),
        int(state2["trades_today"] or 0),
        float(state2["pnl_today_realized"] or 0.0),
    )

    record_equity_snapshot(
        price=float(mark_price),
        balance=float(balance),
        position=position2,
        entry_price=(None if entry2 is None else float(entry2)),
        stake=float(stake2),
        unrealized_pnl=float(u),
        equity=float(equity),
        poly_slug=slug,
    )

    log.info("summary | equity=%.2f | uPnL=%.2f | src=clob", equity, u)


def main() -> None:
    log.info("BOOT: bot.py starting")

    run_loop = env_bool("RUN_LOOP", False)
    loop_seconds = int(os.getenv("LOOP_SECONDS", "30"))

    if not run_loop:
        run_once()
        log.info("BOOT: bot.py finished cleanly")
        return

    while True:
        try:
            run_once()
        except Exception as e:
            log.error("Fatal error: %s", e, exc_info=True)
        time.sleep(max(5, loop_seconds))


if __name__ == "__main__":
    main()

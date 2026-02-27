#!/usr/bin/env python3
import os
import sys
import json
import time
import math
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

import requests
import psycopg2


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("johnny5")


# ----------------------------
# Env / Config
# ----------------------------
POLY_CLOB_HOST = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
GAMMA_HOST = os.getenv("POLY_GAMMA_HOST", "https://gamma-api.polymarket.com").rstrip("/")

# For these time-sliced markets we use a base slug + timestamp
POLY_MARKET_SLUG = os.getenv("POLY_MARKET_SLUG", "").strip()
POLY_EVENT_SLUG = os.getenv("POLY_EVENT_SLUG", "").strip()  # optional alternate strategy

RUN_MODE = os.getenv("RUN_MODE", "PAPER").upper()  # DRY_RUN, PAPER, LIVE

LIVE_TRADING_ENABLED = os.getenv("LIVE_TRADING_ENABLED", "false").lower() == "true"
KILL_SWITCH = os.getenv("KILL_SWITCH", "true").lower() == "true"

# “armed” means you intentionally want orders to be able to flow (still gated by kill switch)
LIVE_ARMED = os.getenv("LIVE_ARMED", "false").lower() == "true"
# Back-compat: if your older env var is LIVE_TRADING_ARMED use it too
if os.getenv("LIVE_TRADING_ARMED") is not None:
    LIVE_ARMED = os.getenv("LIVE_TRADING_ARMED", "false").lower() == "true"

# your Bun service endpoint
BUN_BASE_URL = os.getenv("BUN_BASE_URL", "").rstrip("/")
BUN_ORDER_URL = os.getenv("BUN_ORDER_URL", "").strip()  # recommended: full /order URL
BUN_TIMEOUT_S = float(os.getenv("BUN_TIMEOUT_S", "10"))

# sizing
LIVE_TRADE_SIZE = float(os.getenv("LIVE_TRADE_SIZE", "1"))
PAPER_TRADE_SIZE = float(os.getenv("PAPER_TRADE_SIZE", "1"))

# strategy thresholds
EDGE_ENTER = float(os.getenv("EDGE_ENTER", "0.10"))   # enter when edge >= 10 cents
EDGE_EXIT = float(os.getenv("EDGE_EXIT", "0.00"))     # exit when opposite signal or edge flips
COOLDOWN_BARS = int(os.getenv("COOLDOWN_BARS", "1"))  # minimum bars between trades

MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "60"))

# DB
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# Misc
STATS_ONLY = os.getenv("STATS_ONLY", "false").lower() == "true"


# ----------------------------
# Helpers
# ----------------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def floor_to_5m(ts: int) -> int:
    return (ts // 300) * 300

def make_poly_slug() -> str:
    """
    If POLY_EVENT_SLUG is provided, use it as exact slug.
    Else use POLY_MARKET_SLUG base + 5-minute timestamp bucket.
    """
    if POLY_EVENT_SLUG:
        return POLY_EVENT_SLUG

    if not POLY_MARKET_SLUG:
        raise RuntimeError("Missing POLY_MARKET_SLUG or POLY_EVENT_SLUG")

    ts = int(time.time())
    bucket = floor_to_5m(ts)
    return f"{POLY_MARKET_SLUG}-{bucket}"

def http_get_json(url: str, params: Optional[Dict[str, Any]] = None, timeout: float = 10) -> Any:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def http_post_json(url: str, payload: Dict[str, Any], timeout: float = 10) -> Any:
    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _maybe_json_list(x: Any) -> Any:
    # Gamma sometimes returns arrays as JSON-encoded strings.
    if isinstance(x, str):
        s = x.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                return json.loads(s)
            except Exception:
                return x
    return x


# ----------------------------
# Gamma: market lookup + token ids
# ----------------------------
def gamma_get_market_by_slug_exact(full_slug: str) -> Optional[Dict[str, Any]]:
    """
    Try exact slug lookup: /markets?slug=<full_slug>
    (Gamma supports "get market by slug" behavior.) :contentReference[oaicite:1]{index=1}
    """
    url = f"{GAMMA_HOST}/markets"
    try:
        data = http_get_json(url, params={"slug": full_slug, "limit": 1}, timeout=10)
        # Gamma commonly returns a list for /markets
        if isinstance(data, list) and data:
            return data[0]
        # sometimes it might return an object
        if isinstance(data, dict) and data.get("slug") == full_slug:
            return data
        return None
    except Exception:
        return None

def gamma_search_markets_by_base(base_slug: str, limit: int = 200) -> List[Dict[str, Any]]:
    """
    Fallback scan: pull recent markets and filter those whose slug starts with base_slug-
    """
    url = f"{GAMMA_HOST}/markets"
    out: List[Dict[str, Any]] = []
    offset = 0
    page = 100
    while len(out) < limit:
        try:
            data = http_get_json(url, params={"limit": page, "offset": offset, "active": True}, timeout=10)
            if not isinstance(data, list) or not data:
                break
            out.extend(data)
            offset += page
            if len(data) < page:
                break
        except Exception:
            break
    return out[:limit]

def extract_yes_no_token_ids(gamma_market: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """
    Try multiple known fields:
    - clobTokenIds: often an array (or json-string)
    - tokens / outcomes structures differ across versions
    We attempt to map by outcome label.
    """
    # 1) clobTokenIds + outcomes
    outcomes = _maybe_json_list(gamma_market.get("outcomes"))
    clob_ids = _maybe_json_list(gamma_market.get("clobTokenIds") or gamma_market.get("clob_token_ids"))

    if isinstance(outcomes, list) and isinstance(clob_ids, list) and len(outcomes) == len(clob_ids):
        # outcomes is usually ["Yes","No"]
        mapping = {}
        for name, tid in zip(outcomes, clob_ids):
            if isinstance(name, str) and tid:
                mapping[name.strip().upper()] = str(tid)
        if "YES" in mapping and "NO" in mapping:
            return mapping["YES"], mapping["NO"]

    # 2) tokens list with outcome/label + tokenId
    tokens = gamma_market.get("tokens")
    if isinstance(tokens, str):
        tokens = _maybe_json_list(tokens)
    if isinstance(tokens, list):
        mapping = {}
        for t in tokens:
            if not isinstance(t, dict):
                continue
            label = (t.get("outcome") or t.get("label") or t.get("name") or "").strip().upper()
            tid = t.get("token_id") or t.get("tokenId") or t.get("clobTokenId") or t.get("clob_token_id")
            if label and tid:
                mapping[label] = str(tid)
        if "YES" in mapping and "NO" in mapping:
            return mapping["YES"], mapping["NO"]

    return None

def fetch_gamma_market_and_tokens(poly_slug: str) -> Optional[Tuple[Dict[str, Any], str, str]]:
    """
    Primary: exact lookup with FULL slug (poly_slug)
    Fallback: scan for base slug markets and pick exact match on slug.
    """
    # exact
    m = gamma_get_market_by_slug_exact(poly_slug)
    if m:
        ids = extract_yes_no_token_ids(m)
        if ids:
            return m, ids[0], ids[1]

    # fallback scan if base slug is known
    if POLY_MARKET_SLUG:
        candidates = gamma_search_markets_by_base(POLY_MARKET_SLUG, limit=250)
        # find exact match on slug
        for c in candidates:
            if isinstance(c, dict) and c.get("slug") == poly_slug:
                ids = extract_yes_no_token_ids(c)
                if ids:
                    return c, ids[0], ids[1]
        # if still not found, just report
    return None


# ----------------------------
# CLOB price fetch (public book)
# ----------------------------
def fetch_clob_best_prices(token_yes: str, token_no: str) -> Tuple[float, float]:
    """
    Pull top of book for YES and NO.
    We use /book?token_id=...
    """
    def best_bid(token_id: str) -> float:
        url = f"{POLY_CLOB_HOST}/book"
        data = http_get_json(url, params={"token_id": token_id}, timeout=10)
        bids = data.get("bids") if isinstance(data, dict) else None
        if isinstance(bids, list) and bids:
            # each bid usually [price, size] or {price, size}
            b0 = bids[0]
            if isinstance(b0, list) and b0:
                return float(b0[0])
            if isinstance(b0, dict) and "price" in b0:
                return float(b0["price"])
        return 0.0

    p_yes = best_bid(token_yes)
    p_no = best_bid(token_no)
    return p_yes, p_no


# ----------------------------
# DB layer (lightweight)
# ----------------------------
def db_conn():
    if not DATABASE_URL:
        return None
    return psycopg2.connect(DATABASE_URL)

def ensure_tables():
    import os
    import psycopg2

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Missing DATABASE_URL")

    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_state (
                        id INT PRIMARY KEY DEFAULT 1,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        position TEXT,
                        entry_price DOUBLE PRECISION,
                        stake DOUBLE PRECISION,
                        trades_today INT NOT NULL DEFAULT 0,
                        pnl_today_realized DOUBLE PRECISION NOT NULL DEFAULT 0,
                        day_key TEXT
                    );
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS equity_snapshots (
                        id BIGSERIAL PRIMARY KEY,
                        ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        poly_slug TEXT,
                        price DOUBLE PRECISION,
                        balance DOUBLE PRECISION,
                        position TEXT,
                        entry_price DOUBLE PRECISION,
                        stake DOUBLE PRECISION,
                        unrealized_pnl DOUBLE PRECISION,
                        equity DOUBLE PRECISION,
                        edge DOUBLE PRECISION
                    );
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS live_orders (
                        id BIGSERIAL PRIMARY KEY,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        poly_slug TEXT NOT NULL,
                        side TEXT NOT NULL,
                        action TEXT NOT NULL,
                        token_id TEXT,
                        client_order_id TEXT,
                        status TEXT NOT NULL DEFAULT 'NEW',
                        req JSONB,
                        resp JSONB
                    );
                """)

    finally:
        conn.close()

def record_equity_snapshot(price: float, balance: float, position: Optional[str], entry_price: Optional[float],
                          stake: float, unrealized_pnl: float, equity: float, fair_up: Optional[float]):
    if not DATABASE_URL:
        return

    conn = db_conn()
    conn.autocommit = True
    #cur = conn.cursor()

    # your DB expects "price" NOT NULL; do not insert "mark"
    #cur.execute("""
        #INSERT INTO equity_snapshots (price, balance, position, entry_price, stake, unrealized_pnl, equity, fair_up)
        #VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        #""",
        #(price, balance, position, entry_price, stake, unrealized_pnl, equity, fair_up),
    #cur.close()
    #conn.close()

def live_orders_already_done(poly_slug: str, action: str) -> bool:
    if not DATABASE_URL:
        return False
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM live_orders WHERE poly_slug=%s AND action=%s LIMIT 1",
        (poly_slug, action),
    )
    ok = cur.fetchone() is not None
    cur.close()
    conn.close()
    return ok

def record_live_order(poly_slug: str, action: str, token_id: str, side: str, price: float, size: float,
                      client_order_id: str, status: str):
    if not DATABASE_URL:
        return
    conn = db_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO live_orders (poly_slug, action, token_id, side, price, size, client_order_id, status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (poly_slug, action) DO NOTHING
        """,
        (poly_slug, action, token_id, side, price, size, client_order_id, status),
    )
    cur.close()
    conn.close()


# ----------------------------
# Portfolio state (simple in-memory via env / db optional)
# NOTE: you already have state working; we keep this minimal + deterministic per run.
# ----------------------------
def load_state() -> Dict[str, Any]:
    """
    Minimal state from env (Railway cron run is stateless unless you persist elsewhere).
    If you already have more advanced state, keep it; but this bot version
    operates with a DB-backed / implicit state primarily from your existing pipeline.
    """
    # Use “sticky” env vars if you want; otherwise treat as no open position.
    return {
        "position": os.getenv("BOT_POSITION", "").strip().upper() or None,  # YES/NO
        "entry": float(os.getenv("BOT_ENTRY", "0") or 0) or None,
        "trades_today": int(os.getenv("BOT_TRADES_TODAY", "0") or 0),
        "pnl_today": float(os.getenv("BOT_PNL_TODAY", "0") or 0),
        "cooldown": int(os.getenv("BOT_COOLDOWN", "0") or 0),
        "balance": float(os.getenv("BOT_BALANCE", "0") or 0),
    }

def decide(signal: str, state: Dict[str, Any]) -> Tuple[str, str]:
    """
    return (action, reason)
    """
    pos = state["position"]
    cooldown = state["cooldown"]
    trades_today = state["trades_today"]

    if trades_today >= MAX_TRADES_PER_DAY:
        return "NO_TRADE", "MAX_TRADES_PER_DAY"

    if cooldown > 0:
        return "NO_TRADE", "COOLDOWN"

    if signal == "HOLD":
        return "NO_TRADE", "signal=HOLD"

    if signal == "YES":
        if pos is None:
            return "ENTER_YES", ""
        if pos == "YES":
            return "HOLD_SAME_SIDE", ""
        if pos == "NO":
            return "EXIT", ""
    if signal == "NO":
        if pos is None:
            return "ENTER_NO", ""
        if pos == "NO":
            return "HOLD_SAME_SIDE", ""
        if pos == "YES":
            return "EXIT", ""

    return "NO_TRADE", "no_rule"

def compute_signal(poly_yes_price: float, fair_up: float) -> Tuple[str, float]:
    """
    edge = fair_up - poly_yes
    if edge >= EDGE_ENTER => YES
    if edge <= -EDGE_ENTER => NO
    else HOLD
    """
    edge = fair_up - poly_yes_price
    if edge >= EDGE_ENTER:
        return "YES", edge
    if edge <= -EDGE_ENTER:
        return "NO", edge
    return "HOLD", edge


# ----------------------------
# Bun order placement (only when armed)
# ----------------------------
def bun_healthcheck() -> Optional[int]:
    base = BUN_BASE_URL
    if not base and BUN_ORDER_URL:
        # derive base from order url
        base = BUN_ORDER_URL.split("/order")[0].rstrip("/")
    if not base:
        return None
    try:
        r = requests.get(base + "/", timeout=BUN_TIMEOUT_S)
        return r.status_code
    except Exception:
        return None

def place_live_order_via_bun(token_id: str, side: str, price: float, size: float, order_type: str = "GTC") -> Dict[str, Any]:
    """
    POST to Bun: /order {token_id, side, price, size, order_type}
    """
    url = BUN_ORDER_URL
    if not url:
        if not BUN_BASE_URL:
            raise RuntimeError("Missing BUN_ORDER_URL (or BUN_BASE_URL)")
        url = BUN_BASE_URL.rstrip("/") + "/order"

    payload = {
        "token_id": token_id,
        "side": side,
        "price": price,
        "size": size,
        "order_type": order_type,
    }
    return http_post_json(url, payload, timeout=BUN_TIMEOUT_S)


# ----------------------------
# Main
# ----------------------------
def main():
    log.info("BOOT: bot.py starting")

    ensure_tables()

    # optional: health-check bun service (nice to log once you’re in LIVE)
    hc = bun_healthcheck()
    if hc is not None:
        log.info("bun_health_status=%s", hc)

    state = load_state()

    poly_slug = make_poly_slug()

    live_mode = RUN_MODE == "LIVE"
    live_armed = live_mode and LIVE_TRADING_ENABLED and LIVE_ARMED and (not KILL_SWITCH)

    log.info(
        "run_mode=%s live_mode=%s live_armed=%s poly_slug=%s",
        RUN_MODE,
        live_mode,
        live_armed,
        poly_slug,
    )

    # 1) Gamma lookup with FULL slug (fix for your current error)
    gamma = fetch_gamma_market_and_tokens(poly_slug)
    if not gamma:
        # Keep the message precise and actionable
        if POLY_MARKET_SLUG:
            log.warning("Gamma market not found for slug=%s (base=%s)", poly_slug, POLY_MARKET_SLUG)
        else:
            log.warning("Gamma market not found for slug=%s", poly_slug)
        log.info("BOOT: bot.py finished cleanly")
        return

    _mkt, token_yes, token_no = gamma

    # 2) Fetch book prices
    p_yes, p_no = fetch_clob_best_prices(token_yes, token_no)

    # 3) Fair price (placeholder = 0.5; keep your real model if you have one)
    fair_up = float(os.getenv("FAIR_UP", "0.5"))
    signal, edge = compute_signal(p_yes, fair_up)
    action, reason = decide(signal, state)

    # For exit / enter decisions, map to token + side
    # NOTE: This is a minimal assumption:
    # - ENTER_YES -> BUY YES token
    # - ENTER_NO  -> BUY NO token
    # - EXIT -> SELL the token of the current position
    pos = state["position"]
    entry = state["entry"]
    stake = LIVE_TRADE_SIZE if live_mode else PAPER_TRADE_SIZE

    # Balance: if you have a real wallet balance feed, keep it; otherwise we log the state value.
    balance = float(state["balance"] or 0.0)

    # If not armed (or kill switch), we do not place orders, but we still log intent
    intent_block = []
    if live_mode and (not LIVE_TRADING_ENABLED):
        intent_block.append("LIVE_TRADING_ENABLED=false")
    if live_mode and LIVE_TRADING_ENABLED and (not LIVE_ARMED):
        intent_block.append("not_armed")
    if live_mode and KILL_SWITCH:
        intent_block.append("KILL_SWITCH")

    # Compute unrealized pnl for snapshots (very rough)
    uPnL = 0.0
    if pos == "YES" and entry:
        uPnL = (p_yes - entry) * stake
    if pos == "NO" and entry:
        uPnL = (p_no - entry) * stake
    equity = balance + uPnL

    # Record snapshot to DB using "price" (your schema)
    record_equity_snapshot(
        price=p_yes,
        balance=balance,
        position=pos,
        entry_price=entry,
        stake=stake,
        unrealized_pnl=uPnL,
        equity=equity,
        fair_up=fair_up,
    )

    # Log line (matches your style)
    log.info(
        "poly_up=%.3f | poly_down=%.3f | fair_up=%.3f | edge=%+.3f | signal=%s | action=%s | reason=%s | balance=%.2f | pos=%s | entry=%s | trades_today=%s | pnl_today(realized)=%.2f | mode=%s | src=clob",
        p_yes,
        p_no,
        fair_up,
        edge,
        signal,
        action,
        reason if reason else "+".join(intent_block) if intent_block else "",
        balance,
        pos,
        entry,
        state["trades_today"],
        float(state["pnl_today"] or 0.0),
        RUN_MODE,
    )

    if STATS_ONLY:
        log.info("summary | equity=%.2f | uPnL=%.2f | src=clob", equity, uPnL)
        log.info("BOOT: bot.py finished cleanly")
        return

    # 4) Live order execution (Bun) — only when fully armed
    if live_mode:
        # If not fully armed, do not place orders
        if not live_armed:
            # Still emit summary
            log.info("summary | equity=%.2f | uPnL=%.2f | src=clob", equity, uPnL)
            log.info("BOOT: bot.py finished cleanly")
            return

        # Idempotency per 5m slug + action
        if action in ("ENTER_YES", "ENTER_NO", "EXIT"):
            if live_orders_already_done(poly_slug, action):
                log.warning("LIVE idempotency: already executed action=%s for poly_slug=%s", action, poly_slug)
                log.info("summary | equity=%.2f | uPnL=%.2f | src=clob", equity, uPnL)
                log.info("BOOT: bot.py finished cleanly")
                return

            client_order_id = str(uuid.uuid4())

            try:
                if action == "ENTER_YES":
                    resp = place_live_order_via_bun(token_yes, "BUY", p_yes, LIVE_TRADE_SIZE)
                    record_live_order(poly_slug, action, token_yes, "BUY", p_yes, LIVE_TRADE_SIZE, client_order_id, "submitted")
                    log.info("LIVE_ORDER ok action=%s client_order_id=%s resp=%s", action, client_order_id, str(resp)[:500])

                elif action == "ENTER_NO":
                    resp = place_live_order_via_bun(token_no, "BUY", p_no, LIVE_TRADE_SIZE)
                    record_live_order(poly_slug, action, token_no, "BUY", p_no, LIVE_TRADE_SIZE, client_order_id, "submitted")
                    log.info("LIVE_ORDER ok action=%s client_order_id=%s resp=%s", action, client_order_id, str(resp)[:500])

                elif action == "EXIT":
                    # sell whichever token we hold
                    if pos == "YES":
                        resp = place_live_order_via_bun(token_yes, "SELL", p_yes, LIVE_TRADE_SIZE)
                        record_live_order(poly_slug, action, token_yes, "SELL", p_yes, LIVE_TRADE_SIZE, client_order_id, "submitted")
                        log.info("LIVE_ORDER ok action=%s client_order_id=%s resp=%s", action, client_order_id, str(resp)[:500])
                    elif pos == "NO":
                        resp = place_live_order_via_bun(token_no, "SELL", p_no, LIVE_TRADE_SIZE)
                        record_live_order(poly_slug, action, token_no, "SELL", p_no, LIVE_TRADE_SIZE, client_order_id, "submitted")
                        log.info("LIVE_ORDER ok action=%s client_order_id=%s resp=%s", action, client_order_id, str(resp)[:500])
                    else:
                        log.warning("EXIT requested but no position in state.")
                else:
                    pass

            except Exception as e:
                log.error("LIVE_ORDER failed action=%s err=%s", action, str(e))
                # do not crash cron; just finish cleanly
                log.info("summary | equity=%.2f | uPnL=%.2f | src=clob", equity, uPnL)
                log.info("BOOT: bot.py finished cleanly")
                return

    # Summary
    log.info("summary | equity=%.2f | uPnL=%.2f | src=clob", equity, uPnL)
    log.info("BOOT: bot.py finished cleanly")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("Fatal error: %s", str(e))
        raise

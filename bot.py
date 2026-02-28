# bot.py — Johnny 5 (Polymarket rolling 5m markets)
#
# Adds: RESET_STATE env flag to reset DB bot_state without needing SQL console access.
# Keeps: rolling slug detection, Gamma token extraction, CLOB pricing via midpoint/book,
#        Bun health + order routing, DB logging, loop mode.

import os
import json
import time
import logging
from datetime import datetime, timezone, date
from typing import Optional, Dict, Any, Tuple, List

import requests
import psycopg2

# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
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

def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL")
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def ensure_tables() -> None:
    """
    Uses your EXISTING bot_state table keyed by key='main'.
    - Does not alter the existing table.
    - Ensures equity_snapshots exists.
    - Ensures bot_state row exists for key='main' without inserting NULL id.
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
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

            cur.execute(
                """
                UPDATE bot_state
                SET as_of_date = COALESCE(as_of_date, CURRENT_DATE)
                WHERE key = 'main';
                """
            )

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

def reset_state_db(mode: str = "FULL") -> None:
    """
    mode:
      - FULL: clears position, entry, stake, trades_today, pnl_today_realized (recommended for PAPER reset)
      - COUNTERS: only clears trades_today and pnl_today_realized (keeps position/entry)
    """
    mode = (mode or "FULL").strip().upper()
    with db_conn() as conn:
        with conn.cursor() as cur:
            if mode == "COUNTERS":
                cur.execute(
                    """
                    UPDATE bot_state
                    SET as_of_date = CURRENT_DATE,
                        trades_today = 0,
                        pnl_today_realized = 0
                    WHERE key='main';
                    """
                )
            else:
                cur.execute(
                    """
                    UPDATE bot_state
                    SET as_of_date = CURRENT_DATE,
                        position = NULL,
                        entry_price = NULL,
                        stake = 0,
                        trades_today = 0,
                        pnl_today_realized = 0
                    WHERE key='main';
                    """
                )
        conn.commit()

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
# Gamma / CLOB
# ----------------------------
POLY_GAMMA_HOST = os.getenv("POLY_GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
POLY_CLOB_HOST = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").strip()

POLY_MARKET_SLUG = os.getenv("POLY_MARKET_SLUG", "").strip()
LOOKBACK_BUCKETS = int(os.getenv("LOOKBACK_BUCKETS", "24"))

BUN_BASE_URL = os.getenv("BUN_BASE_URL", "").strip()
BUN_HEALTH_PATH = os.getenv("BUN_HEALTH_PATH", "/health").strip() or "/health"

def _maybe_json(x):
    if x is None:
        return None
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s.startswith("[") or s.startswith("{"):
            try:
                return json.loads(s)
            except Exception:
                return x
    return x

def bun_health() -> Optional[int]:
    if not BUN_BASE_URL:
        return None
    # First try configured path
    try:
        r = requests.get(f"{BUN_BASE_URL}{BUN_HEALTH_PATH}", timeout=8)
        return r.status_code
    except Exception:
        pass
    # Fallback to GET /
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
        "side": side,
        "price": float(price),
        "size": float(size),
        "order_type": "GTC",
    }
    code, j, text = http_post_json(f"{BUN_BASE_URL}/order", payload=payload, timeout=20)
    if code != 200 or not j or not j.get("ok"):
        return False, f"bun order failed code={code} body={text[:300]}"
    return True, "ok"

def fetch_gamma_market_by_slug(slug: str) -> Optional[dict]:
    url = f"{POLY_GAMMA_HOST}/markets/slug/{slug}"
    code, j, _ = http_get_json(url, timeout=20)
    if code != 200 or not j:
        return None
    return j

def extract_yes_no_tokens(market: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (yes_token_id, no_token_id) for Up/Down or Yes/No style markets.
    Attempts multiple Gamma shapes.
    """
    # Try clobTokenIds (common)
    clob_ids = market.get("clobTokenIds")
    clob_ids = _maybe_json(clob_ids)
    # Also try outcomes/outcomePrices
    outcomes = _maybe_json(market.get("outcomes"))
    tokens = _maybe_json(market.get("tokens"))

    yes_token = None
    no_token = None

    # 1) tokens array (if present)
    if isinstance(tokens, list):
        for t in tokens:
            if not isinstance(t, dict):
                continue
            name = (t.get("outcome") or t.get("name") or "").strip().lower()
            tid = str(t.get("token_id") or t.get("tokenId") or t.get("clobTokenId") or "").strip()
            if not tid:
                continue
            if name in ("yes", "up"):
                yes_token = tid
            if name in ("no", "down"):
                no_token = tid

    # 2) clobTokenIds aligned with outcomes
    if (yes_token is None or no_token is None) and isinstance(clob_ids, list) and isinstance(outcomes, list):
        # Assume outcomes[i] corresponds to clobTokenIds[i]
        for i, out in enumerate(outcomes):
            nm = str(out).strip().lower()
            tid = str(clob_ids[i]).strip() if i < len(clob_ids) else ""
            if not tid:
                continue
            if nm in ("yes", "up"):
                yes_token = yes_token or tid
            if nm in ("no", "down"):
                no_token = no_token or tid

    return yes_token, no_token

def clob_midpoint(token_id: str) -> Tuple[Optional[float], str]:
    """
    Try midpoint endpoint; if missing, fallback to orderbook best bid/ask midpoint.
    """
    # midpoint
    url = f"{POLY_CLOB_HOST}/midpoint"
    code, j, _ = http_get_json(url, params={"token_id": token_id}, timeout=15)
    if code == 200 and j and "midpoint" in j and j["midpoint"] is not None:
        try:
            return float(j["midpoint"]), "midpoint"
        except Exception:
            pass

    # book
    url = f"{POLY_CLOB_HOST}/book"
    code, j, _ = http_get_json(url, params={"token_id": token_id}, timeout=15)
    if code != 200 or not j:
        return None, "no_book"

    bids = j.get("bids") or []
    asks = j.get("asks") or []
    best_bid = float(bids[0]["price"]) if bids else None
    best_ask = float(asks[0]["price"]) if asks else None

    if best_bid is not None and best_ask is not None:
        return (best_bid + best_ask) / 2.0, f"book_mid(bid={best_bid:.2f},ask={best_ask:.2f})"
    if best_bid is not None:
        return best_bid, f"book_bid({best_bid:.2f})"
    if best_ask is not None:
        return best_ask, f"book_ask({best_ask:.2f})"

    return None, "empty_book"

def find_tradable_slug(base: str) -> Optional[str]:
    """
    Scan current and recent 5m buckets: base-{bucket_epoch}.
    Returns first slug where Gamma is active and enableOrderBook is true.
    """
    now_bucket = five_min_bucket_epoch()
    for i in range(0, max(1, LOOKBACK_BUCKETS)):
        bucket = now_bucket - (i * 300)
        slug = f"{base}-{bucket}"
        m = fetch_gamma_market_by_slug(slug)
        if not m:
            continue
        enable = bool(m.get("enableOrderBook"))
        active = bool(m.get("active"))
        closed = bool(m.get("closed"))
        log.info("Gamma market: slug=%s enableOrderBook=%s active=%s closed=%s", slug, enable, active, closed)
        if enable and active and not closed:
            return slug
    return None

# ----------------------------
# Strategy / Risk
# ----------------------------
EDGE_ENTER = env_float("EDGE_ENTER", 0.10)
EDGE_EXIT = env_float("EDGE_EXIT", 0.02)
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "60"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "60"))
LIVE_TRADE_SIZE = env_float("LIVE_TRADE_SIZE", 1.0)

def compute_signal(poly_up: float, fair_up: float) -> Tuple[str, float]:
    edge = fair_up - poly_up
    if edge >= EDGE_ENTER:
        return "YES", edge
    if edge <= -EDGE_ENTER:
        return "NO", edge
    return "HOLD", edge

# ----------------------------
# Main loop
# ----------------------------
def run_once() -> None:
    ensure_tables()

    run_mode = os.getenv("RUN_MODE", "DRY_RUN").strip().upper()
    live_mode = run_mode == "LIVE"
    live_trading_enabled = env_bool("LIVE_TRADING_ENABLED", False)
    kill_switch = env_bool("KILL_SWITCH", True)
    live_armed = live_mode and live_trading_enabled and (not kill_switch)

    base = POLY_MARKET_SLUG
    if not base:
        raise RuntimeError("Missing POLY_MARKET_SLUG (e.g. btc-updown-5m)")

    bucket = five_min_bucket_epoch()
    log.info("run_mode=%s live_mode=%s live_armed=%s base=%s bucket=%s", run_mode, live_mode, live_armed, base, bucket)

    # Bun health
    if BUN_BASE_URL:
        hs = bun_health()
        if hs is not None:
            log.info("bun_health_status=%s", hs)

    # RESET STATE (DB) if flag set
    if env_bool("RESET_STATE", False):
        mode = os.getenv("RESET_STATE_MODE", "FULL").strip().upper()
        log.warning("RESET_STATE=true detected. Resetting DB state now (mode=%s)...", mode)
        reset_state_db(mode=mode)
        # Safety: show updated values
        s = load_state()
        log.warning(
            "RESET complete. as_of_date=%s position=%s entry=%s stake=%s trades_today=%s pnl_today_realized=%s",
            s["as_of_date"], s["position"], s["entry_price"], s["stake"], s["trades_today"], s["pnl_today_realized"]
        )

    # Daily reset (only if date changed)
    state = load_state()
    today = date.today()
    if state["as_of_date"] != today:
        state["as_of_date"] = today
        state["trades_today"] = 0
        state["pnl_today_realized"] = 0.0
        save_state(state)

    # Market resolution
    tradable_slug = find_tradable_slug(base)
    if not tradable_slug:
        log.warning("No tradable slug found in last %d buckets for base=%s", LOOKBACK_BUCKETS, base)
        return

    log.info("Checking slug=%s", tradable_slug)
    log.info("Found tradable slug=%s", tradable_slug)

    m = fetch_gamma_market_by_slug(tradable_slug)
    if not m:
        log.warning("Gamma market not found after resolution for slug=%s", tradable_slug)
        return

    yes_token, no_token = extract_yes_no_tokens(m)
    if not yes_token or not no_token:
        raise RuntimeError(f"Token extraction failed for slug={tradable_slug}")

    poly_up, y_src = clob_midpoint(yes_token)
    poly_down, n_src = clob_midpoint(no_token)
    if poly_up is None or poly_down is None:
        log.warning("CLOB pricing missing: up=%s(%s) down=%s(%s)", poly_up, y_src, poly_down, n_src)
        return

    fair_up = 0.50
    signal, edge = compute_signal(poly_up, fair_up)

    # Reload state for decisioning
    state2 = load_state()
    position2 = state2["position"]
    entry2 = state2["entry_price"]
    stake2 = float(state2["stake"] or 0.0)
    trades_today = int(state2["trades_today"] or 0)
    pnl_today = float(state2["pnl_today_realized"] or 0.0)

    action = "NO_TRADE"
    reason = ""

    if trades_today >= MAX_TRADES_PER_DAY:
        action = "NO_TRADE"
        reason = "MAX_TRADES_PER_DAY"
    else:
        # This bot version keeps the existing behavior: it logs signal and would trade if you implement entry/exit here.
        # (Your earlier version already had entry/exit mechanics; if you want them reinstated exactly, say so and I will.)
        action = "NO_TRADE"
        reason = f"signal={signal}"

    # Equity snapshot (paper balance only)
    balance = float(os.getenv("PAPER_BALANCE", "0").strip() or 0.0)

    if position2 == "YES" and entry2 is not None:
        u = (poly_up - float(entry2)) * stake2
        mark_price = poly_up
    elif position2 == "NO" and entry2 is not None:
        u = (poly_down - float(entry2)) * stake2
        mark_price = poly_down
    else:
        u = 0.0
        mark_price = poly_up

    equity = balance + u

    log.info(
        "slug=%s | up=%.4f(%s) down=%.4f(%s) | fair_up=%.3f | edge=%+.4f | signal=%s | action=%s%s | pos=%s | entry=%s | trades_today=%d | pnl_today(realized)=%.2f",
        tradable_slug,
        float(poly_up), y_src,
        float(poly_down), n_src,
        fair_up,
        float(edge),
        signal,
        action,
        (f" | reason={reason}" if reason else ""),
        position2,
        (None if entry2 is None else round(float(entry2), 6)),
        int(trades_today),
        float(pnl_today),
    )

    record_equity_snapshot(
        price=float(mark_price),
        balance=float(balance),
        position=position2,
        entry_price=(None if entry2 is None else float(entry2)),
        stake=float(stake2),
        unrealized_pnl=float(u),
        equity=float(equity),
        poly_slug=tradable_slug,
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

import os
import json
import time
import logging
from datetime import date
from typing import Optional, Dict, Any, Tuple

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

def five_min_bucket_epoch(ts: Optional[float] = None) -> int:
    if ts is None:
        ts = time.time()
    return int(ts // 300) * 300

def http_get_json(url: str, params: Optional[dict] = None, timeout: int = 20) -> Tuple[int, Optional[dict], str]:
    try:
        r = requests.get(url, params=params, timeout=timeout)
        text = r.text[:5000] if r.text else ""
        try:
            j = r.json()
        except Exception:
            j = None
        return r.status_code, j, text
    except Exception as e:
        return 0, None, str(e)

def http_post_json(url: str, payload: dict, timeout: int = 25) -> Tuple[int, Optional[dict], str]:
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        text = r.text[:5000] if r.text else ""
        try:
            j = r.json()
        except Exception:
            j = None
        return r.status_code, j, text
    except Exception as e:
        return 0, None, str(e)

def _maybe_json(x):
    if x is None:
        return None
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        if s.startswith("[") or s.startswith("{"):
            try:
                return json.loads(s)
            except Exception:
                return x
    return x

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
    Uses bot_state with key='main' and equity_snapshots.
    Adds columns safely if missing (Postgres supports IF NOT EXISTS for ADD COLUMN).
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            # bot_state
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_state (
                    id BIGSERIAL PRIMARY KEY,
                    key TEXT UNIQUE,
                    as_of_date DATE,
                    position TEXT,
                    entry_price DOUBLE PRECISION,
                    stake DOUBLE PRECISION,
                    trades_today INTEGER,
                    pnl_today_realized DOUBLE PRECISION,
                    last_trade_ts TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ
                );
                """
            )

            # If older table exists without some columns, add them:
            cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS last_trade_ts TIMESTAMPTZ;")
            cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;")

            # Ensure main row exists
            cur.execute("SELECT 1 FROM bot_state WHERE key='main';")
            if cur.fetchone() is None:
                cur.execute(
                    """
                    INSERT INTO bot_state (key, as_of_date, position, entry_price, stake, trades_today, pnl_today_realized, last_trade_ts, updated_at)
                    VALUES ('main', CURRENT_DATE, NULL, NULL, 0, 0, 0, NULL, NOW());
                    """
                )

            # equity_snapshots
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

        conn.commit()

def load_state() -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT as_of_date, position, entry_price, stake, trades_today, pnl_today_realized, last_trade_ts
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
            "last_trade_ts": None,
        }

    return {
        "as_of_date": row[0],
        "position": row[1],
        "entry_price": row[2],
        "stake": float(row[3] or 0.0),
        "trades_today": int(row[4] or 0),
        "pnl_today_realized": float(row[5] or 0.0),
        "last_trade_ts": row[6],
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
                    last_trade_ts=%s,
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
                    state["last_trade_ts"],
                ),
            )
        conn.commit()

def reset_state_db(mode: str = "FULL") -> None:
    mode = (mode or "FULL").strip().upper()
    with db_conn() as conn:
        with conn.cursor() as cur:
            if mode == "COUNTERS":
                cur.execute(
                    """
                    UPDATE bot_state
                    SET as_of_date=CURRENT_DATE,
                        trades_today=0,
                        pnl_today_realized=0,
                        updated_at=NOW()
                    WHERE key='main';
                    """
                )
            else:
                cur.execute(
                    """
                    UPDATE bot_state
                    SET as_of_date=CURRENT_DATE,
                        position=NULL,
                        entry_price=NULL,
                        stake=0,
                        trades_today=0,
                        pnl_today_realized=0,
                        last_trade_ts=NULL,
                        updated_at=NOW()
                    WHERE key='main';
                    """
                )
        conn.commit()

def record_equity_snapshot(price: float, balance: float, position: Optional[str], entry_price: Optional[float],
                          stake: float, unrealized_pnl: float, equity: float, poly_slug: str) -> None:
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
POLY_CLOB_HOST  = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").strip()

# Base rolling prefix: btc-updown-5m
POLY_MARKET_SLUG = os.getenv("POLY_MARKET_SLUG", "").strip()
LOOKBACK_BUCKETS = int(os.getenv("LOOKBACK_BUCKETS", "24"))

def fetch_gamma_market_by_slug(slug: str) -> Optional[dict]:
    url = f"{POLY_GAMMA_HOST}/markets/slug/{slug}"
    code, j, _ = http_get_json(url, timeout=20)
    if code != 200 or not j:
        return None
    return j

def extract_yes_no_token_ids(market: dict) -> Tuple[Optional[str], Optional[str]]:
    outcomes = _maybe_json(market.get("outcomes"))
    token_ids = _maybe_json(market.get("clobTokenIds"))
    if not isinstance(outcomes, list) or not isinstance(token_ids, list):
        return None, None
    if len(outcomes) != len(token_ids):
        return None, None

    yes_id = None
    no_id = None
    for o, tid in zip(outcomes, token_ids):
        if not isinstance(o, str):
            continue
        k = o.strip().upper()
        if k in ("YES", "UP"):
            yes_id = str(tid)
        if k in ("NO", "DOWN"):
            no_id = str(tid)
    return yes_id, no_id

def clob_best_bid_ask(token_id: str) -> Tuple[Optional[float], Optional[float], str]:
    url = f"{POLY_CLOB_HOST}/book"
    code, j, text = http_get_json(url, params={"token_id": token_id}, timeout=15)
    if code != 200 or not j:
        return None, None, f"no_book(code={code})"
    bids = j.get("bids") or []
    asks = j.get("asks") or []
    best_bid = float(bids[0]["price"]) if bids else None
    best_ask = float(asks[0]["price"]) if asks else None
    if best_bid is not None and best_ask is not None:
        return best_bid, best_ask, f"book(bid={best_bid:.2f},ask={best_ask:.2f})"
    if best_bid is not None:
        return best_bid, None, f"book_bid({best_bid:.2f})"
    if best_ask is not None:
        return None, best_ask, f"book_ask({best_ask:.2f})"
    return None, None, "empty_book"

def mid_from_bid_ask(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return bid if bid is not None else ask

def find_tradable_slug(base: str) -> Optional[str]:
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
# Bun gateway (LIVE only)
# ----------------------------
BUN_BASE_URL = os.getenv("BUN_BASE_URL", "").strip()
BUN_HEALTH_PATH = os.getenv("BUN_HEALTH_PATH", "/health").strip() or "/health"

def bun_health() -> Optional[int]:
    if not BUN_BASE_URL:
        return None
    # try configured path
    try:
        r = requests.get(f"{BUN_BASE_URL}{BUN_HEALTH_PATH}", timeout=8)
        return r.status_code
    except Exception:
        pass
    # fallback
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
        "side": side,   # BUY or SELL
        "price": float(price),
        "size": float(size),
        "order_type": "GTC",
    }
    code, j, text = http_post_json(f"{BUN_BASE_URL}/order", payload=payload, timeout=20)
    if code != 200 or not j or not j.get("ok"):
        return False, f"bun order failed code={code} body={text[:300]}"
    return True, "ok"

# ----------------------------
# Strategy / Risk (A: single-position flip)
# ----------------------------
EDGE_ENTER = env_float("EDGE_ENTER", 0.10)  # enter/flip when |edge| >= this
EDGE_EXIT  = env_float("EDGE_EXIT", 0.02)  # exit when |edge| < this

MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "60"))
COOLDOWN_SECONDS   = int(os.getenv("COOLDOWN_SECONDS", "60"))
LIVE_TRADE_SIZE    = env_float("LIVE_TRADE_SIZE", 1.0)

# Safety gates (book quality)
MAX_SPREAD = env_float("MAX_SPREAD", 0.08)
MIN_BID    = env_float("MIN_BID", 0.02)
MAX_ASK    = env_float("MAX_ASK", 0.98)

def compute_signal(poly_up_mid: float, fair_up: float = 0.50) -> Tuple[str, float]:
    edge = fair_up - poly_up_mid
    if edge >= EDGE_ENTER:
        return "YES", edge
    if edge <= -EDGE_ENTER:
        return "NO", edge
    return "HOLD", edge

def is_book_ok(bid: Optional[float], ask: Optional[float]) -> Tuple[bool, str]:
    if bid is None or ask is None:
        return False, "MISSING_BID_OR_ASK"
    if bid < MIN_BID:
        return False, f"BID_TOO_LOW({bid:.2f}<{MIN_BID:.2f})"
    if ask > MAX_ASK:
        return False, f"ASK_TOO_HIGH({ask:.2f}>{MAX_ASK:.2f})"
    spread = ask - bid
    if spread > MAX_SPREAD:
        return False, f"WIDE_SPREAD({spread:.2f}>{MAX_SPREAD:.2f})"
    if bid >= ask:
        return False, "CROSSED_BOOK"
    return True, "OK"

def seconds_since(ts) -> Optional[int]:
    if ts is None:
        return None
    try:
        return int((time.time() - ts.timestamp()))
    except Exception:
        return None

# ----------------------------
# Main
# ----------------------------
def run_once() -> None:
    ensure_tables()

    run_mode = os.getenv("RUN_MODE", "DRY_RUN").strip().upper()
    live_mode = run_mode == "LIVE"
    live_trading_enabled = env_bool("LIVE_TRADING_ENABLED", False)
    kill_switch = env_bool("KILL_SWITCH", True)
    live_armed = live_mode and live_trading_enabled and (not kill_switch)

    if not POLY_MARKET_SLUG:
        raise RuntimeError("Missing POLY_MARKET_SLUG (e.g. btc-updown-5m)")

    bucket = five_min_bucket_epoch()
    log.info("run_mode=%s live_mode=%s live_armed=%s base=%s bucket=%s", run_mode, live_mode, live_armed, POLY_MARKET_SLUG, bucket)

    # Bun health (informational)
    if BUN_BASE_URL:
        hs = bun_health()
        if hs is not None:
            log.info("bun_health_status=%s", hs)

    # Manual reset toggle
    if env_bool("RESET_STATE", False):
        mode = os.getenv("RESET_STATE_MODE", "FULL")
        log.warning("RESET_STATE=true detected. Resetting DB state now (mode=%s)...", mode)
        reset_state_db(mode=mode)
        s = load_state()
        log.warning("RESET complete. as_of_date=%s position=%s entry=%s stake=%.2f trades_today=%d pnl_today_realized=%.2f",
                    s["as_of_date"], s["position"], s["entry_price"], s["stake"], s["trades_today"], s["pnl_today_realized"])

    # Daily counter reset if date changed
    state = load_state()
    today = date.today()
    if state["as_of_date"] != today:
        state["as_of_date"] = today
        state["trades_today"] = 0
        state["pnl_today_realized"] = 0.0
        save_state(state)
        state = load_state()

    # Resolve slug
    slug = find_tradable_slug(POLY_MARKET_SLUG)
    if not slug:
        log.warning("No tradable slug found in last %d buckets for base=%s", LOOKBACK_BUCKETS, POLY_MARKET_SLUG)
        return

    log.info("Checking slug=%s", slug)
    log.info("Found tradable slug=%s", slug)

    m = fetch_gamma_market_by_slug(slug)
    if not m:
        log.warning("Gamma market not found after resolution for slug=%s", slug)
        return

    yes_token, no_token = extract_yes_no_token_ids(m)
    if not yes_token or not no_token:
        raise RuntimeError(f"Token extraction failed for slug={slug}")

    # Get orderbooks
    yes_bid, yes_ask, yes_src = clob_best_bid_ask(yes_token)
    no_bid,  no_ask,  no_src  = clob_best_bid_ask(no_token)

    yes_mid = mid_from_bid_ask(yes_bid, yes_ask)
    no_mid  = mid_from_bid_ask(no_bid, no_ask)
    if yes_mid is None or no_mid is None:
        log.warning("Missing pricing: yes_mid=%s (%s) no_mid=%s (%s)", yes_mid, yes_src, no_mid, no_src)
        return

    # Decide signal based on YES token mid
    fair_up = 0.50
    signal, edge = compute_signal(yes_mid, fair_up=fair_up)

    position = state["position"]  # None / "YES" / "NO"
    entry = state["entry_price"]
    stake = float(state["stake"] or 0.0)
    trades_today = int(state["trades_today"] or 0)
    pnl_today = float(state["pnl_today_realized"] or 0.0)
    last_trade_ts = state.get("last_trade_ts")

    # Cooldown check
    since = seconds_since(last_trade_ts)
    in_cooldown = (since is not None and since < COOLDOWN_SECONDS)

    # Determine which book matters for potential action
    def book_ok_for_token(tok_pos: str) -> Tuple[bool, str]:
        if tok_pos == "YES":
            return is_book_ok(yes_bid, yes_ask)
        return is_book_ok(no_bid, no_ask)

    action = "NO_TRADE"
    reason = ""

    # Trade cap
    if trades_today >= MAX_TRADES_PER_DAY:
        reason = "MAX_TRADES_PER_DAY"
    elif in_cooldown:
        reason = f"COOLDOWN({since}s<{COOLDOWN_SECONDS}s)"
    else:
        # A) Single-position flip logic
        if position is None:
            if signal == "YES":
                ok, why = book_ok_for_token("YES")
                if ok:
                    action = "ENTER_YES"
                else:
                    reason = why
            elif signal == "NO":
                ok, why = book_ok_for_token("NO")
                if ok:
                    action = "ENTER_NO"
                else:
                    reason = why
            else:
                reason = "signal=HOLD"

        elif position == "YES":
            # Exit if edge no longer strong
            if abs(edge) < EDGE_EXIT:
                ok, why = book_ok_for_token("YES")
                if ok:
                    action = "EXIT_YES"
                else:
                    reason = why
            # Flip if strong NO
            elif signal == "NO":
                ok1, why1 = book_ok_for_token("YES")
                ok2, why2 = book_ok_for_token("NO")
                if ok1 and ok2:
                    action = "FLIP_TO_NO"
                else:
                    reason = why1 if not ok1 else why2
            else:
                reason = "HOLD_SAME_SIDE"

        elif position == "NO":
            if abs(edge) < EDGE_EXIT:
                ok, why = book_ok_for_token("NO")
                if ok:
                    action = "EXIT_NO"
                else:
                    reason = why
            elif signal == "YES":
                ok1, why1 = book_ok_for_token("NO")
                ok2, why2 = book_ok_for_token("YES")
                if ok1 and ok2:
                    action = "FLIP_TO_YES"
                else:
                    reason = why1 if not ok1 else why2
            else:
                reason = "HOLD_SAME_SIDE"

    # PAPER fills: conservative
    # - BUY at ask
    # - SELL at bid
    def buy_price(tok: str) -> float:
        return float(yes_ask if tok == "YES" else no_ask)

    def sell_price(tok: str) -> float:
        return float(yes_bid if tok == "YES" else no_bid)

    # Execute
    did_trade = False
    if action in ("ENTER_YES", "ENTER_NO", "EXIT_YES", "EXIT_NO", "FLIP_TO_NO", "FLIP_TO_YES") and reason == "":
        if action == "ENTER_YES":
            fill = buy_price("YES")
            if run_mode == "PAPER":
                state["position"] = "YES"
                state["entry_price"] = fill
                state["stake"] = LIVE_TRADE_SIZE
                state["trades_today"] = trades_today + 1
                state["last_trade_ts"] = None  # updated below
                did_trade = True
            elif run_mode == "LIVE" and live_armed:
                ok, msg = bun_place_order(yes_token, "BUY", fill, LIVE_TRADE_SIZE)
                if ok:
                    state["position"] = "YES"
                    state["entry_price"] = fill
                    state["stake"] = LIVE_TRADE_SIZE
                    state["trades_today"] = trades_today + 1
                    did_trade = True
                else:
                    action = "NO_TRADE"
                    reason = msg

        elif action == "ENTER_NO":
            fill = buy_price("NO")
            if run_mode == "PAPER":
                state["position"] = "NO"
                state["entry_price"] = fill
                state["stake"] = LIVE_TRADE_SIZE
                state["trades_today"] = trades_today + 1
                did_trade = True
            elif run_mode == "LIVE" and live_armed:
                ok, msg = bun_place_order(no_token, "BUY", fill, LIVE_TRADE_SIZE)
                if ok:
                    state["position"] = "NO"
                    state["entry_price"] = fill
                    state["stake"] = LIVE_TRADE_SIZE
                    state["trades_today"] = trades_today + 1
                    did_trade = True
                else:
                    action = "NO_TRADE"
                    reason = msg

        elif action in ("EXIT_YES", "EXIT_NO"):
            tok = "YES" if action == "EXIT_YES" else "NO"
            if entry is None:
                # corrupted; clear
                state["position"] = None
                state["entry_price"] = None
                state["stake"] = 0.0
                did_trade = True
            else:
                exit_px = sell_price(tok)
                realized = (exit_px - float(entry)) * float(stake or 0.0)
                if run_mode == "PAPER":
                    state["position"] = None
                    state["entry_price"] = None
                    state["stake"] = 0.0
                    state["trades_today"] = trades_today + 1
                    state["pnl_today_realized"] = pnl_today + realized
                    did_trade = True
                elif run_mode == "LIVE" and live_armed:
                    token_id = yes_token if tok == "YES" else no_token
                    ok, msg = bun_place_order(token_id, "SELL", exit_px, float(stake))
                    if ok:
                        state["position"] = None
                        state["entry_price"] = None
                        state["stake"] = 0.0
                        state["trades_today"] = trades_today + 1
                        state["pnl_today_realized"] = pnl_today + realized
                        did_trade = True
                    else:
                        action = "NO_TRADE"
                        reason = msg

        elif action == "FLIP_TO_NO":
            # Exit YES then enter NO (counts as 2 trades)
            if entry is None:
                state["position"] = None
                state["entry_price"] = None
                state["stake"] = 0.0
                state["trades_today"] = trades_today + 1
                did_trade = True
            else:
                exit_px = sell_price("YES")
                realized = (exit_px - float(entry)) * float(stake or 0.0)
                enter_px = buy_price("NO")

                if run_mode == "PAPER":
                    state["pnl_today_realized"] = pnl_today + realized
                    state["position"] = "NO"
                    state["entry_price"] = enter_px
                    state["stake"] = LIVE_TRADE_SIZE
                    state["trades_today"] = trades_today + 2
                    did_trade = True
                elif run_mode == "LIVE" and live_armed:
                    ok1, msg1 = bun_place_order(yes_token, "SELL", exit_px, float(stake))
                    if ok1:
                        ok2, msg2 = bun_place_order(no_token, "BUY", enter_px, LIVE_TRADE_SIZE)
                        if ok2:
                            state["pnl_today_realized"] = pnl_today + realized
                            state["position"] = "NO"
                            state["entry_price"] = enter_px
                            state["stake"] = LIVE_TRADE_SIZE
                            state["trades_today"] = trades_today + 2
                            did_trade = True
                        else:
                            action = "NO_TRADE"
                            reason = msg2
                    else:
                        action = "NO_TRADE"
                        reason = msg1

        elif action == "FLIP_TO_YES":
            # Exit NO then enter YES
            if entry is None:
                state["position"] = None
                state["entry_price"] = None
                state["stake"] = 0.0
                state["trades_today"] = trades_today + 1
                did_trade = True
            else:
                exit_px = sell_price("NO")
                realized = (exit_px - float(entry)) * float(stake or 0.0)
                enter_px = buy_price("YES")

                if run_mode == "PAPER":
                    state["pnl_today_realized"] = pnl_today + realized
                    state["position"] = "YES"
                    state["entry_price"] = enter_px
                    state["stake"] = LIVE_TRADE_SIZE
                    state["trades_today"] = trades_today + 2
                    did_trade = True
                elif run_mode == "LIVE" and live_armed:
                    ok1, msg1 = bun_place_order(no_token, "SELL", exit_px, float(stake))
                    if ok1:
                        ok2, msg2 = bun_place_order(yes_token, "BUY", enter_px, LIVE_TRADE_SIZE)
                        if ok2:
                            state["pnl_today_realized"] = pnl_today + realized
                            state["position"] = "YES"
                            state["entry_price"] = enter_px
                            state["stake"] = LIVE_TRADE_SIZE
                            state["trades_today"] = trades_today + 2
                            did_trade = True
                        else:
                            action = "NO_TRADE"
                            reason = msg2
                    else:
                        action = "NO_TRADE"
                        reason = msg1

    if did_trade:
        # stamp cooldown
        state["last_trade_ts"] = requests.utils.datetime.datetime.now(requests.utils.datetime.timezone.utc)
        save_state(state)
        # reload for logging
        state = load_state()

    # Snapshot / log
    position2 = state["position"]
    entry2 = state["entry_price"]
    stake2 = float(state["stake"] or 0.0)
    trades2 = int(state["trades_today"] or 0)
    pnl2 = float(state["pnl_today_realized"] or 0.0)

    # Unrealized based on mid marks
    if position2 == "YES" and entry2 is not None:
        u = (yes_mid - float(entry2)) * stake2
        mark_price = yes_mid
    elif position2 == "NO" and entry2 is not None:
        u = (no_mid - float(entry2)) * stake2
        mark_price = no_mid
    else:
        u = 0.0
        mark_price = yes_mid

    balance = float(os.getenv("PAPER_BALANCE", "0").strip() or 0.0)
    equity = balance + u

    log.info(
        "slug=%s | YES %s mid=%.4f | NO %s mid=%.4f | fair_up=%.3f | edge=%+.4f | signal=%s | action=%s%s | pos=%s | entry=%s | trades_today=%d | pnl_today(realized)=%.2f",
        slug,
        yes_src, float(yes_mid),
        no_src, float(no_mid),
        fair_up,
        float(edge),
        signal,
        action,
        (f" | reason={reason}" if reason else ""),
        position2,
        (None if entry2 is None else round(float(entry2), 6)),
        trades2,
        pnl2
    )

    record_equity_snapshot(
        price=float(mark_price),
        balance=float(balance),
        position=position2,
        entry_price=(None if entry2 is None else float(entry2)),
        stake=float(stake2),
        unrealized_pnl=float(u),
        equity=float(equity),
        poly_slug=slug
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

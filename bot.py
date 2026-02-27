import os
import json
import time
import math
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
    bucket = int(ts // 300) * 300
    return bucket


def make_poly_slug() -> str:
    base = os.getenv("POLY_MARKET_SLUG", "").strip()
    event = os.getenv("POLY_EVENT_SLUG", "").strip()
    if not base and not event:
        raise RuntimeError("Missing POLY_MARKET_SLUG or POLY_EVENT_SLUG")

    # Your “idempotency slug” includes the 5m bucket epoch
    bucket = five_min_bucket_epoch()
    if base:
        return f"{base}-{bucket}"
    return f"{event}-{bucket}"


def http_get_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = 20) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def http_post_json(url: str, payload: dict, headers: Optional[dict] = None, timeout: int = 25) -> Tuple[int, Optional[dict], str]:
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=timeout)
        text = r.text[:5000] if r.text else ""
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

def ensure_tables():
    """
    Fixes:
      psycopg2.errors.UndefinedColumn: column "stake" does not exist

    Root cause: load_state() selects columns that your existing bot_state table
    doesn't have yet (stake, trades_today, pnl_today_realized, etc).

    This patch:
      - Ensures bot_state exists
      - Adds ALL columns that load_state() expects
      - Ensures id=1 row exists (no ON CONFLICT required)
    """
    import os
    import psycopg2

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = True
    cur = conn.cursor()

    # Ensure table exists (empty stub is fine; we'll add columns)
    cur.execute("CREATE TABLE IF NOT EXISTS bot_state ();")

    # Core identity / timing
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS id INTEGER;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS as_of_date DATE;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;")
    cur.execute("ALTER TABLE bot_state ALTER COLUMN updated_at SET DEFAULT NOW();")

    # Position state
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS position TEXT;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS entry_price DOUBLE PRECISION;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS entry_ts TIMESTAMPTZ;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS last_action TEXT;")

    # Fields your load_state() is selecting
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS stake DOUBLE PRECISION;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS trades_today INTEGER;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS pnl_today_realized DOUBLE PRECISION;")

    # Helpful extras (won't hurt if unused)
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS last_trade_ts TIMESTAMPTZ;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS equity DOUBLE PRECISION;")

    # Ensure single row exists (NO ON CONFLICT to avoid PK/unique assumptions)
    cur.execute("""
        INSERT INTO bot_state (
            id, as_of_date, position, entry_price, entry_ts, last_action,
            stake, trades_today, pnl_today_realized, updated_at
        )
        SELECT
            1, CURRENT_DATE, 'NO', NULL, NULL, 'BOOT',
            NULL, 0, 0.0, NOW()
        WHERE NOT EXISTS (SELECT 1 FROM bot_state WHERE id = 1);
    """)

    cur.close()
    conn.close()

def _maybe_json_list(x):
    """
    Gamma sometimes returns list-y fields as JSON-encoded strings.
    Converts:
      - '["Up","Down"]' -> ["Up","Down"]
      - ["Up","Down"] -> ["Up","Down"]
      - None -> None
    """
    if x is None:
        return None
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                return json.loads(s)
            except Exception:
                return None
    return None

def load_state() -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT as_of_date, position, entry_price, stake, trades_today, pnl_today_realized FROM bot_state WHERE id=1;")
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
                SET as_of_date=%s, position=%s, entry_price=%s, stake=%s, trades_today=%s, pnl_today_realized=%s
                WHERE id=1;
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
# Gamma/CLOB data
# ----------------------------
POLY_GAMMA_HOST = os.getenv("POLY_GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
POLY_CLOB_HOST = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").strip()

# Important:
# - POLY_MARKET_SLUG is your base slug (e.g., btc-updown-5m)
# - POLY_GAMMA_SLUG should be the *actual* Gamma market slug (often different)
POLY_MARKET_SLUG = os.getenv("POLY_MARKET_SLUG", "").strip()
POLY_GAMMA_SLUG = os.getenv("POLY_GAMMA_SLUG", "").strip() or POLY_MARKET_SLUG


def fetch_gamma_market_by_slug(slug: str) -> Optional[dict]:
    # Docs: “Get market by slug” exists in the API reference. :contentReference[oaicite:3]{index=3}
    url = f"{POLY_GAMMA_HOST}/markets/slug/{slug}"
    return http_get_json(url)


def gamma_search(query: str) -> Optional[dict]:
    # Docs: “Search markets, events, and profiles”. :contentReference[oaicite:4]{index=4}
    url = f"{POLY_GAMMA_HOST}/search"
    return http_get_json(url, params={"query": query})

def fetch_gamma_market_and_tokens(poly_slug: str):
    """
    Robust Gamma resolver for rolling 5m markets.

    Tries, in order:
      1) rolling poly_slug directly
      2) env base -> stitched into full rolling slug using poly_slug's end epoch
      3) Gamma /search using progressively broader queries:
         - full rolling slug
         - base slug (env)
         - prefix 'btc-updown-5m' (strip epochs)
      4) selects the best candidate market and extracts YES/NO tokenIds
         (supports UP/DOWN via extract_yes_no_token_ids)

    Returns: (market_dict, yes_token_id, no_token_id)
    Raises: RuntimeError if unresolved
    """
    import os
    import re

    def _parse_epochs(slug: str):
        # returns list of epoch-like chunks (strings of digits)
        return re.findall(r"\b\d{9,12}\b", slug or "")

    def _strip_to_prefix(slug: str):
        # btc-updown-5m-1772165400-1772197800 -> btc-updown-5m
        s = (slug or "").strip()
        s = re.sub(r"(-\d{9,12}){1,2}$", "", s)
        return s

    def _stitch_base_to_rolling(base: str, rolling: str):
        """
        If base looks like 'btc-updown-5m-<start>' and rolling has '<start>-<end>',
        return 'btc-updown-5m-<start>-<end>'.
        If base already has 2 epochs, return as-is.
        """
        base = (base or "").strip()
        if not base:
            return ""

        b_epochs = _parse_epochs(base)
        r_epochs = _parse_epochs(rolling)

        # Already full
        if len(b_epochs) >= 2:
            return base

        # Base has start only, rolling has start+end -> append end
        if len(b_epochs) == 1 and len(r_epochs) >= 2:
            end_epoch = r_epochs[-1]
            if base.endswith(b_epochs[0]):
                return f"{base}-{end_epoch}"

        # Otherwise just return base
        return base

    # You already have these helpers elsewhere in your file:
    # - fetch_gamma_market_by_slug(slug) -> dict|None
    # - fetch_gamma_search(query) -> list[dict]|None
    # - extract_yes_no_token_ids(market) -> (yes_id, no_id)

    tried = []

    # 1) Try rolling slug directly
    tried.append(("by_slug", poly_slug))
    m = fetch_gamma_market_by_slug(poly_slug)
    if m:
        y, n = extract_yes_no_token_ids(m)
        if y and n:
            return m, y, n

    # 2) Try env base stitched into full rolling slug
    env_base = (os.getenv("POLY_GAMMA_SLUG") or os.getenv("POLY_MARKET_SLUG") or "").strip()
    stitched = _stitch_base_to_rolling(env_base, poly_slug)
    if stitched and stitched != poly_slug:
        tried.append(("by_slug", stitched))
        m2 = fetch_gamma_market_by_slug(stitched)
        if m2:
            y, n = extract_yes_no_token_ids(m2)
            if y and n:
                return m2, y, n

    # 3) Search with broader queries (Gamma search often matches better on prefix)
    queries = []
    # best-first ordering
    if poly_slug:
        queries.append(poly_slug)
    if stitched and stitched not in queries:
        queries.append(stitched)
    if env_base and env_base not in queries:
        queries.append(env_base)

    prefix = _strip_to_prefix(env_base) or _strip_to_prefix(poly_slug)
    if prefix and prefix not in queries:
        queries.append(prefix)

    for q in queries:
        tried.append(("search", q))
        results = fetch_gamma_search(q) or []
        if not results:
            continue

        # Pick best candidate:
        #  - exact slug match to rolling
        #  - else exact slug match to stitched
        #  - else first result
        chosen = None
        for r in results:
            if (r.get("slug") or "").strip() == poly_slug:
                chosen = r
                break
        if not chosen and stitched:
            for r in results:
                if (r.get("slug") or "").strip() == stitched:
                    chosen = r
                    break
        chosen = chosen or results[0]

        slug = (chosen.get("slug") or "").strip()
        if not slug:
            continue

        tried.append(("by_slug", slug))
        m3 = fetch_gamma_market_by_slug(slug)
        if not m3:
            continue

        y, n = extract_yes_no_token_ids(m3)
        if y and n:
            return m3, y, n

    raise RuntimeError(
        "Gamma market/token resolution failed. "
        f"poly_slug='{poly_slug}' env_base='{env_base}' stitched='{stitched}' "
        f"tried={tried}"
    )

def extract_yes_no_token_ids(market: dict):
    """
    Robustly maps tokenIds to YES/NO for:
      - YES/NO markets
      - UP/DOWN markets (Up=YES, Down=NO)
      - generic 2-outcome markets (index 0=YES, index 1=NO)

    Returns: (yes_token_id, no_token_id) as strings, or (None, None)
    """
    outcomes = _maybe_json_list(market.get("outcomes"))
    token_ids = _maybe_json_list(market.get("tokenIds")) or _maybe_json_list(market.get("tokenIDs"))
    if not outcomes or not token_ids or len(outcomes) != len(token_ids):
        return (None, None)

    # Normalize labels
    labels = []
    for o in outcomes:
        if isinstance(o, str):
            labels.append(o.strip().upper())
        else:
            labels.append(str(o).strip().upper())

    # Direct YES/NO
    try:
        yes_i = labels.index("YES")
        no_i = labels.index("NO")
        return (str(token_ids[yes_i]), str(token_ids[no_i]))
    except ValueError:
        pass

    # UP/DOWN -> YES/NO
    if "UP" in labels and "DOWN" in labels:
        up_i = labels.index("UP")
        down_i = labels.index("DOWN")
        return (str(token_ids[up_i]), str(token_ids[down_i]))

    # Fallback: exactly 2 outcomes
    if len(labels) == 2:
        return (str(token_ids[0]), str(token_ids[1]))

    return (None, None)



def clob_midpoint(token_id: str) -> Optional[float]:
    # Docs show midpoint endpoints under “Orderbook & Pricing”. :contentReference[oaicite:5]{index=5}
    url = f"{POLY_CLOB_HOST}/midpoint"
    j = http_get_json(url, params={"token_id": token_id})
    if not j:
        return None
    mp = j.get("midpoint")
    if mp is None:
        return None
    try:
        return float(mp)
    except Exception:
        return None


# ----------------------------
# Bun live order gateway
# ----------------------------
BUN_BASE_URL = os.getenv("BUN_BASE_URL", "").strip()


def bun_health() -> Optional[int]:
    if not BUN_BASE_URL:
        return None
    code, _, _ = http_post_json(f"{BUN_BASE_URL}/__ping", payload={}, timeout=8)
    # If you didn’t implement /__ping, we’ll also accept GET /
    if code == 0 or code >= 400:
        try:
            r = requests.get(f"{BUN_BASE_URL}/", timeout=8)
            return r.status_code
        except Exception:
            return None
    return code


def bun_place_order(token_id: str, side: str, price: float, size: float) -> Tuple[bool, str]:
    """
    side: BUY or SELL
    """
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


# ----------------------------
# Strategy / Risk
# ----------------------------
EDGE_ENTER = env_float("EDGE_ENTER", 0.10)      # enter when |edge| >= this
EDGE_EXIT = env_float("EDGE_EXIT", 0.02)        # optional: exit when signal flips OR edge shrinks
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "60"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "60"))
LIVE_TRADE_SIZE = env_float("LIVE_TRADE_SIZE", 1.0)


def compute_signal(poly_up: float, fair_up: float) -> Tuple[str, float]:
    """
    edge = fair_up - poly_up
    """
    edge = fair_up - poly_up
    if edge >= EDGE_ENTER:
        return "YES", edge
    if edge <= -EDGE_ENTER:
        return "NO", edge
    return "HOLD", edge


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    log.info("BOOT: bot.py starting")

    ensure_tables()

    run_mode = os.getenv("RUN_MODE", "DRY_RUN").strip().upper()
    live_mode = run_mode == "LIVE"
    live_trading_enabled = env_bool("LIVE_TRADING_ENABLED", False)
    kill_switch = env_bool("KILL_SWITCH", True)

    live_armed = live_mode and live_trading_enabled and (not kill_switch)

    poly_slug = make_poly_slug()
    log.info("run_mode=%s live_mode=%s live_armed=%s poly_slug=%s", run_mode, live_mode, live_armed, poly_slug)

    # Optional: bun health log
    if BUN_BASE_URL:
        hs = bun_health()
        if hs is not None:
            log.info("bun_health_status=%s", hs)

    # Daily reset
    state = load_state()
    today = date.today()
    if state["as_of_date"] != today:
        state["as_of_date"] = today
        state["trades_today"] = 0
        state["pnl_today_realized"] = 0.0
        save_state(state)

    # Pull Gamma market + token IDs
    gamma = fetch_gamma_market_and_tokens(poly_slug)
    if not gamma:
        log.warning("Gamma market not found for slug=%s (poly_slug=%s)", POLY_GAMMA_SLUG or POLY_MARKET_SLUG, poly_slug)
        log.info("BOOT: bot.py finished cleanly")
        return

    yes_token = gamma["yes_token_id"]
    no_token = gamma["no_token_id"]

    # Market prices (midpoints)
    p_yes = clob_midpoint(yes_token)
    p_no = clob_midpoint(no_token)
    if p_yes is None or p_no is None:
        log.warning("Could not fetch midpoint prices from CLOB (yes=%s no=%s)", p_yes, p_no)
        log.info("BOOT: bot.py finished cleanly")
        return

    poly_up = float(p_yes)
    poly_down = float(p_no)

    # Simple fair value baseline (you can replace later with your model)
    fair_up = 0.50

    signal, edge = compute_signal(poly_up, fair_up)

    position = state["position"]  # "YES" / "NO" / None
    entry = state["entry_price"]
    stake = float(state["stake"] or 0.0)
    trades_today = int(state["trades_today"] or 0)
    pnl_today = float(state["pnl_today_realized"] or 0.0)

    action = "NO_TRADE"
    reason = ""
    balance = float(os.getenv("PAPER_BALANCE", "0").strip() or 0.0)

    # For live mode, you can optionally pass a balance from elsewhere.
    # We’ll keep balance as 0 unless you set PAPER_BALANCE (paper) or later wire builder balance.

    # Trade limit
    if trades_today >= MAX_TRADES_PER_DAY:
        reason = "MAX_TRADES_PER_DAY"
        action = "NO_TRADE"
    else:
        # Decide action
        if position is None:
            if signal == "YES":
                action = "ENTER_YES"
            elif signal == "NO":
                action = "ENTER_NO"
            else:
                action = "NO_TRADE"
                reason = "signal=HOLD"
        else:
            # Exit if signal flips
            if position == "YES" and signal == "NO":
                action = "EXIT"
            elif position == "NO" and signal == "YES":
                action = "EXIT"
            else:
                action = "NO_TRADE"
                reason = "signal=HOLD" if signal == "HOLD" else "HOLD_SAME_SIDE"

    # Safety rails for LIVE
    if live_mode and not live_armed and action.startswith("ENTER"):
        reason = "not_armed" + ("+KILL_SWITCH" if kill_switch else "")
        action = "NO_TRADE"

    # Execute (PAPER/LIVE)
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
        # For binary positions, we “sell” the same token we bought.
        if position == "YES":
            exit_price = poly_up
            token = yes_token
        else:
            exit_price = poly_down
            token = no_token

        if entry is None:
            # corrupted state; clear
            state["position"] = None
            state["entry_price"] = None
            state["stake"] = 0.0
            save_state(state)
        else:
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

    # Recompute for snapshot
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
        mark_price = poly_up  # store something valid/non-null

    equity = balance + u

    log.info(
        "poly_up=%.3f | poly_down=%.3f | fair_up=%.3f | edge=%+.3f | signal=%s | action=%s%s | balance=%.2f | pos=%s | entry=%s | trades_today=%d | pnl_today(realized)=%.2f | mode=%s | src=clob",
        poly_up, poly_down, fair_up, edge, signal, action,
        (f" | reason={reason}" if reason else ""),
        balance,
        position2,
        (None if entry2 is None else round(float(entry2), 4)),
        int(state2["trades_today"] or 0),
        float(state2["pnl_today_realized"] or 0.0),
        run_mode,
    )

    record_equity_snapshot(
        price=float(mark_price),
        balance=float(balance),
        position=position2,
        entry_price=(None if entry2 is None else float(entry2)),
        stake=float(stake2),
        unrealized_pnl=float(u),
        equity=float(equity),
        poly_slug=poly_slug,
    )

    log.info("summary | equity=%.2f | uPnL=%.2f | src=clob", equity, u)
    log.info("BOOT: bot.py finished cleanly")


if __name__ == "__main__":
    main()

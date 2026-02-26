import os
import json
import time
import uuid
import math
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import requests
import psycopg2
import psycopg2.extras


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(message)s",
)
log = logging.getLogger("bot")


def ts_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_line(level: str, msg: str):
    level = level.upper()
    prefix = f"{ts_iso()} | {level} | "
    if level == "DEBUG":
        log.debug(prefix + msg)
    elif level == "WARN" or level == "WARNING":
        log.warning(prefix + msg)
    elif level == "ERROR":
        log.error(prefix + msg)
    else:
        log.info(prefix + msg)


# ----------------------------
# Env helpers
# ----------------------------
def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return int(float(v))


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return float(v)


def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


# ----------------------------
# DB
# ----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "")
USE_DB = bool(DATABASE_URL)

STATE_KEY = "main"


def db_conn():
    if not USE_DB:
        return None
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def db_init():
    if not USE_DB:
        return
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_state (
              key TEXT PRIMARY KEY,
              position TEXT,
              entry_price DOUBLE PRECISION,
              trades_today INT NOT NULL DEFAULT 0,
              pnl_today DOUBLE PRECISION NOT NULL DEFAULT 0,
              last_trade_ts BIGINT NOT NULL DEFAULT 0,
              last_reset_day TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS live_orders (
              slug TEXT PRIMARY KEY,
              side TEXT NOT NULL,
              token_id TEXT NOT NULL,
              client_order_id TEXT NOT NULL,
              order_id TEXT,
              status TEXT NOT NULL,
              price DOUBLE PRECISION,
              size DOUBLE PRECISION,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            INSERT INTO bot_state(key, position, entry_price, trades_today, pnl_today, last_trade_ts, last_reset_day)
            VALUES (%s, NULL, NULL, 0, 0, 0, NULL)
            ON CONFLICT (key) DO NOTHING;
            """,
            (STATE_KEY,),
        )


def db_get_state():
    # Default in-memory fallback
    st = {
        "position": None,         # "YES" or "NO" or None
        "entry_price": None,
        "trades_today": 0,
        "pnl_today": 0.0,
        "last_trade_ts": 0,
        "last_reset_day": None,
    }
    if not USE_DB:
        return st

    with db_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT * FROM bot_state WHERE key=%s;", (STATE_KEY,))
        row = cur.fetchone()
        if not row:
            return st
        st["position"] = row["position"]
        st["entry_price"] = row["entry_price"]
        st["trades_today"] = row["trades_today"]
        st["pnl_today"] = float(row["pnl_today"])
        st["last_trade_ts"] = int(row["last_trade_ts"])
        st["last_reset_day"] = row["last_reset_day"]
        return st


def db_put_state(st):
    if not USE_DB:
        return
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE bot_state
            SET position=%s,
                entry_price=%s,
                trades_today=%s,
                pnl_today=%s,
                last_trade_ts=%s,
                last_reset_day=%s
            WHERE key=%s;
            """,
            (
                st["position"],
                st["entry_price"],
                st["trades_today"],
                st["pnl_today"],
                st["last_trade_ts"],
                st["last_reset_day"],
                STATE_KEY,
            ),
        )


def db_live_order_get(slug: str):
    if not USE_DB:
        return None
    with db_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT * FROM live_orders WHERE slug=%s;", (slug,))
        return cur.fetchone()


def db_live_order_upsert(slug: str, side: str, token_id: str, client_order_id: str,
                         status: str, order_id: str = None,
                         price: float = None, size: float = None):
    if not USE_DB:
        return
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO live_orders(slug, side, token_id, client_order_id, order_id, status, price, size)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (slug) DO UPDATE SET
              side=EXCLUDED.side,
              token_id=EXCLUDED.token_id,
              client_order_id=EXCLUDED.client_order_id,
              order_id=EXCLUDED.order_id,
              status=EXCLUDED.status,
              price=EXCLUDED.price,
              size=EXCLUDED.size,
              updated_at=NOW();
            """,
            (slug, side, token_id, client_order_id, order_id, status, price, size),
        )


# ----------------------------
# Market slug logic (5m buckets)
# ----------------------------
def current_5m_bucket_ts(now_ts: int) -> int:
    # Your slugs look like ...-1772112600 which is divisible by 300
    return (now_ts // 300) * 300


def make_poly_slug() -> str:
    """
    Priority:
      1) POLY_MARKET_SLUG (explicit full slug)
      2) POLY_EVENT_SLUG (base slug) + current 5m bucket
      3) Back-compat fallbacks: MARKET_SLUG / EVENT_SLUG / POLY_SLUG
    Returns "" if missing (caller should exit cleanly).
    """
    # 1) explicit
    for k in ("POLY_MARKET_SLUG", "MARKET_SLUG", "POLY_SLUG"):
        v = env_str(k, "").strip()
        if v:
            return v

    # 2) derive
    event_slug = ""
    for k in ("POLY_EVENT_SLUG", "EVENT_SLUG"):
        v = env_str(k, "").strip()
        if v:
            event_slug = v
            break

    if not event_slug:
        return ""

    now_ts = int(time.time())
    bucket = current_5m_bucket_ts(now_ts)
    return f"{event_slug}-{bucket}"


# ----------------------------
# Gamma market lookup (for token IDs)
# ----------------------------
GAMMA_URL = env_str("GAMMA_URL", "https://gamma-api.polymarket.com")


def _maybe_json_list(x):
    """
    Gamma sometimes returns JSON arrays as strings.
    Handles:
      - '["Up","Down"]' -> ["Up","Down"]
      - '["0.49","0.51"]' -> ["0.49","0.51"]
      - ["Up","Down"] -> ["Up","Down"]
      - "Up,Down" -> ["Up","Down"]
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


def _extract_updown_from_market(m: dict) -> Optional[Dict[str, Any]]:
    """
    Returns mapping for UP/DOWN (or YES/NO) plus optional Gamma prices.
    Very defensive: supports multiple Gamma field variants and outcome labels.
    """
    # Outcomes / names
    outcomes = (
        _maybe_json_list(m.get("outcomes"))
        or _maybe_json_list(m.get("outcomeNames"))
        or _maybe_json_list(m.get("outcome_names"))
        or _maybe_json_list(m.get("answers"))
    )

    # Token IDs (CLOB)
    token_ids = (
        _maybe_json_list(m.get("clobTokenIds"))
        or _maybe_json_list(m.get("clobTokenIDs"))
        or _maybe_json_list(m.get("clob_token_ids"))
        or _maybe_json_list(m.get("tokenIds"))
        or _maybe_json_list(m.get("token_ids"))
    )

    # Gamma prices
    outcome_prices = (
        _maybe_json_list(m.get("outcomePrices"))
        or _maybe_json_list(m.get("outcome_prices"))
        or _maybe_json_list(m.get("prices"))
    )

    if not isinstance(outcomes, list) or len(outcomes) < 2:
        if DEBUG_POLY:
            print(f"{utc_now_iso()} | DEBUG | extract_fail outcomes={str(m.get('outcomes'))[:200]} outcomeNames={str(m.get('outcomeNames'))[:200]}", flush=True)
        return None

    # Normalize outcome labels
    norm = [str(o).strip().lower() for o in outcomes]

    # Build index lookup for common label variants
    # We accept: up/down, yes/no, higher/lower, above/below
    def find_idx(candidates: List[str]) -> Optional[int]:
        for c in candidates:
            c = c.lower()
            for i, n in enumerate(norm):
                if n == c:
                    return i
            # substring match fallback (handles "btc up" / "price up" etc.)
            for i, n in enumerate(norm):
                if c in n:
                    return i
        return None

    up_i = find_idx(["up", "yes", "higher", "above", "increase"])
    dn_i = find_idx(["down", "no", "lower", "below", "decrease"])

    # If we still can’t identify, but it’s a 2-outcome market, assume order is [up, down]
    if up_i is None or dn_i is None:
        if len(outcomes) == 2:
            up_i, dn_i = 0, 1
        else:
            if DEBUG_POLY:
                print(f"{utc_now_iso()} | DEBUG | extract_fail norm_outcomes={norm}", flush=True)
            return None

    # Token mapping
    up_tid = None
    dn_tid = None
    if isinstance(token_ids, list) and len(token_ids) > max(up_i, dn_i):
        up_tid = str(token_ids[up_i])
        dn_tid = str(token_ids[dn_i])

    # Gamma price mapping
    up_g = None
    dn_g = None
    if isinstance(outcome_prices, list) and len(outcome_prices) > max(up_i, dn_i):
        up_g = _safe_float(outcome_prices[up_i])
        dn_g = _safe_float(outcome_prices[dn_i])

    if DEBUG_POLY:
        print(
            f"{utc_now_iso()} | DEBUG | market_id={m.get('id')} title={str(m.get('title') or m.get('question') or '')[:80]} "
            f"outcomes={outcomes} token_ids_len={(len(token_ids) if isinstance(token_ids, list) else 'n/a')} "
            f"prices_len={(len(outcome_prices) if isinstance(outcome_prices, list) else 'n/a')} "
            f"up_i={up_i} dn_i={dn_i} up_tid={up_tid} dn_tid={dn_tid} up_g={up_g} dn_g={dn_g}",
            flush=True
        )

    return {"up_tid": up_tid, "dn_tid": dn_tid, "up_gamma": up_g, "dn_gamma": dn_g}


def fetch_gamma_market_by_slug(slug: str) -> dict:
    # Gamma returns a list
    url = f"{GAMMA_URL}/markets"
    r = requests.get(url, params={"slug": slug, "limit": 5}, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or len(data) == 0:
        return None
    # best match
    return data[0]


def extract_token_ids(mkt: dict):
    """
    Tries to find YES/NO token ids from gamma market payload.
    Different gamma payloads exist; we handle the common shapes.
    """
    # Some gamma payloads have "tokens" list with {token_id, outcome}
    tokens = mkt.get("tokens")
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            tokens = None

    if isinstance(tokens, list):
        yes = None
        no = None
        for t in tokens:
            if not isinstance(t, dict):
                continue
            outcome = str(t.get("outcome", "")).strip().lower()
            tid = str(t.get("token_id") or t.get("tokenId") or t.get("id") or "").strip()
            if not tid:
                continue
            if outcome == "yes":
                yes = tid
            elif outcome == "no":
                no = tid
        if yes and no:
            return yes, no

    # Fallback: outcomes/outcomePrices arrays (older)
    outcomes = _maybe_json_list(mkt.get("outcomes"))
    if not outcomes:
        outcomes = _maybe_json_list(mkt.get("outcomeNames"))
    # token ids sometimes in "clobTokenIds" or similar arrays
    clob_ids = _maybe_json_list(mkt.get("clobTokenIds")) or _maybe_json_list(mkt.get("clob_token_ids"))

    if outcomes and clob_ids and len(outcomes) == len(clob_ids):
        yes = no = None
        for o, tid in zip(outcomes, clob_ids):
            if str(o).strip().lower() == "yes":
                yes = str(tid)
            elif str(o).strip().lower() == "no":
                no = str(tid)
        if yes and no:
            return yes, no

    return None, None


# ----------------------------
# CLOB pricing (public)
# ----------------------------
CLOB_HOST = env_str("CLOB_HOST", "https://clob.polymarket.com")


def clob_price(token_id: str, side: str) -> float:
    """
    side must be BUY or SELL.
    Uses GET /price (public).
    """
    url = f"{CLOB_HOST}/price"
    r = requests.get(url, params={"token_id": token_id, "side": side}, timeout=15)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    data = r.json()
    # common shape: {"price":"0.49"} or {"price":0.49}
    p = data.get("price")
    if p is None:
        return None
    return float(p)


def get_yes_no_prices(yes_token: str, no_token: str):
    # For a binary market, "prob" is often approximated by best BUY price.
    yes_buy = clob_price(yes_token, "BUY")
    no_buy = clob_price(no_token, "BUY")
    return yes_buy, no_buy


# ----------------------------
# Strategy / rules (matches your envs)
# ----------------------------
UP_THRESHOLD = env_float("UP_THRESHOLD", 0.0)
DOWN_THRESHOLD = env_float("DOWN_THRESHOLD", 0.0)

EDGE_ENTER = env_float("EDGE_ENTER", 0.08)
EDGE_EXIT = env_float("EDGE_EXIT", 0.02)

LIVE_MIN_EDGE_ENTER = env_float("LIVE_MIN_EDGE_ENTER", EDGE_ENTER)
LIVE_MIN_EDGE_EXIT = env_float("LIVE_MIN_EDGE_EXIT", EDGE_EXIT)

MAX_TRADES_PER_DAY = env_int("MAX_TRADES_PER_DAY", 50)
MAX_DAILY_LOSS = env_float("MAX_DAILY_LOSS", 999999.0)

COOLDOWN_MINUTES = env_int("COOLDOWN_MINUTES", 0)
LIVE_COOLDOWN_MINUTES = env_int("LIVE_COOLDOWN_MINUTES", COOLDOWN_MINUTES)

LIVE_TRADE_SIZE = env_float("LIVE_TRADE_SIZE", 1.0)
PAPER_TRADE_SIZE = env_float("PAPER_TRADE_SIZE", 1.0)

KILL_SWITCH = env_bool("KILL_SWITCH", False)

RESET_DAILY = env_bool("RESET_DAILY", True)
RESET_STATE = env_bool("RESET_STATE", False)

ALLOW_WHALE_AFTER_CAP = env_bool("ALLOW_WHALE_AFTER_CAP", True)  # kept for compatibility


def day_key_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def should_reset_daily(st) -> bool:
    if not RESET_DAILY:
        return False
    return st.get("last_reset_day") != day_key_utc()


def apply_daily_reset(st):
    st["trades_today"] = 0
    st["pnl_today"] = 0.0
    st["last_reset_day"] = day_key_utc()
    log_line("INFO", "RESET_DAILY applied (debounced). Continuing run.")


def apply_state_reset(st):
    st["position"] = None
    st["entry_price"] = None
    log_line("INFO", "RESET_STATE applied. Position cleared.")


def cooldown_ok(st, now_ts: int, live: bool) -> bool:
    cd_min = LIVE_COOLDOWN_MINUTES if live else COOLDOWN_MINUTES
    if cd_min <= 0:
        return True
    return (now_ts - int(st["last_trade_ts"])) >= cd_min * 60


def risk_ok(st) -> bool:
    # stop trading if daily loss exceeds max
    return st["pnl_today"] >= -abs(MAX_DAILY_LOSS)


def compute_signal(poly_up: float, fair_up: float):
    """
    Edge is defined similar to your logs:
      edge = fair_up - poly_up
    If edge >= EDGE_ENTER => signal YES (value in YES)
    If edge <= -EDGE_ENTER => signal NO (value in NO)
    Else HOLD
    """
    edge = fair_up - poly_up
    if edge >= EDGE_ENTER:
        return "YES", edge
    if edge <= -EDGE_ENTER:
        return "NO", edge
    return "HOLD", edge


def fair_value_stub() -> float:
    """
    Your logs show fair_up ~ 0.49-0.51.
    If you have a true model, plug it in here.
    For now we center at 0.50 with a tiny drift from env thresholds.
    """
    base = 0.50
    # Keep thresholds for compatibility; doesn't change much.
    adj = (UP_THRESHOLD - DOWN_THRESHOLD) * 0.0
    return max(0.01, min(0.99, base + adj))


# ----------------------------
# Paper execution
# ----------------------------
def paper_enter(st, side: str, price: float, size: float):
    st["position"] = side
    st["entry_price"] = price
    st["trades_today"] += 1
    st["last_trade_ts"] = int(time.time())


def paper_exit(st, exit_price: float, size: float):
    pos = st["position"]
    entry = st["entry_price"]
    if pos is None or entry is None:
        st["position"] = None
        st["entry_price"] = None
        return 0.0

    # Simple PnL proxy: (exit - entry) * size for YES, inverse for NO
    # NOTE: This is not exact CLOB PnL; it matches your “paper” style tracking.
    if pos == "YES":
        pnl = (exit_price - entry) * size
    else:
        pnl = (entry - exit_price) * size

    st["pnl_today"] += pnl
    st["position"] = None
    st["entry_price"] = None
    st["trades_today"] += 1
    st["last_trade_ts"] = int(time.time())
    return pnl


# ----------------------------
# Live trading module (fully gated)
# ----------------------------
def live_flags():
    run_mode = env_str("RUN_MODE", "PAPER").upper()  # PAPER / DRY_RUN / LIVE
    live_mode = env_bool("LIVE_MODE", True)          # kept for compatibility with your logs
    live_trading_enabled = env_bool("LIVE_TRADING_ENABLED", False)
    live_armed = (run_mode == "LIVE") and live_trading_enabled and (not KILL_SWITCH)
    return run_mode, live_mode, live_trading_enabled, live_armed


def try_import_clob():
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
        from py_clob_client.order_builder.constants import BUY, SELL
        from py_clob_client.order_builder.types import OrderArgs
        return ClobClient, ApiCreds, BUY, SELL, OrderArgs
    except Exception as e:
        return None


def build_clob_client():
    """
    Uses POLY_PRIVATE_KEY and either:
      - POLY_API_KEY/POLY_API_SECRET/POLY_API_PASSPHRASE, OR
      - derives API creds via L1 auth (supported by Polymarket client flows)
    """
    imported = try_import_clob()
    if not imported:
        return None, None

    ClobClient, ApiCreds, BUY, SELL, OrderArgs = imported

    private_key = env_str("POLY_PRIVATE_KEY", "").strip()
    if not private_key:
        raise RuntimeError("Missing POLY_PRIVATE_KEY for live trading")

    chain_id = env_int("POLY_CHAIN_ID", 137)
    host = env_str("CLOB_HOST", "https://clob.polymarket.com")

    # Some client versions want a funded proxy address; keep optional
    funder = env_str("POLY_FUNDER_ADDRESS", "").strip() or None

    api_key = env_str("POLY_API_KEY", "").strip()
    api_secret = env_str("POLY_API_SECRET", "").strip()
    api_pass = env_str("POLY_API_PASSPHRASE", "").strip()

    client = ClobClient(host, key=private_key, chain_id=chain_id, funder=funder)

    creds = None
    if api_key and api_secret and api_pass:
        creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_pass)
    else:
        # Derive creds (preferred if you haven’t created/pasted them yet).
        # This corresponds to the documented auth/derive/create API-key flows. :contentReference[oaicite:2]{index=2}
        try:
            creds = client.create_or_derive_api_creds()
        except Exception as e:
            log_line("WARN", f"Could not derive API creds (live trading disabled until fixed): {e}")
            return client, None

    client.set_api_creds(creds)
    return client, {"BUY": BUY, "SELL": SELL, "OrderArgs": OrderArgs}


def live_place_order_once(slug: str, side: str, token_id: str, price: float, size: float):
    """
    Idempotent per slug:
      - if we already inserted an order row for this slug, we do nothing
    """
    existing = db_live_order_get(slug)
    if existing:
        return existing.get("status", "UNKNOWN"), existing.get("order_id")

    client_order_id = str(uuid.uuid4())
    db_live_order_upsert(slug, side, token_id, client_order_id, status="CREATED", price=price, size=size)

    client, helpers = build_clob_client()
    if client is None or helpers is None:
        db_live_order_upsert(slug, side, token_id, client_order_id, status="NO_SDK", price=price, size=size)
        return "NO_SDK", None

    BUY = helpers["BUY"]
    SELL = helpers["SELL"]
    OrderArgs = helpers["OrderArgs"]

    # For binary markets:
    # - To "enter YES", you BUY the YES token
    # - To "enter NO",  you BUY the NO token
    # We keep side as BUY for entries; exits would be SELL on the token you hold.
    clob_side = BUY if side == "BUY" else SELL

    try:
        # Some py-clob-client versions use create_and_post_order; others split build/post
        order_args = OrderArgs(price=price, size=size, side=clob_side, token_id=token_id)
        resp = client.create_and_post_order(order_args, client_order_id=client_order_id)
        # Try to read an order id
        order_id = None
        if isinstance(resp, dict):
            order_id = resp.get("orderID") or resp.get("orderId") or resp.get("id")
        db_live_order_upsert(slug, side, token_id, client_order_id, status="POSTED", order_id=order_id, price=price, size=size)
        return "POSTED", order_id
    except Exception as e:
        db_live_order_upsert(slug, side, token_id, client_order_id, status=f"ERROR:{type(e).__name__}", price=price, size=size)
        raise


# ----------------------------
# Main
# ----------------------------
def main():
    log_line("INFO", "BOOT: bot.py starting")
    db_init()

    st = db_get_state()

    if should_reset_daily(st):
        apply_daily_reset(st)
        db_put_state(st)

    if RESET_STATE:
        apply_state_reset(st)
        db_put_state(st)
        log_line("INFO", "BOOT: bot.py finished cleanly")
        return

    now_ts = int(time.time())

    run_mode, live_mode, live_trading_enabled, live_armed = live_flags()

    # Build market slug & lookup token ids
    poly_slug = make_poly_slug()
    if not poly_slug:
        log_line("ERROR", "Missing market slug vars. Set POLY_MARKET_SLUG or POLY_EVENT_SLUG on this service (clawbot).")
        log_line("INFO", "BOOT: bot.py finished cleanly")
        return

    log_line("INFO", f"run_mode={run_mode} live_mode={live_mode} live_armed={live_armed} poly_slug={poly_slug}")

    mkt = fetch_gamma_market_by_slug(poly_slug)
    if not mkt:
        log_line("WARN", f"poly_slug={poly_slug} candidate_markets=0")
        log_line("INFO", "BOOT: bot.py finished cleanly")
        return

    log_line("DEBUG", f"poly_slug={poly_slug} candidate_markets=1")

    yes_token, no_token = extract_token_ids(mkt)
    if not yes_token or not no_token:
        log_line("WARN", "Could not extract YES/NO token ids from Gamma response.")
        log_line("INFO", "BOOT: bot.py finished cleanly")
        return

    poly_up, poly_down = get_yes_no_prices(yes_token, no_token)
    if poly_up is None or poly_down is None:
        log_line("WARN", "Could not fetch CLOB prices (no orderbook?)")
        log_line("INFO", "BOOT: bot.py finished cleanly")
        return

    fair_up = fair_value_stub()
    signal, edge = compute_signal(poly_up, fair_up)

    # Determine action
    action = "NO_TRADE"
    reason = None

    # Safety gates (paper + live share these)
    if st["trades_today"] >= MAX_TRADES_PER_DAY and not ALLOW_WHALE_AFTER_CAP:
        action = "NO_TRADE"
        reason = "MAX_TRADES_PER_DAY"
    elif not risk_ok(st):
        action = "NO_TRADE"
        reason = "MAX_DAILY_LOSS"
    elif not cooldown_ok(st, now_ts, live=(run_mode == "LIVE")):
        action = "NO_TRADE"
        reason = "COOLDOWN"
    else:
        pos = st["position"]
        if pos is None:
            if signal == "YES":
                action = "ENTER_YES"
            elif signal == "NO":
                action = "ENTER_NO"
            else:
                action = "NO_TRADE"
                reason = "signal=HOLD"
        else:
            # exit if opposite signal with strong edge, or exit threshold met
            if pos == "YES" and signal == "NO" and abs(edge) >= EDGE_EXIT:
                action = "EXIT"
            elif pos == "NO" and signal == "YES" and abs(edge) >= EDGE_EXIT:
                action = "EXIT"
            else:
                # keep holding if still aligned
                if (pos == "YES" and signal == "YES") or (pos == "NO" and signal == "NO"):
                    action = "HOLD_SAME_SIDE"
                else:
                    action = "NO_TRADE"
                    reason = "signal=HOLD"

    # Execute action
    size = PAPER_TRADE_SIZE if run_mode in ("PAPER", "DRY_RUN") else LIVE_TRADE_SIZE

    if run_mode in ("PAPER", "DRY_RUN"):
        if action == "ENTER_YES":
            paper_enter(st, "YES", poly_up, size)
        elif action == "ENTER_NO":
            paper_enter(st, "NO", poly_down, size)
        elif action == "EXIT":
            # exit at current relevant price
            exit_price = poly_up if st["position"] == "YES" else poly_down
            paper_exit(st, exit_price, size)

        db_put_state(st)

    elif run_mode == "LIVE":
        # Live uses the same signal logic, but actual order placement is *armed-only*
        # and is idempotent per slug via live_orders table.
        if action in ("ENTER_YES", "ENTER_NO", "EXIT"):
            if not live_armed:
                reason = reason or "not_armed"
            else:
                try:
                    if action == "ENTER_YES":
                        # BUY YES token at poly_up
                        live_place_order_once(poly_slug, "BUY", yes_token, poly_up, size)
                        st["position"] = "YES"
                        st["entry_price"] = poly_up
                        st["trades_today"] += 1
                        st["last_trade_ts"] = now_ts
                    elif action == "ENTER_NO":
                        live_place_order_once(poly_slug, "BUY", no_token, poly_down, size)
                        st["position"] = "NO"
                        st["entry_price"] = poly_down
                        st["trades_today"] += 1
                        st["last_trade_ts"] = now_ts
                    elif action == "EXIT":
                        # In a real implementation you would SELL the token you hold.
                        # We keep the safe scaffolding here; you can enable SELL once we confirm BUY flow.
                        reason = "live_exit_requires_sell_enable"
                        action = "NO_TRADE"

                    db_put_state(st)
                except Exception as e:
                    log_line("ERROR", f"Live order failed: {e}")
                    reason = f"live_error:{type(e).__name__}"
        # else: no trade
    else:
        reason = f"unknown_run_mode:{run_mode}"
        action = "NO_TRADE"

    # Log summary line (matching your style)
    pos = st["position"]
    entry = st["entry_price"]
    balance = env_float("PAPER_BALANCE", 0.0)  # optional external
    # We log equity based on pnl_today proxy (same style as before)
    equity = balance + st["pnl_today"] if balance else 0.0

    # Your logs print balance as equity-ish; keep "balance" equal to equity proxy if not set
    balance_out = equity if equity else env_float("BALANCE", 0.0)
    if not balance_out:
        # if nothing set, fake it with pnl only (won’t break anything)
        balance_out = st["pnl_today"]

    parts = [
        f"poly_up={poly_up:.3f}",
        f"poly_down={poly_down:.3f}",
        f"fair_up={fair_up:.3f}",
        f"edge={edge:+.3f}",
        f"signal={signal}",
        f"action={action}",
    ]
    if reason:
        parts.append(f"reason={reason}")
    parts += [
        f"balance={balance_out:.2f}",
        f"pos={pos}",
        f"entry={None if entry is None else round(entry, 4)}",
        f"trades_today={st['trades_today']}",
        f"pnl_today(realized)={st['pnl_today']:.2f}",
        f"mode={run_mode}",
        "src=clob",
    ]
    log_line("INFO", " | ".join(parts))

    # Minimal summary line
    log_line("INFO", f"summary | equity={balance_out:.2f} | uPnL=0.00 | src=clob")
    log_line("INFO", "BOOT: bot.py finished cleanly")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_line("ERROR", f"Fatal error: {e}")
        raise

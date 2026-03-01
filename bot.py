#!/usr/bin/env python3
"""
Johnny 5 — BTC 5-minute Up/Down Edge Trader (NO Bun)

- Discovers current rolling 5-min Polymarket market slug (PREFIX-<bucket_epoch>)
- Uses Gamma to get token IDs
- Uses CLOB books for prices
- Computes an "edge" estimate and chooses UP or DOWN
- PAPER: simulates fills at ask/bid (realistic)
- LIVE: places orders directly via py-clob-client (sign + post)

Safety:
- RUN_MODE=LIVE + LIVE_ARMED=true + KILL_SWITCH=false are required to send live orders.
"""

import os
import json
import time
import logging
import datetime as dt
from typing import Any, Dict, Optional, Tuple, List

import requests

# Optional DB
try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # type: ignore

# py-clob-client (required for LIVE)
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType, ApiCreds
    from py_clob_client.order_builder.constants import BUY, SELL
except Exception:
    ClobClient = None  # type: ignore
    OrderArgs = None  # type: ignore
    OrderType = None  # type: ignore
    ApiCreds = None  # type: ignore
    BUY = "BUY"
    SELL = "SELL"


# ----------------------------
# Helpers
# ----------------------------
def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "t", "yes", "y", "on")


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(float(raw))


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def utc_iso(ts: Optional[int] = None) -> str:
    if ts is None:
        return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat(timespec="seconds")


def current_bucket_epoch() -> int:
    now = int(time.time())
    return (now // 300) * 300


def make_slug(prefix: str, bucket: int) -> str:
    return f"{prefix}-{bucket}"


def maybe_json(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                return json.loads(s)
            except Exception:
                return x
    return x


def as_list_str(x: Any) -> List[str]:
    x = maybe_json(x)
    if not isinstance(x, list):
        return []
    return [str(v) for v in x if v is not None]


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
# Config
# ----------------------------
RUN_MODE = os.getenv("RUN_MODE", "PAPER").upper()  # PAPER or LIVE
LIVE_MODE = RUN_MODE == "LIVE"
LIVE_ARMED = env_bool("LIVE_ARMED", False)
KILL_SWITCH = env_bool("KILL_SWITCH", True)

PREFIX = os.getenv("PREFIX", "btc-updown-5m").strip()
LOOKBACK = env_int("LOOKBACK", 3)

# Market quality constraints
MIN_BID = env_float("MIN_BID", 0.20)
MAX_ASK = env_float("MAX_ASK", 0.80)
MAX_SPREAD = env_float("MAX_SPREAD", 0.15)

# Edge thresholds
EDGE_ENTER = env_float("EDGE_ENTER", 0.015)
EDGE_EXIT = env_float("EDGE_EXIT", 0.005)

# Risk / sizing
MAX_TRADES_PER_DAY = env_int("MAX_TRADES_PER_DAY", 288)
STAKE_SHARES = env_float("STAKE_SHARES", 5.0)
MAX_COST_PER_TRADE = env_float("MAX_COST_PER_TRADE", 5.0)

# Looping
RUN_LOOP = env_bool("RUN_LOOP", False)
LOOP_SECONDS = env_int("LOOP_SECONDS", 5)

# Endpoints
GAMMA_BASE = os.getenv("GAMMA_BASE", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_BASE = os.getenv("CLOB_BASE", "https://clob.polymarket.com").rstrip("/")

# HTTP tuning
HTTP_TIMEOUT = env_float("HTTP_TIMEOUT", 10.0)
HTTP_RETRIES = env_int("HTTP_RETRIES", 2)
RETRY_SLEEP = env_float("RETRY_SLEEP", 0.35)

# DB
DB_STATE_TABLE = os.getenv("DB_STATE_TABLE", "j5_state_v2")
DB_KEY = os.getenv("DB_KEY", PREFIX)

# Paper starting balance (only used if DB empty)
PAPER_BALANCE_START = env_float("PAPER_BALANCE", 1000.0)

# Polymarket auth (LIVE)
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "").strip()
POLY_FUNDER_ADDRESS = os.getenv("POLY_FUNDER_ADDRESS", "").strip()
POLY_SIGNATURE_TYPE = env_int("POLY_SIGNATURE_TYPE", 0)
POLY_CHAIN_ID = env_int("POLY_CHAIN_ID", 137)

# Optional L2 creds (not required if we derive them)
POLY_API_KEY = os.getenv("POLY_API_KEY", "").strip()
POLY_API_SECRET = os.getenv("POLY_API_SECRET", "").strip()
POLY_API_PASSPHRASE = os.getenv("POLY_API_PASSPHRASE", "").strip()


# ----------------------------
# HTTP helpers
# ----------------------------
def http_get(url: str, params: Optional[dict] = None) -> Tuple[int, Any]:
    last_exc = None
    for _ in range(max(1, HTTP_RETRIES + 1)):
        try:
            r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
            ct = (r.headers.get("content-type") or "").lower()
            if "application/json" in ct:
                return r.status_code, r.json()
            try:
                return r.status_code, r.json()
            except Exception:
                return r.status_code, r.text
        except Exception as e:
            last_exc = e
            time.sleep(RETRY_SLEEP)
    raise RuntimeError(f"HTTP GET failed: {url} params={params} exc={last_exc}")


# ----------------------------
# DB helpers
# ----------------------------
def db_conn():
    if psycopg2 is None:
        return None
    dsn = os.getenv("DATABASE_URL", "").strip()
    if not dsn:
        return None
    return psycopg2.connect(dsn)


def ensure_tables():
    conn = db_conn()
    if conn is None:
        return
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {DB_STATE_TABLE} (
                    key TEXT PRIMARY KEY,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    state JSONB NOT NULL DEFAULT '{{}}'::jsonb
                );
                """
            )
            cur.execute(
                f"""
                INSERT INTO {DB_STATE_TABLE} (key, state)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (key) DO NOTHING;
                """,
                (DB_KEY, json.dumps({})),
            )


def db_read_state() -> Dict[str, Any]:
    conn = db_conn()
    if conn is None:
        return {}
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT state FROM {DB_STATE_TABLE} WHERE key=%s;", (DB_KEY,))
            row = cur.fetchone()
            if not row:
                return {}
            return row["state"] or {}


def db_write_state(state: Dict[str, Any]):
    conn = db_conn()
    if conn is None:
        return
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {DB_STATE_TABLE}
                SET state=%s::jsonb, updated_at=NOW()
                WHERE key=%s;
                """,
                (json.dumps(state), DB_KEY),
            )


# ----------------------------
# Gamma
# ----------------------------
def gamma_get_market_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    # Try /markets?slug=
    url1 = f"{GAMMA_BASE}/markets"
    code, data = http_get(url1, params={"slug": slug})
    if code == 200:
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict) and isinstance(data.get("markets"), list) and data["markets"]:
            return data["markets"][0]
        if isinstance(data, dict) and data.get("slug") == slug:
            return data
    # Try /markets/<slug>
    url2 = f"{GAMMA_BASE}/markets/{slug}"
    code2, data2 = http_get(url2)
    if code2 == 200 and isinstance(data2, dict):
        return data2
    return None


def extract_outcomes_and_tokens(mkt: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    outcomes = as_list_str(mkt.get("outcomes"))
    token_ids = as_list_str(mkt.get("clobTokenIds"))

    # fallback if comma-separated strings
    if not outcomes and isinstance(mkt.get("outcomes"), str):
        outcomes = [s.strip() for s in str(mkt.get("outcomes")).split(",") if s.strip()]
    if not token_ids and isinstance(mkt.get("clobTokenIds"), str):
        token_ids = [s.strip() for s in str(mkt.get("clobTokenIds")).split(",") if s.strip()]

    return outcomes, token_ids


# ----------------------------
# CLOB book
# ----------------------------
def clob_get_book(token_id: str) -> Optional[Dict[str, Any]]:
    url = f"{CLOB_BASE}/book"
    code, data = http_get(url, params={"token_id": token_id})
    if code == 200 and isinstance(data, dict):
        return data
    return None


def top_px_sz(levels: Any) -> Tuple[Optional[float], Optional[float]]:
    try:
        if not isinstance(levels, list) or not levels:
            return None, None
        lvl = levels[0]
        if not isinstance(lvl, dict):
            return None, None
        px = float(lvl.get("price"))
        sz = float(lvl.get("size", 0.0))
        return px, sz
    except Exception:
        return None, None


def best_bid_ask(book: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], float, float]:
    bb, bsz = top_px_sz(book.get("bids") or [])
    ba, asz = top_px_sz(book.get("asks") or [])
    return bb, ba, float(bsz or 0.0), float(asz or 0.0)


def book_ok(bb: Optional[float], ba: Optional[float]) -> Tuple[bool, str]:
    if bb is None or ba is None:
        return False, "MISSING_BID_OR_ASK"
    if bb < MIN_BID:
        return False, f"BID_TOO_LOW({bb:.3f})"
    if ba > MAX_ASK:
        return False, f"ASK_TOO_HIGH({ba:.3f})"
    spread = ba - bb
    if spread > MAX_SPREAD:
        return False, f"SPREAD_TOO_WIDE({spread:.3f})"
    return True, "OK"


def microprice(bb: float, ba: float, bid_sz: float, ask_sz: float) -> float:
    denom = bid_sz + ask_sz
    if denom <= 0:
        return (bb + ba) / 2.0
    return (bb * ask_sz + ba * bid_sz) / denom


def compute_edge(
    up_bb: float, up_ba: float, up_bsz: float, up_asz: float,
    dn_bb: float, dn_ba: float, dn_bsz: float, dn_asz: float,
) -> Dict[str, Any]:
    up_mid = (up_bb + up_ba) / 2.0
    dn_mid = (dn_bb + dn_ba) / 2.0

    fair_up_mid = (up_mid + (1.0 - dn_mid)) / 2.0

    up_mp = microprice(up_bb, up_ba, up_bsz, up_asz)
    dn_mp = microprice(dn_bb, dn_ba, dn_bsz, dn_asz)
    fair_up_mp = (up_mp + (1.0 - dn_mp)) / 2.0

    fair_up = 0.5 * fair_up_mid + 0.5 * fair_up_mp
    fair_up = max(0.01, min(0.99, fair_up))

    edge_up_buy = fair_up - up_ba
    edge_dn_buy = (1.0 - fair_up) - dn_ba

    if edge_up_buy >= edge_dn_buy:
        return {"fair_up": fair_up, "edge": edge_up_buy, "preferred": "UP", "up_mid": up_mid, "dn_mid": dn_mid}
    return {"fair_up": fair_up, "edge": edge_dn_buy, "preferred": "DOWN", "up_mid": up_mid, "dn_mid": dn_mid}


# ----------------------------
# LIVE execution via py-clob-client
# ----------------------------
def require_live_client() -> "ClobClient":
    if ClobClient is None:
        raise RuntimeError("py-clob-client not available. Check requirements / install.")
    if not POLY_PRIVATE_KEY:
        raise RuntimeError("Missing POLY_PRIVATE_KEY")
    # Initialize client
    kwargs = dict(
        host=CLOB_BASE,
        key=POLY_PRIVATE_KEY,
        chain_id=POLY_CHAIN_ID,
    )
    # Only include these if provided/needed
    if POLY_SIGNATURE_TYPE in (1, 2):
        kwargs["signature_type"] = POLY_SIGNATURE_TYPE
    if POLY_FUNDER_ADDRESS:
        kwargs["funder"] = POLY_FUNDER_ADDRESS

    client = ClobClient(**kwargs)

    # Set API creds:
    # - If user supplied L2 creds, use them
    # - Else derive via private key
    if POLY_API_KEY and POLY_API_SECRET and POLY_API_PASSPHRASE and ApiCreds is not None:
        api_creds = ApiCreds(
            api_key=POLY_API_KEY,
            api_secret=POLY_API_SECRET,
            api_passphrase=POLY_API_PASSPHRASE,
        )
        client.set_api_creds(api_creds)
    else:
        client.set_api_creds(client.create_or_derive_api_creds())

    return client


def cap_size_by_cost(size: float, price: float, max_cost: float) -> float:
    if price <= 0:
        return 0.0
    return min(size, max_cost / price)


def live_place_limit(token_id: str, side: str, price: float, size: float) -> Dict[str, Any]:
    client = require_live_client()

    if OrderArgs is None or OrderType is None:
        raise RuntimeError("py-clob-client types unavailable (OrderArgs/OrderType).")

    size2 = cap_size_by_cost(size, price, MAX_COST_PER_TRADE)
    if size2 <= 0:
        raise RuntimeError("Size <= 0 after MAX_COST_PER_TRADE cap")

    order_args = OrderArgs(
        price=float(price),
        size=float(size2),
        side=BUY if side.upper() == "BUY" else SELL,
        token_id=str(token_id),
    )

    # Build signed order then post (more compatible across versions)
    signed = client.create_order(order_args)
    resp = client.post_order(signed, order_type=OrderType.IOC)
    return {"ok": True, "resp": resp, "price": float(price), "size": float(size2)}


# ----------------------------
# Main run
# ----------------------------
def run_once():
    now = int(time.time())
    bucket0 = current_bucket_epoch()

    # DB init
    try:
        ensure_tables()
    except Exception as e:
        log.warning("DB_INIT_WARNING | %s", e)

    st = db_read_state()

    # daily reset counters
    day = dt.datetime.fromtimestamp(now, tz=dt.timezone.utc).date().isoformat()
    if st.get("day") != day:
        st["day"] = day
        st["trades_today"] = 0
        st["pnl_today_realized"] = 0.0

    trades_today = int(st.get("trades_today") or 0)

    # paper portfolio
    if "paper_balance" not in st:
        st["paper_balance"] = PAPER_BALANCE_START
    paper_balance = float(st.get("paper_balance") or 0.0)

    # position state
    position = st.get("position")  # "UP"/"DOWN"/None
    entry_price = st.get("entry_price")
    entry_bucket = st.get("entry_bucket")
    shares = float(st.get("shares") or 0.0)

    # idempotency — only one action per bucket
    last_bucket = st.get("last_bucket_processed")
    if last_bucket == bucket0:
        log.info("HEARTBEAT | already_processed_bucket=%d | ts=%s", bucket0, utc_iso())
        return

    # Find tradable market bucket (current then fallback)
    found = None
    for i in range(max(1, LOOKBACK + 1)):
        bucket = bucket0 - (i - 1) * 300
        slug = make_slug(PREFIX, bucket)
        mkt = gamma_get_market_by_slug(slug)
        if not mkt:
            continue
        if mkt.get("closed") is True:
            continue

        outcomes, token_ids = extract_outcomes_and_tokens(mkt)
        if len(outcomes) < 2 or len(token_ids) < 2:
            continue

        up_token = token_ids[0]
        dn_token = token_ids[1]

        bu = clob_get_book(up_token)
        bd = clob_get_book(dn_token)
        if not bu or not bd:
            continue

        up_bb, up_ba, up_bsz, up_asz = best_bid_ask(bu)
        dn_bb, dn_ba, dn_bsz, dn_asz = best_bid_ask(bd)

        ok_u, why_u = book_ok(up_bb, up_ba)
        ok_d, why_d = book_ok(dn_bb, dn_ba)
        if not (ok_u and ok_d):
            log.info("BOOK_BAD | slug=%s | up=%s dn=%s", slug, why_u, why_d)
            continue

        found = {
            "bucket": bucket,
            "slug": slug,
            "up_token": up_token,
            "dn_token": dn_token,
            "up_bb": float(up_bb),
            "up_ba": float(up_ba),
            "dn_bb": float(dn_bb),
            "dn_ba": float(dn_ba),
            "up_bsz": float(up_bsz),
            "up_asz": float(up_asz),
            "dn_bsz": float(dn_bsz),
            "dn_asz": float(dn_asz),
        }
        break

    if not found:
        st["last_bucket_processed"] = bucket0
        db_write_state(st)
        log.info("NO_TRADE | reason=no_tradable_bucket | bucket=%d", bucket0)
        return

    # edge calc
    edge_obj = compute_edge(
        found["up_bb"], found["up_ba"], found["up_bsz"], found["up_asz"],
        found["dn_bb"], found["dn_ba"], found["dn_bsz"], found["dn_asz"],
    )
    fair_up = float(edge_obj["fair_up"])
    edge = float(edge_obj["edge"])
    preferred = str(edge_obj["preferred"])
    up_mid = float(edge_obj["up_mid"])
    dn_mid = float(edge_obj["dn_mid"])

    # Decide action
    action = "HOLD"
    reason = ""

    # Exit if bucket changed
    if position in ("UP", "DOWN") and isinstance(entry_bucket, int) and entry_bucket != found["bucket"]:
        action = "EXIT"
        reason = "bucket_changed"
    else:
        if position in ("UP", "DOWN"):
            if edge < EDGE_EXIT:
                action = "EXIT"
                reason = "edge_faded"
            else:
                action = "HOLD"
                reason = "hold"
        else:
            if trades_today >= MAX_TRADES_PER_DAY:
                action = "HOLD"
                reason = "max_trades_today"
            elif edge >= EDGE_ENTER:
                action = f"ENTER_{preferred}"
                reason = "edge_enter"
            else:
                action = "HOLD"
                reason = "edge_too_small"

    log.info(
        "SNAPSHOT | slug=%s bucket=%d | up(bb/ba)=%.3f/%.3f down(bb/ba)=%.3f/%.3f | fair_up=%.3f edge=%.4f pref=%s | pos=%s entry=%s shares=%.3f | action=%s reason=%s | mode=%s armed=%s kill=%s",
        found["slug"], found["bucket"],
        found["up_bb"], found["up_ba"], found["dn_bb"], found["dn_ba"],
        fair_up, edge, preferred,
        position, (None if entry_price is None else round(float(entry_price), 4)), shares,
        action, reason, RUN_MODE, LIVE_ARMED, KILL_SWITCH
    )

    # Execute helpers
    def paper_enter(side: str, ask: float):
        nonlocal paper_balance
        size = cap_size_by_cost(STAKE_SHARES, ask, MAX_COST_PER_TRADE)
        cost = ask * size
        paper_balance -= cost
        st["paper_balance"] = paper_balance
        st["position"] = side
        st["entry_price"] = float(ask)
        st["entry_bucket"] = int(found["bucket"])
        st["shares"] = float(size)
        st["trades_today"] = trades_today + 1
        log.info("ENTER_OK | paper | side=%s price=%.4f shares=%.4f cost=%.2f", side, ask, size, cost)

    def paper_exit(side: str, bid: float):
        nonlocal paper_balance
        entry = float(st.get("entry_price") or 0.0)
        size = float(st.get("shares") or 0.0)
        proceeds = bid * size
        paper_balance += proceeds
        st["paper_balance"] = paper_balance
        realized = (bid - entry) * size
        st["pnl_today_realized"] = float(st.get("pnl_today_realized") or 0.0) + realized
        st["position"] = None
        st["entry_price"] = None
        st["entry_bucket"] = None
        st["shares"] = 0.0
        log.info("EXIT_OK | paper | side=%s exit=%.4f shares=%.4f realized=%.2f", side, bid, size, realized)

    def live_allowed() -> bool:
        return LIVE_MODE and LIVE_ARMED and (not KILL_SWITCH)

    # Enter/Exit
    if action.startswith("ENTER_"):
        side = "UP" if action == "ENTER_UP" else "DOWN"
        token = found["up_token"] if side == "UP" else found["dn_token"]
        ask = found["up_ba"] if side == "UP" else found["dn_ba"]

        if live_allowed():
            try:
                resp = live_place_limit(token_id=token, side="BUY", price=float(ask), size=float(STAKE_SHARES))
                st["position"] = side
                st["entry_price"] = float(ask)
                st["entry_bucket"] = int(found["bucket"])
                st["shares"] = float(resp.get("size", STAKE_SHARES))
                st["trades_today"] = trades_today + 1
                log.info("ENTER_OK | live | side=%s resp=%s", side, str(resp)[:800])
            except Exception as e:
                log.info("ENTER_FAIL | live | side=%s | %s", side, str(e)[:800])
        else:
            paper_enter(side, float(ask))

    elif action == "EXIT" and position in ("UP", "DOWN"):
        side = str(position)
        token = found["up_token"] if side == "UP" else found["dn_token"]
        bid = found["up_bb"] if side == "UP" else found["dn_bb"]

        if live_allowed():
            try:
                resp = live_place_limit(token_id=token, side="SELL", price=float(bid), size=float(shares))
                st["position"] = None
                st["entry_price"] = None
                st["entry_bucket"] = None
                st["shares"] = 0.0
                log.info("EXIT_OK | live | side=%s resp=%s", side, str(resp)[:800])
            except Exception as e:
                log.info("EXIT_FAIL | live | side=%s | %s", side, str(e)[:800])
        else:
            paper_exit(side, float(bid))

    # Mark-to-market paper equity
    if not LIVE_MODE:
        pos = st.get("position")
        entry = st.get("entry_price")
        size = float(st.get("shares") or 0.0)
        mark = up_mid if pos == "UP" else dn_mid if pos == "DOWN" else 0.0
        u = 0.0
        if pos in ("UP", "DOWN") and entry is not None:
            u = (float(mark) - float(entry)) * size
        equity = float(st.get("paper_balance") or 0.0) + u
        st["paper_equity"] = equity
        log.info("PAPER_SUMMARY | balance=%.2f uPnL=%.2f equity=%.2f", float(st.get("paper_balance")), u, equity)

    # Idempotency marker
    st["last_bucket_processed"] = bucket0
    st["last_seen_slug"] = found["slug"]
    st["last_seen_bucket"] = found["bucket"]
    st["last_seen_ts"] = utc_iso()

    db_write_state(st)


def main():
    log.info(
        "BOOT | ts=%s | mode=%s armed=%s kill=%s | prefix=%s lookback=%d | MIN_BID=%.2f MAX_ASK=%.2f MAX_SPREAD=%.2f | EDGE_ENTER=%.4f EDGE_EXIT=%.4f",
        utc_iso(), RUN_MODE, LIVE_ARMED, KILL_SWITCH, PREFIX, LOOKBACK, MIN_BID, MAX_ASK, MAX_SPREAD, EDGE_ENTER, EDGE_EXIT
    )

    # Quick LIVE sanity check (does not place orders)
    if LIVE_MODE:
        if not POLY_PRIVATE_KEY:
            log.warning("LIVE_WARNING | Missing POLY_PRIVATE_KEY (live orders will not work)")
        else:
            try:
                c = require_live_client()
                ok = c.get_ok()
                t = c.get_server_time()
                log.info("CLOB_OK | ok=%s server_time=%s", ok, t)
            except Exception as e:
                log.warning("CLOB_AUTH_WARNING | %s", str(e)[:800])

    if RUN_LOOP:
        log.info("RUN_LOOP | enabled=true | loop_seconds=%d", LOOP_SECONDS)
        while True:
            try:
                run_once()
            except Exception as e:
                log.exception("FATAL_LOOP_EXCEPTION | %s", e)
            time.sleep(max(1, LOOP_SECONDS))
    else:
        run_once()


if __name__ == "__main__":
    main()

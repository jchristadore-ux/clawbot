import os
import json
import time
import logging
from datetime import date, datetime, timezone
from typing import Optional, Dict, Any, Tuple

import requests
import psycopg2

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("johnny5")

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
    with db_conn() as conn:
        with conn.cursor() as cur:
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
            cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS last_trade_ts TIMESTAMPTZ;")
            cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;")

            cur.execute("SELECT 1 FROM bot_state WHERE key='main';")
            if cur.fetchone() is None:
                cur.execute(
                    """
                    INSERT INTO bot_state (key, as_of_date, position, entry_price, stake, trades_today, pnl_today_realized, last_trade_ts, updated_at)
                    VALUES ('main', CURRENT_DATE, NULL, NULL, 0, 0, 0, NULL, NOW());
                    """
                )

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

# ----------------------------
# Gamma / CLOB
# ----------------------------
POLY_GAMMA_HOST = os.getenv("POLY_GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
POLY_CLOB_HOST  = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").strip()
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
        k = str(o).strip().upper()
        if k in ("YES", "UP"):
            yes_id = str(tid)
        if k in ("NO", "DOWN"):
            no_id = str(tid)
    return yes_id, no_id

def clob_best_bid_ask(token_id: str) -> Tuple[Optional[float], Optional[float], str]:
    url = f"{POLY_CLOB_HOST}/book"
    code, j, _ = http_get_json(url, params={"token_id": token_id}, timeout=15)
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
# Strategy / gates
# ----------------------------
MAX_SPREAD = env_float("MAX_SPREAD", 0.08)
MIN_BID    = env_float("MIN_BID", 0.02)
MAX_ASK    = env_float("MAX_ASK", 0.98)

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
    return True, "OK"

EDGE_ENTER = env_float("EDGE_ENTER", 0.12)
EDGE_EXIT  = env_float("EDGE_EXIT", 0.04)

def compute_signal(yes_mid: float, fair_up: float = 0.50) -> Tuple[str, float]:
    edge = fair_up - yes_mid
    if edge >= EDGE_ENTER:
        return "YES", edge
    if edge <= -EDGE_ENTER:
        return "NO", edge
    return "HOLD", edge

def run_once() -> None:
    ensure_tables()

    # reset (optional)
    if env_bool("RESET_STATE", False):
        mode = os.getenv("RESET_STATE_MODE", "FULL")
        log.warning("RESET_STATE=true detected. Resetting DB state now (mode=%s)...", mode)
        reset_state_db(mode=mode)

    state = load_state()
    if state["as_of_date"] != date.today():
        state["as_of_date"] = date.today()
        state["trades_today"] = 0
        state["pnl_today_realized"] = 0.0
        save_state(state)
        state = load_state()

    slug = find_tradable_slug(POLY_MARKET_SLUG)
    if not slug:
        log.warning("No tradable slug found")
        return

    log.info("Checking slug=%s", slug)
    log.info("Found tradable slug=%s", slug)

    m = fetch_gamma_market_by_slug(slug)
    if not m:
        log.warning("Gamma market missing")
        return

    yes_token, no_token = extract_yes_no_token_ids(m)
    if not yes_token or not no_token:
        raise RuntimeError(f"Token extraction failed for slug={slug}")

    yes_bid, yes_ask, yes_src = clob_best_bid_ask(yes_token)
    no_bid,  no_ask,  no_src  = clob_best_bid_ask(no_token)

    yes_ok, yes_why = is_book_ok(yes_bid, yes_ask)
    no_ok,  no_why  = is_book_ok(no_bid, no_ask)

    yes_mid = mid_from_bid_ask(yes_bid, yes_ask)
    no_mid  = mid_from_bid_ask(no_bid, no_ask)

    if not yes_ok or not no_ok:
        reason = f"YES_{yes_why}" if not yes_ok else f"NO_{no_why}"
        log.info(
            "slug=%s | YES %s mid=%s | NO %s mid=%s | action=NO_TRADE | reason=%s | pos=%s | trades_today=%d",
            slug,
            yes_src, (None if yes_mid is None else f"{yes_mid:.4f}"),
            no_src,  (None if no_mid is None else f"{no_mid:.4f}"),
            reason,
            state["position"],
            int(state["trades_today"] or 0),
        )
        return

    signal, edge = compute_signal(float(yes_mid))
    log.info(
        "slug=%s | YES %s mid=%.4f | NO %s mid=%.4f | edge=%+.4f | signal=%s | action=NO_TRADE | reason=PHASE4_WAIT_FOR_REAL_BOOK",
        slug, yes_src, float(yes_mid), no_src, float(no_mid), float(edge), signal
    )

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

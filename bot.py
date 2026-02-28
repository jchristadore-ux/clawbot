import os
import time
import json
import logging
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any, List
from datetime import datetime, timezone, date

import requests
import psycopg2


# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
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


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except Exception:
        return default


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def five_min_bucket_epoch(ts: Optional[float] = None) -> int:
    if ts is None:
        ts = time.time()
    return int(ts // 300) * 300


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


def _maybe_json(x):
    """
    Gamma sometimes returns outcomes/clobTokenIds as JSON-encoded strings.
    """
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
                  id INTEGER PRIMARY KEY DEFAULT 1,
                  as_of_date DATE,
                  position TEXT,
                  entry_price DOUBLE PRECISION,
                  stake DOUBLE PRECISION,
                  trades_today INTEGER,
                  pnl_today_realized DOUBLE PRECISION
                );
                """
            )
            cur.execute(
                """
                INSERT INTO bot_state (id, as_of_date, position, entry_price, stake, trades_today, pnl_today_realized)
                VALUES (1, CURRENT_DATE, NULL, NULL, 0, 0, 0)
                ON CONFLICT (id) DO NOTHING;
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
                "SELECT as_of_date, position, entry_price, stake, trades_today, pnl_today_realized FROM bot_state WHERE id=1;"
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
                       pnl_today_realized=%s
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

# Base slug prefix you want to trade (no bucket)
# Example: btc-updown-5m
POLY_MARKET_SLUG = os.getenv("POLY_MARKET_SLUG", "").strip()

# Some setups used POLY_EVENT_SLUG historically; keep compatible.
POLY_EVENT_SLUG = os.getenv("POLY_EVENT_SLUG", "").strip()

# If you set POLY_GAMMA_SLUG, we treat it as the base prefix too (optional)
POLY_GAMMA_SLUG = os.getenv("POLY_GAMMA_SLUG", "").strip()


def slug_prefix() -> str:
    """
    Determine the base prefix used to construct full rolling slugs:
      <prefix>-<bucketEpoch>
    """
    for v in (POLY_MARKET_SLUG, POLY_GAMMA_SLUG, POLY_EVENT_SLUG):
        if v and v.strip():
            return v.strip()
    raise RuntimeError("Missing POLY_MARKET_SLUG (recommended) or POLY_GAMMA_SLUG / POLY_EVENT_SLUG")


def make_poly_slug_for_bucket(bucket_epoch: int) -> str:
    return f"{slug_prefix()}-{bucket_epoch}"


def fetch_gamma_market_by_slug(full_slug: str) -> Optional[dict]:
    url = f"{POLY_GAMMA_HOST}/markets/slug/{full_slug}"
    return http_get_json(url)


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
        ou = o.strip().upper()
        if ou == "YES":
            yes_id = str(tid)
        elif ou == "NO":
            no_id = str(tid)
    return yes_id, no_id


def fetch_gamma_market_and_tokens_for_full_slug(full_slug: str) -> Optional[Dict[str, Any]]:
    m = fetch_gamma_market_by_slug(full_slug)
    if not m or not isinstance(m, dict):
        return None
    yes_id, no_id = extract_yes_no_token_ids(m)
    if not yes_id or not no_id:
        return None
    return {"market": m, "yes_token_id": yes_id, "no_token_id": no_id}


@dataclass
class BookTop:
    bid: Optional[float]
    ask: Optional[float]
    bid_size: float = 0.0
    ask_size: float = 0.0

    def __str__(self) -> str:
        if self.bid is None and self.ask is None:
            return "no_book"
        if self.bid is None:
            return f"book_ask({self.ask})"
        if self.ask is None:
            return f"book_bid({self.bid})"
        return f"book(bid={self.bid},ask={self.ask})"


def _parse_price_level(level) -> Tuple[Optional[float], Optional[float]]:
    """
    CLOB book levels often look like ["0.51","12.3"] or {"price":"0.51","size":"12.3"}.
    """
    try:
        if isinstance(level, list) and len(level) >= 2:
            return float(level[0]), float(level[1])
        if isinstance(level, dict):
            p = level.get("price")
            s = level.get("size")
            if p is None or s is None:
                return None, None
            return float(p), float(s)
    except Exception:
        return None, None
    return None, None


def fetch_clob_book_top(token_id: str) -> Tuple[Optional[BookTop], Optional[int]]:
    """
    Returns (BookTop|None, http_status|None). If status is 404, means no orderbook exists.
    """
    url = f"{POLY_CLOB_HOST}/book"
    try:
        r = requests.get(url, params={"token_id": token_id}, timeout=15)
        if r.status_code != 200:
            return None, r.status_code
        j = r.json()
    except Exception:
        return None, None

    bids = j.get("bids") or []
    asks = j.get("asks") or []

    best_bid = None
    best_bid_size = 0.0
    if isinstance(bids, list) and bids:
        p, s = _parse_price_level(bids[0])
        if p is not None:
            best_bid = float(p)
            best_bid_size = float(s or 0.0)

    best_ask = None
    best_ask_size = 0.0
    if isinstance(asks, list) and asks:
        p, s = _parse_price_level(asks[0])
        if p is not None:
            best_ask = float(p)
            best_ask_size = float(s or 0.0)

    return BookTop(bid=best_bid, ask=best_ask, bid_size=best_bid_size, ask_size=best_ask_size), 200


def clob_midpoint(token_id: str) -> Optional[float]:
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
# Book Quality Gates (Phase 4)
# ----------------------------
MIN_BID = env_float("MIN_BID", 0.02)
MAX_ASK = env_float("MAX_ASK", 0.98)
MAX_SPREAD = env_float("MAX_SPREAD", 0.10)

LOOKBACK_BUCKETS = env_int("LOOKBACK_BUCKETS", 96)

# If true, we only run Phase 4 "find real book" + logging, and exit (no trading).
PHASE4_ONLY = env_bool("PHASE4_ONLY", True)


def book_quality_reasons(label: str, top: Optional[BookTop], http_status: Optional[int]) -> List[str]:
    reasons: List[str] = []
    if http_status == 404:
        reasons.append(f"{label}_NO_BOOK_404")
        return reasons
    if top is None:
        reasons.append(f"{label}_NO_BOOK")
        return reasons
    if top.bid is None or top.ask is None:
        reasons.append(f"{label}_MISSING_BID_OR_ASK")
        return reasons

    if top.bid < MIN_BID:
        reasons.append(f"{label}_BID_TOO_LOW({top.bid}< {MIN_BID})")
    if top.ask > MAX_ASK:
        reasons.append(f"{label}_ASK_TOO_HIGH({top.ask}> {MAX_ASK})")
    spread = top.ask - top.bid
    if spread > MAX_SPREAD:
        reasons.append(f"{label}_WIDE_SPREAD({spread:.4f}> {MAX_SPREAD})")
    return reasons


@dataclass
class TradableSelection:
    slug: str
    bucket: int
    yes_token: str
    no_token: str
    yes_top: BookTop
    no_top: BookTop


def find_tradable_slug_with_real_book() -> Tuple[Optional[TradableSelection], Dict[str, int]]:
    """
    Sweep backward across buckets and find the first bucket where BOTH YES and NO
    books pass quality gates.
    """
    now_bucket = five_min_bucket_epoch(time.time())

    counters = {
        "tried": 0,
        "gamma_missing": 0,
        "token_missing": 0,
        "clob_404": 0,
        "book_missing_side": 0,
        "book_bad": 0,
        "ok": 0,
    }

    for i in range(LOOKBACK_BUCKETS):
        bucket = now_bucket - i * 300
        full_slug = make_poly_slug_for_bucket(bucket)
        counters["tried"] += 1

        # PROVE we are iterating buckets
        log.info(
            "TRY_BUCKET | i=%d | bucket=%d | utc=%s | slug=%s",
            i,
            bucket,
            datetime.fromtimestamp(bucket, tz=timezone.utc).isoformat(),
            full_slug,
        )

        gamma = fetch_gamma_market_and_tokens_for_full_slug(full_slug)
        if not gamma:
            counters["gamma_missing"] += 1
            continue

        yes_token = gamma.get("yes_token_id")
        no_token = gamma.get("no_token_id")
        if not yes_token or not no_token:
            counters["token_missing"] += 1
            continue

        yes_top, yes_status = fetch_clob_book_top(yes_token)
        no_top, no_status = fetch_clob_book_top(no_token)

        # Track 404s
        if yes_status == 404 or no_status == 404:
            counters["clob_404"] += 1

        # Apply gates
        reasons = []
        reasons += book_quality_reasons("YES", yes_top, yes_status)
        reasons += book_quality_reasons("NO", no_top, no_status)

        if reasons:
            # Specific counters for quick diagnosis
            if any("MISSING_BID_OR_ASK" in r for r in reasons):
                counters["book_missing_side"] += 1
            else:
                counters["book_bad"] += 1

            # Preserve your existing style of "Skipping slug=..."
            yes_desc = f"{yes_top}" if yes_top else f"no_book(code={yes_status})"
            no_desc = f"{no_top}" if no_top else f"no_book(code={no_status})"
            log.info(
                "Skipping slug=%s due to book quality: BOOK_BAD(%s) | YES %s | NO %s",
                full_slug,
                ",".join(reasons),
                yes_desc,
                no_desc,
            )
            continue

        counters["ok"] += 1
        sel = TradableSelection(
            slug=full_slug,
            bucket=bucket,
            yes_token=str(yes_token),
            no_token=str(no_token),
            yes_top=yes_top,
            no_top=no_top,
        )
        log.info(
            "PHASE4_OK | Selected tradable slug with real book: %s | YES %s | NO %s",
            sel.slug,
            sel.yes_top,
            sel.no_top,
        )
        return sel, counters

    return None, counters


# ----------------------------
# Bun live order gateway (kept for later phases)
# ----------------------------
BUN_BASE_URL = os.getenv("BUN_BASE_URL", "").strip()


def bun_health() -> Optional[int]:
    if not BUN_BASE_URL:
        return None
    code, _, _ = http_post_json(f"{BUN_BASE_URL}/__ping", payload={}, timeout=8)
    if code == 0 or code >= 400:
        try:
            r = requests.get(f"{BUN_BASE_URL}/", timeout=8)
            return r.status_code
        except Exception:
            return None
    return code


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

    log.info(
        "run_mode=%s live_mode=%s live_armed=%s prefix=%s lookback=%d MIN_BID=%.3f MAX_ASK=%.3f MAX_SPREAD=%.3f PHASE4_ONLY=%s",
        run_mode,
        live_mode,
        live_armed,
        slug_prefix(),
        LOOKBACK_BUCKETS,
        MIN_BID,
        MAX_ASK,
        MAX_SPREAD,
        PHASE4_ONLY,
    )

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

    # Phase 4: find real tradable bucket
    sel, counters = find_tradable_slug_with_real_book()

    log.info(
        "SEARCH_SUMMARY | tried=%d | gamma_missing=%d | token_missing=%d | clob_404=%d | book_missing_side=%d | book_bad=%d | ok=%d",
        counters["tried"],
        counters["gamma_missing"],
        counters["token_missing"],
        counters["clob_404"],
        counters["book_missing_side"],
        counters["book_bad"],
        counters["ok"],
    )

    if not sel:
        log.info("NO_TRADE | reason=NO_REAL_BOOK_FOUND | detail=NO_BUCKET_WITH_REAL_BOOK | lookback=%d", LOOKBACK_BUCKETS)
        log.info("BOOT: bot.py finished cleanly")
        return

    # If Phase4-only, stop here (no trading logic yet).
    if PHASE4_ONLY:
        log.info("PHASE4_ONLY | slug=%s | next_step=RE_ENABLE_PAPER_TRADING_LOGIC_WHEN_READY", sel.slug)
        log.info("BOOT: bot.py finished cleanly")
        return

    # --- Later phases would continue from here using sel.slug/sel.tokens ---
    # Keeping placeholder for now so you can flip PHASE4_ONLY=false when ready.
    log.info("PHASE4_DONE | slug=%s | (trading logic currently disabled in this phase)", sel.slug)
    log.info("BOOT: bot.py finished cleanly")


if __name__ == "__main__":
    main()

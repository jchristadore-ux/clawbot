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
# Env helpers
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


def five_min_bucket_epoch(ts: Optional[float] = None) -> int:
    if ts is None:
        ts = time.time()
    return int(ts // 300) * 300


# ----------------------------
# HTTP helpers
# ----------------------------
def http_get_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = 20) -> Tuple[Optional[dict], Optional[int]]:
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return None, r.status_code
        return r.json(), 200
    except Exception:
        return None, None


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


# ----------------------------
# DB (unique table names)
# ----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
STATE_TABLE = os.getenv("J5_STATE_TABLE", "j5_state").strip()


def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def ensure_tables() -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
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
                f"""
                INSERT INTO {STATE_TABLE} (id, as_of_date, position, entry_price, stake, trades_today, pnl_today_realized)
                VALUES (1, CURRENT_DATE, NULL, NULL, 0, 0, 0)
                ON CONFLICT (id) DO NOTHING;
                """
            )
        conn.commit()


def load_state() -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT as_of_date, position, entry_price, stake, trades_today, pnl_today_realized FROM {STATE_TABLE} WHERE id=1;"
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
                f"""
                UPDATE {STATE_TABLE}
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


# ----------------------------
# Gamma / CLOB
# ----------------------------
POLY_GAMMA_HOST = os.getenv("POLY_GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
POLY_CLOB_HOST = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").strip()

POLY_MARKET_SLUG = os.getenv("POLY_MARKET_SLUG", "").strip()
POLY_EVENT_SLUG = os.getenv("POLY_EVENT_SLUG", "").strip()
POLY_GAMMA_SLUG = os.getenv("POLY_GAMMA_SLUG", "").strip()


def slug_prefix() -> str:
    for v in (POLY_MARKET_SLUG, POLY_GAMMA_SLUG, POLY_EVENT_SLUG):
        if v and v.strip():
            return v.strip()
    raise RuntimeError("Missing POLY_MARKET_SLUG (recommended) or POLY_GAMMA_SLUG / POLY_EVENT_SLUG")


def fetch_gamma_market_by_slug(full_slug: str) -> Optional[dict]:
    url = f"{POLY_GAMMA_HOST}/markets/slug/{full_slug}"
    j, code = http_get_json(url)
    return j if code == 200 and isinstance(j, dict) else None


def gamma_search_markets(prefix: str, limit: int = 50) -> List[dict]:
    """
    Uses Gamma search endpoint to find markets matching a term.
    Gamma supports /markets?search=<term> on many deployments.
    If Gamma changes, this will just return [] and we log it.
    """
    url = f"{POLY_GAMMA_HOST}/markets"
    params = {"search": prefix, "limit": limit}
    j, code = http_get_json(url, params=params)
    if code != 200 or not j:
        return []
    if isinstance(j, list):
        return [x for x in j if isinstance(x, dict)]
    # sometimes it comes back as {"markets":[...]}
    markets = j.get("markets") if isinstance(j, dict) else None
    if isinstance(markets, list):
        return [x for x in markets if isinstance(x, dict)]
    return []


def parse_bucket_from_slug(slug: str) -> Optional[int]:
    """
    Many rolling slugs end with '-<epoch>'.
    """
    try:
        tail = slug.split("-")[-1]
        if tail.isdigit():
            return int(tail)
    except Exception:
        return None
    return None


def seed_from_gamma(prefix: str) -> Tuple[Optional[str], Optional[int]]:
    """
    Find a real, currently-listed rolling slug from Gamma, then extract its bucket epoch.
    """
    markets = gamma_search_markets(prefix, limit=50)
    if not markets:
        return None, None

    # Prefer markets whose slug starts with prefix and end in a numeric bucket
    candidates = []
    for m in markets:
        s = m.get("slug")
        if not isinstance(s, str):
            continue
        if not s.startswith(prefix):
            continue
        b = parse_bucket_from_slug(s)
        if b is not None:
            candidates.append((b, s))

    if not candidates:
        # Fall back: any market with numeric suffix, even if prefix mismatch
        for m in markets:
            s = m.get("slug")
            if not isinstance(s, str):
                continue
            b = parse_bucket_from_slug(s)
            if b is not None:
                candidates.append((b, s))

    if not candidates:
        return None, None

    # Pick the most recent bucket
    candidates.sort(key=lambda x: x[0], reverse=True)
    bucket, slug = candidates[0]
    return slug, bucket


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


# ----------------------------
# Phase 4 Book Quality Gates
# ----------------------------
MIN_BID = env_float("MIN_BID", 0.02)
MAX_ASK = env_float("MAX_ASK", 0.98)
MAX_SPREAD = env_float("MAX_SPREAD", 0.08)
LOOKBACK_BUCKETS = env_int("LOOKBACK_BUCKETS", 96)
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


def make_slug_from_seed(seed_slug: str, seed_bucket: int, target_bucket: int) -> str:
    """
    Preserve the EXACT slug shape Gamma uses:
    replace the trailing -<bucket> with the target bucket.
    """
    # remove trailing "-<seed_bucket>"
    base = seed_slug[: -(len(str(seed_bucket)) + 1)]
    return f"{base}-{target_bucket}"


def find_tradable_slug_with_real_book() -> Tuple[Optional[TradableSelection], Dict[str, int]]:
    counters = {
        "tried": 0,
        "gamma_missing": 0,
        "token_missing": 0,
        "clob_404": 0,
        "book_missing_side": 0,
        "book_bad": 0,
        "ok": 0,
    }

    prefix = slug_prefix()
    seed_slug, seed_bucket = seed_from_gamma(prefix)

    log.info("SEED | prefix=%s | seed_slug=%s | seed_bucket=%s", prefix, seed_slug, seed_bucket)

    if not seed_slug or seed_bucket is None:
        # Fall back to naive method (so we still see TRY_BUCKET logs),
        # but we already learned naive isn't working for your prefix.
        now_bucket = five_min_bucket_epoch(time.time())
        for i in range(LOOKBACK_BUCKETS):
            bucket = now_bucket - i * 300
            full_slug = f"{prefix}-{bucket}"
            counters["tried"] += 1
            log.info("TRY_BUCKET | i=%d | bucket=%d | utc=%s | slug=%s", i, bucket, datetime.fromtimestamp(bucket, tz=timezone.utc).isoformat(), full_slug)
            gamma = fetch_gamma_market_and_tokens_for_full_slug(full_slug)
            if not gamma:
                counters["gamma_missing"] += 1
        return None, counters

    # Walk back from the seed bucket (known-good formatting)
    for i in range(LOOKBACK_BUCKETS):
        bucket = seed_bucket - i * 300
        full_slug = make_slug_from_seed(seed_slug, seed_bucket, bucket)
        counters["tried"] += 1

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

        if yes_status == 404 or no_status == 404:
            counters["clob_404"] += 1

        reasons = []
        reasons += book_quality_reasons("YES", yes_top, yes_status)
        reasons += book_quality_reasons("NO", no_top, no_status)

        if reasons:
            if any("MISSING_BID_OR_ASK" in r for r in reasons):
                counters["book_missing_side"] += 1
            else:
                counters["book_bad"] += 1

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
        log.info("PHASE4_OK | Selected tradable slug with real book: %s | YES %s | NO %s", sel.slug, sel.yes_top, sel.no_top)
        return sel, counters

    return None, counters


# ----------------------------
# Bun health (optional)
# ----------------------------
BUN_BASE_URL = os.getenv("BUN_BASE_URL", "").strip()


def bun_health() -> Optional[int]:
    if not BUN_BASE_URL:
        return None
    try:
        r = requests.get(f"{BUN_BASE_URL}/", timeout=8)
        return r.status_code
    except Exception:
        return None


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
        "run_mode=%s live_mode=%s live_armed=%s prefix=%s lookback=%d MIN_BID=%.3f MAX_ASK=%.3f MAX_SPREAD=%.3f PHASE4_ONLY=%s db_state_table=%s",
        run_mode,
        live_mode,
        live_armed,
        slug_prefix(),
        LOOKBACK_BUCKETS,
        MIN_BID,
        MAX_ASK,
        MAX_SPREAD,
        PHASE4_ONLY,
        STATE_TABLE,
    )

    if BUN_BASE_URL:
        hs = bun_health()
        if hs is not None:
            log.info("bun_health_status=%s", hs)

    state = load_state()
    today = date.today()
    if state["as_of_date"] != today:
        state["as_of_date"] = today
        state["trades_today"] = 0
        state["pnl_today_realized"] = 0.0
        save_state(state)

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
        log.info("NO_TRADE | reason=NO_REAL_BOOK_FOUND | detail=NO_BUCKET_WITH_REAL_BOOK_OR_GAMMA_NOT_FOUND | lookback=%d", LOOKBACK_BUCKETS)
        log.info("BOOT: bot.py finished cleanly")
        return

    if PHASE4_ONLY:
        log.info("PHASE4_ONLY | slug=%s | next_step=RE_ENABLE_PAPER_TRADING_LOGIC_WHEN_READY", sel.slug)
        log.info("BOOT: bot.py finished cleanly")
        return

    log.info("PHASE4_DONE | slug=%s | (trading logic disabled in this phase)", sel.slug)
    log.info("BOOT: bot.py finished cleanly")


if __name__ == "__main__":
    main()

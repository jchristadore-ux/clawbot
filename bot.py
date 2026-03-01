#!/usr/bin/env python3
import os
import json
import time
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests

# Optional DB (safe if DATABASE_URL is not set)
try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # type: ignore


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("bot")


# ----------------------------
# Config
# ----------------------------
RUN_MODE = os.getenv("RUN_MODE", "PAPER").upper()  # PAPER or LIVE
LIVE_MODE = RUN_MODE == "LIVE"
LIVE_ARMED = os.getenv("LIVE_ARMED", "false").lower() in ("1", "true", "yes", "y")

PREFIX = os.getenv("PREFIX", "btc-updown-5m").strip()
SEED_SLUG = os.getenv("SEED_SLUG", "").strip()  # e.g. btc-updown-5m-1772308500

LOOKBACK = int(os.getenv("LOOKBACK", "240"))

# Market quality constraints
MIN_BID = float(os.getenv("MIN_BID", "0.20"))
MAX_ASK = float(os.getenv("MAX_ASK", "0.80"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.15"))

# Debug flags
DEBUG_GAMMA = os.getenv("DEBUG_GAMMA", "0").lower() in ("1", "true", "yes", "y")
DEBUG_BOOK = os.getenv("DEBUG_BOOK", "0").lower() in ("1", "true", "yes", "y")

# DB state table + key
DB_STATE_TABLE = os.getenv("DB_STATE_TABLE", "j5_state_v2")
DB_KEY = os.getenv("DB_KEY", PREFIX)

# Endpoints
BUN_HEALTH_URL = os.getenv("BUN_HEALTH_URL", "http://localhost:3000/health").strip()
GAMMA_BASE = os.getenv("GAMMA_BASE", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_BASE = os.getenv("CLOB_BASE", "https://clob.polymarket.com").rstrip("/")

# Bun order routing
BUN_BASE_URL = os.getenv("BUN_BASE_URL", "").strip()
if not BUN_BASE_URL:
    BUN_BASE_URL = BUN_HEALTH_URL.rsplit("/", 1)[0]
BUN_ORDER_URL = os.getenv("BUN_ORDER_URL", f"{BUN_BASE_URL.rstrip('/')}/order").strip()

# Live smoke trade controls
LIVE_SMOKE = os.getenv("LIVE_SMOKE", "false").lower() in ("1", "true", "yes", "y")
LIVE_SMOKE_SIDE = os.getenv("LIVE_SMOKE_SIDE", "A").upper()  # "A" (Up) or "B" (Down)
LIVE_SMOKE_ORDER_TYPE = os.getenv("LIVE_SMOKE_ORDER_TYPE", "IOC").upper()
LIVE_SMOKE_MAX_COST = float(os.getenv("LIVE_SMOKE_MAX_COST", "1.00"))
LIVE_SMOKE_SIZE = float(os.getenv("LIVE_SMOKE_SIZE", "1.0"))

# Smoke bypass controls (lets us prove execution plumbing even if market is wide)
LIVE_SMOKE_BYPASS = os.getenv("LIVE_SMOKE_BYPASS", "false").lower() in ("1", "true", "yes", "y")
LIVE_SMOKE_BYPASS_MAX_ASK = float(os.getenv("LIVE_SMOKE_BYPASS_MAX_ASK", "0.99"))
LIVE_SMOKE_BYPASS_MIN_BID = float(os.getenv("LIVE_SMOKE_BYPASS_MIN_BID", "0.01"))

# Looping
RUN_LOOP = os.getenv("RUN_LOOP", "false").lower() in ("1", "true", "yes", "y")
LOOP_SECONDS = int(os.getenv("LOOP_SECONDS", "30"))

# Request tuning
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "2"))
RETRY_SLEEP = float(os.getenv("RETRY_SLEEP", "0.35"))


# ----------------------------
# Helpers
# ----------------------------
def _now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _safe_get(d: Any, k: str, default: Any = None) -> Any:
    try:
        if isinstance(d, dict):
            return d.get(k, default)
    except Exception:
        pass
    return default


def _maybe_json(x: Any) -> Any:
    """
    Gamma sometimes returns fields (outcomes, clobTokenIds) as JSON-encoded strings.
    """
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


def _coerce_list_str(x: Any) -> List[str]:
    x = _maybe_json(x)
    if not isinstance(x, list):
        return []
    out: List[str] = []
    for v in x:
        if v is None:
            continue
        out.append(str(v))
    return out


def debug_gamma_market(market: Dict[str, Any], slug: str) -> None:
    """Debug-only. No flow control."""
    if not DEBUG_GAMMA:
        return
    payload = {
        "slug": slug,
        "id": _safe_get(market, "id"),
        "conditionId": _safe_get(market, "conditionId"),
        "question": _safe_get(market, "question"),
        "active": _safe_get(market, "active"),
        "closed": _safe_get(market, "closed"),
        "archived": _safe_get(market, "archived"),
        "enableOrderBook": _safe_get(market, "enableOrderBook"),
        "liquidity": _safe_get(market, "liquidity"),
        "liquidityClob": _safe_get(market, "liquidityClob"),
        "liquidityAmm": _safe_get(market, "liquidityAmm"),
        "outcomes_type": type(_safe_get(market, "outcomes")).__name__,
        "clobTokenIds_type": type(_safe_get(market, "clobTokenIds")).__name__,
        "outcomes_parsed": _maybe_json(_safe_get(market, "outcomes")),
        "clobTokenIds_parsed": _maybe_json(_safe_get(market, "clobTokenIds")),
    }
    try:
        log.info("GAMMA_DEBUG: %s", json.dumps(payload, default=str))
    except Exception:
        log.info("GAMMA_DEBUG: %s", payload)


def _http_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> Tuple[int, Any]:
    last_exc = None
    for _ in range(max(1, HTTP_RETRIES + 1)):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
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
    raise RuntimeError(f"HTTP GET failed after retries: {url} params={params} exc={last_exc}")


def _http_post(url: str, payload: dict, headers: Optional[dict] = None) -> Tuple[int, Any]:
    last_exc = None
    for _ in range(max(1, HTTP_RETRIES + 1)):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT)
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
    raise RuntimeError(f"HTTP POST failed after retries: {url} payload={payload} exc={last_exc}")


def bun_healthcheck() -> int:
    try:
        code, _ = _http_get(BUN_HEALTH_URL)
        return int(code)
    except Exception:
        return 0


# ----------------------------
# DB State (safe + minimal)
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


# ----------------------------
# Gamma + Token resolution
# ----------------------------
def gamma_get_market_by_slug(slug: str) -> Tuple[int, Optional[Dict[str, Any]]]:
    url1 = f"{GAMMA_BASE}/markets"
    code, data = _http_get(url1, params={"slug": slug})
    if code == 200:
        if isinstance(data, list) and data:
            return code, data[0]
        if isinstance(data, dict) and data.get("markets") and isinstance(data["markets"], list) and data["markets"]:
            return code, data["markets"][0]
        if isinstance(data, dict) and data.get("slug") == slug:
            return code, data

    url2 = f"{GAMMA_BASE}/markets/{slug}"
    code2, data2 = _http_get(url2)
    if code2 == 200 and isinstance(data2, dict):
        return code2, data2

    return int(code) if isinstance(code, int) else 0, None


def extract_outcomes_and_tokens(mkt: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    outcomes = _maybe_json(_safe_get(mkt, "outcomes"))
    token_ids = _maybe_json(_safe_get(mkt, "clobTokenIds"))

    if isinstance(outcomes, dict) and "outcomes" in outcomes:
        outcomes = outcomes["outcomes"]
    if isinstance(token_ids, dict) and "clobTokenIds" in token_ids:
        token_ids = token_ids["clobTokenIds"]

    outcomes_list = _coerce_list_str(outcomes)
    token_ids_list = _coerce_list_str(token_ids)

    if not outcomes_list and isinstance(outcomes, str):
        outcomes_list = [s.strip() for s in outcomes.split(",") if s.strip()]
    if not token_ids_list and isinstance(token_ids, str):
        token_ids_list = [s.strip() for s in token_ids.split(",") if s.strip()]

    return outcomes_list, token_ids_list


# ----------------------------
# CLOB orderbook
# ----------------------------
def clob_get_book(token_id: str) -> Tuple[int, Optional[Dict[str, Any]]]:
    url = f"{CLOB_BASE}/book"
    code, data = _http_get(url, params={"token_id": token_id})
    if code == 200 and isinstance(data, dict):
        return code, data
    return code, None


def best_bid_ask(book: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    bb = None
    ba = None
    try:
        if bids and isinstance(bids, list) and isinstance(bids[0], dict):
            bb = float(bids[0].get("price"))
        if asks and isinstance(asks, list) and isinstance(asks[0], dict):
            ba = float(asks[0].get("price"))
    except Exception:
        return None, None
    return bb, ba


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


def smoke_ok(bb: Optional[float], ba: Optional[float]) -> Tuple[bool, str]:
    if bb is None or ba is None:
        return False, "MISSING_BID_OR_ASK"
    if bb < LIVE_SMOKE_BYPASS_MIN_BID:
        return False, f"BID_TOO_LOW({bb:.3f})"
    if ba > LIVE_SMOKE_BYPASS_MAX_ASK:
        return False, f"ASK_TOO_HIGH({ba:.3f})"
    return True, "OK"


def bun_place_order(token_id: str, side: str, price: float, size: float, order_type: str = "IOC") -> Tuple[int, Any]:
    payload = {
        "token_id": str(token_id),
        "side": side.upper(),  # BUY / SELL
        "price": float(price),
        "size": float(size),
        "order_type": order_type.upper(),
    }
    return _http_post(BUN_ORDER_URL, payload)


# ----------------------------
# Rolling 5m slug helpers
# ----------------------------
def parse_seed_bucket(seed_slug: str) -> Optional[int]:
    try:
        parts = seed_slug.strip().split("-")
        return int(parts[-1])
    except Exception:
        return None


def make_slug(prefix: str, bucket: int) -> str:
    return f"{prefix}-{bucket}"


def bucket_to_utc_iso(bucket: int) -> str:
    try:
        return dt.datetime.fromtimestamp(bucket, tz=dt.timezone.utc).isoformat()
    except Exception:
        return "n/a"


def resolve_seed_bucket() -> Tuple[str, int]:
    seed_slug = SEED_SLUG.strip()
    if seed_slug:
        b = parse_seed_bucket(seed_slug)
        if b is not None:
            return seed_slug, b

    now = int(time.time())
    seed_bucket = (now // 300) * 300
    seed_slug = make_slug(PREFIX, seed_bucket)
    return seed_slug, seed_bucket


# ----------------------------
# Main
# ----------------------------
def main_once():
    log.info("BOOT: bot.py starting")

    try:
        ensure_tables()
    except Exception as e:
        log.warning(f"DB_INIT_WARNING | {e}")

    bun_code = bun_healthcheck()
    log.info(
        f"run_mode={RUN_MODE} live_mode={LIVE_MODE} live_armed={LIVE_ARMED} "
        f"seed_slug={SEED_SLUG or 'n/a'} prefix={PREFIX} lookback={LOOKBACK} "
        f"MIN_BID={MIN_BID:.3f} MAX_ASK={MAX_ASK:.3f} MAX_SPREAD={MAX_SPREAD:.3f} "
        f"DEBUG_GAMMA={DEBUG_GAMMA} DEBUG_BOOK={DEBUG_BOOK} "
        f"LIVE_SMOKE={LIVE_SMOKE} LIVE_SMOKE_BYPASS={LIVE_SMOKE_BYPASS}"
    )
    log.info(f"bun_health_status={bun_code}")

    seed_slug, seed_bucket = resolve_seed_bucket()
    log.info(f"SEED | seed_slug={seed_slug} | seed_bucket={seed_bucket} | seed_utc={bucket_to_utc_iso(seed_bucket)}")

    counts = {
        "tried": 0,
        "gamma_missing": 0,
        "closed": 0,
        "token_missing": 0,
        "clob_404": 0,
        "book_miss": 0,
        "book_bad": 0,
        "tradable": 0,
        "smoke_bypass_ok": 0,
    }

    for i in range(LOOKBACK):
        bucket = seed_bucket - (i * 300)
        slug = make_slug(PREFIX, bucket)
        counts["tried"] += 1

        log.info(f"TRY_BUCKET | i={i} | bucket={bucket} | utc={bucket_to_utc_iso(bucket)} | slug={slug}")

        code, mkt = gamma_get_market_by_slug(slug)
        if code != 200 or not mkt:
            counts["gamma_missing"] += 1
            log.info(f"GAMMA_MISS | slug={slug} | code={code}")
            continue

        debug_gamma_market(mkt, slug)

        if _safe_get(mkt, "closed") is True:
            counts["closed"] += 1
            log.info(f"SKIP_CLOSED | slug={slug} | bucket={bucket}")
            continue

        outcomes, token_ids = extract_outcomes_and_tokens(mkt)
        if len(outcomes) < 2 or len(token_ids) < 2:
            counts["token_missing"] += 1
            log.info(
                f"TOKEN_MISS | slug={slug} | outcomes={outcomes} token_ids={token_ids} "
                f"| enableOrderBook={_safe_get(mkt,'enableOrderBook')} active={_safe_get(mkt,'active')} closed={_safe_get(mkt,'closed')}"
            )
            continue

        oA, oB = outcomes[0], outcomes[1]
        tA, tB = token_ids[0], token_ids[1]

        codeA, bookA = clob_get_book(tA)
        codeB, bookB = clob_get_book(tB)

        if codeA == 404 or codeB == 404:
            counts["clob_404"] += 1

        if not bookA or not bookB:
            counts["book_miss"] += 1
            log.info(
                f"BOOK_MISS | slug={slug} | A_code={codeA} B_code={codeB} | "
                f"A={'ok' if bookA else 'no_book'} B={'ok' if bookB else 'no_book'}"
            )
            continue

        bbA, baA = best_bid_ask(bookA)
        bbB, baB = best_bid_ask(bookB)

        # Always log that book exists
        log.info(
            f"FOUND_BOOK | slug={slug} | "
            f"{oA} token={tA} bb={bbA} ba={baA} | "
            f"{oB} token={tB} bb={bbB} ba={baB}"
        )

        if DEBUG_BOOK:
            log.info(f"BOOK_TOP | slug={slug} | {oA} bb={bbA} ba={baA} | {oB} bb={bbB} ba={baB}")

        okA, whyA = book_ok(bbA, baA)
        okB, whyB = book_ok(bbB, baB)

        tradable = okA and okB
        bypass_ok = False

        if not tradable:
            # Optional bypass ONLY for LIVE smoke
            if LIVE_MODE and LIVE_ARMED and LIVE_SMOKE and LIVE_SMOKE_BYPASS:
                sA, sWhyA = smoke_ok(bbA, baA)
                sB, sWhyB = smoke_ok(bbB, baB)
                if sA and sB:
                    bypass_ok = True
                    counts["smoke_bypass_ok"] += 1
                    log.info(
                        f"SMOKE_BYPASS_OK | slug={slug} | "
                        f"A bb={bbA} ba={baA} | B bb={bbB} ba={baB} | "
                        f"limits: min_bid={LIVE_SMOKE_BYPASS_MIN_BID} max_ask={LIVE_SMOKE_BYPASS_MAX_ASK}"
                    )
                else:
                    counts["book_bad"] += 1
                    log.info(
                        f"BOOK_BAD | slug={slug} | "
                        f"A_{whyA} bb={bbA} ba={baA} smoke={sWhyA} | "
                        f"B_{whyB} bb={bbB} ba={baB} smoke={sWhyB}"
                    )
                    continue
            else:
                counts["book_bad"] += 1
                log.info(f"BOOK_BAD | slug={slug} | A_{whyA} bb={bbA} ba={baA} | B_{whyB} bb={bbB} ba={baB}")
                continue
        else:
            counts["tradable"] += 1
            log.info(
                f"FOUND_TRADABLE_BOOK | slug={slug} | "
                f"{oA} token={tA} bb={bbA:.4f} ba={baA:.4f} | "
                f"{oB} token={tB} bb={bbB:.4f} ba={baB:.4f}"
            )

        # Persist state (tradable or bypass-ok bucket, both are “last seen good” for different purposes)
        try:
            st = db_read_state()
            st["last_seen"] = {
                "ts": _now_utc_iso(),
                "slug": slug,
                "bucket": bucket,
                "outcomes": [oA, oB],
                "token_ids": [tA, tB],
                "books": {
                    oA: {"best_bid": bbA, "best_ask": baA},
                    oB: {"best_bid": bbB, "best_ask": baB},
                },
                "tradable": bool(tradable),
                "smoke_bypass_ok": bool(bypass_ok),
            }
            db_write_state(st)
        except Exception as e:
            log.warning(f"DB_WRITE_WARNING | {e}")

        # LIVE SMOKE (optional)
        if LIVE_MODE and LIVE_ARMED and LIVE_SMOKE:
            st = db_read_state()
            last_bucket = (st.get("live") or {}).get("last_smoke_bucket")

            if last_bucket == bucket:
                log.info(f"LIVE_SMOKE_SKIP | reason=already_smoked_this_bucket | bucket={bucket} | slug={slug}")
            else:
                if LIVE_SMOKE_SIDE == "B":
                    pick_name, pick_token, pick_ask = oB, tB, baB
                else:
                    pick_name, pick_token, pick_ask = oA, tA, baA

                if pick_ask is None:
                    log.info(f"LIVE_SMOKE_ABORT | reason=missing_ask | outcome={pick_name} token={pick_token}")
                else:
                    price = float(pick_ask)
                    size = float(LIVE_SMOKE_SIZE)

                    if price > 0:
                        max_size_by_cost = LIVE_SMOKE_MAX_COST / price
                        if size > max_size_by_cost:
                            size = max_size_by_cost

                    if size <= 0:
                        log.info(
                            f"LIVE_SMOKE_ABORT | reason=size<=0_after_cap | price={price} max_cost={LIVE_SMOKE_MAX_COST}"
                        )
                    else:
                        log.info(
                            f"LIVE_SMOKE_SEND | slug={slug} | outcome={pick_name} | token={pick_token} "
                            f"| side=BUY price={price:.4f} size={size:.6f} order_type={LIVE_SMOKE_ORDER_TYPE}"
                        )
                        codeO, respO = bun_place_order(
                            token_id=pick_token,
                            side="BUY",
                            price=price,
                            size=size,
                            order_type=LIVE_SMOKE_ORDER_TYPE,
                        )
                        log.info(f"LIVE_SMOKE_RESULT | http_code={codeO} | resp={str(respO)[:1200]}")

                        # idempotency marker
                        st = db_read_state()
                        live_obj = st.get("live") or {}
                        live_obj["last_smoke_bucket"] = bucket
                        live_obj["last_smoke_slug"] = slug
                        live_obj["last_smoke_ts"] = _now_utc_iso()
                        st["live"] = live_obj
                        try:
                            db_write_state(st)
                        except Exception as e:
                            log.warning(f"DB_WRITE_WARNING | {e}")

        # Stop at first bucket that passes either:
        # - real tradable constraints OR
        # - smoke bypass constraints (only matters in LIVE smoke mode)
        break

    log.info(
        "SEARCH_SUMMARY | tried={tried} | gamma_missing={gamma_missing} | closed={closed} | token_missing={token_missing} | "
        "clob_404={clob_404} | book_miss={book_miss} | book_bad={book_bad} | tradable={tradable} | smoke_bypass_ok={smoke_bypass_ok}".format(
            **counts
        )
    )

    if counts["tradable"] == 0:
        log.info("NO_TRADE | reason=NO_TRADABLE_BOOK_FOUND | lookback=%d", LOOKBACK)

    log.info("BOOT: bot.py finished cleanly")


def main():
    if RUN_LOOP:
        log.info(f"RUN_LOOP | enabled=true | loop_seconds={LOOP_SECONDS}")
        while True:
            try:
                main_once()
            except Exception as e:
                log.exception(f"FATAL_LOOP_EXCEPTION | {e}")
            time.sleep(LOOP_SECONDS)
    else:
        main_once()


if __name__ == "__main__":
    main()

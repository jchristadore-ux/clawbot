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

# Rolling 5m market prefix + seed slug
PREFIX = os.getenv("PREFIX", "btc-updown-5m")
SEED_SLUG = os.getenv("SEED_SLUG", "")  # e.g. btc-updown-5m-1772308500
LOOKBACK = int(os.getenv("LOOKBACK", "96"))  # how many 5-min buckets to try

# Market quality constraints (same defaults you show)
MIN_BID = float(os.getenv("MIN_BID", "0.020"))
MAX_ASK = float(os.getenv("MAX_ASK", "0.980"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.080"))

# Phase 4 only behavior flag (kept for parity)
PHASE4_ONLY = os.getenv("PHASE4_ONLY", "true").lower() in ("1", "true", "yes", "y")

# DB state table (fixes the earlier key=null issue)
DB_STATE_TABLE = os.getenv("DB_STATE_TABLE", "j5_state")
DB_KEY = os.getenv("DB_KEY", PREFIX)  # always non-null

# Endpoints
BUN_HEALTH_URL = os.getenv("BUN_HEALTH_URL", "http://localhost:3000/health")
GAMMA_BASE = os.getenv("GAMMA_BASE", "https://gamma-api.polymarket.com")
CLOB_BASE = os.getenv("CLOB_BASE", "https://clob.polymarket.com")

# Request tuning
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "2"))
RETRY_SLEEP = float(os.getenv("RETRY_SLEEP", "0.35"))


# ----------------------------
# Helpers
# ----------------------------
def _now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _maybe_json(x: Any) -> Any:
    """
    Gamma sometimes returns fields (outcomes, clobTokenIds, outcomePrices) as JSON-encoded strings.
    This converts:
      - '["Up","Down"]' -> ["Up","Down"]
      - '["0x..","0x.."]' -> ["0x..","0x.."]
    Otherwise returns x unchanged.
    """
    if isinstance(x, str):
        s = x.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                return json.loads(s)
            except Exception:
                return x
    return x


def _http_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> Tuple[int, Any]:
    last_exc = None
    for _ in range(max(1, HTTP_RETRIES + 1)):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
            ct = (r.headers.get("content-type") or "").lower()
            if "application/json" in ct:
                return r.status_code, r.json()
            # sometimes endpoints return json with wrong content-type
            try:
                return r.status_code, r.json()
            except Exception:
                return r.status_code, r.text
        except Exception as e:
            last_exc = e
            time.sleep(RETRY_SLEEP)
    raise RuntimeError(f"HTTP GET failed after retries: {url} params={params} exc={last_exc}")


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
    """
    Creates a simple key/value state table. Guarantees non-null key usage to avoid:
      psycopg2.errors.NotNullViolation: null value in column "key"
    """
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
            # Ensure a row exists for our DB_KEY (never null)
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
    """
    Gamma has multiple shapes depending on endpoint.
    We'll try a couple robustly and normalize.
    """
    # 1) /markets?slug=
    url1 = f"{GAMMA_BASE}/markets"
    code, data = _http_get(url1, params={"slug": slug})
    if code == 200:
        if isinstance(data, list) and data:
            return code, data[0]
        if isinstance(data, dict) and data.get("markets") and isinstance(data["markets"], list) and data["markets"]:
            return code, data["markets"][0]
        if isinstance(data, dict) and data.get("slug") == slug:
            return code, data

    # 2) /markets/{slug} (some deployments support this)
    url2 = f"{GAMMA_BASE}/markets/{slug}"
    code2, data2 = _http_get(url2)
    if code2 == 200 and isinstance(data2, dict):
        return code2, data2

    return code if isinstance(code, int) else 0, None


def extract_outcomes_and_tokens(mkt: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    Returns (outcomes, clobTokenIds) as lists.
    Handles Gamma returning these as JSON strings.
    """
    outcomes = _maybe_json(mkt.get("outcomes"))
    token_ids = _maybe_json(mkt.get("clobTokenIds"))

    if isinstance(outcomes, dict) and "outcomes" in outcomes:
        outcomes = outcomes["outcomes"]
    if isinstance(token_ids, dict) and "clobTokenIds" in token_ids:
        token_ids = token_ids["clobTokenIds"]

    if not isinstance(outcomes, list) or not all(isinstance(x, str) for x in outcomes):
        outcomes = []
    if not isinstance(token_ids, list) or not all(isinstance(x, str) for x in token_ids):
        token_ids = []

    return outcomes, token_ids


# ----------------------------
# CLOB orderbook (FIXED ENDPOINT)
# ----------------------------
def clob_get_book(token_id: str) -> Tuple[int, Optional[Dict[str, Any]]]:
    """
    Correct Polymarket endpoint:
      GET https://clob.polymarket.com/book?token_id=...
    """
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


# ----------------------------
# Rolling 5m slug search
# ----------------------------
def parse_seed_bucket(seed_slug: str) -> Optional[int]:
    """
    For btc-updown-5m-1772308500 => 1772308500
    """
    try:
        parts = seed_slug.strip().split("-")
        last = parts[-1]
        return int(last)
    except Exception:
        return None


def make_slug(prefix: str, bucket: int) -> str:
    return f"{prefix}-{bucket}"


def bucket_to_utc_iso(bucket: int) -> str:
    try:
        return dt.datetime.fromtimestamp(bucket, tz=dt.timezone.utc).isoformat()
    except Exception:
        return "n/a"


# ----------------------------
# Main
# ----------------------------
def main():
    log.info("BOOT: bot.py starting")

    # DB setup (safe)
    try:
        ensure_tables()
    except Exception as e:
        # If DB misconfigured, we still keep running in a stateless mode
        log.warning(f"DB_INIT_WARNING | {e}")

    bun_code = bun_healthcheck()
    log.info(f"run_mode={RUN_MODE} live_mode={LIVE_MODE} live_armed={LIVE_ARMED} "
             f"seed_slug={SEED_SLUG or 'n/a'} prefix={PREFIX} lookback={LOOKBACK} "
             f"MIN_BID={MIN_BID:.3f} MAX_ASK={MAX_ASK:.3f} MAX_SPREAD={MAX_SPREAD:.3f} "
             f"PHASE4_ONLY={PHASE4_ONLY} db_state_table={DB_STATE_TABLE}")
    log.info(f"bun_health_status={bun_code}")

    seed_slug = SEED_SLUG.strip() or ""
    if not seed_slug:
        # If no seed, just derive from current time bucket (rounded down to 5m)
        now = int(time.time())
        seed_bucket = (now // 300) * 300
        seed_slug = make_slug(PREFIX, seed_bucket)
    else:
        seed_bucket = parse_seed_bucket(seed_slug)
        if seed_bucket is None:
            # fall back to now
            now = int(time.time())
            seed_bucket = (now // 300) * 300
            seed_slug = make_slug(PREFIX, seed_bucket)

    log.info(f"SEED | seed_slug={seed_slug} | prefix={PREFIX} | seed_bucket={seed_bucket} | seed_utc={bucket_to_utc_iso(seed_bucket)}")

    counts = {
        "tried": 0,
        "gamma_missing": 0,
        "token_missing": 0,
        "clob_404": 0,
        "book_bad": 0,
        "ok": 0,
    }

    for i in range(LOOKBACK):
        bucket = seed_bucket - (i * 300)
        slug = make_slug(PREFIX, bucket)
        counts["tried"] += 1

        log.info(f"TRY_BUCKET | i={i} | bucket={bucket} | utc={bucket_to_utc_iso(bucket)} | slug={slug}")

        code, mkt = gamma_get_market_by_slug(slug)
        if code != 200 or not mkt:
            counts["gamma_missing"] += 1
            log.info(f"GAMMA_MISS | slug={slug} | slug_endpoint code={code}")
            continue

        outcomes, token_ids = extract_outcomes_and_tokens(mkt)

        if len(outcomes) < 2 or len(token_ids) < 2:
            counts["token_missing"] += 1
            keys = list(mkt.keys())
            fields = {
                "slug": type(mkt.get("slug")).__name__,
                "outcomes": type(mkt.get("outcomes")).__name__,
                "clobTokenIds": type(mkt.get("clobTokenIds")).__name__,
            }
            log.info(
                f"TOKEN_DEBUG | slug={slug} | slug_endpoint code={code} | token_extract_failed | "
                f"keys={keys[:40]}... fields={fields}"
            )
            log.info(f"GAMMA_MISS | slug={slug} | slug_endpoint code={code} | token_extract_failed")
            continue

        # For binary markets: token_ids[0] corresponds to outcomes[0], etc.
        # We'll fetch books for the first two outcomes.
        oA, oB = outcomes[0], outcomes[1]
        tA, tB = token_ids[0], token_ids[1]

        codeA, bookA = clob_get_book(tA)
        codeB, bookB = clob_get_book(tB)

        if codeA == 404 or codeB == 404:
            counts["clob_404"] += 1

        if not bookA or not bookB:
            # Mirror your existing "no_book(code=404)" style message
            a_msg = f"no_book(code={codeA})" if not bookA else "ok"
            b_msg = f"no_book(code={codeB})" if not bookB else "ok"
            log.info(
                f"Skipping slug={slug} ({oA} vs {oB}) due to book quality: "
                f"BOOK_BAD(A_{'NO_BOOK_'+str(codeA) if not bookA else 'OK'},B_{'NO_BOOK_'+str(codeB) if not bookB else 'OK'}) | "
                f"A {a_msg} | B {b_msg}"
            )
            counts["book_bad"] += 1
            continue

        bbA, baA = best_bid_ask(bookA)
        bbB, baB = best_bid_ask(bookB)

        okA, whyA = book_ok(bbA, baA)
        okB, whyB = book_ok(bbB, baB)

        if not okA or not okB:
            counts["book_bad"] += 1
            log.info(
                f"Skipping slug={slug} ({oA} vs {oB}) due to book quality: "
                f"BOOK_BAD(A_{whyA},B_{whyB}) | "
                f"A bb={bbA} ba={baA} | B bb={bbB} ba={baB}"
            )
            continue

        counts["ok"] += 1
        log.info(
            f"FOUND_REAL_BOOK | slug={slug} | "
            f"{oA} token={tA} bb={bbA:.4f} ba={baA:.4f} | "
            f"{oB} token={tB} bb={bbB:.4f} ba={baB:.4f}"
        )

        # Save last-good in DB state
        st = db_read_state()
        st["last_good"] = {
            "ts": _now_utc_iso(),
            "slug": slug,
            "bucket": bucket,
            "outcomes": [oA, oB],
            "token_ids": [tA, tB],
            "books": {
                oA: {"best_bid": bbA, "best_ask": baA},
                oB: {"best_bid": bbB, "best_ask": baB},
            },
        }
        try:
            db_write_state(st)
        except Exception as e:
            log.warning(f"DB_WRITE_WARNING | {e}")

        # Stop at first good bucket (matches your previous behavior)
        break

    log.info(
        "SEARCH_SUMMARY | tried={tried} | gamma_missing={gamma_missing} | token_missing={token_missing} | "
        "clob_404={clob_404} | book_bad={book_bad} | ok={ok}".format(**counts)
    )

    if counts["ok"] == 0:
        log.info("NO_TRADE | reason=NO_REAL_BOOK_FOUND_OR_TOKEN_PARSE_FAIL | lookback=%d", LOOKBACK)

    log.info("BOOT: bot.py finished cleanly")


if __name__ == "__main__":
    main()

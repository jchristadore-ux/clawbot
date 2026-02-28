import os
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


# ----------------------------
# Time helpers
# ----------------------------
def iso_utc_from_epoch(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


# ----------------------------
# HTTP helpers
# ----------------------------
def http_get(url: str, params: Optional[dict] = None, timeout: int = 20) -> Tuple[Optional[Any], Optional[int], str]:
    try:
        r = requests.get(url, params=params, timeout=timeout)
        txt = (r.text or "")[:400]
        if r.status_code != 200:
            return None, r.status_code, txt
        try:
            return r.json(), 200, txt
        except Exception:
            return None, r.status_code, txt
    except Exception as e:
        return None, None, str(e)


def _maybe_json(x):
    """
    Gamma sometimes returns arrays as JSON-encoded strings.
    Converts:
      - '["Up","Down"]' -> ["Up","Down"]
      - ["Up","Down"] -> ["Up","Down"]
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

POLY_SEED_SLUG = os.getenv("POLY_SEED_SLUG", "btc-updown-5m-1772308500").strip()

MIN_BID = env_float("MIN_BID", 0.02)
MAX_ASK = env_float("MAX_ASK", 0.98)
MAX_SPREAD = env_float("MAX_SPREAD", 0.08)
LOOKBACK_BUCKETS = env_int("LOOKBACK_BUCKETS", 96)
PHASE4_ONLY = env_bool("PHASE4_ONLY", True)

TOKEN_DEBUG_SAMPLES = env_int("TOKEN_DEBUG_SAMPLES", 5)


def seed_prefix_from_slug(seed_slug: str) -> str:
    parts = seed_slug.split("-")
    if len(parts) < 2:
        raise RuntimeError(f"Bad POLY_SEED_SLUG: {seed_slug}")
    return "-".join(parts[:-1])


def seed_bucket_from_slug(seed_slug: str) -> int:
    tail = seed_slug.split("-")[-1]
    if not tail.isdigit():
        raise RuntimeError(f"Seed slug does not end with numeric bucket: {seed_slug}")
    b = int(tail)
    if b < 1_600_000_000 or b > 2_000_000_000:
        raise RuntimeError(f"Seed bucket looks wrong (not epoch seconds): {b} from {seed_slug}")
    return b


def gamma_get_market_by_slug(full_slug: str) -> Tuple[Optional[dict], str]:
    attempts: List[str] = []

    url1 = f"{POLY_GAMMA_HOST}/markets/slug/{full_slug}"
    j, code, _ = http_get(url1, timeout=20)
    attempts.append(f"slug_endpoint code={code}")
    if isinstance(j, dict):
        return j, " | ".join(attempts)

    url2 = f"{POLY_GAMMA_HOST}/markets"
    j, code, _ = http_get(url2, params={"slug": full_slug, "limit": 1}, timeout=20)
    attempts.append(f"markets?slug code={code}")
    if isinstance(j, list) and j and isinstance(j[0], dict):
        return j[0], " | ".join(attempts)
    if isinstance(j, dict):
        mkts = j.get("markets")
        if isinstance(mkts, list) and mkts and isinstance(mkts[0], dict):
            return mkts[0], " | ".join(attempts)

    j, code, _ = http_get(url2, params={"search": full_slug, "limit": 5}, timeout=20)
    attempts.append(f"markets?search code={code}")
    if isinstance(j, list):
        for m in j:
            if isinstance(m, dict) and m.get("slug") == full_slug:
                return m, " | ".join(attempts)
    if isinstance(j, dict):
        mkts = j.get("markets")
        if isinstance(mkts, list):
            for m in mkts:
                if isinstance(m, dict) and m.get("slug") == full_slug:
                    return m, " | ".join(attempts)

    return None, " | ".join(attempts)


def extract_two_outcomes_and_token_ids(market: dict) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], str]:
    """
    We do NOT assume 'YES/NO'. We assume a 2-outcome market and take outcome/token by index.
    Returns: (token_a, token_b, outcome_a, outcome_b, method_or_reason)
    """

    # Shape A: outcomes + clobTokenIds (often JSON strings)
    for outcomes_key in ("outcomes", "Outcomes"):
        for token_key in ("clobTokenIds", "clob_token_ids", "clobTokenIDs", "clob_tokenIds", "tokenIds", "token_ids"):
            outcomes = _maybe_json(market.get(outcomes_key))
            token_ids = _maybe_json(market.get(token_key))
            if isinstance(outcomes, list) and isinstance(token_ids, list) and len(outcomes) >= 2 and len(token_ids) >= 2:
                # Align by index (Gamma commonly keeps them aligned)
                o0 = outcomes[0] if isinstance(outcomes[0], str) else str(outcomes[0])
                o1 = outcomes[1] if isinstance(outcomes[1], str) else str(outcomes[1])
                t0 = str(token_ids[0])
                t1 = str(token_ids[1])
                return t0, t1, o0, o1, f"shapeA({outcomes_key}+{token_key})"

    # Shape B: tokens array
    tokens = market.get("tokens") or market.get("Tokens")
    if isinstance(tokens, str):
        tokens = _maybe_json(tokens)
    if isinstance(tokens, list) and len(tokens) >= 2:
        def get_tid(t: dict) -> Optional[str]:
            tid = (
                t.get("token_id")
                or t.get("tokenId")
                or t.get("clobTokenId")
                or t.get("clob_token_id")
                or t.get("clobTokenID")
            )
            return str(tid) if tid is not None else None

        def get_outcome(t: dict) -> str:
            outcome = t.get("outcome") or t.get("name") or t.get("label")
            return outcome if isinstance(outcome, str) else str(outcome)

        if isinstance(tokens[0], dict) and isinstance(tokens[1], dict):
            t0 = get_tid(tokens[0])
            t1 = get_tid(tokens[1])
            if t0 and t1:
                o0 = get_outcome(tokens[0])
                o1 = get_outcome(tokens[1])
                return t0, t1, o0, o1, "shapeB(tokens[])"

    # Shape C: nested {"market": {...}}
    nested = market.get("market")
    if isinstance(nested, dict) and nested is not market:
        t0, t1, o0, o1, why = extract_two_outcomes_and_token_ids(nested)
        if t0 and t1:
            return t0, t1, o0, o1, f"shapeC(nested_market->{why})"

    return None, None, None, None, "token_extract_failed"


def market_schema_snapshot(market: dict) -> str:
    keys = sorted(list(market.keys()))

    def tname(v):
        return type(v).__name__

    fields = {}
    for k in ("slug", "outcomes", "clobTokenIds", "clob_token_ids", "tokenIds", "tokens"):
        if k in market:
            fields[k] = tname(market.get(k))

    # Also include small preview if possible (not full payload)
    out_preview = None
    tok_preview = None
    try:
        outs = _maybe_json(market.get("outcomes"))
        toks = _maybe_json(market.get("clobTokenIds"))
        if isinstance(outs, list):
            out_preview = outs[:4]
        if isinstance(toks, list):
            tok_preview = toks[:4]
    except Exception:
        pass

    return f"keys={keys[:40]}{'...' if len(keys)>40 else ''} fields={fields} outcomes_preview={out_preview} clobTokenIds_preview={tok_preview}"


def fetch_gamma_market_and_tokens_for_slug(full_slug: str) -> Tuple[Optional[Dict[str, Any]], str]:
    market, dbg = gamma_get_market_by_slug(full_slug)
    if not market:
        return None, dbg

    t0, t1, o0, o1, how = extract_two_outcomes_and_token_ids(market)
    if not t0 or not t1:
        return None, dbg + " | " + how

    return {
        "market": market,
        "token_a": t0,
        "token_b": t1,
        "outcome_a": o0 or "A",
        "outcome_b": o1 or "B",
    }, dbg + " | " + how


# ----------------------------
# CLOB book
# ----------------------------
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
# Phase 4 gates
# ----------------------------
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
    token_a: str
    token_b: str
    outcome_a: str
    outcome_b: str
    top_a: BookTop
    top_b: BookTop


def find_tradable_slug_with_real_book() -> Tuple[Optional[TradableSelection], Dict[str, int]]:
    counters = {
        "tried": 0,
        "gamma_missing": 0,
        "token_missing": 0,
        "token_debug_logged": 0,
        "clob_404": 0,
        "book_missing_side": 0,
        "book_bad": 0,
        "ok": 0,
    }

    seed_slug = POLY_SEED_SLUG
    prefix = seed_prefix_from_slug(seed_slug)
    seed_bucket = seed_bucket_from_slug(seed_slug)

    log.info(
        "SEED | seed_slug=%s | prefix=%s | seed_bucket=%d | seed_utc=%s",
        seed_slug,
        prefix,
        seed_bucket,
        iso_utc_from_epoch(seed_bucket),
    )

    for i in range(LOOKBACK_BUCKETS):
        bucket = seed_bucket - i * 300
        full_slug = f"{prefix}-{bucket}"
        counters["tried"] += 1

        log.info("TRY_BUCKET | i=%d | bucket=%d | utc=%s | slug=%s", i, bucket, iso_utc_from_epoch(bucket), full_slug)

        gamma, dbg = fetch_gamma_market_and_tokens_for_slug(full_slug)
        if not gamma:
            if "token_extract_failed" in dbg:
                counters["token_missing"] += 1
                if counters["token_debug_logged"] < TOKEN_DEBUG_SAMPLES:
                    counters["token_debug_logged"] += 1
                    mkt, _ = gamma_get_market_by_slug(full_slug)
                    if isinstance(mkt, dict):
                        log.info("TOKEN_DEBUG | slug=%s | %s | %s", full_slug, dbg, market_schema_snapshot(mkt))
                    else:
                        log.info("TOKEN_DEBUG | slug=%s | %s | market_unavailable_for_snapshot", full_slug, dbg)
            else:
                counters["gamma_missing"] += 1

            if i < 3:
                log.info("GAMMA_MISS | slug=%s | %s", full_slug, dbg)
            continue

        token_a = gamma["token_a"]
        token_b = gamma["token_b"]
        outcome_a = gamma.get("outcome_a") or "A"
        outcome_b = gamma.get("outcome_b") or "B"

        top_a, status_a = fetch_clob_book_top(token_a)
        top_b, status_b = fetch_clob_book_top(token_b)

        if status_a == 404 or status_b == 404:
            counters["clob_404"] += 1

        reasons = []
        reasons += book_quality_reasons("A", top_a, status_a)
        reasons += book_quality_reasons("B", top_b, status_b)

        if reasons:
            if any("MISSING_BID_OR_ASK" in r for r in reasons):
                counters["book_missing_side"] += 1
            else:
                counters["book_bad"] += 1

            a_desc = f"{top_a}" if top_a else f"no_book(code={status_a})"
            b_desc = f"{top_b}" if top_b else f"no_book(code={status_b})"
            log.info(
                "Skipping slug=%s (%s vs %s) due to book quality: BOOK_BAD(%s) | A %s | B %s",
                full_slug,
                outcome_a,
                outcome_b,
                ",".join(reasons),
                a_desc,
                b_desc,
            )
            continue

        counters["ok"] += 1
        sel = TradableSelection(
            slug=full_slug,
            bucket=bucket,
            token_a=str(token_a),
            token_b=str(token_b),
            outcome_a=str(outcome_a),
            outcome_b=str(outcome_b),
            top_a=top_a,
            top_b=top_b,
        )
        log.info(
            "PHASE4_OK | Selected tradable slug with real book: %s | outcomes=(%s,%s) | A %s | B %s",
            sel.slug,
            sel.outcome_a,
            sel.outcome_b,
            sel.top_a,
            sel.top_b,
        )
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
        "run_mode=%s live_mode=%s live_armed=%s seed_slug=%s lookback=%d MIN_BID=%.3f MAX_ASK=%.3f MAX_SPREAD=%.3f PHASE4_ONLY=%s db_state_table=%s",
        run_mode,
        live_mode,
        live_armed,
        POLY_SEED_SLUG,
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

    # Daily reset
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
        log.info("NO_TRADE | reason=NO_REAL_BOOK_FOUND_OR_TOKEN_PARSE_FAIL | lookback=%d", LOOKBACK_BUCKETS)
        log.info("BOOT: bot.py finished cleanly")
        return

    if PHASE4_ONLY:
        log.info(
            "PHASE4_ONLY | slug=%s | outcomes=(%s,%s) | next_step=RE_ENABLE_PAPER_TRADING_LOGIC_WHEN_READY",
            sel.slug,
            sel.outcome_a,
            sel.outcome_b,
        )
        log.info("BOOT: bot.py finished cleanly")
        return

    log.info("PHASE4_DONE | slug=%s | (trading logic disabled in this phase)", sel.slug)
    log.info("BOOT: bot.py finished cleanly")


if __name__ == "__main__":
    main()

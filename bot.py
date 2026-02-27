#!/usr/bin/env python3
"""
Johnny 5 - Polymarket bot (direct trading + auto market selection)

Key upgrade:
- If the provided rolling 5m slug resolves to tokens with NO orderbook (404),
  the bot auto-searches Gamma for a tradable market whose slug contains POLY_PREFIX,
  validates books exist for BOTH sides, and trades that instead.

This is designed to restore "hands-free every 5 minutes" behavior even when some
rolling windows have no CLOB orderbook.

Trading:
- Uses py-clob-client to sign & post orders directly (Option A).
- Orders are ONLY placed when LIVE_ARMED=true.

Docs:
- Trading/auth flow: use SDK clients; derive API creds then trade. 
- py-clob-client repo/interface. 
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

# ---- Execution client (official) ----
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY


# -----------------------------
# Config
# -----------------------------

GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com").rstrip("/")

CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))  # Polygon mainnet (typical)

RUN_MODE = os.getenv("RUN_MODE", "LIVE").upper()  # LIVE or PAPER or TEST
LIVE_MODE = os.getenv("LIVE_MODE", "true").lower() == "true"
LIVE_ARMED = os.getenv("LIVE_ARMED", "false").lower() == "true"

POLY_SLUG = os.getenv("POLY_SLUG", "").strip()
POLY_PREFIX = os.getenv("POLY_PREFIX", "").strip()  # e.g. btc-updown-5m

RUN_LOOP = os.getenv("RUN_LOOP", "false").lower() == "true"
LOOP_SLEEP_S = int(os.getenv("LOOP_SLEEP_S", "300"))  # 5 minutes

HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "10"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
HTTP_RETRY_SLEEP_S = float(os.getenv("HTTP_RETRY_SLEEP_S", "0.6"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# ---- Trading config ----
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "").strip()
POLY_API_KEY = os.getenv("POLY_API_KEY", "").strip()
POLY_API_SECRET = os.getenv("POLY_API_SECRET", "").strip()
POLY_API_PASSPHRASE = os.getenv("POLY_API_PASSPHRASE", "").strip()

SIGNATURE_TYPE = int(os.getenv("POLY_SIGNATURE_TYPE", "0"))
FUNDER_ADDRESS = os.getenv("POLY_FUNDER_ADDRESS", "").strip()

ORDER_SIZE_SHARES = float(os.getenv("LIVE_ORDER_SIZE_SHARES", "1.0"))
TICK_SIZE = float(os.getenv("TICK_SIZE", "0.01"))

EDGE_MIN = float(os.getenv("EDGE_MIN", "0.00"))

# How many Gamma candidates to scan when hunting for a tradable market
GAMMA_SCAN_LIMIT = int(os.getenv("GAMMA_SCAN_LIMIT", "50"))


# -----------------------------
# Logging
# -----------------------------

logger = logging.getLogger("johnny5")
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.handlers = []
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)


# -----------------------------
# HTTP helpers (public endpoints)
# -----------------------------

def http_get_json(url: str, params: Optional[Any] = None) -> Optional[Any]:
    last_err = None
    for attempt in range(1, max(1, HTTP_RETRIES) + 1):
        try:
            r = requests.get(url, params=params, timeout=HTTP_TIMEOUT_S)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    logger.warning("HTTP 200 but JSON decode failed: url=%s params=%s body=%r", url, params, r.text[:200])
                    return None

            logger.warning("HTTP non-200: url=%s params=%s status=%s body=%r", url, params, r.status_code, r.text[:200])
            return None
        except Exception as e:
            last_err = e
            logger.warning("HTTP exception: url=%s params=%s attempt=%s err=%s", url, params, attempt, e)
            time.sleep(HTTP_RETRY_SLEEP_S)

    logger.warning("HTTP GET failed after retries: url=%s params=%s last_err=%s", url, params, last_err)
    return None


# -----------------------------
# Models
# -----------------------------

@dataclass
class ResolvedMarket:
    slug: str
    market_id: Optional[str]
    outcomes: List[str]
    token_ids: List[str]
    yes_token: Optional[str]
    no_token: Optional[str]


# -----------------------------
# Gamma parsing
# -----------------------------

def _maybe_json(x: Any) -> Any:
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


def extract_yes_no_token_ids(market: dict) -> Tuple[Optional[str], Optional[str], List[str], List[str]]:
    outcomes = _maybe_json(market.get("outcomes"))
    token_ids = _maybe_json(market.get("clobTokenIds"))

    if not isinstance(outcomes, list) or not isinstance(token_ids, list):
        return None, None, [], []
    if len(outcomes) != len(token_ids) or len(outcomes) < 2:
        return None, None, [], []

    outcomes_str = [str(o) for o in outcomes]
    token_ids_str = [str(t) for t in token_ids]
    norm = [o.strip().upper() for o in outcomes_str]

    yes_id = None
    no_id = None
    for o, tid in zip(norm, token_ids_str):
        if o in ("YES", "UP"):
            yes_id = tid
        elif o in ("NO", "DOWN"):
            no_id = tid

    if (yes_id is None or no_id is None) and len(token_ids_str) >= 2:
        if norm[0] in ("YES", "UP") and norm[1] in ("NO", "DOWN"):
            yes_id, no_id = token_ids_str[0], token_ids_str[1]
        elif norm[0] in ("NO", "DOWN") and norm[1] in ("YES", "UP"):
            yes_id, no_id = token_ids_str[1], token_ids_str[0]

    return yes_id, no_id, outcomes_str, token_ids_str


def fetch_market_by_slug(slug: str) -> Optional[dict]:
    j = http_get_json(f"{GAMMA_HOST}/markets", params={"slug": slug})
    if isinstance(j, list) and j and isinstance(j[0], dict):
        return j[0]
    if isinstance(j, dict) and j:
        return j

    ej = http_get_json(f"{GAMMA_HOST}/events", params={"slug": slug})
    if isinstance(ej, list) and ej and isinstance(ej[0], dict):
        ev = ej[0]
        markets = ev.get("markets")
        if isinstance(markets, list) and markets and isinstance(markets[0], dict):
            for m in markets:
                if isinstance(m, dict) and str(m.get("slug", "")).strip() == slug:
                    return m
            return markets[0]
    return None


def resolve_market(slug: str) -> ResolvedMarket:
    m = fetch_market_by_slug(slug)
    if not m:
        raise RuntimeError(f"Gamma market resolution failed for slug='{slug}'")

    yes_token, no_token, outcomes, token_ids = extract_yes_no_token_ids(m)

    market_id = None
    if "id" in m:
        market_id = str(m.get("id"))
    elif "marketId" in m:
        market_id = str(m.get("marketId"))

    return ResolvedMarket(
        slug=slug,
        market_id=market_id,
        outcomes=outcomes,
        token_ids=token_ids,
        yes_token=yes_token,
        no_token=no_token,
    )


# -----------------------------
# CLOB book / price helpers
# -----------------------------

def _parse_price_levels(levels: Any) -> List[float]:
    out: List[float] = []
    if not isinstance(levels, list):
        return out
    for lvl in levels:
        p = None
        if isinstance(lvl, dict):
            p = lvl.get("price")
        elif isinstance(lvl, (list, tuple)) and len(lvl) >= 1:
            p = lvl[0]
        if p is None:
            continue
        try:
            out.append(float(p))
        except Exception:
            continue
    return out


def clob_book_best(token_id: str) -> Tuple[Optional[float], Optional[float], bool]:
    """
    Returns (best_bid, best_ask, has_book)
    has_book==False if CLOB returned 404/no orderbook.
    """
    b = http_get_json(f"{CLOB_HOST}/book", params={"token_id": token_id})
    if not isinstance(b, dict):
        return None, None, False

    bids = _parse_price_levels(b.get("bids") or [])
    asks = _parse_price_levels(b.get("asks") or [])
    best_bid = max(bids) if bids else None
    best_ask = min(asks) if asks else None
    return best_bid, best_ask, True


def midpoint_from_best(best_bid: Optional[float], best_ask: Optional[float]) -> Optional[float]:
    if best_bid is None or best_ask is None:
        return None
    return (best_bid + best_ask) / 2.0


# -----------------------------
# Market selection: ensure tradable
# -----------------------------

def gamma_public_search(q: str, limit: int) -> List[dict]:
    """
    Gamma /public-search expects query parameter name `q` (not `query`) in your environment.
    """
    j = http_get_json(f"{GAMMA_HOST}/public-search", params={"q": q, "limit": limit})
    if not isinstance(j, dict):
        return []

    markets: List[dict] = []

    # Defensive extraction (Gamma payloads can vary)
    for key in ("markets", "results", "data"):
        block = j.get(key)
        if isinstance(block, list):
            for item in block:
                if not isinstance(item, dict):
                    continue
                # market object directly
                if "clobTokenIds" in item and "outcomes" in item:
                    markets.append(item)
                # wrapper object
                inner = item.get("market")
                if isinstance(inner, dict) and "clobTokenIds" in inner and "outcomes" in inner:
                    markets.append(inner)

    # Dedup by slug
    seen = set()
    deduped = []
    for m in markets:
        s = str(m.get("slug", "")).strip()
        if s and s not in seen:
            seen.add(s)
            deduped.append(m)

    return deduped

def gamma_search_markets_by_prefix(prefix: str, limit: int) -> List[dict]:
    """
    Search strategy:
    - First: prefix-derived query (btc updown 5m)
    - Then: broader bitcoin query
    """
    q1 = prefix.replace("-", " ").strip()
    markets = gamma_public_search(q1, limit)

    if not markets:
        markets = gamma_public_search("bitcoin", limit)

    # Filter by prefix if it actually appears in slug; otherwise return whatever we found.
    filtered = []
    for m in markets:
        s = str(m.get("slug", "")).strip()
        if prefix in s:
            filtered.append(m)

    return filtered if filtered else markets

def market_has_books(yes_token: str, no_token: str) -> bool:
    _, _, yes_has = clob_book_best(yes_token)
    _, _, no_has = clob_book_best(no_token)
    return yes_has and no_has

def resolve_tradable_market(slug: str, prefix: str) -> ResolvedMarket:
    """
    1) Resolve exact slug
    2) If not CLOB-enabled or no book, search for a tradable BTC market using /public-search
    """
    rm = resolve_market(slug)

    # Log enableOrderBook if present (key per docs) :contentReference[oaicite:4]{index=4}
    try:
        m = fetch_market_by_slug(slug)
        if isinstance(m, dict):
            logger.info("gamma.enableOrderBook=%s", m.get("enableOrderBook"))
    except Exception:
        pass

    if rm.yes_token and rm.no_token:
        if market_has_books(rm.yes_token, rm.no_token):
            logger.info("Market is tradable as-is (books exist).")
            return rm
        logger.warning("Resolved slug but tokens have no orderbooks; searching for a tradable BTC market…")

    # Hunt via /public-search (better than paging latest markets)
    for scan_limit in (50, 200, 500):
        candidates = gamma_search_markets_by_prefix(prefix, scan_limit)
        logger.info("Search candidate count (prefix=%s limit=%s): %s", prefix, scan_limit, len(candidates))

        for m in candidates:
            try:
                # Skip markets that are explicitly not orderbook-enabled
                # (Docs: markets can be traded via CLOB if enableOrderBook is true.) :contentReference[oaicite:5]{index=5}
                if m.get("enableOrderBook") is False:
                    continue

                s = str(m.get("slug", "")).strip()
                yes_token, no_token, outcomes, token_ids = extract_yes_no_token_ids(m)
                if not yes_token or not no_token:
                    continue

                if not market_has_books(yes_token, no_token):
                    continue

                market_id = None
                if "id" in m:
                    market_id = str(m.get("id"))
                elif "marketId" in m:
                    market_id = str(m.get("marketId"))

                chosen = ResolvedMarket(
                    slug=s,
                    market_id=market_id,
                    outcomes=outcomes,
                    token_ids=token_ids,
                    yes_token=yes_token,
                    no_token=no_token,
                )
                logger.info("Selected tradable market slug=%s market_id=%s enableOrderBook=%s", chosen.slug, chosen.market_id, m.get("enableOrderBook"))
                return chosen
            except Exception:
                continue

    raise RuntimeError(
        "Could not find ANY BTC market with a live CLOB orderbook via /public-search. "
        "This usually means your target slugs are synthetic/non-frontend, or the markets aren’t CLOB-enabled."
    )
# -----------------------------
# Trading client init (py-clob-client)
# -----------------------------

def build_clob_client() -> ClobClient:
    if not POLY_PRIVATE_KEY:
        raise RuntimeError("Missing POLY_PRIVATE_KEY env var (required for direct trading).")

    creds: Optional[ApiCreds] = None
    if POLY_API_KEY and POLY_API_SECRET and POLY_API_PASSPHRASE:
        creds = ApiCreds(api_key=POLY_API_KEY, api_secret=POLY_API_SECRET, api_passphrase=POLY_API_PASSPHRASE)

    client = ClobClient(
        CLOB_HOST,
        CHAIN_ID,
        POLY_PRIVATE_KEY,
        creds,
        SIGNATURE_TYPE,
        FUNDER_ADDRESS if FUNDER_ADDRESS else None,
    )

    if creds is None:
        derived = client.create_or_derive_api_creds()
        logger.info("Derived API creds (store as env vars): apiKey=%s passphrase=%s", derived.api_key, derived.api_passphrase)
        client = ClobClient(
            CLOB_HOST,
            CHAIN_ID,
            POLY_PRIVATE_KEY,
            derived,
            SIGNATURE_TYPE,
            FUNDER_ADDRESS if FUNDER_ADDRESS else None,
        )

    return client


# -----------------------------
# Edge placeholder
# -----------------------------

def compute_signal(p_yes: float, p_no: float) -> str:
    if not (0.0 < p_yes < 1.0 and 0.0 < p_no < 1.0):
        return "NO"
    s = p_yes + p_no
    if s < 0.90 or s > 1.10:
        return "NO"
    if p_yes < 0.48 - EDGE_MIN:
        return "BUY_YES"
    if p_no < 0.48 - EDGE_MIN:
        return "BUY_NO"
    return "NO"


def round_to_tick(px: float) -> float:
    if TICK_SIZE <= 0:
        return px
    return round(px / TICK_SIZE) * TICK_SIZE


def compute_current_5m_slug(prefix: str) -> str:
    now = int(time.time())
    end_ts = ((now // 300) * 300) + 300
    return f"{prefix}-{end_ts}"


# -----------------------------
# Execute one cycle
# -----------------------------

def run_once(slug: str, prefix: str, client: Optional[ClobClient]) -> None:
    rm = resolve_tradable_market(slug, prefix)

    logger.info(
        "resolved_tradable_market slug=%s market_id=%s outcomes=%s yes_token=%s no_token=%s",
        rm.slug, rm.market_id, rm.outcomes, rm.yes_token, rm.no_token
    )

    assert rm.yes_token and rm.no_token

    yes_bid, yes_ask, _ = clob_book_best(rm.yes_token)
    no_bid, no_ask, _ = clob_book_best(rm.no_token)

    p_yes = midpoint_from_best(yes_bid, yes_ask)
    p_no = midpoint_from_best(no_bid, no_ask)

    if p_yes is None or p_no is None:
        logger.warning("Books exist but one side empty; skipping (yes_mid=%s no_mid=%s)", p_yes, p_no)
        return

    logger.info(
        "book_mid yes=%.6f no=%.6f sum=%.6f | yes(bid=%s ask=%s) no(bid=%s ask=%s)",
        p_yes, p_no, (p_yes + p_no),
        f"{yes_bid:.6f}" if yes_bid is not None else None,
        f"{yes_ask:.6f}" if yes_ask is not None else None,
        f"{no_bid:.6f}" if no_bid is not None else None,
        f"{no_ask:.6f}" if no_ask is not None else None,
    )

    signal = compute_signal(p_yes, p_no)
    logger.info("signal=%s", signal)

    if not LIVE_MODE or RUN_MODE != "LIVE" or not LIVE_ARMED:
        logger.info("Not armed (live_mode=%s run_mode=%s live_armed=%s). No orders will be placed.", LIVE_MODE, RUN_MODE, LIVE_ARMED)
        return

    if client is None:
        raise RuntimeError("LIVE_ARMED=true but trading client not initialized.")

    if signal == "BUY_YES":
        px = yes_ask if yes_ask is not None else p_yes
        px = round_to_tick(px)
        order = OrderArgs(token_id=rm.yes_token, price=px, size=ORDER_SIZE_SHARES, side=BUY)
        signed = client.create_order(order)
        resp = client.post_order(signed, OrderType.GTC)
        logger.info("ORDER BUY_YES posted price=%.6f size=%.4f resp=%s", px, ORDER_SIZE_SHARES, resp)

    elif signal == "BUY_NO":
        px = no_ask if no_ask is not None else p_no
        px = round_to_tick(px)
        order = OrderArgs(token_id=rm.no_token, price=px, size=ORDER_SIZE_SHARES, side=BUY)
        signed = client.create_order(order)
        resp = client.post_order(signed, OrderType.GTC)
        logger.info("ORDER BUY_NO posted price=%.6f size=%.4f resp=%s", px, ORDER_SIZE_SHARES, resp)

    else:
        logger.info("No trade condition met.")


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    logger.info("BOOT: bot.py starting")
    logger.info(
        "run_mode=%s live_mode=%s live_armed=%s run_loop=%s poly_slug=%s poly_prefix=%s",
        RUN_MODE, LIVE_MODE, LIVE_ARMED, RUN_LOOP, POLY_SLUG, POLY_PREFIX
    )

    prefix = POLY_PREFIX or "btc-updown-5m"
    client: Optional[ClobClient] = None

    if LIVE_MODE and RUN_MODE == "LIVE" and LIVE_ARMED:
        client = build_clob_client()
        logger.info("Trading client initialized (direct).")

    def get_slug() -> str:
        if POLY_SLUG:
            return POLY_SLUG
        return compute_current_5m_slug(prefix)

    if not RUN_LOOP:
        slug = sys.argv[1].strip() if len(sys.argv) >= 2 and sys.argv[1].strip() else get_slug()
        run_once(slug, prefix, client)
        logger.info("BOOT: bot.py finished cleanly")
        return

    while True:
        try:
            slug = get_slug()
            run_once(slug, prefix, client)
        except Exception as e:
            logger.exception("Cycle error: %s", e)
        time.sleep(LOOP_SLEEP_S)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        raise

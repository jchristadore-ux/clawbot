#!/usr/bin/env python3
"""
Johnny 5 - Polymarket bot (direct trading, hands-free ready)

What this file does:
- Resolve market by slug via Gamma
- Map outcomes to token IDs (YES/NO or UP/DOWN -> yes/no convention)
- Read orderbook + midpoints from CLOB public endpoints
- Compute a signal (hook to your edge logic later)
- If LIVE_ARMED=true, place signed limit orders directly via py-clob-client

Hands-free operation:
- If your infrastructure already runs this every 5 minutes, you're done.
- Otherwise set RUN_LOOP=true and (optionally) POLY_PREFIX to auto-compute rolling slugs.

Docs:
- Polymarket recommends using SDK clients for signing/auth/submission. (py-clob-client) :contentReference[oaicite:4]{index=4}
- Auth is 2-level: derive API creds with EIP-712, then use HMAC creds for trading. :contentReference[oaicite:5]{index=5}
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
# pip install py-clob-client
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL


# -----------------------------
# Config
# -----------------------------

GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com").rstrip("/")

CHAIN_ID = int(os.getenv("CHAIN_ID", "137"))  # Polygon mainnet per docs :contentReference[oaicite:6]{index=6}

RUN_MODE = os.getenv("RUN_MODE", "LIVE").upper()  # LIVE or PAPER or TEST
LIVE_MODE = os.getenv("LIVE_MODE", "true").lower() == "true"
LIVE_ARMED = os.getenv("LIVE_ARMED", "false").lower() == "true"

# Scheduler provides this usually
POLY_SLUG = os.getenv("POLY_SLUG", "").strip()

# Optional: auto compute rolling slug when running in a loop
# Example: POLY_PREFIX="btc-updown-5m"
POLY_PREFIX = os.getenv("POLY_PREFIX", "").strip()

RUN_LOOP = os.getenv("RUN_LOOP", "false").lower() == "true"
LOOP_SLEEP_S = int(os.getenv("LOOP_SLEEP_S", "300"))  # 5 minutes

HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "10"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
HTTP_RETRY_SLEEP_S = float(os.getenv("HTTP_RETRY_SLEEP_S", "0.6"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# ---- Trading config ----
# Your private key MUST be provided via env var (never paste it here)
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "").strip()

# If you already derived creds, set these (recommended).
POLY_API_KEY = os.getenv("POLY_API_KEY", "").strip()
POLY_API_SECRET = os.getenv("POLY_API_SECRET", "").strip()
POLY_API_PASSPHRASE = os.getenv("POLY_API_PASSPHRASE", "").strip()

# Signature type per docs (0 EOA, 1 POLY_PROXY, 2 GNOSIS_SAFE). :contentReference[oaicite:7]{index=7}
SIGNATURE_TYPE = int(os.getenv("POLY_SIGNATURE_TYPE", "0"))

# Funder address: for EOA type 0, this is usually your wallet address.
# For proxy/safe accounts, this must be your proxy wallet address. :contentReference[oaicite:8]{index=8}
FUNDER_ADDRESS = os.getenv("POLY_FUNDER_ADDRESS", "").strip()

# Default order sizing (shares)
ORDER_SIZE_SHARES = float(os.getenv("LIVE_ORDER_SIZE_SHARES", "1.0"))

# Tick size and negative risk flags (market-specific; keep defaults safe)
TICK_SIZE = os.getenv("TICK_SIZE", "0.01")
NEG_RISK = os.getenv("NEG_RISK", "false").lower() == "true"

# Edge threshold placeholder (replace with your real edge)
EDGE_MIN = float(os.getenv("EDGE_MIN", "0.00"))


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
            # IMPORTANT: this will explain why you get None midpoints (often 404/no book)
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

    # positional fallback
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
# CLOB pricing: book + midpoint fallback
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


def clob_book_best(token_id: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (best_bid, best_ask) for token_id, or (None, None) if missing.
    """
    b = http_get_json(f"{CLOB_HOST}/book", params={"token_id": token_id})
    if not isinstance(b, dict):
        return None, None
    bids = _parse_price_levels(b.get("bids") or [])
    asks = _parse_price_levels(b.get("asks") or [])
    best_bid = max(bids) if bids else None
    best_ask = min(asks) if asks else None
    return best_bid, best_ask


def clob_midpoint(token_id: str) -> Optional[float]:
    """
    Try official /midpoint first, then compute from /book.
    """
    j = http_get_json(f"{CLOB_HOST}/midpoint", params={"token_id": token_id})
    if isinstance(j, dict):
        mp = j.get("mid_price") or j.get("midpoint")
        if mp is not None:
            try:
                return float(mp)
            except Exception:
                pass

    best_bid, best_ask = clob_book_best(token_id)
    if best_bid is None or best_ask is None:
        return None
    return (best_bid + best_ask) / 2.0


# -----------------------------
# Trading client init (py-clob-client)
# -----------------------------

def build_clob_client() -> ClobClient:
    """
    Uses py-clob-client, which handles:
    - L1: derive API creds from private key
    - L2: authenticated endpoints
    - EIP-712 order signing and posting
    """
    if not POLY_PRIVATE_KEY:
        raise RuntimeError("Missing POLY_PRIVATE_KEY env var (required for direct trading).")

    creds: Optional[ApiCreds] = None
    if POLY_API_KEY and POLY_API_SECRET and POLY_API_PASSPHRASE:
        creds = ApiCreds(api_key=POLY_API_KEY, api_secret=POLY_API_SECRET, api_passphrase=POLY_API_PASSPHRASE)

    # Create client (with or without creds)
    client = ClobClient(
        CLOB_HOST,
        CHAIN_ID,
        POLY_PRIVATE_KEY,
        creds,
        SIGNATURE_TYPE,
        FUNDER_ADDRESS if FUNDER_ADDRESS else None,
    )

    # If creds not provided, derive them (recommended flow per docs). :contentReference[oaicite:9]{index=9}
    if creds is None:
        derived = client.create_or_derive_api_creds()
        logger.info("Derived API creds (store these as env vars for stability): apiKey=%s passphrase=%s", derived.api_key, derived.api_passphrase)
        # IMPORTANT: secret is sensitive; we do not print it. Set it from your logs/tools where you capture it safely.
        # Rebuild client with derived creds
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
# Signal / edge placeholder
# -----------------------------

def compute_signal(p_yes: float, p_no: float) -> str:
    """
    Placeholder. Replace with your real edge logic.
    """
    if not (0.0 < p_yes < 1.0 and 0.0 < p_no < 1.0):
        return "NO"
    s = p_yes + p_no
    if s < 0.90 or s > 1.10:
        return "NO"

    # Example only:
    if p_yes < 0.48 - EDGE_MIN:
        return "BUY_YES"
    if p_no < 0.48 - EDGE_MIN:
        return "BUY_NO"
    return "NO"


# -----------------------------
# Slug helpers (optional hands-free loop)
# -----------------------------

def compute_current_5m_slug(prefix: str) -> str:
    """
    Rolling markets in your system look like: <prefix>-<endEpoch>
    Example observed: btc-updown-5m-1772219700

    We compute the NEXT 5-min boundary end timestamp in UNIX seconds.
    """
    now = int(time.time())
    end_ts = ((now // 300) * 300) + 300
    return f"{prefix}-{end_ts}"


# -----------------------------
# Execute one cycle
# -----------------------------

def run_once(slug: str, client: Optional[ClobClient]) -> None:
    rm = resolve_market(slug)
    logger.info(
        "resolved_market slug=%s market_id=%s outcomes=%s yes_token=%s no_token=%s",
        rm.slug, rm.market_id, rm.outcomes, rm.yes_token, rm.no_token
    )

    if not rm.yes_token or not rm.no_token:
        raise RuntimeError(f"Could not map outcome tokens for slug={slug} outcomes={rm.outcomes}")

    # Prefer book best bid/ask; midpoint as fallback
    yes_bid, yes_ask = clob_book_best(rm.yes_token)
    no_bid, no_ask = clob_book_best(rm.no_token)

    p_yes = clob_midpoint(rm.yes_token)
    p_no = clob_midpoint(rm.no_token)

    if p_yes is None or p_no is None:
        logger.warning("No usable prices (midpoints) for this window (yes=%s no=%s). Likely no book/liquidity.", p_yes, p_no)
        return

    logger.info(
        "prices yes_mid=%.6f no_mid=%.6f sum=%.6f | yes(bid=%s ask=%s) no(bid=%s ask=%s)",
        p_yes, p_no, (p_yes + p_no),
        f"{yes_bid:.6f}" if yes_bid is not None else None,
        f"{yes_ask:.6f}" if yes_ask is not None else None,
        f"{no_bid:.6f}" if no_bid is not None else None,
        f"{no_ask:.6f}" if no_ask is not None else None,
    )

    signal = compute_signal(p_yes, p_no)
    logger.info("signal=%s", signal)

    # Gate trading
    if not LIVE_MODE or RUN_MODE != "LIVE" or not LIVE_ARMED:
        logger.info("Not armed (live_mode=%s run_mode=%s live_armed=%s). No orders will be placed.", LIVE_MODE, RUN_MODE, LIVE_ARMED)
        return

    if client is None:
        raise RuntimeError("LIVE_ARMED=true but trading client is not initialized.")

    # Choose an executable limit price using the book:
    # - BUY: cross to best ask if available (more likely to fill)
    # - Otherwise: use midpoint rounded to tick
    def round_to_tick(px: float) -> float:
        try:
            t = float(TICK_SIZE)
            return round(px / t) * t
        except Exception:
            return px

    if signal == "BUY_YES":
        px = yes_ask if yes_ask is not None else p_yes
        px = round_to_tick(px)
        order = OrderArgs(token_id=rm.yes_token, price=px, size=ORDER_SIZE_SHARES, side=BUY)
        signed = client.create_order(order)
        resp = client.post_order(signed, OrderType.GTC)  # stable interface :contentReference[oaicite:10]{index=10}
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

    # Only initialize trading client if we might trade
    client: Optional[ClobClient] = None
    if LIVE_MODE and RUN_MODE == "LIVE" and LIVE_ARMED:
        client = build_clob_client()
        logger.info("Trading client initialized (direct).")

    def get_slug() -> str:
        if POLY_SLUG:
            return POLY_SLUG
        if POLY_PREFIX:
            return compute_current_5m_slug(POLY_PREFIX)
        raise RuntimeError("No POLY_SLUG provided and POLY_PREFIX is empty. Provide one.")

    if not RUN_LOOP:
        slug = sys.argv[1].strip() if len(sys.argv) >= 2 and sys.argv[1].strip() else get_slug()
        run_once(slug, client)
        logger.info("BOOT: bot.py finished cleanly")
        return

    # Hands-free loop mode
    while True:
        try:
            slug = get_slug()
            run_once(slug, client)
        except Exception as e:
            logger.exception("Cycle error: %s", e)
        time.sleep(LOOP_SLEEP_S)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Fatal error: %s", e)
        raise

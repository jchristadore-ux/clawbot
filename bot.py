#!/usr/bin/env python3
"""
Johnny 5 - Polymarket bot (clean rebuild)
- Resolves a Polymarket market (Gamma) by slug
- Extracts CLOB token IDs for the two outcomes (YES/NO or UP/DOWN)
- Fetches midpoint prices from the CLOB API (mid_price)
- Computes a basic signal
- Optionally places orders only when live_armed=True (order placement is stubbed / Bun optional)

Fixes issues you hit:
- NameError: clob_midpoint not defined
- CLOB /midpoint returns {"mid_price": "..."} not {"midpoint": "..."}
- Up/Down markets mapping
- Gamma outcomes / clobTokenIds sometimes JSON-encoded strings
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


# -----------------------------
# Config
# -----------------------------

GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com")

# Optional: you appear to have a Bun service running (you log bun_health_status=200)
BUN_HOST = os.getenv("BUN_HOST", "http://127.0.0.1:3000")

RUN_MODE = os.getenv("RUN_MODE", "LIVE").upper()  # LIVE or PAPER or TEST
LIVE_MODE = os.getenv("LIVE_MODE", "true").lower() == "true"
LIVE_ARMED = os.getenv("LIVE_ARMED", "false").lower() == "true"

# Your logs show poly_slug is passed in env; allow CLI override too
POLY_SLUG = os.getenv("POLY_SLUG", "").strip()

HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "10"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "3"))
HTTP_RETRY_SLEEP_S = float(os.getenv("HTTP_RETRY_SLEEP_S", "0.6"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


# -----------------------------
# Logging
# -----------------------------

logger = logging.getLogger("johnny5")
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)


# -----------------------------
# HTTP helpers
# -----------------------------

def http_get_json(url: str, params: Optional[Any] = None, headers: Optional[Dict[str, str]] = None) -> Optional[Any]:
    last_err = None
    for _ in range(max(1, HTTP_RETRIES)):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT_S)
            if r.status_code == 200:
                return r.json()
            # Common: CLOB endpoints return 404 when no book exists for token
            logger.debug("GET %s -> %s %s", url, r.status_code, r.text[:200])
            return None
        except Exception as e:
            last_err = e
            time.sleep(HTTP_RETRY_SLEEP_S)
    logger.warning("HTTP GET failed: %s (%s)", url, last_err)
    return None


def http_post_json(url: str, payload: Any, headers: Optional[Dict[str, str]] = None) -> Optional[Any]:
    last_err = None
    for _ in range(max(1, HTTP_RETRIES)):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_S)
            if r.status_code == 200:
                return r.json()
            logger.debug("POST %s -> %s %s", url, r.status_code, r.text[:200])
            return None
        except Exception as e:
            last_err = e
            time.sleep(HTTP_RETRY_SLEEP_S)
    logger.warning("HTTP POST failed: %s (%s)", url, last_err)
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
# Bun health (optional)
# -----------------------------

def bun_healthcheck() -> Optional[int]:
    # You can change the endpoint if your Bun service uses something else
    url = f"{BUN_HOST.rstrip('/')}/ok"
    try:
        r = requests.get(url, timeout=3)
        return r.status_code
    except Exception:
        return None


# -----------------------------
# Gamma parsing helpers
# -----------------------------

def _maybe_json(x: Any) -> Any:
    """
    Gamma sometimes returns outcomes / outcomePrices / clobTokenIds as JSON-encoded strings.
    This converts:
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


def extract_yes_no_token_ids(market: dict) -> Tuple[Optional[str], Optional[str], List[str], List[str]]:
    """
    Returns (yes_token, no_token, outcomes_list, token_ids_list)
    Supports:
      - YES/NO
      - UP/DOWN (treat UP as YES side, DOWN as NO side for strategy conventions)
    """
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

    # If still missing, try positional fallback for common 2-outcome markets
    if yes_id is None or no_id is None:
        if len(token_ids_str) >= 2:
            # If outcomes look like [UP, DOWN] or [YES, NO] but casing/spacing weird:
            if norm[0] in ("YES", "UP") and norm[1] in ("NO", "DOWN"):
                yes_id, no_id = token_ids_str[0], token_ids_str[1]
            elif norm[0] in ("NO", "DOWN") and norm[1] in ("YES", "UP"):
                yes_id, no_id = token_ids_str[1], token_ids_str[0]

    return yes_id, no_id, outcomes_str, token_ids_str


def fetch_market_by_slug(slug: str) -> Optional[dict]:
    """
    Gamma supports fetching by slug.
    We try /markets?slug= first, then /events?slug= and pull a market from the event.
    """
    # 1) markets?slug=
    j = http_get_json(f"{GAMMA_HOST}/markets", params={"slug": slug})
    if isinstance(j, list) and len(j) > 0 and isinstance(j[0], dict):
        return j[0]
    if isinstance(j, dict) and j:
        # Some Gamma endpoints may return dict for single result
        return j

    # 2) events?slug= -> event contains "markets" (often)
    ej = http_get_json(f"{GAMMA_HOST}/events", params={"slug": slug})
    if isinstance(ej, list) and len(ej) > 0 and isinstance(ej[0], dict):
        ev = ej[0]
        markets = ev.get("markets")
        if isinstance(markets, list) and markets and isinstance(markets[0], dict):
            # If event has multiple markets, try to match exact slug first
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

    # market_id can be "id" or "marketId" depending on shape
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
# CLOB pricing
# -----------------------------

def clob_midpoints(token_ids: List[str]) -> Dict[str, Optional[float]]:
    """
    Uses GET /midpoints (query parameters) to fetch mid prices for multiple token IDs.
    Returns dict[token_id] -> float or None
    """
    if not token_ids:
        return {}

    # Polymarket docs: GET /midpoints expects repeated token_ids query params
    params = [("token_ids", tid) for tid in token_ids]
    j = http_get_json(f"{CLOB_HOST}/midpoints", params=params)

    out: Dict[str, Optional[float]] = {tid: None for tid in token_ids}

    if not isinstance(j, dict):
        return out

    for tid in token_ids:
        v = j.get(tid)
        if v is None:
            continue
        try:
            out[tid] = float(v)
        except Exception:
            out[tid] = None

    return out


def clob_midpoint(token_id: str) -> Optional[float]:
    """
    Single-token convenience wrapper (fixes your NameError forever).
    """
    mids = clob_midpoints([token_id])
    return mids.get(token_id)


# -----------------------------
# Signal / strategy (minimal)
# -----------------------------

def compute_signal(p_yes: float, p_no: float) -> str:
    """
    Replace with your actual strategy.
    This is a placeholder that never trades unless there is a clear imbalance.
    """
    # sanity: prices should be [0,1] and typically sum ~1
    if not (0.0 < p_yes < 1.0 and 0.0 < p_no < 1.0):
        return "NO"

    s = p_yes + p_no
    if s < 0.90 or s > 1.10:
        # Something off: stale or weird market; don't trade.
        return "NO"

    # Simple example:
    # If YES is much cheaper than NO (i.e., YES < 0.48), buy YES (mean reversion idea)
    if p_yes < 0.48:
        return "BUY_YES"
    if p_no < 0.48:
        return "BUY_NO"

    return "NO"


# -----------------------------
# Order placement (stub / Bun optional)
# -----------------------------

def place_order_via_bun(side: str, token_id: str, price: float, size: float) -> bool:
    """
    Optional: If your Bun service actually places orders, implement its endpoint here.
    This function is SAFE by default (returns False if endpoint not present).
    """
    url = f"{BUN_HOST.rstrip('/')}/place-order"
    payload = {
        "side": side,
        "token_id": token_id,
        "price": price,
        "size": size,
    }
    j = http_post_json(url, payload)
    if not j:
        return False
    # Accept either {"ok": true} or similar
    if isinstance(j, dict) and (j.get("ok") is True or j.get("success") is True):
        return True
    return False


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    logger.info("BOOT: bot.py starting")

    slug = POLY_SLUG
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        slug = sys.argv[1].strip()

    if not slug:
        raise RuntimeError("POLY_SLUG is empty. Set POLY_SLUG env var or pass slug as CLI arg.")

    logger.info(
        "run_mode=%s live_mode=%s live_armed=%s poly_slug=%s",
        RUN_MODE,
        LIVE_MODE,
        LIVE_ARMED,
        slug,
    )

    bun_status = bun_healthcheck()
    if bun_status is None:
        logger.info("bun_health_status=unreachable")
    else:
        logger.info("bun_health_status=%s", bun_status)

    rm = resolve_market(slug)
    logger.info(
        "resolved_market slug=%s market_id=%s outcomes=%s yes_token=%s no_token=%s",
        rm.slug,
        rm.market_id,
        rm.outcomes,
        rm.yes_token,
        rm.no_token,
    )

    if not rm.yes_token or not rm.no_token:
        raise RuntimeError(
            f"Could not map outcome tokens (yes_token={rm.yes_token} no_token={rm.no_token}) outcomes={rm.outcomes}"
        )

    # Fetch midpoints
    mids = clob_midpoints([rm.yes_token, rm.no_token])
    p_yes = mids.get(rm.yes_token)
    p_no = mids.get(rm.no_token)

    if p_yes is None or p_no is None:
        logger.warning("Could not fetch midpoint prices from CLOB (yes=%s no=%s)", p_yes, p_no)
        logger.info("BOOT: bot.py finished cleanly")
        return

    logger.info("midpoints yes=%.6f no=%.6f sum=%.6f", p_yes, p_no, (p_yes + p_no))

    # Decide
    signal = compute_signal(p_yes, p_no)
    logger.info("signal=%s", signal)

    # Gate trading
    if not LIVE_MODE or RUN_MODE != "LIVE" or not LIVE_ARMED:
        logger.info("Not armed for live trading (live_mode=%s run_mode=%s live_armed=%s). Exiting.", LIVE_MODE, RUN_MODE, LIVE_ARMED)
        logger.info("BOOT: bot.py finished cleanly")
        return

    # If armed, place a tiny order (you should replace this logic with your own sizing)
    size = float(os.getenv("LIVE_ORDER_SIZE", "1.0"))  # units in CLOB terms; replace with your convention
    # choose the cheaper side to buy (example only)
    if signal == "BUY_YES":
        ok = place_order_via_bun(side="BUY", token_id=rm.yes_token, price=p_yes, size=size)
        logger.info("order_result ok=%s side=BUY token=yes price=%.6f size=%.4f", ok, p_yes, size)
    elif signal == "BUY_NO":
        ok = place_order_via_bun(side="BUY", token_id=rm.no_token, price=p_no, size=size)
        logger.info("order_result ok=%s side=BUY token=no price=%.6f size=%.4f", ok, p_no, size)
    else:
        logger.info("No trade condition met.")

    logger.info("BOOT: bot.py finished cleanly")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Keep your container logs clean and explicit
        logger.exception("Fatal error: %s", e)
        raise

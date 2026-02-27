#!/usr/bin/env python3
"""
Johnny 5 - Polymarket bot (stabilized)

What this version fixes vs your current bot.py:
- Pricing reliability: tries /midpoint, then /midpoints, then /book->computed midpoint
- Better logging when pricing fails (status + body preview)
- Keeps Up/Down mapping to YES/NO convention
- Keeps clob_midpoint defined and stable
- Keeps live gating (LIVE_MODE + RUN_MODE + LIVE_ARMED)
- Bun remains optional

NOTE: This still does NOT implement direct signed order placement to Polymarket.
It only supports the optional Bun bridge (if/when you have it).
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

GAMMA_HOST = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com").rstrip("/")
CLOB_HOST = os.getenv("CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
BUN_HOST = os.getenv("BUN_HOST", "http://127.0.0.1:3000").rstrip("/")

RUN_MODE = os.getenv("RUN_MODE", "LIVE").upper()  # LIVE or PAPER or TEST
LIVE_MODE = os.getenv("LIVE_MODE", "true").lower() == "true"
LIVE_ARMED = os.getenv("LIVE_ARMED", "false").lower() == "true"

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
logger.handlers = []
logger.addHandler(handler)
logger.setLevel(LOG_LEVEL)


# -----------------------------
# HTTP helpers (with useful failure logs)
# -----------------------------

def http_get_json(url: str, params: Optional[Any] = None, headers: Optional[Dict[str, str]] = None) -> Optional[Any]:
    """
    GET JSON with retries.
    If non-200, returns None but logs status/body at WARNING so you can see WHY it failed.
    """
    last_err = None
    for attempt in range(1, max(1, HTTP_RETRIES) + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT_S)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    logger.warning("HTTP 200 but JSON decode failed: url=%s params=%s body=%r", url, params, r.text[:200])
                    return None

            # Log why it failed (often 404: no orderbook)
            logger.warning(
                "HTTP non-200: url=%s params=%s status=%s body=%r",
                url, params, r.status_code, r.text[:200]
            )
            return None

        except Exception as e:
            last_err = e
            logger.warning("HTTP exception: url=%s params=%s attempt=%s err=%s", url, params, attempt, e)
            time.sleep(HTTP_RETRY_SLEEP_S)

    logger.warning("HTTP GET failed after retries: url=%s params=%s last_err=%s", url, params, last_err)
    return None


def http_post_json(url: str, payload: Any, headers: Optional[Dict[str, str]] = None) -> Optional[Any]:
    last_err = None
    for attempt in range(1, max(1, HTTP_RETRIES) + 1):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=HTTP_TIMEOUT_S)
            if r.status_code == 200:
                try:
                    return r.json()
                except Exception:
                    logger.warning("HTTP 200 but JSON decode failed: url=%s body=%r", url, r.text[:200])
                    return None

            logger.warning("HTTP non-200 POST: url=%s status=%s body=%r", url, r.status_code, r.text[:200])
            return None

        except Exception as e:
            last_err = e
            logger.warning("HTTP exception POST: url=%s attempt=%s err=%s", url, attempt, e)
            time.sleep(HTTP_RETRY_SLEEP_S)

    logger.warning("HTTP POST failed after retries: url=%s last_err=%s", url, last_err)
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
    # If your Bun server uses a different health endpoint, change this path.
    url = f"{BUN_HOST}/ok"
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
    Gamma sometimes returns outcomes/clobTokenIds as JSON-encoded strings.
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


def extract_yes_no_token_ids(market: dict) -> Tuple[Optional[str], Optional[str], List[str], List[str]]:
    """
    Returns (yes_token, no_token, outcomes_list, token_ids_list)
    Supports:
      - YES/NO
      - UP/DOWN (treat UP as YES side, DOWN as NO side)
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

    # Positional fallback for common 2-outcome markets
    if (yes_id is None or no_id is None) and len(token_ids_str) >= 2:
        if norm[0] in ("YES", "UP") and norm[1] in ("NO", "DOWN"):
            yes_id, no_id = token_ids_str[0], token_ids_str[1]
        elif norm[0] in ("NO", "DOWN") and norm[1] in ("YES", "UP"):
            yes_id, no_id = token_ids_str[1], token_ids_str[0]

    return yes_id, no_id, outcomes_str, token_ids_str


def fetch_market_by_slug(slug: str) -> Optional[dict]:
    """
    Try:
      1) GET /markets?slug=...
      2) GET /events?slug=... -> pick matching market from event.markets
    """
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
# CLOB pricing (robust)
# -----------------------------

def _parse_price_levels(levels: Any) -> List[float]:
    """
    Levels can be:
      - list of dicts: [{"price":"0.51","size":"12"}, ...]
      - list of lists: [["0.51","12"], ...]
    Returns list of float prices.
    """
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


def clob_midpoint_from_book(token_id: str) -> Optional[float]:
    b = http_get_json(f"{CLOB_HOST}/book", params={"token_id": token_id})
    if not isinstance(b, dict):
        return None

    bids = _parse_price_levels(b.get("bids") or [])
    asks = _parse_price_levels(b.get("asks") or [])

    if not bids or not asks:
        # If one side missing, midpoint is undefined
        return None

    best_bid = max(bids)
    best_ask = min(asks)
    return (best_bid + best_ask) / 2.0


def clob_midpoint_single(token_id: str) -> Optional[float]:
    """
    Try:
      1) GET /midpoint?token_id=...  -> {"mid_price": "..."}
      2) GET /book?token_id=...      -> computed midpoint
    """
    j = http_get_json(f"{CLOB_HOST}/midpoint", params={"token_id": token_id})
    if isinstance(j, dict):
        mp = j.get("mid_price")
        if mp is None:
            mp = j.get("midpoint")  # defensive
        if mp is not None:
            try:
                return float(mp)
            except Exception:
                return None

    # fallback: compute from book if possible
    return clob_midpoint_from_book(token_id)


def clob_midpoints(token_ids: List[str]) -> Dict[str, Optional[float]]:
    """
    Try batch midpoints first, then single fallback for any missing.
    Batch endpoint sometimes behaves differently, so we keep it but don’t rely on it.
    """
    out: Dict[str, Optional[float]] = {tid: None for tid in token_ids}
    if not token_ids:
        return out

    # Attempt batch
    params = [("token_ids", tid) for tid in token_ids]
    j = http_get_json(f"{CLOB_HOST}/midpoints", params=params)

    if isinstance(j, dict):
        for tid in token_ids:
            v = j.get(tid)
            if v is None:
                continue
            try:
                out[tid] = float(v)
            except Exception:
                out[tid] = None

    # Fill in any missing via single midpoint + book fallback
    for tid in token_ids:
        if out.get(tid) is None:
            out[tid] = clob_midpoint_single(tid)

    return out


def clob_midpoint(token_id: str) -> Optional[float]:
    return clob_midpoint_single(token_id)


# -----------------------------
# Signal / strategy (minimal)
# -----------------------------

def compute_signal(p_yes: float, p_no: float) -> str:
    # sanity
    if not (0.0 < p_yes < 1.0 and 0.0 < p_no < 1.0):
        return "NO"
    s = p_yes + p_no
    if s < 0.90 or s > 1.10:
        return "NO"
    if p_yes < 0.48:
        return "BUY_YES"
    if p_no < 0.48:
        return "BUY_NO"
    return "NO"


# -----------------------------
# Order placement (stub / Bun optional)
# -----------------------------

def place_order_via_bun(side: str, token_id: str, price: float, size: float) -> bool:
    url = f"{BUN_HOST}/place-order"
    payload = {"side": side, "token_id": token_id, "price": price, "size": size}
    j = http_post_json(url, payload)
    if not j:
        return False
    return isinstance(j, dict) and (j.get("ok") is True or j.get("success") is True)


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
        RUN_MODE, LIVE_MODE, LIVE_ARMED, slug
    )

    bun_status = bun_healthcheck()
    if bun_status is None:
        logger.info("bun_health_status=unreachable")
    else:
        logger.info("bun_health_status=%s", bun_status)

    rm = resolve_market(slug)
    logger.info(
        "resolved_market slug=%s market_id=%s outcomes=%s yes_token=%s no_token=%s",
        rm.slug, rm.market_id, rm.outcomes, rm.yes_token, rm.no_token
    )

    if not rm.yes_token or not rm.no_token:
        raise RuntimeError(
            f"Could not map outcome tokens (yes_token={rm.yes_token} no_token={rm.no_token}) outcomes={rm.outcomes}"
        )

    # Fetch midpoints (robust)
    mids = clob_midpoints([rm.yes_token, rm.no_token])
    p_yes = mids.get(rm.yes_token)
    p_no = mids.get(rm.no_token)

    if p_yes is None or p_no is None:
        logger.warning("Could not fetch prices from CLOB after fallbacks (yes=%s no=%s)", p_yes, p_no)
        logger.info("BOOT: bot.py finished cleanly")
        return

    logger.info("midpoints yes=%.6f no=%.6f sum=%.6f", p_yes, p_no, (p_yes + p_no))

    signal = compute_signal(p_yes, p_no)
    logger.info("signal=%s", signal)

    # Gate trading
    if not LIVE_MODE or RUN_MODE != "LIVE" or not LIVE_ARMED:
        logger.info(
            "Not armed for live trading (live_mode=%s run_mode=%s live_armed=%s). Exiting.",
            LIVE_MODE, RUN_MODE, LIVE_ARMED
        )
        logger.info("BOOT: bot.py finished cleanly")
        return

    size = float(os.getenv("LIVE_ORDER_SIZE", "1.0"))

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
        logger.exception("Fatal error: %s", e)
        raise

#!/usr/bin/env python3
"""
Johnny 5 – Polymarket bot (rollover-stable)

Key fix vs your logs:
- Rollover is handled as a single state transition.
- Cooldown is tracked with `cooldown_until` so we do NOT repeatedly "roll over" each loop.
- When no open markets exist, we back off and keep current ticker (don’t thrash).
"""

from __future__ import annotations

import os
import time
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


# =========================
# CONFIG (edit or env vars)
# =========================

SERIES = os.getenv("SERIES", "KXBTC15M")  # <-- CHANGE THIS if needed
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "5"))
ROLLOVER_COOLDOWN_SECONDS = int(os.getenv("ROLLOVER_COOLDOWN_SECONDS", "30"))

# Gamma / Polymarket endpoints (adjust to your setup)
GAMMA_BASE_URL = os.getenv("GAMMA_BASE_URL", "https://gamma-api.polymarket.com")
CLOB_BASE_URL = os.getenv("CLOB_BASE_URL", "https://clob.polymarket.com")

# Optional: if you require auth headers/cookies for gamma/clob, add here:
GAMMA_TIMEOUT = float(os.getenv("GAMMA_TIMEOUT", "10"))
CLOB_TIMEOUT = float(os.getenv("CLOB_TIMEOUT", "10"))

# Wallet / trading config (stubs here — wire into your existing trade code)
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")  # <-- SET THIS in env
LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() in ("1", "true", "yes")
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "50"))
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.01"))  # e.g., required edge to trade

# Backoff when gamma returns no markets
NO_MARKETS_BACKOFF_SECONDS = int(os.getenv("NO_MARKETS_BACKOFF_SECONDS", "20"))


# =========================
# LOGGING
# =========================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("bot")


# =========================
# HELPERS
# =========================

def _now() -> float:
    return time.time()

def _sleep(seconds: float) -> None:
    time.sleep(max(0.0, seconds))

def _safe_json(x: Any) -> Any:
    """
    Gamma sometimes returns JSON-encoded strings for fields that should be lists/dicts.
    """
    if isinstance(x, str):
        s = x.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                return json.loads(s)
            except Exception:
                return x
    return x

def _http_get(url: str, timeout: float, headers: Optional[Dict[str, str]] = None) -> Any:
    r = requests.get(url, headers=headers or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


# =========================
# DATA MODELS
# =========================

@dataclass
class Market:
    ticker: str
    slug: Optional[str]
    start_ts: Optional[int]
    end_ts: Optional[int]
    is_open: bool
    raw: Dict[str, Any]

@dataclass
class BotState:
    current_ticker: Optional[str] = None
    cooldown_until: float = 0.0
    last_no_markets_until: float = 0.0


# =========================
# MARKET DISCOVERY / SELECTION
# =========================

def fetch_open_markets_for_series(series: str) -> List[Market]:
    """
    Pull markets from Gamma and filter to open markets in this series.

    IMPORTANT: Gamma schema varies. You may need to tweak fields here to match your exact market objects.
    """
    # Common gamma endpoint patterns vary; you may already have a working one.
    # If yours differs, replace this URL with your known-good one.
    # Examples seen in the wild:
    #   /markets?closed=false&limit=100
    #   /markets?series=<...>
    #
    # We'll do a broad fetch then filter locally:
    url = f"{GAMMA_BASE_URL}/markets?closed=false&limit=200"
    data = _http_get(url, timeout=GAMMA_TIMEOUT)

    markets: List[Market] = []
    for m in data if isinstance(data, list) else data.get("markets", []):
        mm = dict(m)
        # Try to normalize fields (adjust as needed)
        ticker = mm.get("ticker") or mm.get("marketTicker") or ""
        if not ticker:
            continue

        # series match: ticker starts with series + "-"
        if not ticker.startswith(series + "-"):
            continue

        is_open = bool(mm.get("closed") is False) or bool(mm.get("isOpen", True))
        # sometimes "closed" missing; use "active" or "status"
        if "closed" in mm:
            is_open = (mm["closed"] is False)

        slug = mm.get("slug")
        start_ts = mm.get("startTime") or mm.get("startTs") or mm.get("start_ts")
        end_ts = mm.get("endTime") or mm.get("endTs") or mm.get("end_ts")

        # ensure ints where possible
        try:
            start_ts = int(start_ts) if start_ts is not None else None
        except Exception:
            start_ts = None
        try:
            end_ts = int(end_ts) if end_ts is not None else None
        except Exception:
            end_ts = None

        markets.append(Market(
            ticker=ticker,
            slug=slug,
            start_ts=start_ts,
            end_ts=end_ts,
            is_open=is_open,
            raw=mm,
        ))

    # filter to open only
    return [m for m in markets if m.is_open]


def pick_current_market(markets: List[Market], now_ts: Optional[int] = None) -> Optional[Market]:
    """
    Decide which market is "current".
    Default rule: pick the open market whose [start_ts, end_ts] contains now.
    If timestamps are missing, fall back to lexicographic latest ticker.
    """
    if not markets:
        return None

    if now_ts is None:
        now_ts = int(_now())

    with_window = [m for m in markets if m.start_ts and m.end_ts]
    if with_window:
        active = [m for m in with_window if (m.start_ts <= now_ts < m.end_ts)]
        if active:
            # If multiple, pick the one with the nearest end_ts (soonest ending)
            return sorted(active, key=lambda m: m.end_ts or 0)[0]

        # If none active (clock skew or gaps), pick the one with the closest start_ts <= now
        past = [m for m in with_window if (m.start_ts <= now_ts)]
        if past:
            return sorted(past, key=lambda m: m.start_ts or 0, reverse=True)[0]

        # Otherwise pick earliest upcoming
        future = [m for m in with_window if (m.start_ts > now_ts)]
        if future:
            return sorted(future, key=lambda m: m.start_ts or 0)[0]

    # Fallback: pick "latest" ticker by sorting (works if tickers encode time)
    return sorted(markets, key=lambda m: m.ticker)[-1]


# =========================
# TRADING (HOOK YOUR LOGIC HERE)
# =========================

def compute_signal_for_market(market: Market) -> Tuple[str, Dict[str, Any]]:
    """
    Replace this with your real strategy.
    Return: (signal_name, debug_dict)
    """
    # Placeholder strategy: do nothing
    return ("NO", {"reason": "placeholder"})


def place_trade_if_needed(market: Market, signal: str, debug: Dict[str, Any]) -> None:
    """
    Replace this with your real order placement logic.
    """
    if signal == "NO":
        return

    # Example: stub
    if LIVE_MODE:
        log.info("LIVE trade would be placed here | ticker=%s signal=%s debug=%s", market.ticker, signal, debug)
    else:
        log.info("PAPER trade | ticker=%s signal=%s debug=%s", market.ticker, signal, debug)


# =========================
# MAIN LOOP (ROLLOVER-SAFE)
# =========================

def maybe_rollover(state: BotState, target_market: Optional[Market]) -> None:
    """
    Apply rollover once when ticker changes. Sets cooldown timer.
    """
    if target_market is None:
        return

    if state.current_ticker != target_market.ticker:
        state.current_ticker = target_market.ticker
        state.cooldown_until = _now() + ROLLOVER_COOLDOWN_SECONDS
        log.info("Market rollover -> current ticker=%s", state.current_ticker)
        log.info("Cooldown at rollover (%ss) — waiting.", ROLLOVER_COOLDOWN_SECONDS)


def run() -> None:
    state = BotState()

    log.info("BOOT: bot.py starting")
    log.info("series=%s poll=%ss cooldown=%ss live=%s", SERIES, POLL_SECONDS, ROLLOVER_COOLDOWN_SECONDS, LIVE_MODE)

    while True:
        now = _now()

        # If we are in cooldown, do NOT recompute rollover; just wait.
        if now < state.cooldown_until:
            _sleep(min(POLL_SECONDS, state.cooldown_until - now))
            continue

        # If we recently saw "no markets", back off a bit.
        if now < state.last_no_markets_until:
            _sleep(POLL_SECONDS)
            continue

        # Fetch open markets
        try:
            markets = fetch_open_markets_for_series(SERIES)
        except Exception as e:
            log.warning("Failed to fetch markets: %s", e)
            _sleep(POLL_SECONDS)
            continue

        if not markets:
            log.warning("No open markets found for series=%s", SERIES)
            state.last_no_markets_until = _now() + NO_MARKETS_BACKOFF_SECONDS
            _sleep(POLL_SECONDS)
            continue

        # Pick the market that should be current
        target = pick_current_market(markets)

        # Perform rollover at most once per new ticker
        maybe_rollover(state, target)

        # If cooldown just started, loop will sleep next tick
        if _now() < state.cooldown_until:
            _sleep(POLL_SECONDS)
            continue

        # Trade on current market
        current = next((m for m in markets if m.ticker == state.current_ticker), None)
        if current is None:
            # If current ticker vanished from open list, force selection next loop
            log.warning("Current ticker not in open list; will reselect. current=%s", state.current_ticker)
            state.current_ticker = None
            _sleep(POLL_SECONDS)
            continue

        signal, dbg = compute_signal_for_market(current)
        place_trade_if_needed(current, signal, dbg)

        _sleep(POLL_SECONDS)


if __name__ == "__main__":
    run()

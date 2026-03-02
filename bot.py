#!/usr/bin/env python3
"""
Johnny 5 – Kalshi-only bot for KXBTC15M (BTC Up/Down 15m)

Goals:
- NO Polymarket/Gamma code at all.
- Trade Kalshi series KXBTC15M only (rolling market).
- Long backoff when no open markets found (stop thrashing).
- Optional fail-fast exit if stuck too long in "no markets" loop.
- Logs only when:
    * boot / equity snapshot
    * a trade is submitted
    * (optional) periodic equity heartbeat

How “edge” works here (pluggable):
- You provide FAIR_PROB_UP (0..1), your model’s fair probability that BTC ends UP.
- We compare to best ask prices in the orderbook (YES=UP, NO=DOWN).
- If edge >= MIN_EDGE, we place a limit buy at best ask (or improved price if you want).

Kalshi Python SDK:
- pip install kalshi_python_sync
Docs quickstart uses host: https://api.elections.kalshi.com/trade-api/v2
"""

from __future__ import annotations

import os
import sys
import time
import json
import uuid
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

from kalshi_python_sync import Configuration, KalshiClient  # pip install kalshi_python_sync


# =========================
# CONFIG (env vars)
# =========================

# Kalshi API host (default per official quickstart)
KALSHI_HOST = os.getenv("KALSHI_HOST", "https://api.elections.kalshi.com/trade-api/v2")

# AUTH (required for live trading)
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip()  # path/to/private_key.pem

# SERIES (you said KXBTC15M only)
SERIES_TICKER = os.getenv("SERIES_TICKER", "KXBTC15M").strip()

# Polling
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "5"))

# Long backoff when no markets
NO_MARKETS_BACKOFF_START = float(os.getenv("NO_MARKETS_BACKOFF_START", "15"))   # seconds
NO_MARKETS_BACKOFF_MAX = float(os.getenv("NO_MARKETS_BACKOFF_MAX", "600"))      # 10 min max sleep
EXIT_ON_NO_MARKETS_AFTER_MIN = float(os.getenv("EXIT_ON_NO_MARKETS_AFTER_MIN", "30"))  # minutes; 0 disables

# Trading
LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() in ("1", "true", "yes", "y")
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.02"))  # 2% edge threshold (tune this)
MAX_CONTRACTS_PER_TRADE = int(os.getenv("MAX_CONTRACTS_PER_TRADE", "10"))
MAX_TOTAL_POSITION_CONTRACTS = int(os.getenv("MAX_TOTAL_POSITION_CONTRACTS", "50"))  # simple cap (both sides)

# Edge input (choose one):
FAIR_PROB_UP_ENV = os.getenv("FAIR_PROB_UP", "").strip()  # e.g. "0.53"
FAIR_PROB_JSON_PATH = os.getenv("FAIR_PROB_JSON_PATH", "").strip()  # e.g. "/app/fair_prob.json"
# JSON format expected: {"fair_prob_up": 0.53}

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
EQUITY_HEARTBEAT_SECONDS = float(os.getenv("EQUITY_HEARTBEAT_SECONDS", "0"))  # 0 = off, else log equity periodically


# =========================
# LOGGING (quiet by default)
# =========================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("johnny5")


# =========================
# DATA
# =========================

@dataclass
class OrderbookTop:
    yes_ask: Optional[int]  # price in cents, 1..99
    no_ask: Optional[int]
    yes_bid: Optional[int]
    no_bid: Optional[int]


@dataclass
class BotState:
    current_market_ticker: Optional[str] = None
    no_markets_since_ts: Optional[float] = None
    no_markets_backoff: float = NO_MARKETS_BACKOFF_START
    last_equity_log_ts: float = 0.0


# =========================
# CLIENT
# =========================

def build_client() -> KalshiClient:
    config = Configuration(host=KALSHI_HOST)

    if KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH:
        with open(KALSHI_PRIVATE_KEY_PATH, "r") as f:
            private_key_pem = f.read()
        config.api_key_id = KALSHI_API_KEY_ID
        config.private_key_pem = private_key_pem

    return KalshiClient(config)


# =========================
# EDGE INPUT
# =========================

def read_fair_prob_up() -> Optional[float]:
    """
    Returns your model's fair probability that BTC ends UP for the current 15m window.
    """
    # Env wins
    if FAIR_PROB_UP_ENV:
        try:
            p = float(FAIR_PROB_UP_ENV)
            if 0.0 <= p <= 1.0:
                return p
        except Exception:
            return None

    # JSON file (so your model can update it without restarting bot)
    if FAIR_PROB_JSON_PATH:
        try:
            with open(FAIR_PROB_JSON_PATH, "r") as f:
                obj = json.load(f)
            p = float(obj.get("fair_prob_up"))
            if 0.0 <= p <= 1.0:
                return p
        except Exception:
            return None

    return None


# =========================
# MARKET SELECTION (rolling)
# =========================

def select_current_market(client: KalshiClient) -> Optional[Dict[str, Any]]:
    """
    Fetch open markets for SERIES_TICKER and pick the one that should be traded now.
    Heuristic: choose the open market with the *earliest close_time* in the future.
    """
    # SDK method names can vary by version; these are consistent with the v2 client patterns.
    # get_markets supports series_ticker/status/cursor/limit per API.
    resp = client.get_markets(series_ticker=SERIES_TICKER, status="open", limit=200)

    markets = getattr(resp, "markets", None) or resp.get("markets") if isinstance(resp, dict) else None
    if not markets:
        return None

    # normalize into dicts
    norm: List[Dict[str, Any]] = []
    for m in markets:
        if isinstance(m, dict):
            norm.append(m)
        else:
            # SDK objects often have model_dump() or dict-like attrs
            if hasattr(m, "model_dump"):
                norm.append(m.model_dump())
            elif hasattr(m, "to_dict"):
                norm.append(m.to_dict())
            else:
                norm.append({k: getattr(m, k) for k in dir(m) if not k.startswith("_")})

    now = time.time()

    def close_ts(m: Dict[str, Any]) -> float:
        # close_time can be ISO string; SDK may provide close_time_ts; try a few keys
        for key in ("close_time_ts", "close_ts", "closeTimeTs", "close_time"):
            v = m.get(key)
            if v is None:
                continue
            if isinstance(v, (int, float)):
                return float(v)
            # ISO fallback (best-effort)
            if isinstance(v, str) and v:
                try:
                    # minimal ISO parsing without extra deps:
                    # expected like "2026-03-26T01:22:30Z"
                    # convert to epoch by time.strptime in UTC
                    s = v.replace("Z", "+0000")
                    # "YYYY-MM-DDTHH:MM:SS+0000"
                    t = time.strptime(s, "%Y-%m-%dT%H:%M:%S%z")
                    return float(time.mktime(t))  # note: mktime uses local; but close enough for ordering
                except Exception:
                    pass
        # if missing, push to end
        return now + 10**9

    # pick soonest-closing (i.e., current window)
    norm_sorted = sorted(norm, key=close_ts)
    return norm_sorted[0]


# =========================
# ORDERBOOK
# =========================

def get_top_of_book(client: KalshiClient, market_ticker: str) -> OrderbookTop:
    """
    Reads the market orderbook and extracts best bid/ask for YES/NO.
    Returns prices in cents or None if unavailable.
    """
    ob = client.get_market_orderbook(market_ticker=market_ticker)

    # normalize
    if not isinstance(ob, dict):
        if hasattr(ob, "model_dump"):
            ob = ob.model_dump()
        elif hasattr(ob, "to_dict"):
            ob = ob.to_dict()
        else:
            ob = {k: getattr(ob, k) for k in dir(ob) if not k.startswith("_")}

    # Kalshi orderbook typically has yes/no sides with bids/asks arrays.
    # We handle a few possible shapes defensively.
    def best_price(side_obj: Any, field: str) -> Optional[int]:
        # field in ("bids", "asks")
        if not side_obj:
            return None
        arr = side_obj.get(field) if isinstance(side_obj, dict) else None
        if not arr:
            return None
        # entries can be dicts like {"price": 54, "count": 12} or lists [price, count]
        first = arr[0]
        if isinstance(first, dict):
            p = first.get("price")
        elif isinstance(first, (list, tuple)) and first:
            p = first[0]
        else:
            p = None
        try:
            return int(p) if p is not None else None
        except Exception:
            return None

    yes = ob.get("yes") or ob.get("YES") or {}
    no = ob.get("no") or ob.get("NO") or {}

    yes_ask = best_price(yes, "asks")
    yes_bid = best_price(yes, "bids")
    no_ask = best_price(no, "asks")
    no_bid = best_price(no, "bids")

    return OrderbookTop(yes_ask=yes_ask, no_ask=no_ask, yes_bid=yes_bid, no_bid=no_bid)


# =========================
# PORTFOLIO / EQUITY
# =========================

def get_equity_cents(client: KalshiClient) -> Optional[int]:
    try:
        bal = client.get_balance()
        if isinstance(bal, dict):
            return int(bal.get("balance", 0))
        return int(getattr(bal, "balance"))
    except Exception:
        return None


def get_realized_pnl_cents_best_effort(client: KalshiClient) -> Optional[int]:
    """
    Best-effort: realized PnL is sometimes available via fills/trades endpoints depending on account + SDK version.
    If unavailable, returns None (we'll still show equity).
    """
    try:
        fills = client.get_fills(limit=50)
        if not isinstance(fills, dict):
            fills = fills.model_dump() if hasattr(fills, "model_dump") else fills.to_dict() if hasattr(fills, "to_dict") else {}
        items = fills.get("fills") or []
        realized = 0
        for f in items:
            # Not all payloads include realized PnL. If your fills include it, we’ll sum it.
            # Keys to try:
            for k in ("realized_pnl", "realized_pnl_cents", "pnl", "pnl_cents"):
                if k in f and f[k] is not None:
                    try:
                        realized += int(f[k])
                        break
                    except Exception:
                        pass
        return realized if items else None
    except Exception:
        return None


# =========================
# TRADING LOGIC
# =========================

def compute_edge_and_side(fair_prob_up: float, top: OrderbookTop) -> Tuple[Optional[str], float, Optional[int]]:
    """
    Decide whether to buy YES (UP) or NO (DOWN) based on edge vs best ask.
    Returns: (side, edge, price_cents)
      side in {"yes","no"} if tradeable else None
    """
    best = (None, 0.0, None)  # side, edge, price

    # Buy YES if fair_prob_up - yes_ask_prob is strong
    if top.yes_ask is not None:
        yes_ask_prob = top.yes_ask / 100.0
        edge_yes = fair_prob_up - yes_ask_prob
        if edge_yes > best[1]:
            best = ("yes", edge_yes, top.yes_ask)

    # Buy NO if fair_prob_down - no_ask_prob is strong
    if top.no_ask is not None:
        fair_prob_down = 1.0 - fair_prob_up
        no_ask_prob = top.no_ask / 100.0
        edge_no = fair_prob_down - no_ask_prob
        if edge_no > best[1]:
            best = ("no", edge_no, top.no_ask)

    side, edge, px = best
    if side is None or px is None:
        return (None, 0.0, None)
    if edge < MIN_EDGE:
        return (None, edge, px)

    return (side, edge, px)


def get_position_count_best_effort(client: KalshiClient, series_ticker: str) -> int:
    """
    Best-effort position cap across the series to avoid runaway.
    If endpoint shape differs, returns 0 (won't block trading).
    """
    try:
        pos = client.get_positions(limit=200)
        if not isinstance(pos, dict):
            pos = pos.model_dump() if hasattr(pos, "model_dump") else pos.to_dict() if hasattr(pos, "to_dict") else {}
        items = pos.get("positions") or []
        total = 0
        for p in items:
            mt = p.get("market_ticker") or p.get("ticker") or ""
            if isinstance(mt, str) and mt.startswith(series_ticker + "-"):
                for k in ("position", "net_position", "count"):
                    if k in p and p[k] is not None:
                        try:
                            total += abs(int(p[k]))
                            break
                        except Exception:
                            pass
        return total
    except Exception:
        return 0


def submit_buy_order(
    client: KalshiClient,
    market_ticker: str,
    side: str,
    price_cents: int,
    count: int,
) -> Dict[str, Any]:
    """
    Places a limit BUY order.
    If LIVE_MODE is false, returns a simulated response.
    """
    client_order_id = str(uuid.uuid4())

    if not LIVE_MODE:
        return {
            "simulated": True,
            "market_ticker": market_ticker,
            "side": side,
            "price": price_cents,
            "count": count,
            "client_order_id": client_order_id,
        }

    # Payload shape varies slightly across SDK versions; these keys match the v2 trade API conventions.
    # If your SDK complains, paste the exception and I’ll adjust the exact field names.
    resp = client.create_order(
        market_ticker=market_ticker,
        action="buy",
        side=side,
        type="limit",
        yes_price=price_cents if side == "yes" else None,
        no_price=price_cents if side == "no" else None,
        count=count,
        client_order_id=client_order_id,
    )

    if isinstance(resp, dict):
        return resp
    if hasattr(resp, "model_dump"):
        return resp.model_dump()
    if hasattr(resp, "to_dict"):
        return resp.to_dict()
    return {"ok": True, "client_order_id": client_order_id}


# =========================
# MAIN LOOP
# =========================

def main() -> int:
    client = build_client()
    state = BotState()

    # Boot equity snapshot
    eq = get_equity_cents(client)
    pnl = get_realized_pnl_cents_best_effort(client)
    if eq is not None:
        log.info("BOOT | kalshi_host=%s | series=%s | live=%s | equity=$%.2f%s",
                 KALSHI_HOST, SERIES_TICKER, LIVE_MODE, eq / 100.0,
                 f" | realized_pnl=$%.2f" % (pnl / 100.0) if pnl is not None else "")
    else:
        log.info("BOOT | kalshi_host=%s | series=%s | live=%s", KALSHI_HOST, SERIES_TICKER, LIVE_MODE)

    while True:
        # Optional equity heartbeat
        if EQUITY_HEARTBEAT_SECONDS and (time.time() - state.last_equity_log_ts) >= EQUITY_HEARTBEAT_SECONDS:
            eq = get_equity_cents(client)
            pnl = get_realized_pnl_cents_best_effort(client)
            if eq is not None:
                log.info("EQUITY | $%.2f%s", eq / 100.0,
                         f" | realized_pnl=$%.2f" % (pnl / 100.0) if pnl is not None else "")
            state.last_equity_log_ts = time.time()

        # Find current open market in series
        try:
            m = select_current_market(client)
        except Exception as e:
            # Keep this quiet-ish: sleep and retry
            time.sleep(POLL_SECONDS)
            continue

        if not m:
            now = time.time()
            if state.no_markets_since_ts is None:
                state.no_markets_since_ts = now

            # Fail-fast option if we're stuck too long
            if EXIT_ON_NO_MARKETS_AFTER_MIN > 0:
                mins = (now - state.no_markets_since_ts) / 60.0
                if mins >= EXIT_ON_NO_MARKETS_AFTER_MIN:
                    log.error("EXIT | No open markets for %.1f minutes (series=%s). Exiting to restart.",
                              mins, SERIES_TICKER)
                    return 2

            # Backoff (long sleep)
            time.sleep(state.no_markets_backoff)
            state.no_markets_backoff = min(NO_MARKETS_BACKOFF_MAX, state.no_markets_backoff * 1.5)
            continue

        # reset no-market backoff
        state.no_markets_since_ts = None
        state.no_markets_backoff = NO_MARKETS_BACKOFF_START

        market_ticker = m.get("ticker") or m.get("market_ticker") or m.get("marketTicker")
        if not market_ticker:
            time.sleep(POLL_SECONDS)
            continue

        state.current_market_ticker = str(market_ticker)

        # Read your fair probability input
        fair_prob_up = read_fair_prob_up()
        if fair_prob_up is None:
            # If your model hasn't published yet, just sleep quietly
            time.sleep(POLL_SECONDS)
            continue

        # Pull orderbook top
        try:
            top = get_top_of_book(client, state.current_market_ticker)
        except Exception:
            time.sleep(POLL_SECONDS)
            continue

        side, edge, px = compute_edge_and_side(fair_prob_up, top)
        if side is None or px is None:
            time.sleep(POLL_SECONDS)
            continue

        # Risk cap (simple)
        total_pos = get_position_count_best_effort(client, SERIES_TICKER)
        if total_pos >= MAX_TOTAL_POSITION_CONTRACTS:
            time.sleep(POLL_SECONDS)
            continue

        # Size
        count = min(MAX_CONTRACTS_PER_TRADE, max(1, MAX_TOTAL_POSITION_CONTRACTS - total_pos))

        # Submit order
        try:
            resp = submit_buy_order(client, state.current_market_ticker, side, px, count)
        except Exception as e:
            # If live order fails, surface it (this matters)
            log.error("ORDER_FAIL | ticker=%s side=%s px=%s count=%s edge=%.4f err=%s",
                      state.current_market_ticker, side, px, count, edge, e)
            time.sleep(POLL_SECONDS)
            continue

        # Post-trade equity snapshot (this is what you asked for)
        eq = get_equity_cents(client)
        pnl = get_realized_pnl_cents_best_effort(client)

        log.info(
            "TRADE | ticker=%s | side=%s | px=%sc | count=%s | edge=%.4f | fair_up=%.4f | equity=%s%s | resp=%s",
            state.current_market_ticker,
            side,
            px,
            count,
            edge,
            fair_prob_up,
            (f"${eq/100.0:.2f}" if eq is not None else "n/a"),
            (f" | realized_pnl=${pnl/100.0:.2f}" if pnl is not None else ""),
            ("SIM" if resp.get("simulated") else "OK"),
        )

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)

#!/usr/bin/env python3
"""
Johnny 5 — Kalshi-only trading bot (quiet logs + long backoff)

Goals:
- ONLY Kalshi (no Gamma/Polymarket references)
- Long backoff when no open markets found (avoid tight loops)
- Optional hard-stop after N consecutive "no markets" cycles (so container can restart)
- Logs basically ONLY when a trade is made, showing equity + pnl

Kalshi API uses:
- Headers: KALSHI-ACCESS-KEY / KALSHI-ACCESS-SIGNATURE / KALSHI-ACCESS-TIMESTAMP (ms)  (docs)
- Create Order: POST /trade-api/v2/portfolio/orders (docs)
"""

from __future__ import annotations

import os
import time
import json
import uuid
import base64
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

# Crypto for RSA-PSS signing
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


# =========================
# CONFIG (ENV VARS)
# =========================

# Kalshi base URL (docs show elections subdomain for trade api)
KALSHI_BASE_URL = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com")

# Market discovery:
# Set this to the prefix of the market tickers you want to trade.
# Example idea: "KXBTC15M" -> matches "KXBTC15M-20260302-0300" style tickers.
MARKET_PREFIX = os.getenv("MARKET_PREFIX", "KXBTC15M")

# Polling / backoff:
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "10"))  # normal poll when things are healthy
LONG_SLEEP_SECONDS = int(os.getenv("LONG_SLEEP_SECONDS", "900"))  # 15 min backoff when no markets
STOP_AFTER_NO_MARKETS = int(os.getenv("STOP_AFTER_NO_MARKETS", "20"))  # exit after N consecutive no-market checks

# Trading:
LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() in ("1", "true", "yes")
MAX_ORDER_COUNT = int(os.getenv("MAX_ORDER_COUNT", "2"))  # contracts per order (simple cap)
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.02"))  # placeholder edge threshold (2%)

# Auth (REQUIRED):
KALSHI_KEY_ID = os.getenv("KALSHI_KEY_ID", "").strip()
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip()

# Optional: Subaccount
SUBACCOUNT = os.getenv("KALSHI_SUBACCOUNT", "").strip()  # e.g. "0" or empty

# Timeouts
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "15"))


# =========================
# LOGGING (QUIET)
# =========================
# Only emit INFO logs for actual trades. Everything else: WARNING/ERROR.
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.WARNING),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("johnny5")


def _now() -> float:
    return time.time()


def _sleep(sec: float) -> None:
    time.sleep(max(0.0, sec))


# =========================
# KALSHI CLIENT
# =========================

class KalshiClient:
    """
    Minimal Kalshi Trade API v2 client with RSA-PSS request signing.
    """

    def __init__(self, base_url: str, key_id: str, private_key_pem: bytes):
        self.base_url = base_url.rstrip("/")
        self.key_id = key_id
        self.private_key = serialization.load_pem_private_key(private_key_pem, password=None)

        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def _sign(self, timestamp_ms: str, method: str, path: str, body: str) -> str:
        """
        Kalshi signature: RSA-PSS over a canonical message.
        We use a common canonical form: "{timestamp}{method}{path}{body}"
        (This matches the standard approach described in Kalshi docs examples.)
        """
        msg = (timestamp_ms + method.upper() + path + body).encode("utf-8")

        sig = self.private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, json_body: Optional[Dict[str, Any]] = None) -> Any:
        ts_ms = str(int(_now() * 1000))
        body_str = "" if json_body is None else json.dumps(json_body, separators=(",", ":"), ensure_ascii=False)

        signature = self._sign(ts_ms, method, path, body_str)

        headers = {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        }

        url = f"{self.base_url}{path}"

        r = self.session.request(
            method=method.upper(),
            url=url,
            params=params or None,
            data=None if json_body is None else body_str,
            headers=headers,
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()

    # ---- API wrappers ----

    def get_balance(self) -> Dict[str, Any]:
        return self._request("GET", "/trade-api/v2/portfolio/balance")

    def get_markets(self, status: str = "open", limit: int = 200, cursor: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"status": status, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return self._request("GET", "/trade-api/v2/markets", params=params)

    def create_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/trade-api/v2/portfolio/orders", json_body=payload)


# =========================
# DATA MODELS
# =========================

@dataclass
class Market:
    ticker: str
    status: str
    raw: Dict[str, Any]


# =========================
# MARKET DISCOVERY / PICK
# =========================

def fetch_open_markets_matching_prefix(client: KalshiClient, prefix: str) -> List[Market]:
    """
    Pull open markets, paginate, filter by ticker prefix.
    """
    out: List[Market] = []
    cursor: Optional[str] = None

    # Keep pagination bounded to avoid runaway loops
    for _ in range(20):
        data = client.get_markets(status="open", limit=200, cursor=cursor)
        items = data.get("markets") or data.get("data") or []
        for m in items:
            t = m.get("ticker") or ""
            if t.startswith(prefix):
                out.append(Market(ticker=t, status=m.get("status", "open"), raw=m))
        cursor = data.get("cursor") or data.get("next_cursor")
        if not cursor:
            break

    return out


def pick_current_market(markets: List[Market]) -> Optional[Market]:
    """
    Simple selection:
    - pick the lexicographically greatest ticker (often embeds time)
    """
    if not markets:
        return None
    return sorted(markets, key=lambda m: m.ticker)[-1]


# =========================
# STRATEGY (PLACEHOLDER)
# =========================

def compute_signal(market: Market) -> Tuple[str, Dict[str, Any]]:
    """
    Replace this with your real strategy.
    Return:
      ("NO_TRADE", {...}) OR ("BUY_YES"/"BUY_NO"/"SELL_YES"/"SELL_NO", {...})

    Right now: placeholder => never trades.
    """
    return ("NO_TRADE", {"reason": "strategy_placeholder"})


def build_order_payload(market_ticker: str, action: str, side: str, count: int) -> Dict[str, Any]:
    """
    Minimal order payload for Kalshi Create Order.

    We use market orders indirectly by setting a price; but since we don’t have
    your pricing logic here, we leave price out of the placeholder trade flow.
    When you wire your strategy, you will set yes_price/no_price.

    Docs: POST /trade-api/v2/portfolio/orders
    """
    payload: Dict[str, Any] = {
        "ticker": market_ticker,
        "action": action,   # "buy" or "sell"
        "side": side,       # "yes" or "no"
        "count": int(count),
        "client_order_id": str(uuid.uuid4()),
        # You should set one of:
        #  - "yes_price": int cents 1..99
        #  - "no_price": int cents 1..99
        #
        # Example:
        # "yes_price": 54
        # (meaning $0.54)
        #
        # And optionally:
        # "time_in_force": "fill_or_kill" / "good_til_cancelled" etc.
    }
    if SUBACCOUNT:
        try:
            payload["subaccount"] = int(SUBACCOUNT)
        except Exception:
            pass
    return payload


# =========================
# MAIN
# =========================

def dollars_from_balance_resp(balance_resp: Dict[str, Any]) -> float:
    """
    Balance response shape can vary; we defensively try common fields.
    Many Kalshi money fields are in cents.
    """
    # Common possibilities:
    # balance_resp["balance"]["available_cash"] or similar
    b = balance_resp.get("balance") if isinstance(balance_resp.get("balance"), dict) else balance_resp

    for key in ("equity", "balance", "available_cash", "cash_balance", "available_balance"):
        v = b.get(key)
        if v is None:
            continue
        # if already float dollars
        if isinstance(v, float):
            return float(v)
        # if int cents
        if isinstance(v, int):
            return v / 100.0
        # if string
        if isinstance(v, str):
            try:
                # could be dollars or cents; assume dollars if it has a decimal point
                if "." in v:
                    return float(v)
                return int(v) / 100.0
            except Exception:
                continue

    # If nothing matched, return 0
    return 0.0


def run() -> None:
    if not KALSHI_KEY_ID or not KALSHI_PRIVATE_KEY_PATH:
        raise SystemExit(
            "Missing required env vars: KALSHI_KEY_ID and/or KALSHI_PRIVATE_KEY_PATH"
        )

    with open(KALSHI_PRIVATE_KEY_PATH, "rb") as f:
        private_key_pem = f.read()

    client = KalshiClient(
        base_url=KALSHI_BASE_URL,
        key_id=KALSHI_KEY_ID,
        private_key_pem=private_key_pem,
    )

    # One boot line (you can silence this too by setting LOG_LEVEL=ERROR)
    log.warning("BOOT: Johnny 5 starting | kalshi=%s | prefix=%s | live=%s", KALSHI_BASE_URL, MARKET_PREFIX, LIVE_MODE)

    # Track equity baseline for simple PnL
    try:
        start_bal = client.get_balance()
        start_equity = dollars_from_balance_resp(start_bal)
    except Exception:
        start_equity = 0.0

    consecutive_no_markets = 0
    current_ticker: Optional[str] = None

    while True:
        try:
            markets = fetch_open_markets_matching_prefix(client, MARKET_PREFIX)
        except Exception as e:
            # Quiet by default; treat as "no markets" and long sleep
            consecutive_no_markets += 1
            if consecutive_no_markets >= STOP_AFTER_NO_MARKETS:
                raise SystemExit(f"Exiting after repeated failures fetching markets ({consecutive_no_markets}). Last error: {e}")
            _sleep(LONG_SLEEP_SECONDS)
            continue

        if not markets:
            consecutive_no_markets += 1
            if consecutive_no_markets >= STOP_AFTER_NO_MARKETS:
                raise SystemExit(f"Exiting: no open markets for prefix={MARKET_PREFIX} after {consecutive_no_markets} checks.")
            _sleep(LONG_SLEEP_SECONDS)
            continue

        # healthy
        consecutive_no_markets = 0

        target = pick_current_market(markets)
        if target is None:
            _sleep(LONG_SLEEP_SECONDS)
            continue

        if current_ticker != target.ticker:
            current_ticker = target.ticker
            # No log here (keeps it quiet)

        # Strategy decision
        signal, dbg = compute_signal(target)

        if signal == "NO_TRADE":
            _sleep(POLL_SECONDS)
            continue

        # Map signal -> order
        # Expected formats: BUY_YES, BUY_NO, SELL_YES, SELL_NO
        try:
            action, side = signal.split("_", 1)
            action = action.lower()  # buy/sell
            side = side.lower()      # yes/no
            if action not in ("buy", "sell") or side not in ("yes", "no"):
                raise ValueError("bad signal")
        except Exception:
            # If signal is malformed, just skip quietly
            _sleep(POLL_SECONDS)
            continue

        count = min(MAX_ORDER_COUNT, int(dbg.get("count", MAX_ORDER_COUNT)))
        payload = build_order_payload(target.ticker, action=action, side=side, count=count)

        # IMPORTANT: you MUST set yes_price/no_price in dbg or inside compute_signal()
        # before live trading can work.
        if side == "yes" and "yes_price" in dbg:
            payload["yes_price"] = int(dbg["yes_price"])
        elif side == "no" and "no_price" in dbg:
            payload["no_price"] = int(dbg["no_price"])
        else:
            # no price => skip (prevents accidental market-like behavior)
            _sleep(POLL_SECONDS)
            continue

        # Execute
        if LIVE_MODE:
            resp = client.create_order(payload)
            mode = "LIVE"
        else:
            resp = {"paper": True, "payload": payload}
            mode = "PAPER"

        # Balance + PnL snapshot (logged ONLY when trade happens)
        try:
            bal = client.get_balance()
            equity = dollars_from_balance_resp(bal)
        except Exception:
            equity = 0.0

        pnl = equity - start_equity if start_equity else 0.0

        log.info(
            "TRADE | mode=%s | ticker=%s | %s_%s | count=%s | px=%s | equity=%.2f | pnl=%.2f",
            mode,
            target.ticker,
            action.upper(),
            side.upper(),
            payload.get("count"),
            dbg.get("yes_price") if side == "yes" else dbg.get("no_price"),
            equity,
            pnl,
        )

        _sleep(POLL_SECONDS)


if __name__ == "__main__":
    run()

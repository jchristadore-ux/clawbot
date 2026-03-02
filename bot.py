#!/usr/bin/env python3
from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


# ---------- Runtime config ----------
KALSHI_BASE_URL = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/")
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
KALSHI_PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY_PEM", "").strip()
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip()
SERIES_TICKER = os.getenv("SERIES_TICKER", "KXBTC15M").strip()

LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() == "true"
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "8"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))

START_EQUITY = float(os.getenv("START_EQUITY", "1000"))
RISK_FRACTION = float(os.getenv("RISK_FRACTION", "0.08"))
MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "150"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "50"))

EDGE_ENTER = float(os.getenv("EDGE_ENTER", "0.07"))
EDGE_EXIT = float(os.getenv("EDGE_EXIT", "0.02"))
LOOKBACK = int(os.getenv("LOOKBACK", "20"))
MIN_CONVICTION_Z = float(os.getenv("MIN_CONVICTION_Z", "1.0"))
FAIR_MOVE_SCALE = float(os.getenv("FAIR_MOVE_SCALE", "0.15"))

STATUS_FILE = Path(os.getenv("STATUS_FILE", ".runtime/status.json"))
STATE_FILE = Path(os.getenv("STATE_FILE", ".runtime/state.json"))
KRAKEN_TICKER_URL = os.getenv(
    "KRAKEN_TICKER_URL", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"
)


# ---------- Logging: only trades/errors ----------
logging.basicConfig(level=logging.WARNING, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("kalshi-bot")


@dataclass
class BotState:
    equity: float = START_EQUITY
    realized_pnl: float = 0.0
    daily_realized_pnl: float = 0.0
    trades: int = 0
    last_day: str = ""
    position_side: Optional[str] = None  # YES/NO
    position_entry_prob: Optional[float] = None
    position_contracts: int = 0
    market_ticker: Optional[str] = None


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_runtime_dir() -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)


def load_state() -> BotState:
    if not STATE_FILE.exists():
        return BotState(last_day=datetime.now(timezone.utc).date().isoformat())
    raw = json.loads(STATE_FILE.read_text())
    return BotState(**raw)


def save_state(state: BotState) -> None:
    STATE_FILE.write_text(json.dumps(asdict(state), indent=2))


def write_status(state: BotState, message: str, fair_yes: float, mark_yes: float, edge: float) -> None:
    unrealized = 0.0
    if state.position_side == "YES" and state.position_entry_prob is not None:
        unrealized = (mark_yes - state.position_entry_prob) * state.position_contracts
    elif state.position_side == "NO" and state.position_entry_prob is not None:
        unrealized = ((1 - mark_yes) - state.position_entry_prob) * state.position_contracts

    payload = {
        "ts": utc_iso(),
        "message": message,
        "live_mode": LIVE_MODE,
        "equity": round(state.equity + unrealized, 4),
        "cash": round(state.equity, 4),
        "realized_pnl": round(state.realized_pnl, 4),
        "daily_realized_pnl": round(state.daily_realized_pnl, 4),
        "position": {
            "side": state.position_side,
            "entry_prob": state.position_entry_prob,
            "contracts": state.position_contracts,
            "market_ticker": state.market_ticker,
        },
        "model": {"fair_yes": round(fair_yes, 6), "mark_yes": round(mark_yes, 6), "edge": round(edge, 6)},
    }
    STATUS_FILE.write_text(json.dumps(payload, indent=2))


class KalshiClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.private_key = None

        pem = self._resolve_private_key_material()
        if pem:
            self.private_key = serialization.load_pem_private_key(pem.encode(), password=None)

    @staticmethod
    def _resolve_private_key_material() -> str:
        # Preferred: direct PEM in env var.
        if KALSHI_PRIVATE_KEY_PEM.strip():
            return KALSHI_PRIVATE_KEY_PEM.strip()

        raw = KALSHI_PRIVATE_KEY_PATH.strip()
        if not raw:
            return ""

        # Support users accidentally putting PEM/base64 in *_PATH env.
        if "BEGIN" in raw:
            return raw

        # Try file path first, but guard against giant non-path strings.
        try:
            p = Path(raw)
            if len(raw) < 512 and p.exists():
                return p.read_text()
        except OSError:
            pass

        # Try base64 encoded PEM fallback.
        try:
            decoded = base64.b64decode(raw, validate=True).decode("utf-8")
            if "BEGIN" in decoded:
                return decoded
        except (binascii.Error, UnicodeDecodeError):
            pass

        return ""

    def _headers(self, method: str, path: str) -> dict[str, str]:
        if not (KALSHI_API_KEY_ID and self.private_key):
            return {}
        ts_ms = str(int(time.time() * 1000))
        msg = f"{ts_ms}{method.upper()}{path}".encode()
        sig = self.private_key.sign(msg, padding.PKCS1v15(), hashes.SHA256()).hex()
        return {
            "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": sig,
        }

    def _request(self, method: str, path: str, payload: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        headers = self._headers(method, path)
        url = f"{KALSHI_BASE_URL}{path}"
        response = self.session.request(
            method=method.upper(),
            url=url,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json()

    def get_open_market(self) -> dict[str, Any]:
        data = self._request("GET", f"/trade-api/v2/markets?series_ticker={SERIES_TICKER}&status=open")
        markets = data.get("markets", [])
        if not markets:
            raise RuntimeError(f"No open markets for {SERIES_TICKER}")
        return markets[0]

    def get_orderbook(self, ticker: str) -> dict[str, Any]:
        return self._request("GET", f"/trade-api/v2/markets/{ticker}/orderbook")

    def place_order(self, ticker: str, side: str, contracts: int, price_cents: int) -> dict[str, Any]:
        if not LIVE_MODE:
            return {"ok": True, "paper": True}
        action = "buy"
        payload = {
            "ticker": ticker,
            "action": action,
            "side": side.lower(),
            "count": contracts,
            "type": "limit",
            "yes_price": price_cents if side == "YES" else None,
            "no_price": price_cents if side == "NO" else None,
            "client_order_id": f"bot-{int(time.time())}",
        }
        return self._request("POST", "/trade-api/v2/portfolio/orders", payload)


def kraken_last_price() -> float:
    resp = requests.get(KRAKEN_TICKER_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    resp.raise_for_status()
    body = resp.json()
    result = body.get("result", {})
    if not result:
        raise RuntimeError("Kraken price missing")
    key = next(iter(result))
    return float(result[key]["c"][0])


def mark_yes_from_orderbook(orderbook: dict[str, Any]) -> float:
    ob = orderbook.get("orderbook", orderbook)
    yes_bids = ob.get("yes", []) or ob.get("yes_bids", [])
    no_bids = ob.get("no", []) or ob.get("no_bids", [])
    if not yes_bids or not no_bids:
        raise RuntimeError("Orderbook missing yes/no bids")

    best_yes_bid = int(yes_bids[0][0] if isinstance(yes_bids[0], list) else yes_bids[0].get("price"))
    best_no_bid = int(no_bids[0][0] if isinstance(no_bids[0], list) else no_bids[0].get("price"))
    implied_yes_ask = 100 - best_no_bid
    mid_yes = (best_yes_bid + implied_yes_ask) / 2
    return max(0.01, min(0.99, mid_yes / 100.0))


def model_fair_yes(prices: list[float]) -> tuple[float, float]:
    if len(prices) < LOOKBACK:
        return 0.5, 0.0
    window = prices[-LOOKBACK:]
    rets = []
    for i in range(1, len(window)):
        r = (window[i] - window[i - 1]) / window[i - 1]
        rets.append(r)
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, (len(rets) - 1))
    std = max(var**0.5, 1e-8)
    z = (rets[-1] - mean) / std
    fair = max(0.01, min(0.99, 0.5 + z * FAIR_MOVE_SCALE))
    return fair, z


def compute_contracts(equity: float, price_prob: float) -> int:
    usd = min(MAX_POSITION_USD, max(5.0, equity * RISK_FRACTION))
    return max(1, int(usd / max(price_prob, 0.01)))


def trade_log(event: str, state: BotState, market_price: float, fair_price: float, edge: float) -> None:
    unrealized = 0.0
    if state.position_side == "YES" and state.position_entry_prob is not None:
        unrealized = (market_price - state.position_entry_prob) * state.position_contracts
    elif state.position_side == "NO" and state.position_entry_prob is not None:
        unrealized = ((1 - market_price) - state.position_entry_prob) * state.position_contracts
    equity = state.equity + unrealized
    print(
        json.dumps(
            {
                "ts": utc_iso(),
                "event": event,
                "equity": round(equity, 4),
                "cash": round(state.equity, 4),
                "realized_pnl": round(state.realized_pnl, 4),
                "daily_realized_pnl": round(state.daily_realized_pnl, 4),
                "position": {
                    "side": state.position_side,
                    "contracts": state.position_contracts,
                    "entry_prob": state.position_entry_prob,
                    "market_ticker": state.market_ticker,
                },
                "market_yes": round(market_price, 6),
                "fair_yes": round(fair_price, 6),
                "edge": round(edge, 6),
                "live": LIVE_MODE,
            }
        ),
        flush=True,
    )


def rollover_day(state: BotState) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    if state.last_day != today:
        state.last_day = today
        state.daily_realized_pnl = 0.0


def maybe_enter(state: BotState, ticker: str, fair_yes: float, mark_yes: float, z: float, client: KalshiClient) -> bool:
    if state.position_side is not None:
        return False
    if state.daily_realized_pnl <= -abs(MAX_DAILY_LOSS):
        return False

    edge = fair_yes - mark_yes
    conviction = abs(z) >= MIN_CONVICTION_Z
    side = None
    if conviction and edge >= EDGE_ENTER:
        side = "YES"
    elif conviction and edge <= -EDGE_ENTER:
        side = "NO"

    if side is None:
        return False

    price = mark_yes if side == "YES" else (1 - mark_yes)
    contracts = compute_contracts(state.equity, price)
    cents = int(round(price * 100))
    client.place_order(ticker=ticker, side=side, contracts=contracts, price_cents=max(1, min(99, cents)))

    state.position_side = side
    state.position_entry_prob = price
    state.position_contracts = contracts
    state.market_ticker = ticker
    state.trades += 1
    trade_log("ENTER", state, mark_yes, fair_yes, edge)
    return True


def maybe_exit(state: BotState, fair_yes: float, mark_yes: float, client: KalshiClient) -> bool:
    if state.position_side is None or state.position_entry_prob is None:
        return False

    edge = fair_yes - mark_yes
    should_exit = False
    if state.position_side == "YES":
        should_exit = edge <= EDGE_EXIT
        pnl = (mark_yes - state.position_entry_prob) * state.position_contracts
        exit_price = mark_yes
    else:
        should_exit = edge >= -EDGE_EXIT
        pnl = ((1 - mark_yes) - state.position_entry_prob) * state.position_contracts
        exit_price = 1 - mark_yes

    if not should_exit:
        return False

    cents = int(round(exit_price * 100))
    client.place_order(
        ticker=state.market_ticker or SERIES_TICKER,
        side="NO" if state.position_side == "YES" else "YES",
        contracts=state.position_contracts,
        price_cents=max(1, min(99, cents)),
    )

    state.equity += pnl
    state.realized_pnl += pnl
    state.daily_realized_pnl += pnl
    state.position_side = None
    state.position_entry_prob = None
    state.position_contracts = 0
    state.market_ticker = None
    state.trades += 1
    trade_log("EXIT", state, mark_yes, fair_yes, edge)
    return True


def main() -> None:
    ensure_runtime_dir()
    state = load_state()
    client = KalshiClient()
    prices: list[float] = []

    while True:
        try:
            rollover_day(state)
            prices.append(kraken_last_price())
            if len(prices) > LOOKBACK * 5:
                prices = prices[-LOOKBACK * 5 :]

            market = client.get_open_market()
            ticker = market.get("ticker")
            if not ticker:
                raise RuntimeError("Open market missing ticker")

            mark_yes = mark_yes_from_orderbook(client.get_orderbook(ticker))
            fair_yes, z = model_fair_yes(prices)
            edge = fair_yes - mark_yes

            did_trade = maybe_exit(state, fair_yes, mark_yes, client)
            if not did_trade:
                maybe_enter(state, ticker, fair_yes, mark_yes, z, client)

            write_status(state, message="running", fair_yes=fair_yes, mark_yes=mark_yes, edge=edge)
            save_state(state)
            time.sleep(POLL_SECONDS)
        except KeyboardInterrupt:
            return
        except Exception as exc:
            log.error("loop error: %s", exc)
            write_status(state, message=f"error: {exc}", fair_yes=0.5, mark_yes=0.5, edge=0.0)
            save_state(state)
            time.sleep(max(POLL_SECONDS, 10))


if __name__ == "__main__":
    main()

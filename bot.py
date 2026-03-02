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
from typing import Any, Optional, Tuple

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

print("BOOT: bot.py started", flush=True)

# =============================================================================
# Safe env helpers (fixes Railway blank var crashes)
# =============================================================================

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v != "" else default

def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return int(float(v))  # tolerate "8.0"

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return float(v)


# =============================================================================
# Runtime config
# =============================================================================

KALSHI_BASE_URL = env_str("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/")
KALSHI_API_KEY_ID = env_str("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PEM = env_str("KALSHI_PRIVATE_KEY_PEM", "")
KALSHI_PRIVATE_KEY_PATH = env_str("KALSHI_PRIVATE_KEY_PATH", "")
SERIES_TICKER = env_str("SERIES_TICKER", "KXBTC15M")

LIVE_MODE = env_bool("LIVE_MODE", False)

POLL_SECONDS = env_float("POLL_SECONDS", 8.0)
REQUEST_TIMEOUT_SECONDS = env_int("REQUEST_TIMEOUT_SECONDS", 15)

START_EQUITY = env_float("START_EQUITY", 1000.0)
RISK_FRACTION = env_float("RISK_FRACTION", 0.08)
MAX_POSITION_USD = env_float("MAX_POSITION_USD", 150.0)
MAX_DAILY_LOSS = env_float("MAX_DAILY_LOSS", 50.0)

EDGE_ENTER = env_float("EDGE_ENTER", 0.07)
EDGE_EXIT = env_float("EDGE_EXIT", 0.02)
LOOKBACK = env_int("LOOKBACK", 20)
MIN_CONVICTION_Z = env_float("MIN_CONVICTION_Z", 1.0)
FAIR_MOVE_SCALE = env_float("FAIR_MOVE_SCALE", 0.15)

STATUS_FILE = Path(env_str("STATUS_FILE", ".runtime/status.json"))
STATE_FILE = Path(env_str("STATE_FILE", ".runtime/state.json"))

KRAKEN_TICKER_URL = env_str("KRAKEN_TICKER_URL", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD")


# =============================================================================
# Logging: only trades + errors
# =============================================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("johnny5")

def trade_log_json(payload: dict) -> None:
    print(json.dumps(payload), flush=True)


# =============================================================================
# State
# =============================================================================

@dataclass
class BotState:
    equity: float = START_EQUITY
    realized_pnl: float = 0.0
    daily_realized_pnl: float = 0.0
    trades: int = 0
    last_day: str = ""
    position_side: Optional[str] = None  # "YES" or "NO"
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
        s = BotState()
        s.last_day = datetime.now(timezone.utc).date().isoformat()
        return s
    raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    s = BotState(**raw)
    if not s.last_day:
        s.last_day = datetime.now(timezone.utc).date().isoformat()
    return s


def save_state(state: BotState) -> None:
    STATE_FILE.write_text(json.dumps(asdict(state), indent=2), encoding="utf-8")


def rollover_day(state: BotState) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    if state.last_day != today:
        state.last_day = today
        state.daily_realized_pnl = 0.0


def write_status(state: BotState, message: str, fair_yes: float, mark_yes: float, edge: float) -> None:
    unrealized = 0.0
    if state.position_side == "YES" and state.position_entry_prob is not None:
        unrealized = (mark_yes - state.position_entry_prob) * state.position_contracts
    elif state.position_side == "NO" and state.position_entry_prob is not None:
        unrealized = ((1.0 - mark_yes) - state.position_entry_prob) * state.position_contracts

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
    STATUS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# =============================================================================
# Kalshi client
# =============================================================================

class KalshiClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.private_key = None

        pem = self._resolve_private_key_material()
        if pem:
            try:
                self.private_key = serialization.load_pem_private_key(
                    pem.encode("utf-8"),
                    password=None,
                )
            except Exception as e:
                print(f"BOOT_WARN: could not load private key: {e}", flush=True)
                self.private_key = None
        else:
            print("BOOT_WARN: no private key found", flush=True)

    @staticmethod
    def _resolve_private_key_material() -> str:
        raw_pem = KALSHI_PRIVATE_KEY_PEM.strip()

        if raw_pem:
            raw_pem = raw_pem.replace("\\n", "\n").strip().strip('"').strip("'")
            return raw_pem

        raw = KALSHI_PRIVATE_KEY_PATH.strip()
        if not raw:
            return ""

        if "BEGIN" in raw:
            return raw.replace("\\n", "\n").strip().strip('"').strip("'")

        try:
            p = Path(raw)
            if len(raw) < 512 and p.exists():
                txt = p.read_text(encoding="utf-8")
                return txt.replace("\\n", "\n").strip().strip('"').strip("'")
        except OSError:
            pass

        try:
            decoded = base64.b64decode(raw, validate=True).decode("utf-8")
            decoded = decoded.replace("\\n", "\n").strip().strip('"').strip("'")
            return decoded
        except (binascii.Error, UnicodeDecodeError):
            pass

        return ""

    def _headers(self, method: str, path: str) -> dict[str, str]:
        if not (KALSHI_API_KEY_ID and self.private_key):
            return {}

        ts_ms = str(int(time.time() * 1000))
        msg = f"{ts_ms}{method.upper()}{path}".encode("utf-8")

        sig = self.private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )

        sig_b64 = base64.b64encode(sig).decode("utf-8")

        return {
            "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": sig_b64,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, payload: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        headers = self._headers(method, path)
        url = f"{KALSHI_BASE_URL}{path}"

        r = self.session.request(
            method=method.upper(),
            url=url,
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        r.raise_for_status()
        return r.json()
# =============================================================================
# Market data
# =============================================================================

def kraken_last_price() -> float:
    r = requests.get(KRAKEN_TICKER_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    r.raise_for_status()
    body = r.json()
    result = body.get("result", {})
    if not result:
        raise RuntimeError("Kraken price missing")
    key = next(iter(result))
    return float(result[key]["c"][0])


def mark_yes_from_orderbook(orderbook: dict[str, Any]) -> float:
    # Kalshi orderbook typically returns bids; we use YES best bid and implied YES ask = 100 - NO best bid
    ob = orderbook.get("orderbook", orderbook)
    yes_bids = ob.get("yes_bids") or ob.get("yes") or []
    no_bids = ob.get("no_bids") or ob.get("no") or []
    if not yes_bids or not no_bids:
        raise RuntimeError("Orderbook missing yes/no bids")

    def price_of(level: Any) -> int:
        if isinstance(level, list) and len(level) >= 1:
            return int(level[0])
        if isinstance(level, dict) and "price" in level:
            return int(level["price"])
        raise RuntimeError("Bad orderbook level shape")

    best_yes_bid = price_of(yes_bids[0])
    best_no_bid = price_of(no_bids[0])

    implied_yes_ask = 100 - best_no_bid
    mid_yes = (best_yes_bid + implied_yes_ask) / 2.0
    return max(0.01, min(0.99, mid_yes / 100.0))


def model_fair_yes(prices: list[float]) -> Tuple[float, float]:
    if len(prices) < LOOKBACK:
        return 0.5, 0.0

    window = prices[-LOOKBACK:]
    rets = []
    for i in range(1, len(window)):
        prev = window[i - 1]
        cur = window[i]
        if prev <= 0:
            continue
        rets.append((cur - prev) / prev)

    if len(rets) < 2:
        return 0.5, 0.0

    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, (len(rets) - 1))
    std = max(var ** 0.5, 1e-8)
    z = (rets[-1] - mean) / std

    fair = 0.5 + z * FAIR_MOVE_SCALE
    fair = max(0.01, min(0.99, fair))
    return fair, z


def compute_contracts(equity: float, price_prob: float) -> int:
    usd = min(MAX_POSITION_USD, max(5.0, equity * RISK_FRACTION))
    return max(1, int(usd / max(price_prob, 0.01)))


# =============================================================================
# Trading logic
# =============================================================================

def emit_trade(event: str, state: BotState, mark_yes: float, fair_yes: float, edge: float, note: str = "") -> None:
    unrealized = 0.0
    if state.position_side == "YES" and state.position_entry_prob is not None:
        unrealized = (mark_yes - state.position_entry_prob) * state.position_contracts
    elif state.position_side == "NO" and state.position_entry_prob is not None:
        unrealized = ((1.0 - mark_yes) - state.position_entry_prob) * state.position_contracts

    trade_log_json(
        {
            "ts": utc_iso(),
            "event": event,
            "note": note,
            "live": LIVE_MODE,
            "equity": round(state.equity + unrealized, 4),
            "cash": round(state.equity, 4),
            "realized_pnl": round(state.realized_pnl, 4),
            "daily_realized_pnl": round(state.daily_realized_pnl, 4),
            "position": {
                "side": state.position_side,
                "contracts": state.position_contracts,
                "entry_prob": state.position_entry_prob,
                "market_ticker": state.market_ticker,
            },
            "market_yes": round(mark_yes, 6),
            "fair_yes": round(fair_yes, 6),
            "edge": round(edge, 6),
        }
    )


def maybe_enter(state: BotState, ticker: str, fair_yes: float, mark_yes: float, z: float, client: KalshiClient) -> bool:
    if state.position_side is not None:
        return False
    if state.daily_realized_pnl <= -abs(MAX_DAILY_LOSS):
        return False

    edge = fair_yes - mark_yes
    conviction = abs(z) >= MIN_CONVICTION_Z

    side: Optional[str] = None
    if conviction and edge >= EDGE_ENTER:
        side = "YES"
    elif conviction and edge <= -EDGE_ENTER:
        side = "NO"

    if side is None:
        return False

    price = mark_yes if side == "YES" else (1.0 - mark_yes)
    contracts = compute_contracts(state.equity, price)
    cents = max(1, min(99, int(round(price * 100))))

    client.place_order_buy(ticker=ticker, side=side, contracts=contracts, price_cents=cents)

    state.position_side = side
    state.position_entry_prob = price
    state.position_contracts = contracts
    state.market_ticker = ticker
    state.trades += 1

    emit_trade("ENTER", state, mark_yes, fair_yes, edge)
    return True


def maybe_exit(state: BotState, fair_yes: float, mark_yes: float, client: KalshiClient) -> bool:
    if state.position_side is None or state.position_entry_prob is None:
        return False

    edge = fair_yes - mark_yes

    if state.position_side == "YES":
        should_exit = edge <= EDGE_EXIT
        pnl = (mark_yes - state.position_entry_prob) * state.position_contracts
        close_price = mark_yes
        close_side = "NO"  # hedge-close (buy opposite) since this simple bot only uses buy actions
    else:
        should_exit = edge >= -EDGE_EXIT
        pnl = ((1.0 - mark_yes) - state.position_entry_prob) * state.position_contracts
        close_price = 1.0 - mark_yes
        close_side = "YES"

    if not should_exit:
        return False

    cents = max(1, min(99, int(round(close_price * 100))))
    client.place_order_buy(
        ticker=state.market_ticker or SERIES_TICKER,
        side=close_side,
        contracts=state.position_contracts,
        price_cents=cents,
    )

    state.equity += pnl
    state.realized_pnl += pnl
    state.daily_realized_pnl += pnl

    state.position_side = None
    state.position_entry_prob = None
    state.position_contracts = 0
    state.market_ticker = None
    state.trades += 1

    emit_trade("EXIT", state, mark_yes, fair_yes, edge, note=f"pnl={pnl:.4f}")
    return True


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    ensure_runtime_dir()
    state = load_state()
    client = KalshiClient()
    prices: list[float] = []

    # One boot line (not spammy, but helps you confirm env)
    log.warning(
        "BOOT | live=%s series=%s poll=%.1fs timeout=%ss state=%s status=%s",
        LIVE_MODE, SERIES_TICKER, POLL_SECONDS, REQUEST_TIMEOUT_SECONDS, str(STATE_FILE), str(STATUS_FILE)
    )

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

            did = maybe_exit(state, fair_yes, mark_yes, client)
            if not did:
                maybe_enter(state, ticker, fair_yes, mark_yes, z, client)

            write_status(state, message="running", fair_yes=fair_yes, mark_yes=mark_yes, edge=edge)
            save_state(state)

            time.sleep(max(1.0, POLL_SECONDS))

        except KeyboardInterrupt:
            return
        except Exception as exc:
            # only errors (no spam)
            log.error("loop error: %s", exc)
            try:
                write_status(state, message=f"error: {exc}", fair_yes=0.5, mark_yes=0.5, edge=0.0)
                save_state(state)
            except Exception:
                pass
            time.sleep(max(10.0, POLL_SECONDS))


if __name__ == "__main__":
    main()

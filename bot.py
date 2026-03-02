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
from typing import Any, Optional, Tuple, List

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

print("BOT_VERSION=2026-03-02_24H_METRICS_v1", flush=True)


# =============================================================================
# Safe env helpers (Railway blank-var proof)
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
    return int(float(v))

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return float(v)

def parse_iso(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def prune_24h(state: BotState) -> None:
    if not state.trade_history_24h:
        state.trade_history_24h = []
        return
    now = datetime.now(timezone.utc)
    keep: list[dict[str, Any]] = []
    for t in state.trade_history_24h:
        ts = t.get("ts")
        dt = parse_iso(ts) if isinstance(ts, str) else None
        if dt and (now - dt) <= timedelta(hours=24):
            keep.append(t)
    state.trade_history_24h = keep

def record_trade_24h(state: BotState, typ: str, pnl: float) -> None:
    if state.trade_history_24h is None:
        state.trade_history_24h = []
    state.trade_history_24h.append({"ts": utc_iso(), "type": typ, "pnl": float(pnl)})
    prune_24h(state)

def summarize_24h(state: BotState) -> tuple[int, float]:
    prune_24h(state)
    trades = len(state.trade_history_24h or [])
    pnl = 0.0
    for t in state.trade_history_24h or []:
        try:
            pnl += float(t.get("pnl", 0.0))
        except Exception:
            pass
    return trades, pnl


# =============================================================================
# Runtime config
# =============================================================================

KALSHI_BASE_URL = env_str("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/")
KALSHI_API_KEY_ID = env_str("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PEM = env_str("KALSHI_PRIVATE_KEY_PEM", "")
KALSHI_PRIVATE_KEY_PATH = env_str("KALSHI_PRIVATE_KEY_PATH", "")
SERIES_TICKER = env_str("SERIES_TICKER", "KXBTC15M").upper()

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

STATUS_FILE = Path(env_str("STATUS_FILE", "/data/status.json"))
STATE_FILE = Path(env_str("STATE_FILE", "/data/state.json"))

KRAKEN_TICKER_URL = env_str("KRAKEN_TICKER_URL", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD")

# Trade-only logs: we will NOT print loop errors unless DEBUG_ERRORS=true
DEBUG_ERRORS = env_bool("DEBUG_ERRORS", False)
DEBUG_BOOK_DUMP = env_bool("DEBUG_BOOK_DUMP", False)  # dumps a small orderbook preview when empty (throttled)


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(level=logging.WARNING, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("johnny5")


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_runtime_dir() -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)


# =============================================================================
# State
# =============================================================================
@dataclass
class BotState:
    equity: float = START_EQUITY                 # cash balance (realized)
    realized_pnl: float = 0.0                    # lifetime realized pnl (paper accounting)
    daily_realized_pnl: float = 0.0              # kept for compatibility; no longer the “main” 24h metric
    trades: int = 0                              # lifetime trade count
    last_day: str = ""

    # Position
    position_side: Optional[str] = None          # "YES" or "NO"
    position_entry_prob: Optional[float] = None
    position_contracts: int = 0
    market_ticker: Optional[str] = None

    # Rolling 24h metrics (persisted)
    trade_history_24h: list[dict[str, Any]] = None  # list of {"ts": "...Z", "type": "ENTER/EXIT", "pnl": float}

def load_state() -> BotState:
    if not STATE_FILE.exists():
        s = BotState()
        s.last_day = datetime.now(timezone.utc).date().isoformat()
        return s
    raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    s = BotState(**raw)
    if s.trade_history_24h is None:
        s.trade_history_24h = []
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
# Throttled debug printing (prevents spam)
# =============================================================================

_last_debug: dict[str, float] = {}

def debug_throttle(key: str, every_seconds: float) -> bool:
    now = time.time()
    last = _last_debug.get(key, 0.0)
    if now - last >= every_seconds:
        _last_debug[key] = now
        return True
    return False


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
                # Don't crash paper mode
                print(f"BOOT_WARN: could not load private key: {e}", flush=True)
                self.private_key = None
        else:
            print("BOOT_WARN: no private key found", flush=True)

    @staticmethod
    def _resolve_private_key_material() -> str:
        raw_pem = KALSHI_PRIVATE_KEY_PEM.strip()
        if raw_pem:
            return raw_pem.replace("\\n", "\n").strip().strip('"').strip("'")

        raw = KALSHI_PRIVATE_KEY_PATH.strip()
        if not raw:
            return ""

        if "BEGIN" in raw:
            return raw.replace("\\n", "\n").strip().strip('"').strip("'")

        # file path
        try:
            p = Path(raw)
            if len(raw) < 512 and p.exists():
                txt = p.read_text(encoding="utf-8")
                return txt.replace("\\n", "\n").strip().strip('"').strip("'")
        except OSError:
            pass

        # base64 pem
        try:
            decoded = base64.b64decode(raw, validate=True).decode("utf-8")
            return decoded.replace("\\n", "\n").strip().strip('"').strip("'")
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
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
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

        if r.status_code >= 400:
            raise RuntimeError(f"Kalshi HTTP {r.status_code} on {path}: {(r.text or '')[:300]}")

        try:
            out = r.json()
        except Exception:
            raise RuntimeError(f"Kalshi non-JSON response on {path}: {(r.text or '')[:300]}")

        if not isinstance(out, dict):
            raise RuntimeError(f"Kalshi unexpected JSON type on {path}: {type(out).__name__}")

        return out

    def get_open_markets(self) -> List[dict[str, Any]]:
        data = self._request("GET", f"/trade-api/v2/markets?series_ticker={SERIES_TICKER}&status=open")
        markets = data.get("markets", [])
        return markets if isinstance(markets, list) else []

    @staticmethod
    def pick_active_market(markets: List[dict[str, Any]]) -> Optional[dict[str, Any]]:
        """
        Prefer a market that is currently active (open_time <= now < close_time).
        If times aren't present, fallback to first.
        """
        now = datetime.now(timezone.utc)

        def parse_ts(x: Any) -> Optional[datetime]:
            if not isinstance(x, str) or not x:
                return None
            try:
                return datetime.fromisoformat(x.replace("Z", "+00:00"))
            except Exception:
                return None

        active: List[Tuple[datetime, dict[str, Any]]] = []
        future: List[Tuple[datetime, dict[str, Any]]] = []

        for m in markets:
            if not isinstance(m, dict):
                continue
            ot = parse_ts(m.get("open_time") or m.get("open_ts"))
            ct = parse_ts(m.get("close_time") or m.get("close_ts"))
            if ot and ct:
                if ot <= now < ct:
                    active.append((ct, m))
                elif ct >= now:
                    future.append((ct, m))

        if active:
            active.sort(key=lambda x: x[0])  # soonest close
            return active[0][1]
        if future:
            future.sort(key=lambda x: x[0])
            return future[0][1]
        return markets[0] if markets else None

    def get_orderbook(self, ticker: str) -> dict[str, Any]:
        return self._request("GET", f"/trade-api/v2/markets/{ticker}/orderbook")

    def place_order_buy(self, ticker: str, side: str, contracts: int, price_cents: int) -> dict[str, Any]:
        if not LIVE_MODE:
            return {"ok": True, "paper": True}

        if side not in ("YES", "NO"):
            raise ValueError("side must be YES or NO")

        payload: dict[str, Any] = {
            "ticker": ticker,
            "action": "buy",
            "type": "limit",
            "side": side.lower(),
            "count": int(contracts),
            "client_order_id": f"j5-{int(time.time())}",
        }
        if side == "YES":
            payload["yes_price"] = int(price_cents)
        else:
            payload["no_price"] = int(price_cents)

        return self._request("POST", "/trade-api/v2/portfolio/orders", payload)


# =============================================================================
# Kraken price + model
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
# Orderbook -> mark
# =============================================================================

def _levels(ob: dict[str, Any]) -> Tuple[list, list]:
    root = ob.get("orderbook", ob)
    yes = root.get("yes_bids") or root.get("yes") or []
    no = root.get("no_bids") or root.get("no") or []
    if not isinstance(yes, list):
        yes = []
    if not isinstance(no, list):
        no = []
    return yes, no

def _top_price(level: Any) -> Optional[int]:
    if isinstance(level, list) and len(level) >= 1:
        try:
            return int(level[0])
        except Exception:
            return None
    if isinstance(level, dict) and "price" in level:
        try:
            return int(level["price"])
        except Exception:
            return None
    return None

def mark_yes_from_orderbook(orderbook: dict[str, Any], ticker: str = "") -> Optional[float]:
    yes_bids, no_bids = _levels(orderbook)

    best_yes = _top_price(yes_bids[0]) if yes_bids else None
    best_no = _top_price(no_bids[0]) if no_bids else None

    # If both sides empty -> no liquidity right now; skip quietly
    if best_yes is None and best_no is None:
        if DEBUG_BOOK_DUMP and debug_throttle(f"empty_book:{ticker}", 60):
            preview = {"ticker": ticker, "yes_len": len(yes_bids), "no_len": len(no_bids)}
            print(json.dumps({"ts": utc_iso(), "event": "EMPTY_BOOK", "preview": preview}), flush=True)
        return None

    # If one side missing, fall back to a conservative estimate:
    # - if only YES bid exists: assume implied ask is 100 - best_yes (worst-ish), mid around best_yes
    # - if only NO bid exists: implied YES ask is 100 - best_no; mid around that
    if best_yes is None and best_no is not None:
        implied_yes_ask = 100 - best_no
        mid_yes = implied_yes_ask  # conservative: use ask-like level
    elif best_no is None and best_yes is not None:
        mid_yes = best_yes  # conservative: use bid-like level
    else:
        implied_yes_ask = 100 - int(best_no)
        mid_yes = (int(best_yes) + implied_yes_ask) / 2.0

    p = max(0.01, min(0.99, float(mid_yes) / 100.0))
    return p


# =============================================================================
# Trading
# =============================================================================
def emit_trade(event: str, state: BotState, mark_yes: float, fair_yes: float, edge: float, note: str = "", pnl_delta: float = 0.0) -> None:
    # Update rolling 24h stats
    record_trade_24h(state, event, pnl_delta)
    trades_24h, pnl_24h = summarize_24h(state)

    # Compute unrealized for equity
    unrealized = 0.0
    if state.position_side == "YES" and state.position_entry_prob is not None:
        unrealized = (mark_yes - state.position_entry_prob) * state.position_contracts
    elif state.position_side == "NO" and state.position_entry_prob is not None:
        unrealized = ((1.0 - mark_yes) - state.position_entry_prob) * state.position_contracts

    total_equity = state.equity + unrealized  # cash + unrealized
    cash_balance = state.equity

    print(
        json.dumps(
            {
                "ts": utc_iso(),
                "event": event,
                "note": note,
                "live": LIVE_MODE,

                # what you asked for:
                "balance_cash": round(cash_balance, 4),
                "equity_total": round(total_equity, 4),
                "pnl_24h_realized": round(pnl_24h, 4),
                "trades_24h": trades_24h,

                # extra context (still useful):
                "pnl_lifetime_realized": round(state.realized_pnl, 4),
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
        ),
        flush=True,
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

    emit_trade("ENTER", state, mark_yes, fair_yes, edge, pnl_delta=0.0)
    return True


def maybe_exit(state: BotState, fair_yes: float, mark_yes: float, client: KalshiClient) -> bool:
    if state.position_side is None or state.position_entry_prob is None:
        return False

    edge = fair_yes - mark_yes

    if state.position_side == "YES":
        should_exit = edge <= EDGE_EXIT
        pnl = (mark_yes - state.position_entry_prob) * state.position_contracts
        close_price = mark_yes
        close_side = "NO"  # hedge close
    else:
        should_exit = edge >= -EDGE_EXIT
        pnl = ((1.0 - mark_yes) - state.position_entry_prob) * state.position_contracts
        close_price = 1.0 - mark_yes
        close_side = "YES"

    if not should_exit:
        return False

    cents = max(1, min(99, int(round(close_price * 100))))
    client.place_order_buy(
        ticker=state.market_ticker or ticker_fallback(client),
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

    emit_trade("EXIT", state, mark_yes, fair_yes, edge, note=f"pnl={pnl:.4f}", pnl_delta=pnl)
    return True


def ticker_fallback(client: KalshiClient) -> str:
    markets = client.get_open_markets()
    m = client.pick_active_market(markets)
    if not m or not m.get("ticker"):
        return SERIES_TICKER
    return str(m["ticker"])


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    print("BOOT: bot.py started", flush=True)
    ensure_runtime_dir()
    state = load_state()
    client = KalshiClient()
    prices: list[float] = []

    log.warning(
        "BOOT | live=%s series=%s poll=%.1fs timeout=%ss state=%s status=%s",
        LIVE_MODE,
        SERIES_TICKER,
        POLL_SECONDS,
        REQUEST_TIMEOUT_SECONDS,
        str(STATE_FILE),
        str(STATUS_FILE),
    )

    while True:
        try:
            rollover_day(state)

            prices.append(kraken_last_price())
            if len(prices) > LOOKBACK * 5:
                prices = prices[-LOOKBACK * 5 :]

            # pick current active market
            markets = client.get_open_markets()
            m = client.pick_active_market(markets)
            if not m or not m.get("ticker"):
                # no market => sleep quietly
                write_status(state, message="no_open_market", fair_yes=0.5, mark_yes=0.5, edge=0.0)
                save_state(state)
                time.sleep(max(5.0, POLL_SECONDS))
                continue

            ticker = str(m["ticker"])

            ob = client.get_orderbook(ticker)
            mark_yes = mark_yes_from_orderbook(ob, ticker=ticker)
            if mark_yes is None:
                # No liquidity / empty book — this is normal, skip quietly (no error spam)
                write_status(state, message="empty_orderbook", fair_yes=0.5, mark_yes=0.5, edge=0.0)
                save_state(state)
                time.sleep(max(3.0, POLL_SECONDS))
                continue

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
            # always print a single-line error summary (Railway-friendly)
            print(json.dumps({"ts": utc_iso(), "event": "ERROR", "error": str(exc)[:300]}), flush=True)
            if DEBUG_ERRORS:
              log.exception("loop error")
            time.sleep(max(10.0, POLL_SECONDS))
        
if __name__ == "__main__":
    main()

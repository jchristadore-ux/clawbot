#!/usr/bin/env python3
from __future__ import annotations

import base64
import binascii
import json
import os
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Optional, Tuple, List

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


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


# =============================================================================
# Config
# =============================================================================

BOT_VERSION = "KALSHI_CLEAN_REBUILD_v1"

# Kalshi
KALSHI_BASE_URL = env_str("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/")
KALSHI_API_KEY_ID = env_str("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PEM = env_str("KALSHI_PRIVATE_KEY_PEM", "")
KALSHI_PRIVATE_KEY_PATH = env_str("KALSHI_PRIVATE_KEY_PATH", "")
SERIES_TICKER = env_str("SERIES_TICKER", "KXBTC15M").upper()

LIVE_MODE = env_bool("LIVE_MODE", False)

# Safety / ops
KILL_SWITCH = env_bool("KILL_SWITCH", False)  # if true: no new entries (optionally close)
CLOSE_ON_KILL_SWITCH = env_bool("CLOSE_ON_KILL_SWITCH", False)

POLL_SECONDS = env_float("POLL_SECONDS", 8.0)
REQUEST_TIMEOUT_SECONDS = env_int("REQUEST_TIMEOUT_SECONDS", 15)

# Strategy
LOOKBACK = env_int("LOOKBACK", 20)
FAIR_MOVE_SCALE = env_float("FAIR_MOVE_SCALE", 0.15)          # converts z-score into fair prob shift
MIN_CONVICTION_Z = env_float("MIN_CONVICTION_Z", 1.0)         # conviction threshold
EDGE_ENTER = env_float("EDGE_ENTER", 0.07)                    # enter when |edge| >= this
EDGE_EXIT = env_float("EDGE_EXIT", 0.02)                      # exit when edge mean-reverts past this

# Position sizing / risk
START_EQUITY = env_float("START_EQUITY", 50.0)
RISK_FRACTION = env_float("RISK_FRACTION", 0.08)
MAX_POSITION_USD = env_float("MAX_POSITION_USD", 10.0)        # keep small in paper; raise later
MAX_DAILY_LOSS = env_float("MAX_DAILY_LOSS", 10.0)            # realized loss cap per 24h window

# Anti-churn
MIN_HOLD_SECONDS = env_int("MIN_HOLD_SECONDS", 60)            # must hold at least this long before exiting
COOLDOWN_SECONDS = env_int("COOLDOWN_SECONDS", 30)            # wait after any trade before another

# State
STATE_FILE = Path(env_str("STATE_FILE", ".runtime/state.json"))
STATUS_FILE = Path(env_str("STATUS_FILE", ".runtime/status.json"))

# External price feed (for model)
KRAKEN_TICKER_URL = env_str("KRAKEN_TICKER_URL", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD")

# Debug (keep off normally)
DEBUG_ERRORS = env_bool("DEBUG_ERRORS", False)
DEBUG_BOOK_DUMP = env_bool("DEBUG_BOOK_DUMP", False)          # prints RAW_BOOK once/min


# =============================================================================
# Health server (for Railway healthchecks)
# =============================================================================

def start_health_server() -> None:
    port = env_int("PORT", 3000)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            p = (self.path or "").lower()
            if p in ("/", "/health", "/healthz", "/healthcheck"):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                body = json.dumps({"ok": True, "service": "johnny5", "version": BOT_VERSION}).encode("utf-8")
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt, *args):
            return

    server = HTTPServer(("0.0.0.0", port), Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()


# =============================================================================
# Helpers
# =============================================================================

def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

_last_debug: dict[str, float] = {}

def debug_throttle(key: str, every_seconds: float) -> bool:
    now = time.time()
    last = _last_debug.get(key, 0.0)
    if now - last >= every_seconds:
        _last_debug[key] = now
        return True
    return False

def ensure_dirs() -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)

def safe_write_json(path: Path, obj: dict) -> None:
    ensure_dirs()
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def parse_iso(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


# =============================================================================
# State + 24h metrics
# =============================================================================

@dataclass
class BotState:
    cash: float = START_EQUITY                   # realized cash balance
    realized_pnl_lifetime: float = 0.0
    trade_history_24h: list[dict[str, Any]] = None

    # position
    position_side: Optional[str] = None          # "YES" or "NO"
    position_entry_price: Optional[float] = None # probability paid/received per contract (0-1)
    position_contracts: int = 0
    position_ticker: Optional[str] = None
    position_open_ts: Optional[str] = None       # utc_iso()

    # anti-churn
    last_trade_ts: Optional[str] = None

def load_state() -> BotState:
    ensure_dirs()
    if not STATE_FILE.exists():
        s = BotState()
        s.trade_history_24h = []
        return s
    raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    s = BotState(**raw)
    if s.trade_history_24h is None:
        s.trade_history_24h = []
    return s

def save_state(state: BotState) -> None:
    ensure_dirs()
    STATE_FILE.write_text(json.dumps(asdict(state), indent=2), encoding="utf-8")

def prune_24h(state: BotState) -> None:
    if state.trade_history_24h is None:
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

def record_trade_24h(state: BotState, typ: str, pnl_delta: float) -> None:
    if state.trade_history_24h is None:
        state.trade_history_24h = []
    state.trade_history_24h.append({"ts": utc_iso(), "type": typ, "pnl": float(pnl_delta)})
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

def can_trade_now(state: BotState) -> bool:
    if not state.last_trade_ts:
        return True
    dt = parse_iso(state.last_trade_ts)
    if not dt:
        return True
    return (datetime.now(timezone.utc) - dt).total_seconds() >= COOLDOWN_SECONDS

def held_long_enough(state: BotState) -> bool:
    if not state.position_open_ts:
        return True
    dt = parse_iso(state.position_open_ts)
    if not dt:
        return True
    return (datetime.now(timezone.utc) - dt).total_seconds() >= MIN_HOLD_SECONDS


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
                print(json.dumps({"ts": utc_iso(), "event": "BOOT_WARN", "msg": f"private_key_load_failed: {e}"}), flush=True)
                self.private_key = None
        else:
            print(json.dumps({"ts": utc_iso(), "event": "BOOT_WARN", "msg": "no_private_key_found"}), flush=True)

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
        # If creds missing, return empty headers (some endpoints may still work)
        if not (KALSHI_API_KEY_ID and self.private_key):
            return {"Content-Type": "application/json"}

        ts_ms = str(int(time.time() * 1000))
        msg = f"{ts_ms}{method.upper()}{path}".encode("utf-8")

        # Match typical "RSA-SHA256" (PKCS#1 v1.5) style signing; signature sent base64.
        sig = self.private_key.sign(
            msg,
            padding.PKCS1v15(),
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
        url = f"{KALSHI_BASE_URL}{path}"
        headers = self._headers(method, path)

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

    def list_open_markets(self) -> List[dict[str, Any]]:
        data = self._request("GET", f"/trade-api/v2/markets?series_ticker={SERIES_TICKER}&status=open")
        markets = data.get("markets", [])
        return markets if isinstance(markets, list) else []

    @staticmethod
    def pick_active_market(markets: List[dict[str, Any]]) -> Optional[dict[str, Any]]:
        ""
        Choose active market with highest total bid liquidity.
        """
        now = datetime.now(timezone.utc)

        def parse_ts(x: Any) -> Optional[datetime]:
            if not isinstance(x, str) or not x:
                 return None
            try:
                return datetime.fromisoformat(x.replace("Z", "+00:00"))
            except Exception:
                return None

        candidates = []

        for m in markets:
            ot = parse_ts(m.get("open_time") or m.get("open_ts"))
            ct = parse_ts(m.get("close_time") or m.get("close_ts"))
            if not ot or not ct:
                continue
            if not (ot <= now < ct):
                continue

            # crude liquidity proxy: volume or open_interest
            vol = m.get("volume") or 0
            oi = m.get("open_interest") or 0
            score = float(vol) + float(oi)

            candidates.append((score, m))

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    def get_orderbook(self, ticker: str) -> dict[str, Any]:
        return self._request("GET", f"/trade-api/v2/markets/{ticker}/orderbook")

    def place_order(self, action: str, ticker: str, side: str, contracts: int, price_cents: int) -> dict[str, Any]:
        """
        action: "buy" or "sell"
        side: "YES" or "NO"
        """
        if not LIVE_MODE:
            return {"ok": True, "paper": True}

        if action not in ("buy", "sell"):
            raise ValueError("action must be buy or sell")
        if side not in ("YES", "NO"):
            raise ValueError("side must be YES or NO")

        payload: dict[str, Any] = {
            "ticker": ticker,
            "action": action,
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
# Market data + model
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
    rets: list[float] = []
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

def compute_contracts(cash: float, price_prob: float) -> int:
    usd = min(MAX_POSITION_USD, max(1.0, cash * RISK_FRACTION))
    # Approx $ per contract ≈ price_prob (YES) or (1-price_prob) for NO
    return max(1, int(usd / max(price_prob, 0.01)))


# =============================================================================
# Orderbook parsing -> mark
# =============================================================================

def _top_price(levels: Any) -> Optional[int]:
    if not isinstance(levels, list) or not levels:
        return None
    lvl = levels[0]
    if isinstance(lvl, list) and len(lvl) >= 1:
        try:
            return int(lvl[0])
        except Exception:
            return None
    if isinstance(lvl, dict) and "price" in lvl:
        try:
            return int(lvl["price"])
        except Exception:
            return None
    return None

def parse_best_bids(orderbook: dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    """
    Returns: (best_yes_bid_cents, best_no_bid_cents)
    Kalshi orderbook here is shaped like:
      {"orderbook": {"yes": [[price_cents, qty], ...], "no": [[price_cents, qty], ...], ...}}
    """
    root = orderbook.get("orderbook", orderbook)

    yes_levels = root.get("yes") or root.get("yes_bids") or []
    no_levels = root.get("no") or root.get("no_bids") or []

    best_yes = _top_price(yes_levels)
    best_no = _top_price(no_levels)
    return best_yes, best_no


def mark_yes_from_book(best_yes_bid: Optional[int], best_no_bid: Optional[int]) -> Optional[float]:
    """
    We need BOTH sides to compute a defensible midpoint:
      YES bid is best_yes_bid
      YES ask is implied by (100 - best_no_bid)
    """
    if best_yes_bid is None or best_no_bid is None:
        return None

    implied_yes_ask = 100 - int(best_no_bid)
    mid_yes_cents = (int(best_yes_bid) + implied_yes_ask) / 2.0
    p = mid_yes_cents / 100.0
    return max(0.01, min(0.99, p))


# =============================================================================
# Logging (ONLY on trades)
# =============================================================================

def trade_log(
    event: str,
    state: BotState,
    ticker: str,
    side: Optional[str],
    contracts: int,
    entry_price: Optional[float],
    exec_price: float,
    mark_yes: float,
    fair_yes: float,
    z: float,
    edge: float,
    pnl_delta: float,
    note: str = "",
) -> None:
    record_trade_24h(state, event, pnl_delta)
    trades_24h, pnl_24h = summarize_24h(state)

    # Unrealized (mark-to-mid)
    unrealized = 0.0
    if state.position_side and state.position_entry_price is not None:
        if state.position_side == "YES":
            unrealized = (mark_yes - state.position_entry_price) * state.position_contracts
        else:
            unrealized = ((1.0 - mark_yes) - state.position_entry_price) * state.position_contracts

    equity_total = state.cash + unrealized

    print(
        json.dumps(
            {
                "ts": utc_iso(),
                "event": event,
                "live": LIVE_MODE,
                "ticker": ticker,
                "side": side,
                "contracts": contracts,
                "entry_price": entry_price,
                "exec_price": round(exec_price, 6),
                "balance_cash": round(state.cash, 4),
                "equity_total": round(equity_total, 4),
                "pnl_24h_realized": round(pnl_24h, 4),
                "trades_24h": trades_24h,
                "pnl_lifetime_realized": round(state.realized_pnl_lifetime, 4),
                "market_yes": round(mark_yes, 6),
                "fair_yes": round(fair_yes, 6),
                "z": round(z, 6),
                "edge": round(edge, 6),
                "note": note,
            }
        ),
        flush=True,
    )


def write_status(state: BotState, message: str, ticker: str, mark_yes: Optional[float], fair_yes: float, z: float, edge: float) -> None:
    # keep status lightweight
    payload = {
        "ts": utc_iso(),
        "message": message,
        "live": LIVE_MODE,
        "kill_switch": KILL_SWITCH,
        "ticker": ticker,
        "cash": state.cash,
        "position": {
            "side": state.position_side,
            "contracts": state.position_contracts,
            "entry_price": state.position_entry_price,
            "open_ts": state.position_open_ts,
        },
        "model": {"mark_yes": mark_yes, "fair_yes": fair_yes, "z": z, "edge": edge},
    }
    safe_write_json(STATUS_FILE, payload)


# =============================================================================
# Trading logic
# =============================================================================

def maybe_enter(
    state: BotState,
    client: KalshiClient,
    ticker: str,
    best_yes_bid: int,
    best_no_bid: int,
    mark_yes: float,
    fair_yes: float,
    z: float,
) -> bool:
    if state.position_side is not None:
        return False
    if not can_trade_now(state):
        return False
    if KILL_SWITCH:
        return False

    # Risk stop (24h realized)
    trades_24h, pnl_24h = summarize_24h(state)
    if pnl_24h <= -abs(MAX_DAILY_LOSS):
        return False

    edge = fair_yes - mark_yes
    conviction = abs(z) >= MIN_CONVICTION_Z

    side: Optional[str] = None
    if conviction and edge >= EDGE_ENTER:
        side = "YES"
        # enter at YES ask implied by NO bid: 100 - best_no_bid
        entry_cents = max(1, min(99, 100 - best_no_bid))
        entry_price = entry_cents / 100.0
    elif conviction and edge <= -EDGE_ENTER:
        side = "NO"
        # enter at NO ask implied by YES bid: 100 - best_yes_bid
        entry_cents = max(1, min(99, 100 - best_yes_bid))
        entry_price = entry_cents / 100.0
    else:
        return False

    contracts = compute_contracts(state.cash, entry_price)

    # Live: BUY. Paper: simulate fill at entry_price.
    client.place_order("buy", ticker=ticker, side=side, contracts=contracts, price_cents=int(round(entry_price * 100)))

    # Paper accounting: cash decreases by cost per contract (simple approximation)
    # YES contract costs entry_price dollars; NO costs entry_price dollars as well (prob cost).
    if not LIVE_MODE:
        state.cash -= entry_price * contracts

    state.position_side = side
    state.position_entry_price = entry_price
    state.position_contracts = contracts
    state.position_ticker = ticker
    state.position_open_ts = utc_iso()
    state.last_trade_ts = utc_iso()

    trade_log(
        event="ENTER",
        state=state,
        ticker=ticker,
        side=side,
        contracts=contracts,
        entry_price=entry_price,
        exec_price=entry_price,
        mark_yes=mark_yes,
        fair_yes=fair_yes,
        z=z,
        edge=edge,
        pnl_delta=0.0,
        note="enter",
    )
    return True


def maybe_exit(
    state: BotState,
    client: KalshiClient,
    ticker: str,
    best_yes_bid: int,
    best_no_bid: int,
    mark_yes: float,
    fair_yes: float,
    z: float,
) -> bool:
    if state.position_side is None or state.position_entry_price is None:
        return False
    if not can_trade_now(state):
        return False
    if not held_long_enough(state) and not (KILL_SWITCH and CLOSE_ON_KILL_SWITCH):
        return False

    edge = fair_yes - mark_yes

    # Exit conditions:
    # YES position: exit when edge mean-reverts low enough (edge <= EDGE_EXIT)
    # NO position: exit when edge mean-reverts high enough (edge >= -EDGE_EXIT)
    if state.position_side == "YES":
        should_exit = edge <= EDGE_EXIT
        # Sell YES at best YES bid
        exit_cents = max(1, min(99, best_yes_bid))
        exit_price = exit_cents / 100.0
    else:
        should_exit = edge >= -EDGE_EXIT
        # Sell NO at best NO bid
        exit_cents = max(1, min(99, best_no_bid))
        exit_price = exit_cents / 100.0

    if KILL_SWITCH and CLOSE_ON_KILL_SWITCH:
        should_exit = True

    if not should_exit:
        return False

    contracts = state.position_contracts
    side = state.position_side
    entry_price = state.position_entry_price

    # Live: SELL to flatten. Paper: simulate fill at exit_price.
    client.place_order("sell", ticker=ticker, side=side, contracts=contracts, price_cents=int(round(exit_price * 100)))

    # Paper accounting: receive exit_price*contracts cash back.
    if not LIVE_MODE:
        state.cash += exit_price * contracts

    # Realized PnL (paper approximation): (exit - entry) * contracts for both YES and NO contracts
    pnl = (exit_price - entry_price) * contracts
    state.realized_pnl_lifetime += pnl

    state.position_side = None
    state.position_entry_price = None
    state.position_contracts = 0
    state.position_ticker = None
    state.position_open_ts = None
    state.last_trade_ts = utc_iso()

    trade_log(
        event="EXIT",
        state=state,
        ticker=ticker,
        side=side,
        contracts=contracts,
        entry_price=entry_price,
        exec_price=exit_price,
        mark_yes=mark_yes,
        fair_yes=fair_yes,
        z=z,
        edge=edge,
        pnl_delta=pnl,
        note=f"exit pnl={pnl:.4f}",
    )
    return True


# =============================================================================
# Main loop
# =============================================================================

def main() -> None:
    print(f"BOT_VERSION={BOT_VERSION}", flush=True)
    print("BOOT: bot.py started", flush=True)

    start_health_server()
    ensure_dirs()

    state = load_state()
    client = KalshiClient()
    prices: list[float] = []

    # One boot summary line
    print(
        json.dumps(
            {
                "ts": utc_iso(),
                "event": "BOOT",
                "live": LIVE_MODE,
                "series": SERIES_TICKER,
                "poll": POLL_SECONDS,
                "timeout": REQUEST_TIMEOUT_SECONDS,
                "state_file": str(STATE_FILE),
                "status_file": str(STATUS_FILE),
                "kill_switch": KILL_SWITCH,
            }
        ),
        flush=True,
    )

    while True:
        try:
            # Update model prices
            prices.append(kraken_last_price())
            if len(prices) > LOOKBACK * 10:
                prices = prices[-LOOKBACK * 10 :]

            # Choose current market
            markets = client.list_open_markets()
            m = client.pick_active_market(markets)
            if not m or not m.get("ticker"):
                write_status(state, "no_open_market", ticker="", mark_yes=None, fair_yes=0.5, z=0.0, edge=0.0)
                save_state(state)
                time.sleep(max(5.0, POLL_SECONDS))
                continue

            ticker = str(m["ticker"])

            # Get book
            ob = client.get_orderbook(ticker)
            if DEBUG_BOOK_DUMP and debug_throttle("raw_book", 60):
                print(json.dumps({"ts": utc_iso(), "event": "RAW_BOOK", "ticker": ticker, "book": ob})[:5000], flush=True)

            best_yes_bid, best_no_bid = parse_best_bids(ob)
            mark_yes = mark_yes_from_book(best_yes_bid, best_no_bid)

            if debug_throttle("markdbg", 60):
                print(json.dumps({
                    "ts": utc_iso(),
                    "event": "MARK_DEBUG",
                    "ticker": ticker,
                    "best_yes_bid": best_yes_bid,
                    "best_no_bid": best_no_bid,
                    "mark_yes": mark_yes
                }), flush=True)

            # If book is missing either side, do not trade.
            if mark_yes is None or abs(mark_yes - 0.5) < 1e-9:
                write_status(state, "bad_mark_skip", ticker=ticker, mark_yes=mark_yes, fair_yes=0.5, z=0.0, edge=0.0)
                save_state(state)
                time.sleep(max(3.0, POLL_SECONDS))
                continue

            fair_yes, z = model_fair_yes(prices)
            edge = fair_yes - mark_yes

            # Exit then enter
            _ = maybe_exit(state, client, ticker, best_yes_bid, best_no_bid, mark_yes, fair_yes, z)
            _ = maybe_enter(state, client, ticker, best_yes_bid, best_no_bid, mark_yes, fair_yes, z)

            write_status(state, "running", ticker=ticker, mark_yes=mark_yes, fair_yes=fair_yes, z=z, edge=edge)
            save_state(state)

            time.sleep(max(1.0, POLL_SECONDS))

        except KeyboardInterrupt:
            return
        except Exception as exc:
            # No spam: print a compact error occasionally
            if DEBUG_ERRORS and debug_throttle("err", 10):
                print(json.dumps({"ts": utc_iso(), "event": "ERROR", "error": str(exc)[:300]}), flush=True)
            # Keep running
            try:
                write_status(state, f"error: {exc}", ticker=state.position_ticker or "", mark_yes=None, fair_yes=0.5, z=0.0, edge=0.0)
                save_state(state)
            except Exception:
                pass
            time.sleep(max(10.0, POLL_SECONDS))


if __name__ == "__main__":
    main()

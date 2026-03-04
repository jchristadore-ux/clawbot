#!/usr/bin/env python3
from __future__ import annotations

import base64
import binascii
import json
import os
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

def send_telegram(message: str):
    try:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

        if not token or not chat_id:
            log.warning("Telegram not configured. Skipping Telegram message.")
            return

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "disable_web_page_preview": True,
        }

        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            log.error(f"Telegram send failed: {r.status_code} {r.text}")
        else:
            log.info("Telegram notification sent")

    except Exception as e:
        log.error(f"Telegram send exception: {e}")


# =============================================================================
# Safe env helpers (handles blank Railway vars)
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
    try:
        return int(v)
    except Exception:
        return int(float(v))

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return float(v)

def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_iso(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


# =============================================================================
# Config (all defaults safe)
# =============================================================================

BOT_VERSION = "JOHNNY5_KALSHI_BTC15M_CLEAN_v1"

# Kalshi API
KALSHI_BASE_URL = env_str("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/")
KALSHI_API_KEY_ID = env_str("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PEM = env_str("KALSHI_PRIVATE_KEY_PEM", "")
KALSHI_PRIVATE_KEY_PATH = env_str("KALSHI_PRIVATE_KEY_PATH", "")
SERIES_TICKER = env_str("SERIES_TICKER", "KXBTC15M").upper()

# Runtime / ops
LIVE_MODE = env_bool("LIVE_MODE", False)
POLL_SECONDS = env_float("POLL_SECONDS", 8.0)
REQUEST_TIMEOUT_SECONDS = env_int("REQUEST_TIMEOUT_SECONDS", 15)

# Railway healthcheck
PORT = env_int("PORT", 3000)
HEALTH_PATH = env_str("HEALTH_PATH", "/health")

# State paths (DO NOT use /data unless you mounted it)
STATE_FILE = Path(env_str("STATE_FILE", ".runtime/state.json"))
STATUS_FILE = Path(env_str("STATUS_FILE", ".runtime/status.json"))

# External price feed for model
KRAKEN_TICKER_URL = env_str("KRAKEN_TICKER_URL", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD")

# Strategy
LOOKBACK = env_int("LOOKBACK", 20)
FAIR_MOVE_SCALE = env_float("FAIR_MOVE_SCALE", 0.15)
MIN_CONVICTION_Z = env_float("MIN_CONVICTION_Z", 1.0)
EDGE_ENTER = env_float("EDGE_ENTER", 0.07)
EDGE_EXIT = env_float("EDGE_EXIT", 0.02)

# Risk / sizing
START_EQUITY = env_float("START_EQUITY", 50.0)
RISK_FRACTION = env_float("RISK_FRACTION", 0.08)
MAX_POSITION_USD = env_float("MAX_POSITION_USD", 10.0)
MAX_DAILY_LOSS_24H = env_float("MAX_DAILY_LOSS_24H", 10.0)

# Anti-churn
MIN_HOLD_SECONDS = env_int("MIN_HOLD_SECONDS", 60)
COOLDOWN_SECONDS = env_int("COOLDOWN_SECONDS", 30)

# Market sanity (prevents trading on ghost books like 1/1)
MIN_BID_CENTS = env_int("MIN_BID_CENTS", 2)     # ignore books where best bid < 2
MAX_BID_CENTS = env_int("MAX_BID_CENTS", 98)    # ignore books where best bid > 98 (usually nonsense)

# Control switches
KILL_SWITCH = env_bool("KILL_SWITCH", False)                 # block new entries
CLOSE_ON_KILL_SWITCH = env_bool("CLOSE_ON_KILL_SWITCH", False)  # if true, bot will try to flatten when safe

# Debug (off by default)
DEBUG_ERRORS = env_bool("DEBUG_ERRORS", False)
DEBUG_MARK = env_bool("DEBUG_MARK", False)    # prints MARK_DEBUG once/minute
DEBUG_PICK = env_bool("DEBUG_PICK", False)    # prints PICK_DEBUG once/minute


# =============================================================================
# Health server (so Railway healthcheck doesn't restart the container)
# =============================================================================

def start_health_server() -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            p = (self.path or "").lower()
            if p == HEALTH_PATH.lower() or p in ("/", "/healthz", "/healthcheck"):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                body = json.dumps({"ok": True, "service": "johnny5", "version": BOT_VERSION}).encode("utf-8")
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def _message(self, fmt: str, *args: Any) -> None:
            return

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()


# =============================================================================
# State + 24h metrics
# =============================================================================

@dataclass
class BotState:
    cash: float = START_EQUITY
    realized_pnl_lifetime: float = 0.0
    trade_history_24h: List[Dict[str, Any]] = None  # [{"ts":..., "type":..., "pnl":...}, ...]

    position_side: Optional[str] = None             # "YES" or "NO"
    position_entry_price: Optional[float] = None    # 0..1
    position_contracts: int = 0
    position_ticker: Optional[str] = None
    position_open_ts: Optional[str] = None

    last_trade_ts: Optional[str] = None

def ensure_dirs() -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)

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
    keep: List[Dict[str, Any]] = []
    for t in state.trade_history_24h:
        ts = t.get("ts")
        dt = parse_iso(ts) if isinstance(ts, str) else None
        if dt and (now - dt) <= timedelta(hours=24):
            keep.append(t)
    state.trade_history_24h = keep

def summarize_24h(state: BotState) -> Tuple[int, float]:
    prune_24h(state)
    trades = len(state.trade_history_24h or [])
    pnl = 0.0
    for t in state.trade_history_24h or []:
        try:
            pnl += float(t.get("pnl", 0.0))
        except Exception:
            pass
    return trades, pnl

def record_trade_24h(state: BotState, typ: str, pnl_delta: float) -> None:
    if state.trade_history_24h is None:
        state.trade_history_24h = []
    state.trade_history_24h.append({"ts": utc_iso(), "type": typ, "pnl": float(pnl_delta)})
    prune_24h(state)

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

def write_status(state: BotState, message: str, ticker: str, mark_yes: Optional[float], fair_yes: float, z: float, edge: float) -> None:
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
        "model": {
            "mark_yes": mark_yes,
            "fair_yes": fair_yes,
            "z": z,
            "edge": edge,
        },
    }
    ensure_dirs()
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
                self.private_key = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
            except Exception as e:
                self.private_key = None
                print(json.dumps({"ts": utc_iso(), "event": "BOOT_WARN", "msg": f"private_key_load_failed: {str(e)[:200]}"}), flush=True)
        else:
            print(json.dumps({"ts": utc_iso(), "event": "BOOT_WARN", "msg": "no_private_key_found"}), flush=True)

    @staticmethod
    def _resolve_private_key_material() -> str:
        # Preferred: direct PEM in env
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
                return p.read_text(encoding="utf-8").replace("\\n", "\n").strip().strip('"').strip("'")
        except Exception:
            pass

        # base64 PEM
        try:
            decoded = base64.b64decode(raw, validate=True).decode("utf-8")
            return decoded.replace("\\n", "\n").strip().strip('"').strip("'")
        except (binascii.Error, UnicodeDecodeError):
            return ""

    def _headers(self, method: str, path: str) -> Dict[str, str]:
        if not (KALSHI_API_KEY_ID and self.private_key):
            return {"Content-Type": "application/json"}

        ts_ms = str(int(time.time() * 1000))
        msg = f"{ts_ms}{method.upper()}{path}".encode("utf-8")
        sig = self.private_key.sign(msg, padding.PKCS1v15(), hashes.SHA256())
        sig_b64 = base64.b64encode(sig).decode("utf-8")

        return {
            "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": sig_b64,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{KALSHI_BASE_URL}{path}"
        r = self.session.request(
            method=method.upper(),
            url=url,
            headers=self._headers(method, path),
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"Kalshi HTTP {r.status_code} {path}: {(r.text or '')[:300]}")
        try:
            out = r.json()
        except Exception:
            raise RuntimeError(f"Kalshi non-JSON {path}: {(r.text or '')[:300]}")
        if not isinstance(out, dict):
            raise RuntimeError(f"Kalshi unexpected JSON type on {path}: {type(out).__name__}")
        return out

    def list_open_markets(self) -> List[Dict[str, Any]]:
        data = self._request("GET", f"/trade-api/v2/markets?series_ticker={SERIES_TICKER}&status=open")
        markets = data.get("markets", [])
        return markets if isinstance(markets, list) else []

    def get_orderbook(self, ticker: str) -> Dict[str, Any]:
        return self._request("GET", f"/trade-api/v2/markets/{ticker}/orderbook")

    def place_order(self, action: str, ticker: str, side: str, contracts: int, price_cents: int) -> Dict[str, Any]:
    """
    Routes order placement through the Bun gateway (modest-patience) to avoid Python RSA signing issues.
    Set KALSHI_ORDER_GATEWAY_URL to your modest-patience URL, e.g.:
      https://modest-patience-production-651b.up.railway.app
    """
    # Paper mode: simulate success
    if not LIVE_MODE:
        return {"ok": True, "paper": True}

    gateway = env_str("KALSHI_ORDER_GATEWAY_URL", "").rstrip("/")
    if not gateway:
        raise RuntimeError("KALSHI_ORDER_GATEWAY_URL is required in LIVE_MODE")

    if action not in ("buy", "sell"):
        raise ValueError("action must be buy or sell")
    if side not in ("YES", "NO"):
        raise ValueError("side must be YES or NO")

    payload: Dict[str, Any] = {
        "ticker": ticker,
        "action": action,
        "type": "limit",
        "side": side,
        "count": int(contracts),
    }
    if side == "YES":
        payload["yes_price"] = int(price_cents)
    else:
        payload["no_price"] = int(price_cents)

    r = requests.post(f"{gateway}/order", json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
    if r.status_code >= 400:
        raise RuntimeError(f"Order gateway HTTP {r.status_code}: {(r.text or '')[:300]}")
    out = r.json()
    if not isinstance(out, dict):
        raise RuntimeError("Order gateway returned non-JSON object")
    if not out.get("ok"):
        raise RuntimeError(f"Order gateway rejected order: {json.dumps(out)[:300]}")
    return out


# =============================================================================
# Market selection (prefer liquid active bucket)
# =============================================================================

def _as_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str) and x.strip() != "":
            return float(x)
    except Exception:
        return 0.0
    return 0.0

def _parse_ts_any(x: Any) -> Optional[datetime]:
    if not isinstance(x, str) or not x:
        return None
    try:
        return datetime.fromisoformat(x.replace("Z", "+00:00"))
    except Exception:
        return None

def pick_best_active_market(markets: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    active: List[Tuple[float, datetime, Dict[str, Any]]] = []
    fallback: List[Tuple[datetime, Dict[str, Any]]] = []

    for m in markets:
        if not isinstance(m, dict):
            continue
        ot = _parse_ts_any(m.get("open_time") or m.get("open_ts"))
        ct = _parse_ts_any(m.get("close_time") or m.get("close_ts"))
        if not ot or not ct:
            continue
        if ot <= now < ct:
            vol = _as_float(m.get("volume") or m.get("volume_24h") or m.get("vol"))
            oi = _as_float(m.get("open_interest") or m.get("openInterest") or m.get("oi"))
            score = vol + oi
            active.append((score, ct, m))
            fallback.append((ct, m))

    if not active:
        return None

    active.sort(key=lambda x: (x[0], -x[1].timestamp()), reverse=True)
    best_score = active[0][0]
    if best_score <= 0.0:
        fallback.sort(key=lambda x: x[0])
        return fallback[0][1]
    return active[0][2]


# =============================================================================
# Orderbook parsing + mark
# =============================================================================

def _best_price_cents(levels: Any) -> Optional[int]:
    """
    levels is typically [[price_cents, qty], ...] OR list of dicts {"price":..., "quantity":...}
    We'll compute MAX price as "best bid".
    """
    if not isinstance(levels, list) or not levels:
        return None

    best: Optional[int] = None
    for lvl in levels:
        price: Optional[int] = None
        if isinstance(lvl, list) and len(lvl) >= 1:
            try:
                price = int(lvl[0])
            except Exception:
                price = None
        elif isinstance(lvl, dict) and "price" in lvl:
            try:
                price = int(lvl["price"])
            except Exception:
                price = None

        if price is None:
            continue
        if best is None or price > best:
            best = price

    return best

def parse_best_bids(orderbook: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    root = orderbook.get("orderbook", orderbook)
    yes_levels = root.get("yes") or root.get("yes_bids") or []
    no_levels = root.get("no") or root.get("no_bids") or []
    best_yes = _best_price_cents(yes_levels)
    best_no = _best_price_cents(no_levels)
    return best_yes, best_no

def mark_yes_from_bids(best_yes_bid: Optional[int], best_no_bid: Optional[int]) -> Optional[float]:
    """
    Mark YES as midpoint between:
      YES best bid = best_yes_bid
      YES implied ask = 100 - best_no_bid
    Requires both sides.
    """
    if best_yes_bid is None or best_no_bid is None:
        return None
    if best_yes_bid < MIN_BID_CENTS or best_no_bid < MIN_BID_CENTS:
        return None
    if best_yes_bid > MAX_BID_CENTS or best_no_bid > MAX_BID_CENTS:
        return None

    implied_yes_ask = 100 - int(best_no_bid)
    mid = (int(best_yes_bid) + implied_yes_ask) / 2.0
    p = mid / 100.0
    return max(0.01, min(0.99, p))


# =============================================================================
# Model + sizing
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

def model_fair_yes(prices: List[float]) -> Tuple[float, float]:
    if len(prices) < LOOKBACK:
        return 0.5, 0.0

    window = prices[-LOOKBACK:]
    rets: List[float] = []
    for i in range(1, len(window)):
        prev = window[i - 1]
        cur = window[i]
        if prev <= 0:
            continue
        rets.append((cur - prev) / prev)

    if len(rets) < 2:
        return 0.5, 0.0

    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
    std = max(var ** 0.5, 1e-8)
    z = (rets[-1] - mean) / std

    fair = 0.5 + z * FAIR_MOVE_SCALE
    fair = max(0.01, min(0.99, fair))
    return fair, z

def compute_contracts(cash: float, price_prob: float) -> int:
    usd = min(MAX_POSITION_USD, max(1.0, cash * RISK_FRACTION))
    return max(1, int(usd / max(price_prob, 0.01)))


# =============================================================================
# Trade ging (ONLY on trades)
# =============================================================================

def log_trade(
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
    note: str,
) -> None:
    record_trade_24h(state, event, pnl_delta)
    trades_24h, pnl_24h = summarize_24h(state)

    unrealized = 0.0
    if state.position_side and state.position_entry_price is not None:
        if state.position_side == "YES":
            unrealized = (mark_yes - state.position_entry_price) * state.position_contracts
        else:
            unrealized = ((1.0 - mark_yes) - state.position_entry_price) * state.position_contracts

    equity_total = state.cash + unrealized

    # Always print the trade log (ENTER + EXIT)
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

    # Telegram: ONLY notify on ENTER
    if event == "ENTER":
        try:
            msg = (
                f"ENTER ✅ {ticker}\n"
                f"Side: {side} | Contracts: {contracts} | Price: {exec_price:.2f}\n"
                f"Edge: {edge:.4f} | z: {z:.2f}\n"
                f"Cash: ${state.cash:.2f} | Equity: ${equity_total:.2f}\n"
                f"PnL 24h (realized): ${pnl_24h:.2f} | Lifetime (realized): ${state.realized_pnl_lifetime:.2f}"
            )
            send_telegram(msg)
        except Exception:
            pass

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
    if KILL_SWITCH:
        return False
    if not can_trade_now(state):
        return False

    trades_24h, pnl_24h = summarize_24h(state)
    if pnl_24h <= -abs(MAX_DAILY_LOSS_24H):
        return False

    edge = fair_yes - mark_yes
    if abs(z) < MIN_CONVICTION_Z:
        return False

    side: Optional[str] = None
    entry_price: Optional[float] = None

    if edge >= EDGE_ENTER:
        side = "YES"
        # buy at implied YES ask = 100 - best_no_bid
        entry_cents = max(1, min(99, 100 - best_no_bid))
        entry_price = entry_cents / 100.0
    elif edge <= -EDGE_ENTER:
        side = "NO"
        # buy at implied NO ask = 100 - best_yes_bid
        entry_cents = max(1, min(99, 100 - best_yes_bid))
        entry_price = entry_cents / 100.0
    else:
        return False

    contracts = compute_contracts(state.cash, float(entry_price))

    client.place_order("buy", ticker=ticker, side=side, contracts=contracts, price_cents=int(round(entry_price * 100)))

    # Paper accounting (simple): pay entry_price per contract
    if not LIVE_MODE:
        state.cash -= float(entry_price) * contracts

    state.position_side = side
    state.position_entry_price = float(entry_price)
    state.position_contracts = contracts
    state.position_ticker = ticker
    state.position_open_ts = utc_iso()
    state.last_trade_ts = utc_iso()

    log_trade(
        event="ENTER",
        state=state,
        ticker=ticker,
        side=side,
        contracts=contracts,
        entry_price=float(entry_price),
        exec_price=float(entry_price),
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

    # If kill-switch wants flattening, allow exit even if min-hold not met
    if not held_long_enough(state) and not (KILL_SWITCH and CLOSE_ON_KILL_SWITCH):
        return False

    edge = fair_yes - mark_yes

    # Standard exit condition
    should_exit = False
    if state.position_side == "YES":
        should_exit = edge <= EDGE_EXIT
        exit_cents = max(1, min(99, best_yes_bid))   # sell at best YES bid
        exit_price = exit_cents / 100.0
    else:
        should_exit = edge >= -EDGE_EXIT
        exit_cents = max(1, min(99, best_no_bid))    # sell at best NO bid
        exit_price = exit_cents / 100.0

    if KILL_SWITCH and CLOSE_ON_KILL_SWITCH:
        should_exit = True

    if not should_exit:
        return False

    contracts = state.position_contracts
    side = state.position_side
    entry_price = state.position_entry_price

    client.place_order("sell", ticker=ticker, side=side, contracts=contracts, price_cents=int(round(exit_price * 100)))

    # Paper accounting: receive exit_price per contract
    if not LIVE_MODE:
        state.cash += exit_price * contracts

    pnl = (exit_price - entry_price) * contracts
    state.realized_pnl_lifetime += pnl

    state.position_side = None
    state.position_entry_price = None
    state.position_contracts = 0
    state.position_ticker = None
    state.position_open_ts = None
    state.last_trade_ts = utc_iso()

    log_trade(
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
# Main
# =============================================================================

_last_dbg: Dict[str, float] = {}

def dbg_every(key: str, seconds: float) -> bool:
    now = time.time()
    last = _last_dbg.get(key, 0.0)
    if now - last >= seconds:
        _last_dbg[key] = now
        return True
    return False

def main() -> None:
    print(f"BOT_VERSION={BOT_VERSION}", flush=True)
    print("BOOT: bot.py started", flush=True)

    start_health_server()
    ensure_dirs()

    state = load_state()
    client = KalshiClient()
    prices: List[float] = []

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
            # Model input price
            prices.append(kraken_last_price())
            if len(prices) > LOOKBACK * 10:
                prices = prices[-LOOKBACK * 10 :]

            # Pick market
            markets = client.list_open_markets()
            m = pick_best_active_market(markets)
            if not m or not m.get("ticker"):
                write_status(state, "no_open_market", ticker="", mark_yes=None, fair_yes=0.5, z=0.0, edge=0.0)
                save_state(state)
                time.sleep(max(5.0, POLL_SECONDS))
                continue

            ticker = str(m["ticker"])

            if DEBUG_PICK and dbg_every("pick", 60):
                vol = m.get("volume") or m.get("volume_24h") or m.get("vol")
                oi = m.get("open_interest") or m.get("openInterest") or m.get("oi")
                print(json.dumps({"ts": utc_iso(), "event": "PICK_DEBUG", "ticker": ticker, "volume": vol, "open_interest": oi}), flush=True)

            ob = client.get_orderbook(ticker)
            best_yes_bid, best_no_bid = parse_best_bids(ob)
            mark_yes = mark_yes_from_bids(best_yes_bid, best_no_bid)

            if DEBUG_MARK and dbg_every("mark", 60):
                print(
                    json.dumps(
                        {
                            "ts": utc_iso(),
                            "event": "MARK_DEBUG",
                            "ticker": ticker,
                            "best_yes_bid": best_yes_bid,
                            "best_no_bid": best_no_bid,
                            "mark_yes": mark_yes,
                        }
                    ),
                    flush=True,
                )

            # Skip if book is missing / ghost
            if mark_yes is None:
                write_status(state, "skip_bad_or_one_sided_book", ticker=ticker, mark_yes=None, fair_yes=0.5, z=0.0, edge=0.0)
                save_state(state)
                time.sleep(max(3.0, POLL_SECONDS))
                continue

            fair_yes, z = model_fair_yes(prices)
            edge = fair_yes - mark_yes

            # If kill-switch is on, allow only exit (optionally)
            _ = maybe_exit(state, client, ticker, int(best_yes_bid), int(best_no_bid), float(mark_yes), fair_yes, z)
            _ = maybe_enter(state, client, ticker, int(best_yes_bid), int(best_no_bid), float(mark_yes), fair_yes, z)

            write_status(state, "running", ticker=ticker, mark_yes=float(mark_yes), fair_yes=fair_yes, z=z, edge=edge)
            save_state(state)

            time.sleep(max(1.0, POLL_SECONDS))

        except KeyboardInterrupt:
            return
        except Exception as exc:
            if DEBUG_ERRORS and dbg_every("err", 10):
                print(json.dumps({"ts": utc_iso(), "event": "ERROR", "error": str(exc)[:300]}), flush=True)
            try:
                write_status(state, f"error: {str(exc)[:200]}", ticker=state.position_ticker or "", mark_yes=None, fair_yes=0.5, z=0.0, edge=0.0)
                save_state(state)
            except Exception:
                pass
            time.sleep(max(10.0, POLL_SECONDS))


if __name__ == "__main__":
    main()

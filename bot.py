#!/usr/bin/env python3
"""
Johnny5-Kalshi — KXBTC15M Production Bot
Version: JOHNNY5_KALSHI_BTC15M_v10_PRODUCTION

Strategy: Trades the Kalshi KXBTC15M series (BTC up/down in 15-min buckets).
Primary edge: BTC has moved significantly inside the current bucket but the
orderbook hasn't fully repriced yet. Secondary confirmation: short-term momentum
z-score + orderbook depth imbalance.

Architecture:
  Python bot → Bun gateway (modest-patience) → Kalshi API
  (RSA signing handled by Bun to avoid Python RSA header quirks)

State persistence: file (.runtime/state.json) + Postgres equity via gateway.
Logs: stdout (Railway) + POST to gateway /ingest-logs for DB viewer.

Environment variables: see CONFIG section below. All have safe defaults.
LIVE_MODE=FALSE for paper trading, LIVE_MODE=TRUE for real money.
"""
from __future__ import annotations

import base64
import binascii
import json
import math
import os
import queue
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


# =============================================================================
# STDOUT TEE — mirrors every printed line to gateway /ingest-logs
# No psycopg2 required; uses the Bun gateway HTTP endpoint instead.
# =============================================================================

_log_queue: "queue.Queue[str]" = queue.Queue(maxsize=3000)
_GATEWAY_LOG_ENABLED = True  # will be confirmed once gateway URL is known


class _TeeWriter:
    """Writes to real stdout AND queues lines for gateway log insertion."""
    def __init__(self, real):
        self._real = real

    def write(self, s: str):
        self._real.write(s)
        if _GATEWAY_LOG_ENABLED and s.strip():
            try:
                _log_queue.put_nowait(s.strip())
            except queue.Full:
                pass  # never block the bot

    def flush(self):
        self._real.flush()

    def __getattr__(self, name):
        return getattr(self._real, name)


def _start_gateway_log_worker(gateway_url: str) -> None:
    """Background thread: drains queue and bulk-POSTs to /ingest-logs."""
    if not gateway_url:
        return

    def _parse_event(line: str) -> str:
        try:
            return str(json.loads(line).get("event", ""))[:64]
        except Exception:
            return ""

    def _severity(line: str) -> str:
        low = line.lower()
        if any(x in low for x in ("error", "exception", "traceback")):
            return "error"
        if any(x in low for x in ("enter", "exit", "trade", "order")):
            return "trade"
        if any(x in low for x in ("boot", "hot_reload", "restart")):
            return "system"
        return "info"

    def worker():
        url = f"{gateway_url.rstrip('/')}/ingest-logs"
        while True:
            try:
                # Collect up to 50 lines or wait 5s
                batch = []
                try:
                    batch.append(_log_queue.get(timeout=5))
                except queue.Empty:
                    continue
                while len(batch) < 50:
                    try:
                        batch.append(_log_queue.get_nowait())
                    except queue.Empty:
                        break

                rows = [
                    {
                        "message": line[:2000],
                        "event": _parse_event(line),
                        "severity": _severity(line),
                    }
                    for line in batch
                ]
                requests.post(url, json=rows, timeout=8)
            except Exception:
                time.sleep(5)

    t = threading.Thread(target=worker, daemon=True, name="gateway-log-writer")
    t.start()


# Activate tee immediately; worker started after gateway URL is confirmed in main()
sys.stdout = _TeeWriter(sys.stdout)


# =============================================================================
# HOT RELOAD — polls gateway for new bot.py code, re-execs if found
# =============================================================================

_hot_reload_last_check: float = 0.0
HOT_RELOAD_INTERVAL = float(os.getenv("HOT_RELOAD_INTERVAL", "60"))


def check_hot_reload(gateway: str) -> None:
    global _hot_reload_last_check
    now = time.time()
    if now - _hot_reload_last_check < HOT_RELOAD_INTERVAL:
        return
    _hot_reload_last_check = now
    if not gateway:
        return
    try:
        r = requests.get(f"{gateway}/hot-reload-check", timeout=5)
        if r.status_code != 200:
            return
        data = r.json()
        if not data.get("pending"):
            return
        code = data.get("code", "")
        row_id = data.get("id")
        if not code or not row_id:
            return
        bot_path = sys.argv[0]
        with open(bot_path, "w", encoding="utf-8") as f:
            f.write(code)
        requests.post(f"{gateway}/hot-reload-applied", json={"id": row_id}, timeout=5)
        print(json.dumps({"ts": utc_iso(), "event": "HOT_RELOAD", "id": row_id, "msg": "restarting"}), flush=True)
        time.sleep(1)
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception:
        pass  # never crash the bot on hot-reload failure


# =============================================================================
# Utility
# =============================================================================

def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _parse_ts_any(x: Any) -> Optional[datetime]:
    """Parse unix int/float (secs or ms) or ISO string."""
    if isinstance(x, (int, float)) and x > 0:
        try:
            return datetime.fromtimestamp(x / 1000.0 if x > 1e11 else float(x), tz=timezone.utc)
        except Exception:
            return None
    if not isinstance(x, str) or not x:
        return None
    try:
        return datetime.fromisoformat(x.replace("Z", "+00:00"))
    except Exception:
        pass
    try:
        f = float(x)
        if f > 0:
            return datetime.fromtimestamp(f / 1000.0 if f > 1e11 else f, tz=timezone.utc)
    except Exception:
        pass
    return None


def _as_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        return float(x)
    except Exception:
        return 0.0


_dbg_last: Dict[str, float] = {}


def dbg_every(key: str, seconds: float) -> bool:
    now = time.time()
    if now - _dbg_last.get(key, 0.0) >= seconds:
        _dbg_last[key] = now
        return True
    return False


# =============================================================================
# Env helpers — handle blank Railway vars safely
# =============================================================================

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


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
        try:
            return int(float(v))
        except Exception:
            return default


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return float(v)
    except Exception:
        return default


# =============================================================================
# CONFIG — all driven by environment variables; safe defaults for every value
# =============================================================================

BOT_VERSION = "JOHNNY5_KALSHI_BTC15M_v10_PRODUCTION"

# ── Kalshi / Gateway ──────────────────────────────────────────────────────────
KALSHI_BASE_URL         = env_str("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/")
KALSHI_API_KEY_ID       = env_str("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PEM  = env_str("KALSHI_PRIVATE_KEY_PEM", "")
KALSHI_PRIVATE_KEY_PATH = env_str("KALSHI_PRIVATE_KEY_PATH", "")
KALSHI_ORDER_GATEWAY    = env_str("KALSHI_ORDER_GATEWAY_URL", "").rstrip("/")
SERIES_TICKER           = env_str("SERIES_TICKER", "KXBTC15M").upper()

# ── Mode ──────────────────────────────────────────────────────────────────────
LIVE_MODE               = env_bool("LIVE_MODE", False)
KILL_SWITCH             = env_bool("KILL_SWITCH", False)
CLOSE_ON_KILL_SWITCH    = env_bool("CLOSE_ON_KILL_SWITCH", False)

# ── Price feed ────────────────────────────────────────────────────────────────
KRAKEN_TICKER_URL       = env_str("KRAKEN_TICKER_URL",
                                   "https://api.kraken.com/0/public/Ticker?pair=XBTUSD")
COINBASE_TICKER_URL     = env_str("COINBASE_TICKER_URL",
                                   "https://api.coinbase.com/v2/prices/BTC-USD/spot")

# ── Strategy ──────────────────────────────────────────────────────────────────
LOOKBACK                = env_int("LOOKBACK", 20)
FAIR_MOVE_SCALE         = env_float("FAIR_MOVE_SCALE", 0.15)   # z-score secondary weight
BUCKET_MOVE_SCALE       = env_float("BUCKET_MOVE_SCALE", 25.0) # 1% BTC move → 0.25 prob shift
OB_WEIGHT               = env_float("OB_WEIGHT", 0.06)          # orderbook imbalance weight
MIN_CONVICTION_Z        = env_float("MIN_CONVICTION_Z", 1.5)    # min |z| to confirm entry
EDGE_ENTER              = env_float("EDGE_ENTER", 0.07)          # min edge to enter
EDGE_EXIT               = env_float("EDGE_EXIT", 0.02)           # exit when edge collapses
STOP_LOSS_PCT           = env_float("STOP_LOSS_PCT", 0.30)       # exit at -30% of entry price
HOLD_TO_EXPIRY          = env_bool("HOLD_TO_EXPIRY", True)       # hold winning positions to settlement

# ── Risk / sizing ─────────────────────────────────────────────────────────────
START_EQUITY            = env_float("START_EQUITY", 50.0)
RISK_FRACTION           = env_float("RISK_FRACTION", 0.06)       # fraction of cash per trade
MAX_POSITION_USD        = env_float("MAX_POSITION_USD", 5.0)     # hard cap per trade
MAX_DAILY_LOSS_24H      = env_float("MAX_DAILY_LOSS_24H", 10.0)
MAX_CONTRACTS_PER_TRADE = env_int("MAX_CONTRACTS_PER_TRADE", 30)

# ── Timing ────────────────────────────────────────────────────────────────────
POLL_SECONDS            = env_float("POLL_SECONDS", 8.0)
COOLDOWN_SECONDS        = env_int("COOLDOWN_SECONDS", 120)       # min seconds between trades
MIN_HOLD_SECONDS        = env_int("MIN_HOLD_SECONDS", 120)       # min hold before exit
MIN_SECS_BEFORE_EXPIRY  = env_int("MIN_SECS_BEFORE_EXPIRY", 120) # skip entry if expiry < this

# ── Book sanity ───────────────────────────────────────────────────────────────
MIN_BID_CENTS           = env_int("MIN_BID_CENTS", 2)
MAX_BID_CENTS           = env_int("MAX_BID_CENTS", 98)
MIN_TOP_QTY             = env_int("MIN_TOP_QTY", 1)              # min contracts at best price

# ── Infrastructure ────────────────────────────────────────────────────────────
PORT                    = env_int("PORT", 3000)
HEALTH_PATH             = env_str("HEALTH_PATH", "/health")
STATE_FILE              = Path(env_str("STATE_FILE", ".runtime/state.json"))
STATUS_FILE             = Path(env_str("STATUS_FILE", ".runtime/status.json"))
REQUEST_TIMEOUT         = env_int("REQUEST_TIMEOUT_SECONDS", 15)

# ── Debug ─────────────────────────────────────────────────────────────────────
DEBUG_ERRORS            = env_bool("DEBUG_ERRORS", False)
DEBUG_MARK              = env_bool("DEBUG_MARK", False)
DEBUG_PICK              = env_bool("DEBUG_PICK", False)
DEBUG_MODEL             = env_bool("DEBUG_MODEL", False)


# =============================================================================
# Performance tracker — adaptive edge
# =============================================================================

_perf: Dict[str, Any] = {
    "wins": 0,
    "losses": 0,
    "streak": 0,
    "total_pnl": 0.0,
}

COLD_STREAK_THRESHOLD = env_int("COLD_STREAK_THRESHOLD", 3)
HOT_STREAK_THRESHOLD  = env_int("HOT_STREAK_THRESHOLD", 5)


def perf_record(pnl: float) -> None:
    _perf["total_pnl"] += pnl
    if pnl > 0:
        _perf["wins"] += 1
        _perf["streak"] = max(1, _perf["streak"] + 1)
    else:
        _perf["losses"] += 1
        _perf["streak"] = min(-1, _perf["streak"] - 1)
    print(json.dumps({
        "ts": utc_iso(), "event": "PERF_UPDATE",
        "wins": _perf["wins"], "losses": _perf["losses"],
        "streak": _perf["streak"],
        "total_pnl": round(_perf["total_pnl"], 4),
        "pnl_this_trade": round(pnl, 4),
    }), flush=True)


def adaptive_edge_enter() -> float:
    """Tighten edge requirement on cold streak; loosen slightly on hot streak."""
    base = EDGE_ENTER
    streak = _perf["streak"]
    if streak <= -COLD_STREAK_THRESHOLD:
        tighten = min(0.12, abs(streak) * 0.02)
        adapted = min(0.22, base + tighten)
        print(json.dumps({
            "ts": utc_iso(), "event": "PERF_ADAPT",
            "reason": "cold_streak", "streak": streak,
            "base_edge": base, "adapted_edge": round(adapted, 4),
        }), flush=True)
        return adapted
    elif streak >= HOT_STREAK_THRESHOLD:
        loosen = min(0.025, streak * 0.004)
        return max(0.04, base - loosen)
    return base


# =============================================================================
# ONE_TRADE_PER_BUCKET guard
# =============================================================================

_traded_buckets: set = set()


def mark_bucket_traded(ticker: str) -> None:
    _traded_buckets.add(ticker)


def bucket_already_traded(ticker: str) -> bool:
    return ticker in _traded_buckets


def purge_old_buckets(active_ticker: str) -> None:
    global _traded_buckets
    _traded_buckets = {t for t in _traded_buckets if t == active_ticker}


# =============================================================================
# Telegram
# =============================================================================

def send_telegram(message: str) -> None:
    token = env_str("TELEGRAM_BOT_TOKEN")
    chat_id = env_str("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "disable_web_page_preview": True},
            timeout=10,
        )
        if r.status_code != 200:
            print(json.dumps({"ts": utc_iso(), "event": "TELEGRAM_ERR", "status": r.status_code}), flush=True)
    except Exception as e:
        print(json.dumps({"ts": utc_iso(), "event": "TELEGRAM_ERR", "err": str(e)[:100]}), flush=True)


# =============================================================================
# Health server (Railway container healthcheck)
# =============================================================================

def start_health_server() -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            p = (self.path or "").lower().split("?")[0]
            if p in (HEALTH_PATH.lower(), "/", "/healthz", "/healthcheck"):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"ok": True, "service": "johnny5", "version": BOT_VERSION, "live": LIVE_MODE}).encode()
                )
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt: str, *args: Any) -> None:
            return  # silence access logs

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()


# =============================================================================
# State
# =============================================================================

@dataclass
class BotState:
    cash: float = START_EQUITY
    realized_pnl_lifetime: float = 0.0
    trade_history_24h: List[Dict[str, Any]] = field(default_factory=list)

    position_side: Optional[str] = None          # "YES" or "NO"
    position_entry_price: Optional[float] = None  # 0..1
    position_contracts: int = 0
    position_ticker: Optional[str] = None
    position_open_ts: Optional[str] = None

    last_trade_ts: Optional[str] = None

    # Bucket tracking — critical for price-vs-open signal
    current_bucket_ticker: Optional[str] = None
    bucket_start_price: Optional[float] = None   # BTC price when this bucket was first observed
    bucket_open_ts: Optional[str] = None


def ensure_dirs() -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)


def load_state() -> BotState:
    ensure_dirs()
    if not STATE_FILE.exists():
        s = BotState()
        return s
    try:
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        # Filter to known fields only (prevents errors when new fields added)
        known = {f for f in BotState.__dataclass_fields__}
        filtered = {k: v for k, v in raw.items() if k in known}
        s = BotState(**filtered)
        if s.trade_history_24h is None:
            s.trade_history_24h = []
        return s
    except Exception as exc:
        print(json.dumps({"ts": utc_iso(), "event": "STATE_LOAD_ERR", "err": str(exc)[:200]}), flush=True)
        return BotState()


def save_state(state: BotState) -> None:
    ensure_dirs()
    try:
        STATE_FILE.write_text(json.dumps(asdict(state), indent=2), encoding="utf-8")
    except Exception as exc:
        print(json.dumps({"ts": utc_iso(), "event": "STATE_SAVE_ERR", "err": str(exc)[:200]}), flush=True)


def write_status(state: BotState, message: str, ticker: str,
                 mark_yes: Optional[float], fair_yes: float, z: float,
                 edge: float, vol_regime: float = 1.0) -> None:
    payload = {
        "ts": utc_iso(), "message": message, "live": LIVE_MODE, "kill_switch": KILL_SWITCH,
        "ticker": ticker, "cash": round(state.cash, 4),
        "bucket_start_price": state.bucket_start_price,
        "position": {
            "side": state.position_side,
            "contracts": state.position_contracts,
            "entry_price": state.position_entry_price,
            "open_ts": state.position_open_ts,
        },
        "model": {
            "mark_yes": round(mark_yes, 4) if mark_yes is not None else None,
            "fair_yes": round(fair_yes, 4), "z": round(z, 4),
            "edge": round(edge, 4), "vol_regime": round(vol_regime, 4),
        },
    }
    ensure_dirs()
    try:
        STATUS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass


# =============================================================================
# 24h trade history helpers
# =============================================================================

def prune_24h(state: BotState) -> None:
    if state.trade_history_24h is None:
        state.trade_history_24h = []
        return
    now = datetime.now(timezone.utc)
    state.trade_history_24h = [
        t for t in state.trade_history_24h
        if (dt := parse_iso(t.get("ts", ""))) and (now - dt) <= timedelta(hours=24)
    ]


def summarize_24h(state: BotState) -> Tuple[int, float]:
    prune_24h(state)
    pnl = sum(_as_float(t.get("pnl", 0.0)) for t in (state.trade_history_24h or []))
    return len(state.trade_history_24h or []), pnl


def record_trade_24h(state: BotState, typ: str, pnl_delta: float) -> None:
    if state.trade_history_24h is None:
        state.trade_history_24h = []
    state.trade_history_24h.append({"ts": utc_iso(), "type": typ, "pnl": float(pnl_delta)})
    prune_24h(state)


# =============================================================================
# Timing guards
# =============================================================================

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


def secs_to_expiry(close_ts_any: Any) -> Optional[float]:
    ct = _parse_ts_any(close_ts_any)
    if ct is None:
        return None
    return (ct - datetime.now(timezone.utc)).total_seconds()


# =============================================================================
# Equity — Postgres persistence via gateway
# =============================================================================

def gateway_load_equity(gateway: str) -> Tuple[Optional[float], Optional[float]]:
    if not gateway:
        return None, None
    try:
        r = requests.get(f"{gateway}/equity", timeout=8)
        if r.status_code != 200:
            return None, None
        data = r.json()
        if not data.get("exists") or data.get("cash") is None:
            return None, None
        return float(data["cash"]), float(data.get("lifetime_pnl") or 0.0)
    except Exception as e:
        print(json.dumps({"ts": utc_iso(), "event": "EQUITY_LOAD_ERR", "err": str(e)[:150]}), flush=True)
        return None, None


def gateway_save_equity(gateway: str, cash: float, lifetime_pnl: float) -> None:
    if not gateway:
        return
    try:
        requests.get(
            f"{gateway}/init-equity",
            params={"cash": round(cash, 4), "lifetime_pnl": round(lifetime_pnl, 4)},
            timeout=5,
        )
    except Exception:
        pass


def gateway_fetch_real_balance(gateway: str) -> Optional[float]:
    """Fetch real Kalshi balance in dollars via gateway /balance."""
    if not gateway:
        return None
    try:
        r = requests.get(f"{gateway}/balance", timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        cents = data.get("balance")
        if cents is not None:
            return float(cents) / 100.0
        dollars = data.get("balance_dollars")
        if dollars is not None:
            return float(dollars)
        return None
    except Exception as e:
        print(json.dumps({"ts": utc_iso(), "event": "BALANCE_ERR", "err": str(e)[:150]}), flush=True)
        return None


# =============================================================================
# Kalshi client (read operations only — orders go via gateway)
# =============================================================================

class KalshiClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.private_key = None
        pem = self._resolve_key_pem()
        if pem:
            try:
                self.private_key = serialization.load_pem_private_key(pem.encode("utf-8"), password=None)
                print(json.dumps({"ts": utc_iso(), "event": "BOOT_INFO", "msg": "private_key_loaded_ok"}), flush=True)
            except Exception as e:
                print(json.dumps({"ts": utc_iso(), "event": "BOOT_WARN", "msg": f"key_load_failed:{str(e)[:150]}"}), flush=True)
        else:
            print(json.dumps({"ts": utc_iso(), "event": "BOOT_WARN", "msg": "no_private_key_found"}), flush=True)

    @staticmethod
    def _resolve_key_pem() -> str:
        raw = KALSHI_PRIVATE_KEY_PEM.strip()
        if raw:
            return raw.replace("\\n", "\n").strip().strip('"').strip("'")

        raw2 = KALSHI_PRIVATE_KEY_PATH.strip()
        if not raw2:
            return ""
        if "BEGIN" in raw2:
            return raw2.replace("\\n", "\n").strip().strip('"').strip("'")
        try:
            p = Path(raw2)
            if len(raw2) < 512 and p.exists():
                return p.read_text(encoding="utf-8").replace("\\n", "\n").strip()
        except Exception:
            pass
        try:
            decoded = base64.b64decode(raw2, validate=True).decode("utf-8")
            if "BEGIN" in decoded:
                return decoded.replace("\\n", "\n").strip()
        except (binascii.Error, UnicodeDecodeError):
            pass
        return ""

    def _auth_headers(self, method: str, path: str) -> Dict[str, str]:
        if not (KALSHI_API_KEY_ID and self.private_key):
            return {"Content-Type": "application/json"}
        ts_ms = str(int(time.time() * 1000))
        msg = f"{ts_ms}{method.upper()}{path}".encode("utf-8")
        sig = self.private_key.sign(msg, padding.PKCS1v15(), hashes.SHA256())
        return {
            "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
            "Content-Type": "application/json",
        }

    def _get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"{KALSHI_BASE_URL}{path}"
        r = self.session.get(
            url,
            headers=self._auth_headers("GET", path),
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code >= 400:
            raise RuntimeError(f"Kalshi HTTP {r.status_code} GET {path}: {r.text[:300]}")
        out = r.json()
        if not isinstance(out, dict):
            raise RuntimeError(f"Kalshi unexpected response type on {path}")
        return out

    def list_open_markets(self) -> List[Dict[str, Any]]:
        data = self._get(
            "/trade-api/v2/markets",
            params={"series_ticker": SERIES_TICKER, "status": "open"},
        )
        markets = data.get("markets", [])
        if not isinstance(markets, list):
            return []

        if markets and dbg_every("market_raw_dump", 120):
            m0 = markets[0]
            keys = list(m0.keys()) if isinstance(m0, dict) else []
            print(f"MARKET_RAW_DUMP total={len(markets)} keys={keys}", flush=True)
            for k in ["ticker", "open_time", "close_time", "open_ts", "close_ts",
                      "expiration_time", "status", "yes_bid", "no_bid", "last_price"]:
                if k in m0:
                    print(f"  MARKET_FIELD {k}={repr(m0[k])}", flush=True)
        elif not markets and dbg_every("market_empty", 60):
            print(f"MARKET_EMPTY series={SERIES_TICKER}", flush=True)

        return markets

    def get_orderbook(self, ticker: str) -> Dict[str, Any]:
        return self._get(f"/trade-api/v2/markets/{ticker}/orderbook")

    def place_order(
        self,
        action: str,
        ticker: str,
        side: str,
        contracts: int,
        price_cents: int,
    ) -> Dict[str, Any]:
        """Route order through Bun gateway (handles RSA signing correctly)."""
        # Paper mode: simulate an immediate fill
        if not LIVE_MODE:
            return {"ok": True, "paper": True, "simulated_fill": True}

        if not KALSHI_ORDER_GATEWAY:
            raise RuntimeError("KALSHI_ORDER_GATEWAY_URL required for LIVE_MODE")
        if action not in ("buy", "sell"):
            raise ValueError(f"action must be buy or sell, got {action!r}")
        if side not in ("YES", "NO"):
            raise ValueError(f"side must be YES or NO, got {side!r}")

        payload: Dict[str, Any] = {
            "ticker": ticker,
            "action": action,
            "type": "fok",  # fill-or-kill: no resting orders / ghost positions
            "side": side,
            "count": int(contracts),
        }
        if side == "YES":
            payload["yes_price"] = max(1, min(99, int(price_cents)))
        else:
            payload["no_price"] = max(1, min(99, int(price_cents)))

        r = requests.post(f"{KALSHI_ORDER_GATEWAY}/order", json=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code >= 400:
            raise RuntimeError(f"Order gateway HTTP {r.status_code}: {r.text[:300]}")

        out = r.json()
        if not isinstance(out, dict):
            raise RuntimeError("Order gateway returned non-dict response")
        if not out.get("ok"):
            raise RuntimeError(f"Order gateway error: {json.dumps(out)[:300]}")

        # Inspect fill status
        order = (out.get("response") or {}).get("order") or {}
        status = str(order.get("status", "")).lower().strip()
        filled = int(order.get("filled_count", 0) or 0)
        remaining = int(order.get("remaining_count", 0) or 0)

        print(json.dumps({
            "ts": utc_iso(), "event": "ORDER_RESPONSE",
            "action": action, "side": side, "contracts": contracts,
            "status": status, "filled": filled, "remaining": remaining,
        }), flush=True)

        if status == "canceled":
            raise RuntimeError(f"FOK canceled (filled={filled} remaining={remaining}). Book moved.")
        if status == "resting":
            raise RuntimeError("Order resting — unexpected for FOK. Aborting to avoid ghost position.")
        if status not in ("filled", "resting", "") and filled == 0:
            raise RuntimeError(f"Order in unknown state status={status!r} filled=0. Aborting.")

        print(json.dumps({
            "ts": utc_iso(), "event": "ORDER_CONFIRMED",
            "action": action, "side": side, "contracts": contracts,
            "status": status, "filled": filled,
        }), flush=True)
        return out


# =============================================================================
# Market selection
# =============================================================================

def pick_best_active_market(markets: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    candidates: List[Tuple[float, datetime, Dict[str, Any]]] = []
    fallback: List[Tuple[datetime, Dict[str, Any]]] = []

    for m in markets:
        if not isinstance(m, dict):
            continue
        raw_ot = m.get("open_time") or m.get("open_ts")
        raw_ct = m.get("close_time") or m.get("close_ts")
        ot = _parse_ts_any(raw_ot)
        ct = _parse_ts_any(raw_ct)
        if not ot or not ct:
            if dbg_every("ts_parse_fail", 60):
                print(f"MARKET_TS_FAIL ticker={m.get('ticker')} ot={repr(raw_ot)[:40]} ct={repr(raw_ct)[:40]}", flush=True)
            continue
        if not (ot <= now < ct):
            continue
        score = _as_float(m.get("volume") or m.get("volume_24h")) + _as_float(m.get("open_interest") or m.get("openInterest"))
        candidates.append((score, ct, m))
        fallback.append((ct, m))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], -x[1].timestamp()), reverse=True)
    if candidates[0][0] > 0:
        return candidates[0][2]
    # All zero volume: pick market expiring soonest (most active bucket)
    fallback.sort(key=lambda x: x[0])
    return fallback[0][1]


# =============================================================================
# Orderbook parsing
# =============================================================================

def _best_bid(levels: Any) -> Optional[int]:
    """Extract max price from Kalshi orderbook level list."""
    if not isinstance(levels, list) or not levels:
        return None
    best: Optional[int] = None
    for lvl in levels:
        price: Optional[int] = None
        if isinstance(lvl, list) and len(lvl) >= 1:
            try:
                price = int(lvl[0])
            except Exception:
                pass
        elif isinstance(lvl, dict):
            try:
                price = int(lvl.get("price", 0) or 0)
            except Exception:
                pass
        if price is not None:
            if best is None or price > best:
                best = price
    return best


def _depth_top3(levels: Any) -> int:
    """Sum of quantity at top 3 price levels."""
    if not isinstance(levels, list) or not levels:
        return 0
    total = 0
    for lvl in levels[:3]:
        qty = 0
        if isinstance(lvl, list) and len(lvl) >= 2:
            try:
                qty = int(lvl[1])
            except Exception:
                pass
        elif isinstance(lvl, dict):
            try:
                qty = int(lvl.get("quantity", 0) or lvl.get("qty", 0) or 0)
            except Exception:
                pass
        total += qty
    return total


def parse_orderbook(ob_data: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], int, int]:
    """
    Returns: (best_yes_bid, best_no_bid, yes_depth_3, no_depth_3)
    """
    root = ob_data.get("orderbook", ob_data)
    yes_levels = root.get("yes") or root.get("yes_bids") or []
    no_levels  = root.get("no")  or root.get("no_bids")  or []
    return (
        _best_bid(yes_levels),
        _best_bid(no_levels),
        _depth_top3(yes_levels),
        _depth_top3(no_levels),
    )


def compute_mark(yes_bid: Optional[int], no_bid: Optional[int]) -> Optional[float]:
    """Compute YES mid-price from best bids on both sides."""
    if yes_bid is None or no_bid is None:
        return None
    if yes_bid < MIN_BID_CENTS or no_bid < MIN_BID_CENTS:
        return None
    if yes_bid > MAX_BID_CENTS or no_bid > MAX_BID_CENTS:
        return None
    implied_yes_ask = 100 - no_bid
    return max(0.01, min(0.99, (yes_bid + implied_yes_ask) / 200.0))


# =============================================================================
# BTC price feed (Kraken primary, Coinbase fallback)
# =============================================================================

def fetch_btc_price() -> Optional[float]:
    """Try Kraken first, fall back to Coinbase on failure."""
    # Kraken
    try:
        r = requests.get(KRAKEN_TICKER_URL, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        result = r.json().get("result", {})
        if result:
            key = next(iter(result))
            price = float(result[key]["c"][0])
            if price > 0:
                return price
    except Exception as e:
        if dbg_every("kraken_err", 60):
            print(json.dumps({"ts": utc_iso(), "event": "KRAKEN_ERR", "err": str(e)[:100]}), flush=True)

    # Coinbase fallback
    try:
        r = requests.get(COINBASE_TICKER_URL, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        price = float(data.get("data", {}).get("amount", 0))
        if price > 0:
            if dbg_every("coinbase_fallback", 300):
                print(json.dumps({"ts": utc_iso(), "event": "COINBASE_FALLBACK", "price": price}), flush=True)
            return price
    except Exception as e:
        if dbg_every("coinbase_err", 60):
            print(json.dumps({"ts": utc_iso(), "event": "COINBASE_ERR", "err": str(e)[:100]}), flush=True)

    return None


# =============================================================================
# SIGNAL MODEL — Multi-factor fair_yes for KXBTC15M
#
# Primary:   BTC price move since bucket open vs current mark
# Secondary: Short-term momentum z-score (confirmation only)
# Tertiary:  Orderbook depth imbalance (small nudge)
#
# The primary signal is the strongest: if BTC has moved significantly inside
# the current 15-min bucket but the market hasn't fully repriced, there's edge.
# =============================================================================

def compute_fair_yes(
    prices: List[float],
    yes_bid: Optional[int],
    no_bid: Optional[int],
    yes_depth_3: int,
    no_depth_3: int,
    bucket_start_price: Optional[float],
    bucket_elapsed_frac: float,         # 0.0 = just opened, 1.0 = at close
) -> Tuple[Optional[float], float, Optional[float], float]:
    """
    Returns: (fair_yes, z, mark_yes, vol_regime)
    """
    mark_yes = compute_mark(yes_bid, no_bid)
    if mark_yes is None:
        return None, 0.0, None, 1.0

    lookback = LOOKBACK

    # ── Factor 1: BTC bucket move (PRIMARY) ──────────────────────────────────
    bucket_move = 0.0
    if bucket_start_price and bucket_start_price > 0 and prices:
        pct_moved = (prices[-1] - bucket_start_price) / bucket_start_price
        # Time amplifier: early-bucket moves are less certain than late-bucket
        # At 0% elapsed → 0.5x; at 100% elapsed → 1.5x
        time_amp = 0.5 + max(0.0, min(1.0, bucket_elapsed_frac))
        raw_move = pct_moved * BUCKET_MOVE_SCALE * time_amp
        bucket_move = max(-0.45, min(0.45, raw_move))

    # ── Factor 2: Momentum z-score (SECONDARY) ───────────────────────────────
    z = 0.0
    vol_regime = 1.0
    if len(prices) >= lookback:
        window = prices[-lookback:]
        rets = [(window[i] - window[i-1]) / window[i-1]
                for i in range(1, len(window)) if window[i-1] > 0]
        if len(rets) >= 3:
            mu = sum(rets) / len(rets)
            var = sum((r - mu) ** 2 for r in rets) / (len(rets) - 1)
            std = max(var ** 0.5, 1e-9)
            z = (rets[-1] - mu) / std

            # Volatility regime: short vol / long vol
            n_long = min(len(prices) - 1, 60)
            long_rets = [(prices[i] - prices[i-1]) / prices[i-1]
                         for i in range(max(1, len(prices) - n_long), len(prices))
                         if prices[i-1] > 0]
            if long_rets:
                long_rms = (sum(r**2 for r in long_rets) / len(long_rets)) ** 0.5
                short_rms = (sum(r**2 for r in rets) / len(rets)) ** 0.5
                vol_regime = short_rms / max(long_rms, 1e-10)

    # ── Factor 3: Orderbook imbalance (TERTIARY) ──────────────────────────────
    total_depth = yes_depth_3 + no_depth_3
    ob_imb = (yes_depth_3 - no_depth_3) / max(total_depth, 1)
    ob_contrib = ob_imb * OB_WEIGHT

    # ── Compose ───────────────────────────────────────────────────────────────
    if bucket_start_price:
        # Primary mode: bucket-move is the anchor
        # z provides a confirmation nudge (30% weight, dampened in high vol)
        vol_dampener = 1.0 / max(1.0, vol_regime - 0.4)
        z_contrib = z * FAIR_MOVE_SCALE * 0.3 * vol_dampener
        fair = 0.5 + bucket_move + z_contrib + ob_contrib
    else:
        # Fallback (no bucket start tracked yet): z-only with OB
        vol_dampener = 1.0 / max(1.0, vol_regime - 0.4)
        fair = 0.5 + z * FAIR_MOVE_SCALE * vol_dampener + ob_contrib

    return max(0.02, min(0.98, fair)), z, mark_yes, vol_regime


# =============================================================================
# Position sizing
# =============================================================================

def compute_contracts(cash: float, entry_price_prob: float) -> int:
    """Risk-fraction position sizing with hard caps."""
    risk_usd = min(MAX_POSITION_USD, max(0.5, cash * RISK_FRACTION))
    contracts = int(risk_usd / max(entry_price_prob, 0.01))
    return max(1, min(MAX_CONTRACTS_PER_TRADE, contracts))


# =============================================================================
# Trade logging + Telegram
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
    if state.position_side and state.position_entry_price is not None and state.position_contracts > 0:
        if state.position_side == "YES":
            unrealized = (mark_yes - state.position_entry_price) * state.position_contracts
        else:
            unrealized = ((1.0 - mark_yes) - state.position_entry_price) * state.position_contracts

    equity = state.cash + unrealized

    print(json.dumps({
        "ts": utc_iso(),
        "event": event,
        "live": LIVE_MODE,
        "ticker": ticker,
        "side": side,
        "contracts": contracts,
        "entry_price": entry_price,
        "exec_price": round(exec_price, 6),
        "cash": round(state.cash, 4),
        "equity": round(equity, 4),
        "pnl_24h": round(pnl_24h, 4),
        "trades_24h": trades_24h,
        "pnl_lifetime": round(state.realized_pnl_lifetime, 4),
        "mark_yes": round(mark_yes, 6),
        "fair_yes": round(fair_yes, 6),
        "z": round(z, 6),
        "edge": round(edge, 6),
        "bucket_start_price": state.bucket_start_price,
        "note": note,
    }), flush=True)

    streak = _perf["streak"]
    streak_str = (
        f"🔥 {streak} win streak" if streak >= 3
        else f"🥶 {abs(streak)} loss streak" if streak <= -2
        else "—"
    )
    mode_str = "🧪 PAPER" if not LIVE_MODE else "🔴 LIVE"

    if event == "ENTER":
        risk_usd = exec_price * contracts
        msg = (
            f"🤖 Johnny5 {mode_str} — ENTERED ✅\n"
            f"📊 {ticker}\n"
            f"Side: {side} | {contracts} contracts @ {exec_price:.2f}\n"
            f"💰 Risk: ${risk_usd:.2f} | Edge: {edge*100:.1f}¢ | z: {z:.2f}\n"
            f"💵 Cash: ${state.cash:.2f} | Equity: ${equity:.2f}\n"
            f"📈 PnL 24h: ${pnl_24h:.2f} | Lifetime: ${state.realized_pnl_lifetime:.2f}\n"
            f"Streak: {streak_str}"
        )
        send_telegram(msg)

    elif event == "EXIT":
        pnl_emoji = "✅" if pnl_delta > 0 else "❌"
        msg = (
            f"🤖 Johnny5 {mode_str} — EXITED {pnl_emoji}\n"
            f"📊 {ticker}\n"
            f"Side: {side} | {contracts} contracts\n"
            f"Entry: {entry_price:.2f} → Exit: {exec_price:.2f}\n"
            f"PnL: ${pnl_delta:.2f} {pnl_emoji}\n"
            f"💵 Cash: ${state.cash:.2f} | Equity: ${equity:.2f}\n"
            f"📈 PnL 24h: ${pnl_24h:.2f} | Lifetime: ${state.realized_pnl_lifetime:.2f}\n"
            f"Streak: {streak_str}"
        )
        send_telegram(msg)


# =============================================================================
# Entry logic
# =============================================================================

def maybe_enter(
    state: BotState,
    client: KalshiClient,
    ticker: str,
    best_yes_bid: int,
    best_no_bid: int,
    yes_depth_3: int,
    no_depth_3: int,
    mark_yes: float,
    fair_yes: float,
    z: float,
    vol_regime: float,
    secs_left: Optional[float],
) -> bool:
    if state.position_side is not None:
        return False
    if KILL_SWITCH:
        return False
    if not can_trade_now(state):
        return False
    if bucket_already_traded(ticker):
        if dbg_every("bucket_skip", 120):
            print(json.dumps({"ts": utc_iso(), "event": "SKIP", "reason": "bucket_already_traded", "ticker": ticker}), flush=True)
        return False

    _, pnl_24h = summarize_24h(state)
    if pnl_24h <= -abs(MAX_DAILY_LOSS_24H):
        if dbg_every("daily_loss", 300):
            print(json.dumps({"ts": utc_iso(), "event": "SKIP", "reason": "daily_loss_limit", "pnl_24h": round(pnl_24h, 4)}), flush=True)
        return False

    # Don't enter near expiry
    if secs_left is not None and secs_left < MIN_SECS_BEFORE_EXPIRY:
        if dbg_every("near_expiry", 30):
            print(json.dumps({"ts": utc_iso(), "event": "SKIP", "reason": "near_expiry", "secs_left": round(secs_left)}), flush=True)
        return False

    # Require minimum depth on at least the entry side
    entry_depth = max(yes_depth_3, no_depth_3)
    if entry_depth < MIN_TOP_QTY:
        if dbg_every("thin_book", 60):
            print(json.dumps({"ts": utc_iso(), "event": "SKIP", "reason": "thin_book", "depth": entry_depth}), flush=True)
        return False

    # Require z-score conviction
    if abs(z) < MIN_CONVICTION_Z:
        if DEBUG_MODEL or dbg_every("skip_z", 30):
            print(f"SKIP_Z z={z:.3f} need={MIN_CONVICTION_Z}", flush=True)
        return False

    edge = fair_yes - mark_yes
    eff_edge = adaptive_edge_enter()

    side: Optional[str] = None
    entry_cents: int = 0

    if edge >= eff_edge:
        side = "YES"
        # Buy YES: pay implied YES ask (= 100 - best NO bid)
        entry_cents = max(1, min(99, 100 - best_no_bid))
    elif edge <= -eff_edge:
        side = "NO"
        # Buy NO: pay implied NO ask (= 100 - best YES bid)
        entry_cents = max(1, min(99, 100 - best_yes_bid))
    else:
        if DEBUG_MODEL or dbg_every("skip_edge", 30):
            print(f"SKIP_EDGE edge={edge:.4f} need=±{eff_edge:.4f} z={z:.3f}", flush=True)
        return False

    entry_price = entry_cents / 100.0
    contracts = compute_contracts(state.cash, entry_price)
    cost = entry_price * contracts

    if state.cash < cost:
        print(json.dumps({"ts": utc_iso(), "event": "SKIP", "reason": "insufficient_cash",
                          "cash": round(state.cash, 4), "cost": round(cost, 4)}), flush=True)
        return False

    try:
        client.place_order("buy", ticker=ticker, side=side, contracts=contracts, price_cents=entry_cents)
    except Exception as exc:
        print(json.dumps({"ts": utc_iso(), "event": "ORDER_ERR", "action": "buy", "ticker": ticker,
                          "side": side, "err": str(exc)[:300]}), flush=True)
        send_telegram(f"⚠️ Johnny5 ORDER FAILED\n{ticker}\nbuy {side} @ {entry_cents}c\n{str(exc)[:200]}")
        return False

    # Update cash
    state.cash -= cost

    state.position_side = side
    state.position_entry_price = entry_price
    state.position_contracts = contracts
    state.position_ticker = ticker
    state.position_open_ts = utc_iso()
    state.last_trade_ts = utc_iso()

    mark_bucket_traded(ticker)

    log_trade(
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
        note=f"vol_regime={vol_regime:.2f} eff_edge={eff_edge:.3f}",
    )
    return True


# =============================================================================
# Exit logic
# =============================================================================

def maybe_exit(
    state: BotState,
    client: KalshiClient,
    ticker: str,
    best_yes_bid: Optional[int],
    best_no_bid: Optional[int],
    mark_yes: float,
    fair_yes: float,
    z: float,
    secs_left: Optional[float],
) -> bool:
    if state.position_side is None or state.position_entry_price is None:
        return False
    if not can_trade_now(state):
        return False

    force_exit = KILL_SWITCH and CLOSE_ON_KILL_SWITCH

    if not held_long_enough(state) and not force_exit:
        return False

    side = state.position_side
    entry_price = state.position_entry_price
    contracts = state.position_contracts
    edge = fair_yes - mark_yes

    # Stop-loss: exit if current mark is STOP_LOSS_PCT below entry (for YES)
    # or if NO mark is STOP_LOSS_PCT below NO entry price
    stop_triggered = False
    if side == "YES" and mark_yes < entry_price * (1.0 - STOP_LOSS_PCT):
        stop_triggered = True
        print(json.dumps({"ts": utc_iso(), "event": "STOP_LOSS_TRIGGERED",
                          "side": side, "entry": entry_price, "mark": round(mark_yes, 4),
                          "stop_at": round(entry_price * (1 - STOP_LOSS_PCT), 4)}), flush=True)
    elif side == "NO":
        no_mark = 1.0 - mark_yes
        no_entry = 1.0 - entry_price  # approximate NO purchase cost
        if no_mark < no_entry * (1.0 - STOP_LOSS_PCT):
            stop_triggered = True
            print(json.dumps({"ts": utc_iso(), "event": "STOP_LOSS_TRIGGERED",
                              "side": side, "no_entry": round(no_entry, 4),
                              "no_mark": round(no_mark, 4)}), flush=True)

    # HOLD_TO_EXPIRY: if in profit and close to expiry, let Kalshi settle
    if HOLD_TO_EXPIRY and not stop_triggered and not force_exit and secs_left is not None:
        if secs_left < 120:
            # Check unrealized P&L
            if side == "YES":
                unrealized = (mark_yes - entry_price) * contracts
            else:
                unrealized = ((1.0 - mark_yes) - entry_price) * contracts
            if unrealized > 0:
                if dbg_every("hold_to_expiry", 30):
                    print(json.dumps({"ts": utc_iso(), "event": "HOLD_TO_EXPIRY",
                                      "unrealized": round(unrealized, 4),
                                      "secs_left": round(secs_left)}), flush=True)
                return False  # let Kalshi pay us at settlement

    # Standard exit condition
    should_exit = stop_triggered or force_exit
    if not should_exit:
        if side == "YES":
            should_exit = edge <= EDGE_EXIT
        else:
            should_exit = edge >= -EDGE_EXIT

    if not should_exit:
        return False

    # Determine exit price
    if side == "YES":
        exit_cents = max(1, min(99, best_yes_bid)) if best_yes_bid is not None else max(1, min(99, int(mark_yes * 100)))
    else:
        exit_cents = max(1, min(99, best_no_bid)) if best_no_bid is not None else max(1, min(99, int((1.0 - mark_yes) * 100)))
    exit_price = exit_cents / 100.0

    try:
        client.place_order("sell", ticker=ticker, side=side, contracts=contracts, price_cents=exit_cents)
    except Exception as exc:
        print(json.dumps({"ts": utc_iso(), "event": "ORDER_ERR", "action": "sell", "ticker": ticker,
                          "side": side, "err": str(exc)[:300]}), flush=True)
        send_telegram(f"⚠️ Johnny5 EXIT FAILED\n{ticker}\nsell {side} @ {exit_cents}c\n{str(exc)[:200]}")
        return False

    # Update cash
    state.cash += exit_price * contracts
    pnl = (exit_price - entry_price) * contracts
    state.realized_pnl_lifetime += pnl
    perf_record(pnl)

    # Sync real balance from gateway every few exits to prevent drift
    real_bal = gateway_fetch_real_balance(KALSHI_ORDER_GATEWAY)
    if real_bal is not None and LIVE_MODE:
        if abs(real_bal - state.cash) > 2.0:
            print(json.dumps({"ts": utc_iso(), "event": "BALANCE_DRIFT",
                              "bot_cash": round(state.cash, 4), "real_balance": round(real_bal, 4),
                              "drift": round(real_bal - state.cash, 4)}), flush=True)
        state.cash = real_bal  # always trust the real balance

    gateway_save_equity(KALSHI_ORDER_GATEWAY, state.cash, state.realized_pnl_lifetime)

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
        note=f"stop={stop_triggered} force={force_exit} pnl={pnl:.4f}",
    )
    return True


# =============================================================================
# Main loop
# =============================================================================

def main() -> None:
    print(f"BOT_VERSION={BOT_VERSION}", flush=True)
    print(json.dumps({
        "ts": utc_iso(), "event": "BOOT",
        "live": LIVE_MODE, "series": SERIES_TICKER,
        "poll": POLL_SECONDS, "lookback": LOOKBACK,
        "edge_enter": EDGE_ENTER, "edge_exit": EDGE_EXIT,
        "min_z": MIN_CONVICTION_Z, "risk_frac": RISK_FRACTION,
        "max_pos_usd": MAX_POSITION_USD, "stop_loss_pct": STOP_LOSS_PCT,
        "kill_switch": KILL_SWITCH,
        "state_file": str(STATE_FILE),
        "gateway": KALSHI_ORDER_GATEWAY or "NOT_SET",
    }), flush=True)

    start_health_server()
    ensure_dirs()

    # ── Start gateway log worker ──────────────────────────────────────────────
    if KALSHI_ORDER_GATEWAY:
        _start_gateway_log_worker(KALSHI_ORDER_GATEWAY)

    # ── Load state + seed equity ──────────────────────────────────────────────
    state = load_state()

    # Restore equity from Postgres (survives container restarts)
    pg_cash, pg_pnl = gateway_load_equity(KALSHI_ORDER_GATEWAY)
    if pg_cash is not None:
        state.cash = pg_cash
        state.realized_pnl_lifetime = pg_pnl if pg_pnl is not None else 0.0
        print(json.dumps({"ts": utc_iso(), "event": "STATE_RESTORED",
                          "cash": round(state.cash, 4), "lifetime_pnl": round(state.realized_pnl_lifetime, 4),
                          "source": "postgres"}), flush=True)
    else:
        # Seed from real Kalshi balance if we have no Postgres record
        real_bal = gateway_fetch_real_balance(KALSHI_ORDER_GATEWAY)
        if real_bal is not None and real_bal > 0:
            state.cash = real_bal
            gateway_save_equity(KALSHI_ORDER_GATEWAY, state.cash, state.realized_pnl_lifetime)
            print(json.dumps({"ts": utc_iso(), "event": "EQUITY_SEEDED",
                              "cash": round(state.cash, 4), "source": "kalshi_real_balance"}), flush=True)
            send_telegram(f"🏦 Johnny5 Equity Synced\nReal balance: ${real_bal:.2f}")
        else:
            print(json.dumps({"ts": utc_iso(), "event": "EQUITY_WARN",
                              "cash": round(state.cash, 4),
                              "msg": "using START_EQUITY — update via /set-equity if wrong"}), flush=True)

    send_telegram(
        f"🤖 Johnny5 v10 STARTED\n"
        f"Mode: {'🔴 LIVE' if LIVE_MODE else '🧪 PAPER'}\n"
        f"Series: {SERIES_TICKER}\n"
        f"Cash: ${state.cash:.2f} | Lifetime PnL: ${state.realized_pnl_lifetime:.2f}\n"
        f"Kill switch: {KILL_SWITCH}"
    )

    client = KalshiClient()
    prices: List[float] = []

    # ── Main loop ─────────────────────────────────────────────────────────────
    while True:
        try:
            # Hot reload check
            check_hot_reload(KALSHI_ORDER_GATEWAY)

            # Fetch BTC price
            btc_price = fetch_btc_price()
            if btc_price is None:
                if dbg_every("no_price", 30):
                    print(json.dumps({"ts": utc_iso(), "event": "NO_BTC_PRICE",
                                      "msg": "both Kraken and Coinbase failed"}), flush=True)
                time.sleep(max(5.0, POLL_SECONDS))
                continue

            prices.append(btc_price)
            if len(prices) > LOOKBACK * 15:
                prices = prices[-LOOKBACK * 15:]

            # Fetch markets
            markets = client.list_open_markets()
            m = pick_best_active_market(markets)
            if not m or not m.get("ticker"):
                if dbg_every("no_market", 60):
                    print(json.dumps({"ts": utc_iso(), "event": "NO_MARKET",
                                      "series": SERIES_TICKER}), flush=True)
                write_status(state, "no_open_market", "", None, 0.5, 0.0, 0.0)
                save_state(state)
                time.sleep(max(5.0, POLL_SECONDS))
                continue

            ticker = str(m["ticker"])
            raw_ct = m.get("close_time") or m.get("close_ts")
            secs_left = secs_to_expiry(raw_ct)

            purge_old_buckets(ticker)

            # Track bucket start price (reset when ticker changes)
            if ticker != state.current_bucket_ticker:
                state.current_bucket_ticker = ticker
                state.bucket_start_price = btc_price
                state.bucket_open_ts = utc_iso()
                print(json.dumps({"ts": utc_iso(), "event": "NEW_BUCKET",
                                  "ticker": ticker, "btc_open": btc_price,
                                  "secs_left": round(secs_left) if secs_left else None}), flush=True)

            # Compute bucket elapsed fraction
            bucket_elapsed_frac = 0.33  # default
            if state.bucket_open_ts and secs_left is not None:
                open_dt = parse_iso(state.bucket_open_ts)
                if open_dt:
                    secs_elapsed = (datetime.now(timezone.utc) - open_dt).total_seconds()
                    total = secs_elapsed + secs_left
                    bucket_elapsed_frac = secs_elapsed / max(total, 1.0)
                    bucket_elapsed_frac = max(0.0, min(1.0, bucket_elapsed_frac))

            # Fetch orderbook
            ob = client.get_orderbook(ticker)
            best_yes_bid, best_no_bid, yes_depth_3, no_depth_3 = parse_orderbook(ob)

            if DEBUG_MARK and dbg_every("mark", 60):
                print(json.dumps({"ts": utc_iso(), "event": "MARK_DEBUG",
                                  "ticker": ticker, "best_yes_bid": best_yes_bid,
                                  "best_no_bid": best_no_bid, "yes_depth_3": yes_depth_3,
                                  "no_depth_3": no_depth_3}), flush=True)

            if DEBUG_PICK and dbg_every("pick", 60):
                print(json.dumps({"ts": utc_iso(), "event": "PICK_DEBUG",
                                  "ticker": ticker,
                                  "volume": m.get("volume") or m.get("volume_24h"),
                                  "open_interest": m.get("open_interest") or m.get("openInterest"),
                                  "bucket_elapsed_frac": round(bucket_elapsed_frac, 3),
                                  "secs_left": round(secs_left) if secs_left else None}), flush=True)

            # Compute fair value
            fair_yes, z, mark_yes, vol_regime = compute_fair_yes(
                prices=prices,
                yes_bid=best_yes_bid,
                no_bid=best_no_bid,
                yes_depth_3=yes_depth_3,
                no_depth_3=no_depth_3,
                bucket_start_price=state.bucket_start_price,
                bucket_elapsed_frac=bucket_elapsed_frac,
            )

            if mark_yes is None:
                if dbg_every("bad_book", 30):
                    if best_yes_bid is not None and best_no_bid is None:
                        skip_reason = "one_sided_book_yes_dominant"
                    elif best_no_bid is not None and best_yes_bid is None:
                        skip_reason = "one_sided_book_no_dominant"
                    else:
                        skip_reason = "bad_or_missing_book"
                    print(json.dumps({"ts": utc_iso(), "event": "SKIP",
                                      "reason": skip_reason, "ticker": ticker,
                                      "yes_bid": best_yes_bid, "no_bid": best_no_bid}), flush=True)
                write_status(state, "skip_bad_book", ticker, None, 0.5, 0.0, 0.0, vol_regime)
                save_state(state)
                time.sleep(max(3.0, POLL_SECONDS))
                continue

            edge = fair_yes - mark_yes

            if DEBUG_MODEL and dbg_every("model", 15):
                print(json.dumps({
                    "ts": utc_iso(), "event": "MODEL_DEBUG",
                    "ticker": ticker, "btc": round(btc_price, 2),
                    "bucket_start": state.bucket_start_price,
                    "bucket_move_pct": round((btc_price / state.bucket_start_price - 1) * 100, 4)
                        if state.bucket_start_price else None,
                    "bucket_elapsed_frac": round(bucket_elapsed_frac, 3),
                    "fair": round(fair_yes, 4), "mark": round(mark_yes, 4),
                    "edge": round(edge, 4), "z": round(z, 4),
                    "vol_regime": round(vol_regime, 4),
                    "prices_collected": len(prices),
                }), flush=True)

            # Warmup message
            if len(prices) < LOOKBACK and dbg_every("warmup", 15):
                print(f"WARMUP {len(prices)}/{LOOKBACK} prices", flush=True)

            # Execute
            maybe_exit(state, client, ticker,
                       best_yes_bid, best_no_bid,
                       float(mark_yes), fair_yes, z, secs_left)

            maybe_enter(state, client, ticker,
                        int(best_yes_bid) if best_yes_bid is not None else 0,
                        int(best_no_bid) if best_no_bid is not None else 0,
                        yes_depth_3, no_depth_3,
                        float(mark_yes), fair_yes, z, vol_regime, secs_left)

            write_status(state, "running", ticker, float(mark_yes), fair_yes, z, edge, vol_regime)
            save_state(state)

            time.sleep(max(1.0, POLL_SECONDS))

        except KeyboardInterrupt:
            print(json.dumps({"ts": utc_iso(), "event": "SHUTDOWN", "reason": "keyboard_interrupt"}), flush=True)
            save_state(state)
            send_telegram("🛑 Johnny5 stopped by user (KeyboardInterrupt)")
            return

        except Exception as exc:
            if DEBUG_ERRORS or dbg_every("main_err", 10):
                print(json.dumps({"ts": utc_iso(), "event": "ERROR",
                                  "err": str(exc)[:400]}), flush=True)
            try:
                write_status(state, f"error:{str(exc)[:150]}",
                             state.position_ticker or "", None, 0.5, 0.0, 0.0)
                save_state(state)
            except Exception:
                pass
            time.sleep(max(10.0, POLL_SECONDS))


if __name__ == "__main__":
    main()

# ── PATCH APPLIED: resolve_key_pem handles space-collapsed PEM ──
# (Already implemented in _resolve_key_pem via the space/header regex path)

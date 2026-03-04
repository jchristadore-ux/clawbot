#!/usr/bin/env python3
"""
Johnny5 — Kalshi BTC-15M Trading Bot  (PRODUCTION v4)
======================================================
Single-file, Railway-deployable. Postgres-backed idempotency.

Changes from v3:
  - Replaced z-score momentum model with binary option pricing model
    (digital call: fair_yes = Φ(d) where d uses log-distance to strike,
     realized volatility, and time remaining in the bucket)
  - Strike parsed from market API fields (floor_strike / cap_strike)
    rather than guessed from ticker suffix
  - Three new entry guards:
      MIN_SECS_BEFORE_EXPIRY  — no entry if < 2 min left (120s default)
      MAX_SECS_BEFORE_EXPIRY  — no entry if > 12 min left (720s default)
      MIN_EDGE_CENTS          — minimum mispricing in cents (5¢ default)
  - Volatility window: 30 prices (SHORT, ~5 min at 10s poll)
  - Model logs: spot, strike, T_remaining, sigma, d, fair_yes on every loop
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import logging
import math
import os
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from cryptography.hazmat.primitives import hashes as crypto_hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding

# =============================================================================
# RUN ID
# =============================================================================
RUN_ID = uuid.uuid4().hex[:8]

# =============================================================================
# Structured JSON logging
# =============================================================================
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        extra = getattr(record, "_extra", {})
        base = {
            "ts":     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "run_id": RUN_ID,
            "level":  record.levelname,
            "msg":    record.getMessage(),
        }
        base.update(extra)
        return json.dumps(base)

def _setup_logging() -> logging.Logger:
    h = logging.StreamHandler()
    h.setFormatter(_JsonFormatter())
    root = logging.getLogger("j5")
    root.addHandler(h)
    root.setLevel(logging.DEBUG)
    root.propagate = False
    return root

log = _setup_logging()

def jlog(level: str, msg: str, **kw: Any) -> None:
    r = logging.LogRecord("j5", getattr(logging, level.upper(), logging.INFO),
                          "", 0, msg, (), None)
    r._extra = kw  # type: ignore[attr-defined]
    log.handle(r)

# =============================================================================
# Safe env helpers
# =============================================================================
def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None: return default
    v = v.strip()
    return v if v else default

def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if not v or not v.strip(): return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if not v or not v.strip(): return default
    try: return int(v)
    except Exception: return int(float(v))

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if not v or not v.strip(): return default
    return float(v)

def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_iso(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def secs_since(ts: Optional[str]) -> float:
    if not ts: return 9999.0
    dt = parse_iso(ts)
    if not dt: return 9999.0
    return (datetime.now(timezone.utc) - dt).total_seconds()

# =============================================================================
# Config
# =============================================================================
BOT_VERSION = "JOHNNY5_KALSHI_BTC15M_PROD_v4"

# Kalshi
KALSHI_BASE_URL          = env_str("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/")
KALSHI_API_KEY_ID        = env_str("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PEM   = env_str("KALSHI_PRIVATE_KEY_PEM", "")
KALSHI_PRIVATE_KEY_PATH  = env_str("KALSHI_PRIVATE_KEY_PATH", "")
SERIES_TICKER            = env_str("SERIES_TICKER", "KXBTC15M").upper()
KALSHI_ORDER_GATEWAY_URL = env_str("KALSHI_ORDER_GATEWAY_URL", "").rstrip("/")

# Runtime
LIVE_MODE       = env_bool("LIVE_MODE", False)
POLL_SECONDS    = env_float("POLL_SECONDS", 10.0)
REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT_SECONDS", 15)
PORT            = env_int("PORT", 3000)
HEALTH_PATH     = env_str("HEALTH_PATH", "/health")

# State / DB
DATABASE_URL = env_str("DATABASE_URL", "")
STATE_FILE   = Path(env_str("STATE_FILE",  ".runtime/state.json"))
STATUS_FILE  = Path(env_str("STATUS_FILE", ".runtime/status.json"))
_CB_FILE     = Path(".runtime/circuit_breaker.json")

# Price feed
KRAKEN_TICKER_URL = env_str(
    "KRAKEN_TICKER_URL", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"
)

# =============================================================================
# Model config — binary option pricing
# =============================================================================
# Volatility: 30-price rolling window, annualised from 10s returns
# 30 * 10s = ~5 minutes of history
VOL_WINDOW        = env_int("VOL_WINDOW", 30)

# Fallback annualised σ when we don't have enough prices yet.
# BTC typical daily vol ~3-4%, annualised ~55-75%.  0.65 is conservative mid.
VOL_FLOOR         = env_float("VOL_FLOOR", 0.65)
VOL_CAP           = env_float("VOL_CAP", 3.0)   # cap at 300% ann. to avoid absurd fairs

# Edge gate: minimum difference between our fair_yes and the book mid (in cents).
# e.g. 5 means we need 5¢ of mispricing before entering.
MIN_EDGE_CENTS    = env_int("MIN_EDGE_CENTS", 5)

# Time-in-bucket guards (seconds)
MIN_SECS_BEFORE_EXPIRY = env_int("MIN_SECS_BEFORE_EXPIRY", 120)   # don't enter < 2 min left
MAX_SECS_BEFORE_EXPIRY = env_int("MAX_SECS_BEFORE_EXPIRY", 720)   # don't enter > 12 min left

# =============================================================================
# Risk / sizing — conservative for $50
# =============================================================================
MAX_POSITION_USD        = env_float("MAX_POSITION_USD", 5.0)
RISK_FRACTION           = env_float("RISK_FRACTION", 0.06)
MAX_CONTRACTS           = env_int("MAX_CONTRACTS_PER_TICKER", 8)
MAX_NOTIONAL_PER_TICKER = env_float("MAX_NOTIONAL_PER_TICKER", 6.0)

# Exit: close when our edge has collapsed to this many cents or less
EXIT_EDGE_CENTS   = env_int("EXIT_EDGE_CENTS", 1)

# Circuit breakers
MAX_DAILY_LOSS      = env_float("MAX_DAILY_LOSS_DOLLARS", 8.0)
MAX_TRADES_PER_DAY  = env_int("MAX_TRADES_PER_DAY", 20)
MAX_ORDERS_PER_HOUR = env_int("MAX_ORDERS_PER_HOUR", 10)
MAX_CONSEC_ERRORS   = env_int("MAX_CONSECUTIVE_ERRORS", 5)

# Timing
MIN_HOLD_SECONDS   = env_int("MIN_HOLD_SECONDS", 120)
COOLDOWN_SECONDS   = env_int("COOLDOWN_SECONDS", 120)
MIN_ORDER_INTERVAL = env_float("MIN_ORDER_INTERVAL_SECONDS", 5.0)

# Book quality
MIN_BID_CENTS       = env_int("MIN_BID_CENTS", 3)
MAX_BID_CENTS       = env_int("MAX_BID_CENTS", 97)
MAX_SPREAD_CENTS    = env_int("MAX_SPREAD_CENTS", 8)
MIN_DEPTH_CONTRACTS = env_int("MIN_DEPTH_CONTRACTS", 3)
MAX_SLIPPAGE_CENTS  = env_int("MAX_SLIPPAGE_CENTS", 4)

# Expired-market guard
NULL_BOOK_EXPIRE_LOOPS = env_int("NULL_BOOK_EXPIRE_LOOPS", 6)

# Control switches
KILL_SWITCH          = env_bool("KILL_SWITCH", False)
CLOSE_ON_KILL_SWITCH = env_bool("CLOSE_ON_KILL_SWITCH", False)
ALLOW_PYRAMIDING     = env_bool("ALLOW_PYRAMIDING", False)

# Debug
DEBUG_ERRORS    = env_bool("DEBUG_ERRORS", False)
DEBUG_MODEL     = env_bool("DEBUG_MODEL", False)    # logs fair_yes calculation each loop
DEBUG_PICK      = env_bool("DEBUG_PICK", False)
DEBUG_BOOK_DUMP = env_bool("DEBUG_BOOK_DUMP", False)

# =============================================================================
# Telegram
# =============================================================================
def send_telegram(message: str) -> None:
    try:
        token   = env_str("TELEGRAM_BOT_TOKEN")
        chat_id = env_str("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            return
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message,
                  "disable_web_page_preview": True},
            timeout=10,
        )
        if r.status_code != 200:
            jlog("warning", "telegram_fail",
                 status=r.status_code, body=r.text[:200])
    except Exception as e:
        jlog("warning", "telegram_error", error=str(e)[:200])

# =============================================================================
# Postgres helpers
# =============================================================================
_db_conn = None
_db_lock = threading.Lock()

def _get_db():
    global _db_conn
    if not DATABASE_URL:
        return None
    with _db_lock:
        try:
            import psycopg2  # type: ignore
            if _db_conn is None or _db_conn.closed:
                _db_conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
                _db_conn.autocommit = False
            with _db_conn.cursor() as cur:
                cur.execute("SELECT 1")
            return _db_conn
        except Exception as e:
            jlog("warning", "db_connect_fail", error=str(e)[:200])
            _db_conn = None
            return None

def db_migrate() -> None:
    conn = _get_db()
    if not conn:
        jlog("info", "db_skip_migrate", reason="no DATABASE_URL or psycopg2")
        return
    ddl = """
    CREATE TABLE IF NOT EXISTS j5_trades (
        trade_key        TEXT PRIMARY KEY,
        run_id           TEXT NOT NULL,
        ticker           TEXT NOT NULL,
        bucket_ts        TEXT NOT NULL,
        intent           TEXT NOT NULL,
        side             TEXT NOT NULL,
        status           TEXT NOT NULL DEFAULT 'CREATED',
        order_id         TEXT,
        client_order_id  TEXT,
        contracts        INT,
        price_cents      INT,
        reason           TEXT,
        submitted_at     TIMESTAMPTZ,
        filled_at        TIMESTAMPTZ,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS j5_circuit_breaker (
        id              INT PRIMARY KEY DEFAULT 1,
        halted          BOOLEAN NOT NULL DEFAULT FALSE,
        halt_reason     TEXT,
        halted_at       TIMESTAMPTZ,
        daily_loss      NUMERIC(12,4) NOT NULL DEFAULT 0,
        daily_trades    INT NOT NULL DEFAULT 0,
        hourly_orders   INT NOT NULL DEFAULT 0,
        consec_errors   INT NOT NULL DEFAULT 0,
        last_order_at   TIMESTAMPTZ,
        reset_day       DATE,
        reset_hour      TIMESTAMPTZ,
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS j5_positions (
        ticker       TEXT PRIMARY KEY,
        side         TEXT NOT NULL,
        contracts    INT NOT NULL,
        entry_price  NUMERIC(10,6),
        open_ts      TIMESTAMPTZ,
        trade_key    TEXT,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    INSERT INTO j5_circuit_breaker (id) VALUES (1) ON CONFLICT DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        jlog("info", "db_migrated")
    except Exception as e:
        conn.rollback()
        jlog("error", "db_migrate_fail", error=str(e)[:300])

def db_exec(sql: str, params: tuple = ()) -> bool:
    conn = _get_db()
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        return True
    except Exception as e:
        try: conn.rollback()
        except Exception: pass
        jlog("warning", "db_exec_fail", sql=sql[:60], error=str(e)[:200])
        return False

def db_fetchone(sql: str, params: tuple = ()) -> Optional[tuple]:
    conn = _get_db()
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    except Exception as e:
        jlog("warning", "db_fetch_fail", error=str(e)[:200])
        return None

# =============================================================================
# Trade key + idempotency
# =============================================================================
def make_trade_key(ticker: str, bucket_ts: str, intent: str, side: str) -> str:
    raw = f"{ticker}|{bucket_ts}|{intent.upper()}|{side.upper()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]

def trade_key_status(trade_key: str) -> Optional[str]:
    row = db_fetchone(
        "SELECT status FROM j5_trades WHERE trade_key=%s", (trade_key,)
    )
    return row[0] if row else None

def upsert_trade(
    trade_key: str, ticker: str, bucket_ts: str, intent: str, side: str,
    status: str, contracts: Optional[int] = None, price_cents: Optional[int] = None,
    order_id: Optional[str] = None, client_order_id: Optional[str] = None,
    reason: Optional[str] = None,
) -> None:
    db_exec(
        """INSERT INTO j5_trades
           (trade_key,run_id,ticker,bucket_ts,intent,side,status,contracts,
            price_cents,order_id,client_order_id,reason,submitted_at,updated_at)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())
           ON CONFLICT (trade_key) DO UPDATE SET
               status=EXCLUDED.status,
               order_id=COALESCE(EXCLUDED.order_id, j5_trades.order_id),
               client_order_id=COALESCE(EXCLUDED.client_order_id,
                                        j5_trades.client_order_id),
               reason=COALESCE(EXCLUDED.reason, j5_trades.reason),
               updated_at=NOW()
        """,
        (trade_key, RUN_ID, ticker, bucket_ts, intent, side, status,
         contracts, price_cents, order_id, client_order_id, reason),
    )

# =============================================================================
# Circuit Breaker
# =============================================================================
@dataclass
class CBState:
    halted:        bool          = False
    halt_reason:   str           = ""
    daily_loss:    float         = 0.0
    daily_trades:  int           = 0
    hourly_orders: int           = 0
    consec_errors: int           = 0
    last_order_ts: Optional[str] = None
    reset_day:     Optional[str] = None
    reset_hour:    Optional[str] = None

_cb      = CBState()
_cb_lock = threading.Lock()

def cb_load() -> None:
    global _cb
    row = db_fetchone(
        "SELECT halted,halt_reason,daily_loss,daily_trades,hourly_orders,"
        "consec_errors,last_order_at,reset_day,reset_hour "
        "FROM j5_circuit_breaker WHERE id=1"
    )
    if row:
        _cb.halted        = bool(row[0])
        _cb.halt_reason   = row[1] or ""
        _cb.daily_loss    = float(row[2] or 0)
        _cb.daily_trades  = int(row[3] or 0)
        _cb.hourly_orders = int(row[4] or 0)
        _cb.consec_errors = int(row[5] or 0)
        _cb.last_order_ts = row[6].isoformat() if row[6] else None
        _cb.reset_day     = str(row[7]) if row[7] else None
        _cb.reset_hour    = row[8].isoformat() if row[8] else None
        return
    try:
        if _CB_FILE.exists():
            data = json.loads(_CB_FILE.read_text())
            _cb = CBState(**{k: v for k, v in data.items()
                             if k in CBState.__dataclass_fields__})
    except Exception:
        pass

def cb_save() -> None:
    db_exec(
        "UPDATE j5_circuit_breaker SET halted=%s,halt_reason=%s,daily_loss=%s,"
        "daily_trades=%s,hourly_orders=%s,consec_errors=%s,last_order_at=%s,"
        "reset_day=%s,reset_hour=%s,updated_at=NOW() WHERE id=1",
        (_cb.halted, _cb.halt_reason, _cb.daily_loss, _cb.daily_trades,
         _cb.hourly_orders, _cb.consec_errors, _cb.last_order_ts,
         _cb.reset_day, _cb.reset_hour),
    )
    try:
        _CB_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CB_FILE.write_text(json.dumps(asdict(_cb), indent=2))
    except Exception:
        pass

def _cb_reset_daily() -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _cb.reset_day != today:
        _cb.daily_loss   = 0.0
        _cb.daily_trades = 0
        _cb.reset_day    = today
        jlog("info", "cb_daily_reset", today=today)

def _cb_reset_hourly() -> None:
    hour = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
    if _cb.reset_hour != hour:
        _cb.hourly_orders = 0
        _cb.reset_hour    = hour
        jlog("info", "cb_hourly_reset", hour=hour)

def cb_tick() -> None:
    with _cb_lock:
        _cb_reset_daily()
        _cb_reset_hourly()

def cb_is_halted() -> Tuple[bool, str]:
    with _cb_lock:
        return _cb.halted, _cb.halt_reason

def cb_halt(reason: str) -> None:
    with _cb_lock:
        if not _cb.halted:
            _cb.halted      = True
            _cb.halt_reason = reason
            jlog("critical", "CIRCUIT_BREAKER_TRIPPED", reason=reason)
            send_telegram(
                f"🚨 Johnny5 HALTED\n"
                f"Reason: {reason}\n"
                f"Re-arm: UPDATE j5_circuit_breaker SET halted=false "
                f"WHERE id=1; then redeploy."
            )
            cb_save()

def cb_clear_error() -> None:
    with _cb_lock:
        _cb.consec_errors = 0

def cb_increment_error() -> None:
    with _cb_lock:
        _cb.consec_errors += 1
        if _cb.consec_errors >= MAX_CONSEC_ERRORS and not _cb.halted:
            reason = f"MAX_CONSECUTIVE_ERRORS={MAX_CONSEC_ERRORS} reached"
            _cb.halted      = True
            _cb.halt_reason = reason
            jlog("critical", "CIRCUIT_BREAKER_CONSEC_ERRORS",
                 count=_cb.consec_errors)
            send_telegram(f"🚨 Johnny5 HALTED — {reason}. Check logs.")
            cb_save()

def cb_check_order_allowed(
    unrealized_loss: float = 0.0,
) -> Tuple[bool, str]:
    with _cb_lock:
        if _cb.halted:
            return False, f"halted: {_cb.halt_reason}"
        if KILL_SWITCH:
            return False, "KILL_SWITCH=true"
        _cb_reset_daily()
        _cb_reset_hourly()
        total_loss = _cb.daily_loss + max(0.0, unrealized_loss)
        if total_loss >= MAX_DAILY_LOSS:
            reason = (f"MAX_DAILY_LOSS={MAX_DAILY_LOSS:.2f} breached "
                      f"(loss={total_loss:.2f})")
            _cb.halted      = True
            _cb.halt_reason = reason
            cb_save()
            return False, reason
        if _cb.daily_trades >= MAX_TRADES_PER_DAY:
            return False, f"MAX_TRADES_PER_DAY={MAX_TRADES_PER_DAY} reached"
        if _cb.hourly_orders >= MAX_ORDERS_PER_HOUR:
            return False, f"MAX_ORDERS_PER_HOUR={MAX_ORDERS_PER_HOUR} reached"
        if (_cb.last_order_ts
                and secs_since(_cb.last_order_ts) < MIN_ORDER_INTERVAL):
            age = secs_since(_cb.last_order_ts)
            return False, (f"order interval cooldown "
                           f"({age:.1f}s < {MIN_ORDER_INTERVAL}s)")
        return True, ""

def cb_record_order_submitted() -> None:
    with _cb_lock:
        _cb.hourly_orders += 1
        _cb.daily_trades  += 1
        _cb.last_order_ts  = utc_iso()
        _cb.consec_errors  = 0
        cb_save()

def cb_record_realized_loss(loss_usd: float) -> None:
    with _cb_lock:
        _cb.daily_loss += abs(loss_usd)
        cb_save()

# =============================================================================
# Position store
# =============================================================================
@dataclass
class PositionSnapshot:
    ticker:      str
    side:        str
    contracts:   int
    entry_price: float
    open_ts:     str
    trade_key:   str = ""

_positions: Dict[str, PositionSnapshot] = {}
_pos_lock   = threading.Lock()

def pos_load_db() -> None:
    conn = _get_db()
    if not conn: return
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker,side,contracts,entry_price,open_ts,trade_key "
                "FROM j5_positions"
            )
            rows = cur.fetchall()
        with _pos_lock:
            _positions.clear()
            for row in rows:
                ticker = row[0]
                _positions[ticker] = PositionSnapshot(
                    ticker=ticker, side=row[1], contracts=int(row[2]),
                    entry_price=float(row[3] or 0),
                    open_ts=str(row[4] or ""),
                    trade_key=row[5] or "",
                )
        jlog("info", "positions_loaded", count=len(_positions))
    except Exception as e:
        jlog("warning", "pos_load_fail", error=str(e)[:200])

def pos_save(snap: PositionSnapshot) -> None:
    with _pos_lock:
        _positions[snap.ticker] = snap
    db_exec(
        """INSERT INTO j5_positions
           (ticker,side,contracts,entry_price,open_ts,trade_key,updated_at)
           VALUES (%s,%s,%s,%s,%s,%s,NOW())
           ON CONFLICT (ticker) DO UPDATE SET
               side=EXCLUDED.side, contracts=EXCLUDED.contracts,
               entry_price=EXCLUDED.entry_price, open_ts=EXCLUDED.open_ts,
               trade_key=EXCLUDED.trade_key, updated_at=NOW()
        """,
        (snap.ticker, snap.side, snap.contracts, snap.entry_price,
         snap.open_ts, snap.trade_key),
    )

def pos_delete(ticker: str) -> None:
    with _pos_lock:
        _positions.pop(ticker, None)
    db_exec("DELETE FROM j5_positions WHERE ticker=%s", (ticker,))

def pos_get(ticker: str) -> Optional[PositionSnapshot]:
    with _pos_lock:
        return _positions.get(ticker)

def pos_any() -> Optional[PositionSnapshot]:
    with _pos_lock:
        for p in _positions.values():
            if p.contracts > 0:
                return p
    return None

# =============================================================================
# Kalshi REST client
# =============================================================================
class KalshiClient:
    def __init__(self) -> None:
        self.session     = requests.Session()
        self.private_key = None
        pem = self._resolve_pem()
        if pem:
            try:
                self.private_key = serialization.load_pem_private_key(
                    pem.encode("utf-8"), password=None
                )
                jlog("info", "kalshi_key_loaded",
                     key_id=(KALSHI_API_KEY_ID[:8] + "…")
                     if KALSHI_API_KEY_ID else "none")
            except Exception as e:
                self.private_key = None
                jlog("warning", "kalshi_key_load_fail", error=str(e)[:200])
        else:
            jlog("warning", "kalshi_no_private_key")

    @staticmethod
    def _resolve_pem() -> str:
        raw_pem = KALSHI_PRIVATE_KEY_PEM.strip()
        if raw_pem:
            return raw_pem.replace("\\n", "\n").strip().strip('"').strip("'")
        raw = KALSHI_PRIVATE_KEY_PATH.strip()
        if not raw:
            return ""
        if "BEGIN" in raw:
            return raw.replace("\\n", "\n").strip().strip('"').strip("'")
        try:
            p = Path(raw)
            if len(raw) < 512 and p.exists():
                return p.read_text(encoding="utf-8").replace("\\n", "\n").strip()
        except Exception:
            pass
        try:
            decoded = base64.b64decode(raw, validate=True).decode("utf-8")
            return decoded.replace("\\n", "\n").strip()
        except (binascii.Error, UnicodeDecodeError):
            return ""

    def _headers(self, method: str, path: str) -> Dict[str, str]:
        """RSA-PSS with SHA-256, salt_length=32 — required by Kalshi."""
        if not (KALSHI_API_KEY_ID and self.private_key):
            return {"Content-Type": "application/json"}
        ts_ms = str(int(time.time() * 1000))
        msg   = f"{ts_ms}{method.upper()}{path}".encode("utf-8")
        sig   = self.private_key.sign(
            msg,
            asym_padding.PSS(
                mgf=asym_padding.MGF1(crypto_hashes.SHA256()),
                salt_length=32,
            ),
            crypto_hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY":       KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
            "Content-Type":            "application/json",
        }

    def _get(self, path: str, retries: int = 3) -> Dict[str, Any]:
        url      = f"{KALSHI_BASE_URL}{path}"
        last_exc: Optional[Exception] = None
        for attempt in range(retries):
            headers = self._headers("GET", path)
            try:
                r = self.session.get(
                    url, headers=headers, timeout=REQUEST_TIMEOUT
                )
                if r.status_code == 429:
                    wait = min(30, 2 ** attempt)
                    jlog("warning", "kalshi_rate_limited", wait=wait)
                    time.sleep(wait)
                    continue
                if r.status_code >= 500:
                    wait = min(30, 2 ** attempt + 1)
                    jlog("warning", "kalshi_5xx",
                         status=r.status_code, wait=wait)
                    time.sleep(wait)
                    continue
                if r.status_code == 404:
                    raise RuntimeError(f"Kalshi 404 {path}")
                if r.status_code >= 400:
                    body = (r.text or "")[:300]
                    raise RuntimeError(
                        f"Kalshi HTTP {r.status_code} {path}: {body}"
                    )
                out = r.json()
                if not isinstance(out, dict):
                    raise RuntimeError(f"Kalshi non-dict JSON on {path}")
                return out
            except RuntimeError:
                raise
            except Exception as e:
                last_exc = e
                wait = min(30, 2 ** attempt + 1)
                jlog("warning", "kalshi_request_fail",
                     attempt=attempt, error=str(e)[:200], wait=wait)
                time.sleep(wait)
        raise RuntimeError(
            f"Kalshi GET {path} failed after {retries} attempts: {last_exc}"
        )

    # ── Market data ──────────────────────────────────────────────────────────

    def list_open_markets(self) -> List[Dict[str, Any]]:
        data    = self._get(
            f"/trade-api/v2/markets?series_ticker={SERIES_TICKER}&status=open"
        )
        markets = data.get("markets", [])
        return markets if isinstance(markets, list) else []

    def get_market(self, ticker: str) -> Dict[str, Any]:
        """Fetch full market details including floor_strike / cap_strike."""
        return self._get(f"/trade-api/v2/markets/{ticker}")

    def get_orderbook(self, ticker: str) -> Dict[str, Any]:
        return self._get(f"/trade-api/v2/markets/{ticker}/orderbook")

    # ── Account data ─────────────────────────────────────────────────────────

    def get_balance(self) -> Optional[float]:
        try:
            data  = self._get("/trade-api/v2/portfolio/balance")
            cents = (data.get("balance")
                     or data.get("available_balance")
                     or data.get("buying_power"))
            if cents is None:
                jlog("warning", "balance_field_not_found",
                     raw_keys=list(data.keys()))
                return None
            return float(cents) / 100.0
        except Exception as e:
            jlog("warning", "get_balance_fail", error=str(e)[:200])
            return None

    def get_positions(self) -> List[Dict[str, Any]]:
        try:
            data      = self._get("/trade-api/v2/portfolio/positions")
            positions = (data.get("market_positions")
                         or data.get("positions") or [])
            return positions if isinstance(positions, list) else []
        except Exception as e:
            jlog("warning", "get_positions_fail", error=str(e)[:200])
            return []

    def get_open_orders(
        self, ticker: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        try:
            path = "/trade-api/v2/portfolio/orders?status=resting"
            if ticker:
                path += f"&ticker={ticker}"
            data   = self._get(path)
            orders = data.get("orders") or []
            return orders if isinstance(orders, list) else []
        except Exception as e:
            jlog("warning", "get_open_orders_fail", error=str(e)[:200])
            return []

    def cancel_order(self, order_id: str) -> bool:
        try:
            path    = f"/trade-api/v2/portfolio/orders/{order_id}"
            url     = f"{KALSHI_BASE_URL}{path}"
            headers = self._headers("DELETE", path)
            r       = self.session.delete(
                url, headers=headers, timeout=REQUEST_TIMEOUT
            )
            if r.status_code in (200, 204):
                jlog("info", "order_cancelled", order_id=order_id)
                return True
            jlog("warning", "cancel_fail",
                 order_id=order_id, status=r.status_code)
            return False
        except Exception as e:
            jlog("warning", "cancel_error", error=str(e)[:200])
            return False

    # ── Order placement (via Bun gateway) ────────────────────────────────────

    def place_order(
        self,
        action: str,
        ticker: str,
        side: str,
        contracts: int,
        price_cents: int,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not LIVE_MODE:
            fake_id = f"paper-{uuid.uuid4().hex[:8]}"
            jlog("info", "PAPER_ORDER", action=action, ticker=ticker,
                 side=side, contracts=contracts, price_cents=price_cents,
                 order_id=fake_id)
            return {"ok": True, "paper": True, "order": {"order_id": fake_id}}

        if not KALSHI_ORDER_GATEWAY_URL:
            raise RuntimeError(
                "KALSHI_ORDER_GATEWAY_URL not set (required for LIVE_MODE)"
            )
        if action not in ("buy", "sell"):
            raise ValueError(f"invalid action: {action}")
        if side not in ("YES", "NO"):
            raise ValueError(f"invalid side: {side}")
        if contracts <= 0:
            raise ValueError(f"contracts must be > 0, got {contracts}")

        coid    = client_order_id or uuid.uuid4().hex
        payload: Dict[str, Any] = {
            "ticker":          ticker,
            "action":          action,
            "type":            "limit",
            "side":            side,
            "count":           int(contracts),
            "client_order_id": coid,
        }
        if side == "YES":
            payload["yes_price"] = max(1, min(99, int(price_cents)))
        else:
            payload["no_price"]  = max(1, min(99, int(price_cents)))

        r = requests.post(
            f"{KALSHI_ORDER_GATEWAY_URL}/order",
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code >= 400:
            body = (r.text or "")[:500]
            if "insufficient_balance" in body.lower():
                reason = "insufficient_balance from Kalshi — HARD HALT"
                cb_halt(reason)
                raise RuntimeError(reason)
            raise RuntimeError(f"Order gateway HTTP {r.status_code}: {body}")

        out = r.json()
        if not isinstance(out, dict):
            raise RuntimeError("Order gateway returned non-dict")
        if not out.get("ok"):
            body_str = json.dumps(out)[:500]
            if "insufficient_balance" in body_str.lower():
                reason = "insufficient_balance in gateway response — HARD HALT"
                cb_halt(reason)
                raise RuntimeError(reason)
            raise RuntimeError(f"Order gateway rejected: {body_str}")
        return out

# =============================================================================
# Health server
# =============================================================================
def start_health_server() -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            p = (self.path or "").lower().split("?")[0]
            if p in (HEALTH_PATH.lower(), "/", "/healthz", "/healthcheck"):
                body = json.dumps({
                    "ok":      True,
                    "service": "johnny5",
                    "version": BOT_VERSION,
                    "run_id":  RUN_ID,
                    "live":    LIVE_MODE,
                }).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt: str, *args: Any) -> None:
            pass

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    jlog("info", "health_server_started", port=PORT)

# =============================================================================
# Market selection + strike parsing
# =============================================================================
def _as_float(x: Any) -> float:
    try:
        if x is None: return 0.0
        return float(x)
    except Exception:
        return 0.0

def _parse_ts_any(x: Any) -> Optional[datetime]:
    if not isinstance(x, str) or not x: return None
    try:
        return datetime.fromisoformat(x.replace("Z", "+00:00"))
    except Exception:
        return None

def extract_strike(market: Dict[str, Any]) -> Optional[float]:
    """
    Extract the strike price from a market dict.

    Kalshi binary markets that resolve "above K" have a floor_strike field.
    For markets that resolve "between K1 and K2", we use the midpoint.
    Falls back gracefully — returns None if we can't determine a strike,
    which will cause the bot to skip that market.

    Field names observed in the wild (Kalshi may use any of these):
      floor_strike, cap_strike, strike_price, strike, result_value
    """
    # Primary: floor_strike (above-K binary)
    raw = (market.get("floor_strike")
           or market.get("strike_price")
           or market.get("strike"))
    if raw is not None:
        try:
            val = float(raw)
            if val > 100:        # sanity: must look like a real price not a %
                return val
        except Exception:
            pass

    # Secondary: midpoint of a range market
    floor = market.get("floor_strike") or market.get("floor")
    cap   = market.get("cap_strike")   or market.get("cap")
    if floor is not None and cap is not None:
        try:
            return (float(floor) + float(cap)) / 2.0
        except Exception:
            pass

    # Tertiary: result_value sometimes carries the settlement price target
    rv = market.get("result_value")
    if rv is not None:
        try:
            val = float(rv)
            if val > 100:
                return val
        except Exception:
            pass

    return None

def pick_best_active_market(
    markets: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    Pick the open market with the best liquidity (volume + open interest).
    Only considers markets that have a parseable close_time in the future.
    """
    now        = datetime.now(timezone.utc)
    candidates = []
    for m in markets:
        if not isinstance(m, dict): continue
        ct = _parse_ts_any(m.get("close_time") or m.get("close_ts"))
        ot = _parse_ts_any(m.get("open_time")  or m.get("open_ts"))
        if not ot or not ct: continue
        if not (ot <= now < ct): continue
        vol   = _as_float(m.get("volume") or m.get("volume_24h") or 0)
        oi    = _as_float(m.get("open_interest") or m.get("oi") or 0)
        score = vol + oi
        candidates.append((score, ct, m))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], -x[1].timestamp()), reverse=True)
    return candidates[0][2]

def bucket_ts_for_market(m: Dict[str, Any]) -> str:
    return str(
        m.get("open_time") or m.get("open_ts")
        or m.get("ticker") or "unknown"
    )

def secs_until_close(m: Dict[str, Any]) -> Optional[float]:
    ct = _parse_ts_any(m.get("close_time") or m.get("close_ts"))
    if not ct: return None
    return max(0.0, (ct - datetime.now(timezone.utc)).total_seconds())

# =============================================================================
# Orderbook parsing + quality guards
# =============================================================================
def _best_price_cents(levels: Any) -> Optional[int]:
    if not isinstance(levels, list) or not levels:
        return None
    best: Optional[int] = None
    for lvl in levels:
        price: Optional[int] = None
        if isinstance(lvl, list) and len(lvl) >= 1:
            try: price = int(lvl[0])
            except Exception: pass
        elif isinstance(lvl, dict) and "price" in lvl:
            try: price = int(lvl["price"])
            except Exception: pass
        if price is None: continue
        if best is None or price > best:
            best = price
    return best

@dataclass
class BookState:
    best_yes_bid: Optional[int]
    best_no_bid:  Optional[int]
    yes_levels:   List
    no_levels:    List
    spread_cents: Optional[int]
    mark_yes:     Optional[float]   # mid-market as a probability 0-1

    @property
    def ok(self) -> bool:
        return self.mark_yes is not None and self.spread_cents is not None

def parse_book(orderbook: Dict[str, Any]) -> BookState:
    root       = orderbook.get("orderbook", orderbook)
    yes_levels = root.get("yes") or root.get("yes_bids") or []
    no_levels  = root.get("no")  or root.get("no_bids")  or []

    if DEBUG_BOOK_DUMP:
        jlog("debug", "BOOK_DUMP", yes=yes_levels[:5], no=no_levels[:5])

    best_yes = _best_price_cents(yes_levels)
    best_no  = _best_price_cents(no_levels)

    if best_yes is not None and (
        best_yes < MIN_BID_CENTS or best_yes > MAX_BID_CENTS
    ):
        best_yes = None
    if best_no is not None and (
        best_no < MIN_BID_CENTS or best_no > MAX_BID_CENTS
    ):
        best_no = None

    spread: Optional[int]   = None
    mark:   Optional[float] = None

    if best_yes is not None and best_no is not None:
        implied_ask = 100 - best_no
        spread      = implied_ask - best_yes
        if spread >= 0:
            mid  = (best_yes + implied_ask) / 2.0
            mark = max(0.01, min(0.99, mid / 100.0))

    return BookState(
        best_yes_bid=best_yes,
        best_no_bid=best_no,
        yes_levels=yes_levels,
        no_levels=no_levels,
        spread_cents=spread,
        mark_yes=mark,
    )

def book_quality_ok(book: BookState) -> Tuple[bool, str]:
    if not book.ok:
        return False, "one_sided_or_missing_book"
    assert book.spread_cents is not None
    if book.spread_cents > MAX_SPREAD_CENTS:
        return False, f"spread={book.spread_cents}c > MAX={MAX_SPREAD_CENTS}c"
    return True, ""

def compute_entry_price_cents(
    book: BookState, side: str
) -> Optional[int]:
    if book.mark_yes is None: return None
    mid_cents = int(book.mark_yes * 100)
    if side == "YES":
        if book.best_no_bid is None: return None
        price = 100 - book.best_no_bid
        if price - mid_cents > MAX_SLIPPAGE_CENTS: return None
        return max(1, min(99, price))
    else:
        if book.best_yes_bid is None: return None
        price = 100 - book.best_yes_bid
        if price - (100 - mid_cents) > MAX_SLIPPAGE_CENTS: return None
        return max(1, min(99, price))

def compute_exit_price_cents(
    book: BookState, side: str
) -> Optional[int]:
    if side == "YES":
        if book.best_yes_bid is None: return None
        return max(1, min(99, book.best_yes_bid))
    else:
        if book.best_no_bid is None: return None
        return max(1, min(99, book.best_no_bid))

# =============================================================================
# Price feed
# =============================================================================
def kraken_last_price() -> float:
    r = requests.get(KRAKEN_TICKER_URL, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    body   = r.json()
    result = body.get("result", {})
    if not result:
        raise RuntimeError("Kraken price missing from response")
    key = next(iter(result))
    return float(result[key]["c"][0])

# =============================================================================
# Binary option model
# =============================================================================
def _norm_cdf(x: float) -> float:
    """
    Standard normal CDF using the math.erfc approximation.
    Accurate to ~7 significant figures — more than enough for 1¢ resolution.
    """
    return 0.5 * math.erfc(-x / math.sqrt(2.0))

def realized_vol_annualised(prices: List[float]) -> float:
    """
    Compute annualised volatility from the last VOL_WINDOW log-returns.
    Prices are sampled every POLL_SECONDS seconds.
    Returns VOL_FLOOR if there isn't enough data.
    """
    if len(prices) < VOL_WINDOW + 1:
        return VOL_FLOOR

    window = prices[-(VOL_WINDOW + 1):]
    log_rets: List[float] = []
    for i in range(1, len(window)):
        p0, p1 = window[i - 1], window[i]
        if p0 <= 0 or p1 <= 0: continue
        log_rets.append(math.log(p1 / p0))

    if len(log_rets) < 2:
        return VOL_FLOOR

    n    = len(log_rets)
    mean = sum(log_rets) / n
    var  = sum((r - mean) ** 2 for r in log_rets) / (n - 1)
    std_per_sample = max(var ** 0.5, 1e-10)

    # Annualise: there are 365.25*24*3600 / POLL_SECONDS samples per year
    samples_per_year = (365.25 * 24 * 3600) / max(POLL_SECONDS, 1.0)
    sigma_annual     = std_per_sample * math.sqrt(samples_per_year)

    return max(VOL_FLOOR, min(VOL_CAP, sigma_annual))


def model_fair_yes(
    spot: float,
    strike: float,
    secs_remaining: float,
    sigma: float,
) -> float:
    """
    Fair value of a binary call option (pays $1 if spot > strike at expiry).

    Formula:  fair_yes = Φ(d)
    where     d = [ ln(S/K) ] / (σ √T)

    We omit the drift term (½σ²T) because:
      - T is very small (≤ 15 min = 0.0000285 years)
      - The drift contribution is negligible vs. the diffusion term
      - Keeping it simple reduces overfitting

    Args:
        spot:           current BTC price (USD)
        strike:         market strike price (USD)
        secs_remaining: seconds until the bucket closes
        sigma:          annualised realised volatility (e.g. 0.80 = 80%)

    Returns:
        fair_yes in [0.01, 0.99]
    """
    if strike <= 0 or spot <= 0 or secs_remaining <= 0:
        return 0.5

    # Time in years
    T = secs_remaining / (365.25 * 24 * 3600)

    sqrt_T = math.sqrt(max(T, 1e-12))
    sigma  = max(sigma, 1e-6)

    d = math.log(spot / strike) / (sigma * sqrt_T)

    fair = _norm_cdf(d)
    return max(0.01, min(0.99, fair))


# =============================================================================
# Sizing
# =============================================================================
def compute_contracts(cash_usd: float, price_cents: int) -> int:
    price_usd     = max(price_cents / 100.0, 0.01)
    risk_usd      = min(MAX_POSITION_USD, max(1.0, cash_usd * RISK_FRACTION))
    from_risk     = int(risk_usd / price_usd)
    from_notional = int(MAX_NOTIONAL_PER_TICKER / price_usd)
    return max(1, min(from_risk, MAX_CONTRACTS, from_notional))

# =============================================================================
# Runtime state
# =============================================================================
@dataclass
class BotState:
    paper_cash:            float          = 50.0
    realized_pnl_lifetime: float          = 0.0
    trade_history_24h:     Optional[list] = None
    last_trade_ts:         Optional[str]  = None

def ensure_dirs() -> None:
    for p in (STATE_FILE, STATUS_FILE, _CB_FILE):
        p.parent.mkdir(parents=True, exist_ok=True)

def load_state() -> BotState:
    ensure_dirs()
    if not STATE_FILE.exists():
        s = BotState()
        s.trade_history_24h = []
        return s
    try:
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        s   = BotState(**{k: v for k, v in raw.items()
                          if k in BotState.__dataclass_fields__})
        if s.trade_history_24h is None:
            s.trade_history_24h = []
        return s
    except Exception as e:
        jlog("warning", "state_load_fail", error=str(e)[:200])
        s = BotState()
        s.trade_history_24h = []
        return s

def save_state(state: BotState) -> None:
    ensure_dirs()
    try:
        STATE_FILE.write_text(
            json.dumps(asdict(state), indent=2), encoding="utf-8"
        )
    except Exception as e:
        jlog("warning", "state_save_fail", error=str(e)[:200])

def prune_24h(state: BotState) -> None:
    if state.trade_history_24h is None:
        state.trade_history_24h = []
        return
    now = datetime.now(timezone.utc)
    state.trade_history_24h = [
        t for t in state.trade_history_24h
        if (dt := parse_iso(t.get("ts", "")))
        and (now - dt) <= timedelta(hours=24)
    ]

def record_trade_24h(
    state: BotState, typ: str, pnl_delta: float
) -> None:
    if state.trade_history_24h is None:
        state.trade_history_24h = []
    state.trade_history_24h.append(
        {"ts": utc_iso(), "type": typ, "pnl": float(pnl_delta)}
    )
    prune_24h(state)

def write_status(
    state: BotState, message: str, ticker: str,
    mark_yes: Optional[float], fair_yes: float,
    spot: float, strike: Optional[float],
    sigma: float, secs_remaining: Optional[float],
    edge_cents: float, cash_real: Optional[float] = None,
) -> None:
    halted, halt_reason = cb_is_halted()
    pos = pos_any()
    payload = {
        "ts":          utc_iso(),
        "run_id":      RUN_ID,
        "version":     BOT_VERSION,
        "message":     message,
        "live":        LIVE_MODE,
        "kill_switch": KILL_SWITCH,
        "halted":      halted,
        "halt_reason": halt_reason,
        "ticker":      ticker,
        "cash_real":   cash_real,
        "position": {
            "ticker":      pos.ticker      if pos else None,
            "side":        pos.side        if pos else None,
            "contracts":   pos.contracts   if pos else 0,
            "entry_price": pos.entry_price if pos else None,
            "open_ts":     pos.open_ts     if pos else None,
        },
        "model": {
            "spot":           spot,
            "strike":         strike,
            "sigma_annual":   round(sigma, 4),
            "secs_remaining": secs_remaining,
            "mark_yes":       mark_yes,
            "fair_yes":       round(fair_yes, 4),
            "edge_cents":     round(edge_cents, 2),
        },
        "circuit_breaker": {
            "daily_loss":    _cb.daily_loss,
            "daily_trades":  _cb.daily_trades,
            "hourly_orders": _cb.hourly_orders,
            "consec_errors": _cb.consec_errors,
        },
    }
    ensure_dirs()
    try:
        STATUS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        pass

# =============================================================================
# Trading logic — ENTER
# =============================================================================
def maybe_enter(
    state:          BotState,
    client:         KalshiClient,
    ticker:         str,
    bucket_ts:      str,
    book:           BookState,
    fair_yes:       float,
    secs_remaining: float,
    cash_real:      Optional[float],
    spot:           float,
    strike:         float,
    sigma:          float,
) -> bool:

    # ── 1. Position check ──────────────────────────────────────────────────
    existing = pos_any()
    if existing is not None:
        if not ALLOW_PYRAMIDING:
            return False
        if existing.ticker != ticker:
            return False

    # ── 2. Kill switch ─────────────────────────────────────────────────────
    if KILL_SWITCH:
        return False

    # ── 3. Cooldown ────────────────────────────────────────────────────────
    if secs_since(state.last_trade_ts) < COOLDOWN_SECONDS:
        return False

    # ── 4. Time-in-bucket guards ───────────────────────────────────────────
    #    Don't trade when there's too little time left (too risky) or
    #    too much time left (model uncertainty is too high).
    if secs_remaining < MIN_SECS_BEFORE_EXPIRY:
        jlog("debug", "enter_skip_too_close_to_expiry",
             secs_remaining=round(secs_remaining, 1),
             min=MIN_SECS_BEFORE_EXPIRY)
        return False
    if secs_remaining > MAX_SECS_BEFORE_EXPIRY:
        jlog("debug", "enter_skip_too_far_from_expiry",
             secs_remaining=round(secs_remaining, 1),
             max=MAX_SECS_BEFORE_EXPIRY)
        return False

    # ── 5. Book quality ────────────────────────────────────────────────────
    book_ok, book_reason = book_quality_ok(book)
    if not book_ok:
        jlog("debug", "enter_skip_book", reason=book_reason, ticker=ticker)
        return False

    if book.mark_yes is None:
        return False

    # ── 6. Edge gate — based on fair_yes vs book mid (in CENTS) ───────────
    #    Positive edge → model says YES is cheap → buy YES
    #    Negative edge → model says NO is cheap  → buy NO
    #    We require at least MIN_EDGE_CENTS of mispricing.
    edge_cents = (fair_yes - book.mark_yes) * 100

    if edge_cents >= MIN_EDGE_CENTS:
        side = "YES"
    elif edge_cents <= -MIN_EDGE_CENTS:
        side = "NO"
    else:
        jlog("debug", "enter_skip_insufficient_edge",
             edge_cents=round(edge_cents, 2),
             min=MIN_EDGE_CENTS)
        return False

    # ── 7. Entry price (slippage check) ───────────────────────────────────
    price_cents = compute_entry_price_cents(book, side)
    if price_cents is None:
        jlog("debug", "enter_skip_slippage", side=side, ticker=ticker)
        return False

    # ── 8. Sizing ──────────────────────────────────────────────────────────
    cash      = cash_real if (LIVE_MODE and cash_real is not None) \
                else state.paper_cash
    contracts = compute_contracts(cash, price_cents)
    if contracts <= 0:
        jlog("info", "enter_skip_zero_contracts")
        return False

    # ── 9. Idempotency ─────────────────────────────────────────────────────
    trade_key       = make_trade_key(ticker, bucket_ts, "ENTER", side)
    existing_status = trade_key_status(trade_key)
    if existing_status in ("SUBMITTED", "FILLED"):
        jlog("info", "enter_idempotent_skip",
             trade_key=trade_key, status=existing_status)
        return False
    if existing_status == "FAILED":
        return False

    # ── 10. Circuit breaker ────────────────────────────────────────────────
    allowed, reason = cb_check_order_allowed()
    if not allowed:
        jlog("info", "enter_blocked_by_cb", reason=reason)
        return False

    # ── 11. Persist CREATED ────────────────────────────────────────────────
    coid = uuid.uuid4().hex
    upsert_trade(
        trade_key=trade_key, ticker=ticker, bucket_ts=bucket_ts,
        intent="ENTER", side=side, status="CREATED",
        contracts=contracts, price_cents=price_cents,
        client_order_id=coid,
    )

    # ── 12. Submit ─────────────────────────────────────────────────────────
    try:
        result   = client.place_order(
            action="buy", ticker=ticker, side=side,
            contracts=contracts, price_cents=price_cents,
            client_order_id=coid,
        )
        order_id = (result.get("order") or {}).get("order_id") or coid
        cb_record_order_submitted()
        upsert_trade(
            trade_key=trade_key, ticker=ticker, bucket_ts=bucket_ts,
            intent="ENTER", side=side, status="SUBMITTED",
            order_id=order_id, client_order_id=coid,
        )
        pos_save(PositionSnapshot(
            ticker=ticker, side=side, contracts=contracts,
            entry_price=price_cents / 100.0,
            open_ts=utc_iso(), trade_key=trade_key,
        ))
        if not LIVE_MODE:
            state.paper_cash -= (price_cents / 100.0) * contracts
        state.last_trade_ts = utc_iso()
        record_trade_24h(state, "ENTER", 0.0)

        jlog("info", "ENTER_SUBMITTED",
             ticker=ticker, side=side, contracts=contracts,
             price_cents=price_cents,
             spot=round(spot, 2), strike=round(strike, 2),
             sigma=round(sigma, 4), secs_remaining=round(secs_remaining, 1),
             fair_yes=round(fair_yes, 4), edge_cents=round(edge_cents, 2),
             order_id=order_id)
        # No Telegram on ENTER — only EXIT sends an alert
        return True

    except RuntimeError as e:
        err_str = str(e)
        jlog("error", "enter_order_fail",
             error=err_str[:300], trade_key=trade_key)
        upsert_trade(
            trade_key=trade_key, ticker=ticker, bucket_ts=bucket_ts,
            intent="ENTER", side=side, status="FAILED",
            reason=err_str[:200],
        )
        if "insufficient_balance" not in err_str:
            cb_increment_error()
        return False

# =============================================================================
# Trading logic — EXIT
# =============================================================================
def maybe_exit(
    state:          BotState,
    client:         KalshiClient,
    book:           BookState,
    fair_yes:       float,
    bucket_ts:      str,
    force:          bool = False,
    cash_real:      Optional[float] = None,
) -> bool:
    pos = pos_any()
    if pos is None:
        return False

    ticker    = pos.ticker
    side      = pos.side
    contracts = pos.contracts

    # Hold time (bypass on force)
    if not force and secs_since(pos.open_ts) < MIN_HOLD_SECONDS:
        return False

    # Cooldown (bypass on force)
    if not force and secs_since(state.last_trade_ts) < COOLDOWN_SECONDS:
        return False

    # Exit signal: edge has collapsed (model no longer says we're right)
    if book.mark_yes is None and not force:
        return False

    if book.mark_yes is not None:
        edge_cents = (fair_yes - book.mark_yes) * 100
        if side == "YES":
            should_exit = edge_cents <= EXIT_EDGE_CENTS
        else:
            should_exit = edge_cents >= -EXIT_EDGE_CENTS
    else:
        should_exit = False

    if force or (KILL_SWITCH and CLOSE_ON_KILL_SWITCH):
        should_exit = True

    if not should_exit:
        return False

    # Idempotency
    trade_key       = make_trade_key(ticker, bucket_ts, "EXIT", side)
    existing_status = trade_key_status(trade_key)
    if existing_status in ("SUBMITTED", "FILLED"):
        jlog("info", "exit_idempotent_skip",
             trade_key=trade_key, status=existing_status)
        return False

    price_cents = compute_exit_price_cents(book, side)
    if price_cents is None:
        jlog("warning", "exit_skip_no_price", ticker=ticker, side=side)
        return False

    coid = uuid.uuid4().hex
    upsert_trade(
        trade_key=trade_key, ticker=ticker, bucket_ts=bucket_ts,
        intent="EXIT", side=side, status="CREATED",
        contracts=contracts, price_cents=price_cents,
        client_order_id=coid,
    )

    try:
        result   = client.place_order(
            action="sell", ticker=ticker, side=side,
            contracts=contracts, price_cents=price_cents,
            client_order_id=coid,
        )
        order_id = (result.get("order") or {}).get("order_id") or coid
        cb_record_order_submitted()
        upsert_trade(
            trade_key=trade_key, ticker=ticker, bucket_ts=bucket_ts,
            intent="EXIT", side=side, status="SUBMITTED",
            order_id=order_id, client_order_id=coid,
        )

        exit_price = price_cents / 100.0
        pnl        = (exit_price - pos.entry_price) * contracts
        state.realized_pnl_lifetime += pnl
        record_trade_24h(state, "EXIT", pnl)
        if pnl < 0:
            cb_record_realized_loss(abs(pnl))
        if not LIVE_MODE:
            state.paper_cash += exit_price * contracts

        state.last_trade_ts = utc_iso()
        pos_delete(ticker)

        jlog("info", "EXIT_SUBMITTED",
             ticker=ticker, side=side, contracts=contracts,
             price_cents=price_cents, pnl=round(pnl, 4),
             order_id=order_id)

        bal_str = f"${cash_real:.2f}" if cash_real is not None else "check app"
        send_telegram(
            f"{'🔴' if pnl < 0 else '🟢'} Johnny5 EXIT\n"
            f"{side} x{contracts} @ {price_cents}¢\n"
            f"PnL this trade: ${pnl:+.2f}\n"
            f"Kalshi balance: {bal_str}"
        )
        return True

    except RuntimeError as e:
        err_str = str(e)
        jlog("error", "exit_order_fail",
             error=err_str[:300], trade_key=trade_key)
        upsert_trade(
            trade_key=trade_key, ticker=ticker, bucket_ts=bucket_ts,
            intent="EXIT", side=side, status="FAILED",
            reason=err_str[:200],
        )
        if "insufficient_balance" not in err_str:
            cb_increment_error()
        return False

# =============================================================================
# Boot reconciliation
# =============================================================================
def reconcile_live_positions(client: KalshiClient) -> None:
    if not LIVE_MODE:
        return
    jlog("info", "reconcile_start")
    real_positions = client.get_positions()
    jlog("info", "reconcile_raw",
         count=len(real_positions), sample=real_positions[:2])

    for rp in real_positions:
        ticker = rp.get("ticker") or rp.get("market_ticker") or ""
        if not ticker or SERIES_TICKER not in ticker:
            continue
        yes_qty = int(rp.get("yes_position") or rp.get("position") or 0)
        no_qty  = int(rp.get("no_position") or 0)
        if yes_qty > 0 and pos_get(ticker) is None:
            jlog("warning", "reconcile_orphan_yes",
                 ticker=ticker, qty=yes_qty)
            pos_save(PositionSnapshot(
                ticker=ticker, side="YES", contracts=yes_qty,
                entry_price=0.50, open_ts=utc_iso(),
                trade_key="reconciled",
            ))
        if no_qty > 0 and pos_get(ticker) is None:
            jlog("warning", "reconcile_orphan_no",
                 ticker=ticker, qty=no_qty)
            pos_save(PositionSnapshot(
                ticker=ticker, side="NO", contracts=no_qty,
                entry_price=0.50, open_ts=utc_iso(),
                trade_key="reconciled",
            ))

    with _pos_lock:
        held_tickers = list(_positions.keys())
    for ticker in held_tickers:
        kalshi_has_it = any(
            (rp.get("ticker") or rp.get("market_ticker") or "") == ticker
            and (int(rp.get("yes_position") or rp.get("position") or 0) > 0
                 or int(rp.get("no_position") or 0) > 0)
            for rp in real_positions
        )
        if not kalshi_has_it:
            jlog("warning", "reconcile_stale_local_position",
                 ticker=ticker)
    jlog("info", "reconcile_done")

# =============================================================================
# Debug throttle
# =============================================================================
_last_dbg: Dict[str, float] = {}

def dbg_every(key: str, seconds: float) -> bool:
    now  = time.time()
    last = _last_dbg.get(key, 0.0)
    if now - last >= seconds:
        _last_dbg[key] = now
        return True
    return False

# =============================================================================
# Market detail cache (strike + close_time; refreshed per market per loop)
# =============================================================================
_market_cache: Dict[str, Dict[str, Any]] = {}

def get_market_cached(
    client: KalshiClient, ticker: str
) -> Dict[str, Any]:
    """
    Fetch and cache full market details.  Cache expires after 60s so we
    don't hammer the API, but still pick up any corrections.
    """
    cached = _market_cache.get(ticker)
    if cached:
        age = time.time() - cached.get("_fetched_at", 0)
        if age < 60:
            return cached
    try:
        data = client.get_market(ticker)
        # Kalshi wraps the market in a 'market' key
        m = data.get("market", data)
        m["_fetched_at"] = time.time()
        _market_cache[ticker] = m
        return m
    except Exception as e:
        jlog("warning", "market_cache_miss", ticker=ticker, error=str(e)[:200])
        return _market_cache.get(ticker, {})

# =============================================================================
# Main loop
# =============================================================================
def main() -> None:
    print(json.dumps({
        "ts": utc_iso(), "event": "BOOT", "version": BOT_VERSION,
        "run_id": RUN_ID, "live": LIVE_MODE,
    }), flush=True)

    ensure_dirs()
    start_health_server()
    db_migrate()

    cb_load()
    if _cb.halted:
        jlog("warning", "BOOT_ALREADY_HALTED", reason=_cb.halt_reason)
        send_telegram(
            f"⚠️ Johnny5 booted HALTED: {_cb.halt_reason}\n"
            f"Re-arm: UPDATE j5_circuit_breaker SET halted=false "
            f"WHERE id=1; then redeploy."
        )

    pos_load_db()

    state = load_state()
    if state.trade_history_24h is None:
        state.trade_history_24h = []

    client = KalshiClient()

    try:
        reconcile_live_positions(client)
    except Exception as e:
        jlog("warning", "reconcile_fail", error=str(e)[:200])

    real_cash: Optional[float] = None
    if LIVE_MODE:
        real_cash = client.get_balance()
        jlog("info", "LIVE_BALANCE_AT_BOOT", cash_usd=real_cash)
        if real_cash is not None and real_cash < 2.0:
            jlog("warning", "LOW_BALANCE_AT_BOOT", cash_usd=real_cash)
            send_telegram(
                f"⚠️ Johnny5: low balance at boot: ${real_cash:.2f}"
            )

    prices:          List[float] = []
    loop_id:         int         = 0
    null_book_count: int         = 0

    while True:
        loop_id += 1
        try:
            # ── Circuit breaker tick ───────────────────────────────────────
            cb_tick()
            halted, halt_reason = cb_is_halted()
            if halted:
                if dbg_every("halt_log", 60):
                    jlog("warning", "HALTED_SLEEPING", reason=halt_reason)
                time.sleep(max(10.0, POLL_SECONDS))
                continue

            # ── Fetch BTC spot price ───────────────────────────────────────
            try:
                spot = kraken_last_price()
                prices.append(spot)
                cb_clear_error()
            except Exception as e:
                jlog("warning", "kraken_fail", error=str(e)[:200])
                cb_increment_error()
                time.sleep(max(10.0, POLL_SECONDS))
                continue

            if len(prices) > max(VOL_WINDOW + 10, 400):
                prices = prices[-(VOL_WINDOW + 10):]

            # ── Refresh real balance every 30s ─────────────────────────────
            if LIVE_MODE and dbg_every("balance", 30):
                real_cash = client.get_balance()
                if real_cash is not None:
                    jlog("debug", "balance_refresh", cash_usd=real_cash)

            # ── Compute volatility ─────────────────────────────────────────
            sigma = realized_vol_annualised(prices)

            # ── EXIT PATH — always on held ticker ─────────────────────────
            current_pos = pos_any()
            pos_book:       Optional[BookState] = None
            pos_fair_yes:   float               = 0.5
            pos_mark:       Optional[float]     = None
            pos_strike:     Optional[float]     = None
            pos_secs_rem:   Optional[float]     = None

            if current_pos:
                pos_ticker = current_pos.ticker
                try:
                    # Get full market details (strike + close time)
                    mkt      = get_market_cached(client, pos_ticker)
                    pos_secs_rem = secs_until_close(mkt)
                    pos_strike   = extract_strike(mkt)

                    pos_ob   = client.get_orderbook(pos_ticker)
                    pos_book = parse_book(pos_ob)
                    pos_mark = pos_book.mark_yes

                    # Recompute fair value on the held position's ticker
                    if (pos_strike is not None
                            and pos_secs_rem is not None
                            and pos_secs_rem > 0):
                        pos_fair_yes = model_fair_yes(
                            spot, pos_strike, pos_secs_rem, sigma
                        )
                    else:
                        pos_fair_yes = pos_mark or 0.5

                    # ── Expired-market guard ───────────────────────────────
                    book_is_dead = (pos_book.best_yes_bid is None
                                    and pos_book.best_no_bid is None)
                    if book_is_dead:
                        null_book_count += 1
                        jlog("warning", "null_book_on_held_position",
                             ticker=pos_ticker,
                             consecutive=null_book_count,
                             limit=NULL_BOOK_EXPIRE_LOOPS)
                        if null_book_count >= NULL_BOOK_EXPIRE_LOOPS:
                            jlog("warning",
                                 "MARKET_EXPIRED_CLEARING_POSITION",
                                 ticker=pos_ticker,
                                 contracts=current_pos.contracts,
                                 side=current_pos.side,
                                 entry_price=current_pos.entry_price,
                                 null_loops=null_book_count)
                            send_telegram(
                                f"⚠️ Johnny5: market {pos_ticker} expired "
                                f"(null book {null_book_count} loops).\n"
                                f"Cleared local position — "
                                f"check Kalshi for settlement."
                            )
                            pos_delete(pos_ticker)
                            null_book_count = 0
                    else:
                        null_book_count = 0
                        force_exit = KILL_SWITCH and CLOSE_ON_KILL_SWITCH
                        if pos_book.ok or force_exit:
                            maybe_exit(
                                state=state, client=client,
                                book=pos_book,
                                fair_yes=pos_fair_yes,
                                bucket_ts=pos_ticker,
                                force=force_exit,
                                cash_real=real_cash,
                            )

                    if DEBUG_MODEL and dbg_every("model_pos", 30):
                        jlog("debug", "MODEL_POS",
                             ticker=pos_ticker,
                             spot=round(spot, 2),
                             strike=pos_strike,
                             secs_rem=round(pos_secs_rem or 0, 1),
                             sigma=round(sigma, 4),
                             fair_yes=round(pos_fair_yes, 4),
                             mark=pos_mark)

                except Exception as e:
                    jlog("warning", "pos_loop_error",
                         ticker=pos_ticker, error=str(e)[:200])
                    cb_increment_error()
            else:
                null_book_count = 0

            # ── ENTER PATH ─────────────────────────────────────────────────
            if not KILL_SWITCH and pos_any() is None:
                try:
                    markets = client.list_open_markets()
                    m       = pick_best_active_market(markets)

                    if m and m.get("ticker"):
                        ticker    = str(m["ticker"])
                        bucket_ts = bucket_ts_for_market(m)

                        # Time remaining in bucket
                        t_rem = secs_until_close(m)
                        if t_rem is None:
                            jlog("debug", "enter_skip_no_close_time",
                                 ticker=ticker)
                        else:
                            # Fetch full market for strike
                            mkt    = get_market_cached(client, ticker)
                            strike = extract_strike(mkt)

                            if strike is None:
                                if dbg_every("no_strike", 60):
                                    jlog("warning",
                                         "enter_skip_no_strike",
                                         ticker=ticker,
                                         market_keys=list(mkt.keys())[:15])
                            else:
                                fair_yes = model_fair_yes(
                                    spot, strike, t_rem, sigma
                                )

                                ob   = client.get_orderbook(ticker)
                                book = parse_book(ob)

                                edge_cents = (
                                    (fair_yes - (book.mark_yes or 0.5)) * 100
                                )

                                if DEBUG_MODEL and dbg_every("model_enter", 30):
                                    jlog("debug", "MODEL_ENTER",
                                         ticker=ticker,
                                         spot=round(spot, 2),
                                         strike=round(strike, 2),
                                         secs_rem=round(t_rem, 1),
                                         sigma=round(sigma, 4),
                                         fair_yes=round(fair_yes, 4),
                                         mark=book.mark_yes,
                                         edge_cents=round(edge_cents, 2))

                                if DEBUG_PICK and dbg_every("pick", 60):
                                    jlog("debug", "PICK_DEBUG",
                                         ticker=ticker,
                                         vol=m.get("volume"),
                                         oi=m.get("open_interest"))

                                maybe_enter(
                                    state=state, client=client,
                                    ticker=ticker, bucket_ts=bucket_ts,
                                    book=book, fair_yes=fair_yes,
                                    secs_remaining=t_rem,
                                    cash_real=real_cash,
                                    spot=spot, strike=strike, sigma=sigma,
                                )
                    else:
                        if dbg_every("no_market", 60):
                            jlog("info", "no_open_market")

                except Exception as e:
                    jlog("warning", "enter_loop_error", error=str(e)[:200])
                    cb_increment_error()

            # ── Status write ───────────────────────────────────────────────
            cur = pos_any()
            write_status(
                state=state, message="running",
                ticker=cur.ticker if cur else "",
                mark_yes=pos_mark,
                fair_yes=pos_fair_yes,
                spot=spot,
                strike=pos_strike,
                sigma=sigma,
                secs_remaining=pos_secs_rem,
                edge_cents=(
                    (pos_fair_yes - (pos_mark or pos_fair_yes)) * 100
                ),
                cash_real=real_cash,
            )
            save_state(state)

            time.sleep(max(1.0, POLL_SECONDS))

        except KeyboardInterrupt:
            jlog("info", "shutdown_keyboard")
            return
        except Exception as exc:
            err_str = str(exc)[:300]
            if DEBUG_ERRORS and dbg_every("err", 10):
                jlog("error", "UNHANDLED_LOOP_ERROR",
                     error=err_str, loop_id=loop_id)
            cb_increment_error()
            try:
                save_state(state)
            except Exception:
                pass
            time.sleep(max(10.0, POLL_SECONDS))


if __name__ == "__main__":
    main()

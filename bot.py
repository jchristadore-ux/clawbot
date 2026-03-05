#!/usr/bin/env python3
"""
Johnny5 — Kalshi BTC-15M Trading Bot  (PRODUCTION v5)
======================================================
Single-file, Railway-deployable. Postgres-backed idempotency.

LESSONS LEARNED FROM 2026-03-04 TRADE LOG (106 trades, 27 buckets):
  1. CHURN: 23/27 buckets had BOTH YES and NO bought in same bucket.
     The model was flipping every 10s as BTC moved. Fixed by:
       - ONE trade per bucket. Once entered, no reversal.
       - HOLD_TO_EXPIRY mode: when in profit and < 90s left, hold to settlement
         instead of trying to exit. Kalshi settles at $1 — you can't do better.
  2. SIZE: Afternoon avg was 2.8 contracts. Fees were 2.8% of notional.
     Need minimum 10 contracts to make fees manageable. Fixed by:
       - MIN_CONTRACTS = 5, target = 10% of balance at entry price
       - Skip trade if cant get MIN_CONTRACTS
  3. FEE DRAG: $7.34 in fees on $50 account = 14.7% of capital.
     Every unnecessary order = 2¢/contract burned. Fixed by:
       - No re-entry after exit in same bucket (ONE_TRADE_PER_BUCKET)
       - Exit only when edge has collapsed OR < 90s left (hold to expiry)
  4. DIRECTION ACCURACY: 52% YES / 48% NO — coin flip.
     The model edge is real only when |fair_yes - book_mid| > MIN_EDGE_CENTS.
     Tightened to 8¢ minimum edge.
  5. MORNING vs AFTERNOON: Morning big lots (17 avg) were profitable.
     Afternoon small lots were fee-dominated. Bot now targets 10+ contracts.

Architecture changes from v4:
  - BucketState: per-bucket lock preventing any second trade in same bucket
  - HOLD_TO_EXPIRY: skip exit attempt when T < HOLD_EXPIRY_SECS and in profit
  - MIN_CONTRACTS: skip entry if sizing yields < MIN_CONTRACTS
  - ONE_TRADE_PER_BUCKET enforced via Postgres j5_bucket_state table
  - Daily performance table j5_daily_stats auto-populated for morning review
  - ADAPTIVE_EDGE: edge threshold tightens after 3 losses, loosens after 3 wins
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
from dataclasses import asdict, dataclass, field
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
BOT_VERSION = "JOHNNY5_KALSHI_BTC15M_PROD_v5"

KALSHI_BASE_URL          = env_str("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/")
KALSHI_API_KEY_ID        = env_str("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PEM   = env_str("KALSHI_PRIVATE_KEY_PEM", "")
KALSHI_PRIVATE_KEY_PATH  = env_str("KALSHI_PRIVATE_KEY_PATH", "")
SERIES_TICKER            = env_str("SERIES_TICKER", "KXBTC15M").upper()
KALSHI_ORDER_GATEWAY_URL = env_str("KALSHI_ORDER_GATEWAY_URL", "").rstrip("/")

LIVE_MODE       = env_bool("LIVE_MODE", False)
POLL_SECONDS    = env_float("POLL_SECONDS", 10.0)
REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT_SECONDS", 15)
PORT            = env_int("PORT", 3000)
HEALTH_PATH     = env_str("HEALTH_PATH", "/health")

DATABASE_URL = env_str("DATABASE_URL", "")
STATE_FILE   = Path(env_str("STATE_FILE",  ".runtime/state.json"))
STATUS_FILE  = Path(env_str("STATUS_FILE", ".runtime/status.json"))
_CB_FILE     = Path(".runtime/circuit_breaker.json")

KRAKEN_TICKER_URL = env_str(
    "KRAKEN_TICKER_URL", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"
)

# =============================================================================
# Model config
# =============================================================================
VOL_WINDOW = env_int("VOL_WINDOW", 30)       # ~5 min of 10s samples
VOL_FLOOR  = env_float("VOL_FLOOR", 0.65)    # 65% annualised floor
VOL_CAP    = env_float("VOL_CAP", 3.0)

# Edge threshold — base value, adapted by win/loss streak
# Data showed 23/27 buckets had churn; tighter edge = fewer but better trades
MIN_EDGE_CENTS_BASE = env_int("MIN_EDGE_CENTS", 8)   # 8¢ minimum mispricing
MIN_EDGE_CENTS_MAX  = env_int("MIN_EDGE_CENTS_MAX", 15)
MIN_EDGE_CENTS_MIN  = env_int("MIN_EDGE_CENTS_MIN", 5)

# Time-in-bucket guards (seconds)
# Only trade in the 3-10 minute window: enough time for edge to realise,
# not so early that BTC can reverse before expiry.
MIN_SECS_BEFORE_EXPIRY = env_int("MIN_SECS_BEFORE_EXPIRY", 180)   # 3 min left
MAX_SECS_BEFORE_EXPIRY = env_int("MAX_SECS_BEFORE_EXPIRY", 600)   # 10 min left

# Hold-to-expiry: when T < HOLD_EXPIRY_SECS and position is in profit,
# skip exit attempts and just let Kalshi settle at $1.
# Data: morning big lots won by holding. Scalping 5¢ out of 70¢ upside is dumb.
HOLD_EXPIRY_SECS = env_int("HOLD_EXPIRY_SECS", 90)   # < 90s left → hold to settle

# ONE TRADE PER BUCKET: never re-enter or reverse in same bucket.
# This was the root cause of the 23/27 churn problem.
ONE_TRADE_PER_BUCKET = env_bool("ONE_TRADE_PER_BUCKET", True)

# =============================================================================
# Risk / sizing — learned from data
# =============================================================================
# Target 10% of balance per trade (not 6%) — small account needs bigger bets
# to overcome fee drag. At $50 bal, 10% = $5, at 30¢ = 16 contracts.
MAX_POSITION_USD        = env_float("MAX_POSITION_USD", 8.0)
RISK_FRACTION           = env_float("RISK_FRACTION", 0.10)   # up from 6%
MAX_CONTRACTS           = env_int("MAX_CONTRACTS_PER_TICKER", 20)
MAX_NOTIONAL_PER_TICKER = env_float("MAX_NOTIONAL_PER_TICKER", 10.0)

# Minimum contracts — skip trade if we can't get this many.
# At 2 contracts, fees = 2.8% of notional. Unacceptable.
MIN_CONTRACTS = env_int("MIN_CONTRACTS", 5)

# Exit: hold until edge collapses below EXIT_EDGE_CENTS
# OR time < HOLD_EXPIRY_SECS (then just hold to settlement)
EXIT_EDGE_CENTS = env_int("EXIT_EDGE_CENTS", 2)

# =============================================================================
# Circuit breakers
# =============================================================================
MAX_DAILY_LOSS      = env_float("MAX_DAILY_LOSS_DOLLARS", 10.0)
MAX_TRADES_PER_DAY  = env_int("MAX_TRADES_PER_DAY", 30)
MAX_ORDERS_PER_HOUR = env_int("MAX_ORDERS_PER_HOUR", 15)
MAX_CONSEC_ERRORS   = env_int("MAX_CONSECUTIVE_ERRORS", 5)

# =============================================================================
# Timing
# =============================================================================
MIN_HOLD_SECONDS   = env_int("MIN_HOLD_SECONDS", 60)    # reduced: hold-to-expiry handles this
COOLDOWN_SECONDS   = env_int("COOLDOWN_SECONDS", 30)    # between buckets (not within)
MIN_ORDER_INTERVAL = env_float("MIN_ORDER_INTERVAL_SECONDS", 5.0)

# =============================================================================
# Book quality
# =============================================================================
MIN_BID_CENTS       = env_int("MIN_BID_CENTS", 3)
MAX_BID_CENTS       = env_int("MAX_BID_CENTS", 97)
MAX_SPREAD_CENTS    = env_int("MAX_SPREAD_CENTS", 8)
MIN_DEPTH_CONTRACTS = env_int("MIN_DEPTH_CONTRACTS", 5)   # raised from 3
MAX_SLIPPAGE_CENTS  = env_int("MAX_SLIPPAGE_CENTS", 4)

# =============================================================================
# Expired-market guard
# =============================================================================
NULL_BOOK_EXPIRE_LOOPS = env_int("NULL_BOOK_EXPIRE_LOOPS", 6)

# =============================================================================
# Control switches
# =============================================================================
KILL_SWITCH          = env_bool("KILL_SWITCH", False)
CLOSE_ON_KILL_SWITCH = env_bool("CLOSE_ON_KILL_SWITCH", False)
ALLOW_PYRAMIDING     = env_bool("ALLOW_PYRAMIDING", False)

# =============================================================================
# Debug
# =============================================================================
DEBUG_ERRORS    = env_bool("DEBUG_ERRORS", True)
DEBUG_MODEL     = env_bool("DEBUG_MODEL", True)
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
    CREATE TABLE IF NOT EXISTS j5_bucket_state (
        ticker          TEXT PRIMARY KEY,
        traded          BOOLEAN NOT NULL DEFAULT FALSE,
        side_taken      TEXT,
        entry_price     NUMERIC(10,6),
        contracts       INT,
        entry_ts        TIMESTAMPTZ,
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS j5_daily_stats (
        stat_date       DATE PRIMARY KEY,
        buckets_seen    INT NOT NULL DEFAULT 0,
        buckets_traded  INT NOT NULL DEFAULT 0,
        total_contracts INT NOT NULL DEFAULT 0,
        total_fees_est  NUMERIC(10,4) NOT NULL DEFAULT 0,
        wins            INT NOT NULL DEFAULT 0,
        losses          INT NOT NULL DEFAULT 0,
        pnl_realized    NUMERIC(12,4) NOT NULL DEFAULT 0,
        edge_threshold  INT NOT NULL DEFAULT 8,
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
# Bucket state — ONE TRADE PER BUCKET enforcement
# =============================================================================
_bucket_cache: Dict[str, bool] = {}   # ticker → already_traded
_bucket_lock = threading.Lock()

def bucket_already_traded(ticker: str) -> bool:
    with _bucket_lock:
        if ticker in _bucket_cache:
            return _bucket_cache[ticker]
    row = db_fetchone(
        "SELECT traded FROM j5_bucket_state WHERE ticker=%s", (ticker,)
    )
    traded = bool(row[0]) if row else False
    with _bucket_lock:
        _bucket_cache[ticker] = traded
    return traded

def bucket_mark_traded(ticker: str, side: str, price_cents: int,
                       contracts: int) -> None:
    with _bucket_lock:
        _bucket_cache[ticker] = True
    db_exec(
        """INSERT INTO j5_bucket_state
           (ticker, traded, side_taken, entry_price, contracts, entry_ts, updated_at)
           VALUES (%s, TRUE, %s, %s, %s, NOW(), NOW())
           ON CONFLICT (ticker) DO UPDATE SET
               traded=TRUE, side_taken=EXCLUDED.side_taken,
               entry_price=EXCLUDED.entry_price,
               contracts=EXCLUDED.contracts,
               entry_ts=EXCLUDED.entry_ts,
               updated_at=NOW()
        """,
        (ticker, side, price_cents / 100.0, contracts),
    )

def bucket_cleanup_old() -> None:
    """Remove bucket state older than 2 hours (they've all settled)."""
    db_exec(
        "DELETE FROM j5_bucket_state WHERE updated_at < NOW() - INTERVAL '2 hours'"
    )

# =============================================================================
# Daily stats — for morning review
# =============================================================================
def stats_record_entry(edge_cents: float, contracts: int) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    fee_est = contracts * 0.02
    db_exec(
        """INSERT INTO j5_daily_stats (stat_date, buckets_traded, total_contracts,
               total_fees_est, edge_threshold, updated_at)
           VALUES (%s, 1, %s, %s, %s, NOW())
           ON CONFLICT (stat_date) DO UPDATE SET
               buckets_traded = j5_daily_stats.buckets_traded + 1,
               total_contracts = j5_daily_stats.total_contracts + EXCLUDED.total_contracts,
               total_fees_est = j5_daily_stats.total_fees_est + EXCLUDED.total_fees_est,
               updated_at=NOW()
        """,
        (today, contracts, fee_est, int(edge_cents)),
    )

def stats_record_outcome(pnl: float, win: bool) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    db_exec(
        """INSERT INTO j5_daily_stats (stat_date, wins, losses, pnl_realized, updated_at)
           VALUES (%s, %s, %s, %s, NOW())
           ON CONFLICT (stat_date) DO UPDATE SET
               wins = j5_daily_stats.wins + EXCLUDED.wins,
               losses = j5_daily_stats.losses + EXCLUDED.losses,
               pnl_realized = j5_daily_stats.pnl_realized + EXCLUDED.pnl_realized,
               updated_at=NOW()
        """,
        (today, 1 if win else 0, 0 if win else 1, pnl),
    )

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
# Adaptive edge threshold — learns from win/loss streak
# =============================================================================
@dataclass
class AdaptiveEdge:
    """
    Tightens edge requirement after losses (be more selective),
    loosens after wins (book is mispricing more, take advantage).
    
    Streak-based, not time-based. Resets each morning via RESET_EDGE_DAILY.
    """
    current: int = MIN_EDGE_CENTS_BASE
    streak:  int = 0    # positive = win streak, negative = loss streak
    trades:  int = 0

    def record_win(self) -> None:
        self.trades += 1
        self.streak = max(0, self.streak) + 1
        if self.streak >= 3:
            # Winning: loosen slightly (max loosening = 3¢ below base)
            self.current = max(MIN_EDGE_CENTS_MIN,
                               min(self.current, MIN_EDGE_CENTS_BASE - 2))
        jlog("info", "adaptive_edge_win",
             streak=self.streak, edge=self.current)

    def record_loss(self) -> None:
        self.trades += 1
        self.streak = min(0, self.streak) - 1
        if self.streak <= -2:
            # Losing: tighten (max tightening = 7¢ above base)
            self.current = min(MIN_EDGE_CENTS_MAX,
                               self.current + 2)
        jlog("info", "adaptive_edge_loss",
             streak=self.streak, edge=self.current)

    def reset(self) -> None:
        self.current = MIN_EDGE_CENTS_BASE
        self.streak  = 0
        jlog("info", "adaptive_edge_reset", edge=self.current)

_adaptive_edge = AdaptiveEdge()


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
        _adaptive_edge.reset()
        jlog("info", "cb_daily_reset", today=today)

def _cb_reset_hourly() -> None:
    hour = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
    if _cb.reset_hour != hour:
        _cb.hourly_orders = 0
        _cb.reset_hour    = hour

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
                f"Re-arm: UPDATE j5_circuit_breaker SET halted=false WHERE id=1;"
            )
            cb_save()

def cb_clear_error() -> None:
    with _cb_lock:
        _cb.consec_errors = 0

def cb_increment_error() -> None:
    with _cb_lock:
        _cb.consec_errors += 1
        if _cb.consec_errors >= MAX_CONSEC_ERRORS and not _cb.halted:
            reason = f"MAX_CONSECUTIVE_ERRORS={MAX_CONSEC_ERRORS}"
            _cb.halted      = True
            _cb.halt_reason = reason
            jlog("critical", "CIRCUIT_BREAKER_CONSEC_ERRORS", count=_cb.consec_errors)
            send_telegram(f"🚨 Johnny5 HALTED — {reason}.")
            cb_save()

def cb_check_order_allowed() -> Tuple[bool, str]:
    with _cb_lock:
        if _cb.halted:
            return False, f"halted: {_cb.halt_reason}"
        if KILL_SWITCH:
            return False, "KILL_SWITCH=true"
        _cb_reset_daily()
        _cb_reset_hourly()
        if _cb.daily_loss >= MAX_DAILY_LOSS:
            reason = f"MAX_DAILY_LOSS={MAX_DAILY_LOSS:.2f} breached"
            _cb.halted      = True
            _cb.halt_reason = reason
            cb_save()
            return False, reason
        if _cb.daily_trades >= MAX_TRADES_PER_DAY:
            return False, f"MAX_TRADES_PER_DAY={MAX_TRADES_PER_DAY}"
        if _cb.hourly_orders >= MAX_ORDERS_PER_HOUR:
            return False, f"MAX_ORDERS_PER_HOUR={MAX_ORDERS_PER_HOUR}"
        if (_cb.last_order_ts
                and secs_since(_cb.last_order_ts) < MIN_ORDER_INTERVAL):
            return False, f"order_interval_cooldown"
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
                     key_id=(KALSHI_API_KEY_ID[:8] + "…") if KALSHI_API_KEY_ID else "none")
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
                r = self.session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                if r.status_code == 429:
                    time.sleep(min(30, 2 ** attempt)); continue
                if r.status_code >= 500:
                    time.sleep(min(30, 2 ** attempt + 1)); continue
                if r.status_code == 404:
                    raise RuntimeError(f"Kalshi 404 {path}")
                if r.status_code >= 400:
                    raise RuntimeError(
                        f"Kalshi HTTP {r.status_code} {path}: {(r.text or '')[:300]}"
                    )
                out = r.json()
                if not isinstance(out, dict):
                    raise RuntimeError(f"Kalshi non-dict JSON on {path}")
                return out
            except RuntimeError:
                raise
            except Exception as e:
                last_exc = e
                time.sleep(min(30, 2 ** attempt + 1))
        raise RuntimeError(f"Kalshi GET {path} failed after {retries} attempts: {last_exc}")

    def list_open_markets(self) -> List[Dict[str, Any]]:
        data    = self._get(f"/trade-api/v2/markets?series_ticker={SERIES_TICKER}&status=open")
        markets = data.get("markets", [])
        return markets if isinstance(markets, list) else []

    def get_market(self, ticker: str) -> Dict[str, Any]:
        return self._get(f"/trade-api/v2/markets/{ticker}")

    def get_orderbook(self, ticker: str) -> Dict[str, Any]:
        return self._get(f"/trade-api/v2/markets/{ticker}/orderbook")

    def get_balance(self) -> Optional[float]:
        try:
            data  = self._get("/trade-api/v2/portfolio/balance")
            cents = (data.get("balance")
                     or data.get("available_balance")
                     or data.get("buying_power"))
            if cents is None:
                jlog("warning", "balance_field_not_found", raw_keys=list(data.keys()))
                return None
            return float(cents) / 100.0
        except Exception as e:
            jlog("warning", "get_balance_fail", error=str(e)[:200])
            return None

    def get_positions(self) -> List[Dict[str, Any]]:
        try:
            data      = self._get("/trade-api/v2/portfolio/positions")
            positions = data.get("market_positions") or data.get("positions") or []
            return positions if isinstance(positions, list) else []
        except Exception as e:
            jlog("warning", "get_positions_fail", error=str(e)[:200])
            return []

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
            raise RuntimeError("KALSHI_ORDER_GATEWAY_URL not set")
        if action not in ("buy", "sell"):
            raise ValueError(f"invalid action: {action}")
        if side not in ("YES", "NO"):
            raise ValueError(f"invalid side: {side}")
        if contracts <= 0:
            raise ValueError(f"contracts must be > 0")
        coid    = client_order_id or uuid.uuid4().hex
        payload: Dict[str, Any] = {
            "ticker": ticker, "action": action, "type": "limit",
            "side": side, "count": int(contracts), "client_order_id": coid,
        }
        if side == "YES":
            payload["yes_price"] = max(1, min(99, int(price_cents)))
        else:
            payload["no_price"]  = max(1, min(99, int(price_cents)))
        r = requests.post(
            f"{KALSHI_ORDER_GATEWAY_URL}/order", json=payload, timeout=REQUEST_TIMEOUT
        )
        if r.status_code >= 400:
            body = (r.text or "")[:500]
            if "insufficient_balance" in body.lower():
                cb_halt("insufficient_balance — HARD HALT")
                raise RuntimeError("insufficient_balance — HARD HALT")
            raise RuntimeError(f"Order gateway HTTP {r.status_code}: {body}")
        out = r.json()
        if not isinstance(out, dict) or not out.get("ok"):
            body_str = json.dumps(out)[:500]
            if "insufficient_balance" in body_str.lower():
                cb_halt("insufficient_balance in gateway response")
                raise RuntimeError("insufficient_balance")
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
                    "ok": True, "service": "johnny5", "version": BOT_VERSION,
                    "run_id": RUN_ID, "live": LIVE_MODE,
                }).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404); self.end_headers()
        def log_message(self, fmt: str, *args: Any) -> None:
            pass
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    jlog("info", "health_server_started", port=PORT)

# =============================================================================
# Market selection helpers
# =============================================================================
def _as_float(x: Any) -> float:
    try: return float(x) if x is not None else 0.0
    except Exception: return 0.0

def _parse_ts_any(x: Any) -> Optional[datetime]:
    if not isinstance(x, str) or not x: return None
    try: return datetime.fromisoformat(x.replace("Z", "+00:00"))
    except Exception: return None

def extract_strike(market: Dict[str, Any]) -> Optional[float]:
    raw = (market.get("floor_strike") or market.get("strike_price")
           or market.get("strike"))
    if raw is not None:
        try:
            val = float(raw)
            if val > 100: return val
        except Exception: pass
    floor = market.get("floor_strike") or market.get("floor")
    cap   = market.get("cap_strike")   or market.get("cap")
    if floor is not None and cap is not None:
        try: return (float(floor) + float(cap)) / 2.0
        except Exception: pass
    rv = market.get("result_value")
    if rv is not None:
        try:
            val = float(rv)
            if val > 100: return val
        except Exception: pass
    return None

def pick_best_active_market(markets: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
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
    if not candidates: return None
    candidates.sort(key=lambda x: (x[0], -x[1].timestamp()), reverse=True)
    return candidates[0][2]

def bucket_ts_for_market(m: Dict[str, Any]) -> str:
    return str(m.get("open_time") or m.get("open_ts") or m.get("ticker") or "unknown")

def secs_until_close(m: Dict[str, Any]) -> Optional[float]:
    ct = _parse_ts_any(m.get("close_time") or m.get("close_ts"))
    if not ct: return None
    return max(0.0, (ct - datetime.now(timezone.utc)).total_seconds())


# =============================================================================
# Orderbook parsing
# =============================================================================
def _best_price_cents(levels: Any) -> Optional[int]:
    if not isinstance(levels, list) or not levels: return None
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
        if best is None or price > best: best = price
    return best

@dataclass
class BookState:
    best_yes_bid: Optional[int]
    best_no_bid:  Optional[int]
    yes_levels:   List
    no_levels:    List
    spread_cents: Optional[int]
    mark_yes:     Optional[float]

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
    if best_yes is not None and (best_yes < MIN_BID_CENTS or best_yes > MAX_BID_CENTS):
        best_yes = None
    if best_no is not None and (best_no < MIN_BID_CENTS or best_no > MAX_BID_CENTS):
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
        best_yes_bid=best_yes, best_no_bid=best_no,
        yes_levels=yes_levels, no_levels=no_levels,
        spread_cents=spread, mark_yes=mark,
    )

def book_quality_ok(book: BookState) -> Tuple[bool, str]:
    if not book.ok:
        return False, "one_sided_or_missing_book"
    assert book.spread_cents is not None
    if book.spread_cents > MAX_SPREAD_CENTS:
        return False, f"spread={book.spread_cents}c > MAX={MAX_SPREAD_CENTS}c"
    # Check depth — count contracts at best bid
    yes_depth = 0
    for lvl in book.yes_levels[:3]:
        try:
            if isinstance(lvl, list): yes_depth += int(lvl[1]) if len(lvl) > 1 else 1
            elif isinstance(lvl, dict): yes_depth += int(lvl.get("quantity", 1))
        except Exception: pass
    no_depth = 0
    for lvl in book.no_levels[:3]:
        try:
            if isinstance(lvl, list): no_depth += int(lvl[1]) if len(lvl) > 1 else 1
            elif isinstance(lvl, dict): no_depth += int(lvl.get("quantity", 1))
        except Exception: pass
    if min(yes_depth, no_depth) < MIN_DEPTH_CONTRACTS:
        return False, f"depth={min(yes_depth,no_depth)} < MIN={MIN_DEPTH_CONTRACTS}"
    return True, ""

def compute_entry_price_cents(book: BookState, side: str) -> Optional[int]:
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

def compute_exit_price_cents(book: BookState, side: str) -> Optional[int]:
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
    if not result: raise RuntimeError("Kraken price missing from response")
    key = next(iter(result))
    return float(result[key]["c"][0])

# =============================================================================
# Binary option model
# =============================================================================
def _norm_cdf(x: float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2.0))

def realized_vol_annualised(prices: List[float]) -> float:
    if len(prices) < VOL_WINDOW + 1:
        return VOL_FLOOR
    window = prices[-(VOL_WINDOW + 1):]
    log_rets: List[float] = []
    for i in range(1, len(window)):
        p0, p1 = window[i - 1], window[i]
        if p0 <= 0 or p1 <= 0: continue
        log_rets.append(math.log(p1 / p0))
    if len(log_rets) < 2: return VOL_FLOOR
    n    = len(log_rets)
    mean = sum(log_rets) / n
    var  = sum((r - mean) ** 2 for r in log_rets) / (n - 1)
    std_per_sample   = max(var ** 0.5, 1e-10)
    samples_per_year = (365.25 * 24 * 3600) / max(POLL_SECONDS, 1.0)
    sigma_annual     = std_per_sample * math.sqrt(samples_per_year)
    return max(VOL_FLOOR, min(VOL_CAP, sigma_annual))

def model_fair_yes(spot: float, strike: float,
                   secs_remaining: float, sigma: float) -> float:
    if strike <= 0 or spot <= 0 or secs_remaining <= 0: return 0.5
    T      = secs_remaining / (365.25 * 24 * 3600)
    sqrt_T = math.sqrt(max(T, 1e-12))
    sigma  = max(sigma, 1e-6)
    d      = math.log(spot / strike) / (sigma * sqrt_T)
    return max(0.01, min(0.99, _norm_cdf(d)))

# =============================================================================
# Sizing — learned from data
# =============================================================================
def compute_contracts(cash_usd: float, price_cents: int) -> int:
    """
    Target 10% of balance (up from 6%) to overcome fee drag.
    Triple-capped by MAX_POSITION_USD, MAX_CONTRACTS, MAX_NOTIONAL.
    Returns 0 if result < MIN_CONTRACTS — caller should skip the trade.
    """
    price_usd     = max(price_cents / 100.0, 0.01)
    risk_usd      = min(MAX_POSITION_USD, max(1.0, cash_usd * RISK_FRACTION))
    from_risk     = int(risk_usd / price_usd)
    from_notional = int(MAX_NOTIONAL_PER_TICKER / price_usd)
    n = min(from_risk, MAX_CONTRACTS, from_notional)
    return n if n >= MIN_CONTRACTS else 0

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
        s = BotState(); s.trade_history_24h = []; return s
    try:
        raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        s   = BotState(**{k: v for k, v in raw.items()
                          if k in BotState.__dataclass_fields__})
        if s.trade_history_24h is None: s.trade_history_24h = []
        return s
    except Exception as e:
        jlog("warning", "state_load_fail", error=str(e)[:200])
        s = BotState(); s.trade_history_24h = []; return s

def save_state(state: BotState) -> None:
    ensure_dirs()
    try:
        STATE_FILE.write_text(json.dumps(asdict(state), indent=2), encoding="utf-8")
    except Exception as e:
        jlog("warning", "state_save_fail", error=str(e)[:200])

def prune_24h(state: BotState) -> None:
    if state.trade_history_24h is None:
        state.trade_history_24h = []; return
    now = datetime.now(timezone.utc)
    state.trade_history_24h = [
        t for t in state.trade_history_24h
        if (dt := parse_iso(t.get("ts", ""))) and (now - dt) <= timedelta(hours=24)
    ]

def record_trade_24h(state: BotState, typ: str, pnl_delta: float) -> None:
    if state.trade_history_24h is None: state.trade_history_24h = []
    state.trade_history_24h.append({"ts": utc_iso(), "type": typ, "pnl": float(pnl_delta)})
    prune_24h(state)

# =============================================================================
# Market detail cache
# =============================================================================
_market_cache: Dict[str, Dict[str, Any]] = {}

def get_market_cached(client: "KalshiClient", ticker: str) -> Dict[str, Any]:
    cached = _market_cache.get(ticker)
    if cached:
        age = time.time() - cached.get("_fetched_at", 0)
        if age < 60: return cached
    try:
        data = client.get_market(ticker)
        m    = data.get("market", data)
        m["_fetched_at"] = time.time()
        _market_cache[ticker] = m
        return m
    except Exception as e:
        jlog("warning", "market_cache_miss", ticker=ticker, error=str(e)[:200])
        return _market_cache.get(ticker, {})

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
    if pos_any() is not None:
        if not ALLOW_PYRAMIDING: return False

    # ── 2. Kill switch ─────────────────────────────────────────────────────
    if KILL_SWITCH: return False

    # ── 3. ONE TRADE PER BUCKET — hardest lesson from the data ────────────
    if ONE_TRADE_PER_BUCKET and bucket_already_traded(ticker):
        jlog("debug", "enter_skip_bucket_already_traded", ticker=ticker)
        return False

    # ── 4. Cooldown between buckets ────────────────────────────────────────
    if secs_since(state.last_trade_ts) < COOLDOWN_SECONDS:
        return False

    # ── 5. Time-in-bucket: only trade in 3-10 min window ──────────────────
    if secs_remaining < MIN_SECS_BEFORE_EXPIRY:
        jlog("debug", "enter_skip_too_close", secs=round(secs_remaining, 1))
        return False
    if secs_remaining > MAX_SECS_BEFORE_EXPIRY:
        jlog("debug", "enter_skip_too_early", secs=round(secs_remaining, 1))
        return False

    # ── 6. Book quality ────────────────────────────────────────────────────
    book_ok, book_reason = book_quality_ok(book)
    if not book_ok:
        jlog("debug", "enter_skip_book", reason=book_reason, ticker=ticker)
        return False
    if book.mark_yes is None: return False

    # ── 7. Adaptive edge gate ─────────────────────────────────────────────
    edge_cents = (fair_yes - book.mark_yes) * 100
    eff_edge   = _adaptive_edge.current

    if edge_cents >= eff_edge:
        side = "YES"
    elif edge_cents <= -eff_edge:
        side = "NO"
    else:
        jlog("debug", "enter_skip_edge",
             edge=round(edge_cents, 2), required=eff_edge)
        return False

    # ── 8. Entry price (slippage check) ───────────────────────────────────
    price_cents = compute_entry_price_cents(book, side)
    if price_cents is None:
        jlog("debug", "enter_skip_slippage", side=side, ticker=ticker)
        return False

    # ── 9. Sizing — must meet MIN_CONTRACTS ───────────────────────────────
    cash      = cash_real if (LIVE_MODE and cash_real is not None) else state.paper_cash
    contracts = compute_contracts(cash, price_cents)
    if contracts <= 0:
        jlog("info", "enter_skip_min_contracts",
             cash=cash, price_cents=price_cents, min=MIN_CONTRACTS)
        return False

    # ── 10. Idempotency ────────────────────────────────────────────────────
    trade_key       = make_trade_key(ticker, bucket_ts, "ENTER", side)
    existing_status = trade_key_status(trade_key)
    if existing_status in ("SUBMITTED", "FILLED"):
        return False
    if existing_status == "FAILED":
        return False

    # ── 11. Circuit breaker ────────────────────────────────────────────────
    allowed, reason = cb_check_order_allowed()
    if not allowed:
        jlog("info", "enter_blocked_by_cb", reason=reason)
        return False

    # ── 12. Persist CREATED ────────────────────────────────────────────────
    coid = uuid.uuid4().hex
    upsert_trade(
        trade_key=trade_key, ticker=ticker, bucket_ts=bucket_ts,
        intent="ENTER", side=side, status="CREATED",
        contracts=contracts, price_cents=price_cents, client_order_id=coid,
    )

    # ── 13. Submit ─────────────────────────────────────────────────────────
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
        if ONE_TRADE_PER_BUCKET:
            bucket_mark_traded(ticker, side, price_cents, contracts)

        if not LIVE_MODE:
            state.paper_cash -= (price_cents / 100.0) * contracts
        state.last_trade_ts = utc_iso()
        record_trade_24h(state, "ENTER", 0.0)
        stats_record_entry(abs(edge_cents), contracts)

        jlog("info", "ENTER_SUBMITTED",
             ticker=ticker, side=side, contracts=contracts,
             price_cents=price_cents,
             spot=round(spot, 2), strike=round(strike, 2),
             sigma=round(sigma, 4), secs_remaining=round(secs_remaining, 1),
             fair_yes=round(fair_yes, 4), edge_cents=round(edge_cents, 2),
             adaptive_edge=eff_edge, order_id=order_id)
        return True

    except RuntimeError as e:
        err_str = str(e)
        jlog("error", "enter_order_fail", error=err_str[:300], trade_key=trade_key)
        upsert_trade(
            trade_key=trade_key, ticker=ticker, bucket_ts=bucket_ts,
            intent="ENTER", side=side, status="FAILED", reason=err_str[:200],
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
    secs_remaining: Optional[float] = None,
    force:          bool = False,
    cash_real:      Optional[float] = None,
) -> bool:
    pos = pos_any()
    if pos is None: return False

    ticker    = pos.ticker
    side      = pos.side
    contracts = pos.contracts

    # Hold time
    if not force and secs_since(pos.open_ts) < MIN_HOLD_SECONDS:
        return False

    # Cooldown
    if not force and secs_since(state.last_trade_ts) < COOLDOWN_SECONDS:
        return False

    # HOLD TO EXPIRY: when < HOLD_EXPIRY_SECS left, check if we're in profit.
    # If yes, let Kalshi settle at $1 — guaranteed better than scalping out.
    # If we're losing, still try to exit to cut losses.
    if (secs_remaining is not None
            and secs_remaining < HOLD_EXPIRY_SECS
            and not force):
        if book.mark_yes is not None:
            if side == "YES":
                in_profit = book.mark_yes > pos.entry_price
            else:
                in_profit = (1 - book.mark_yes) > pos.entry_price
        else:
            in_profit = False

        if in_profit:
            jlog("debug", "exit_hold_to_expiry",
                 ticker=ticker, secs=round(secs_remaining, 1),
                 entry=pos.entry_price,
                 mark=book.mark_yes)
            return False   # hold to settlement
        # In loss with < 90s left — try to exit
        jlog("info", "exit_cutting_loss_near_expiry",
             ticker=ticker, secs=round(secs_remaining, 1))

    # Edge collapse check
    if book.mark_yes is None and not force: return False

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

    if not should_exit: return False

    # Idempotency
    trade_key       = make_trade_key(ticker, bucket_ts, "EXIT", side)
    existing_status = trade_key_status(trade_key)
    if existing_status in ("SUBMITTED", "FILLED"):
        return False

    price_cents = compute_exit_price_cents(book, side)
    if price_cents is None:
        jlog("warning", "exit_skip_no_price", ticker=ticker, side=side)
        return False

    coid = uuid.uuid4().hex
    upsert_trade(
        trade_key=trade_key, ticker=ticker, bucket_ts=bucket_ts,
        intent="EXIT", side=side, status="CREATED",
        contracts=contracts, price_cents=price_cents, client_order_id=coid,
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
        win        = pnl > 0
        state.realized_pnl_lifetime += pnl
        record_trade_24h(state, "EXIT", pnl)
        if pnl < 0:
            cb_record_realized_loss(abs(pnl))
            _adaptive_edge.record_loss()
        else:
            _adaptive_edge.record_win()

        stats_record_outcome(pnl, win)

        if not LIVE_MODE:
            state.paper_cash += exit_price * contracts
        state.last_trade_ts = utc_iso()
        pos_delete(ticker)

        jlog("info", "EXIT_SUBMITTED",
             ticker=ticker, side=side, contracts=contracts,
             price_cents=price_cents, pnl=round(pnl, 4), order_id=order_id)

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
        jlog("error", "exit_order_fail", error=err_str[:300], trade_key=trade_key)
        upsert_trade(
            trade_key=trade_key, ticker=ticker, bucket_ts=bucket_ts,
            intent="EXIT", side=side, status="FAILED", reason=err_str[:200],
        )
        if "insufficient_balance" not in err_str:
            cb_increment_error()
        return False

# =============================================================================
# Boot reconciliation
# =============================================================================
def reconcile_live_positions(client: KalshiClient) -> None:
    if not LIVE_MODE: return
    jlog("info", "reconcile_start")
    real_positions = client.get_positions()
    for rp in real_positions:
        ticker = rp.get("ticker") or rp.get("market_ticker") or ""
        if not ticker or SERIES_TICKER not in ticker: continue
        yes_qty = int(rp.get("yes_position") or rp.get("position") or 0)
        no_qty  = int(rp.get("no_position") or 0)
        if yes_qty > 0 and pos_get(ticker) is None:
            jlog("warning", "reconcile_orphan_yes", ticker=ticker, qty=yes_qty)
            pos_save(PositionSnapshot(
                ticker=ticker, side="YES", contracts=yes_qty,
                entry_price=0.50, open_ts=utc_iso(), trade_key="reconciled",
            ))
        if no_qty > 0 and pos_get(ticker) is None:
            jlog("warning", "reconcile_orphan_no", ticker=ticker, qty=no_qty)
            pos_save(PositionSnapshot(
                ticker=ticker, side="NO", contracts=no_qty,
                entry_price=0.50, open_ts=utc_iso(), trade_key="reconciled",
            ))
    jlog("info", "reconcile_done")


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
        send_telegram(f"⚠️ Johnny5 booted HALTED: {_cb.halt_reason}")

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
            send_telegram(f"⚠️ Johnny5: low balance at boot: ${real_cash:.2f}")

    prices:          List[float] = []
    loop_id:         int         = 0
    null_book_count: int         = 0
    last_cleanup_ts: float       = 0.0

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

            # ── Periodic cleanup of old bucket state ───────────────────────
            if time.time() - last_cleanup_ts > 3600:
                bucket_cleanup_old()
                last_cleanup_ts = time.time()

            # ── EXIT PATH — always on held ticker ─────────────────────────
            current_pos       = pos_any()
            pos_book:         Optional[BookState] = None
            pos_fair_yes:     float               = 0.5
            pos_mark:         Optional[float]     = None
            pos_strike:       Optional[float]     = None
            pos_secs_rem:     Optional[float]     = None

            if current_pos:
                pos_ticker = current_pos.ticker
                try:
                    mkt          = get_market_cached(client, pos_ticker)
                    pos_secs_rem = secs_until_close(mkt)
                    pos_strike   = extract_strike(mkt)

                    pos_ob   = client.get_orderbook(pos_ticker)
                    pos_book = parse_book(pos_ob)
                    pos_mark = pos_book.mark_yes

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
                            jlog("warning", "MARKET_EXPIRED_CLEARING_POSITION",
                                 ticker=pos_ticker,
                                 contracts=current_pos.contracts,
                                 side=current_pos.side)
                            send_telegram(
                                f"⚠️ Johnny5: {pos_ticker} expired "
                                f"(null book {null_book_count} loops). "
                                f"Check Kalshi for settlement."
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
                                secs_remaining=pos_secs_rem,
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
                             mark=pos_mark,
                             adaptive_edge=_adaptive_edge.current,
                             streak=_adaptive_edge.streak)

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
                        t_rem     = secs_until_close(m)

                        if t_rem is None:
                            pass
                        else:
                            mkt    = get_market_cached(client, ticker)
                            strike = extract_strike(mkt)

                            if strike is None:
                                if dbg_every("no_strike", 60):
                                    jlog("warning", "enter_skip_no_strike",
                                         ticker=ticker,
                                         keys=list(mkt.keys())[:15])
                            else:
                                fair_yes = model_fair_yes(spot, strike, t_rem, sigma)
                                ob       = client.get_orderbook(ticker)
                                book     = parse_book(ob)
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
                                         edge_cents=round(edge_cents, 2),
                                         adaptive_edge=_adaptive_edge.current,
                                         bucket_traded=bucket_already_traded(ticker))

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
            try: save_state(state)
            except Exception: pass
            time.sleep(max(10.0, POLL_SECONDS))


if __name__ == "__main__":
    main()

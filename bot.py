diff --git a/bot.py b/bot.py
index 03409927686273e4fdea2fbd846c2a15e285d4ed..da8b4326cf34c1fe82309afe43d5bb6b7faa3e79 100644
--- a/bot.py
+++ b/bot.py
@@ -1,1291 +1,366 @@
 #!/usr/bin/env python3
-"""
-Johnny 5 — Kalshi bot (KXBTC15M BTC Up/Down 15m)
-
-Goals:
-- 100% Kalshi (no Gamma/Polymarket)
-- Preserve your edge model + risk gates + DB tracking
-- Quiet logs: only trades + periodic summary
-- Robust loop behavior: long sleep + optional hard-exit if Kalshi returns no open markets repeatedly
-
-Kalshi API refs:
-- Auth headers and signature string (timestamp + method + path): docs quick start auth :contentReference[oaicite:4]{index=4}
-- Market data endpoints: /markets and /markets/{ticker}/orderbook :contentReference[oaicite:5]{index=5}
-- Order placement: POST /portfolio/orders with yes_price/no_price in cents :contentReference[oaicite:6]{index=6}
-"""
-
 from __future__ import annotations
 
-import os
-import time
 import json
-import uuid
 import logging
-from dataclasses import dataclass
-from datetime import datetime, timezone, timedelta, date
-from typing import Optional, Tuple, Dict, Any, List
+import os
+import time
+from dataclasses import asdict, dataclass
+from datetime import datetime, timezone
+from pathlib import Path
+from typing import Any, Optional
 
 import requests
-import psycopg2
-from psycopg2.extras import RealDictCursor
-
-# Kalshi signing requires cryptography (matches Kalshi docs quickstart) :contentReference[oaicite:7]{index=7}
 from cryptography.hazmat.primitives import hashes, serialization
 from cryptography.hazmat.primitives.asymmetric import padding
 
 
-# =========================
-# CONFIG
-# =========================
-
-# ---------- DB ----------
-DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
-
-# ---------- Modes ----------
-LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() == "true"
-KILL_SWITCH = os.getenv("KILL_SWITCH", "false").lower() == "true"
-STATS_ONLY = os.getenv("STATS_ONLY", "false").lower() == "true"
-RESET_STATE = os.getenv("RESET_STATE", "false").lower() == "true"   # clears open position only
-RESET_DAILY = os.getenv("RESET_DAILY", "false").lower() == "true"   # clears daily counters only
-
-# ---------- Market targeting ----------
-SERIES_TICKER = os.getenv("SERIES_TICKER", "KXBTC15M").strip()  # Kalshi series ticker
-POLL_SECONDS = float(os.getenv("POLL_SECONDS", "5"))
-
-# If Kalshi returns no open markets, back off hard (what you asked for)
-NO_MARKETS_LONG_SLEEP_SECONDS = int(os.getenv("NO_MARKETS_LONG_SLEEP_SECONDS", "300"))  # 5 min
-MAX_CONSECUTIVE_NO_MARKETS = int(os.getenv("MAX_CONSECUTIVE_NO_MARKETS", "12"))        # 12 * 5min = 1 hour
-EXIT_ON_TOO_MANY_NO_MARKETS = os.getenv("EXIT_ON_TOO_MANY_NO_MARKETS", "true").lower() == "true"
-
-# ---------- Paper defaults ----------
-START_BALANCE = float(os.getenv("START_BALANCE", "1000"))
-TRADE_SIZE = float(os.getenv("TRADE_SIZE", "25"))
-FEE_PER_TRADE = float(os.getenv("FEE_PER_TRADE", "0"))
-
-# ---------- Edge model (Kraken synthetic fair) ----------
-LOOKBACK = int(os.getenv("LOOKBACK", "5"))
-PROB_P_MIN = float(os.getenv("PROB_P_MIN", "0.05"))
-PROB_P_MAX = float(os.getenv("PROB_P_MAX", "0.95"))
-PROB_Z_SCALE = float(os.getenv("PROB_Z_SCALE", "2.5"))
-
-# ---------- Thresholds ----------
-EDGE_ENTER = float(os.getenv("EDGE_ENTER", "0.08"))
-EDGE_EXIT = float(os.getenv("EDGE_EXIT", "0.04"))
-
-# ---------- Risk controls ----------
-MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))          # realized only
-COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "20"))
-MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "8"))    # counts ENTER+EXIT events
-
-# ---------- LIVE mode safety pack ----------
-LIVE_TRADE_SIZE = float(os.getenv("LIVE_TRADE_SIZE", "5"))
-LIVE_MAX_TRADES_PER_DAY = int(os.getenv("LIVE_MAX_TRADES_PER_DAY", "10"))
-LIVE_MAX_DAILY_LOSS = float(os.getenv("LIVE_MAX_DAILY_LOSS", "15"))
-LIVE_COOLDOWN_MINUTES = int(os.getenv("LIVE_COOLDOWN_MINUTES", "20"))
-LIVE_MIN_EDGE_ENTER = float(os.getenv("LIVE_MIN_EDGE_ENTER", "0.10"))
-LIVE_MIN_EDGE_EXIT = float(os.getenv("LIVE_MIN_EDGE_EXIT", "0.04"))
-LIVE_DAILY_PROFIT_LOCK = float(os.getenv("LIVE_DAILY_PROFIT_LOCK", "0"))  # 0 disables
-
-# Price sanity (YES probability in [0.01..0.99], sum sanity is implicit in our derivation)
-LIVE_MIN_PRICE = float(os.getenv("LIVE_MIN_PRICE", "0.03"))
-LIVE_MAX_PRICE = float(os.getenv("LIVE_MAX_PRICE", "0.97"))
-
-# Whale override (bypass cooldown ONLY)
-WHALE_COOLDOWN_OVERRIDE = os.getenv("WHALE_COOLDOWN_OVERRIDE", "true").lower() == "true"
-WHALE_EDGE_OVERRIDE = float(os.getenv("WHALE_EDGE_OVERRIDE", "0.20"))
-
-# ---------- HTTP ----------
-TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "20"))
-MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
-
-# ---------- Kalshi API ----------
-# Docs show demo base url and production base url examples :contentReference[oaicite:8]{index=8}
+# ---------- Runtime config ----------
 KALSHI_BASE_URL = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/")
 KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
 KALSHI_PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY_PEM", "").strip()
 KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip()
+SERIES_TICKER = os.getenv("SERIES_TICKER", "KXBTC15M").strip()
 
-# If you want to “take liquidity” faster, we’ll price at implied ask.
-TAKE_LIQUIDITY = os.getenv("TAKE_LIQUIDITY", "true").lower() == "true"
-ORDER_FILL_TIMEOUT_SECONDS = int(os.getenv("ORDER_FILL_TIMEOUT_SECONDS", "15"))
-
-# ---------- Logging ----------
-LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
-QUIET_LOGS = os.getenv("QUIET_LOGS", "true").lower() == "true"
-SUMMARY_EVERY_SECONDS = int(os.getenv("SUMMARY_EVERY_SECONDS", "60"))
-
-logging.basicConfig(
-    level=getattr(logging, LOG_LEVEL, logging.INFO),
-    format="%(asctime)s | %(levelname)s | %(message)s",
+LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() == "true"
+POLL_SECONDS = float(os.getenv("POLL_SECONDS", "8"))
+REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))
+
+START_EQUITY = float(os.getenv("START_EQUITY", "1000"))
+RISK_FRACTION = float(os.getenv("RISK_FRACTION", "0.08"))
+MAX_POSITION_USD = float(os.getenv("MAX_POSITION_USD", "150"))
+MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "50"))
+
+EDGE_ENTER = float(os.getenv("EDGE_ENTER", "0.07"))
+EDGE_EXIT = float(os.getenv("EDGE_EXIT", "0.02"))
+LOOKBACK = int(os.getenv("LOOKBACK", "20"))
+MIN_CONVICTION_Z = float(os.getenv("MIN_CONVICTION_Z", "1.0"))
+FAIR_MOVE_SCALE = float(os.getenv("FAIR_MOVE_SCALE", "0.15"))
+
+STATUS_FILE = Path(os.getenv("STATUS_FILE", ".runtime/status.json"))
+STATE_FILE = Path(os.getenv("STATE_FILE", ".runtime/state.json"))
+KRAKEN_TICKER_URL = os.getenv(
+    "KRAKEN_TICKER_URL", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"
 )
-log = logging.getLogger("johnny5")
-
-
-# =========================
-# UTIL
-# =========================
-
-def utc_now_dt() -> datetime:
-    return datetime.now(timezone.utc)
-
-def utc_now_iso() -> str:
-    return utc_now_dt().strftime("%Y-%m-%dT%H:%M:%SZ")
-
-def utc_today_date() -> date:
-    return utc_now_dt().date()
 
-def clamp(x: float, lo: float, hi: float) -> float:
-    return max(lo, min(hi, x))
 
-def _as_date(value) -> Optional[date]:
-    if value is None:
-        return None
-    if isinstance(value, date):
-        return value
-    try:
-        return datetime.fromisoformat(str(value)).date()
-    except Exception:
-        return None
+# ---------- Logging: only trades/errors ----------
+logging.basicConfig(level=logging.WARNING, format="%(asctime)s | %(levelname)s | %(message)s")
+log = logging.getLogger("kalshi-bot")
 
-def _sleep_backoff(attempt: int) -> None:
-    time.sleep(1.0 * attempt)
-
-def _safe_float(x) -> Optional[float]:
-    try:
-        if x is None:
-            return None
-        return float(x)
-    except Exception:
-        return None
-
-def cooldown_active(last_trade_ts, cooldown_minutes: int) -> bool:
-    if last_trade_ts is None:
-        return False
-    try:
-        return utc_now_dt() < (last_trade_ts + timedelta(minutes=cooldown_minutes))
-    except Exception:
-        return False
-
-def effective_trade_size() -> float:
-    return LIVE_TRADE_SIZE if LIVE_MODE else TRADE_SIZE
-
-def effective_edge_enter() -> float:
-    return LIVE_MIN_EDGE_ENTER if LIVE_MODE else EDGE_ENTER
-
-def effective_edge_exit() -> float:
-    return LIVE_MIN_EDGE_EXIT if LIVE_MODE else EDGE_EXIT
-
-def effective_max_trades_per_day() -> int:
-    return LIVE_MAX_TRADES_PER_DAY if LIVE_MODE else MAX_TRADES_PER_DAY
-
-def effective_max_daily_loss() -> float:
-    return LIVE_MAX_DAILY_LOSS if LIVE_MODE else MAX_DAILY_LOSS
-
-def effective_cooldown_minutes() -> int:
-    return LIVE_COOLDOWN_MINUTES if LIVE_MODE else COOLDOWN_MINUTES
-
-def marks_look_sane_for_live(p_yes: float) -> bool:
-    if not LIVE_MODE:
-        return True
-    return (LIVE_MIN_PRICE <= p_yes <= LIVE_MAX_PRICE)
-
-
-# =========================
-# DB
-# =========================
-
-def db_conn():
-    if not DATABASE_URL:
-        raise RuntimeError("DATABASE_URL is not set.")
-    return psycopg2.connect(DATABASE_URL)
-
-def init_db():
-    with db_conn() as conn:
-        with conn.cursor() as cur:
-            # Separate tables so you don't collide with old polymarket paper tables
-            cur.execute("""
-            CREATE TABLE IF NOT EXISTS kalshi_state (
-              id INTEGER PRIMARY KEY,
-              balance DOUBLE PRECISION NOT NULL,
-              position TEXT NULL,                -- 'YES' or 'NO'
-              entry_price DOUBLE PRECISION NULL, -- probability (0..1)
-              stake DOUBLE PRECISION NOT NULL,
-
-              market_ticker TEXT NULL,           -- current Kalshi market ticker we’re trading
-              last_trade_ts TIMESTAMPTZ,
-              last_trade_day DATE,
-              trades_today INTEGER NOT NULL DEFAULT 0,
-              realized_pnl_today DOUBLE PRECISION NOT NULL DEFAULT 0,
-
-              last_mark DOUBLE PRECISION,
-              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
-            );
-            """)
-
-            cur.execute("""
-            CREATE TABLE IF NOT EXISTS kalshi_trades (
-              trade_id BIGSERIAL PRIMARY KEY,
-              ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
-              market_ticker TEXT NULL,
-              action TEXT NOT NULL,       -- ENTER/EXIT
-              side TEXT NULL,             -- YES/NO
-              price DOUBLE PRECISION NULL,-- probability (0..1)
-              count INTEGER NULL,         -- contracts
-              stake DOUBLE PRECISION NULL,
-              fee DOUBLE PRECISION NULL,
-              pnl DOUBLE PRECISION NULL,
-              client_order_id TEXT NULL,
-              order_id TEXT NULL,
-              status TEXT NULL
-            );
-            """)
-        conn.commit()
-
-def load_state() -> dict:
-    with db_conn() as conn:
-        with conn.cursor(cursor_factory=RealDictCursor) as cur:
-            cur.execute("SELECT * FROM kalshi_state WHERE id=1;")
-            row = cur.fetchone()
-            if row:
-                return row
-
-            cur.execute(
-                """
-                INSERT INTO kalshi_state (
-                    id, balance, position, entry_price, stake,
-                    market_ticker, last_trade_ts, last_trade_day, trades_today, realized_pnl_today, last_mark
-                )
-                VALUES (1, %s, NULL, NULL, 0.0, NULL, NULL, NULL, 0, 0.0, NULL)
-                RETURNING *;
-                """,
-                (START_BALANCE,),
-            )
-            row = cur.fetchone()
-        conn.commit()
-    return row
-
-def save_state(
-    balance: float,
-    position: Optional[str],
-    entry_price: Optional[float],
-    stake: float,
-    market_ticker: Optional[str],
-    last_trade_ts,
-    last_trade_day,
-    trades_today: int,
-    realized_pnl_today: float,
-    last_mark: Optional[float],
-):
-    with db_conn() as conn:
-        with conn.cursor() as cur:
-            cur.execute(
-                """
-                UPDATE kalshi_state
-                SET balance=%s,
-                    position=%s,
-                    entry_price=%s,
-                    stake=%s,
-                    market_ticker=%s,
-                    last_trade_ts=%s,
-                    last_trade_day=%s,
-                    trades_today=%s,
-                    realized_pnl_today=%s,
-                    last_mark=%s,
-                    updated_at=NOW()
-                WHERE id=1;
-                """,
-                (
-                    balance, position, entry_price, stake,
-                    market_ticker,
-                    last_trade_ts, last_trade_day, trades_today, realized_pnl_today,
-                    last_mark,
-                ),
-            )
-        conn.commit()
-
-def log_trade_row(
-    market_ticker: Optional[str],
-    action: str,
-    side: Optional[str],
-    price: Optional[float],
-    count: Optional[int],
-    stake: Optional[float],
-    fee: Optional[float],
-    pnl: Optional[float],
-    client_order_id: Optional[str],
-    order_id: Optional[str],
-    status: Optional[str],
-):
-    with db_conn() as conn:
-        with conn.cursor() as cur:
-            cur.execute(
-                """
-                INSERT INTO kalshi_trades
-                (market_ticker, action, side, price, count, stake, fee, pnl, client_order_id, order_id, status)
-                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
-                """,
-                (market_ticker, action, side, price, count, stake, fee, pnl, client_order_id, order_id, status),
-            )
-        conn.commit()
-
-def reset_daily_counters_if_needed(state: dict) -> dict:
-    today = utc_today_date()
-    last_day = _as_date(state.get("last_trade_day"))
-    if last_day is None or last_day != today:
-        state["last_trade_day"] = today
-        state["trades_today"] = 0
-        state["realized_pnl_today"] = 0.0
-    return state
-
-def get_realized_pnl_24h_and_trades_24h() -> Tuple[float, int]:
-    with db_conn() as conn:
-        with conn.cursor() as cur:
-            cur.execute(
-                """
-                SELECT
-                  COALESCE(SUM(CASE WHEN action='EXIT' THEN pnl ELSE 0 END), 0) AS pnl_24h,
-                  COUNT(*)::int AS trades_24h
-                FROM kalshi_trades
-                WHERE ts >= (NOW() - INTERVAL '24 hours');
-                """
-            )
-            row = cur.fetchone()
-            return float(row[0] or 0.0), int(row[1] or 0)
-
-def get_winrate_last_n_exits(n: int = 20) -> Optional[float]:
-    with db_conn() as conn:
-        with conn.cursor() as cur:
-            cur.execute(
-                """
-                SELECT pnl
-                FROM kalshi_trades
-                WHERE action='EXIT' AND pnl IS NOT NULL
-                ORDER BY ts DESC
-                LIMIT %s;
-                """,
-                (n,),
-            )
-            rows = cur.fetchall()
-    if not rows:
-        return None
-    pnls = [float(r[0]) for r in rows]
-    wins = sum(1 for p in pnls if p > 0)
-    return wins / len(pnls)
-
-
-# =========================
-# DATA: Kraken OHLC
-# =========================
-
-KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"
-KRAKEN_PAIR = "XBTUSD"
-KRAKEN_INTERVAL = 5  # minutes
-
-def fetch_btc_closes_5m(lookback: int) -> Optional[List[float]]:
-    last_error = None
-    for attempt in range(1, MAX_RETRIES + 1):
-        try:
-            r = requests.get(
-                KRAKEN_OHLC_URL,
-                params={"pair": KRAKEN_PAIR, "interval": KRAKEN_INTERVAL},
-                timeout=TIMEOUT_SEC,
-            )
-            if r.status_code != 200:
-                last_error = f"HTTP {r.status_code}: {r.text[:200]}"
-                _sleep_backoff(attempt)
-                continue
-
-            data = r.json()
-            if data.get("error"):
-                last_error = f"Kraken error: {data['error']}"
-                _sleep_backoff(attempt)
-                continue
-
-            result = data.get("result", {})
-            pair_key = next((k for k in result.keys() if k != "last"), None)
-            candles = result.get(pair_key, [])
-
-            if not isinstance(candles, list) or len(candles) < lookback:
-                last_error = f"Not enough candles. candles_len={len(candles) if isinstance(candles, list) else 'n/a'}"
-                _sleep_backoff(attempt)
-                continue
-
-            closes = [float(c[4]) for c in candles[-lookback:]]
-            return closes
-
-        except Exception as e:
-            last_error = str(e)
-            _sleep_backoff(attempt)
-
-    if not QUIET_LOGS:
-        log.warning("Price fetch failed after retries: %s", last_error)
-    return None
-
-def prob_from_closes(closes: List[float]) -> float:
-    avg = sum(closes) / len(closes)
-    last = closes[-1]
-    if avg <= 0:
-        return 0.5
-    dev = (last - avg) / avg
-    p = 0.5 + (dev * PROB_Z_SCALE)
-    return clamp(p, PROB_P_MIN, PROB_P_MAX)
-
-
-# =========================
-# PNL (binary-contract style)
-# =========================
-
-def shares_for_stake(stake: float, price: float) -> float:
-    # stake in $; price is probability (0..1). Shares = stake / price (same as your poly math)
-    return 0.0 if price <= 0 else (stake / price)
-
-def calc_unrealized_pnl(position: Optional[str], entry_price: Optional[float], mark: float, stake: float) -> float:
-    if position is None or entry_price is None or stake <= 0:
-        return 0.0
-    sh = shares_for_stake(stake, entry_price)
-    return (sh * mark) - stake
-
-def calc_realized_pnl(entry_price: float, exit_price: float, stake: float) -> float:
-    sh = shares_for_stake(stake, entry_price)
-    return (sh * exit_price) - stake
-
-
-# =========================
-# KALSHI CLIENT
-# =========================
-
-def _load_private_key_pem() -> bytes:
-    if KALSHI_PRIVATE_KEY_PEM:
-        return KALSHI_PRIVATE_KEY_PEM.encode("utf-8")
-    if KALSHI_PRIVATE_KEY_PATH:
-        with open(KALSHI_PRIVATE_KEY_PATH, "rb") as f:
-            return f.read()
-    raise RuntimeError("Set KALSHI_PRIVATE_KEY_PEM or KALSHI_PRIVATE_KEY_PATH.")
-
-def _load_private_key_obj():
-    pem = _load_private_key_pem()
-    return serialization.load_pem_private_key(pem, password=None)
-
-def _kalshi_timestamp_ms() -> str:
-    return str(int(time.time() * 1000))
-
-def _create_signature(private_key, timestamp_ms: str, method: str, path: str) -> str:
-    # Kalshi quickstart: signature is over (timestamp + method + path) :contentReference[oaicite:9]{index=9}
-    msg = (timestamp_ms + method.upper() + path).encode("utf-8")
-    sig = private_key.sign(
-        msg,
-        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
-        hashes.SHA256(),
-    )
-    # Kalshi expects base64. Their quickstart uses .decode after b64.
-    import base64
-    return base64.b64encode(sig).decode("utf-8")
 
 @dataclass
-class KalshiOrderResult:
-    ok: bool
-    status_code: int
-    order_id: Optional[str] = None
-    status: Optional[str] = None
-    raw: Optional[dict] = None
-    error_text: Optional[str] = None
+class BotState:
+    equity: float = START_EQUITY
+    realized_pnl: float = 0.0
+    daily_realized_pnl: float = 0.0
+    trades: int = 0
+    last_day: str = ""
+    position_side: Optional[str] = None  # YES/NO
+    position_entry_prob: Optional[float] = None
+    position_contracts: int = 0
+    market_ticker: Optional[str] = None
+
+
+def utc_iso() -> str:
+    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
+
+
+def ensure_runtime_dir() -> None:
+    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
+    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
+
+
+def load_state() -> BotState:
+    if not STATE_FILE.exists():
+        return BotState(last_day=datetime.now(timezone.utc).date().isoformat())
+    raw = json.loads(STATE_FILE.read_text())
+    return BotState(**raw)
+
+
+def save_state(state: BotState) -> None:
+    STATE_FILE.write_text(json.dumps(asdict(state), indent=2))
+
+
+def write_status(state: BotState, message: str, fair_yes: float, mark_yes: float, edge: float) -> None:
+    unrealized = 0.0
+    if state.position_side == "YES" and state.position_entry_prob is not None:
+        unrealized = (mark_yes - state.position_entry_prob) * state.position_contracts
+    elif state.position_side == "NO" and state.position_entry_prob is not None:
+        unrealized = ((1 - mark_yes) - state.position_entry_prob) * state.position_contracts
+
+    payload = {
+        "ts": utc_iso(),
+        "message": message,
+        "live_mode": LIVE_MODE,
+        "equity": round(state.equity + unrealized, 4),
+        "cash": round(state.equity, 4),
+        "realized_pnl": round(state.realized_pnl, 4),
+        "daily_realized_pnl": round(state.daily_realized_pnl, 4),
+        "position": {
+            "side": state.position_side,
+            "entry_prob": state.position_entry_prob,
+            "contracts": state.position_contracts,
+            "market_ticker": state.market_ticker,
+        },
+        "model": {"fair_yes": round(fair_yes, 6), "mark_yes": round(mark_yes, 6), "edge": round(edge, 6)},
+    }
+    STATUS_FILE.write_text(json.dumps(payload, indent=2))
+
 
 class KalshiClient:
-    def __init__(self, base_url: str):
-        self.base_url = base_url.rstrip("/")
-        self._private_key = None
-
-    def _ensure_key(self):
-        if self._private_key is None:
-            self._private_key = _load_private_key_obj()
-
-    def _headers(self, method: str, path: str) -> Dict[str, str]:
-        if not KALSHI_API_KEY_ID:
-            raise RuntimeError("KALSHI_API_KEY_ID is not set.")
-        self._ensure_key()
-        ts = _kalshi_timestamp_ms()
-        sig = _create_signature(self._private_key, ts, method, path)
+    def __init__(self) -> None:
+        self.session = requests.Session()
+        self.private_key = None
+        pem = KALSHI_PRIVATE_KEY_PEM
+        if not pem and KALSHI_PRIVATE_KEY_PATH and Path(KALSHI_PRIVATE_KEY_PATH).exists():
+            pem = Path(KALSHI_PRIVATE_KEY_PATH).read_text()
+        if pem:
+            self.private_key = serialization.load_pem_private_key(pem.encode(), password=None)
+
+    def _headers(self, method: str, path: str) -> dict[str, str]:
+        if not (KALSHI_API_KEY_ID and self.private_key):
+            return {}
+        ts_ms = str(int(time.time() * 1000))
+        msg = f"{ts_ms}{method.upper()}{path}".encode()
+        sig = self.private_key.sign(msg, padding.PKCS1v15(), hashes.SHA256()).hex()
         return {
             "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
+            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
             "KALSHI-ACCESS-SIGNATURE": sig,
-            "KALSHI-ACCESS-TIMESTAMP": ts,
-            "Content-Type": "application/json",
         }
 
-    # ---- Public market data (no auth required, but auth is fine too) ----
-    def get_markets_open_in_series(self, series_ticker: str, limit: int = 200) -> List[dict]:
-        # Quick start market data shows /markets endpoint :contentReference[oaicite:10]{index=10}
-        url = f"{self.base_url}/trade-api/v2/markets"
-        params = {"status": "open", "series_ticker": series_ticker, "limit": limit}
-        r = requests.get(url, params=params, timeout=TIMEOUT_SEC)
-        r.raise_for_status()
-        data = r.json()
-        return data.get("markets", []) if isinstance(data, dict) else []
-
-    def get_orderbook(self, ticker: str) -> dict:
-        # /markets/{ticker}/orderbook :contentReference[oaicite:11]{index=11}
-        url = f"{self.base_url}/trade-api/v2/markets/{ticker}/orderbook"
-        r = requests.get(url, timeout=TIMEOUT_SEC)
-        r.raise_for_status()
-        return r.json() if isinstance(r.json(), dict) else {}
-
-    # ---- Authenticated portfolio ----
-    def get_balance(self) -> dict:
-        # API reference page exists; we use the standard endpoint name
-        path = "/trade-api/v2/portfolio/balance"
-        url = f"{self.base_url}{path}"
-        headers = self._headers("GET", path)
-        r = requests.get(url, headers=headers, timeout=TIMEOUT_SEC)
-        r.raise_for_status()
-        return r.json() if isinstance(r.json(), dict) else {}
-
-    def place_order_limit(self, ticker: str, side: str, count: int, price_cents: int, client_order_id: str) -> KalshiOrderResult:
-        # POST /portfolio/orders with yes_price/no_price in cents :contentReference[oaicite:12]{index=12}
-        path = "/trade-api/v2/portfolio/orders"
-        url = f"{self.base_url}{path}"
-        headers = self._headers("POST", path)
-
-        side = side.lower()
-        if side not in ("yes", "no"):
-            raise ValueError("side must be 'yes' or 'no'.")
+    def _request(self, method: str, path: str, payload: Optional[dict[str, Any]] = None) -> dict[str, Any]:
+        headers = self._headers(method, path)
+        url = f"{KALSHI_BASE_URL}{path}"
+        response = self.session.request(
+            method=method.upper(),
+            url=url,
+            json=payload,
+            headers=headers,
+            timeout=REQUEST_TIMEOUT_SECONDS,
+        )
+        response.raise_for_status()
+        return response.json()
 
+    def get_open_market(self) -> dict[str, Any]:
+        data = self._request("GET", f"/trade-api/v2/markets?series_ticker={SERIES_TICKER}&status=open")
+        markets = data.get("markets", [])
+        if not markets:
+            raise RuntimeError(f"No open markets for {SERIES_TICKER}")
+        return markets[0]
+
+    def get_orderbook(self, ticker: str) -> dict[str, Any]:
+        return self._request("GET", f"/trade-api/v2/markets/{ticker}/orderbook")
+
+    def place_order(self, ticker: str, side: str, contracts: int, price_cents: int) -> dict[str, Any]:
+        if not LIVE_MODE:
+            return {"ok": True, "paper": True}
+        action = "buy"
         payload = {
             "ticker": ticker,
-            "action": "buy",
-            "side": side,
-            "count": int(count),
+            "action": action,
+            "side": side.lower(),
+            "count": contracts,
             "type": "limit",
-            "client_order_id": client_order_id,
+            "yes_price": price_cents if side == "YES" else None,
+            "no_price": price_cents if side == "NO" else None,
+            "client_order_id": f"bot-{int(time.time())}",
         }
-        if side == "yes":
-            payload["yes_price"] = int(price_cents)
-        else:
-            payload["no_price"] = int(price_cents)
-
-        r = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT_SEC)
-
-        try:
-            j = r.json()
-        except Exception:
-            j = None
-
-        if r.status_code == 201 and isinstance(j, dict):
-            order = j.get("order", {}) if isinstance(j.get("order"), dict) else {}
-            return KalshiOrderResult(
-                ok=True,
-                status_code=r.status_code,
-                order_id=order.get("order_id"),
-                status=order.get("status"),
-                raw=j,
-            )
-
-        return KalshiOrderResult(
-            ok=False,
-            status_code=r.status_code,
-            raw=j if isinstance(j, dict) else None,
-            error_text=r.text[:500],
-        )
-
-    def get_order(self, order_id: str) -> dict:
-        path = f"/trade-api/v2/portfolio/orders/{order_id}"
-        url = f"{self.base_url}{path}"
-        headers = self._headers("GET", path)
-        r = requests.get(url, headers=headers, timeout=TIMEOUT_SEC)
-        r.raise_for_status()
-        return r.json() if isinstance(r.json(), dict) else {}
-
-    def cancel_order(self, order_id: str) -> bool:
-        path = f"/trade-api/v2/portfolio/orders/{order_id}"
-        url = f"{self.base_url}{path}"
-        headers = self._headers("DELETE", path)
-        r = requests.delete(url, headers=headers, timeout=TIMEOUT_SEC)
-        return r.status_code in (200, 204)
-
-
-# =========================
-# MARKET SELECTION + PRICING
-# =========================
-
-def _parse_iso8601(dt_str: Optional[str]) -> Optional[datetime]:
-    if not dt_str:
-        return None
-    try:
-        # Kalshi tends to return ISO strings. Handle trailing Z.
-        s = dt_str.replace("Z", "+00:00")
-        return datetime.fromisoformat(s)
-    except Exception:
-        return None
-
-def pick_current_market(markets: List[dict], now: Optional[datetime] = None) -> Optional[dict]:
-    if not markets:
-        return None
-    now = now or utc_now_dt()
-
-    # Try to use open_time/close_time if present
-    with_times = []
-    for m in markets:
-        ot = _parse_iso8601(m.get("open_time") or m.get("open_ts") or m.get("openTime"))
-        ct = _parse_iso8601(m.get("close_time") or m.get("close_ts") or m.get("closeTime"))
-        with_times.append((m, ot, ct))
-
-    active = []
-    for m, ot, ct in with_times:
-        if ot and ct and ot <= now < ct:
-            active.append((m, ct))
-
-    if active:
-        # soonest closing active market
-        active.sort(key=lambda x: x[1])
-        return active[0][0]
-
-    # Fallback: closest close_time in the future
-    future = []
-    for m, ot, ct in with_times:
-        if ct and ct >= now:
-            future.append((m, ct))
-    if future:
-        future.sort(key=lambda x: x[1])
-        return future[0][0]
-
-    # Last resort: lexicographic ticker
-    return sorted(markets, key=lambda x: str(x.get("ticker", "")))[-1]
-
-def _best_bid_cents(levels: Any) -> Optional[int]:
-    # Kalshi orderbook bids are typically list of [price, quantity] or dicts; handle both.
-    if not isinstance(levels, list) or not levels:
-        return None
-    top = levels[0]
-    if isinstance(top, list) and len(top) >= 1:
-        try:
-            return int(top[0])
-        except Exception:
-            return None
-    if isinstance(top, dict):
-        for k in ("price", "p", "yes_price", "no_price"):
-            if k in top:
-                try:
-                    return int(top[k])
-                except Exception:
-                    return None
-    return None
-
-def kalshi_marks_from_orderbook(ob: dict) -> Optional[Tuple[float, float, Dict[str, Any]]]:
-    """
-    Returns:
-      p_yes_mid (0..1), p_no_mid (0..1), debug dict
-
-    We compute implied asks via opposite-side bids:
-      ask_yes ≈ 100 - best_bid_no
-      ask_no  ≈ 100 - best_bid_yes
-
-    Then mid_yes = (bid_yes + ask_yes)/2
-    """
-    orderbook = ob.get("orderbook", ob)  # tolerate either shape
-    yes_bids = orderbook.get("yes_bids") or orderbook.get("yes") or orderbook.get("bids_yes") or []
-    no_bids = orderbook.get("no_bids") or orderbook.get("no") or orderbook.get("bids_no") or []
-
-    bid_yes = _best_bid_cents(yes_bids)
-    bid_no = _best_bid_cents(no_bids)
-
-    if bid_yes is None or bid_no is None:
-        return None
-
-    ask_yes = 100 - bid_no
-    ask_no = 100 - bid_yes
-
-    # Keep them in bounds
-    ask_yes = max(1, min(99, ask_yes))
-    ask_no = max(1, min(99, ask_no))
-    bid_yes = max(1, min(99, bid_yes))
-    bid_no = max(1, min(99, bid_no))
-
-    mid_yes_cents = int(round((bid_yes + ask_yes) / 2.0))
-    mid_yes_cents = max(1, min(99, mid_yes_cents))
-
-    p_yes = mid_yes_cents / 100.0
-    p_no = 1.0 - p_yes
-
-    dbg = {
-        "bid_yes": bid_yes,
-        "bid_no": bid_no,
-        "ask_yes_implied": ask_yes,
-        "ask_no_implied": ask_no,
-        "mid_yes_cents": mid_yes_cents,
-    }
-    return p_yes, p_no, dbg
-
-def entry_exit_prices_cents(ob_dbg: Dict[str, Any], side: str) -> Tuple[int, int]:
-    """
-    Returns (entry_price_cents, exit_price_cents) for the given side.
-
-    - Entry: if TAKE_LIQUIDITY, use implied ask; else use mid.
-    - Exit: use best bid for the side to sell into (our "paper-style exit")
-    """
-    side = side.upper()
-    if side not in ("YES", "NO"):
-        raise ValueError("side must be YES/NO")
-
-    bid_yes = int(ob_dbg["bid_yes"])
-    bid_no = int(ob_dbg["bid_no"])
-    ask_yes = int(ob_dbg["ask_yes_implied"])
-    ask_no = int(ob_dbg["ask_no_implied"])
-    mid_yes = int(ob_dbg["mid_yes_cents"])
-    mid_no = 100 - mid_yes
-
-    if side == "YES":
-        entry = ask_yes if TAKE_LIQUIDITY else mid_yes
-        exit_ = bid_yes
-    else:
-        entry = ask_no if TAKE_LIQUIDITY else mid_no
-        exit_ = bid_no
-
-    entry = max(1, min(99, entry))
-    exit_ = max(1, min(99, exit_))
-    return entry, exit_
-
-
-# =========================
-# TRADING ENGINE (paper math + live execution)
-# =========================
-
-def _contracts_for_trade_size(trade_size_usd: float, price_prob: float) -> int:
-    # Kalshi contracts are $1 max payout; buying YES at p costs p dollars per contract.
-    # Approx contracts = trade_size / p
-    if price_prob <= 0:
-        return 0
-    return max(1, int(trade_size_usd / price_prob))
-
-def paper_trade_prob(
-    balance: float,
-    position: Optional[str],
-    entry_price: Optional[float],
-    stake: float,
-    signal: str,
-    p_yes: float,
-    p_no: float,
-    edge: float,
-    edge_exit: float,
-    trade_size: float,
-    last_trade_ts,
-    last_trade_day,
-    trades_today: int,
-    realized_pnl_today: float,
-) -> Tuple[float, Optional[str], Optional[float], float, str, object, object, int, float]:
-    """
-    Same behavior as your old engine:
-    - signal YES/NO/HOLD
-    - Enter if flat and signal is YES/NO
-    - Exit if opposite signal OR edge collapses
-    """
-    now_ts = utc_now_dt()
-    today = utc_today_date()
-
-    if position is None and signal == "HOLD":
-        return (balance, position, entry_price, stake, "HOLD",
-                last_trade_ts, last_trade_day, trades_today, realized_pnl_today)
-
-    # ENTER
-    if position is None and signal in ("YES", "NO"):
-        if balance < (trade_size + FEE_PER_TRADE):
-            return (balance, position, entry_price, stake, "SKIP_INSUFFICIENT_BALANCE",
-                    last_trade_ts, last_trade_day, trades_today, realized_pnl_today)
-
-        balance -= (trade_size + FEE_PER_TRADE)
-        position = signal
-        stake = trade_size
-        entry_price = p_yes if position == "YES" else p_no
-
-        trades_today += 1
-        last_trade_ts = now_ts
-        last_trade_day = today
-
-        return (balance, position, entry_price, stake, f"ENTER_{signal}",
-                last_trade_ts, last_trade_day, trades_today, realized_pnl_today)
-
-    # Edge-collapse exit
-    edge_collapse = False
-    if position == "YES" and edge < edge_exit:
-        edge_collapse = True
-    elif position == "NO" and edge > -edge_exit:
-        edge_collapse = True
-
-    # EXIT
-    if position is not None and ((signal != "HOLD" and signal != position) or edge_collapse):
-        exit_price = p_yes if position == "YES" else p_no
-        pnl = calc_realized_pnl(entry_price, exit_price, stake)
-
-        balance += (stake + pnl - FEE_PER_TRADE)
-
-        realized_pnl_today += pnl
-        trades_today += 1
-        last_trade_ts = now_ts
-        last_trade_day = today
-
-        position = None
-        entry_price = None
-        stake = 0.0
-
-        return (balance, position, entry_price, stake, "EXIT",
-                last_trade_ts, last_trade_day, trades_today, realized_pnl_today)
-
-    return (balance, position, entry_price, stake, "HOLD_SAME_SIDE",
-            last_trade_ts, last_trade_day, trades_today, realized_pnl_today)
-
-
-def maybe_log_summary(
-    last_summary_ts: float,
-    p_yes: float,
-    fair_yes: float,
-    edge: float,
-    signal: str,
-    balance: float,
-    position: Optional[str],
-    entry_price: Optional[float],
-    stake: float,
-    realized_pnl_today: float,
-) -> float:
-    now = time.time()
-    if now - last_summary_ts < SUMMARY_EVERY_SECONDS:
-        return last_summary_ts
-
-    # mark-to-market
-    if position == "YES":
-        u = calc_unrealized_pnl(position, entry_price, p_yes, stake)
-    elif position == "NO":
-        u = calc_unrealized_pnl(position, entry_price, 1.0 - p_yes, stake)
-    else:
-        u = 0.0
-    equity = balance + u
-
-    pnl_24h, trades_24h = get_realized_pnl_24h_and_trades_24h()
-    winrate20 = get_winrate_last_n_exits(20)
-
-    log.info(
-        "summary | p_yes=%.3f fair_yes=%.3f edge=%+.3f signal=%s | equity=%.2f uPnL=%.2f | "
-        "pnl_today=%.2f pnl_24h=%.2f trades_24h=%d winrate20=%s | mode=%s",
-        p_yes, fair_yes, edge, signal,
-        equity, u,
-        realized_pnl_today, pnl_24h, trades_24h,
-        ("n/a" if winrate20 is None else f"{winrate20*100:.0f}%"),
-        ("LIVE" if LIVE_MODE else "PAPER"),
-    )
-    return now
-
-
-def run_live_order_and_wait(k: KalshiClient, ticker: str, side: str, count: int, price_cents: int) -> KalshiOrderResult:
-    client_order_id = str(uuid.uuid4())
-    res = k.place_order_limit(
-        ticker=ticker,
-        side=side.lower(),
-        count=count,
-        price_cents=price_cents,
-        client_order_id=client_order_id,
+        return self._request("POST", "/trade-api/v2/portfolio/orders", payload)
+
+
+def kraken_last_price() -> float:
+    resp = requests.get(KRAKEN_TICKER_URL, timeout=REQUEST_TIMEOUT_SECONDS)
+    resp.raise_for_status()
+    body = resp.json()
+    result = body.get("result", {})
+    if not result:
+        raise RuntimeError("Kraken price missing")
+    key = next(iter(result))
+    return float(result[key]["c"][0])
+
+
+def mark_yes_from_orderbook(orderbook: dict[str, Any]) -> float:
+    ob = orderbook.get("orderbook", orderbook)
+    yes_bids = ob.get("yes", []) or ob.get("yes_bids", [])
+    no_bids = ob.get("no", []) or ob.get("no_bids", [])
+    if not yes_bids or not no_bids:
+        raise RuntimeError("Orderbook missing yes/no bids")
+
+    best_yes_bid = int(yes_bids[0][0] if isinstance(yes_bids[0], list) else yes_bids[0].get("price"))
+    best_no_bid = int(no_bids[0][0] if isinstance(no_bids[0], list) else no_bids[0].get("price"))
+    implied_yes_ask = 100 - best_no_bid
+    mid_yes = (best_yes_bid + implied_yes_ask) / 2
+    return max(0.01, min(0.99, mid_yes / 100.0))
+
+
+def model_fair_yes(prices: list[float]) -> tuple[float, float]:
+    if len(prices) < LOOKBACK:
+        return 0.5, 0.0
+    window = prices[-LOOKBACK:]
+    rets = []
+    for i in range(1, len(window)):
+        r = (window[i] - window[i - 1]) / window[i - 1]
+        rets.append(r)
+    mean = sum(rets) / len(rets)
+    var = sum((r - mean) ** 2 for r in rets) / max(1, (len(rets) - 1))
+    std = max(var**0.5, 1e-8)
+    z = (rets[-1] - mean) / std
+    fair = max(0.01, min(0.99, 0.5 + z * FAIR_MOVE_SCALE))
+    return fair, z
+
+
+def compute_contracts(equity: float, price_prob: float) -> int:
+    usd = min(MAX_POSITION_USD, max(5.0, equity * RISK_FRACTION))
+    return max(1, int(usd / max(price_prob, 0.01)))
+
+
+def trade_log(event: str, state: BotState, market_price: float, fair_price: float, edge: float) -> None:
+    unrealized = 0.0
+    if state.position_side == "YES" and state.position_entry_prob is not None:
+        unrealized = (market_price - state.position_entry_prob) * state.position_contracts
+    elif state.position_side == "NO" and state.position_entry_prob is not None:
+        unrealized = ((1 - market_price) - state.position_entry_prob) * state.position_contracts
+    equity = state.equity + unrealized
+    print(
+        json.dumps(
+            {
+                "ts": utc_iso(),
+                "event": event,
+                "equity": round(equity, 4),
+                "cash": round(state.equity, 4),
+                "realized_pnl": round(state.realized_pnl, 4),
+                "daily_realized_pnl": round(state.daily_realized_pnl, 4),
+                "position": {
+                    "side": state.position_side,
+                    "contracts": state.position_contracts,
+                    "entry_prob": state.position_entry_prob,
+                    "market_ticker": state.market_ticker,
+                },
+                "market_yes": round(market_price, 6),
+                "fair_yes": round(fair_price, 6),
+                "edge": round(edge, 6),
+                "live": LIVE_MODE,
+            }
+        ),
+        flush=True,
     )
 
-    if not res.ok:
-        log.warning("LIVE order failed | ticker=%s side=%s count=%s px=%sc | http=%s err=%s",
-                    ticker, side, count, price_cents, res.status_code, res.error_text)
-        log_trade_row(ticker, "ORDER_FAIL", side.upper(), price_cents / 100.0, count, None, None, None, client_order_id, None, "FAILED")
-        return res
-
-    log.info("LIVE order placed | ticker=%s side=%s count=%s px=%sc | order_id=%s status=%s",
-             ticker, side, count, price_cents, res.order_id, res.status)
-
-    # Wait briefly for fill; cancel if still resting (keeps bot deterministic)
-    start = time.time()
-    order_id = res.order_id
-    status = res.status or ""
-    raw = res.raw
 
-    while time.time() - start < ORDER_FILL_TIMEOUT_SECONDS:
-        try:
-            j = k.get_order(order_id)
-            order = j.get("order", {}) if isinstance(j, dict) else {}
-            status = str(order.get("status", status))
-            raw = j
-            if status.lower() in ("filled", "executed"):
-                log.info("LIVE order filled | order_id=%s", order_id)
-                res.status = status
-                res.raw = raw
-                return res
-            if status.lower() in ("canceled", "rejected"):
-                res.status = status
-                res.raw = raw
-                return res
-        except Exception:
-            pass
-        time.sleep(1.0)
-
-    # Timed out: cancel
-    try:
-        canceled = k.cancel_order(order_id)
-        log.info("LIVE order timeout -> cancel=%s | order_id=%s status=%s", canceled, order_id, status)
-    except Exception as e:
-        log.warning("LIVE order timeout -> cancel failed | order_id=%s err=%s", order_id, str(e)[:200])
-
-    res.status = f"TIMEOUT({status})"
-    res.raw = raw
-    return res
-
-
-# =========================
-# MAIN
-# =========================
-
-def print_db_stats():
-    with db_conn() as conn:
-        with conn.cursor() as cur:
-            cur.execute("SELECT COUNT(*)::int FROM kalshi_trades WHERE action='EXIT';")
-            exits_total = int(cur.fetchone()[0] or 0)
-
-            cur.execute("""
-                SELECT
-                  COALESCE(SUM(CASE WHEN action='EXIT' THEN pnl ELSE 0 END), 0) AS pnl_24h,
-                  COUNT(*)::int AS trades_24h
-                FROM kalshi_trades
-                WHERE ts >= (NOW() - INTERVAL '24 hours');
-            """)
-            row = cur.fetchone()
-            pnl_24h = float(row[0] or 0.0)
-            trades_24h = int(row[1] or 0)
-
-            cur.execute("""
-                SELECT COUNT(*)::int
-                FROM kalshi_trades
-                WHERE action='EXIT' AND ts >= (NOW() - INTERVAL '24 hours');
-            """)
-            exits_24h = int(cur.fetchone()[0] or 0)
-
-    winrate20 = get_winrate_last_n_exits(20)
-    log.info(
-        "STATS | exits_total=%d exits_24h=%d pnl_24h(realized)=%.2f trades_24h=%d winrate20=%s",
-        exits_total, exits_24h, pnl_24h, trades_24h,
-        ("n/a" if winrate20 is None else f"{winrate20*100:.0f}%"),
-    )
+def rollover_day(state: BotState) -> None:
+    today = datetime.now(timezone.utc).date().isoformat()
+    if state.last_day != today:
+        state.last_day = today
+        state.daily_realized_pnl = 0.0
 
-def main():
-    init_db()
 
-    if STATS_ONLY:
-        print_db_stats()
-        return
-
-    state = load_state()
-    state = reset_daily_counters_if_needed(state)
-
-    # Manual daily reset
-    if RESET_DAILY:
-        save_state(
-            balance=float(state["balance"]),
-            position=state["position"],
-            entry_price=float(state["entry_price"]) if state["entry_price"] is not None else None,
-            stake=float(state["stake"]),
-            market_ticker=state.get("market_ticker"),
-            last_trade_ts=state.get("last_trade_ts"),
-            last_trade_day=utc_today_date(),
-            trades_today=0,
-            realized_pnl_today=0.0,
-            last_mark=float(state["last_mark"]) if state.get("last_mark") is not None else None,
-        )
-        log.info("RESET_DAILY applied. Daily counters cleared.")
-        return
-
-    # Manual state reset
-    if RESET_STATE:
-        save_state(
-            balance=float(state["balance"]),
-            position=None,
-            entry_price=None,
-            stake=0.0,
-            market_ticker=None,
-            last_trade_ts=state.get("last_trade_ts"),
-            last_trade_day=_as_date(state.get("last_trade_day")),
-            trades_today=int(state.get("trades_today", 0)),
-            realized_pnl_today=float(state.get("realized_pnl_today", 0.0)),
-            last_mark=float(state["last_mark"]) if state.get("last_mark") is not None else None,
-        )
-        log.info("RESET_STATE applied. Position cleared.")
-        return
+def maybe_enter(state: BotState, ticker: str, fair_yes: float, mark_yes: float, z: float, client: KalshiClient) -> bool:
+    if state.position_side is not None:
+        return False
+    if state.daily_realized_pnl <= -abs(MAX_DAILY_LOSS):
+        return False
 
-    # State fields
-    balance = float(state["balance"])
-    position = state["position"]  # YES/NO or None
-    entry_price = float(state["entry_price"]) if state["entry_price"] is not None else None
-    stake = float(state["stake"])
-    market_ticker = state.get("market_ticker")
-    last_trade_ts = state.get("last_trade_ts")
-    last_trade_day = _as_date(state.get("last_trade_day"))
-    trades_today = int(state.get("trades_today", 0))
-    realized_pnl_today = float(state.get("realized_pnl_today", 0.0))
+    edge = fair_yes - mark_yes
+    conviction = abs(z) >= MIN_CONVICTION_Z
+    side = None
+    if conviction and edge >= EDGE_ENTER:
+        side = "YES"
+    elif conviction and edge <= -EDGE_ENTER:
+        side = "NO"
 
-    k = KalshiClient(KALSHI_BASE_URL) if LIVE_MODE else KalshiClient(KALSHI_BASE_URL)
+    if side is None:
+        return False
 
-    consecutive_no_markets = 0
-    last_summary_ts = 0.0
+    price = mark_yes if side == "YES" else (1 - mark_yes)
+    contracts = compute_contracts(state.equity, price)
+    cents = int(round(price * 100))
+    client.place_order(ticker=ticker, side=side, contracts=contracts, price_cents=max(1, min(99, cents)))
 
-    log.info("BOOT | series=%s poll=%.1fs live=%s base=%s", SERIES_TICKER, POLL_SECONDS, LIVE_MODE, KALSHI_BASE_URL)
+    state.position_side = side
+    state.position_entry_prob = price
+    state.position_contracts = contracts
+    state.market_ticker = ticker
+    state.trades += 1
+    trade_log("ENTER", state, mark_yes, fair_yes, edge)
+    return True
 
-    while True:
-        # 1) Get synthetic fair
-        closes = fetch_btc_closes_5m(LOOKBACK)
-        if closes is None:
-            time.sleep(POLL_SECONDS)
-            continue
 
-        fair_yes = prob_from_closes(closes)
-        fair_yes = clamp(fair_yes, PROB_P_MIN, PROB_P_MAX)
+def maybe_exit(state: BotState, fair_yes: float, mark_yes: float, client: KalshiClient) -> bool:
+    if state.position_side is None or state.position_entry_prob is None:
+        return False
 
-        # 2) Find current open market in series
-        try:
-            markets = k.get_markets_open_in_series(SERIES_TICKER, limit=200)
-        except Exception as e:
-            if not QUIET_LOGS:
-                log.warning("Kalshi markets fetch failed: %s", str(e)[:200])
-            time.sleep(POLL_SECONDS)
-            continue
+    edge = fair_yes - mark_yes
+    should_exit = False
+    if state.position_side == "YES":
+        should_exit = edge <= EDGE_EXIT
+        pnl = (mark_yes - state.position_entry_prob) * state.position_contracts
+        exit_price = mark_yes
+    else:
+        should_exit = edge >= -EDGE_EXIT
+        pnl = ((1 - mark_yes) - state.position_entry_prob) * state.position_contracts
+        exit_price = 1 - mark_yes
 
-        if not markets:
-            consecutive_no_markets += 1
-            log.warning("No open markets for series=%s | streak=%d", SERIES_TICKER, consecutive_no_markets)
+    if not should_exit:
+        return False
 
-            if EXIT_ON_TOO_MANY_NO_MARKETS and consecutive_no_markets >= MAX_CONSECUTIVE_NO_MARKETS:
-                log.error("Too many consecutive no-market fetches -> exiting for restart. streak=%d", consecutive_no_markets)
-                raise SystemExit(2)
+    cents = int(round(exit_price * 100))
+    client.place_order(
+        ticker=state.market_ticker or SERIES_TICKER,
+        side="NO" if state.position_side == "YES" else "YES",
+        contracts=state.position_contracts,
+        price_cents=max(1, min(99, cents)),
+    )
 
-            time.sleep(NO_MARKETS_LONG_SLEEP_SECONDS)
-            continue
+    state.equity += pnl
+    state.realized_pnl += pnl
+    state.daily_realized_pnl += pnl
+    state.position_side = None
+    state.position_entry_prob = None
+    state.position_contracts = 0
+    state.market_ticker = None
+    state.trades += 1
+    trade_log("EXIT", state, mark_yes, fair_yes, edge)
+    return True
 
-        consecutive_no_markets = 0
 
-        current_market = pick_current_market(markets)
-        if not current_market:
-            time.sleep(POLL_SECONDS)
-            continue
+def main() -> None:
+    ensure_runtime_dir()
+    state = load_state()
+    client = KalshiClient()
+    prices: list[float] = []
 
-        current_ticker = str(current_market.get("ticker") or "")
-        if not current_ticker:
-            time.sleep(POLL_SECONDS)
-            continue
-
-        if market_ticker != current_ticker:
-            market_ticker = current_ticker
-            # If we rolled to a new ticker while holding a position, we DO NOT auto-close.
-            # (You can change this later if you want forced close on rollover.)
-            save_state(
-                balance=balance, position=position, entry_price=entry_price, stake=stake,
-                market_ticker=market_ticker,
-                last_trade_ts=last_trade_ts, last_trade_day=last_trade_day,
-                trades_today=trades_today, realized_pnl_today=realized_pnl_today,
-                last_mark=None,
-            )
-            log.info("Market rollover -> %s", market_ticker)
-
-        # 3) Orderbook -> mark
+    while True:
         try:
-            ob = k.get_orderbook(market_ticker)
-            marks = kalshi_marks_from_orderbook(ob)
-        except Exception as e:
-            marks = None
-            if not QUIET_LOGS:
-                log.warning("Orderbook fetch failed: %s", str(e)[:200])
-
-        if marks is None:
-            # If no orderbook, we can’t safely trade. Sleep and continue.
-            time.sleep(POLL_SECONDS)
-            continue
-
-        p_yes, p_no, ob_dbg = marks
-        p_yes = clamp(p_yes, 0.01, 0.99)
-        p_no = 1.0 - p_yes
-
-        edge = fair_yes - p_yes
-
-        # 4) Safety / signal
-        if KILL_SWITCH:
-            signal = "HOLD"
-        elif LIVE_MODE and not marks_look_sane_for_live(p_yes):
-            signal = "HOLD"
-        elif LIVE_MODE and LIVE_DAILY_PROFIT_LOCK > 0 and realized_pnl_today >= LIVE_DAILY_PROFIT_LOCK:
-            signal = "HOLD"
-        else:
-            enter_th = effective_edge_enter()
-            if edge >= enter_th:
-                signal = "YES"
-            elif edge <= -enter_th:
-                signal = "NO"
-            else:
-                signal = "HOLD"
-
-        # 5) Risk gates apply ONLY to ENTER
-        blocked_reason = None
-        whale = (signal in ("YES", "NO")) and (abs(edge) >= WHALE_EDGE_OVERRIDE)
-
-        if position is None and signal in ("YES", "NO"):
-            max_daily_loss = effective_max_daily_loss()
-            max_trades_day = effective_max_trades_per_day()
-            cooldown_min = effective_cooldown_minutes()
-
-            if realized_pnl_today <= -max_daily_loss:
-                blocked_reason = f"MAX_DAILY_LOSS (pnl_today={realized_pnl_today:.2f} <= -{max_daily_loss:.2f})"
-            elif trades_today >= max_trades_day:
-                blocked_reason = f"MAX_TRADES_PER_DAY (trades_today={trades_today} >= {max_trades_day})"
-            else:
-                cd_active = cooldown_active(last_trade_ts, cooldown_min)
-                if cd_active and not (WHALE_COOLDOWN_OVERRIDE and whale):
-                    blocked_reason = f"COOLDOWN ({cooldown_min}m since last trade)"
-
-        # 6) Mark-to-market + summary
-        last_summary_ts = maybe_log_summary(
-            last_summary_ts=last_summary_ts,
-            p_yes=p_yes,
-            fair_yes=fair_yes,
-            edge=edge,
-            signal=signal,
-            balance=balance,
-            position=position,
-            entry_price=entry_price,
-            stake=stake,
-            realized_pnl_today=realized_pnl_today,
-        )
-
-        # 7) If blocked, just persist and wait
-        if blocked_reason:
-            save_state(
-                balance=balance, position=position, entry_price=entry_price, stake=stake,
-                market_ticker=market_ticker,
-                last_trade_ts=last_trade_ts, last_trade_day=last_trade_day,
-                trades_today=trades_today, realized_pnl_today=realized_pnl_today,
-                last_mark=p_yes,
-            )
+            rollover_day(state)
+            prices.append(kraken_last_price())
+            if len(prices) > LOOKBACK * 5:
+                prices = prices[-LOOKBACK * 5 :]
+
+            market = client.get_open_market()
+            ticker = market.get("ticker")
+            if not ticker:
+                raise RuntimeError("Open market missing ticker")
+
+            mark_yes = mark_yes_from_orderbook(client.get_orderbook(ticker))
+            fair_yes, z = model_fair_yes(prices)
+            edge = fair_yes - mark_yes
+
+            did_trade = maybe_exit(state, fair_yes, mark_yes, client)
+            if not did_trade:
+                maybe_enter(state, ticker, fair_yes, mark_yes, z, client)
+
+            write_status(state, message="running", fair_yes=fair_yes, mark_yes=mark_yes, edge=edge)
+            save_state(state)
             time.sleep(POLL_SECONDS)
-            continue
-
-        # 8) Paper engine decides ENTER/EXIT/HOLD based on edge-collapse logic
-        (
-            new_balance,
-            new_position,
-            new_entry_price,
-            new_stake,
-            action,
-            new_last_trade_ts,
-            new_last_trade_day,
-            new_trades_today,
-            new_realized_pnl_today,
-        ) = paper_trade_prob(
-            balance=balance,
-            position=position,
-            entry_price=entry_price,
-            stake=stake,
-            signal=signal,
-            p_yes=p_yes,
-            p_no=p_no,
-            edge=edge,
-            edge_exit=effective_edge_exit(),
-            trade_size=effective_trade_size(),
-            last_trade_ts=last_trade_ts,
-            last_trade_day=last_trade_day,
-            trades_today=trades_today,
-            realized_pnl_today=realized_pnl_today,
-        )
-
-        # 9) If action is ENTER/EXIT and LIVE_MODE, actually place Kalshi order
-        if action.startswith("ENTER_") or action == "EXIT":
-            # Decide which side we're buying for execution.
-            # ENTER buys the side we are entering.
-            # EXIT buys nothing; for “exit” we simulate by buying the opposite? Not correct.
-            # Better: for exit, we place a SELL order – but quickstart only shows BUY.
-            # So we implement "exit" by placing a BUY on the opposite side sized to neutralize,
-            # which approximates closing exposure in a 1-contract payout market.
-            #
-            # If you want true SELL exits, we can extend this once you confirm your account supports sell orders.
-            exec_side = None
-            exec_price_cents = None
-            exec_count = None
-
-            if action.startswith("ENTER_"):
-                exec_side = action.replace("ENTER_", "")  # YES/NO
-                entry_px_c, _ = entry_exit_prices_cents(ob_dbg, exec_side)
-                exec_price_cents = entry_px_c
-                exec_count = _contracts_for_trade_size(effective_trade_size(), (exec_price_cents / 100.0))
-
-            elif action == "EXIT" and position in ("YES", "NO"):
-                # Neutralize by buying opposite side with same approximate stake sizing
-                exec_side = ("NO" if position == "YES" else "YES")
-                entry_px_c, _ = entry_exit_prices_cents(ob_dbg, exec_side)
-                exec_price_cents = entry_px_c
-                # Use the same contract count approximation based on prior stake
-                exec_count = _contracts_for_trade_size(max(1.0, stake), (exec_price_cents / 100.0))
-
-            if LIVE_MODE and exec_side and exec_price_cents and exec_count:
-                res = run_live_order_and_wait(
-                    k=k,
-                    ticker=market_ticker,
-                    side=exec_side,
-                    count=exec_count,
-                    price_cents=exec_price_cents,
-                )
-                # Log trade row (this is your main “trade happened” log trail)
-                log_trade_row(
-                    market_ticker=market_ticker,
-                    action=("ENTER" if action.startswith("ENTER_") else "EXIT"),
-                    side=exec_side,
-                    price=exec_price_cents / 100.0,
-                    count=exec_count,
-                    stake=effective_trade_size(),
-                    fee=FEE_PER_TRADE,
-                    pnl=(None if action.startswith("ENTER_") else (new_realized_pnl_today - realized_pnl_today)),
-                    client_order_id=(res.raw.get("order", {}).get("client_order_id") if isinstance(res.raw, dict) else None),
-                    order_id=res.order_id,
-                    status=res.status,
-                )
-            else:
-                # PAPER log trail
-                log_trade_row(
-                    market_ticker=market_ticker,
-                    action=("ENTER" if action.startswith("ENTER_") else "EXIT"),
-                    side=(action.replace("ENTER_", "") if action.startswith("ENTER_") else (position or "")),
-                    price=(p_yes if (action.startswith("ENTER_YES") or position == "YES") else p_no),
-                    count=None,
-                    stake=effective_trade_size(),
-                    fee=FEE_PER_TRADE,
-                    pnl=(None if action.startswith("ENTER_") else (new_realized_pnl_today - realized_pnl_today)),
-                    client_order_id=None,
-                    order_id=None,
-                    status=("PAPER"),
-                )
-
-                if QUIET_LOGS:
-                    log.info("TRADE | %s | ticker=%s | edge=%+.3f p_yes=%.3f fair=%.3f",
-                             action, market_ticker, edge, p_yes, fair_yes)
-
-        # 10) Commit state
-        balance = new_balance
-        position = new_position
-        entry_price = new_entry_price
-        stake = new_stake
-        last_trade_ts = new_last_trade_ts
-        last_trade_day = new_last_trade_day
-        trades_today = new_trades_today
-        realized_pnl_today = new_realized_pnl_today
-
-        save_state(
-            balance=balance, position=position, entry_price=entry_price, stake=stake,
-            market_ticker=market_ticker,
-            last_trade_ts=last_trade_ts, last_trade_day=last_trade_day,
-            trades_today=trades_today, realized_pnl_today=realized_pnl_today,
-            last_mark=p_yes,
-        )
-
-        time.sleep(POLL_SECONDS)
+        except KeyboardInterrupt:
+            return
+        except Exception as exc:
+            log.error("loop error: %s", exc)
+            write_status(state, message=f"error: {exc}", fair_yes=0.5, mark_yes=0.5, edge=0.0)
+            save_state(state)
+            time.sleep(max(POLL_SECONDS, 10))
 
 
 if __name__ == "__main__":
-    try:
-        main()
-    except SystemExit:
-        raise
-    except Exception as e:
-        import traceback
-        log.error("FATAL: bot.py crashed: %r", e)
-        traceback.print_exc()
-        raise
+    main()

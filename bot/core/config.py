"""
core/config.py — All configuration driven by environment variables.

v13 CHANGES vs v12:
  - BAD_BOOK_BACKOFF_MAX: new exponential backoff cap for dead orderbooks (default 60s)
  - BAD_BOOK_BACKOFF_BASE: base sleep per consecutive BAD_BOOK (default 2s)
  - RATE_LIMIT_BACKOFF: sleep after any RATE_LIMITED event (default 10s)
  - BOT_VERSION bumped to v13
  - All v12 params preserved unchanged
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return int(float(v.strip()))
    except Exception:
        return default


def _float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return float(v.strip())
    except Exception:
        return default


@dataclass(frozen=True)
class Config:
    # ── Identity ──────────────────────────────────────────────────────────────
    BOT_VERSION: str = "JOHNNY5_KALSHI_BTC15M_v13"

    # ── Kalshi API ────────────────────────────────────────────────────────────
    KALSHI_BASE_URL: str = field(default_factory=lambda: _str("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/"))
    KALSHI_API_KEY_ID: str = field(default_factory=lambda: _str("KALSHI_API_KEY_ID"))
    KALSHI_PRIVATE_KEY_PEM: str = field(default_factory=lambda: _str("KALSHI_PRIVATE_KEY_PEM"))
    KALSHI_PRIVATE_KEY_B64: str = field(default_factory=lambda: _str("KALSHI_PRIVATE_KEY_B64"))
    KALSHI_PRIVATE_KEY_PATH: str = field(default_factory=lambda: _str("KALSHI_PRIVATE_KEY_PATH"))
    SERIES_TICKER: str = field(default_factory=lambda: _str("SERIES_TICKER", "KXBTC15M").upper())

    # ── Gateway ───────────────────────────────────────────────────────────────
    GATEWAY_URL: str = field(default_factory=lambda: _str("KALSHI_ORDER_GATEWAY_URL").rstrip("/"))

    # ── Mode ──────────────────────────────────────────────────────────────────
    LIVE_MODE: bool = field(default_factory=lambda: _bool("LIVE_MODE", False))
    DRY_RUN: bool = field(default_factory=lambda: _bool("DRY_RUN", False))
    KILL_SWITCH: bool = field(default_factory=lambda: _bool("KILL_SWITCH", False))
    CLOSE_ON_KILL_SWITCH: bool = field(default_factory=lambda: _bool("CLOSE_ON_KILL_SWITCH", False))

    # ── Price feeds ───────────────────────────────────────────────────────────
    KRAKEN_TICKER_URL: str = field(default_factory=lambda: _str("KRAKEN_TICKER_URL", "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"))
    COINBASE_TICKER_URL: str = field(default_factory=lambda: _str("COINBASE_TICKER_URL", "https://api.coinbase.com/v2/prices/BTC-USD/spot"))
    BINANCE_TICKER_URL: str = field(default_factory=lambda: _str("BINANCE_TICKER_URL", "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"))

    # ── Signal model ──────────────────────────────────────────────────────────
    LOOKBACK_SHORT: int = field(default_factory=lambda: _int("LOOKBACK_SHORT", 20))
    LOOKBACK_LONG: int = field(default_factory=lambda: _int("LOOKBACK_LONG", 60))
    BUCKET_MOVE_SCALE: float = field(default_factory=lambda: _float("BUCKET_MOVE_SCALE", 20.0))
    MOMENTUM_WEIGHT: float = field(default_factory=lambda: _float("MOMENTUM_WEIGHT", 0.10))
    OB_WEIGHT: float = field(default_factory=lambda: _float("OB_WEIGHT", 0.04))
    MIN_CONVICTION_Z: float = field(default_factory=lambda: _float("MIN_CONVICTION_Z", 2.0))

    # ── Edge thresholds ───────────────────────────────────────────────────────
    EDGE_ENTER: float = field(default_factory=lambda: _float("EDGE_ENTER", 0.08))
    EDGE_EXIT: float = field(default_factory=lambda: _float("EDGE_EXIT", 0.02))

    # ── Price range filter ────────────────────────────────────────────────────
    MIN_ENTRY_PRICE: float = field(default_factory=lambda: _float("MIN_ENTRY_PRICE", 0.25))
    MAX_ENTRY_PRICE: float = field(default_factory=lambda: _float("MAX_ENTRY_PRICE", 0.75))

    # ── Risk / sizing ─────────────────────────────────────────────────────────
    START_EQUITY: float = field(default_factory=lambda: _float("START_EQUITY", 50.0))
    RISK_FRACTION: float = field(default_factory=lambda: _float("RISK_FRACTION", 0.05))
    MAX_POSITION_USD: float = field(default_factory=lambda: _float("MAX_POSITION_USD", 3.0))
    MAX_CONTRACTS_PER_TRADE: int = field(default_factory=lambda: _int("MAX_CONTRACTS_PER_TRADE", 5))
    MAX_DAILY_LOSS: float = field(default_factory=lambda: _float("MAX_DAILY_LOSS", 6.0))
    MAX_DRAWDOWN_PCT: float = field(default_factory=lambda: _float("MAX_DRAWDOWN_PCT", 0.15))
    STOP_LOSS_PCT: float = field(default_factory=lambda: _float("STOP_LOSS_PCT", 0.25))
    STOP_LOSS_PCT_CHEAP: float = field(default_factory=lambda: _float("STOP_LOSS_PCT_CHEAP", 0.40))
    KELLY_FRACTION: float = field(default_factory=lambda: _float("KELLY_FRACTION", 0.25))

    # ── Market quality filters ────────────────────────────────────────────────
    MIN_BID_CENTS: int = field(default_factory=lambda: _int("MIN_BID_CENTS", 3))
    MAX_BID_CENTS: int = field(default_factory=lambda: _int("MAX_BID_CENTS", 97))
    MIN_TOP_QTY: int = field(default_factory=lambda: _int("MIN_TOP_QTY", 2))
    MIN_SECS_BEFORE_EXPIRY: int = field(default_factory=lambda: _int("MIN_SECS_BEFORE_EXPIRY", 90))
    MAX_SECS_BEFORE_EXPIRY: int = field(default_factory=lambda: _int("MAX_SECS_BEFORE_EXPIRY", 860))
    MAX_SPREAD_CENTS: int = field(default_factory=lambda: _int("MAX_SPREAD_CENTS", 15))

    # ── Timing ────────────────────────────────────────────────────────────────
    POLL_SECONDS: float = field(default_factory=lambda: _float("POLL_SECONDS", 8.0))
    COOLDOWN_SECONDS: int = field(default_factory=lambda: _int("COOLDOWN_SECONDS", 120))
    MIN_HOLD_SECONDS: int = field(default_factory=lambda: _int("MIN_HOLD_SECONDS", 90))
    HOLD_TO_EXPIRY: bool = field(default_factory=lambda: _bool("HOLD_TO_EXPIRY", True))

    # ── BAD_BOOK / Rate-limit backoff (NEW in v13) ────────────────────────────
    # When the orderbook is dead, back off exponentially instead of hammering
    # the API at full poll speed. This eliminates the rate-limiting seen in
    # the Mar 16 logs where 81 RATE_LIMITED events hit despite zero trading.
    #
    # Backoff schedule on consecutive BAD_BOOKs (same ticker):
    #   1st:  normal poll (POLL_SECONDS)
    #   2nd:  POLL_SECONDS + BAD_BOOK_BACKOFF_BASE   (default: 8 + 2 = 10s)
    #   3rd:  POLL_SECONDS + BAD_BOOK_BACKOFF_BASE*2 (16s)
    #   ...
    #   Nth:  capped at BAD_BOOK_BACKOFF_MAX          (default 60s)
    #
    # Resets to 0 on any new bucket or a good book.
    BAD_BOOK_BACKOFF_BASE: float = field(default_factory=lambda: _float("BAD_BOOK_BACKOFF_BASE", 2.0))
    BAD_BOOK_BACKOFF_MAX: float = field(default_factory=lambda: _float("BAD_BOOK_BACKOFF_MAX", 60.0))

    # After any RATE_LIMITED response, sleep this many seconds before retrying.
    # Kalshi's retry_after is typically 5s; we add a buffer.
    RATE_LIMIT_BACKOFF: float = field(default_factory=lambda: _float("RATE_LIMIT_BACKOFF", 12.0))

    # ── Infrastructure ────────────────────────────────────────────────────────
    PORT: int = field(default_factory=lambda: _int("PORT", 3000))
    STATE_FILE: Path = field(default_factory=lambda: Path(_str("STATE_FILE", ".runtime/state.json")))
    STATUS_FILE: Path = field(default_factory=lambda: Path(_str("STATUS_FILE", ".runtime/status.json")))
    REQUEST_TIMEOUT: int = field(default_factory=lambda: _int("REQUEST_TIMEOUT_SECONDS", 15))

    # ── Resilience ────────────────────────────────────────────────────────────
    CIRCUIT_BREAKER_THRESHOLD: int = field(default_factory=lambda: _int("CIRCUIT_BREAKER_THRESHOLD", 5))
    MAX_RETRIES: int = field(default_factory=lambda: _int("MAX_RETRIES", 3))

    # ── Debug ─────────────────────────────────────────────────────────────────
    DEBUG_MODEL: bool = field(default_factory=lambda: _bool("DEBUG_MODEL", False))
    DEBUG_BOOK: bool = field(default_factory=lambda: _bool("DEBUG_BOOK", False))
    DEBUG_ERRORS: bool = field(default_factory=lambda: _bool("DEBUG_ERRORS", False))

    def validate(self) -> list[str]:
        warnings = []
        if not self.KALSHI_API_KEY_ID:
            warnings.append("KALSHI_API_KEY_ID not set — read operations may fail")
        if not any([self.KALSHI_PRIVATE_KEY_PEM, self.KALSHI_PRIVATE_KEY_B64, self.KALSHI_PRIVATE_KEY_PATH]):
            warnings.append("No private key found — will run in paper mode only")
        if self.LIVE_MODE and not self.GATEWAY_URL:
            warnings.append("LIVE_MODE=true but KALSHI_ORDER_GATEWAY_URL not set — orders will fail")
        if self.LIVE_MODE and self.DRY_RUN:
            warnings.append("LIVE_MODE=true AND DRY_RUN=true — no orders will be placed")
        if self.RISK_FRACTION > 0.15:
            warnings.append(f"RISK_FRACTION={self.RISK_FRACTION} is aggressive (>15%)")
        if self.MIN_ENTRY_PRICE < 0.20:
            warnings.append(f"MIN_ENTRY_PRICE={self.MIN_ENTRY_PRICE} is low — lottery trades possible")
        if self.MAX_CONTRACTS_PER_TRADE > 8:
            warnings.append(f"MAX_CONTRACTS_PER_TRADE={self.MAX_CONTRACTS_PER_TRADE} is high — consider ≤5")
        return warnings


# Singleton — import this everywhere
cfg = Config()

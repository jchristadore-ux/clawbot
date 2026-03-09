"""
core/config.py — All configuration driven by environment variables.
Every value has a safe default. No magic. No hidden state.

See docs/env_vars.md for full documentation of each variable.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


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
    BOT_VERSION: str = "JOHNNY5_KALSHI_BTC15M_v11"

    # ── Kalshi API ────────────────────────────────────────────────────────────
    KALSHI_BASE_URL: str = field(default_factory=lambda: _str("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/"))
    KALSHI_API_KEY_ID: str = field(default_factory=lambda: _str("KALSHI_API_KEY_ID"))
    KALSHI_PRIVATE_KEY_PEM: str = field(default_factory=lambda: _str("KALSHI_PRIVATE_KEY_PEM"))
    KALSHI_PRIVATE_KEY_B64: str = field(default_factory=lambda: _str("KALSHI_PRIVATE_KEY_B64"))
    KALSHI_PRIVATE_KEY_PATH: str = field(default_factory=lambda: _str("KALSHI_PRIVATE_KEY_PATH"))
    SERIES_TICKER: str = field(default_factory=lambda: _str("SERIES_TICKER", "KXBTC15M").upper())

    # ── Gateway (Bun RSA signing proxy) ───────────────────────────────────────
    GATEWAY_URL: str = field(default_factory=lambda: _str("KALSHI_ORDER_GATEWAY_URL").rstrip("/"))

    # ── Mode ──────────────────────────────────────────────────────────────────
    LIVE_MODE: bool = field(default_factory=lambda: _bool("LIVE_MODE", False))
    DRY_RUN: bool = field(default_factory=lambda: _bool("DRY_RUN", False))     # logs orders but never submits, even in LIVE
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
    MIN_CONVICTION_Z: float = field(default_factory=lambda: _float("MIN_CONVICTION_Z", 1.5))

    # ── Edge thresholds ───────────────────────────────────────────────────────
    EDGE_ENTER: float = field(default_factory=lambda: _float("EDGE_ENTER", 0.07))
    EDGE_EXIT: float = field(default_factory=lambda: _float("EDGE_EXIT", 0.02))

    # ── Risk / sizing ─────────────────────────────────────────────────────────
    START_EQUITY: float = field(default_factory=lambda: _float("START_EQUITY", 50.0))
    RISK_FRACTION: float = field(default_factory=lambda: _float("RISK_FRACTION", 0.05))
    MAX_POSITION_USD: float = field(default_factory=lambda: _float("MAX_POSITION_USD", 5.0))
    MAX_CONTRACTS_PER_TRADE: int = field(default_factory=lambda: _int("MAX_CONTRACTS_PER_TRADE", 25))
    MAX_DAILY_LOSS: float = field(default_factory=lambda: _float("MAX_DAILY_LOSS", 8.0))
    MAX_DRAWDOWN_PCT: float = field(default_factory=lambda: _float("MAX_DRAWDOWN_PCT", 0.15))
    STOP_LOSS_PCT: float = field(default_factory=lambda: _float("STOP_LOSS_PCT", 0.30))
    KELLY_FRACTION: float = field(default_factory=lambda: _float("KELLY_FRACTION", 0.25))  # fractional Kelly cap
    VAR_DAILY_LIMIT: float = field(default_factory=lambda: _float("VAR_DAILY_LIMIT", 10.0))

    # ── Market quality filters ────────────────────────────────────────────────
    MIN_BID_CENTS: int = field(default_factory=lambda: _int("MIN_BID_CENTS", 3))
    MAX_BID_CENTS: int = field(default_factory=lambda: _int("MAX_BID_CENTS", 97))
    MIN_TOP_QTY: int = field(default_factory=lambda: _int("MIN_TOP_QTY", 2))
    MIN_SECS_BEFORE_EXPIRY: int = field(default_factory=lambda: _int("MIN_SECS_BEFORE_EXPIRY", 90))
    MAX_SECS_BEFORE_EXPIRY: int = field(default_factory=lambda: _int("MAX_SECS_BEFORE_EXPIRY", 860))  # skip if bucket just opened
    MAX_SPREAD_CENTS: int = field(default_factory=lambda: _int("MAX_SPREAD_CENTS", 15))

    # ── Timing ────────────────────────────────────────────────────────────────
    POLL_SECONDS: float = field(default_factory=lambda: _float("POLL_SECONDS", 8.0))
    COOLDOWN_SECONDS: int = field(default_factory=lambda: _int("COOLDOWN_SECONDS", 120))
    MIN_HOLD_SECONDS: int = field(default_factory=lambda: _int("MIN_HOLD_SECONDS", 90))
    HOLD_TO_EXPIRY: bool = field(default_factory=lambda: _bool("HOLD_TO_EXPIRY", True))

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
        """Return list of configuration warnings (not errors — we run in paper mode if unconfigured)."""
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
        if self.EDGE_ENTER < 0.04:
            warnings.append(f"EDGE_ENTER={self.EDGE_ENTER} is very low (<4%) — consider 7%+")
        return warnings


# Singleton — import this everywhere
cfg = Config()

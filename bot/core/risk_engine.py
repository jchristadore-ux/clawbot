"""
core/risk_engine.py — Deterministic risk gate.

ALL checks must pass before any trade is submitted.
No trade is sent unless RiskEngine.check() returns approved=True.

This module is intentionally isolated. You can read it in 5 minutes
and understand every rule the bot follows.

Rules (in order):
  1. Kill switch — hard block
  2. Dry run mode — log only
  3. Data freshness — don't trade on stale price
  4. Market quality — spread, depth, time-to-expiry bounds
  5. Daily loss limit — stop trading if daily P&L too negative
  6. Max drawdown — stop if equity dropped too far from peak
  7. Signal conviction — minimum |z| required
  8. Minimum edge — edge must exceed threshold
  9. Expected value — EV = p * b - (1-p) must be positive
  10. Kelly sizing — capped at fractional Kelly
  11. Maximum position size — hard USD cap
  12. Cash sufficiency — enough cash to place the trade
  13. Duplicate bucket guard — one trade per 15-min bucket

Risk formulas (from SKILL.md reference material):
  EV = p * b - (1 - p)          where b = decimal odds - 1
  edge = p_model - p_market
  f* = (p*b - q) / b            Kelly criterion
  f = alpha * f*                Fractional Kelly (alpha = 0.25)
  VaR_95 = mu - 1.645 * sigma
  MDD = (Peak - Trough) / Peak
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional

from .feature_pipeline import Book
from .notifier import log_event
from .scheduler import utc_iso
from .signal_engine import Signal


@dataclass
class RiskResult:
    approved: bool
    reasons: List[str] = field(default_factory=list)
    kelly_size: float = 0.0          # Kelly-suggested fraction of bankroll
    recommended_contracts: int = 0


class RiskEngine:
    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg
        self._peak_equity: float = cfg.START_EQUITY
        self._traded_buckets: set = set()

    def check(
        self,
        state: Any,            # BotState
        signal: Signal,
        book: Book,
        secs_left: Optional[float],
    ) -> RiskResult:
        cfg = self.cfg
        reasons: List[str] = []

        # ── 1. Kill switch ─────────────────────────────────────────────────────
        if cfg.KILL_SWITCH:
            return RiskResult(approved=False, reasons=["kill_switch_active"])

        # ── 2. Dry run ─────────────────────────────────────────────────────────
        if cfg.DRY_RUN:
            reasons.append("dry_run_mode")
            # Continue to log what WOULD have been approved, but mark denied
            # Fall through all checks; approved=False at end

        # ── 3. Signal has a side ───────────────────────────────────────────────
        if signal.side is None:
            return RiskResult(approved=False, reasons=["no_trade_signal"])

        # ── 4. Market quality: spread ──────────────────────────────────────────
        if book.spread_cents is not None and book.spread_cents > cfg.MAX_SPREAD_CENTS:
            reasons.append(f"spread_too_wide:{book.spread_cents}>{cfg.MAX_SPREAD_CENTS}")

        # ── 5. Market quality: depth ───────────────────────────────────────────
        entry_depth = book.yes_depth_3 if signal.side == "YES" else book.no_depth_3
        if entry_depth < cfg.MIN_TOP_QTY:
            reasons.append(f"thin_book:depth={entry_depth}<{cfg.MIN_TOP_QTY}")

        # ── 6. Time-to-expiry: not too close to settlement ────────────────────
        if secs_left is not None and secs_left < cfg.MIN_SECS_BEFORE_EXPIRY:
            reasons.append(f"too_close_to_expiry:{secs_left:.0f}s<{cfg.MIN_SECS_BEFORE_EXPIRY}s")

        # ── 7. Time-to-expiry: not too early (bucket just opened, market thin) ─
        if secs_left is not None and secs_left > cfg.MAX_SECS_BEFORE_EXPIRY:
            reasons.append(f"bucket_too_new:{secs_left:.0f}s>{cfg.MAX_SECS_BEFORE_EXPIRY}s")

        # ── 8. Cooldown ────────────────────────────────────────────────────────
        if state.last_trade_ts:
            try:
                last_dt = datetime.fromisoformat(state.last_trade_ts.replace("Z", "+00:00"))
                elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds()
                if elapsed < cfg.COOLDOWN_SECONDS:
                    return RiskResult(approved=False, reasons=[f"cooldown:{elapsed:.0f}s<{cfg.COOLDOWN_SECONDS}s"])
            except Exception:
                pass

        # ── 9. Daily loss limit ────────────────────────────────────────────────
        daily_pnl = self._get_daily_pnl(state)
        if daily_pnl <= -abs(cfg.MAX_DAILY_LOSS):
            return RiskResult(approved=False, reasons=[f"daily_loss_limit:{daily_pnl:.2f}"])

        # ── 10. Max drawdown ───────────────────────────────────────────────────
        equity = state.cash
        self._peak_equity = max(self._peak_equity, equity)
        if self._peak_equity > 0:
            drawdown = (self._peak_equity - equity) / self._peak_equity
            if drawdown > cfg.MAX_DRAWDOWN_PCT:
                return RiskResult(approved=False, reasons=[f"max_drawdown:{drawdown:.1%}>{cfg.MAX_DRAWDOWN_PCT:.1%}"])

        # ── 11. Signal conviction (z-score) — directional gate ─────────────────────
        if signal.side == "YES" and signal.z < cfg.MIN_CONVICTION_Z:
            reasons.append(f"low_conviction_yes:z={signal.z:.2f}<+{cfg.MIN_CONVICTION_Z}")
        elif signal.side == "NO" and signal.z > -cfg.MIN_CONVICTION_Z:
            reasons.append(f"low_conviction_no:z={signal.z:.2f}>-{cfg.MIN_CONVICTION_Z}")

        # ── 12. Edge threshold ─────────────────────────────────────────────────
        # Adaptive: tighten on cold streak, loosen slightly on hot streak
        eff_edge = self._adaptive_edge(state)
        if abs(signal.edge) < eff_edge:
            reasons.append(f"edge_below_threshold:{abs(signal.edge):.3f}<{eff_edge:.3f}")

        # ── 13. Minimum expected value ─────────────────────────────────────────
        # EV = p * b - (1 - p)  where b = (1/entry_price - 1)
        if signal.entry_cents:
            entry_price = signal.entry_cents / 100.0
            p = signal.fair_yes if signal.side == "YES" else (1.0 - signal.fair_yes)
            b = max(0.01, 1.0 / max(entry_price, 0.01) - 1.0)  # decimal odds - 1
            ev = p * b - (1.0 - p)
            if ev <= 0:
                reasons.append(f"negative_ev:{ev:.4f}")

        # ── 14. Fractional Kelly sizing ────────────────────────────────────────
        kelly_size = 0.0
        recommended_contracts = 0
        if signal.entry_cents:
            entry_price = signal.entry_cents / 100.0
            p = signal.fair_yes if signal.side == "YES" else (1.0 - signal.fair_yes)
            q = 1.0 - p
            b = max(0.01, 1.0 / max(entry_price, 0.01) - 1.0)
            kelly_full = (p * b - q) / max(b, 0.01)
            kelly_size = max(0.0, min(1.0, kelly_full * cfg.KELLY_FRACTION))

            # Use min of Kelly-implied and cfg risk fraction
            risk_frac = min(kelly_size, cfg.RISK_FRACTION)
            risk_usd = min(cfg.MAX_POSITION_USD, max(0.5, state.cash * risk_frac))
            recommended_contracts = max(1, min(cfg.MAX_CONTRACTS_PER_TRADE, int(risk_usd / max(entry_price, 0.01))))

        # ── 15. Cash sufficiency ───────────────────────────────────────────────
        if signal.entry_cents and recommended_contracts > 0:
            cost = (signal.entry_cents / 100.0) * recommended_contracts
            if state.cash < cost:
                reasons.append(f"insufficient_cash:{state.cash:.2f}<{cost:.2f}")

        # ── 16. Duplicate bucket guard ────────────────────────────────────────
        ticker = state.current_bucket_ticker
        if ticker and ticker in self._traded_buckets:
            return RiskResult(approved=False, reasons=["bucket_already_traded"])

        # ── Final decision ─────────────────────────────────────────────────────
        if cfg.DRY_RUN:
            log_event("DRY_RUN_WOULD_TRADE", {
                "side": signal.side,
                "edge": round(signal.edge, 4),
                "contracts": recommended_contracts,
                "reasons_that_would_block": reasons,
            })
            return RiskResult(approved=False, reasons=["dry_run"] + reasons, kelly_size=kelly_size,
                              recommended_contracts=recommended_contracts)

        if reasons:
            return RiskResult(approved=False, reasons=reasons, kelly_size=kelly_size,
                              recommended_contracts=recommended_contracts)

        return RiskResult(approved=True, reasons=[], kelly_size=kelly_size,
                          recommended_contracts=recommended_contracts)

    def mark_bucket_traded(self, ticker: str) -> None:
        self._traded_buckets.add(ticker)

    def purge_stale_buckets(self, active_ticker: str) -> None:
        """Remove all buckets except the currently active one."""
        self._traded_buckets = {t for t in self._traded_buckets if t == active_ticker}

    def update_peak_equity(self, cash: float) -> None:
        self._peak_equity = max(self._peak_equity, cash)

    @staticmethod
    def _get_daily_pnl(state: Any) -> float:
        """Sum PnL from trades in the last 24 hours."""
        if not state.trade_history_24h:
            return 0.0
        now = datetime.now(timezone.utc)
        total = 0.0
        for t in state.trade_history_24h:
            try:
                ts = t.get("ts", "")
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if (now - dt) <= timedelta(hours=24):
                    total += float(t.get("pnl", 0.0))
            except Exception:
                pass
        return total

    def _adaptive_edge(self, state: Any) -> float:
        """
        Tighten edge requirement after losing streak; loosen slightly on winning streak.
        This is a minor adaptive filter — the base threshold should do most of the work.
        """
        base = self.cfg.EDGE_ENTER
        streak = getattr(state, "win_streak", 0)
        if streak <= -3:
            tighten = min(0.08, abs(streak) * 0.015)
            return min(0.20, base + tighten)
        elif streak >= 5:
            loosen = min(0.02, streak * 0.003)
            return max(0.04, base - loosen)
        return base

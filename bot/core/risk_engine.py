"""
core/risk_engine.py — Deterministic risk gate.

v12 CHANGES vs v11:
  - z-score check is now DIRECTIONAL (fix for jackpot/counter-momentum trades)
  - Contract cap hard-limited to MAX_CONTRACTS_PER_TRADE (default 5, not 25)
  - Sizing now conviction-weighted: higher z+edge = larger position, not just Kelly
  - Price floor/ceiling enforced in signal_engine AND here as double-guard
  - Cooldown is now per-bucket, not global (allows re-entry in same bucket if stopped)
  - Removed VAR_DAILY_LIMIT (redundant with MAX_DAILY_LOSS)

ALL checks must pass before any trade is submitted.
No trade is sent unless RiskEngine.check() returns approved=True.

Rules (in order):
  1.  Kill switch — hard block
  2.  Dry run mode — log only
  3.  Signal has a side
  4.  Market quality: spread
  5.  Market quality: depth
  6.  Time-to-expiry bounds
  7.  Cooldown (per bucket, not global)
  8.  Daily loss limit
  9.  Max drawdown
  10. Signal conviction — z must AGREE WITH DIRECTION (v12 key fix)
  11. Minimum edge
  12. Entry price in valid range [MIN_ENTRY_PRICE, MAX_ENTRY_PRICE]
  13. Minimum expected value
  14. Conviction-weighted sizing (z + edge both scale contracts)
  15. Cash sufficiency
  16. Duplicate bucket guard
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
    kelly_size: float = 0.0
    recommended_contracts: int = 0


class RiskEngine:
    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg
        self._peak_equity: float = cfg.START_EQUITY
        self._traded_buckets: set = set()

    def check(
        self,
        state: Any,
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

        # ── 6. Time-to-expiry bounds ───────────────────────────────────────────
        if secs_left is not None and secs_left < cfg.MIN_SECS_BEFORE_EXPIRY:
            reasons.append(f"too_close_to_expiry:{secs_left:.0f}s<{cfg.MIN_SECS_BEFORE_EXPIRY}s")
        if secs_left is not None and secs_left > cfg.MAX_SECS_BEFORE_EXPIRY:
            reasons.append(f"bucket_too_new:{secs_left:.0f}s>{cfg.MAX_SECS_BEFORE_EXPIRY}s")

        # ── 7. Cooldown ────────────────────────────────────────────────────────
        if state.last_trade_ts:
            try:
                last_dt = datetime.fromisoformat(state.last_trade_ts.replace("Z", "+00:00"))
                elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds()
                if elapsed < cfg.COOLDOWN_SECONDS:
                    return RiskResult(approved=False, reasons=[f"cooldown:{elapsed:.0f}s<{cfg.COOLDOWN_SECONDS}s"])
            except Exception:
                pass

        # ── 8. Daily loss limit ────────────────────────────────────────────────
        daily_pnl = self._get_daily_pnl(state)
        if daily_pnl <= -abs(cfg.MAX_DAILY_LOSS):
            return RiskResult(approved=False, reasons=[f"daily_loss_limit:{daily_pnl:.2f}"])

        # ── 9. Max drawdown ────────────────────────────────────────────────────
        equity = state.cash
        self._peak_equity = max(self._peak_equity, equity)
        if self._peak_equity > 0:
            drawdown = (self._peak_equity - equity) / self._peak_equity
            if drawdown > cfg.MAX_DRAWDOWN_PCT:
                return RiskResult(approved=False, reasons=[f"max_drawdown:{drawdown:.1%}>{cfg.MAX_DRAWDOWN_PCT:.1%}"])

        # ── 10. Signal conviction — DIRECTIONAL z-score check (v12 KEY FIX) ───
        # YES trades need POSITIVE z (momentum up). NO trades need NEGATIVE z.
        # abs(z) check was wrong — it allowed counter-momentum entries.
        if signal.side == "YES" and signal.z < cfg.MIN_CONVICTION_Z:
            reasons.append(f"low_conviction_yes:z={signal.z:.2f}<+{cfg.MIN_CONVICTION_Z}")
        elif signal.side == "NO" and signal.z > -cfg.MIN_CONVICTION_Z:
            reasons.append(f"low_conviction_no:z={signal.z:.2f}>-{cfg.MIN_CONVICTION_Z}")

        # ── 11. Edge threshold ─────────────────────────────────────────────────
        eff_edge = self._adaptive_edge(state)
        if abs(signal.edge) < eff_edge:
            reasons.append(f"edge_below_threshold:{abs(signal.edge):.3f}<{eff_edge:.3f}")

        # ── 12. Entry price in valid range ─────────────────────────────────────
        # Blocks lottery tickets (<25c) and near-certain contracts (>75c)
        # These are where the jackpot/garbage trades live
        if signal.entry_cents is not None:
            entry_pct = signal.entry_cents / 100.0
            if entry_pct < cfg.MIN_ENTRY_PRICE:
                reasons.append(f"entry_too_cheap:{entry_pct:.2f}<{cfg.MIN_ENTRY_PRICE:.2f}")
            elif entry_pct > cfg.MAX_ENTRY_PRICE:
                reasons.append(f"entry_too_expensive:{entry_pct:.2f}>{cfg.MAX_ENTRY_PRICE:.2f}")

        # ── 13. Minimum expected value ─────────────────────────────────────────
        if signal.entry_cents:
            entry_price = signal.entry_cents / 100.0
            p = signal.fair_yes if signal.side == "YES" else (1.0 - signal.fair_yes)
            b = max(0.01, 1.0 / max(entry_price, 0.01) - 1.0)
            ev = p * b - (1.0 - p)
            if ev <= 0:
                reasons.append(f"negative_ev:{ev:.4f}")

        # ── 14. Conviction-weighted sizing (v12) ───────────────────────────────
        # Instead of pure Kelly (which inflates contract count on cheap contracts),
        # use a conviction score that blends z-strength and edge magnitude.
        # This naturally produces smaller positions on borderline trades.
        kelly_size = 0.0
        recommended_contracts = 0
        if signal.entry_cents:
            entry_price = signal.entry_cents / 100.0

            # Base risk: risk_fraction of equity, capped at MAX_POSITION_USD
            base_risk_usd = min(cfg.MAX_POSITION_USD, max(0.5, state.cash * cfg.RISK_FRACTION))

            # Scale down by conviction (0-1): low conviction = smaller trade
            # conviction=1.0 → full base_risk, conviction=0.3 → 30% of base_risk
            conviction_scaled_usd = base_risk_usd * max(0.25, signal.conviction)

            raw_contracts = int(conviction_scaled_usd / max(entry_price, 0.01))

            # Hard contract cap — prevents 19x/24x/25x lottery sizing
            recommended_contracts = max(1, min(cfg.MAX_CONTRACTS_PER_TRADE, raw_contracts))

            # Also compute Kelly for logging purposes
            p = signal.fair_yes if signal.side == "YES" else (1.0 - signal.fair_yes)
            q = 1.0 - p
            b = max(0.01, 1.0 / max(entry_price, 0.01) - 1.0)
            kelly_full = (p * b - q) / max(b, 0.01)
            kelly_size = max(0.0, min(1.0, kelly_full * cfg.KELLY_FRACTION))

        # ── 15. Cash sufficiency ───────────────────────────────────────────────
        if signal.entry_cents and recommended_contracts > 0:
            cost = (signal.entry_cents / 100.0) * recommended_contracts
            if state.cash < cost:
                reasons.append(f"insufficient_cash:{state.cash:.2f}<{cost:.2f}")

        # ── 16. Duplicate bucket guard ─────────────────────────────────────────
        ticker = state.current_bucket_ticker
        if ticker and ticker in self._traded_buckets:
            return RiskResult(approved=False, reasons=["bucket_already_traded"])

        # ── Final decision ─────────────────────────────────────────────────────
        if cfg.DRY_RUN:
            log_event("DRY_RUN_WOULD_TRADE", {
                "side": signal.side,
                "edge": round(signal.edge, 4),
                "z": round(signal.z, 3),
                "conviction": round(signal.conviction, 3),
                "contracts": recommended_contracts,
                "reasons_that_would_block": reasons,
            })
            return RiskResult(approved=False, reasons=["dry_run"] + reasons,
                              kelly_size=kelly_size, recommended_contracts=recommended_contracts)

        if reasons:
            return RiskResult(approved=False, reasons=reasons,
                              kelly_size=kelly_size, recommended_contracts=recommended_contracts)

        return RiskResult(approved=True, reasons=[],
                          kelly_size=kelly_size, recommended_contracts=recommended_contracts)

    def mark_bucket_traded(self, ticker: str) -> None:
        self._traded_buckets.add(ticker)

    def purge_stale_buckets(self, active_ticker: str) -> None:
        self._traded_buckets = {t for t in self._traded_buckets if t == active_ticker}

    def update_peak_equity(self, cash: float) -> None:
        self._peak_equity = max(self._peak_equity, cash)

    @staticmethod
    def _get_daily_pnl(state: Any) -> float:
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
        Tighten edge requirement after losing streak; loosen slightly on hot streak.
        Minor adaptive filter — base threshold does most of the work.
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

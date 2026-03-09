"""
core/signal_engine.py — Calibrated probability model for KXBTC15M.

Model architecture:
  Three factors, composed additively around 0.5 base:

  Factor 1 (PRIMARY): BTC bucket move
    - Anchor: what % has BTC moved since this 15-min window opened?
    - Time amplifier: early-bucket moves are less committed; late-bucket moves are more certain
    - This is the core edge hypothesis: Kalshi orderbook reprices slowly relative to BTC spot

  Factor 2 (SECONDARY): Short-term BTC momentum z-score
    - Z-score of most recent return vs rolling window
    - Confirmation signal only (low weight)
    - Dampened in high-volatility regimes

  Factor 3 (TERTIARY): Orderbook depth imbalance
    - YES depth vs NO depth at top 3 levels
    - Tiny weight; orderbook can be gamed

  Composition: fair_yes = 0.5 + bucket_move + momentum_nudge + ob_nudge

Output includes:
  - fair_yes: model probability (0-1)
  - mark_yes: market-implied probability (0-1)
  - edge: fair_yes - mark_yes (positive = YES is underpriced)
  - z: momentum z-score for conviction filter
  - vol_regime: short/long vol ratio for regime flag

IMPORTANT: This model is NOT validated. Do not assume edge is real.
Run backtest/backtest_engine.py before risking real money.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional

from .feature_pipeline import Book, FeatureSnapshot
from .notifier import log_event


@dataclass
class Signal:
    fair_yes: float          # model probability (0-1)
    mark_yes: float          # market mid (0-1)
    edge: float              # fair_yes - mark_yes
    z: float                 # momentum z-score
    vol_regime: float        # short_vol / long_vol ratio
    bucket_move_pct: float   # % BTC move since bucket open
    bucket_elapsed_frac: float
    side: Optional[str]      # "YES", "NO", or None (no trade signal)
    entry_cents: Optional[int]  # entry price in cents if side is set


class SignalEngine:
    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg

    def compute(self, snap: FeatureSnapshot, book: Book) -> Signal:
        """
        Compute the full signal from current features and orderbook.
        Returns a Signal with fair_yes, edge, z, and suggested entry.
        """
        cfg = self.cfg
        prices = snap.prices
        mark_yes = book.mark_yes  # already validated non-None by caller

        # ── Factor 1: BTC bucket move ─────────────────────────────────────────
        bucket_move = 0.0
        bucket_move_pct = 0.0
        if snap.bucket_start_price and snap.bucket_start_price > 0 and snap.btc_price > 0:
            bucket_move_pct = (snap.btc_price - snap.bucket_start_price) / snap.bucket_start_price
            # Time amplifier: 0.4x at bucket open → 1.6x at bucket close
            time_amp = 0.4 + 1.2 * max(0.0, min(1.0, snap.bucket_elapsed_frac))
            raw_move = bucket_move_pct * cfg.BUCKET_MOVE_SCALE * time_amp
            bucket_move = max(-0.45, min(0.45, raw_move))

        # ── Factor 2: Momentum z-score ────────────────────────────────────────
        z = 0.0
        vol_regime = 1.0
        if len(prices) >= cfg.LOOKBACK_SHORT:
            window = prices[-cfg.LOOKBACK_SHORT:]
            rets = [
                (window[i] - window[i-1]) / window[i-1]
                for i in range(1, len(window))
                if window[i-1] > 0
            ]
            if len(rets) >= 3:
                mu = sum(rets) / len(rets)
                var = sum((r - mu)**2 for r in rets) / max(1, len(rets) - 1)
                std = max(var**0.5, 1e-9)
                z = (rets[-1] - mu) / std

                # Vol regime: compare short vol to long vol
                if len(prices) >= cfg.LOOKBACK_LONG:
                    long_rets = [
                        (prices[i] - prices[i-1]) / prices[i-1]
                        for i in range(max(1, len(prices) - cfg.LOOKBACK_LONG), len(prices))
                        if prices[i-1] > 0
                    ]
                    if long_rets:
                        long_rms = math.sqrt(sum(r**2 for r in long_rets) / len(long_rets))
                        short_rms = math.sqrt(sum(r**2 for r in rets) / max(1, len(rets)))
                        vol_regime = short_rms / max(long_rms, 1e-10)

        # ── Factor 3: Orderbook imbalance ─────────────────────────────────────
        total_depth = book.yes_depth_3 + book.no_depth_3
        ob_imb = (book.yes_depth_3 - book.no_depth_3) / max(total_depth, 1)
        ob_nudge = ob_imb * cfg.OB_WEIGHT

        # ── Compose ────────────────────────────────────────────────────────────
        if snap.bucket_start_price:
            # Primary path: bucket move anchors the signal
            vol_dampener = 1.0 / max(1.0, vol_regime - 0.5)
            momentum_nudge = z * cfg.MOMENTUM_WEIGHT * vol_dampener
            fair_yes = 0.5 + bucket_move + momentum_nudge + ob_nudge
        else:
            # Fallback (no bucket start yet): momentum only
            vol_dampener = 1.0 / max(1.0, vol_regime - 0.5)
            fair_yes = 0.5 + z * cfg.MOMENTUM_WEIGHT * vol_dampener + ob_nudge

        fair_yes = max(0.02, min(0.98, fair_yes))
        edge = fair_yes - mark_yes

        # ── Trade direction ────────────────────────────────────────────────────
        side: Optional[str] = None
        entry_cents: Optional[int] = None

        if book.best_yes_bid is not None and book.best_no_bid is not None:
            if edge >= cfg.EDGE_ENTER:
                side = "YES"
                entry_cents = max(1, min(99, 100 - book.best_no_bid))  # buy at YES ask
            elif edge <= -cfg.EDGE_ENTER:
                side = "NO"
                entry_cents = max(1, min(99, 100 - book.best_yes_bid))  # buy at NO ask

        if cfg.DEBUG_MODEL:
            log_event("SIGNAL", {
                "fair": round(fair_yes, 4),
                "mark": round(mark_yes, 4),
                "edge": round(edge, 4),
                "z": round(z, 3),
                "vol_regime": round(vol_regime, 3),
                "bucket_move_pct": round(bucket_move_pct * 100, 3),
                "bucket_elapsed": round(snap.bucket_elapsed_frac, 3),
                "side": side,
            }, throttle_key="signal_dbg", throttle_s=15)

        return Signal(
            fair_yes=fair_yes,
            mark_yes=mark_yes,
            edge=edge,
            z=z,
            vol_regime=vol_regime,
            bucket_move_pct=bucket_move_pct,
            bucket_elapsed_frac=snap.bucket_elapsed_frac,
            side=side,
            entry_cents=entry_cents,
        )

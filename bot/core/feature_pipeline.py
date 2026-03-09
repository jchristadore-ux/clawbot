"""
core/feature_pipeline.py — BTC price feeds, orderbook parsing, feature computation.

Manages:
- Multi-source BTC price fetching with fallback chain
- Rolling price history for signal computation
- Bucket tracking (open price per 15-min window)
- Orderbook parsing into a clean Book datatype
- Feature snapshot for the signal engine

Data flow:
  fetch_btc_price() → update(price) → get_snapshot() → SignalEngine.compute()
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from .notifier import log_event
from .scheduler import utc_iso


@dataclass
class Book:
    """Parsed orderbook state."""
    best_yes_bid: Optional[int]   # cents (0-100)
    best_no_bid: Optional[int]    # cents (0-100)
    yes_depth_3: int              # contracts at top 3 YES bid levels
    no_depth_3: int               # contracts at top 3 NO bid levels
    spread_cents: Optional[int]   # implied spread in cents
    mark_yes: Optional[float]     # mid-price (0-1) or None if bad book


@dataclass
class FeatureSnapshot:
    """All features needed for signal computation at a point in time."""
    prices: List[float]           # recent BTC prices (most recent last)
    bucket_start_price: Optional[float]
    bucket_elapsed_frac: float    # 0=just opened, 1=about to close
    secs_left: Optional[float]
    btc_price: float


class FeaturePipeline:
    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg
        self._prices: List[float] = []
        self._bucket_ticker: Optional[str] = None
        self._bucket_start_price: Optional[float] = None
        self._bucket_open_ts: Optional[float] = None  # unix timestamp
        self._secs_left: Optional[float] = None
        self._max_prices = cfg.LOOKBACK_LONG * 20

    # ── BTC price fetching ────────────────────────────────────────────────────

    def fetch_btc_price(self) -> Optional[float]:
        """Try Kraken → Coinbase → Binance. Return price or None."""
        for fetcher in [self._fetch_kraken, self._fetch_coinbase, self._fetch_binance]:
            try:
                price = fetcher()
                if price and price > 1000:
                    return price
            except Exception as exc:
                log_event("PRICE_FEED_ERR", {"source": fetcher.__name__, "err": str(exc)[:100]},
                          throttle_key=fetcher.__name__, throttle_s=60)
        return None

    def _fetch_kraken(self) -> Optional[float]:
        r = requests.get(self.cfg.KRAKEN_TICKER_URL, timeout=self.cfg.REQUEST_TIMEOUT)
        r.raise_for_status()
        result = r.json().get("result", {})
        if not result:
            return None
        key = next(iter(result))
        return float(result[key]["c"][0])

    def _fetch_coinbase(self) -> Optional[float]:
        r = requests.get(self.cfg.COINBASE_TICKER_URL, timeout=self.cfg.REQUEST_TIMEOUT)
        r.raise_for_status()
        return float(r.json()["data"]["amount"])

    def _fetch_binance(self) -> Optional[float]:
        r = requests.get(self.cfg.BINANCE_TICKER_URL, timeout=self.cfg.REQUEST_TIMEOUT)
        r.raise_for_status()
        return float(r.json()["price"])

    # ── Price history ─────────────────────────────────────────────────────────

    def update(self, price: float) -> None:
        """Add a new BTC price observation."""
        self._prices.append(price)
        if len(self._prices) > self._max_prices:
            self._prices = self._prices[-self._max_prices:]

    def update_bucket(self, ticker: str, btc_price: float, secs_left: Optional[float]) -> None:
        """Track when the bucket changes; record the opening BTC price."""
        self._secs_left = secs_left
        if ticker != self._bucket_ticker:
            self._bucket_ticker = ticker
            self._bucket_start_price = btc_price
            self._bucket_open_ts = time.time()
            log_event("NEW_BUCKET", {
                "ticker": ticker,
                "btc_open": round(btc_price, 2),
                "secs_left": round(secs_left) if secs_left else None,
            })

    def get_snapshot(self) -> FeatureSnapshot:
        """Return current feature snapshot for signal computation."""
        elapsed_frac = 0.33
        if self._bucket_open_ts and self._secs_left is not None:
            secs_elapsed = time.time() - self._bucket_open_ts
            total = secs_elapsed + self._secs_left
            elapsed_frac = max(0.0, min(1.0, secs_elapsed / max(total, 1.0)))

        return FeatureSnapshot(
            prices=list(self._prices),
            bucket_start_price=self._bucket_start_price,
            bucket_elapsed_frac=elapsed_frac,
            secs_left=self._secs_left,
            btc_price=self._prices[-1] if self._prices else 0.0,
        )

    # ── Orderbook parsing ─────────────────────────────────────────────────────

    def parse_orderbook(self, ob_data: Dict[str, Any]) -> Book:
        """Parse Kalshi orderbook response into a clean Book datatype."""
        root = ob_data.get("orderbook", ob_data)
        yes_levels = root.get("yes") or root.get("yes_bids") or []
        no_levels = root.get("no") or root.get("no_bids") or []

        best_yes = self._best_bid(yes_levels)
        best_no = self._best_bid(no_levels)
        yes_depth = self._depth_top3(yes_levels)
        no_depth = self._depth_top3(no_levels)

        if self.cfg.DEBUG_BOOK:
            log_event("BOOK_RAW", {
                "yes_levels_count": len(yes_levels) if isinstance(yes_levels, list) else 0,
                "no_levels_count": len(no_levels) if isinstance(no_levels, list) else 0,
                "best_yes": best_yes,
                "best_no": best_no,
            }, throttle_key="book_raw", throttle_s=30)

        mark = self._compute_mark(best_yes, best_no)
        spread = self._compute_spread(best_yes, best_no)

        return Book(
            best_yes_bid=best_yes,
            best_no_bid=best_no,
            yes_depth_3=yes_depth,
            no_depth_3=no_depth,
            spread_cents=spread,
            mark_yes=mark,
        )

    @staticmethod
    def _best_bid(levels: Any) -> Optional[int]:
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
            if price is not None and (best is None or price > best):
                best = price
        return best

    @staticmethod
    def _depth_top3(levels: Any) -> int:
        if not isinstance(levels, list) or not levels:
            return 0
        # Sort by price descending, take top 3
        parsed = []
        for lvl in levels:
            if isinstance(lvl, list) and len(lvl) >= 2:
                try:
                    parsed.append((int(lvl[0]), int(lvl[1])))
                except Exception:
                    pass
            elif isinstance(lvl, dict):
                try:
                    p = int(lvl.get("price", 0) or 0)
                    q = int(lvl.get("quantity", 0) or lvl.get("qty", 0) or 0)
                    parsed.append((p, q))
                except Exception:
                    pass
        parsed.sort(key=lambda x: x[0], reverse=True)
        return sum(q for _, q in parsed[:3])

    def _compute_mark(self, yes_bid: Optional[int], no_bid: Optional[int]) -> Optional[float]:
        cfg = self.cfg
        if yes_bid is None or no_bid is None:
            return None
        if yes_bid < cfg.MIN_BID_CENTS or no_bid < cfg.MIN_BID_CENTS:
            return None
        if yes_bid > cfg.MAX_BID_CENTS or no_bid > cfg.MAX_BID_CENTS:
            return None
        implied_yes_ask = 100 - no_bid
        mid = (yes_bid + implied_yes_ask) / 200.0
        return max(0.01, min(0.99, mid))

    @staticmethod
    def _compute_spread(yes_bid: Optional[int], no_bid: Optional[int]) -> Optional[int]:
        if yes_bid is None or no_bid is None:
            return None
        implied_yes_ask = 100 - no_bid
        return max(0, implied_yes_ask - yes_bid)

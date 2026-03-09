"""
core/market_selector.py — Select the best active KXBTC15M bucket.

Selection logic:
1. Parse all open markets, extract open_time and close_time
2. Filter to markets where now is between open and close
3. Among those, prefer the one with highest volume+OI (most liquid)
4. If all have zero volume (market just opened), prefer earliest-closing (current bucket)
5. Annotate with secs_left for downstream use

Why this matters:
- Kalshi returns multiple open markets at once (future buckets may be listed)
- We only want the CURRENT 15-minute bucket, not future ones
- Picking the wrong bucket means the price signal is meaningless
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .notifier import log_event
from .scheduler import utc_iso


def _parse_ts(x: Any) -> Optional[datetime]:
    """Parse unix int/float (secs or ms) or ISO string → UTC datetime."""
    if isinstance(x, (int, float)) and x > 0:
        try:
            ts = x / 1000.0 if x > 1e11 else float(x)
            return datetime.fromtimestamp(ts, tz=timezone.utc)
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
            ts = f / 1000.0 if f > 1e11 else f
            return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        pass
    return None


def _safe_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        return float(x)
    except Exception:
        return 0.0


def pick_best_active_market(
    markets: List[Dict[str, Any]],
    series_ticker: str = "KXBTC15M",
) -> Optional[Dict[str, Any]]:
    """
    Returns the best active market dict, augmented with 'secs_left' key,
    or None if no suitable market found.
    """
    now = datetime.now(timezone.utc)
    candidates: List[Tuple[float, float, Dict[str, Any]]] = []  # (score, close_ts_epoch, market)
    parse_failures = 0

    for m in markets:
        if not isinstance(m, dict):
            continue

        ticker = str(m.get("ticker", ""))

        # Try multiple known field name variants (Kalshi has changed these)
        raw_ot = (m.get("open_time") or m.get("open_ts") or m.get("openTime") or
                  m.get("open_date") or m.get("open"))
        raw_ct = (m.get("close_time") or m.get("close_ts") or m.get("closeTime") or
                  m.get("close_date") or m.get("close") or m.get("expiration_time") or
                  m.get("expiry_time"))

        ot = _parse_ts(raw_ot)
        ct = _parse_ts(raw_ct)

        if ot is None or ct is None:
            parse_failures += 1
            if parse_failures <= 3:
                log_event("MARKET_TS_PARSE_FAIL", {
                    "ticker": ticker,
                    "raw_ot": str(raw_ot)[:40],
                    "raw_ct": str(raw_ct)[:40],
                    "available_keys": list(m.keys())[:15],
                }, throttle_key=f"ts_fail_{ticker}", throttle_s=120)
            continue

        # Only include currently open buckets
        if not (ot <= now < ct):
            continue

        secs_left = (ct - now).total_seconds()
        score = (
            _safe_float(m.get("volume") or m.get("volume_24h") or m.get("vol")) +
            _safe_float(m.get("open_interest") or m.get("openInterest") or m.get("oi"))
        )
        candidates.append((score, ct.timestamp(), m, secs_left))

    if not candidates:
        return None

    # Sort: prefer high score (liquid), break ties by earliest close (current bucket)
    candidates.sort(key=lambda x: (x[0], -x[1]), reverse=True)
    best = candidates[0]
    market = dict(best[2])
    market["secs_left"] = best[3]
    return market

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from clawbot import config


def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def current_btc_5m_slug(now: Optional[datetime] = None) -> str:
    now = now or utc_now_dt()
    floored = now.replace(second=0, microsecond=0)
    minute = (floored.minute // 5) * 5
    floored = floored.replace(minute=minute)
    return f"btc-updown-5m-{int(floored.timestamp())}"


def _sleep_backoff(attempt: int):
    time.sleep(1.0 * attempt)


def _safe_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _maybe_json_list(x):
    if x is None:
        return None
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                return json.loads(s)
            except Exception:
                return None
        if "," in s:
            return [p.strip().strip('"').strip("'") for p in s.split(",") if p.strip()]
        return [s]
    return None


def fetch_btc_closes_5m(lookback: int) -> Optional[List[float]]:
    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            r = requests.get(
                config.KRAKEN_OHLC_URL,
                params={"pair": config.KRAKEN_PAIR, "interval": config.KRAKEN_INTERVAL},
                timeout=config.TIMEOUT_SEC,
            )
            if r.status_code != 200:
                _sleep_backoff(attempt)
                continue
            data = r.json()
            if data.get("error"):
                _sleep_backoff(attempt)
                continue
            result = data.get("result", {})
            pair_key = next((k for k in result if k != "last"), None)
            candles = result.get(pair_key, [])
            if not isinstance(candles, list) or len(candles) < lookback:
                _sleep_backoff(attempt)
                continue
            return [float(c[4]) for c in candles[-lookback:]]
        except Exception:
            _sleep_backoff(attempt)
    return None


def _extract_updown_from_market(m: dict) -> Optional[Dict[str, Any]]:
    outcomes = _maybe_json_list(m.get("outcomes")) or _maybe_json_list(m.get("outcomeNames"))
    token_ids = _maybe_json_list(m.get("clobTokenIds")) or _maybe_json_list(m.get("clobTokenIDs"))
    prices = _maybe_json_list(m.get("outcomePrices"))
    if not isinstance(outcomes, list) or len(outcomes) < 2:
        return None

    norm = [str(o).strip().lower() for o in outcomes]
    token_map: Dict[str, str] = {}
    if isinstance(token_ids, list) and len(token_ids) >= 2:
        for name, tid in zip(norm, token_ids):
            token_map[name] = str(tid)

    gamma_map: Dict[str, float] = {}
    if isinstance(prices, list) and len(prices) >= 2:
        for name, px in zip(norm, prices):
            f = _safe_float(px)
            if f is not None:
                gamma_map[name] = f

    return {
        "up_tid": token_map.get("up") or token_map.get("yes"),
        "dn_tid": token_map.get("down") or token_map.get("no"),
        "up_gamma": gamma_map.get("up") or gamma_map.get("yes"),
        "dn_gamma": gamma_map.get("down") or gamma_map.get("no"),
    }


def _get_market_by_slug(slug: str):
    r = requests.get(config.GAMMA_MARKETS_URL, params={"slug": slug}, timeout=config.POLY_TIMEOUT_SEC)
    if r.status_code != 200:
        return None
    data = r.json()
    if isinstance(data, list) and data:
        return data[0]
    if isinstance(data, dict):
        return data
    return None


def _fetch_clob_price(token_id: str) -> float:
    r = requests.get(config.CLOB_PRICE_URL, params={"token_id": token_id, "side": "BUY"}, timeout=config.POLY_TIMEOUT_SEC)
    r.raise_for_status()
    return float(r.json()["price"])


def fetch_polymarket_marks(slug: str) -> Optional[Tuple[float, float, str]]:
    m = _get_market_by_slug(slug)
    if not isinstance(m, dict):
        return None
    info = _extract_updown_from_market(m)
    if not info:
        return None

    up_tid, dn_tid = info.get("up_tid"), info.get("dn_tid")
    if up_tid and dn_tid:
        try:
            p_up = _fetch_clob_price(up_tid)
            p_dn = _fetch_clob_price(dn_tid)
            if abs((p_up + p_dn) - 1.0) <= 0.15:
                return p_up, p_dn, "clob"
        except Exception:
            pass

    up_g, dn_g = info.get("up_gamma"), info.get("dn_gamma")
    if up_g is not None and dn_g is not None:
        return float(up_g), float(dn_g), "gamma"
    return None

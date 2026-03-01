#!/usr/bin/env python3
"""
Johnny 5 — Kalshi KXBTC15M trader (15-minute BTC up/down)

- No Bun. No Polymarket.
- Auto-discovers the currently-open market in series KXBTC15M.
- Computes best YES/NO bid and implied asks from bids-only orderbook.
- Simple deterministic "edge" model based on recent BTC momentum + volatility.
- Trades at most once per market window. Uses IOC limit orders.
- Supports PAPER and LIVE (gated by LIVE_ARMED + KILL_SWITCH).

Kalshi Auth:
- Headers: KALSHI-ACCESS-KEY, KALSHI-ACCESS-TIMESTAMP, KALSHI-ACCESS-SIGNATURE
- Signature: base64(RSA-PSS-SHA256( timestamp + METHOD + path_without_query ))  (docs)
"""

import base64
import datetime as dt
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# Optional DB (safe if DATABASE_URL is not set)
try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # type: ignore


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("johnny5_kalshi")


# ----------------------------
# Config
# ----------------------------
RUN_MODE = os.getenv("RUN_MODE", "PAPER").upper()          # PAPER or LIVE
LIVE_MODE = RUN_MODE == "LIVE"
LIVE_ARMED = os.getenv("LIVE_ARMED", "false").lower() in ("1", "true", "yes", "y")
KILL_SWITCH = os.getenv("KILL_SWITCH", "true").lower() in ("1", "true", "yes", "y")

# Kalshi API
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
KALSHI_PRIVATE_KEY_B64 = os.getenv("KALSHI_PRIVATE_KEY_B64", "").strip()
KALSHI_PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY_PEM", "").strip()
KALSHI_BASE_URL = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com").rstrip("/")
KALSHI_API_PREFIX = "/trade-api/v2"

# Market selection
SERIES_TICKER = os.getenv("SERIES_TICKER", "KXBTC15M").strip()
# Optional override: if you provide it, we’ll trade only this ticker.
MARKET_TICKER_OVERRIDE = os.getenv("MARKET_TICKER", "").strip()

# Timing
RUN_LOOP = os.getenv("RUN_LOOP", "true").lower() in ("1", "true", "yes", "y")
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "5"))              # poll interval
WINDOW_COOLDOWN_SECONDS = int(os.getenv("WINDOW_COOLDOWN_SECONDS", "30"))  # avoid flapping at rollover

# Risk / sizing
MAX_COST_PER_TRADE_USD = float(os.getenv("MAX_COST_PER_TRADE_USD", "2.00"))   # max spend per 15-min window
MIN_CONTRACTS = int(os.getenv("MIN_CONTRACTS", "1"))
MAX_CONTRACTS = int(os.getenv("MAX_CONTRACTS", "50"))

MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "200"))  # hard cap safety

# Execution style
ORDER_TYPE = os.getenv("ORDER_TYPE", "IOC").upper()     # IOC recommended (no resting orders)
SLIPPAGE_CENTS = int(os.getenv("SLIPPAGE_CENTS", "1"))  # pay 1¢ worse than best ask to reduce partial fills

# Market quality filters (cents)
MAX_SPREAD_CENTS = int(os.getenv("MAX_SPREAD_CENTS", "12"))  # if too wide, skip
MIN_TOP_QTY = int(os.getenv("MIN_TOP_QTY", "3"))             # min qty at best price level

# Edge model controls
EDGE_THRESHOLD = float(os.getenv("EDGE_THRESHOLD", "0.03"))  # 3% edge before trading
MOMENTUM_MINUTES = int(os.getenv("MOMENTUM_MINUTES", "10"))
VOL_MINUTES = int(os.getenv("VOL_MINUTES", "60"))
MODEL_K = float(os.getenv("MODEL_K", "1.6"))                 # logistic scale for z-score
MODEL_BIAS = float(os.getenv("MODEL_BIAS", "0.0"))           # shift probability up/down

# Pricing source
BTC_PRICE_SOURCE = os.getenv("BTC_PRICE_SOURCE", "coinbase").lower()  # coinbase or kraken

# State storage
DB_STATE_TABLE = os.getenv("DB_STATE_TABLE", "j5_state_kalshi")
DB_KEY = os.getenv("DB_KEY", f"{SERIES_TICKER}_default")

# HTTP tuning
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "2"))
RETRY_SLEEP = float(os.getenv("RETRY_SLEEP", "0.35"))


# ----------------------------
# Data structures
# ----------------------------
@dataclass
class OrderbookTop:
    yes_bid: Optional[int]   # best YES bid (cents)
    yes_bid_qty: int
    no_bid: Optional[int]    # best NO bid (cents)
    no_bid_qty: int

    @property
    def yes_ask(self) -> Optional[int]:
        # bids-only book: NO bid at p implies YES ask at 100 - p :contentReference[oaicite:2]{index=2}
        if self.no_bid is None:
            return None
        return 100 - self.no_bid

    @property
    def no_ask(self) -> Optional[int]:
        # YES bid at p implies NO ask at 100 - p :contentReference[oaicite:3]{index=3}
        if self.yes_bid is None:
            return None
        return 100 - self.yes_bid


# ----------------------------
# Small utilities
# ----------------------------
def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def ts_ms() -> str:
    return str(int(time.time() * 1000))

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def jitter_sleep(seconds: float) -> None:
    # tiny jitter avoids thundering herd if deployed multiple times
    time.sleep(max(0.0, seconds + (0.05 * (2.0 * (time.time() % 1) - 1.0))))

def http_get(url: str, headers: Optional[dict] = None, params: Optional[dict] = None) -> Tuple[int, Any]:
    last_exc = None
    for _ in range(max(1, HTTP_RETRIES + 1)):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
            ct = (r.headers.get("content-type") or "").lower()
            if "application/json" in ct:
                return r.status_code, r.json()
            try:
                return r.status_code, r.json()
            except Exception:
                return r.status_code, r.text
        except Exception as e:
            last_exc = e
            time.sleep(RETRY_SLEEP)
    raise RuntimeError(f"HTTP GET failed after retries: {url} exc={last_exc}")

def http_post(url: str, headers: Optional[dict] = None, payload: Optional[dict] = None) -> Tuple[int, Any]:
    last_exc = None
    for _ in range(max(1, HTTP_RETRIES + 1)):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=HTTP_TIMEOUT)
            ct = (r.headers.get("content-type") or "").lower()
            if "application/json" in ct:
                return r.status_code, r.json()
            try:
                return r.status_code, r.json()
            except Exception:
                return r.status_code, r.text
        except Exception as e:
            last_exc = e
            time.sleep(RETRY_SLEEP)
    raise RuntimeError(f"HTTP POST failed after retries: {url} exc={last_exc}")


# ----------------------------
# DB state (optional)
# ----------------------------
def db_conn():
    dsn = os.getenv("DATABASE_URL", "").strip()
    if not dsn or psycopg2 is None:
        return None
    return psycopg2.connect(dsn, sslmode="require")

def db_init() -> None:
    conn = db_conn()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {DB_STATE_TABLE} (
                    k TEXT PRIMARY KEY,
                    v JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
    finally:
        conn.close()

def db_get_state() -> Dict[str, Any]:
    conn = db_conn()
    if conn is None:
        return {}
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"SELECT v FROM {DB_STATE_TABLE} WHERE k=%s", (DB_KEY,))
            row = cur.fetchone()
            if not row:
                return {}
            return dict(row["v"])
    finally:
        conn.close()

def db_set_state(state: Dict[str, Any]) -> None:
    conn = db_conn()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {DB_STATE_TABLE} (k, v)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=now();
                """,
                (DB_KEY, json.dumps(state)),
            )
    finally:
        conn.close()


# ----------------------------
# Kalshi Auth
# ----------------------------
def load_private_key():
    if KALSHI_PRIVATE_KEY_B64:
        pem_bytes = base64.b64decode(KALSHI_PRIVATE_KEY_B64.encode("utf-8"))
        return serialization.load_pem_private_key(pem_bytes, password=None, backend=default_backend())
    if KALSHI_PRIVATE_KEY_PEM:
        return serialization.load_pem_private_key(KALSHI_PRIVATE_KEY_PEM.encode("utf-8"), password=None, backend=default_backend())
    raise RuntimeError("Missing KALSHI_PRIVATE_KEY_B64 or KALSHI_PRIVATE_KEY_PEM")

def sign_request(private_key, timestamp_ms: str, method: str, path: str) -> str:
    # Must sign the path without query params :contentReference[oaicite:4]{index=4}
    path_no_q = path.split("?", 1)[0]
    message = f"{timestamp_ms}{method.upper()}{path_no_q}".encode("utf-8")
    sig = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")

class KalshiClient:
    def __init__(self):
        if not KALSHI_API_KEY_ID:
            raise RuntimeError("Missing KALSHI_API_KEY_ID")
        self.private_key = load_private_key()

    def _headers(self, method: str, path: str) -> Dict[str, str]:
        t = ts_ms()
        sig = sign_request(self.private_key, t, method, path)
        return {
            "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-TIMESTAMP": t,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "Content-Type": "application/json",
        }

    def get(self, path: str, params: Optional[dict] = None) -> Tuple[int, Any]:
        url = f"{KALSHI_BASE_URL}{path}"
        headers = self._headers("GET", path)
        return http_get(url, headers=headers, params=params)

    def post(self, path: str, payload: dict) -> Tuple[int, Any]:
        url = f"{KALSHI_BASE_URL}{path}"
        headers = self._headers("POST", path)
        return http_post(url, headers=headers, payload=payload)


# ----------------------------
# Market discovery + data
# ----------------------------
def get_open_markets_for_series(client: KalshiClient, series_ticker: str) -> List[Dict[str, Any]]:
    path = f"{KALSHI_API_PREFIX}/markets"
    params = {"series_ticker": series_ticker, "status": "open", "limit": 200}
    code, data = client.get(path, params=params)
    if code != 200:
        raise RuntimeError(f"Get markets failed code={code} data={data}")
    markets = data.get("markets") or []
    if not isinstance(markets, list):
        return []
    return markets

def pick_current_market(markets: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Choose the open market that closes soonest in the future (nearest close_time).
    This aligns with trading the "current" 15-minute window.
    """
    now = utcnow()
    best = None
    best_close = None

    for m in markets:
        close_time = m.get("close_time") or m.get("close_ts") or m.get("expiration_time")
        if not close_time:
            continue
        # close_time is usually ISO like "2026-03-01T16:45:00Z"
        try:
            if isinstance(close_time, str):
                ct = dt.datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            else:
                # if unix seconds
                ct = dt.datetime.fromtimestamp(int(close_time), tz=dt.timezone.utc)
        except Exception:
            continue

        if ct <= now:
            continue
        if best_close is None or ct < best_close:
            best = m
            best_close = ct

    return best

def get_orderbook_top(client: KalshiClient, ticker: str) -> OrderbookTop:
    path = f"{KALSHI_API_PREFIX}/markets/{ticker}/orderbook"
    code, data = client.get(path)
    if code != 200:
        raise RuntimeError(f"Orderbook failed ticker={ticker} code={code} data={data}")

    ob = (data or {}).get("orderbook") or {}
    yes = ob.get("yes") or []
    no = ob.get("no") or []

    def best_level(levels: Any) -> Tuple[Optional[int], int]:
        # API returns levels like [[price_cents, qty], ...]
        if not isinstance(levels, list) or not levels:
            return None, 0
        try:
            lvl = levels[0]
            price = int(lvl[0])
            qty = int(lvl[1]) if len(lvl) > 1 else 0
            return price, qty
        except Exception:
            return None, 0

    yes_bid, yes_qty = best_level(yes)
    no_bid, no_qty = best_level(no)

    return OrderbookTop(
        yes_bid=yes_bid,
        yes_bid_qty=yes_qty,
        no_bid=no_bid,
        no_bid_qty=no_qty,
    )

def get_balance_cents(client: KalshiClient) -> int:
    path = f"{KALSHI_API_PREFIX}/portfolio/balance"
    code, data = client.get(path)
    if code != 200:
        raise RuntimeError(f"Balance failed code={code} data={data}")
    # docs: returns cents :contentReference[oaicite:5]{index=5}
    return int((data or {}).get("balance") or 0)

def get_recent_fills(client: KalshiClient, ticker: str, min_ts: Optional[int] = None) -> List[Dict[str, Any]]:
    path = f"{KALSHI_API_PREFIX}/portfolio/fills"
    params = {"ticker": ticker, "limit": 50}
    if min_ts is not None:
        params["min_ts"] = int(min_ts)
    code, data = client.get(path, params=params)
    if code != 200:
        raise RuntimeError(f"Fills failed code={code} data={data}")
    fills = (data or {}).get("fills") or []
    if not isinstance(fills, list):
        return []
    return fills


# ----------------------------
# BTC price (simple signal)
# ----------------------------
def fetch_btc_spot_usd() -> float:
    if BTC_PRICE_SOURCE == "kraken":
        # Kraken public ticker: https://api.kraken.com/0/public/Ticker?pair=XBTUSD
        code, data = http_get("https://api.kraken.com/0/public/Ticker", params={"pair": "XBTUSD"})
        if code != 200:
            raise RuntimeError(f"Kraken price failed code={code} data={data}")
        # data["result"] has one key like "XXBTZUSD"
        result = (data or {}).get("result") or {}
        if not isinstance(result, dict) or not result:
            raise RuntimeError(f"Kraken result missing: {data}")
        k = list(result.keys())[0]
        last = float(result[k]["c"][0])
        return last

    # Default: Coinbase spot
    # https://api.coinbase.com/v2/prices/BTC-USD/spot
    code, data = http_get("https://api.coinbase.com/v2/prices/BTC-USD/spot")
    if code != 200:
        raise RuntimeError(f"Coinbase price failed code={code} data={data}")
    amt = float(((data or {}).get("data") or {}).get("amount"))
    return amt

def update_price_history(state: Dict[str, Any], price: float) -> None:
    hist = state.get("btc_prices") or []
    if not isinstance(hist, list):
        hist = []
    hist.append({"t": int(time.time()), "p": price})
    # keep last 4 hours (enough for momentum/vol windows)
    cutoff = int(time.time()) - 4 * 3600
    hist = [x for x in hist if int(x.get("t", 0)) >= cutoff]
    state["btc_prices"] = hist

def compute_momentum_vol_prob(state: Dict[str, Any]) -> float:
    """
    Deterministic signal:
    - momentum over MOMENTUM_MINUTES
    - realized vol over VOL_MINUTES
    - z = momentum / (vol + eps)
    - p_yes = sigmoid(MODEL_K * z + MODEL_BIAS)
    """
    hist = state.get("btc_prices") or []
    if not isinstance(hist, list) or len(hist) < 5:
        return 0.5

    now_t = int(time.time())
    def price_at(age_sec: int) -> Optional[float]:
        target = now_t - age_sec
        # find closest point at or before target
        candidates = [x for x in hist if int(x.get("t", 0)) <= target]
        if not candidates:
            return None
        candidates.sort(key=lambda x: int(x.get("t", 0)))
        return float(candidates[-1]["p"])

    p_now = float(hist[-1]["p"])
    p_m = price_at(MOMENTUM_MINUTES * 60)
    if p_m is None or p_m <= 0:
        return 0.5
    mom = (p_now - p_m) / p_m  # fractional

    # vol: stdev of minute-to-minute returns over VOL_MINUTES
    window_sec = VOL_MINUTES * 60
    cutoff = now_t - window_sec
    w = [x for x in hist if int(x.get("t", 0)) >= cutoff]
    if len(w) < 10:
        return 0.5

    # compute simple returns between successive points
    rets = []
    for i in range(1, len(w)):
        p0 = float(w[i - 1]["p"])
        p1 = float(w[i]["p"])
        if p0 > 0:
            rets.append((p1 - p0) / p0)
    if len(rets) < 5:
        return 0.5
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, (len(rets) - 1))
    vol = math.sqrt(var)

    z = mom / (vol + 1e-9)
    x = (MODEL_K * z) + MODEL_BIAS
    p_yes = 1.0 / (1.0 + math.exp(-x))
    return clamp(p_yes, 0.01, 0.99)


# ----------------------------
# Decision + execution
# ----------------------------
def compute_edges(book: OrderbookTop, p_yes_model: float) -> Dict[str, Any]:
    """
    Compare our model probability vs market price.

    We estimate market "price" as the best ask for the side we would buy.
    If we buy YES, we pay yes_ask cents (derived from NO bids).
    If we buy NO, we pay no_ask cents (derived from YES bids).

    Edge definitions:
      edge_yes = p_yes_model - (yes_ask/100)
      edge_no  = (1-p_yes_model) - (no_ask/100)
    """
    yes_ask = book.yes_ask
    no_ask = book.no_ask

    out = {
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "yes_bid": book.yes_bid,
        "no_bid": book.no_bid,
        "spread_yes": None,
        "spread_no": None,
        "edge_yes": None,
        "edge_no": None,
    }

    if yes_ask is not None and book.yes_bid is not None:
        out["spread_yes"] = int(yes_ask - book.yes_bid)
    if no_ask is not None and book.no_bid is not None:
        out["spread_no"] = int(no_ask - book.no_bid)

    if yes_ask is not None:
        out["edge_yes"] = float(p_yes_model - (yes_ask / 100.0))
    if no_ask is not None:
        out["edge_no"] = float((1.0 - p_yes_model) - (no_ask / 100.0))

    return out

def market_quality_ok(book: OrderbookTop, edges: Dict[str, Any]) -> bool:
    # Need both bids to infer both asks, otherwise it's too illiquid to trade safely
    if book.yes_bid is None or book.no_bid is None:
        return False
    if book.yes_bid_qty < MIN_TOP_QTY or book.no_bid_qty < MIN_TOP_QTY:
        return False

    # Spread check: use YES spread as proxy (it’s symmetric)
    spread_yes = edges.get("spread_yes")
    if spread_yes is None:
        return False
    if int(spread_yes) > MAX_SPREAD_CENTS:
        return False

    # Ensure asks are within 1..99
    if book.yes_ask is None or book.no_ask is None:
        return False
    if not (1 <= book.yes_ask <= 99 and 1 <= book.no_ask <= 99):
        return False

    return True

def choose_trade(edges: Dict[str, Any]) -> Tuple[str, float]:
    """
    Returns (side_to_buy, edge_value) where side_to_buy is "yes" or "no".
    Picks the larger positive edge.
    """
    ey = edges.get("edge_yes")
    en = edges.get("edge_no")
    ey = float(ey) if ey is not None else -999.0
    en = float(en) if en is not None else -999.0

    if ey >= en:
        return "yes", ey
    return "no", en

def compute_order_price(side: str, book: OrderbookTop) -> int:
    """
    We buy at (best ask + slippage) to improve fill probability.
    yes_ask/no_ask derived from bids-only book.
    """
    if side == "yes":
        if book.yes_ask is None:
            raise RuntimeError("Missing yes_ask")
        return int(clamp(book.yes_ask + SLIPPAGE_CENTS, 1, 99))
    else:
        if book.no_ask is None:
            raise RuntimeError("Missing no_ask")
        return int(clamp(book.no_ask + SLIPPAGE_CENTS, 1, 99))

def compute_contract_count(price_cents: int) -> int:
    # cost per contract ~= price_cents/100 dollars (fees ignored for sizing simplicity)
    if price_cents <= 0:
        return MIN_CONTRACTS
    max_by_cost = int(MAX_COST_PER_TRADE_USD / (price_cents / 100.0))
    return int(clamp(max_by_cost, MIN_CONTRACTS, MAX_CONTRACTS))

def place_order(client: KalshiClient, ticker: str, side: str, price_cents: int, count: int) -> Dict[str, Any]:
    path = f"{KALSHI_API_PREFIX}/portfolio/orders"
    payload = {
        "ticker": ticker,
        "side": side,          # "yes" or "no"
        "action": "buy",
        "type": ORDER_TYPE,    # IOC
        "count": int(count),
    }
    # price field depends on side
    if side == "yes":
        payload["yes_price"] = int(price_cents)
    else:
        payload["no_price"] = int(price_cents)

    code, data = client.post(path, payload)
    if code not in (200, 201):
        raise RuntimeError(f"Create order failed code={code} data={data}")
    return data


# ----------------------------
# Main loop logic
# ----------------------------
def ensure_safety() -> None:
    if LIVE_MODE and not LIVE_ARMED:
        log.warning("LIVE mode selected but LIVE_ARMED is false — will not place orders.")
    if LIVE_MODE and KILL_SWITCH:
        log.warning("KILL_SWITCH is TRUE — will not place orders.")
    if not LIVE_MODE:
        log.info("RUN_MODE=PAPER — no real orders will be placed.")

def state_defaults(state: Dict[str, Any]) -> Dict[str, Any]:
    state.setdefault("last_market_ticker", "")
    state.setdefault("last_trade_market_ticker", "")
    state.setdefault("last_trade_ts", 0)
    state.setdefault("trades_today", 0)
    state.setdefault("trades_day", utcnow().date().isoformat())
    state.setdefault("btc_prices", [])
    state.setdefault("last_rollover_ts", 0)
    return state

def reset_daily_counters_if_needed(state: Dict[str, Any]) -> None:
    today = utcnow().date().isoformat()
    if state.get("trades_day") != today:
        state["trades_day"] = today
        state["trades_today"] = 0

def main_once(client: KalshiClient, state: Dict[str, Any]) -> Dict[str, Any]:
    reset_daily_counters_if_needed(state)

    # Update BTC price history for model
    btc = fetch_btc_spot_usd()
    update_price_history(state, btc)

    # Choose market ticker
    ticker = MARKET_TICKER_OVERRIDE
    market_meta = None

    if not ticker:
        markets = get_open_markets_for_series(client, SERIES_TICKER)
        market_meta = pick_current_market(markets)
        if not market_meta:
            log.warning("No open markets found for series=%s", SERIES_TICKER)
            return state
        ticker = str(market_meta.get("ticker") or "").strip()
        if not ticker:
            log.warning("Open market missing ticker in response.")
            return state

    # Rollover handling: if market ticker changed, pause briefly so we don't double-fire at boundary
    if ticker != state.get("last_market_ticker"):
        state["last_market_ticker"] = ticker
        state["last_rollover_ts"] = int(time.time())
        log.info("Market rollover -> current ticker=%s", ticker)

    if int(time.time()) - int(state.get("last_rollover_ts") or 0) < WINDOW_COOLDOWN_SECONDS:
        log.info("Cooldown at rollover (%ss) — waiting.", WINDOW_COOLDOWN_SECONDS)
        return state

    # Pull orderbook top
    book = get_orderbook_top(client, ticker)
    p_yes = compute_momentum_vol_prob(state)
    edges = compute_edges(book, p_yes)

    # Log a compact snapshot
    log.info(
        "snap | ticker=%s | btc=%.2f | p_yes=%.3f | yes_bid=%s(%s) yes_ask=%s | no_bid=%s(%s) no_ask=%s | edge_yes=%s edge_no=%s",
        ticker,
        btc,
        p_yes,
        book.yes_bid,
        book.yes_bid_qty,
        book.yes_ask,
        book.no_bid,
        book.no_bid_qty,
        book.no_ask,
        None if edges["edge_yes"] is None else f"{edges['edge_yes']:.3f}",
        None if edges["edge_no"] is None else f"{edges['edge_no']:.3f}",
    )

    # Safety caps
    if int(state.get("trades_today", 0)) >= MAX_TRADES_PER_DAY:
        log.warning("Daily trade cap reached: %s", MAX_TRADES_PER_DAY)
        return state

    # Already traded this market?
    if state.get("last_trade_market_ticker") == ticker:
        return state

    # Market quality filter
    if not market_quality_ok(book, edges):
        log.info("Skip: market quality filter failed (spread/liq).")
        return state

    # Choose side and edge
    side, edge = choose_trade(edges)
    if edge < EDGE_THRESHOLD:
        log.info("Skip: edge %.3f < threshold %.3f", edge, EDGE_THRESHOLD)
        return state

    price_cents = compute_order_price(side, book)
    count = compute_contract_count(price_cents)

    # PAPER mode: record intent only
    if not LIVE_MODE or not LIVE_ARMED or KILL_SWITCH:
        log.info(
            "PAPER_TRADE | would BUY %s | ticker=%s | price=%sc | count=%s | edge=%.3f",
            side.upper(),
            ticker,
            price_cents,
            count,
            edge,
        )
        state["last_trade_market_ticker"] = ticker
        state["last_trade_ts"] = int(time.time())
        state["trades_today"] = int(state.get("trades_today", 0)) + 1
        return state

    # LIVE: check balance then place
    bal_cents = get_balance_cents(client)
    est_cost_cents = int(price_cents * count)
    if est_cost_cents > bal_cents:
        log.warning("Insufficient balance: need~%sc, have=%sc", est_cost_cents, bal_cents)
        return state

    resp = place_order(client, ticker, side, price_cents, count)
    log.info("LIVE_ORDER_OK | %s", json.dumps(resp, default=str))

    state["last_trade_market_ticker"] = ticker
    state["last_trade_ts"] = int(time.time())
    state["trades_today"] = int(state.get("trades_today", 0)) + 1
    return state


def main():
    db_init()

    ensure_safety()

    client = KalshiClient()
    state = state_defaults(db_get_state())

    # Quick sanity call: balance
    try:
        bal = get_balance_cents(client)
        log.info("KALSHI_OK | balance=$%.2f", bal / 100.0)
    except Exception as e:
        log.error("Kalshi auth/balance failed: %s", e)
        raise

    if not RUN_LOOP:
        new_state = main_once(client, state)
        db_set_state(new_state)
        return

    log.info("Looping enabled: poll every %ss", POLL_SECONDS)

    while True:
        try:
            state = state_defaults(db_get_state())
            new_state = main_once(client, state)
            db_set_state(new_state)
        except Exception as e:
            log.exception("Loop error: %s", e)
        jitter_sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

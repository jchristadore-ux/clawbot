# bot.py — Johnny 5 (Polymarket rolling 5m markets)
#
# What this version fixes (based on your logs):
# 1) Works with your EXISTING bot_state table that has NOT NULL columns (id, key).
#    - We never insert NULL id/key.
#    - We store state under key='main'.
# 2) Resolves rolling 5m slugs by scanning recent buckets until we find tokens with live CLOB books.
# 3) Robust token extraction for Yes/No and Up/Down outcome naming + multiple Gamma shapes.
# 4) CLOB pricing: tries /midpoint; if missing, falls back to /book best bid/ask; handles empty books cleanly.
# 5) Bun health check no longer hardcodes /__ping; configurable via BUN_HEALTH_PATH (default "/health", fallback "/").
#
# Required env:
#   DATABASE_URL=...
#   POLY_MARKET_SLUG=btc-updown-5m
#
# Recommended env:
#   POLY_GAMMA_HOST=https://gamma-api.polymarket.com
#   POLY_CLOB_HOST=https://clob.polymarket.com
#
# Live/trading env (optional):
#   RUN_MODE=DRY_RUN | PAPER | LIVE
#   LIVE_TRADING_ENABLED=true/false
#   KILL_SWITCH=true/false
#   BUN_BASE_URL=https://...      (your Bun order gateway)
#   BUN_HEALTH_PATH=/health       (or whatever your server supports)
#
# Knobs:
#   LOOKBACK_BUCKETS=24           (24*5m = 2 hours)
#   LOOP_SECONDS=30
#   RUN_LOOP=true/false
#   EDGE_ENTER=0.10
#   MAX_TRADES_PER_DAY=60
#   LIVE_TRADE_SIZE=1.0
#   PAPER_BALANCE=0

import os
import json
import time
import logging
from datetime import datetime, timezone, date
from typing import Optional, Dict, Any, Tuple, List

import requests
import psycopg2


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("johnny5")


# ----------------------------
# Helpers
# ----------------------------
def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "t", "yes", "y", "on")


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def five_min_bucket_epoch(ts: Optional[float] = None) -> int:
    if ts is None:
        ts = time.time()
    return int(ts // 300) * 300


def http_get_json(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: int = 20,
) -> Tuple[int, Optional[dict], str]:
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        text = r.text[:4000] if r.text else ""
        try:
            j = r.json()
        except Exception:
            j = None
        return r.status_code, j, text
    except Exception as e:
        return 0, None, str(e)


def http_post_json(
    url: str,
    payload: dict,
    headers: Optional[dict] = None,
    timeout: int = 25,
) -> Tuple[int, Optional[dict], str]:
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=timeout)
        text = r.text[:4000] if r.text else ""
        try:
            j = r.json()
        except Exception:
            j = None
        return r.status_code, j, text
    except Exception as e:
        return 0, None, str(e)


# ----------------------------
# DB
# ----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL")
    # Railway commonly requires SSL
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def ensure_tables() -> None:
    """
    Your existing DB already has a bot_state table with NOT NULL constraints on:
      - id
      - key
    and likely other columns.
    We DO NOT try to recreate/alter that table. We only ensure a row exists for key='main'
    without inserting NULL id.

    We also create equity_snapshots if missing (safe, new table).
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Safe new table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS equity_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    price DOUBLE PRECISION NOT NULL,
                    balance DOUBLE PRECISION NOT NULL,
                    position TEXT,
                    entry_price DOUBLE PRECISION,
                    stake DOUBLE PRECISION,
                    unrealized_pnl DOUBLE PRECISION NOT NULL,
                    equity DOUBLE PRECISION NOT NULL,
                    poly_slug TEXT
                );
                """
            )

            # Touch existing row if it exists
            cur.execute(
                """
                UPDATE bot_state
                SET as_of_date = COALESCE(as_of_date, CURRENT_DATE)
                WHERE key = 'main';
                """
            )

            # If absent, insert with id=max(id)+1 (never NULL)
            if cur.rowcount == 0:
                # NOTE: these column names must exist in your bot_state table.
                # If your bot_state uses different names, you'll see a "column does not exist" error.
                cur.execute(
                    """
                    INSERT INTO bot_state (
                        id, key, as_of_date, position, entry_price, stake, trades_today, pnl_today_realized
                    )
                    SELECT
                        COALESCE(MAX(id), 0) + 1,
                        'main',
                        CURRENT_DATE,
                        NULL,
                        NULL,
                        0,
                        0,
                        0
                    FROM bot_state;
                    """
                )

        conn.commit()


def load_state() -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT as_of_date, position, entry_price, stake, trades_today, pnl_today_realized
                FROM bot_state
                WHERE key='main';
                """
            )
            row = cur.fetchone()

    if not row:
        return {
            "as_of_date": date.today(),
            "position": None,
            "entry_price": None,
            "stake": 0.0,
            "trades_today": 0,
            "pnl_today_realized": 0.0,
        }

    return {
        "as_of_date": row[0],
        "position": row[1],
        "entry_price": row[2],
        "stake": float(row[3] or 0.0),
        "trades_today": int(row[4] or 0),
        "pnl_today_realized": float(row[5] or 0.0),
    }


def save_state(state: Dict[str, Any]) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE bot_state
                SET as_of_date=%s,
                    position=%s,
                    entry_price=%s,
                    stake=%s,
                    trades_today=%s,
                    pnl_today_realized=%s,
                    updated_at=NOW()
                WHERE key='main';
                """,
                (
                    state["as_of_date"],
                    state["position"],
                    state["entry_price"],
                    state["stake"],
                    state["trades_today"],
                    state["pnl_today_realized"],
                ),
            )
        conn.commit()


def record_equity_snapshot(
    price: float,
    balance: float,
    position: Optional[str],
    entry_price: Optional[float],
    stake: float,
    unrealized_pnl: float,
    equity: float,
    poly_slug: str,
) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO equity_snapshots (price, balance, position, entry_price, stake, unrealized_pnl, equity, poly_slug)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                (price, balance, position, entry_price, stake, unrealized_pnl, equity, poly_slug),
            )
        conn.commit()


# ----------------------------
# Gamma / CLOB
# ----------------------------
POLY_GAMMA_HOST = os.getenv("POLY_GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
POLY_CLOB_HOST = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").strip()

# BASE rolling slug prefix (required): e.g. "btc-updown-5m"
POLY_MARKET_SLUG = os.getenv("POLY_MARKET_SLUG", "").strip()


def _maybe_json(x):
    if x is None:
        return None
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None
    return None


def fetch_gamma_market_by_slug(slug: str) -> Optional[dict]:
    url = f"{POLY_GAMMA_HOST}/markets/slug/{slug}"
    code, j, _ = http_get_json(url, timeout=20)
    if code != 200 or not isinstance(j, dict):
        return None
    return j


def _norm_outcome_name(s: str) -> str:
    s = (s or "").strip().upper()
    if s in ("YES", "Y"):
        return "YES"
    if s in ("NO", "N"):
        return "NO"
    # Map Up/Down style markets to YES/NO side semantics
    if s in ("UP", "HIGHER", "ABOVE", "INCREASE", "BULL"):
        return "YES"
    if s in ("DOWN", "LOWER", "BELOW", "DECREASE", "BEAR"):
        return "NO"
    return s


def extract_yes_no_token_ids(market: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Robust extraction for rolling markets where outcomes can be Yes/No or Up/Down.
    Tries multiple Gamma shapes and common field variants.
    Returns (yes_token_id, no_token_id) where "yes" is the UP/YES side.
    """
    # If Gamma wrapped it
    if "market" in market and isinstance(market["market"], dict):
        market = market["market"]

    # 1) Classic: outcomes + clobTokenIds (often JSON-encoded strings)
    outcomes = _maybe_json(market.get("outcomes"))
    token_ids = _maybe_json(market.get("clobTokenIds"))

    if isinstance(outcomes, list) and isinstance(token_ids, list) and len(outcomes) == len(token_ids):
        yes_id, no_id = None, None
        for o, tid in zip(outcomes, token_ids):
            if not isinstance(o, str):
                continue
            side = _norm_outcome_name(o)
            if side == "YES":
                yes_id = str(tid)
            elif side == "NO":
                no_id = str(tid)
        if yes_id and no_id:
            return yes_id, no_id

    # 2) Tokens embedded as objects
    tokens = market.get("tokens") or market.get("outcomeTokens") or market.get("outcome_tokens")
    if isinstance(tokens, str):
        tokens = _maybe_json(tokens)
    if isinstance(tokens, list):
        yes_id, no_id = None, None
        for t in tokens:
            if not isinstance(t, dict):
                continue
            name = t.get("outcome") or t.get("name") or t.get("label") or ""
            tid = t.get("clobTokenId") or t.get("clobTokenID") or t.get("tokenId") or t.get("token_id")
            if tid is None:
                continue
            side = _norm_outcome_name(str(name))
            if side == "YES":
                yes_id = str(tid)
            elif side == "NO":
                no_id = str(tid)
        if yes_id and no_id:
            return yes_id, no_id

    # 3) Fallback variants: tokenIds aligned with outcomes
    for key in ("tokenIds", "token_ids", "clob_token_ids", "clobTokenIDs"):
        maybe = _maybe_json(market.get(key))
        if isinstance(maybe, list) and isinstance(outcomes, list) and len(maybe) == len(outcomes):
            yes_id, no_id = None, None
            for o, tid in zip(outcomes, maybe):
                if not isinstance(o, str):
                    continue
                side = _norm_outcome_name(o)
                if side == "YES":
                    yes_id = str(tid)
                elif side == "NO":
                    no_id = str(tid)
            if yes_id and no_id:
                return yes_id, no_id

    return None, None


def clob_book_exists(token_id: str) -> Tuple[bool, int]:
    """
    Strong existence check: /book returns 200 if the orderbook exists for token_id.
    """
    url = f"{POLY_CLOB_HOST}/book"
    code, _, text = http_get_json(url, params={"token_id": token_id}, timeout=10)
    if code == 200:
        return True, code
    if code not in (404, 400):
        log.warning("CLOB /book unexpected status=%s token_id=%s body=%s", code, token_id, text[:200])
    return False, code


def clob_price(token_id: str) -> Tuple[int, Optional[float], str]:
    """
    Returns (http_status, price_or_none, source).
    Tries /midpoint first; if missing/None, falls back to /book best bid/ask.
    """
    # 1) Try midpoint
    url = f"{POLY_CLOB_HOST}/midpoint"
    code, j, _ = http_get_json(url, params={"token_id": token_id}, timeout=10)

    if code == 200 and isinstance(j, dict):
        mp = j.get("midpoint")
        if mp is None:
            mp = j.get("mid_price")
        if mp is not None:
            try:
                return 200, float(mp), "midpoint"
            except Exception:
                pass  # fallthrough

    # 2) Fallback: orderbook best bid/ask
    url = f"{POLY_CLOB_HOST}/book"
    code2, j2, text2 = http_get_json(url, params={"token_id": token_id}, timeout=10)
    if code2 != 200 or not isinstance(j2, dict):
        return code2, None, f"book_unavailable:{text2[:120]}"

    bids = j2.get("bids") or []
    asks = j2.get("asks") or []

    def _best_px(levels: List[dict], want_max: bool) -> Optional[float]:
        best = None
        for lvl in levels:
            if not isinstance(lvl, dict):
                continue
            p = lvl.get("price")
            if p is None:
                continue
            try:
                pf = float(p)
            except Exception:
                continue
            best = pf if best is None else (max(best, pf) if want_max else min(best, pf))
        return best

    best_bid = _best_px(bids, want_max=True)
    best_ask = _best_px(asks, want_max=False)

    if best_bid is not None and best_ask is not None:
        return 200, (best_bid + best_ask) / 2.0, f"book_mid(bid={best_bid},ask={best_ask})"
    if best_bid is not None:
        return 200, best_bid, f"book_bid({best_bid})"
    if best_ask is not None:
        return 200, best_ask, f"book_ask({best_ask})"

    return 200, None, "book_empty"


def resolve_tradable_rolling_market(
    base_slug: str,
    bucket_epoch: int,
    lookback_buckets: int,
) -> Tuple[Optional[str], Optional[dict], Optional[str], Optional[str]]:

    for i in range(lookback_buckets + 1):
        b = bucket_epoch - (i * 300)
        slug = f"{base_slug}-{b}"

        log.info("Checking slug=%s", slug)

        m = fetch_gamma_market_by_slug(slug)
        if not m:
            log.info("Gamma miss for slug=%s", slug)
            continue

        # Helpful one-line sanity check (keep; low noise)
        log.info(
            "Gamma market: slug=%s enableOrderBook=%s active=%s closed=%s",
            slug, m.get("enableOrderBook"), m.get("active"), m.get("closed"),
        )

        yes_id, no_id = extract_yes_no_token_ids(m)

        if not yes_id or not no_id:
            log.warning(
                "Token extraction failed for slug=%s keys=%s outcomes=%s clobTokenIds=%s",
                slug,
                sorted(list(m.keys()))[:20],
                str(m.get("outcomes"))[:200],
                str(m.get("clobTokenIds"))[:200],
            )
            continue

        y_ok, y_code = clob_book_exists(yes_id)
        n_ok, n_code = clob_book_exists(no_id)

        if y_ok and n_ok:
            log.info("Found tradable slug=%s", slug)
            return slug, m, yes_id, no_id

        log.info(
            "No live books for slug=%s (yes=%s code=%s no=%s code=%s)",
            slug, yes_id, y_code, no_id, n_code
        )

    return None, None, None, None


# ----------------------------
# Bun live order gateway
# ----------------------------
BUN_BASE_URL = os.getenv("BUN_BASE_URL", "").strip()
BUN_HEALTH_PATH = os.getenv("BUN_HEALTH_PATH", "/health").strip() or "/health"


def bun_health() -> Optional[int]:
    if not BUN_BASE_URL:
        return None

    # Try configured path, then fallback to "/"
    for path in (BUN_HEALTH_PATH, "/"):
        try:
            r = requests.get(f"{BUN_BASE_URL}{path}", timeout=8)
            return r.status_code
        except Exception:
            continue
    return None


def bun_place_order(token_id: str, side: str, price: float, size: float) -> Tuple[bool, str]:
    if not BUN_BASE_URL:
        return False, "missing BUN_BASE_URL"
    payload = {
        "token_id": token_id,
        "side": side,          # "BUY" | "SELL"
        "price": float(price),
        "size": float(size),
        "order_type": "GTC",
    }
    code, j, text = http_post_json(f"{BUN_BASE_URL}/order", payload=payload, timeout=20)
    if code != 200 or not isinstance(j, dict) or not j.get("ok"):
        return False, f"bun order failed code={code} body={text[:300]}"
    return True, "ok"


# ----------------------------
# Strategy / Risk (simple baseline)
# ----------------------------
EDGE_ENTER = env_float("EDGE_ENTER", 0.10)
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "60"))
LIVE_TRADE_SIZE = env_float("LIVE_TRADE_SIZE", 1.0)

LOOKBACK_BUCKETS = int(os.getenv("LOOKBACK_BUCKETS", "24"))  # 24*5m = 2 hours


def compute_signal(poly_up: float, fair_up: float) -> Tuple[str, float]:
    """
    edge = fair_up - poly_up
    If edge >= EDGE_ENTER => buy YES (UP side)
    If edge <= -EDGE_ENTER => buy NO (DOWN side)
    else HOLD
    """
    edge = fair_up - poly_up
    if edge >= EDGE_ENTER:
        return "YES", edge
    if edge <= -EDGE_ENTER:
        return "NO", edge
    return "HOLD", edge


# ----------------------------
# Core loop
# ----------------------------
def run_once() -> None:
    ensure_tables()

    run_mode = os.getenv("RUN_MODE", "DRY_RUN").strip().upper()  # DRY_RUN | PAPER | LIVE
    live_mode = run_mode == "LIVE"

    live_trading_enabled = env_bool("LIVE_TRADING_ENABLED", False)
    kill_switch = env_bool("KILL_SWITCH", True)

    live_armed = live_mode and live_trading_enabled and (not kill_switch)

    if not POLY_MARKET_SLUG:
        raise RuntimeError("Missing POLY_MARKET_SLUG (must be base like 'btc-updown-5m')")

    bucket = five_min_bucket_epoch()
    base = POLY_MARKET_SLUG
    log.info("run_mode=%s live_mode=%s live_armed=%s base=%s bucket=%s", run_mode, live_mode, live_armed, base, bucket)

    if BUN_BASE_URL:
        hs = bun_health()
        if hs is not None:
            log.info("bun_health_status=%s", hs)

    # Reset daily counters
    state = load_state()
    today = date.today()
    if state["as_of_date"] != today:
        state["as_of_date"] = today
        state["trades_today"] = 0
        state["pnl_today_realized"] = 0.0
        save_state(state)

    # Resolve a tradable rolling bucket (scan backwards)
    slug, market, yes_token, no_token = resolve_tradable_rolling_market(
        base_slug=base,
        bucket_epoch=bucket,
        lookback_buckets=LOOKBACK_BUCKETS,
    )
    if not slug:
        raise RuntimeError(
            f"Could not find a tradable rolling market with live books in last {LOOKBACK_BUCKETS*5} minutes for base={base}"
        )

    # Get prices (midpoint or book fallback)
    y_code, poly_up, y_src = clob_price(yes_token)
    n_code, poly_down, n_src = clob_price(no_token)

    if poly_up is None or poly_down is None:
        log.warning(
            "Price missing after resolution (yes_code=%s yes_src=%s no_code=%s no_src=%s)",
            y_code, y_src, n_code, n_src
        )
        log.info("BOOT: bot.py finished cleanly")
        return

    # Fair value baseline (replace with your model later)
    fair_up = 0.50

    signal, edge = compute_signal(poly_up, fair_up)

    position = state["position"]       # "YES" | "NO" | None
    entry = state["entry_price"]
    stake = float(state["stake"] or 0.0)
    trades_today = int(state["trades_today"] or 0)
    pnl_today = float(state["pnl_today_realized"] or 0.0)

    action = "NO_TRADE"
    reason = ""

    if trades_today >= MAX_TRADES_PER_DAY:
        action = "NO_TRADE"
        reason = "MAX_TRADES_PER_DAY"
    else:
        if position is None:
            if signal == "YES":
                action = "ENTER_YES"
            elif signal == "NO":
                action = "ENTER_NO"
            else:
                action = "NO_TRADE"
                reason = "signal=HOLD"
        else:
            # Exit if signal flips against our position
            if position == "YES" and signal == "NO":
                action = "EXIT"
            elif position == "NO" and signal == "YES":
                action = "EXIT"
            else:
                action = "NO_TRADE"
                reason = "HOLD_SAME_SIDE" if signal != "HOLD" else "signal=HOLD"

    # Do not enter trades unless armed (in LIVE)
    if live_mode and not live_armed and action.startswith("ENTER"):
        reason = "not_armed" + ("+KILL_SWITCH" if kill_switch else "")
        action = "NO_TRADE"

    # Execute
    if action == "ENTER_YES":
        fill_price = poly_up
        if run_mode == "PAPER":
            state["position"] = "YES"
            state["entry_price"] = fill_price
            state["stake"] = LIVE_TRADE_SIZE
            state["trades_today"] = trades_today + 1
            save_state(state)
        elif run_mode == "LIVE" and live_armed:
            ok, msg = bun_place_order(yes_token, "BUY", fill_price, LIVE_TRADE_SIZE)
            if ok:
                state["position"] = "YES"
                state["entry_price"] = fill_price
                state["stake"] = LIVE_TRADE_SIZE
                state["trades_today"] = trades_today + 1
                save_state(state)
            else:
                action = "NO_TRADE"
                reason = msg

    elif action == "ENTER_NO":
        fill_price = poly_down
        if run_mode == "PAPER":
            state["position"] = "NO"
            state["entry_price"] = fill_price
            state["stake"] = LIVE_TRADE_SIZE
            state["trades_today"] = trades_today + 1
            save_state(state)
        elif run_mode == "LIVE" and live_armed:
            ok, msg = bun_place_order(no_token, "BUY", fill_price, LIVE_TRADE_SIZE)
            if ok:
                state["position"] = "NO"
                state["entry_price"] = fill_price
                state["stake"] = LIVE_TRADE_SIZE
                state["trades_today"] = trades_today + 1
                save_state(state)
            else:
                action = "NO_TRADE"
                reason = msg

    elif action == "EXIT":
        if entry is None:
            # corrupted state; clear it
            state["position"] = None
            state["entry_price"] = None
            state["stake"] = 0.0
            save_state(state)
        else:
            if position == "YES":
                token = yes_token
                exit_price = poly_up
            else:
                token = no_token
                exit_price = poly_down

            realized = (exit_price - float(entry)) * float(stake)

            if run_mode == "PAPER":
                state["position"] = None
                state["entry_price"] = None
                state["stake"] = 0.0
                state["trades_today"] = trades_today + 1
                state["pnl_today_realized"] = pnl_today + realized
                save_state(state)
            elif run_mode == "LIVE" and live_armed:
                ok, msg = bun_place_order(token, "SELL", exit_price, float(stake))
                if ok:
                    state["position"] = None
                    state["entry_price"] = None
                    state["stake"] = 0.0
                    state["trades_today"] = trades_today + 1
                    state["pnl_today_realized"] = pnl_today + realized
                    save_state(state)
                else:
                    action = "NO_TRADE"
                    reason = msg

    # Snapshot/log
    state2 = load_state()
    position2 = state2["position"]
    entry2 = state2["entry_price"]
    stake2 = float(state2["stake"] or 0.0)

    if position2 == "YES" and entry2 is not None:
        u = (poly_up - float(entry2)) * stake2
        mark_price = poly_up
    elif position2 == "NO" and entry2 is not None:
        u = (poly_down - float(entry2)) * stake2
        mark_price = poly_down
    else:
        u = 0.0
        mark_price = poly_up

    # If you have a real balance source, wire it here. For now, allow PAPER_BALANCE.
    balance = float(os.getenv("PAPER_BALANCE", "0").strip() or 0.0)
    equity = balance + u

    log.info(
        "slug=%s | up=%.4f(%s) down=%.4f(%s) | fair_up=%.3f | edge=%+.4f | signal=%s | action=%s%s | pos=%s | entry=%s | trades_today=%d | pnl_today(realized)=%.2f",
        slug,
        poly_up, y_src,
        poly_down, n_src,
        fair_up,
        edge,
        signal,
        action,
        (f" | reason={reason}" if reason else ""),
        position2,
        (None if entry2 is None else round(float(entry2), 6)),
        int(state2["trades_today"] or 0),
        float(state2["pnl_today_realized"] or 0.0),
    )

    record_equity_snapshot(
        price=float(mark_price),
        balance=float(balance),
        position=position2,
        entry_price=(None if entry2 is None else float(entry2)),
        stake=float(stake2),
        unrealized_pnl=float(u),
        equity=float(equity),
        poly_slug=slug,
    )

    log.info("summary | equity=%.2f | uPnL=%.2f | src=clob", equity, u)


def main() -> None:
    log.info("BOOT: bot.py starting")

    run_loop = env_bool("RUN_LOOP", False)
    loop_seconds = int(os.getenv("LOOP_SECONDS", "30"))

    if not run_loop:
        run_once()
        log.info("BOOT: bot.py finished cleanly")
        return

    while True:
        try:
            run_once()
        except Exception as e:
            log.error("Fatal error: %s", e, exc_info=True)
        time.sleep(max(5, loop_seconds))


if __name__ == "__main__":
    main()

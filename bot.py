import os
import json
import time
import logging
import re
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
    """Start epoch of the current 5-minute bucket."""
    if ts is None:
        ts = time.time()
    return int(ts // 300) * 300


def make_poly_slug() -> str:
    """
    Builds rolling 5-minute Polymarket slug: <prefix>-<bucket_epoch>

    Expected env:
      - POLY_MARKET_SLUG=btc-updown-5m   (no epoch)
      - POLY_EVENT_SLUG optional fallback

    Hardening:
      - If env accidentally includes -<epoch> or -<start>-<end>, strip trailing epochs.
    """
    base = (os.getenv("POLY_MARKET_SLUG") or "").strip()
    event = (os.getenv("POLY_EVENT_SLUG") or "").strip()
    if not base and not event:
        raise RuntimeError("Missing POLY_MARKET_SLUG or POLY_EVENT_SLUG")

    prefix = base or event
    prefix = re.sub(r"(-\d{10}){1,2}$", "", prefix)  # strip trailing epoch(s) if present

    return f"{prefix}-{five_min_bucket_epoch()}"


def http_get_json(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: int = 20
) -> Optional[dict]:
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def http_post_json(
    url: str,
    payload: dict,
    headers: Optional[dict] = None,
    timeout: int = 25
) -> Tuple[int, Optional[dict], str]:
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=timeout)
        text = r.text[:5000] if r.text else ""
        try:
            j = r.json()
        except Exception:
            j = None
        return r.status_code, j, text
    except Exception as e:
        return 0, None, str(e)


def _maybe_json_list(x) -> Optional[List[Any]]:
    """
    Gamma often returns list fields as JSON-encoded strings.
    Accepts:
      - list -> list
      - '["a","b"]' -> list
      - None -> None
    """
    if x is None:
        return None
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s.startswith("[") and s.endswith("]"):
            try:
                v = json.loads(s)
                return v if isinstance(v, list) else None
            except Exception:
                return None
    return None


# ----------------------------
# DB
# ----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


def db_conn():
    if not DATABASE_URL:
        raise RuntimeError("Missing DATABASE_URL")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def ensure_tables():
    """Ensures bot_state + equity_snapshots exist and have required columns."""
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS bot_state ();")

    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS id INTEGER;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS as_of_date DATE;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;")
    cur.execute("ALTER TABLE bot_state ALTER COLUMN updated_at SET DEFAULT NOW();")

    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS position TEXT;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS entry_price DOUBLE PRECISION;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS entry_ts TIMESTAMPTZ;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS last_action TEXT;")

    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS stake DOUBLE PRECISION;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS trades_today INTEGER;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS pnl_today_realized DOUBLE PRECISION;")

    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS last_trade_ts TIMESTAMPTZ;")
    cur.execute("ALTER TABLE bot_state ADD COLUMN IF NOT EXISTS equity DOUBLE PRECISION;")

    cur.execute("""
        INSERT INTO bot_state (
            id, as_of_date, position, entry_price, entry_ts, last_action,
            stake, trades_today, pnl_today_realized, updated_at
        )
        SELECT
            1, CURRENT_DATE, NULL, NULL, NULL, 'BOOT',
            0.0, 0, 0.0, NOW()
        WHERE NOT EXISTS (SELECT 1 FROM bot_state WHERE id = 1);
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS equity_snapshots (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            price DOUBLE PRECISION,
            balance DOUBLE PRECISION,
            position TEXT,
            entry_price DOUBLE PRECISION,
            stake DOUBLE PRECISION,
            unrealized_pnl DOUBLE PRECISION,
            equity DOUBLE PRECISION,
            poly_slug TEXT
        );
    """)

    cur.close()
    conn.close()


def load_state() -> Dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT as_of_date, position, entry_price, stake, trades_today, pnl_today_realized "
                "FROM bot_state WHERE id=1;"
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
                SET as_of_date=%s, position=%s, entry_price=%s, stake=%s,
                    trades_today=%s, pnl_today_realized=%s, updated_at=NOW()
                WHERE id=1;
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
    poly_slug: str
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
# Gamma/CLOB
# ----------------------------
POLY_GAMMA_HOST = os.getenv("POLY_GAMMA_HOST", "https://gamma-api.polymarket.com").strip()
POLY_CLOB_HOST = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").strip()

POLY_MARKET_SLUG = os.getenv("POLY_MARKET_SLUG", "").strip()
POLY_GAMMA_SLUG = os.getenv("POLY_GAMMA_SLUG", "").strip() or POLY_MARKET_SLUG

def extract_yes_no_token_ids(market: dict) -> Tuple[Optional[str], Optional[str]]:
    def maybe_json(x):
        :contentReference[oaicite:6]{index=6}urn None
        if isinstance(x, (list, dict)):
            return x
        if isinstance(x, str):
            s :contentReference[oaicite:7]{index=7}s:
                return None
            try:
                return json.loads(s)
            except Exception:
                return None
        return None

    outcomes = maybe_json(market.get("outcomes"))
    token_ids = maybe_json(market.get("clobTokenIds"))

    if not isinstance(outcomes, list) or not isinstance(token_ids, list):
        return None, None
    if len(outcomes) != len(token_ids) or len(outcomes) < 2:
        return None, None

    # Common cases:
    # - ["Yes","No"] => [yes_token, no_token]
    # - ["Up","Down"] => treat Up as YES side, Down as NO side for our strategy conventions
    norm = [str(o).strip().upper() for o in outcomes]

    if norm[0] in ("YES", "UP") and norm[1] in ("NO", "DOWN"):
        return str(token_ids[0]), str(token_ids[1])

    # fallback: try explicit scan
    yes_id = no_id = None
    for o, tid in zip(norm, token_ids):
        if o in ("YES", "UP"):
            yes_id = str(tid)
        elif o in ("NO", "DOWN"):
            no_id = str(tid)

    return yes_id, no_id

def fetch_gamma_market_and_tokens(poly_slug: str) -> Dict[str, Any]:
    """
    Resolver strategy:
      1) Try MARKET by slug (direct)
      2) Try EVENT by slug; then pick a market:
         - if event embeds markets -> take first
         - else fetch markets by event_id
      3) Fallback scan /events with minimal params (limit/offset only) to avoid 422

    Returns:
      {"market": m, "yes_token_id": y, "no_token_id": n, "slug": m.slug}
    """
    GAMMA_BASE = POLY_GAMMA_HOST

    def _epochs(slug: str):
        return re.findall(r"\d{9,12}", slug or "")

    def _prefix(slug: str):
        return re.sub(r"(-\d{9,12}){1,2}$", "", (slug or "").strip())

    def _gamma_get_market_by_slug_exact(slug: str) -> Optional[dict]:
        slug = (slug or "").strip()
        if not slug:
            return None

        r = requests.get(f"{GAMMA_BASE}/markets", params={"slug": slug}, timeout=20)
        if r.status_code == 200:
            j = r.json()
            if isinstance(j, list) and j:
                return j[0]

        r2 = requests.get(f"{GAMMA_BASE}/markets/slug/{slug}", timeout=20)
        if r2.status_code == 200:
            j2 = r2.json()
            if isinstance(j2, dict) and j2:
                return j2

        return None

    def _gamma_get_event_by_slug_exact(slug: str) -> Optional[dict]:
        slug = (slug or "").strip()
        if not slug:
            return None

        r = requests.get(f"{GAMMA_BASE}/events", params={"slug": slug}, timeout=20)
        if r.status_code == 200:
            j = r.json()
            if isinstance(j, list) and j:
                return j[0]

        r2 = requests.get(f"{GAMMA_BASE}/events/slug/{slug}", timeout=20)
        if r2.status_code == 200:
            j2 = r2.json()
            if isinstance(j2, dict) and j2:
                return j2

        return None

    def _gamma_get_markets_by_event_id(event_id: Any) -> List[dict]:
        if event_id is None:
            return []
        r = requests.get(f"{GAMMA_BASE}/markets", params={"event_id": str(event_id)}, timeout=20)
        if r.status_code != 200:
            return []
        j = r.json()
        return j if isinstance(j, list) else []

    def _event_pick_market(ev: dict) -> Optional[dict]:
        if not ev or not isinstance(ev, dict):
            return None

        markets = ev.get("markets")
        markets = markets if isinstance(markets, list) else _maybe_json_list(markets)
        if isinstance(markets, list) and markets:
            return markets[0]

        eid = ev.get("id") or ev.get("event_id") or ev.get("eventId")
        if eid:
            mkts = _gamma_get_markets_by_event_id(eid)
            if mkts:
                return mkts[0]

        return None

    def _gamma_list_events(limit: int = 200, offset: int = 0) -> List[dict]:
        # 422-safe: only limit/offset
        params = {"limit": int(limit), "offset": int(offset)}
        r = requests.get(f"{GAMMA_BASE}/events", params=params, timeout=25)
        if r.status_code != 200:
            return []
        j = r.json()
        return j if isinstance(j, list) else []

    env_base = (os.getenv("POLY_GAMMA_SLUG") or os.getenv("POLY_MARKET_SLUG") or "").strip()
    pfx = _prefix(env_base) or _prefix(poly_slug)

    e = _epochs(poly_slug)
    start_epoch = e[0] if len(e) >= 1 else None
    end_epoch = e[-1] if len(e) >= 1 else None

    tried = []
    tried.append(("prefix", pfx))
    tried.append(("start_epoch", start_epoch))
    tried.append(("end_epoch", end_epoch))

    candidates = []
    if pfx and start_epoch:
        candidates.append(f"{pfx}-{start_epoch}")
    if pfx and end_epoch:
        candidates.append(f"{pfx}-{end_epoch}")
    if poly_slug:
        candidates.append(poly_slug)

    seen = set()
    candidates = [c for c in candidates if c and not (c in seen or seen.add(c))]

    # 1) Direct: market then event
    for slug in candidates:
        tried.append(("market_by_slug", slug))
        m = _gamma_get_market_by_slug_exact(slug)
        if m:
            y, n = extract_yes_no_token_ids(m)
            if y and n:
                return {"market": m, "yes_token_id": y, "no_token_id": n, "slug": (m.get("slug") or slug)}

        tried.append(("event_by_slug", slug))
        ev = _gamma_get_event_by_slug_exact(slug)
        if ev:
            m2 = _event_pick_market(ev)
            if m2:
                y, n = extract_yes_no_token_ids(m2)
                if y and n:
                    return {"market": m2, "yes_token_id": y, "no_token_id": n, "slug": (m2.get("slug") or "")}

    # 2) Fallback: scan events and score by prefix + nearest epoch
    def _extract_last_epoch(s: str) -> Optional[int]:
        nums = re.findall(r"\d{9,12}", s or "")
        if not nums:
            return None
        try:
            return int(nums[-1])
        except Exception:
            return None

    def _score_event(ev: dict) -> int:
        eslug = (ev.get("slug") or "").strip()
        if not eslug or not pfx or not eslug.startswith(pfx):
            return 0
        score = 10
        ep = _extract_last_epoch(eslug)
        if ep is not None and start_epoch is not None:
            try:
                se = int(start_epoch)
                if ep == se:
                    score += 100
                elif abs(ep - se) == 300:
                    score += 70
                elif abs(ep - se) == 600:
                    score += 40
            except Exception:
                pass
        return score

    best = None  # (score, market_obj)
    pages = 50
    limit = 200

    for page in range(pages):
        offset = page * limit
        tried.append(("list_events_offset", offset))
        evs = _gamma_list_events(limit=limit, offset=offset)
        if not evs:
            break

        for ev in evs:
            sc = _score_event(ev)
            if sc <= 0:
                continue
            m3 = _event_pick_market(ev)
            if not m3:
                continue
            if best is None or sc > best[0]:
                best = (sc, m3)

        if best and best[0] >= 110:
            break

    if best:
        m4 = best[1]
        y, n = extract_yes_no_token_ids(m4)
        if y and n:
            return {"market": m4, "yes_token_id": y, "no_token_id": n, "slug": (m4.get("slug") or "")}

    raise RuntimeError(
        "Gamma market/token resolution failed. "
        f"poly_slug='{poly_slug}' env_base='{env_base}' prefix='{pfx}' "
        f"start_epoch='{start_epoch}' end_epoch='{end_epoch}' tried={tried}"
    )

def clob_midpoints(token_ids: list[str]) -> Dict[str, Optional[float]]:
    url = f"{POLY_CLOB_HOST}/midpoints"
    # docs: query params for multiple ids; most implementations use repeated token_ids=
    j = http_get_json(url, params=[("token_ids", tid) for tid in token_ids])
    if not j or not isinstance(j, dict):
        return {tid: None for tid in token_ids}

    out: Dict[str, Optional[float]] = {}
    for tid in token_ids:
        v = j.get(tid)
        try:
            out[tid] = float(v) if v is not None else None
        except Exception:
            out[tid] = None
    return out

# ----------------------------
# Bun live order gateway
# ----------------------------
BUN_BASE_URL = os.getenv("BUN_BASE_URL", "").strip()


def bun_health() -> Optional[int]:
    if not BUN_BASE_URL:
        return None
    code, _, _ = http_post_json(f"{BUN_BASE_URL}/__ping", payload={}, timeout=8)
    if code == 0 or code >= 400:
        try:
            r = requests.get(f"{BUN_BASE_URL}/", timeout=8)
            return r.status_code
        except Exception:
            return None
    return code


def bun_place_order(token_id: str, side: str, price: float, size: float) -> Tuple[bool, str]:
    """
    side: BUY or SELL
    """
    if not BUN_BASE_URL:
        return False, "missing BUN_BASE_URL"
    payload = {
        "token_id": token_id,
        "side": side,
        "price": float(price),
        "size": float(size),
        "order_type": "GTC",
    }
    code, j, text = http_post_json(f"{BUN_BASE_URL}/order", payload=payload, timeout=20)
    if code != 200 or not j or not j.get("ok"):
        return False, f"bun order failed code={code} body={text[:300]}"
    return True, "ok"


# ----------------------------
# Strategy / Risk
# ----------------------------
EDGE_ENTER = env_float("EDGE_ENTER", 0.10)
EDGE_EXIT = env_float("EDGE_EXIT", 0.02)
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "60"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "60"))
LIVE_TRADE_SIZE = env_float("LIVE_TRADE_SIZE", 1.0)


def compute_signal(poly_up: float, fair_up: float) -> Tuple[str, float]:
    edge = fair_up - poly_up
    if edge >= EDGE_ENTER:
        return "YES", edge
    if edge <= -EDGE_ENTER:
        return "NO", edge
    return "HOLD", edge


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    log.info("BOOT: bot.py starting")
    ensure_tables()

    run_mode = os.getenv("RUN_MODE", "DRY_RUN").strip().upper()
    live_mode = run_mode == "LIVE"
    live_trading_enabled = env_bool("LIVE_TRADING_ENABLED", False)
    kill_switch = env_bool("KILL_SWITCH", True)

    live_armed = live_mode and live_trading_enabled and (not kill_switch)

    poly_slug = make_poly_slug()
    log.info("run_mode=%s live_mode=%s live_armed=%s poly_slug=%s", run_mode, live_mode, live_armed, poly_slug)

    if BUN_BASE_URL:
        hs = bun_health()
        if hs is not None:
            log.info("bun_health_status=%s", hs)

    # Daily reset
    state = load_state()
    today = date.today()
    if state["as_of_date"] != today:
        state["as_of_date"] = today
        state["trades_today"] = 0
        state["pnl_today_realized"] = 0.0
        save_state(state)

    # Resolve Gamma -> token ids
    gamma = fetch_gamma_market_and_tokens(poly_slug)
    yes_token = gamma["yes_token_id"]
    no_token = gamma["no_token_id"]

    # Midpoints
    p_yes = clob_midpoint(yes_token)
    p_no = clob_midpoint(no_token)
    if p_yes is None or p_no is None:
        log.warning("Could not fetch midpoint prices from CLOB (yes=%s no=%s)", p_yes, p_no)
        log.info("BOOT: bot.py finished cleanly")
        return

    poly_up = float(p_yes)
    poly_down = float(p_no)

    fair_up = 0.50  # placeholder
    signal, edge = compute_signal(poly_up, fair_up)

    position = state["position"]  # "YES" / "NO" / None
    entry = state["entry_price"]
    stake = float(state["stake"] or 0.0)
    trades_today = int(state["trades_today"] or 0)
    pnl_today = float(state["pnl_today_realized"] or 0.0)

    action = "NO_TRADE"
    reason = ""
    balance = float(os.getenv("PAPER_BALANCE", "0").strip() or 0.0)

    if trades_today >= MAX_TRADES_PER_DAY:
        reason = "MAX_TRADES_PER_DAY"
        action = "NO_TRADE"
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
            if position == "YES" and signal == "NO":
                action = "EXIT"
            elif position == "NO" and signal == "YES":
                action = "EXIT"
            else:
                action = "NO_TRADE"
                reason = "signal=HOLD" if signal == "HOLD" else "HOLD_SAME_SIDE"

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
        if position == "YES":
            exit_price = poly_up
            token = yes_token
        else:
            exit_price = poly_down
            token = no_token

        if entry is None:
            state["position"] = None
            state["entry_price"] = None
            state["stake"] = 0.0
            save_state(state)
        else:
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

    # Snapshot
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

    equity = balance + u

    log.info(
        "poly_up=%.3f | poly_down=%.3f | fair_up=%.3f | edge=%+.3f | signal=%s | action=%s%s | balance=%.2f | pos=%s | entry=%s | trades_today=%d | pnl_today(realized)=%.2f | mode=%s | src=clob",
        poly_up, poly_down, fair_up, edge, signal, action,
        (f" | reason={reason}" if reason else ""),
        balance,
        position2,
        (None if entry2 is None else round(float(entry2), 4)),
        int(state2["trades_today"] or 0),
        float(state2["pnl_today_realized"] or 0.0),
        run_mode,
    )

    record_equity_snapshot(
        price=float(mark_price),
        balance=float(balance),
        position=position2,
        entry_price=(None if entry2 is None else float(entry2)),
        stake=float(stake2),
        unrealized_pnl=float(u),
        equity=float(equity),
        poly_slug=poly_slug,
    )

    log.info("summary | equity=%.2f | uPnL=%.2f | src=clob", equity, u)
    log.info("BOOT: bot.py finished cleanly")


if __name__ == "__main__":
    main()

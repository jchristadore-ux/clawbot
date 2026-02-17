import os
import json
import time
from datetime import datetime, timezone

import requests

# =========================
# CONFIG
# =========================
STATE_DIR = os.getenv("STATE_DIR", "/data")  # Mount a Railway Volume at /data
STATE_FILE = os.path.join(STATE_DIR, "state.json")

START_BALANCE = float(os.getenv("START_BALANCE", "1000"))
TRADE_SIZE = float(os.getenv("TRADE_SIZE", "25"))  # fixed stake per trade (paper)
FEE_PER_TRADE = float(os.getenv("FEE_PER_TRADE", "0"))  # optional paper fee

SYMBOL = "bitcoin"
VS_CURRENCY = "usd"

# Strategy thresholds (tweak later)
UP_THRESHOLD = float(os.getenv("UP_THRESHOLD", "1.002"))   # +0.2% above avg => YES
DOWN_THRESHOLD = float(os.getenv("DOWN_THRESHOLD", "0.998"))  # -0.2% below avg => NO

# Number of 5-min points to use for signal
LOOKBACK = int(os.getenv("LOOKBACK", "5"))

# Basic safety: if API fails, exit cleanly (no crash-loop)
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "20"))


# =========================
# UTIL
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_state_dir():
    # Create directory if allowed (works with /data volume or local run)
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
    except Exception as e:
        print(f"{utc_now_iso()} | WARN | Could not create state dir '{STATE_DIR}': {e}")


def load_state():
    default = {
        "balance": START_BALANCE,
        "position": None,       # "YES" or "NO"
        "entry_price": None,    # BTC price at entry
        "stake": 0.0,           # stake in current position
        "history": []
    }

    if not os.path.exists(STATE_FILE):
        return default

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            s = json.load(f)
        # Merge defaults to tolerate schema changes
        for k, v in default.items():
            if k not in s:
                s[k] = v
        return s
    except Exception as e:
        print(f"{utc_now_iso()} | WARN | Failed to load state.json, resetting state. Error: {e}")
        return default


def save_state(state):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"{utc_now_iso()} | ERROR | Failed to save state.json: {e}")


# =========================
# DATA
# =========================
def fetch_btc_closes_5m(lookback: int):
    """
    Fetch last `lookback` 5-minute closes from Kraken (free/public).
    Kraken OHLC docs: /0/public/OHLC
    """
    url = "https://api.kraken.com/0/public/OHLC"
    params = {
        "pair": "XBTUSD",
        "interval": 5  # 5-minute candles
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT_SEC)
            if r.status_code != 200:
                last_error = f"HTTP {r.status_code}: {r.text[:200]}"
                time.sleep(1.5 * attempt)
                continue

            data = r.json()

            if data.get("error"):
                last_error = f"Kraken error: {data['error']}"
                time.sleep(1.0 * attempt)
                continue

            result = data.get("result", {})
            # result contains one key for the pair (e.g. "XXBTZUSD") plus "last"
            pair_key = next((k for k in result.keys() if k != "last"), None)
            if not pair_key or not isinstance(result.get(pair_key), list):
                last_error = f"Unexpected result shape: {str(data)[:200]}"
                time.sleep(1.0 * attempt)
                continue

            candles = result[pair_key]
            if len(candles) < lookback:
                last_error = f"Not enough candles: {len(candles)}"
                time.sleep(1.0 * attempt)
                continue

            # Candle format:
            # [ time, open, high, low, close, vwap, volume, count ]
            closes = [float(c[4]) for c in candles[-lookback:]]
            return closes

        except Exception as e:
            last_error = str(e)
            time.sleep(1.5 * attempt)

    print(f"{utc_now_iso()} | WARN | Price fetch failed after retries: {last_error}")
    return None

# =========================
# STRATEGY
# =========================
def decide(closes):
    """
    Simple rule:
    - If last close > avg * UP_THRESHOLD => YES
    - If last close < avg * DOWN_THRESHOLD => NO
    - Else HOLD
    """
    avg = sum(closes) / len(closes)
    last = closes[-1]

    if last > avg * UP_THRESHOLD:
        return "YES"
    if last < avg * DOWN_THRESHOLD:
        return "NO"
    return "HOLD"


# =========================
# PAPER TRADING ENGINE
# =========================
def calc_pnl(entry_price: float, exit_price: float, side: str, stake: float) -> float:
    """
    Paper PnL model (simple directional exposure):
    YES = long BTC; NO = short BTC

    We scale PnL by stake in a simple way:
    pnl = stake * (% move) * direction
    """
    if entry_price <= 0:
        return 0.0
    pct_move = (exit_price - entry_price) / entry_price
    direction = 1.0 if side == "YES" else -1.0
    return stake * pct_move * direction


def paper_trade(state, signal, current_price):
    """
    Enter when flat and signal is YES/NO
    Exit when in a position and signal flips to the opposite side
    HOLD does nothing
    """
    # No action on HOLD
    if signal == "HOLD":
        return state, "HOLD"

    pos = state["position"]

    # Enter if flat
    if pos is None:
        if state["balance"] < TRADE_SIZE:
            return state, "SKIP_INSUFFICIENT_BALANCE"

        state["position"] = signal
        state["entry_price"] = current_price
        state["stake"] = TRADE_SIZE
        state["balance"] -= (TRADE_SIZE + FEE_PER_TRADE)

        state["history"].append({
            "time": utc_now_iso(),
            "action": "ENTER",
            "side": signal,
            "price": current_price,
            "stake": TRADE_SIZE,
            "fee": FEE_PER_TRADE
        })
        return state, f"ENTER_{signal}"

    # If in position, exit only if opposite signal
    if pos is not None and signal != pos:
        entry = float(state["entry_price"])
        stake = float(state.get("stake", TRADE_SIZE))

        pnl = calc_pnl(entry, current_price, pos, stake)

        # Return stake + pnl to balance, subtract fee
        state["balance"] += (stake + pnl - FEE_PER_TRADE)

        state["history"].append({
            "time": utc_now_iso(),
            "action": "EXIT",
            "side": pos,
            "price": current_price,
            "pnl": pnl,
            "stake": stake,
            "fee": FEE_PER_TRADE
        })

        state["position"] = None
        state["entry_price"] = None
        state["stake"] = 0.0

        return state, f"EXIT_{pos}"

    return state, "HOLD_SAME_SIDE"


# =========================
# MAIN
# =========================
def main():
    ensure_state_dir()
    state = load_state()

    closes = fetch_btc_closes_5m(LOOKBACK)
    if closes is None:
        # Exit cleanly so Railway doesn't crash-loop
        print(f"{utc_now_iso()} | INFO | No data this run. balance={state['balance']:.2f} pos={state['position']}")
        return

    signal = decide(closes)
    current_price = closes[-1]

    state, action = paper_trade(state, signal, current_price)
    save_state(state)

    print(
        f"{utc_now_iso()} | price={current_price:.2f} | "
        f"signal={signal} | action={action} | "
        f"balance={state['balance']:.2f} | pos={state['position']} | entry={state['entry_price']}"
    )


if __name__ == "__main__":
    main()

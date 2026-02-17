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
    CoinGecko market_chart returns many points; we take the last `lookback` prices.
    Response shape: {"prices": [[ts_ms, price], ...], ...}
    """
    url = f"https://api.coingecko.com/api/v3/coins/{SYMBOL}/market_chart"
    params = {
        "vs_currency": VS_CURRENCY,
        "days": "1",
        "interval": "5m"
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT_SEC)
            if r.status_code != 200:
                last_error = f"HTTP {r.status_code}: {r.text[:200]}"
                # Small backoff
                time.sleep(1.5 * attempt)
                continue

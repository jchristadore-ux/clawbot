#!/usr/bin/env python3

import json
import os
import time
from datetime import datetime, timezone
from typing import Optional, Any

import requests


# =========================
# CONFIG
# =========================

SERIES_TICKER = os.getenv("SERIES_TICKER", "KXBTC15M")
LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() == "true"
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "8"))
START_EQUITY = float(os.getenv("START_EQUITY", "50"))

STATE_FILE = os.getenv("STATE_FILE", "/data/state_clean.json")


# =========================
# STATE
# =========================

state = {
    "equity": START_EQUITY,
    "position": None,
}


def utc():
    return datetime.now(timezone.utc).isoformat()


def save_state():
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)


# =========================
# SIMULATED MARK (TEMP SAFE)
# =========================

def fake_mark():
    # Stable oscillating mark just to prove trading works
    return 0.45 + (time.time() % 20) / 100.0


# =========================
# TRADE LOG
# =========================

def log_trade(event: str, mark: float):
    print(json.dumps({
        "ts": utc(),
        "event": event,
        "live": LIVE_MODE,
        "equity": state["equity"],
        "position": state["position"],
        "mark": round(mark, 4)
    }), flush=True)


# =========================
# MAIN LOOP
# =========================

def main():
    print("BOT_VERSION=CLEAN_RESET_v1", flush=True)
    load_state()

    while True:
        mark = fake_mark()

        if state["position"] is None and mark > 0.55:
            state["position"] = "YES"
            log_trade("ENTER", mark)

        elif state["position"] == "YES" and mark < 0.48:
            state["position"] = None
            log_trade("EXIT", mark)

        save_state()
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

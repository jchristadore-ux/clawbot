"""
core/notifier.py — Structured logging + Telegram notifications.

All logs are JSON to stdout (Railway captures stdout → log stream).
Telegram sends only for high-signal events (ENTER, EXIT, errors).

Throttle support: prevent log spam for repeated non-critical events.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional


# ── Throttle state ─────────────────────────────────────────────────────────────
_last_logged: Dict[str, float] = {}


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_event(
    event: str,
    data: Optional[Dict[str, Any]] = None,
    throttle_key: Optional[str] = None,
    throttle_s: float = 0.0,
) -> None:
    """
    Emit a structured JSON log line to stdout.
    If throttle_key is set, only logs once per throttle_s seconds for that key.
    """
    if throttle_key and throttle_s > 0:
        now = time.time()
        if now - _last_logged.get(throttle_key, 0.0) < throttle_s:
            return
        _last_logged[throttle_key] = now

    record: Dict[str, Any] = {"ts": utc_iso(), "event": event}
    if data:
        record.update(data)
    print(json.dumps(record), flush=True)


def send_telegram(message: str) -> None:
    """Send a message to Telegram. Silently skips if not configured."""
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        return
    try:
        import requests
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "disable_web_page_preview": True},
            timeout=10,
        )
        if r.status_code != 200:
            log_event("TELEGRAM_ERR", {"status": r.status_code}, throttle_key="tg_err", throttle_s=60)
    except Exception as exc:
        log_event("TELEGRAM_ERR", {"err": str(exc)[:100]}, throttle_key="tg_err", throttle_s=60)

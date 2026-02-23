from typing import Optional

import requests

from clawbot import config


class FailureTracker:
    def __init__(self):
        self.consecutive_failures = 0

    def success(self) -> None:
        self.consecutive_failures = 0

    def failure(self) -> int:
        self.consecutive_failures += 1
        return self.consecutive_failures


def send_alert(message: str, level: str = "warning") -> bool:
    if not config.ALERT_WEBHOOK_URL:
        return False
    payload = {"level": level, "message": message}
    try:
        r = requests.post(config.ALERT_WEBHOOK_URL, json=payload, timeout=config.TIMEOUT_SEC)
        return 200 <= r.status_code < 300
    except Exception:
        return False


def maybe_alert_on_failures(tracker: FailureTracker, context: str) -> Optional[str]:
    n = tracker.failure()
    if n >= config.ALERT_CONSECUTIVE_FAILURE_THRESHOLD:
        msg = f"{context}: consecutive_failures={n}"
        send_alert(msg, level="critical")
        return msg
    return None

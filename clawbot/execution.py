import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Dict, Optional

import requests

from clawbot import config


@dataclass
class ExecutionResult:
    order_id: Optional[str]
    status: str
    raw: Dict


def _signed_headers(method: str, path: str, body: str) -> Dict[str, str]:
    ts = str(int(time.time()))
    payload = f"{ts}{method.upper()}{path}{body}".encode()
    secret = config.LIVE_EXEC_API_SECRET.encode()
    sig = base64.b64encode(hmac.new(secret, payload, hashlib.sha256).digest()).decode()
    return {
        "Content-Type": "application/json",
        "X-API-KEY": config.LIVE_EXEC_API_KEY,
        "X-API-SIGN": sig,
        "X-API-TS": ts,
        "X-API-PASSPHRASE": config.LIVE_EXEC_API_PASSPHRASE,
    }


def place_live_order(side: str, price: float, stake: float, symbol: str) -> ExecutionResult:
    if not all([config.LIVE_EXEC_BASE_URL, config.LIVE_EXEC_API_KEY, config.LIVE_EXEC_API_SECRET]):
        raise RuntimeError("Missing live execution env config")

    path = config.LIVE_EXEC_ORDER_PATH
    body_obj = {"side": side, "price": price, "stake": stake, "symbol": symbol}
    body = json.dumps(body_obj, separators=(",", ":"))
    headers = _signed_headers("POST", path, body)

    r = requests.post(f"{config.LIVE_EXEC_BASE_URL}{path}", data=body, headers=headers, timeout=config.TIMEOUT_SEC)
    r.raise_for_status()
    data = r.json()
    return ExecutionResult(order_id=str(data.get("order_id")), status=str(data.get("status", "submitted")), raw=data)


def reconcile_order(order_id: str) -> ExecutionResult:
    path = config.LIVE_EXEC_ORDER_STATUS_PATH.format(order_id=order_id)
    headers = _signed_headers("GET", path, "")
    r = requests.get(f"{config.LIVE_EXEC_BASE_URL}{path}", headers=headers, timeout=config.TIMEOUT_SEC)
    r.raise_for_status()
    data = r.json()
    return ExecutionResult(order_id=str(data.get("order_id", order_id)), status=str(data.get("status", "unknown")), raw=data)

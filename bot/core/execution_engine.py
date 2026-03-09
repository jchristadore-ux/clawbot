"""
core/execution_engine.py — Order placement via the Bun RSA gateway.

All live orders route through function-bun/index.ts which handles:
- RSA-SHA256 signing (more reliable in Bun/Node than Python for Railway)
- client_order_id for idempotency
- FOK order type (fill-or-kill) to prevent resting ghost orders

Paper mode: simulates immediate fill, no HTTP request.

Design principles:
- Never let an exception here crash the main loop silently
- Log every order attempt, fill, and failure
- Return a typed OrderResult, never raw dicts
- Caller decides what to do with failures
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Optional

import requests

from .notifier import log_event, send_telegram
from .scheduler import utc_iso


@dataclass
class OrderResult:
    success: bool
    paper: bool
    filled_contracts: int
    fill_price_cents: int
    client_order_id: str
    error: Optional[str] = None


class ExecutionEngine:
    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg

    def place_order(
        self,
        action: str,          # "buy" or "sell"
        ticker: str,
        side: str,            # "YES" or "NO"
        contracts: int,
        price_cents: int,
    ) -> OrderResult:
        """
        Submit an order. Returns OrderResult with success/failure details.
        Paper mode: always returns simulated success.
        Live mode: routes through Bun gateway with FOK semantics.
        """
        client_order_id = str(uuid.uuid4())

        log_event("ORDER_ATTEMPT", {
            "action": action,
            "ticker": ticker,
            "side": side,
            "contracts": contracts,
            "price_cents": price_cents,
            "live": self.cfg.LIVE_MODE,
            "client_order_id": client_order_id,
        })

        # ── Paper mode ─────────────────────────────────────────────────────────
        if not self.cfg.LIVE_MODE:
            log_event("ORDER_PAPER_FILL", {
                "action": action, "side": side, "contracts": contracts,
                "price_cents": price_cents, "client_order_id": client_order_id,
            })
            return OrderResult(
                success=True,
                paper=True,
                filled_contracts=contracts,
                fill_price_cents=price_cents,
                client_order_id=client_order_id,
            )

        # ── Live mode ──────────────────────────────────────────────────────────
        if not self.cfg.GATEWAY_URL:
            err = "KALSHI_ORDER_GATEWAY_URL not set — cannot place live order"
            log_event("ORDER_FAIL", {"err": err})
            return OrderResult(success=False, paper=False, filled_contracts=0,
                               fill_price_cents=0, client_order_id=client_order_id, error=err)

        payload: dict = {
            "ticker": ticker,
            "action": action,
            "type": "fok",
            "side": side,
            "count": int(contracts),
            "client_order_id": client_order_id,
        }
        if side == "YES":
            payload["yes_price"] = max(1, min(99, int(price_cents)))
        else:
            payload["no_price"] = max(1, min(99, int(price_cents)))

        try:
            r = requests.post(
                f"{self.cfg.GATEWAY_URL}/order",
                json=payload,
                timeout=self.cfg.REQUEST_TIMEOUT,
            )
            if r.status_code >= 400:
                err = f"Gateway HTTP {r.status_code}: {r.text[:300]}"
                log_event("ORDER_FAIL", {"err": err, "client_order_id": client_order_id})
                send_telegram(f"⚠️ ORDER FAILED\n{ticker} {action} {side} @ {price_cents}c\n{err[:200]}")
                return OrderResult(success=False, paper=False, filled_contracts=0,
                                   fill_price_cents=0, client_order_id=client_order_id, error=err)

            out = r.json()
            if not isinstance(out, dict) or not out.get("ok"):
                err = f"Gateway rejected: {str(out)[:300]}"
                log_event("ORDER_FAIL", {"err": err, "client_order_id": client_order_id})
                send_telegram(f"⚠️ ORDER REJECTED\n{ticker} {action} {side}\n{err[:200]}")
                return OrderResult(success=False, paper=False, filled_contracts=0,
                                   fill_price_cents=0, client_order_id=client_order_id, error=err)

            # Parse fill details from response
            order = (out.get("response") or {}).get("order") or {}
            status = str(order.get("status", "")).lower().strip()
            filled = int(order.get("filled_count", 0) or 0)

            if status == "canceled" and filled == 0:
                err = f"FOK canceled — book moved (status={status})"
                log_event("ORDER_CANCELED", {"err": err, "client_order_id": client_order_id})
                return OrderResult(success=False, paper=False, filled_contracts=0,
                                   fill_price_cents=0, client_order_id=client_order_id, error=err)

            if status == "resting":
                err = f"Order resting (unexpected for FOK) — status={status}"
                log_event("ORDER_RESTING_UNEXPECTED", {"err": err, "client_order_id": client_order_id})
                send_telegram(f"⚠️ ORDER RESTING UNEXPECTEDLY\n{ticker}\n{err}")
                return OrderResult(success=False, paper=False, filled_contracts=0,
                                   fill_price_cents=0, client_order_id=client_order_id, error=err)

            actual_filled = filled if filled > 0 else contracts  # FOK: assume full fill if status=filled
            log_event("ORDER_CONFIRMED", {
                "action": action, "side": side, "contracts": contracts,
                "filled": actual_filled, "status": status,
                "client_order_id": client_order_id,
            })
            return OrderResult(
                success=True,
                paper=False,
                filled_contracts=actual_filled,
                fill_price_cents=price_cents,
                client_order_id=client_order_id,
            )

        except Exception as exc:
            err = str(exc)[:300]
            log_event("ORDER_EXCEPTION", {"err": err, "client_order_id": client_order_id})
            send_telegram(f"⚠️ ORDER EXCEPTION\n{ticker} {action} {side}\n{err[:200]}")
            return OrderResult(success=False, paper=False, filled_contracts=0,
                               fill_price_cents=0, client_order_id=client_order_id, error=err)

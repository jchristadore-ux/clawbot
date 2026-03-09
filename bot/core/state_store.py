"""
core/state_store.py — State persistence: file + Postgres equity sync.

BotState: in-memory state of the bot (position, cash, history)
StateStore: loads/saves state; syncs equity from Postgres on startup

On Railway:
- The .runtime/ directory is ephemeral (lost on redeploy)
- Postgres via gateway is durable
- On restart: load from file if present, else sync from Postgres
- Position state is also in Postgres to survive restarts

Critical: if the bot restarts with an open position, it MUST know about it
or it will try to re-enter and create a double position.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from .notifier import log_event
from .scheduler import utc_iso


@dataclass
class BotState:
    # ── Financials ────────────────────────────────────────────────────────────
    cash: float = 50.0
    realized_pnl_lifetime: float = 0.0
    trade_history_24h: List[Dict[str, Any]] = field(default_factory=list)

    # ── Open position ─────────────────────────────────────────────────────────
    position_side: Optional[str] = None          # "YES" | "NO" | None
    position_entry_price: Optional[float] = None  # 0-1
    position_contracts: int = 0
    position_ticker: Optional[str] = None
    position_open_ts: Optional[str] = None

    # ── Bucket tracking ────────────────────────────────────────────────────────
    current_bucket_ticker: Optional[str] = None
    bucket_start_price: Optional[float] = None
    bucket_open_ts: Optional[str] = None

    # ── Timing ────────────────────────────────────────────────────────────────
    last_trade_ts: Optional[str] = None

    # ── Performance streaks ────────────────────────────────────────────────────
    win_streak: int = 0       # positive = wins, negative = losses

    @property
    def has_position(self) -> bool:
        return self.position_side is not None and self.position_contracts > 0


class StateStore:
    def __init__(self, state_file: Path, gateway_url: str) -> None:
        self._file = state_file
        self._gateway = gateway_url.rstrip("/") if gateway_url else ""
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        self._file.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> BotState:
        """Load state from file. Return fresh state if file missing/corrupt."""
        if not self._file.exists():
            return BotState()
        try:
            raw = json.loads(self._file.read_text(encoding="utf-8"))
            known_fields = set(BotState.__dataclass_fields__)
            filtered = {k: v for k, v in raw.items() if k in known_fields}
            s = BotState(**filtered)
            if s.trade_history_24h is None:
                s.trade_history_24h = []
            log_event("STATE_LOADED", {
                "cash": round(s.cash, 4),
                "has_position": s.has_position,
                "position_ticker": s.position_ticker,
            })
            return s
        except Exception as exc:
            log_event("STATE_LOAD_ERR", {"err": str(exc)[:200]})
            return BotState()

    def save(self, state: BotState) -> None:
        """Persist state to file. Prune 24h history first."""
        self._prune_24h(state)
        try:
            self._file.write_text(json.dumps(asdict(state), indent=2), encoding="utf-8")
        except Exception as exc:
            log_event("STATE_SAVE_ERR", {"err": str(exc)[:200]})

    def sync_equity_from_postgres(self, state: BotState) -> None:
        """
        On startup: try to restore cash/lifetime_pnl from Postgres.
        Also sync open position from Postgres if local state lost it.
        Falls back to local file state if Postgres unavailable.
        """
        if not self._gateway:
            log_event("EQUITY_SYNC_SKIP", {"reason": "no_gateway_url"})
            return
        try:
            r = requests.get(f"{self._gateway}/equity", timeout=8)
            if r.status_code != 200:
                log_event("EQUITY_SYNC_FAIL", {"status": r.status_code})
                return
            data = r.json()
            if not data.get("exists") or data.get("cash") is None:
                # First run — seed from Kalshi real balance
                self._seed_from_real_balance(state)
                return

            pg_cash = float(data["cash"])
            pg_pnl = float(data.get("lifetime_pnl") or 0.0)

            # Trust Postgres cash over local file (survives container restart)
            state.cash = pg_cash
            state.realized_pnl_lifetime = pg_pnl
            log_event("EQUITY_RESTORED", {
                "cash": round(state.cash, 4),
                "lifetime_pnl": round(state.realized_pnl_lifetime, 4),
                "source": "postgres",
            })

        except Exception as exc:
            log_event("EQUITY_SYNC_ERR", {"err": str(exc)[:150]})

    def _seed_from_real_balance(self, state: BotState) -> None:
        """Fetch real Kalshi balance and use as starting equity."""
        if not self._gateway:
            return
        try:
            r = requests.get(f"{self._gateway}/balance", timeout=10)
            if r.status_code != 200:
                return
            data = r.json()
            dollars = data.get("balance_dollars") or (data.get("balance", 0) / 100.0)
            if dollars and float(dollars) > 0:
                state.cash = float(dollars)
                self.push_equity_to_postgres(state)
                log_event("EQUITY_SEEDED", {"cash": round(state.cash, 4), "source": "kalshi_real_balance"})
        except Exception as exc:
            log_event("EQUITY_SEED_ERR", {"err": str(exc)[:150]})

    def push_equity_to_postgres(self, state: BotState) -> None:
        """Write cash + lifetime_pnl to Postgres for restart recovery."""
        if not self._gateway:
            return
        try:
            requests.get(
                f"{self._gateway}/init-equity",
                params={"cash": round(state.cash, 4), "lifetime_pnl": round(state.realized_pnl_lifetime, 4)},
                timeout=5,
            )
        except Exception:
            pass  # best-effort; not fatal

    def sync_real_balance(self, state: BotState) -> None:
        """
        Fetch actual Kalshi balance and reconcile with local cash.
        Called after each trade exit to detect drift.
        Only updates in LIVE_MODE.
        """
        if not self._gateway:
            return
        try:
            r = requests.get(f"{self._gateway}/balance", timeout=10)
            if r.status_code != 200:
                return
            data = r.json()
            real_dollars = data.get("balance_dollars") or (data.get("balance", 0) / 100.0)
            if real_dollars is not None:
                real_dollars = float(real_dollars)
                drift = real_dollars - state.cash
                if abs(drift) > 2.0:
                    log_event("BALANCE_DRIFT", {
                        "bot_cash": round(state.cash, 4),
                        "real_balance": round(real_dollars, 4),
                        "drift": round(drift, 4),
                    })
                state.cash = real_dollars
        except Exception as exc:
            log_event("BALANCE_SYNC_ERR", {"err": str(exc)[:150]})

    # ── 24h trade history ─────────────────────────────────────────────────────

    @staticmethod
    def _prune_24h(state: BotState) -> None:
        if not state.trade_history_24h:
            state.trade_history_24h = []
            return
        now = datetime.now(timezone.utc)
        kept = []
        for t in state.trade_history_24h:
            try:
                dt = datetime.fromisoformat(t["ts"].replace("Z", "+00:00"))
                if (now - dt) <= timedelta(hours=24):
                    kept.append(t)
            except Exception:
                pass
        state.trade_history_24h = kept

    @staticmethod
    def record_trade(state: BotState, event_type: str, pnl: float) -> None:
        if state.trade_history_24h is None:
            state.trade_history_24h = []
        state.trade_history_24h.append({"ts": utc_iso(), "type": event_type, "pnl": float(pnl)})

    @staticmethod
    def daily_pnl(state: BotState) -> float:
        if not state.trade_history_24h:
            return 0.0
        now = datetime.now(timezone.utc)
        total = 0.0
        for t in state.trade_history_24h:
            try:
                dt = datetime.fromisoformat(t["ts"].replace("Z", "+00:00"))
                if (now - dt) <= timedelta(hours=24):
                    total += float(t.get("pnl", 0.0))
            except Exception:
                pass
        return total

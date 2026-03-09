"""
core/position_manager.py — Entry and exit lifecycle.

Coordinates:
- Entry: size position, submit order, update state
- Exit: determine exit trigger, submit sell, reconcile P&L
- P&L accounting: both paper and live
- Streak tracking for adaptive edge
- Telegram notifications on ENTER and EXIT
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from .execution_engine import ExecutionEngine
from .feature_pipeline import Book
from .notifier import log_event, send_telegram
from .risk_engine import RiskEngine
from .scheduler import utc_iso
from .signal_engine import Signal
from .state_store import BotState, StateStore


class PositionManager:
    def __init__(self, cfg: Any, kalshi_client: Any, store: StateStore) -> None:
        self.cfg = cfg
        self._client = kalshi_client
        self._store = store
        self._executor = ExecutionEngine(cfg)
        self._risk = RiskEngine(cfg)

    def enter(
        self,
        state: BotState,
        ticker: str,
        book: Book,
        signal: Signal,
    ) -> bool:
        """
        Place an entry order and update state.
        Returns True if order was placed (filled or paper).
        """
        cfg = self.cfg
        side = signal.side
        entry_cents = signal.entry_cents

        if not side or not entry_cents:
            return False

        entry_price = entry_cents / 100.0
        contracts = self._size_position(state.cash, entry_price, signal)

        if contracts <= 0:
            log_event("SIZE_ZERO", {"cash": state.cash, "entry_price": entry_price})
            return False

        cost = entry_price * contracts
        if state.cash < cost:
            log_event("INSUFFICIENT_CASH", {"cash": round(state.cash, 4), "cost": round(cost, 4)})
            return False

        result = self._executor.place_order("buy", ticker, side, contracts, entry_cents)

        if not result.success:
            log_event("ENTER_FAILED", {"err": result.error, "ticker": ticker, "side": side})
            return False

        # Update state
        state.cash -= cost
        state.position_side = side
        state.position_entry_price = entry_price
        state.position_contracts = contracts
        state.position_ticker = ticker
        state.position_open_ts = utc_iso()
        state.last_trade_ts = utc_iso()
        state.current_bucket_ticker = ticker

        self._risk.mark_bucket_traded(ticker)
        StateStore.record_trade(state, "ENTER", 0.0)

        # Sync equity to Postgres after entry
        self._store.push_equity_to_postgres(state)

        log_event("ENTER", {
            "ticker": ticker,
            "side": side,
            "contracts": contracts,
            "entry_price": round(entry_price, 4),
            "cost": round(cost, 4),
            "cash_after": round(state.cash, 4),
            "edge": round(signal.edge, 4),
            "z": round(signal.z, 3),
            "fair_yes": round(signal.fair_yes, 4),
            "mark_yes": round(signal.mark_yes, 4),
            "live": cfg.LIVE_MODE,
        })
        return True

    def maybe_exit(
        self,
        state: BotState,
        ticker: str,
        book: Book,
        signal: Signal,
        secs_left: Optional[float],
    ) -> bool:
        """
        Check exit conditions and place exit order if triggered.
        Returns True if exited.
        """
        if not state.has_position:
            return False

        cfg = self.cfg
        side = state.position_side
        entry_price = state.position_entry_price
        contracts = state.position_contracts
        mark_yes = book.mark_yes or 0.5

        # ── Cooldown / min hold ────────────────────────────────────────────────
        if state.position_open_ts:
            try:
                open_dt = datetime.fromisoformat(state.position_open_ts.replace("Z", "+00:00"))
                held_secs = (datetime.now(timezone.utc) - open_dt).total_seconds()
            except Exception:
                held_secs = 999
        else:
            held_secs = 999

        force_exit = cfg.KILL_SWITCH and cfg.CLOSE_ON_KILL_SWITCH

        if held_secs < cfg.MIN_HOLD_SECONDS and not force_exit:
            return False

        # ── Stop loss ──────────────────────────────────────────────────────────
        stop_triggered = False
        if side == "YES" and entry_price and mark_yes < entry_price * (1.0 - cfg.STOP_LOSS_PCT):
            stop_triggered = True
            log_event("STOP_LOSS", {"entry": entry_price, "mark": round(mark_yes, 4)})
        elif side == "NO" and entry_price:
            no_mark = 1.0 - mark_yes
            no_entry = 1.0 - entry_price
            if no_mark < no_entry * (1.0 - cfg.STOP_LOSS_PCT):
                stop_triggered = True
                log_event("STOP_LOSS", {"side": "NO", "no_entry": round(no_entry, 4), "no_mark": round(no_mark, 4)})

        # ── Hold to expiry (winning trades) ───────────────────────────────────
        if cfg.HOLD_TO_EXPIRY and not stop_triggered and not force_exit:
            if secs_left is not None and secs_left < 90:
                unrealized = self._unrealized_pnl(state, mark_yes)
                if unrealized > 0:
                    log_event("HOLD_TO_EXPIRY", {
                        "unrealized": round(unrealized, 4),
                        "secs_left": round(secs_left),
                    }, throttle_key="hold_expiry", throttle_s=30)
                    return False

        # ── Standard exit condition ────────────────────────────────────────────
        should_exit = stop_triggered or force_exit
        if not should_exit:
            if side == "YES":
                should_exit = signal.edge <= cfg.EDGE_EXIT
            else:
                should_exit = signal.edge >= -cfg.EDGE_EXIT

        if not should_exit:
            return False

        # ── Place exit order ───────────────────────────────────────────────────
        if side == "YES":
            exit_cents = max(1, min(99, book.best_yes_bid or int(mark_yes * 100)))
        else:
            exit_cents = max(1, min(99, book.best_no_bid or int((1.0 - mark_yes) * 100)))

        result = self._executor.place_order("sell", ticker, side, contracts, exit_cents)

        if not result.success:
            log_event("EXIT_FAILED", {"err": result.error, "ticker": ticker, "side": side})
            return False

        exit_price = exit_cents / 100.0
        pnl = (exit_price - entry_price) * contracts

        # Update state
        state.cash += exit_price * contracts
        state.realized_pnl_lifetime += pnl

        # Streak tracking
        if pnl > 0:
            state.win_streak = max(1, state.win_streak + 1)
        else:
            state.win_streak = min(-1, state.win_streak - 1)

        # Clear position
        old_side = state.position_side
        state.position_side = None
        state.position_entry_price = None
        state.position_contracts = 0
        state.position_ticker = None
        state.position_open_ts = None
        state.last_trade_ts = utc_iso()

        StateStore.record_trade(state, "EXIT", pnl)

        # Reconcile with real balance in live mode
        if cfg.LIVE_MODE:
            self._store.sync_real_balance(state)

        self._store.push_equity_to_postgres(state)
        self._risk.update_peak_equity(state.cash)

        _, daily_pnl = self._get_equity_summary(state)

        log_event("EXIT", {
            "ticker": ticker,
            "side": old_side,
            "contracts": contracts,
            "entry_price": round(entry_price, 4),
            "exit_price": round(exit_price, 4),
            "pnl": round(pnl, 4),
            "cash_after": round(state.cash, 4),
            "realized_pnl_lifetime": round(state.realized_pnl_lifetime, 4),
            "stop_triggered": stop_triggered,
            "force_exit": force_exit,
            "live": cfg.LIVE_MODE,
        })

        # Only notify on winning trades
        if pnl > 0:
            send_telegram(
                f"🟢 +${pnl:.2f}\n"
                f"💵 Balance: ${state.cash:.2f}\n"
                f"📅 Daily PnL: ${daily_pnl:.2f}\n"
                f"🏦 Lifetime: ${state.realized_pnl_lifetime:.2f}"
            )
        return True

    def _size_position(self, cash: float, entry_price: float, signal: Signal) -> int:
        """Compute contract count using fractional Kelly + hard caps."""
        cfg = self.cfg
        # Kelly fraction already applied in risk engine; use simple risk_fraction here
        risk_usd = min(cfg.MAX_POSITION_USD, max(0.5, cash * cfg.RISK_FRACTION))
        contracts = int(risk_usd / max(entry_price, 0.01))
        return max(1, min(cfg.MAX_CONTRACTS_PER_TRADE, contracts))

    @staticmethod
    def _unrealized_pnl(state: BotState, mark_yes: float) -> float:
        if not state.has_position or state.position_entry_price is None:
            return 0.0
        if state.position_side == "YES":
            return (mark_yes - state.position_entry_price) * state.position_contracts
        else:
            return ((1.0 - mark_yes) - state.position_entry_price) * state.position_contracts

    @staticmethod
    def _get_equity_summary(state: BotState) -> tuple:
        trades = len(state.trade_history_24h or [])
        pnl = StateStore.daily_pnl(state)
        return trades, pnl

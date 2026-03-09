#!/usr/bin/env python3
"""
Johnny5-Kalshi BTC15M — Production Bot v11
Entry point. Wires together all core modules and runs the main loop.

Architecture:
  main.py
    -> core/config.py        (all env-driven configuration)
    -> core/kalshi_client.py (Kalshi API read layer)
    -> core/market_selector.py (pick the live KXBTC15M bucket)
    -> core/feature_pipeline.py (BTC price feed + feature computation)
    -> core/signal_engine.py  (calibrated probability model)
    -> core/risk_engine.py    (deterministic risk gate — must pass before any trade)
    -> core/execution_engine.py (order routing via Bun gateway)
    -> core/position_manager.py (entry/exit lifecycle)
    -> core/state_store.py    (file + Postgres persistence)
    -> core/notifier.py       (Telegram + structured logging)
    -> core/scheduler.py      (health server + main loop timing)

LIVE_MODE=false → paper trading (default, safe)
LIVE_MODE=true  → real money (requires all credentials set)
"""
from __future__ import annotations

import json
import sys
import time

from core.config import cfg
from core.feature_pipeline import FeaturePipeline
from core.kalshi_client import KalshiClient
from core.market_selector import pick_best_active_market
from core.notifier import log_event, send_telegram
from core.position_manager import PositionManager
from core.risk_engine import RiskEngine
from core.scheduler import start_health_server, utc_iso
from core.signal_engine import SignalEngine
from core.state_store import StateStore


def main() -> None:
    log_event("BOOT", {
        "version": cfg.BOT_VERSION,
        "live": cfg.LIVE_MODE,
        "series": cfg.SERIES_TICKER,
        "poll_s": cfg.POLL_SECONDS,
        "edge_enter": cfg.EDGE_ENTER,
        "edge_exit": cfg.EDGE_EXIT,
        "kill_switch": cfg.KILL_SWITCH,
        "dry_run": cfg.DRY_RUN,
    })

    start_health_server(cfg.PORT)

    store = StateStore(cfg.STATE_FILE, cfg.GATEWAY_URL)
    state = store.load()
    store.sync_equity_from_postgres(state)

    send_telegram(
        f"🤖 Johnny5 v11 STARTED\n"
        f"Mode: {'🔴 LIVE' if cfg.LIVE_MODE else '🧪 PAPER'}\n"
        f"Series: {cfg.SERIES_TICKER}\n"
        f"Cash: ${state.cash:.2f} | Lifetime PnL: ${state.realized_pnl_lifetime:.2f}\n"
        f"Kill switch: {cfg.KILL_SWITCH}"
    )

    client = KalshiClient(cfg)
    features = FeaturePipeline(cfg)
    signal_engine = SignalEngine(cfg)
    risk_engine = RiskEngine(cfg)
    position_mgr = PositionManager(cfg, client, store)

    consecutive_errors = 0

    while True:
        try:
            # ── 1. Fetch BTC price and update feature pipeline ─────────────
            btc_price = features.fetch_btc_price()
            if btc_price is None:
                log_event("NO_BTC_PRICE", {}, throttle_key="no_btc", throttle_s=30)
                time.sleep(max(5.0, cfg.POLL_SECONDS))
                continue

            features.update(btc_price)

            # ── 2. Discover live market ────────────────────────────────────
            markets = client.list_open_markets()
            market = pick_best_active_market(markets, cfg.SERIES_TICKER)
            if market is None:
                log_event("NO_MARKET", {"series": cfg.SERIES_TICKER}, throttle_key="no_mkt", throttle_s=60)
                store.save(state)
                time.sleep(max(5.0, cfg.POLL_SECONDS))
                continue

            ticker = market["ticker"]
            secs_left = market.get("secs_left")

            # ── 3. Update bucket tracking ──────────────────────────────────
            features.update_bucket(ticker, btc_price, secs_left)

            # ── 4. Fetch orderbook ─────────────────────────────────────────
            ob = client.get_orderbook(ticker)
            book = features.parse_orderbook(ob)

            if book.mark_yes is None:
                log_event("BAD_BOOK", {"ticker": ticker}, throttle_key="bad_book", throttle_s=30)
                store.save(state)
                time.sleep(max(3.0, cfg.POLL_SECONDS))
                continue

            # ── 5. Compute signal ──────────────────────────────────────────
            signal = signal_engine.compute(features.get_snapshot(), book)

            # ── 6. Exit check (before entry) ───────────────────────────────
            if state.has_position:
                position_mgr.maybe_exit(state, ticker, book, signal, secs_left)

            # ── 7. Entry check ─────────────────────────────────────────────
            if not state.has_position and not cfg.KILL_SWITCH:
                risk_result = risk_engine.check(state, signal, book, secs_left)
                if risk_result.approved:
                    position_mgr.enter(state, ticker, book, signal)
                else:
                    log_event("RISK_BLOCK", {
                        "ticker": ticker,
                        "reasons": risk_result.reasons,
                        "edge": round(signal.edge, 4),
                    }, throttle_key="risk_block", throttle_s=20)

            store.save(state)
            consecutive_errors = 0
            time.sleep(max(1.0, cfg.POLL_SECONDS))

        except KeyboardInterrupt:
            log_event("SHUTDOWN", {"reason": "keyboard_interrupt"})
            store.save(state)
            send_telegram("🛑 Johnny5 stopped by operator (KeyboardInterrupt)")
            sys.exit(0)

        except Exception as exc:
            consecutive_errors += 1
            log_event("ERROR", {
                "err": str(exc)[:400],
                "consecutive": consecutive_errors,
            }, throttle_key="main_err", throttle_s=10)

            # Circuit breaker: pause longer after repeated failures
            if consecutive_errors >= cfg.CIRCUIT_BREAKER_THRESHOLD:
                log_event("CIRCUIT_BREAKER", {"consecutive_errors": consecutive_errors})
                send_telegram(f"⚠️ Johnny5 CIRCUIT BREAKER: {consecutive_errors} consecutive errors. Pausing.")
                time.sleep(min(300, consecutive_errors * 15))
            else:
                time.sleep(max(10.0, cfg.POLL_SECONDS))

            try:
                store.save(state)
            except Exception:
                pass


if __name__ == "__main__":
    main()

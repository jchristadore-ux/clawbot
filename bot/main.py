#!/usr/bin/env python3
"""
main.py — Johnny5 v13
Entry point. Wires together all core modules and runs the main loop.

v13 CHANGE vs v12: Exponential BAD_BOOK backoff + rate-limit sleep.

Root cause fixed: the poll loop was hitting the Kalshi API at full speed
(every 8s) even during dead orderbook sessions. This caused 81 RATE_LIMITED
events on Mar 16 with zero trades. v13 adds exponential backoff on
consecutive BAD_BOOKs and an explicit sleep after any 429 response.

Backoff: each consecutive BAD_BOOK on the same ticker adds
BAD_BOOK_BACKOFF_BASE seconds (default 2s), capped at BAD_BOOK_BACKOFF_MAX
(default 60s). Resets on any new bucket or good book.
"""
from __future__ import annotations

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
        "min_z": cfg.MIN_CONVICTION_Z,
        "bad_book_backoff_base": cfg.BAD_BOOK_BACKOFF_BASE,
        "bad_book_backoff_max": cfg.BAD_BOOK_BACKOFF_MAX,
        "kill_switch": cfg.KILL_SWITCH,
        "dry_run": cfg.DRY_RUN,
    })

    start_health_server(cfg.PORT)

    store = StateStore(cfg.STATE_FILE, cfg.GATEWAY_URL)
    state = store.load()
    store.sync_equity_from_postgres(state)

    client = KalshiClient(cfg)
    features = FeaturePipeline(cfg)
    signal_engine = SignalEngine(cfg)
    risk_engine = RiskEngine(cfg)
    position_mgr = PositionManager(cfg, client, store)

    consecutive_errors = 0
    bad_book_streak = 0
    last_ticker = None

    while True:
        loop_start = time.monotonic()
        try:
            # ── 1. Kill switch ─────────────────────────────────────────────
            if cfg.KILL_SWITCH:
                if state.has_position and cfg.CLOSE_ON_KILL_SWITCH:
                    log_event("KILL_SWITCH_CLOSE", {})
                else:
                    log_event("KILL_SWITCH_IDLE", {}, throttle_key="ks_idle", throttle_s=300)
                time.sleep(cfg.POLL_SECONDS)
                continue

            # ── 2. BTC price ───────────────────────────────────────────────
            btc_price = features.fetch_btc_price()
            if btc_price is None:
                log_event("NO_BTC_PRICE", {}, throttle_key="no_btc", throttle_s=30)
                time.sleep(max(5.0, cfg.POLL_SECONDS))
                continue

            features.update(btc_price)

            # ── 3. Market discovery ────────────────────────────────────────
            markets = client.list_open_markets()
            market = pick_best_active_market(markets, cfg.SERIES_TICKER)
            if market is None:
                log_event("NO_MARKET", {"series": cfg.SERIES_TICKER})
                bad_book_streak = 0
                last_ticker = None
                store.save(state)
                time.sleep(max(5.0, cfg.POLL_SECONDS))
                continue

            ticker = market["ticker"]
            secs_left = market.get("secs_left")

            # ── 4. Bucket tracking ─────────────────────────────────────────
            features.update_bucket(ticker, btc_price, secs_left)

            # Reset backoff streak on new bucket
            if ticker != last_ticker:
                bad_book_streak = 0
                last_ticker = ticker
                risk_engine.purge_stale_buckets(ticker)

            # ── 5. Orderbook ───────────────────────────────────────────────
            ob = client.get_orderbook(ticker)
            book = features.parse_orderbook(ob)

            if book.mark_yes is None:
                # BAD_BOOK — exponential backoff
                bad_book_streak += 1
                extra_sleep = min(
                    cfg.BAD_BOOK_BACKOFF_BASE * bad_book_streak,
                    cfg.BAD_BOOK_BACKOFF_MAX
                )
                log_event("BAD_BOOK", {
                    "ticker": ticker,
                    "streak": bad_book_streak,
                    "backoff_s": round(extra_sleep, 1),
                })
                store.save(state)
                elapsed = time.monotonic() - loop_start
                time.sleep(max(0, cfg.POLL_SECONDS + extra_sleep - elapsed))
                continue

            # Good book — reset streak
            bad_book_streak = 0
            consecutive_errors = 0

            # ── 6. Signal ──────────────────────────────────────────────────
            signal = signal_engine.compute(features.get_snapshot(), book)

            # ── 7. Exit check ──────────────────────────────────────────────
            if state.has_position:
                position_mgr.maybe_exit(state, ticker, book, signal, secs_left)

            # ── 8. Entry check ─────────────────────────────────────────────
            elif not state.has_position:
                risk_result = risk_engine.check(state, signal, book, secs_left)
                if risk_result.approved:
                    position_mgr.enter(state, ticker, book, signal, risk_result)
                else:
                    log_event("RISK_BLOCK", {
                        "ticker": ticker,
                        "reasons": risk_result.reasons,
                        "edge": round(signal.edge, 4),
                    }, throttle_key="risk_block", throttle_s=20)

            store.save(state)

        except KeyboardInterrupt:
            log_event("SHUTDOWN", {"reason": "keyboard_interrupt"})
            store.save(state)
            send_telegram("🛑 Johnny5 stopped (KeyboardInterrupt)")
            sys.exit(0)

        except Exception as exc:
            consecutive_errors += 1
            err_str = str(exc)

            # Rate limit — back off immediately
            is_rate_limited = "429" in err_str or ("rate" in err_str.lower() and "limit" in err_str.lower())
            if is_rate_limited:
                log_event("RATE_LIMITED", {
                    "err": err_str[:100],
                    "backoff_s": cfg.RATE_LIMIT_BACKOFF,
                })
                time.sleep(cfg.RATE_LIMIT_BACKOFF)
                continue

            log_event("ERROR", {
                "err": err_str[:400],
                "consecutive": consecutive_errors,
            }, throttle_key="main_err", throttle_s=10)

            if consecutive_errors >= cfg.CIRCUIT_BREAKER_THRESHOLD:
                log_event("CIRCUIT_BREAKER", {"consecutive_errors": consecutive_errors})
                send_telegram(f"⚠️ Johnny5 CIRCUIT BREAKER: {consecutive_errors} consecutive errors.")
                consecutive_errors = 0
                time.sleep(min(300, consecutive_errors * 15))
            else:
                time.sleep(max(10.0, cfg.POLL_SECONDS))

            try:
                store.save(state)
            except Exception:
                pass

        # ── Standard poll sleep ────────────────────────────────────────────
        elapsed = time.monotonic() - loop_start
        time.sleep(max(0, cfg.POLL_SECONDS - elapsed))


if __name__ == "__main__":
    main()

"""
main.py — Johnny5 v13 poll loop.

v13 KEY CHANGE: Exponential BAD_BOOK backoff + rate-limit sleep.

The Mar 16 logs showed 81 RATE_LIMITED hits in a single day with zero trades.
Root cause: the poll loop ran at full speed (poll every 8s) even when every
single orderbook check returned BAD_BOOK. This burned ~450 API calls/hour on
empty books.

Fix: track consecutive BAD_BOOK count per ticker. After each BAD_BOOK,
sleep an additional BAD_BOOK_BACKOFF_BASE * consecutive seconds, capped at
BAD_BOOK_BACKOFF_MAX. Reset on new bucket or good book. Also sleep
RATE_LIMIT_BACKOFF seconds immediately after any 429 response.

This reduces dead-session API calls from ~450/hr to ~60/hr while keeping
reaction time to good books at < 10s.
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone

from core.config import cfg
from core.feature_pipeline import FeaturePipeline
from core.kalshi_client import KalshiClient
from core.market_selector import MarketSelector
from core.notifier import log_event, send_telegram
from core.position_manager import PositionManager
from core.risk_engine import RiskEngine
from core.scheduler import start_health_server, utc_iso
from core.signal_engine import SignalEngine
from core.state_store import BotState, StateStore


def main() -> None:
    warnings = cfg.validate()
    for w in warnings:
        log_event("CONFIG_WARNING", {"msg": w})

    log_event("BOOT", {
        "version": cfg.BOT_VERSION,
        "live": cfg.LIVE_MODE,
        "series": cfg.SERIES_TICKER,
        "poll_s": cfg.POLL_SECONDS,
        "edge_enter": cfg.EDGE_ENTER,
        "min_z": cfg.MIN_CONVICTION_Z,
        "bad_book_backoff_base": cfg.BAD_BOOK_BACKOFF_BASE,
        "bad_book_backoff_max": cfg.BAD_BOOK_BACKOFF_MAX,
        "kill_switch": cfg.KILL_SWITCH,
    })

    start_health_server(cfg.PORT)

    client = KalshiClient(cfg)
    store = StateStore(cfg.STATE_FILE, cfg.GATEWAY_URL)
    state = store.load()
    store.sync_equity_from_postgres(state)

    selector = MarketSelector(cfg, client)
    features = FeaturePipeline(cfg)
    signal_engine = SignalEngine(cfg)
    risk_engine = RiskEngine(cfg)
    pos_manager = PositionManager(cfg, client, store)

    consecutive_errors = 0
    bad_book_streak = 0       # consecutive BAD_BOOK on current ticker
    last_ticker = None

    if cfg.LIVE_MODE:
        send_telegram(f"🤖 Johnny5 {cfg.BOT_VERSION} started — LIVE MODE")
    else:
        log_event("PAPER_MODE", {"note": "LIVE_MODE=false — no real orders"})

    while True:
        loop_start = time.monotonic()

        try:
            if cfg.KILL_SWITCH:
                if state.has_position and cfg.CLOSE_ON_KILL_SWITCH:
                    log_event("KILL_SWITCH_CLOSE", {})
                    # position manager will handle close on next book read
                else:
                    log_event("KILL_SWITCH_IDLE", {}, throttle_key="ks_idle", throttle_s=300)
                    time.sleep(cfg.POLL_SECONDS)
                    continue

            # ── Market selection ───────────────────────────────────────────
            ticker, secs_left = selector.get_active_ticker()
            if not ticker:
                log_event("NO_MARKET", {"series": cfg.SERIES_TICKER})
                bad_book_streak = 0
                last_ticker = None
                time.sleep(cfg.POLL_SECONDS)
                continue

            if ticker != last_ticker:
                log_event("NEW_BUCKET", {
                    "ticker": ticker,
                    "btc_open": round(features.current_btc_price or 0, 1),
                    "secs_left": round(secs_left or 0),
                })
                bad_book_streak = 0
                last_ticker = ticker
                risk_engine.purge_stale_buckets(ticker)

            # ── Orderbook ─────────────────────────────────────────────────
            book = client.get_orderbook(ticker)
            if book is None or not book.is_tradeable(cfg):
                bad_book_streak += 1
                # Exponential backoff: base * streak, capped at max
                extra_sleep = min(
                    cfg.BAD_BOOK_BACKOFF_BASE * bad_book_streak,
                    cfg.BAD_BOOK_BACKOFF_MAX
                )
                log_event("BAD_BOOK", {
                    "ticker": ticker,
                    "streak": bad_book_streak,
                    "backoff_s": round(extra_sleep, 1),
                })
                elapsed = time.monotonic() - loop_start
                sleep_for = max(0, cfg.POLL_SECONDS + extra_sleep - elapsed)
                time.sleep(sleep_for)
                continue

            # Good book — reset backoff
            bad_book_streak = 0
            consecutive_errors = 0

            # ── BTC price feed ─────────────────────────────────────────────
            btc = client.get_btc_price()
            if btc:
                features.update_price(btc)

            # ── Feature snapshot ───────────────────────────────────────────
            snap = features.snapshot(ticker, secs_left)

            # ── Open position: check exit first ───────────────────────────
            if state.has_position and state.position_ticker == ticker:
                signal = signal_engine.compute(snap, book)
                exited = pos_manager.maybe_exit(state, ticker, book, signal, secs_left)
                if exited:
                    store.save(state)

            # ── No position: evaluate entry ────────────────────────────────
            elif not state.has_position:
                if snap.is_warmed_up:
                    signal = signal_engine.compute(snap, book)
                    risk = risk_engine.check(state, signal, book, secs_left)

                    if risk.approved:
                        entered = pos_manager.enter(state, ticker, book, signal, risk)
                        if entered:
                            store.save(state)
                    else:
                        log_event("RISK_BLOCK", {
                            "ticker": ticker,
                            "reasons": risk.reasons,
                            "edge": round(signal.edge, 4) if signal else None,
                        }, throttle_key=f"risk_{ticker}", throttle_s=30)
                else:
                    log_event("WARMUP", {
                        "prices": snap.price_count,
                        "needed": cfg.LOOKBACK_SHORT,
                    }, throttle_key="warmup", throttle_s=60)

        except KeyboardInterrupt:
            log_event("SHUTDOWN", {"reason": "keyboard_interrupt"})
            store.save(state)
            sys.exit(0)

        except Exception as exc:
            consecutive_errors += 1
            err_str = str(exc)

            # Detect rate limiting from exception message
            is_rate_limited = "429" in err_str or "rate" in err_str.lower()

            if is_rate_limited:
                log_event("RATE_LIMITED", {
                    "err": err_str[:100],
                    "backoff_s": cfg.RATE_LIMIT_BACKOFF,
                })
                time.sleep(cfg.RATE_LIMIT_BACKOFF)
                continue

            log_event("ERROR", {"err": err_str[:200], "consecutive": consecutive_errors})

            if consecutive_errors >= cfg.CIRCUIT_BREAKER_THRESHOLD:
                log_event("CIRCUIT_BREAKER", {
                    "consecutive_errors": consecutive_errors,
                    "last_err": err_str[:100],
                })
                send_telegram(f"🔴 Johnny5 circuit breaker triggered: {err_str[:80]}")
                consecutive_errors = 0
                time.sleep(60)
                continue

        # ── Standard poll interval ─────────────────────────────────────────
        elapsed = time.monotonic() - loop_start
        sleep_for = max(0, cfg.POLL_SECONDS - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()

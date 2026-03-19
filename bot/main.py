#!/usr/bin/env python3
"""
main.py — Johnny5 v13 (patched)

Changes from v13 initial:
  1. NO_MARKET backoff — same exponential logic as BAD_BOOK.
     The 06:00-09:00 UTC gap on Mar 19 produced 1,354 NO_MARKET events
     in 90 minutes (8 calls/min). Now backs off on consecutive NO_MARKETs.

  2. MAX_SECS_BEFORE_EXPIRY raised to 900s in config. Separately, the
     bucket_too_new check now uses a SOFT block (logs SKIP_NEW_BUCKET)
     instead of accumulating in the RISK_BLOCK reasons — so the bot
     still polls the book and can enter when secs_left drops into range
     within the same bucket. This fixes the case where 18/30 buckets
     were opening with secs_left > 860 and being silently blocked.

  3. SIGNAL debug logging added when DEBUG_MODEL=true — emits signal
     quality every 30s when book is good so we can see exactly what
     z/edge/conviction the model is producing without a trade firing.
     Without this we're flying blind on whether the signal itself is
     the issue vs the filters.
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
        "max_secs": cfg.MAX_SECS_BEFORE_EXPIRY,
        "bad_book_backoff_base": cfg.BAD_BOOK_BACKOFF_BASE,
        "bad_book_backoff_max": cfg.BAD_BOOK_BACKOFF_MAX,
        "no_market_backoff_max": cfg.NO_MARKET_BACKOFF_MAX,
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
    no_market_streak = 0
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
                no_market_streak += 1
                extra_sleep = min(
                    cfg.BAD_BOOK_BACKOFF_BASE * no_market_streak,
                    cfg.NO_MARKET_BACKOFF_MAX,
                )
                log_event("NO_MARKET", {
                    "series": cfg.SERIES_TICKER,
                    "streak": no_market_streak,
                    "backoff_s": round(extra_sleep, 1),
                })
                bad_book_streak = 0
                last_ticker = None
                store.save(state)
                elapsed = time.monotonic() - loop_start
                time.sleep(max(0, cfg.POLL_SECONDS + extra_sleep - elapsed))
                continue

            # Good market — reset NO_MARKET streak
            no_market_streak = 0

            ticker = market["ticker"]
            secs_left = market.get("secs_left")

            # ── 4. Bucket tracking ─────────────────────────────────────────
            features.update_bucket(ticker, btc_price, secs_left)

            if ticker != last_ticker:
                bad_book_streak = 0
                last_ticker = ticker
                risk_engine.purge_stale_buckets(ticker)

            # ── 5. Orderbook ───────────────────────────────────────────────
            ob = client.get_orderbook(ticker)
            book = features.parse_orderbook(ob)

            if book.mark_yes is None:
                bad_book_streak += 1
                extra_sleep = min(
                    cfg.BAD_BOOK_BACKOFF_BASE * bad_book_streak,
                    cfg.BAD_BOOK_BACKOFF_MAX,
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

            # Debug signal even when no trade fires — critical for diagnosis
            if cfg.DEBUG_MODEL:
                log_event("SIGNAL_PROBE", {
                    "ticker": ticker,
                    "fair": round(signal.fair_yes, 4),
                    "mark": round(signal.mark_yes, 4),
                    "edge": round(signal.edge, 4),
                    "z": round(signal.z, 3),
                    "side": signal.side,
                    "entry_cents": signal.entry_cents,
                    "secs_left": round(secs_left) if secs_left else None,
                }, throttle_key="sig_probe", throttle_s=30)

            # ── 7. Exit check ──────────────────────────────────────────────
            if state.has_position:
                position_mgr.maybe_exit(state, ticker, book, signal, secs_left)

            # ── 8. Entry check ─────────────────────────────────────────────
            elif not state.has_position:
                # Soft bucket-too-new check: log and skip but keep polling
                # (old hard check in risk_engine blocked for entire bucket)
                if secs_left is not None and secs_left > cfg.MAX_SECS_BEFORE_EXPIRY:
                    log_event("SKIP_NEW_BUCKET", {
                        "secs_left": round(secs_left),
                        "max": cfg.MAX_SECS_BEFORE_EXPIRY,
                    }, throttle_key=f"skip_new_{ticker}", throttle_s=60)
                else:
                    risk_result = risk_engine.check(state, signal, book, secs_left)
                    if risk_result.approved:
                        position_mgr.enter(state, ticker, book, signal, risk_result)
                    else:
                        log_event("RISK_BLOCK", {
                            "ticker": ticker,
                            "reasons": risk_result.reasons,
                            "edge": round(signal.edge, 4),
                            "z": round(signal.z, 3),
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

from datetime import datetime, timezone

from clawbot import config
from clawbot.alerts import FailureTracker, maybe_alert_on_failures, send_alert
from clawbot.data import current_btc_5m_slug, fetch_btc_closes_5m, fetch_polymarket_marks
from clawbot.execution import place_live_order, reconcile_order
from clawbot.risk import should_block_enter
from clawbot.state import (
    calc_unrealized_pnl,
    init_db,
    load_state,
    log_trade,
    paper_trade_step,
    reserve_interval,
    reset_daily_counters_if_needed,
    save_state,
)
from clawbot.strategy import build_signal, calc_edge, marks_look_sane_for_live, prob_from_closes


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _act_live(signal: str, p_up: float, p_down: float, stake: float, symbol: str):
    if signal not in ("YES", "NO"):
        return "HOLD", None, None
    px = p_up if signal == "YES" else p_down
    res = place_live_order(side=signal, price=px, stake=stake, symbol=symbol)
    rec = reconcile_order(res.order_id) if res.order_id else res
    log_trade("LIVE_ORDER", side=signal, price=px, stake=stake, order_id=res.order_id, order_status=rec.status)
    return f"LIVE_{rec.status.upper()}", res.order_id, rec.status


def main():
    init_db()
    failure_tracker = FailureTracker()

    interval_key = current_btc_5m_slug()
    if not reserve_interval(interval_key):
        print(f"{utc_now_iso()} | INFO | duplicate interval skipped | interval_key={interval_key}", flush=True)
        return

    state = reset_daily_counters_if_needed(load_state())
    closes = fetch_btc_closes_5m(config.LOOKBACK)
    if closes is None:
        msg = maybe_alert_on_failures(failure_tracker, "kraken_ohlc_fetch_failed")
        if msg:
            print(f"{utc_now_iso()} | ALERT | {msg}", flush=True)
        print(f"{utc_now_iso()} | INFO | no market closes", flush=True)
        return
    failure_tracker.success()

    p_up, p_down, source = (None, None, "synthetic")
    pm = fetch_polymarket_marks(interval_key)
    if pm is not None:
        p_up, p_down, source = pm
    else:
        p_up = prob_from_closes(closes)
        p_down = 1.0 - p_up

    fair_up = prob_from_closes(closes)
    edge = calc_edge(fair_up, p_up)

    balance = float(state["balance"])
    position = state["position"]
    entry_price = float(state["entry_price"]) if state["entry_price"] is not None else None
    stake = float(state["stake"])
    trades_today = int(state.get("trades_today", 0))
    realized_pnl_today = float(state.get("realized_pnl_today", 0.0))
    last_trade_ts = state.get("last_trade_ts")
    last_trade_day = state.get("last_trade_day")

    if config.KILL_SWITCH:
        signal = "HOLD"
    elif config.LIVE_MODE and not marks_look_sane_for_live(p_up, p_down):
        signal = "HOLD"
        send_alert(f"marks sanity failed p_up={p_up:.3f} p_down={p_down:.3f}", level="warning")
    elif config.LIVE_MODE and config.LIVE_DAILY_PROFIT_LOCK > 0 and realized_pnl_today >= config.LIVE_DAILY_PROFIT_LOCK:
        signal = "HOLD"
        send_alert("daily profit lock reached", level="info")
    else:
        signal = build_signal(edge=edge, kill_switch=False)

    blocked = should_block_enter(position, signal, edge, trades_today, realized_pnl_today, last_trade_ts)
    if blocked:
        send_alert(f"risk gate blocked enter: {blocked}", level="warning")
        action = f"BLOCKED_{blocked}"
    elif config.LIVE_MODE:
        action, _, _ = _act_live(signal, p_up, p_down, config.effective_trade_size(), interval_key)
    else:
        (
            balance,
            position,
            entry_price,
            stake,
            action,
            trades_today,
            realized_pnl_today,
            last_trade_ts,
            last_trade_day,
        ) = paper_trade_step(
            balance=balance,
            position=position,
            entry_price=entry_price,
            stake=stake,
            signal=signal,
            p_up=p_up,
            p_down=p_down,
            edge=edge,
            edge_exit=config.effective_edge_exit(),
            trades_today=trades_today,
            realized_pnl_today=realized_pnl_today,
            last_trade_ts=last_trade_ts,
            last_trade_day=last_trade_day,
        )

    mark = p_up if position == "YES" else (p_down if position == "NO" else p_up)
    unrealized = calc_unrealized_pnl(position, entry_price, mark, stake)
    equity = balance + unrealized

    save_state(balance, position, entry_price, stake, last_trade_ts, last_trade_day, trades_today, realized_pnl_today, p_up)
    print(
        f"{utc_now_iso()} | src={source} | signal={signal} | action={action} | "
        f"edge={edge:+.3f} | balance={balance:.2f} | equity={equity:.2f} | pos={position}",
        flush=True,
    )


if __name__ == "__main__":
    main()

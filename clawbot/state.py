from datetime import date, datetime, timezone
from typing import Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

from clawbot import config


def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def utc_today_date() -> date:
    return utc_now_dt().date()


def db_conn():
    if not config.DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(config.DATABASE_URL)


def init_db():
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS paper_state (
              id INTEGER PRIMARY KEY,
              balance DOUBLE PRECISION NOT NULL,
              position TEXT NULL,
              entry_price DOUBLE PRECISION NULL,
              stake DOUBLE PRECISION NOT NULL,
              last_trade_ts TIMESTAMPTZ,
              last_trade_day DATE,
              trades_today INTEGER NOT NULL DEFAULT 0,
              realized_pnl_today DOUBLE PRECISION NOT NULL DEFAULT 0,
              last_mark DOUBLE PRECISION,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades (
              trade_id BIGSERIAL PRIMARY KEY,
              ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              action TEXT NOT NULL,
              side TEXT NULL,
              price DOUBLE PRECISION NULL,
              stake DOUBLE PRECISION NULL,
              fee DOUBLE PRECISION NULL,
              pnl DOUBLE PRECISION NULL,
              order_id TEXT NULL,
              order_status TEXT NULL
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS processed_intervals (
              interval_key TEXT PRIMARY KEY,
              processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
        conn.commit()


def load_state() -> dict:
    with db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM paper_state WHERE id=1")
            row = cur.fetchone()
            if row:
                return row
            cur.execute(
                """
                INSERT INTO paper_state (id, balance, position, entry_price, stake, last_trade_ts, last_trade_day, trades_today, realized_pnl_today, last_mark)
                VALUES (1, %s, NULL, NULL, 0, NULL, NULL, 0, 0, NULL)
                RETURNING *
                """,
                (config.START_BALANCE,),
            )
            row = cur.fetchone()
        conn.commit()
    return row


def save_state(balance, position, entry_price, stake, last_trade_ts, last_trade_day, trades_today, realized_pnl_today, last_mark):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE paper_state
                SET balance=%s, position=%s, entry_price=%s, stake=%s,
                    last_trade_ts=%s, last_trade_day=%s, trades_today=%s,
                    realized_pnl_today=%s, last_mark=%s, updated_at=NOW()
                WHERE id=1
                """,
                (balance, position, entry_price, stake, last_trade_ts, last_trade_day, trades_today, realized_pnl_today, last_mark),
            )
        conn.commit()


def log_trade(action: str, side=None, price=None, stake=None, fee=None, pnl=None, order_id=None, order_status=None):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO paper_trades (action, side, price, stake, fee, pnl, order_id, order_status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (action, side, price, stake, fee, pnl, order_id, order_status),
            )
        conn.commit()


def reserve_interval(interval_key: str) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO processed_intervals (interval_key) VALUES (%s) ON CONFLICT DO NOTHING",
                (interval_key,),
            )
            inserted = cur.rowcount == 1
        conn.commit()
    return inserted


def reset_daily_counters_if_needed(state: dict) -> dict:
    today = utc_today_date()
    if state.get("last_trade_day") != today:
        state["last_trade_day"] = today
        state["trades_today"] = 0
        state["realized_pnl_today"] = 0.0
    return state


def shares_for_stake(stake: float, price: float) -> float:
    return 0.0 if price <= 0 else (stake / price)


def calc_unrealized_pnl(position: Optional[str], entry_price: Optional[float], mark: float, stake: float) -> float:
    if position is None or entry_price is None or stake <= 0:
        return 0.0
    return (shares_for_stake(stake, entry_price) * mark) - stake


def calc_realized_pnl(entry_price: float, exit_price: float, stake: float) -> float:
    return (shares_for_stake(stake, entry_price) * exit_price) - stake


def paper_trade_step(balance: float, position: Optional[str], entry_price: Optional[float], stake: float, signal: str, p_up: float, p_down: float, edge: float, edge_exit: float, trades_today: int, realized_pnl_today: float, last_trade_ts, last_trade_day) -> Tuple:
    now = utc_now_dt()
    today = utc_today_date()

    if position is None and signal == "HOLD":
        return balance, position, entry_price, stake, "HOLD", trades_today, realized_pnl_today, last_trade_ts, last_trade_day

    if position is None:
        trade_size = config.effective_trade_size()
        if balance < trade_size + config.FEE_PER_TRADE:
            return balance, position, entry_price, stake, "SKIP_INSUFFICIENT_BALANCE", trades_today, realized_pnl_today, last_trade_ts, last_trade_day
        balance -= (trade_size + config.FEE_PER_TRADE)
        position = signal
        stake = trade_size
        entry_price = p_up if signal == "YES" else p_down
        log_trade("ENTER", side=position, price=entry_price, stake=stake, fee=config.FEE_PER_TRADE)
        return balance, position, entry_price, stake, f"ENTER_{signal}", trades_today + 1, realized_pnl_today, now, today

    edge_collapse = (position == "YES" and edge < edge_exit) or (position == "NO" and edge > -edge_exit)
    if (signal != "HOLD" and signal != position) or edge_collapse:
        exit_price = p_up if position == "YES" else p_down
        pnl = calc_realized_pnl(entry_price, exit_price, stake)
        balance += stake + pnl - config.FEE_PER_TRADE
        log_trade("EXIT", side=position, price=exit_price, stake=stake, fee=config.FEE_PER_TRADE, pnl=pnl)
        return balance, None, None, 0.0, "EXIT", trades_today + 1, realized_pnl_today + pnl, now, today

    return balance, position, entry_price, stake, "HOLD_SAME_SIDE", trades_today, realized_pnl_today, last_trade_ts, last_trade_day

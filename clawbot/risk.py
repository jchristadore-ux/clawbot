from datetime import datetime, timedelta, timezone
from typing import Optional

from clawbot import config


def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def cooldown_active(last_trade_ts, cooldown_minutes: int) -> bool:
    if last_trade_ts is None:
        return False
    try:
        return utc_now_dt() < (last_trade_ts + timedelta(minutes=cooldown_minutes))
    except Exception:
        return False


def should_block_enter(
    position: Optional[str],
    signal: str,
    edge: float,
    trades_today: int,
    realized_pnl_today: float,
    last_trade_ts,
) -> Optional[str]:
    if position is not None or signal not in ("YES", "NO"):
        return None

    if realized_pnl_today <= -config.effective_max_daily_loss():
        return "MAX_DAILY_LOSS"
    if trades_today >= config.effective_max_trades_per_day():
        return "MAX_TRADES_PER_DAY"

    cd_active = cooldown_active(last_trade_ts, config.effective_cooldown_minutes())
    whale = abs(edge) >= config.WHALE_EDGE_OVERRIDE
    if cd_active and not (config.WHALE_COOLDOWN_OVERRIDE and whale):
        return "COOLDOWN"
    return None

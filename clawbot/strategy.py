from typing import List, Optional

from clawbot import config


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def prob_from_closes(closes: List[float]) -> float:
    avg = sum(closes) / len(closes)
    last = closes[-1]
    if avg <= 0:
        return 0.5
    dev = (last - avg) / avg
    p = 0.5 + (dev * config.PROB_Z_SCALE)
    return clamp(p, config.PROB_P_MIN, config.PROB_P_MAX)


def build_signal(edge: float, kill_switch: bool = False) -> str:
    if kill_switch:
        return "HOLD"
    enter = config.effective_edge_enter()
    if edge >= enter:
        return "YES"
    if edge <= -enter:
        return "NO"
    return "HOLD"


def calc_edge(fair_up: float, mark_up: float) -> float:
    return fair_up - mark_up


def marks_look_sane_for_live(p_up: float, p_down: float) -> bool:
    if not config.LIVE_MODE:
        return True
    s = p_up + p_down
    if s < config.LIVE_MIN_MARK_SUM or s > config.LIVE_MAX_MARK_SUM:
        return False
    if not (config.LIVE_MIN_PRICE <= p_up <= config.LIVE_MAX_PRICE):
        return False
    if not (config.LIVE_MIN_PRICE <= p_down <= config.LIVE_MAX_PRICE):
        return False
    return True


def side_mark(position: Optional[str], p_up: float, p_down: float) -> float:
    return p_up if position == "YES" else p_down

import os

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# modes
LIVE_MODE = os.getenv("LIVE_MODE", "false").lower() == "true"
KILL_SWITCH = os.getenv("KILL_SWITCH", "false").lower() == "true"
STATS_ONLY = os.getenv("STATS_ONLY", "false").lower() == "true"

# balances and paper params
START_BALANCE = float(os.getenv("START_BALANCE", "1000"))
TRADE_SIZE = float(os.getenv("TRADE_SIZE", "25"))
FEE_PER_TRADE = float(os.getenv("FEE_PER_TRADE", "0"))

# model
LOOKBACK = int(os.getenv("LOOKBACK", "5"))
PROB_P_MIN = float(os.getenv("PROB_P_MIN", "0.05"))
PROB_P_MAX = float(os.getenv("PROB_P_MAX", "0.95"))
PROB_Z_SCALE = float(os.getenv("PROB_Z_SCALE", "2.5"))

# risk
EDGE_ENTER = float(os.getenv("EDGE_ENTER", "0.08"))
EDGE_EXIT = float(os.getenv("EDGE_EXIT", "0.04"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "20"))
MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "8"))

# live risk overrides
LIVE_TRADE_SIZE = float(os.getenv("LIVE_TRADE_SIZE", "5"))
LIVE_MAX_TRADES_PER_DAY = int(os.getenv("LIVE_MAX_TRADES_PER_DAY", "10"))
LIVE_MAX_DAILY_LOSS = float(os.getenv("LIVE_MAX_DAILY_LOSS", "15"))
LIVE_COOLDOWN_MINUTES = int(os.getenv("LIVE_COOLDOWN_MINUTES", "20"))
LIVE_MIN_EDGE_ENTER = float(os.getenv("LIVE_MIN_EDGE_ENTER", "0.10"))
LIVE_MIN_EDGE_EXIT = float(os.getenv("LIVE_MIN_EDGE_EXIT", "0.04"))
LIVE_MIN_MARK_SUM = float(os.getenv("LIVE_MIN_MARK_SUM", "0.85"))
LIVE_MAX_MARK_SUM = float(os.getenv("LIVE_MAX_MARK_SUM", "1.15"))
LIVE_MIN_PRICE = float(os.getenv("LIVE_MIN_PRICE", "0.03"))
LIVE_MAX_PRICE = float(os.getenv("LIVE_MAX_PRICE", "0.97"))
LIVE_DAILY_PROFIT_LOCK = float(os.getenv("LIVE_DAILY_PROFIT_LOCK", "0"))

WHALE_COOLDOWN_OVERRIDE = os.getenv("WHALE_COOLDOWN_OVERRIDE", "true").lower() == "true"
WHALE_EDGE_OVERRIDE = float(os.getenv("WHALE_EDGE_OVERRIDE", "0.20"))

# networking
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "2"))
TIMEOUT_SEC = int(os.getenv("TIMEOUT_SEC", "20"))
POLY_TIMEOUT_SEC = int(os.getenv("POLY_TIMEOUT_SEC", "20"))

# APIs
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_PRICE_URL = "https://clob.polymarket.com/price"
KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"
KRAKEN_PAIR = "XBTUSD"
KRAKEN_INTERVAL = 5

# alerts
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
ALERT_CONSECUTIVE_FAILURE_THRESHOLD = int(os.getenv("ALERT_CONSECUTIVE_FAILURE_THRESHOLD", "3"))

# live execution config (signed placement)
LIVE_EXEC_BASE_URL = os.getenv("LIVE_EXEC_BASE_URL", "").rstrip("/")
LIVE_EXEC_ORDER_PATH = os.getenv("LIVE_EXEC_ORDER_PATH", "/orders")
LIVE_EXEC_ORDER_STATUS_PATH = os.getenv("LIVE_EXEC_ORDER_STATUS_PATH", "/orders/{order_id}")
LIVE_EXEC_API_KEY = os.getenv("LIVE_EXEC_API_KEY", "")
LIVE_EXEC_API_SECRET = os.getenv("LIVE_EXEC_API_SECRET", "")
LIVE_EXEC_API_PASSPHRASE = os.getenv("LIVE_EXEC_API_PASSPHRASE", "")


def effective_trade_size() -> float:
    return LIVE_TRADE_SIZE if LIVE_MODE else TRADE_SIZE


def effective_edge_enter() -> float:
    return LIVE_MIN_EDGE_ENTER if LIVE_MODE else EDGE_ENTER


def effective_edge_exit() -> float:
    return LIVE_MIN_EDGE_EXIT if LIVE_MODE else EDGE_EXIT


def effective_max_trades_per_day() -> int:
    return LIVE_MAX_TRADES_PER_DAY if LIVE_MODE else MAX_TRADES_PER_DAY


def effective_max_daily_loss() -> float:
    return LIVE_MAX_DAILY_LOSS if LIVE_MODE else MAX_DAILY_LOSS


def effective_cooldown_minutes() -> int:
    return LIVE_COOLDOWN_MINUTES if LIVE_MODE else COOLDOWN_MINUTES

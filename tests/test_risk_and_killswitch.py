import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from clawbot.risk import should_block_enter
from clawbot.strategy import build_signal


class RiskTests(unittest.TestCase):
    def test_kill_switch_forces_hold(self):
        self.assertEqual(build_signal(edge=1.0, kill_switch=True), "HOLD")

    @patch("clawbot.config.effective_max_daily_loss", return_value=3)
    @patch("clawbot.config.effective_max_trades_per_day", return_value=8)
    @patch("clawbot.config.effective_cooldown_minutes", return_value=20)
    def test_max_daily_loss_block(self, *_):
        blocked = should_block_enter(
            position=None,
            signal="YES",
            edge=0.2,
            trades_today=0,
            realized_pnl_today=-3.1,
            last_trade_ts=None,
        )
        self.assertEqual(blocked, "MAX_DAILY_LOSS")

    @patch("clawbot.config.effective_max_daily_loss", return_value=3)
    @patch("clawbot.config.effective_max_trades_per_day", return_value=8)
    @patch("clawbot.config.effective_cooldown_minutes", return_value=20)
    @patch("clawbot.config.WHALE_COOLDOWN_OVERRIDE", False)
    def test_cooldown_block(self, *_):
        now = datetime.now(timezone.utc)
        blocked = should_block_enter(
            position=None,
            signal="YES",
            edge=0.01,
            trades_today=0,
            realized_pnl_today=0.0,
            last_trade_ts=now - timedelta(minutes=5),
        )
        self.assertEqual(blocked, "COOLDOWN")


if __name__ == "__main__":
    unittest.main()

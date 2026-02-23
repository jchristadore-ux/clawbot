import unittest
from unittest.mock import patch

from clawbot.state import calc_realized_pnl, calc_unrealized_pnl, paper_trade_step
from clawbot.strategy import build_signal, calc_edge, prob_from_closes


class StrategyMathTests(unittest.TestCase):
    def test_probability_and_edge(self):
        closes = [100, 100, 100, 100, 101]
        p = prob_from_closes(closes)
        self.assertGreater(p, 0.5)
        edge = calc_edge(fair_up=p, mark_up=0.5)
        self.assertGreater(edge, 0)

    def test_signal_thresholds(self):
        self.assertEqual(build_signal(0.5, kill_switch=True), "HOLD")


class PnlAndTransitionTests(unittest.TestCase):
    def test_realized_and_unrealized_pnl(self):
        unreal = calc_unrealized_pnl("YES", entry_price=0.4, mark=0.5, stake=20)
        self.assertAlmostEqual(unreal, 5.0)
        real = calc_realized_pnl(entry_price=0.4, exit_price=0.6, stake=20)
        self.assertAlmostEqual(real, 10.0)

    @patch("clawbot.state.log_trade")
    def test_enter_exit_transition(self, _mock_log):
        bal, pos, entry, stake, action, trades, pnl_today, lts, ltd = paper_trade_step(
            balance=100,
            position=None,
            entry_price=None,
            stake=0.0,
            signal="YES",
            p_up=0.4,
            p_down=0.6,
            edge=0.2,
            edge_exit=0.05,
            trades_today=0,
            realized_pnl_today=0.0,
            last_trade_ts=None,
            last_trade_day=None,
        )
        self.assertEqual(pos, "YES")
        self.assertTrue(action.startswith("ENTER_"))

        bal2, pos2, _, _, action2, _, pnl2, _, _ = paper_trade_step(
            balance=bal,
            position=pos,
            entry_price=entry,
            stake=stake,
            signal="NO",
            p_up=0.5,
            p_down=0.5,
            edge=-0.2,
            edge_exit=0.05,
            trades_today=trades,
            realized_pnl_today=pnl_today,
            last_trade_ts=lts,
            last_trade_day=ltd,
        )
        self.assertEqual(action2, "EXIT")
        self.assertIsNone(pos2)
        self.assertNotEqual(pnl2, 0.0)


if __name__ == "__main__":
    unittest.main()

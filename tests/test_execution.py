import unittest
from unittest.mock import Mock, patch

from clawbot import execution


class ExecutionTests(unittest.TestCase):
    @patch("clawbot.execution.config.LIVE_EXEC_BASE_URL", "https://api.example.com")
    @patch("clawbot.execution.config.LIVE_EXEC_API_KEY", "k")
    @patch("clawbot.execution.config.LIVE_EXEC_API_SECRET", "s")
    @patch("clawbot.execution.config.LIVE_EXEC_API_PASSPHRASE", "p")
    @patch("clawbot.execution.requests.post")
    def test_place_live_order(self, mock_post):
        resp = Mock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"order_id": "abc", "status": "submitted"}
        mock_post.return_value = resp

        out = execution.place_live_order("YES", 0.51, 5, "btc-updown-5m-1")
        self.assertEqual(out.order_id, "abc")
        self.assertEqual(out.status, "submitted")

    @patch("clawbot.execution.config.LIVE_EXEC_BASE_URL", "https://api.example.com")
    @patch("clawbot.execution.config.LIVE_EXEC_API_KEY", "k")
    @patch("clawbot.execution.config.LIVE_EXEC_API_SECRET", "s")
    @patch("clawbot.execution.config.LIVE_EXEC_API_PASSPHRASE", "p")
    @patch("clawbot.execution.requests.get")
    def test_reconcile_order(self, mock_get):
        resp = Mock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"order_id": "abc", "status": "filled"}
        mock_get.return_value = resp

        out = execution.reconcile_order("abc")
        self.assertEqual(out.status, "filled")


if __name__ == "__main__":
    unittest.main()

import unittest

from src.simulator.broker import BrokerConfig, execute_orders, release_settlements, simulate_fill


class SimulatorBrokerTests(unittest.TestCase):
    def test_buy_limit_fills_at_open_or_limit(self) -> None:
        config = BrokerConfig()
        order = {"side": "buy", "limit_price": 100.0, "analysis_ref": {"close": 98.0}}
        fill, status = simulate_fill(order, {"open": 99.0, "high": 105.0, "low": 98.0, "close": 104.0}, config)
        self.assertEqual(status, "filled")
        self.assertEqual(fill, 99.0)

        fill, status = simulate_fill(order, {"open": 102.0, "high": 105.0, "low": 99.0, "close": 104.0}, config)
        self.assertEqual(status, "filled")
        self.assertEqual(fill, 100.0)

    def test_limit_up_blocked_and_limit_down_trapped(self) -> None:
        config = BrokerConfig()
        buy = {"side": "buy", "limit_price": 100.0, "analysis_ref": {"close": 100.0}}
        _, buy_status = simulate_fill(buy, {"open": 110.0, "high": 110.0, "low": 110.0, "close": 110.0}, config)
        self.assertEqual(buy_status, "limit_up_blocked")

        sell = {"side": "sell", "order_type": "limit", "limit_price": 100.0, "analysis_ref": {"close": 100.0}}
        _, sell_status = simulate_fill(sell, {"open": 90.0, "high": 90.0, "low": 90.0, "close": 90.0}, config)
        self.assertEqual(sell_status, "limit_down_trapped")

    def test_bracket_order_uses_conservative_stop_when_target_and_stop_both_hit(self) -> None:
        config = BrokerConfig()
        order = {
            "side": "sell",
            "order_type": "bracket",
            "limit_price": 110.0,
            "stop_price": 95.0,
            "analysis_ref": {"close": 100.0},
        }
        fill, status = simulate_fill(order, {"open": 100.0, "high": 112.0, "low": 94.0, "close": 108.0}, config)
        self.assertEqual(status, "filled_conservative_stop_first")
        self.assertEqual(fill, 95.0)

    def test_execute_orders_uses_unsettled_cash_for_sell_and_releases_later(self) -> None:
        config = BrokerConfig(min_commission=0.0)
        portfolio = {
            "portfolio_id": "balanced",
            "cash": 0.0,
            "settlements": [],
            "positions": {"2330": {"symbol": "2330", "quantity": 10, "avg_cost": 90.0, "last_price": 100.0}},
        }
        order = {
            "portfolio_id": "balanced",
            "date": "2026-04-29",
            "symbol": "2330",
            "side": "sell",
            "order_type": "limit",
            "quantity": 10,
            "limit_price": 100.0,
            "analysis_ref": {"close": 100.0},
        }
        orders, trades = execute_orders(portfolio, [order], {"2330": {"open": 101.0, "high": 102.0, "low": 99.0, "close": 100.0}}, {"2026-04-29": "2026-05-02"}, config)
        self.assertEqual(orders[0]["status"], "filled")
        self.assertEqual(len(trades), 1)
        self.assertEqual(portfolio["cash"], 0.0)
        self.assertGreater(portfolio["settlements"][0]["amount"], 0)
        release_settlements(portfolio, "2026-05-02")
        self.assertGreater(portfolio["cash"], 0)
        self.assertEqual(portfolio["settlements"], [])


if __name__ == "__main__":
    unittest.main()

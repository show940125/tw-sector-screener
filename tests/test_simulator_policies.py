import unittest

from src.simulator.broker import BrokerConfig
from src.simulator.policies import POLICIES, generate_policy_orders, make_portfolios


def row(
    symbol: str,
    recommendation: str,
    rank: int,
    risk: float,
    confidence: float = 86.0,
    idea: float = 70.0,
    buying_tier: str | None = None,
):
    return {
        "symbol": symbol,
        "name": symbol,
        "market": "TWSE",
        "rank": rank,
        "close": 100.0,
        "idea_score": idea,
        "rank_score": idea,
        "recommendation": recommendation,
        "confidence_score": confidence,
        "risk_score": risk,
        "buying_tier": buying_tier or ("formal_buy" if recommendation == "買入" else "not_buyable"),
        "risk_adjusted_score": 75.0,
        "stock_risk_metrics": {"risk_adjusted_score": 75.0},
        "decision_tier": "buy_now" if recommendation == "買入" else "wait_for_trigger",
        "starter_position_pct": 0.0,
        "target_range": {"low": 98.0, "base": 105.0, "high": 110.0, "basis": "mixed"},
        "data_quality_flags": [],
        "volatility20": 20.0,
        "recommendation_detail": {"evidence_refs": ["trend_score"]},
    }


class SimulatorPolicyTests(unittest.TestCase):
    def test_no_policy_buys_sell_recommendation(self) -> None:
        rows = [row("2330", "賣出", 1, 20.0)]
        for policy_id, policy in POLICIES.items():
            portfolio = make_portfolios(1_000_000)[policy_id]
            orders = generate_policy_orders(portfolio, policy, rows, "2026-04-29", BrokerConfig())
            self.assertEqual([x for x in orders if x["side"] == "buy"], [])

    def test_aggressive_can_buy_top_hold_but_balanced_cannot(self) -> None:
        rows = [row("2330", "持有", 2, 50.0, confidence=80.0, buying_tier="not_buyable")]
        rows[0]["decision_tier"] = "starter_position"
        rows[0]["starter_position_pct"] = 0.5
        aggressive = generate_policy_orders(make_portfolios(1_000_000)["aggressive"], POLICIES["aggressive"], rows, "2026-04-29", BrokerConfig())
        balanced = generate_policy_orders(make_portfolios(1_000_000)["balanced"], POLICIES["balanced"], rows, "2026-04-29", BrokerConfig())
        self.assertEqual(aggressive[0]["side"], "buy")
        self.assertEqual(balanced, [])
        self.assertLessEqual(aggressive[0]["quantity"] * aggressive[0]["limit_price"], 5_000.0)

    def test_aggressive_does_not_buy_plain_hold_without_starter_tier(self) -> None:
        rows = [row("2330", "持有", 2, 50.0, confidence=80.0)]
        orders = generate_policy_orders(make_portfolios(1_000_000)["aggressive"], POLICIES["aggressive"], rows, "2026-04-29", BrokerConfig())
        self.assertEqual(orders, [])

    def test_balanced_can_buy_risk_adjusted_buy_hold(self) -> None:
        rows = [row("2330", "持有", 6, 50.0, confidence=82.0, idea=58.0, buying_tier="risk_adjusted_buy")]
        orders = generate_policy_orders(make_portfolios(1_000_000)["balanced"], POLICIES["balanced"], rows, "2026-04-29", BrokerConfig())
        self.assertEqual(orders[0]["side"], "buy")
        self.assertIn("buying_tier risk_adjusted_buy", orders[0]["reason"])

    def test_conservative_blocks_tactical_buy(self) -> None:
        rows = [row("TACT", "持有", 1, 60.0, confidence=85.0, buying_tier="tactical_buy")]
        orders = generate_policy_orders(make_portfolios(1_000_000)["conservative"], POLICIES["conservative"], rows, "2026-04-29", BrokerConfig())
        self.assertEqual(orders, [])

    def test_aggressive_tactical_buy_uses_small_budget(self) -> None:
        rows = [row("TACT", "持有", 1, 60.0, confidence=85.0, buying_tier="tactical_buy")]
        rows[0]["starter_position_pct"] = 0.5
        orders = generate_policy_orders(make_portfolios(1_000_000)["aggressive"], POLICIES["aggressive"], rows, "2026-04-29", BrokerConfig())
        self.assertEqual(orders[0]["side"], "buy")
        self.assertLessEqual(orders[0]["quantity"] * orders[0]["limit_price"], 5_000.0)

    def test_conservative_blocks_incomplete_target_or_flags(self) -> None:
        bad = row("2330", "買入", 1, 20.0, confidence=90.0)
        bad["target_range"] = {"low": None, "base": None, "high": None, "basis": "insufficient_data"}
        orders = generate_policy_orders(make_portfolios(1_000_000)["conservative"], POLICIES["conservative"], [bad], "2026-04-29", BrokerConfig())
        self.assertEqual(orders, [])

    def test_existing_position_sells_when_recommendation_turns_sell(self) -> None:
        portfolio = make_portfolios(1_000_000)["balanced"]
        portfolio["positions"]["2330"] = {"symbol": "2330", "quantity": 100, "avg_cost": 90.0, "last_price": 100.0}
        orders = generate_policy_orders(portfolio, POLICIES["balanced"], [row("2330", "賣出", 1, 80.0)], "2026-04-29", BrokerConfig())
        self.assertEqual(orders[0]["side"], "sell")
        self.assertEqual(orders[0]["quantity"], 100)


if __name__ == "__main__":
    unittest.main()

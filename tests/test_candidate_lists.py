import unittest

from src.analysis.candidate_lists import build_candidate_lists


def row(
    symbol: str,
    recommendation: str,
    rank_score: float,
    idea_score: float,
    risk_score: float,
    confidence_score: float = 86.0,
    flags: list[str] | None = None,
    risk_adjusted_score: float = 50.0,
) -> dict:
    return {
        "symbol": symbol,
        "name": symbol,
        "rank": int(rank_score),
        "rank_score": rank_score,
        "idea_score": idea_score,
        "risk_score": risk_score,
        "confidence_score": confidence_score,
        "recommendation": recommendation,
        "data_quality_flags": flags or [],
        "stock_risk_metrics": {
            "risk_adjusted_score": risk_adjusted_score,
            "sharpe_ratio": 0.5,
            "sortino_ratio": 0.6,
            "max_drawdown_pct": -10.0,
            "annualized_volatility_pct": 20.0,
        },
    }


class CandidateListTests(unittest.TestCase):
    def test_sell_candidate_never_enters_buying_ranking(self) -> None:
        lists = build_candidate_lists(
            [
                row("SELL", "賣出", 99, 95, 40),
                row("BUY", "買入", 75, 72, 30),
            ],
            top_n=2,
        )

        self.assertEqual([item["symbol"] for item in lists["buying_ranking"]], ["BUY"])
        self.assertIn("SELL", [item["symbol"] for item in lists["watchlist_candidates"]])
        self.assertEqual([item["symbol"] for item in lists["research_list"]], ["SELL", "BUY"])

    def test_buy_candidate_must_pass_risk_gate(self) -> None:
        lists = build_candidate_lists(
            [
                row("GOOD", "買入", 80, 78, 35),
                row("RISKY", "買入", 90, 88, 82),
                row("MISSING", "買入", 85, 82, 30, flags=["quality:fetch_failed"]),
            ],
            top_n=3,
        )

        self.assertEqual([item["symbol"] for item in lists["buying_ranking"]], ["GOOD"])
        self.assertIn("RISKY", [item["symbol"] for item in lists["watchlist_candidates"]])
        self.assertIn("MISSING", [item["symbol"] for item in lists["watchlist_candidates"]])

    def test_list_items_have_semantic_fields_and_backward_alias(self) -> None:
        lists = build_candidate_lists(
            [
                row("A", "買入", 80, 70, 30),
                row("B", "持有", 79, 75, 50),
                row("C", "賣出", 78, 74, 35),
            ],
            top_n=3,
        )

        for key in ["buying_ranking", "watchlist_candidates", "research_list", "picks"]:
            self.assertIn(key, lists)
        self.assertEqual([item["symbol"] for item in lists["picks"]], ["A", "B", "C"])
        self.assertEqual(lists["buying_ranking"][0]["list_type"], "buying_ranking")
        self.assertIn("buyability_score", lists["buying_ranking"][0])
        self.assertIn("risk_adjusted_score", lists["buying_ranking"][0])
        self.assertIn("buying_tier", lists["buying_ranking"][0])
        self.assertIn("research_reason", lists["research_list"][0])
        self.assertIn("monitoring_reason", lists["watchlist_candidates"][0])
        self.assertIn("exclusion_from_buying_reason", lists["watchlist_candidates"][0])

    def test_buying_zero_still_produces_actionable_queue(self) -> None:
        lists = build_candidate_lists(
            [
                row("NEAR", "持有", 88, 74, 70, confidence_score=82, risk_adjusted_score=40),
                row("START", "持有", 84, 72, 82, confidence_score=80),
                row("SELL", "賣出", 99, 90, 35, confidence_score=90),
                row("BLOCK", "持有", 92, 80, 55, confidence_score=90, flags=["extreme-volatility"]),
            ],
            top_n=4,
        )

        self.assertEqual(lists["buying_ranking"], [])
        self.assertEqual([item["symbol"] for item in lists["actionable_queue"]], ["NEAR", "START"])
        self.assertEqual(lists["actionable_queue"][0]["decision_tier"], "near_buy")
        self.assertEqual(lists["actionable_queue"][1]["decision_tier"], "starter_position")
        for item in lists["actionable_queue"]:
            self.assertIn("blocked_by", item)
            self.assertIn("trigger_to_upgrade", item)
            self.assertIn("why_not_buy_now", item)

    def test_avoid_tier_for_sell_hard_blocker_and_extreme_risk(self) -> None:
        lists = build_candidate_lists(
            [
                row("SELL", "賣出", 90, 80, 40),
                row("BLOCK", "持有", 88, 78, 50, flags=["quality:fetch_failed"]),
                row("RISK", "持有", 86, 76, 92),
            ],
            top_n=3,
        )

        self.assertEqual(lists["actionable_queue"], [])
        tiers = {item["symbol"]: item["decision_tier"] for item in lists["research_list"]}
        self.assertEqual(tiers, {"SELL": "avoid", "BLOCK": "avoid", "RISK": "avoid"})

    def test_buying_and_actionable_are_not_limited_by_research_top_n(self) -> None:
        rows = [row(f"R{i:02d}", "持有", 100 - i, 100 - i, 92, confidence_score=82) for i in range(25)]
        rows.append(row("LATEBUY", "買入", 50, 62, 25, confidence_score=90))
        rows.append(row("LATEACTION", "持有", 49, 70, 70, confidence_score=88, risk_adjusted_score=40))

        lists = build_candidate_lists(rows, top_n=20)

        self.assertNotIn("LATEBUY", [item["symbol"] for item in lists["research_list"]])
        self.assertIn("LATEBUY", [item["symbol"] for item in lists["buying_ranking"]])
        self.assertIn("LATEACTION", [item["symbol"] for item in lists["actionable_queue"]])
        self.assertEqual(lists["picks"], lists["research_list"])
        self.assertEqual(len(lists["research_list"]), 20)

    def test_risk_adjusted_metrics_help_buying_order_without_overwriting_idea(self) -> None:
        lists = build_candidate_lists(
            [
                row("WILD", "買入", 80, 80, 40, risk_adjusted_score=10),
                row("STEADY", "買入", 78, 78, 40, risk_adjusted_score=90),
            ],
            top_n=2,
        )

        self.assertEqual([item["symbol"] for item in lists["buying_ranking"]], ["STEADY", "WILD"])
        self.assertEqual(lists["research_list"][0]["symbol"], "WILD")
        self.assertEqual(lists["research_list"][0]["idea_score"], 80)

    def test_risk_adjusted_buy_allows_low_risk_hold_with_lower_idea(self) -> None:
        lists = build_candidate_lists(
            [
                row("STEADY", "持有", 70, 58, 45, confidence_score=85, risk_adjusted_score=82),
            ],
            top_n=20,
        )

        self.assertEqual([item["symbol"] for item in lists["buying_ranking"]], ["STEADY"])
        self.assertEqual(lists["buying_ranking"][0]["buying_tier"], "risk_adjusted_buy")
        self.assertEqual(lists["buying_ranking"][0]["recommendation"], "持有")

    def test_buying_tier_priority_orders_formal_before_risk_adjusted_before_tactical(self) -> None:
        lists = build_candidate_lists(
            [
                row("TACT", "持有", 98, 66, 70, confidence_score=80, risk_adjusted_score=55),
                row("RADJ", "持有", 80, 58, 45, confidence_score=85, risk_adjusted_score=82),
                row("FORMAL", "買入", 72, 70, 45, confidence_score=80, risk_adjusted_score=60),
            ],
            top_n=20,
        )

        self.assertEqual([item["symbol"] for item in lists["buying_ranking"]], ["FORMAL", "RADJ", "TACT"])
        self.assertEqual([item["buying_tier"] for item in lists["buying_ranking"]], ["formal_buy", "risk_adjusted_buy", "tactical_buy"])

    def test_high_idea_high_risk_and_blockers_do_not_enter_buying_v2(self) -> None:
        lists = build_candidate_lists(
            [
                row("HIGH_RISK", "持有", 95, 80, 73, confidence_score=90, risk_adjusted_score=70),
                row("SELL", "賣出", 94, 80, 20, confidence_score=90, risk_adjusted_score=90),
                row("BLOCK", "持有", 93, 80, 20, confidence_score=90, risk_adjusted_score=90, flags=["quality:fetch_failed"]),
                row("OK", "持有", 70, 58, 40, confidence_score=85, risk_adjusted_score=80),
            ],
            top_n=20,
        )

        self.assertEqual([item["symbol"] for item in lists["buying_ranking"]], ["OK"])
        tiers = {item["symbol"]: item["buying_tier"] for item in lists["research_list"]}
        self.assertEqual(tiers["HIGH_RISK"], "not_buyable")
        self.assertEqual(tiers["SELL"], "not_buyable")
        self.assertEqual(tiers["BLOCK"], "not_buyable")


if __name__ == "__main__":
    unittest.main()

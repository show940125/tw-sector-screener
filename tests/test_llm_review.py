import unittest
from unittest.mock import patch

from src.analysis.llm_review import apply_llm_review


class LlmReviewTests(unittest.TestCase):
    def test_missing_api_key_falls_back(self) -> None:
        recommendation = {"recommendation": "買入", "review_notes": {}}
        with patch.dict("os.environ", {"OPENAI_API_KEY": ""}, clear=False):
            reviewed = apply_llm_review(recommendation, {"symbol": "2330"}, provider="openai", model="test")

        self.assertEqual(reviewed["recommendation"], "買入")
        self.assertEqual(reviewed["recommendation_source"], "deterministic_plus_llm")
        self.assertEqual(reviewed["llm_review"]["status"], "fallback:no-api-config")


if __name__ == "__main__":
    unittest.main()

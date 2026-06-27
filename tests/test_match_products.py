import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRAPDB = ROOT / "ScrapDB"
sys.path.insert(0, str(SCRAPDB))

import match_products


class MatchProductHelpersTest(unittest.TestCase):
    def test_parse_price_to_int_handles_chilean_price_text(self):
        self.assertEqual(match_products.parse_price_to_int("$ 147.990"), 147990)
        self.assertEqual(match_products.parse_price_to_int("Oferta $165.000 normal $189.990"), 165000)
        self.assertEqual(match_products.parse_price_to_int("165000165000147990147990"), 147990)

    def test_parse_price_to_int_rejects_impossible_values(self):
        self.assertIsNone(match_products.parse_price_to_int("$0"))
        self.assertIsNone(match_products.parse_price_to_int("$999999999999"))
        self.assertIsNone(match_products.parse_price_to_int("sin precio"))

    def test_normalize_part_number(self):
        self.assertEqual(match_products.normalize_part_number("BX8071512400F"), "BX8071512400F")
        self.assertEqual(match_products.normalize_part_number("90YV0GB2-M0AA00"), "90YV0GB2M0AA00")
        self.assertEqual(match_products.normalize_part_number(" cmk32gx5m2b5600c36 "), "CMK32GX5M2B5600C36")

    def test_stock_markout_requires_healthy_scrape(self):
        self.assertEqual(
            match_products.should_allow_stock_markout(0, 0, {"success": True}),
            (False, "empty_output"),
        )
        self.assertEqual(
            match_products.should_allow_stock_markout(4, 4, {"success": True}),
            (False, "too_few_results"),
        )
        self.assertEqual(
            match_products.should_allow_stock_markout(100, 1, {"success": True}),
            (False, "low_match_rate"),
        )
        self.assertEqual(
            match_products.should_allow_stock_markout(100, 40, {"success": False}),
            (False, "scraper_failed"),
        )
        self.assertEqual(
            match_products.should_allow_stock_markout(100, 40, {"success": True}),
            (True, "healthy_scrape"),
        )

    def test_matching_does_not_use_fuzzy_ilike(self):
        source = (SCRAPDB / "match_products.py").read_text(encoding="utf-8")
        self.assertNotIn(".ilike(", source)
        self.assertNotIn("part_number_fuzzy", source)


if __name__ == "__main__":
    unittest.main()

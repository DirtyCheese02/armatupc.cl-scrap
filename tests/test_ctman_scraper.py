from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRAPER_DIR = ROOT / "ScrapDB" / "PythonsScrap"
FIXTURES = ROOT / "tests" / "fixtures" / "ctman"
sys.path.insert(0, str(SCRAPER_DIR))

import Scrap_CTMan as ctman


class CTManHtmlScraperTests(unittest.TestCase):
    def test_listing_extracts_product_and_pagination(self):
        products, pages = ctman._listing_products(
            (FIXTURES / "listing.html").read_text(encoding="utf-8"), "CPU"
        )
        self.assertEqual(pages, 2)
        self.assertEqual(len(products), 1)
        self.assertEqual(products[0]["url"], "https://www.ctman.cl/products/amd-100-100000457box")

    def test_detail_extracts_positive_price_stock_and_mpn(self):
        product = {
            "type": "CPU",
            "url": "https://www.ctman.cl/products/amd-100-100000457box",
            "name": "Procesador AMD Ryzen 5 5500",
            "image_url": "",
        }
        html = (FIXTURES / "detail.html").read_text(encoding="utf-8")
        with patch.object(ctman, "fetch_text", return_value=html), patch.object(ctman.time, "sleep"):
            result = ctman._detail_product(product)
        self.assertIsNotNone(result)
        self.assertEqual(result["part #"], "100-100000457BOX")
        self.assertEqual(result["cash_price"], "100540")
        self.assertEqual(result["availability"], "available")

    def test_blocked_page_is_not_treated_as_empty_healthy_catalog(self):
        with self.assertRaisesRegex(RuntimeError, "blocked_html_response"):
            ctman._listing_products(
                (FIXTURES / "blocked.html").read_text(encoding="utf-8"), "CPU"
            )


if __name__ == "__main__":
    unittest.main()

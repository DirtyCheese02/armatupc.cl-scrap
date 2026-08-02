from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
SCRAPER_DIR = ROOT / "ScrapDB" / "PythonsScrap"
sys.path.insert(0, str(SCRAPER_DIR))

import Scrap_TecnoShopping as tecnoshopping


class TecnoShoppingReliabilityTests(unittest.TestCase):
    def test_html_product_fallback_extracts_sku_price_and_stock(self):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(
            """
            <h1 class="product_title">Procesador AMD Ryzen demo</h1>
            <div class="product_meta"><span class="sku">100-TEST-WOF</span></div>
            <div class="summary">
              <div class="custom-price-display"><span class="woocommerce-Price-amount">$199.990</span></div>
              <p class="stock">Sin existencias</p>
            </div>
            <img class="wp-post-image" src="/demo.webp">
            """,
            "html.parser",
        )
        result = tecnoshopping.parse_tecnoshopping_html_product(
            soup,
            "https://www.tecnoshopping.cl/procesador-demo/",
            "CPU",
            tecnoshopping.BASE_URL,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["part #"], "100-TEST-WOF")
        self.assertEqual(result["price"], "199990")
        self.assertEqual(result["availability"], "unavailable")

    def test_api_probe_uses_rest_route_when_pretty_route_returns_html(self):
        fallback_url = tecnoshopping._api_candidates()[1]
        response = SimpleNamespace(headers={})
        with patch.object(
            tecnoshopping,
            "fetch_json",
            side_effect=[RuntimeError("invalid_json_response"), ([{"id": 1}], response)],
        ):
            selected = tecnoshopping._select_api_url(Mock())
        self.assertEqual(selected, fallback_url)

    def test_product_page_falls_back_to_alternate_api_route(self):
        preferred, alternate = tecnoshopping._api_candidates()
        response = SimpleNamespace(headers={"X-WP-TotalPages": "1"})
        with patch.object(
            tecnoshopping,
            "fetch_json",
            side_effect=[RuntimeError("403"), ([{"id": 1}], response)],
        ):
            products, returned_response, selected = tecnoshopping._fetch_product_page(
                Mock(),
                preferred,
                params={"page": 1, "per_page": 100},
            )
        self.assertEqual(products, [{"id": 1}])
        self.assertIs(returned_response, response)
        self.assertEqual(selected, alternate)


if __name__ == "__main__":
    unittest.main()

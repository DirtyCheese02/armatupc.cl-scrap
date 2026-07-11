from __future__ import annotations

import sys
import unittest
from pathlib import Path


SCRAPER_DIR = Path(__file__).resolve().parents[1] / "ScrapDB" / "PythonsScrap"
if str(SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(SCRAPER_DIR))

import Scrap_CentralGamer as centralgamer


PRODUCT_META_HTML = """
<div class="entry-product-meta product_meta">
    <div class="part_number_wrapper meta-item">
        <label class="meta-label">PART NUMBER:</label>
        <div class="meta-content">
            <span class="part-number">920-011902</span>
        </div>
    </div>
    <div class="sku_wrapper meta-item">
        <label class="meta-label">Sku:</label>
        <div class="meta-content">
            <span class="sku">CGLGTECPROX60BLA</span>
        </div>
    </div>
</div>
"""


class CentralGamerPartNumberTests(unittest.TestCase):
    def setUp(self) -> None:
        centralgamer.DETAIL_PART_NUMBER_CACHE.clear()

    def tearDown(self) -> None:
        centralgamer.DETAIL_PART_NUMBER_CACHE.clear()

    def test_extracts_product_meta_part_number_instead_of_store_sku(self) -> None:
        self.assertEqual(
            centralgamer.extract_part_number_from_product_meta(PRODUCT_META_HTML),
            "920-011902",
        )

    def test_picker_fetches_detail_page_when_sku_is_internal(self) -> None:
        original_fetch_text = centralgamer.fetch_text
        try:
            centralgamer.fetch_text = lambda session, url: PRODUCT_META_HTML
            product = {
                "sku": "CGLGTECPROX60BLA",
                "permalink": "https://centralgamer.cl/pro/teclado-inalambrico-logitech-pro-x-60-black/",
                "name": "Teclado Mecanico Logitech Pro X 60 Black Inalambrico",
                "short_description": "",
                "description": "",
            }

            self.assertEqual(centralgamer.pick_centralgamer_part_number(product), "920-011902")
        finally:
            centralgamer.fetch_text = original_fetch_text

    def test_picker_does_not_publish_internal_sku_as_part_number(self) -> None:
        original_fetch_text = centralgamer.fetch_text
        try:
            centralgamer.fetch_text = lambda session, url: "<html></html>"
            product = {
                "sku": "CGLGTECPROX60BLA",
                "permalink": "https://centralgamer.cl/pro/without-part-number/",
                "name": "Teclado Logitech Pro X Black Inalambrico",
                "short_description": "",
                "description": "",
            }

            self.assertIsNone(centralgamer.pick_centralgamer_part_number(product))
        finally:
            centralgamer.fetch_text = original_fetch_text


if __name__ == "__main__":
    unittest.main()

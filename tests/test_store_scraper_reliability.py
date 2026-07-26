from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


ROOT = Path(__file__).resolve().parents[1]
SCRAPER_DIR = ROOT / "ScrapDB" / "PythonsScrap"
sys.path.insert(0, str(SCRAPER_DIR))

import Scrap_MyBox as mybox
import Scrap_NiceOne as niceone
import Scrap_Winpy as winpy
import api_scraper_utils


class StoreScraperReliabilityTests(unittest.TestCase):
    def test_mybox_browser_parser_accepts_visible_reference_block(self):
        self.assertIn(
            "//*[contains(@class,'product-reference')]",
            mybox.MYBOX_PRODUCT_CONFIG["part_selectors"],
        )
        self.assertIn(
            "//*[@itemprop='price']",
            mybox.MYBOX_PRODUCT_CONFIG["price_selectors"],
        )
        self.assertEqual(
            mybox.clean_mybox_part_number("Referencia 100-100001594WOF"),
            "100-100001594WOF",
        )

    def test_niceone_probe_fails_fast_on_runner_connectivity_error(self):
        with patch.object(niceone, "fetch_json", side_effect=RuntimeError("connect timeout")) as fetch:
            ok, error = niceone.niceone_connectivity_probe(timeout=7, retries=1)
        self.assertFalse(ok)
        self.assertIn("connect timeout", error or "")
        self.assertEqual(fetch.call_args.kwargs["timeout"], 7)
        self.assertEqual(fetch.call_args.kwargs["retries"], 1)

    def test_prestashop_runner_reports_category_completion(self):
        category_status: dict[str, bool] = {}
        payload = {
            "products": [
                {
                    "url": "https://store.example/product/demo",
                    "name": "Demo",
                    "reference": "DEMO-1",
                    "price_amount": 19990,
                }
            ],
            "pagination": {"pages_count": 1, "pages": []},
        }
        with patch.object(api_scraper_utils, "clean_output_dir"), patch.object(
            api_scraper_utils, "fetch_text_with_referer", return_value="<html></html>"
        ), patch.object(
            api_scraper_utils, "fetch_json", return_value=(payload, object())
        ), patch.object(api_scraper_utils, "write_product_json"):
            saved = api_scraper_utils.run_prestashop_xhr_store(
                store_name="Store",
                base_url="https://store.example",
                category_url_map={"CPU": "https://store.example/cpu"},
                output_dir="unused",
                output_prefix="S",
                category_status=category_status,
            )
        self.assertEqual(saved, 1)
        self.assertEqual(category_status, {"CPU": True})

    def test_winpy_retries_navigation_with_a_fresh_tab(self):
        async def scenario():
            first = AsyncMock()
            second = AsyncMock()
            browser = AsyncMock()
            browser.new_tab.side_effect = [first, second]
            with patch.object(
                winpy,
                "_wait_for_category",
                side_effect=[False, True],
            ), patch.object(winpy.asyncio, "sleep", new=AsyncMock()):
                result = await winpy._open_ready_category_tab(
                    browser,
                    "https://www.winpy.cl/accesorios/audifonos/",
                    category_name="Headphones",
                    ready_timeout=5,
                    label="category",
                    attempts=2,
                )
            self.assertIs(result, second)
            first.close.assert_awaited_once()
            second.close.assert_not_awaited()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()

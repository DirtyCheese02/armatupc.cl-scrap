from __future__ import annotations

import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[1]
SCRAPER_DIR = ROOT / "ScrapDB" / "PythonsScrap"
sys.path.insert(0, str(SCRAPER_DIR))

import Scrap_MyBox as mybox
import Scrap_MyShop as myshop
import Scrap_DazbogStore as dazbog
import Scrap_Alltec as alltec
import Scrap_NiceOne as niceone
import Scrap_InfoSep as infosep
import Scrap_Winpy as winpy
import api_scraper_utils


class StoreScraperReliabilityTests(unittest.TestCase):
    def test_alltec_preserves_explicit_out_of_stock_product(self):
        soup = BeautifulSoup(
            """
            <h1 itemprop="name">Memoria Kingston KF432C16BB/16</h1>
            <p id="product_reference"><span itemprop="sku">KF432C16BB/16</span></p>
            <span id="our_price_display">$49.990</span>
            <span id="availability_value">Sin stock</span>
            <img id="bigpic" src="/memoria.webp">
            """,
            "html.parser",
        )
        result = alltec.parse_alltec_product(
            soup, "https://www.alltec.cl/memoria-demo", "Memory", alltec.BASE_URL
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["availability"], "unavailable")

    def test_infosep_store_api_preserves_explicit_out_of_stock(self):
        product = {
            "permalink": "https://infosep.cl/producto/memoria-demo/",
            "name": "Memoria Kingston Demo",
            "sku": "KF436C18BB/32",
            "prices": {"price": "71895"},
            "is_in_stock": False,
        }

        result = infosep.product_to_output(product, "Memory")

        self.assertIsNotNone(result)
        self.assertEqual(result["availability"], "unavailable")
        self.assertEqual(result["price"], "71895")

    def test_infosep_html_recognizes_sin_existencias_and_stock_class(self):
        soup = BeautifulSoup(
            """
            <h1 class="product_title">Memoria Kingston Demo</h1>
            <span class="sku">KF436C18BB/32</span>
            <p class="price"><span class="woocommerce-Price-amount">$71.895</span></p>
            <p class="stock out-of-stock wd-style-bordered"><span>Sin existencias</span></p>
            """,
            "html.parser",
        )

        result = infosep.parse_infosep_html_product(
            soup,
            "https://infosep.cl/producto/memoria-demo/",
            "Memory",
            infosep.BASE_URL,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["availability"], "unavailable")

    def test_infosep_does_not_infer_stock_without_an_explicit_signal(self):
        self.assertEqual(infosep.normalize_infosep_availability(), "unknown")
        self.assertEqual(
            infosep.normalize_infosep_availability("stock wd-style-bordered"),
            "unknown",
        )

    def test_infosep_browser_detail_rejects_missing_price(self):
        result = infosep.infosep_browser_detail_to_output(
            {
                "name": "Memoria Kingston Demo",
                "part_number": "KF436C18BB/32",
                "price": "0",
                "stock_text": "Sin existencias",
            },
            url="https://infosep.cl/producto/memoria-demo/",
            category_name="Memory",
        )
        self.assertIsNone(result)

    def test_infosep_browser_store_api_reuses_session_and_keeps_oos(self):
        async def scenario():
            product = {
                "permalink": "https://infosep.cl/producto/memoria-demo/",
                "name": "Memoria Kingston Demo",
                "sku": "KF436C18BB/32",
                "prices": {"price": "71895"},
                "is_in_stock": False,
            }
            written = []
            with patch.object(
                infosep,
                "CATEGORY_QUERIES",
                {"Memory": [{"category": 129}]},
            ), patch.object(
                infosep,
                "_infosep_browser_api_page",
                new=AsyncMock(return_value=([product], 1)),
            ), patch.object(
                infosep,
                "write_product_json",
                side_effect=lambda *args: written.append(args[-1]),
            ):
                count, complete = await infosep._scrape_infosep_browser_store_api(
                    object(), "unused", 0
                )

            self.assertTrue(complete)
            self.assertEqual(count, 1)
            self.assertEqual(written[0]["availability"], "unavailable")

        asyncio.run(scenario())

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

    def test_niceone_stops_after_an_explicit_403_probe(self):
        with patch.object(
            niceone,
            "niceone_connectivity_probe",
            return_value=(False, "403 Client Error: Forbidden"),
        ), patch.object(niceone, "run_prestashop_xhr_store") as run_store, patch.object(
            niceone, "write_scraper_health"
        ) as write_health:
            result = niceone.main()
        self.assertEqual(result, 1)
        run_store.assert_not_called()
        self.assertEqual(
            write_health.call_args.kwargs["blocked_reason"],
            "public_catalog_access_blocked",
        )

    def test_mybox_uses_one_browser_probe_before_full_fallback_when_requests_are_blocked(self):
        with patch.object(
            mybox,
            "mybox_connectivity_probe",
            return_value=(False, "403 Client Error: Forbidden"),
        ), patch.object(
            mybox,
            "probe_browser_category",
            return_value=(False, "browser_access_blocked"),
        ) as browser_probe, patch.object(
            mybox, "run_prestashop_xhr_store"
        ) as requests_scraper, patch.object(
            mybox, "run_browser_fallback_store"
        ) as browser_scraper, patch.object(
            mybox, "clean_output_dir"
        ), patch.object(mybox, "write_scraper_health") as write_health:
            result = mybox.main()
        self.assertEqual(result, 1)
        browser_probe.assert_called_once()
        requests_scraper.assert_not_called()
        browser_scraper.assert_not_called()
        self.assertEqual(
            write_health.call_args.kwargs["blocked_reason"],
            "public_catalog_access_blocked",
        )

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

    def test_sphinx_runner_reports_failed_category_without_hiding_other_outputs(self):
        category_status: dict[str, bool] = {}
        with patch.object(api_scraper_utils, "clean_output_dir"), patch.object(
            api_scraper_utils,
            "fetch_text",
            side_effect=[RuntimeError("404 Client Error"), '<form id="filtroShop"></form>'],
        ):
            saved = api_scraper_utils.run_sphinx_store(
                store_name="Store",
                base_url="https://store.example",
                service_url="https://store.example/service",
                category_url_map={
                    "UPS": "https://store.example/missing",
                    "CPU": "https://store.example/cpu",
                },
                output_dir="unused",
                output_prefix="S",
                category_status=category_status,
            )
        self.assertEqual(saved, 0)
        self.assertEqual(category_status, {"UPS": False, "CPU": False})

    def test_myshop_uses_current_ups_listing_and_emits_health(self):
        self.assertEqual(
            myshop.CATEGORY_URL_MAP["UPS"],
            "https://www.myshop.cl/empresas-ups-baterias-externas",
        )
        with patch.object(
            myshop,
            "run_sphinx_store",
            side_effect=lambda **kwargs: kwargs["category_status"].update(
                {name: True for name in myshop.CATEGORY_URL_MAP}
            )
            or 12,
        ), patch.object(myshop, "write_scraper_health") as write_health:
            self.assertEqual(myshop.main(), 0)
        self.assertEqual(write_health.call_args.kwargs["status"], "success")

    def test_dazbog_html_parser_uses_product_json_ld(self):
        soup = BeautifulSoup(
            """
            <script type="application/ld+json">
            {"@graph":[{"@type":"Product","name":"AMD Ryzen Demo","sku":"100-DEMO",
              "image":"https://store.example/demo.webp","brand":{"@type":"Brand","name":"AMD"},
              "offers":[{"@type":"Offer","price":"129990","availability":"https://schema.org/InStock"}]}]}
            </script>
            """,
            "html.parser",
        )
        result = dazbog.parse_dazbog_product(
            soup,
            "https://www.dazbogstore.cl/product/demo/",
            "CPU",
            dazbog.BASE_URL,
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["part #"], "100-DEMO")
        self.assertEqual(result["price"], "129990")
        self.assertEqual(result["availability"], "available")

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

    def test_winpy_allows_zero_category_retry_passes(self):
        with patch.dict(os.environ, {"WINPY_CATEGORY_RETRY_PASSES": "0"}):
            self.assertEqual(
                winpy._env_nonnegative_int("WINPY_CATEGORY_RETRY_PASSES", 2),
                0,
            )

    def test_winpy_retry_can_be_scoped_to_failed_categories(self):
        with patch.dict(
            os.environ,
            {"WINPY_CATEGORY_INCLUDE": "Case,Memory,DoesNotExist"},
        ):
            selected = winpy._active_category_url_map()
        self.assertEqual(set(selected), {"Case", "Memory"})

    def test_winpy_continues_after_one_pagination_timeout(self):
        async def scenario():
            first = AsyncMock()
            third = AsyncMock()
            browser = AsyncMock()
            products = []
            seen = set()
            with patch.object(
                winpy,
                "_open_ready_category_tab",
                new=AsyncMock(side_effect=[first, None, third]),
            ), patch.object(
                winpy,
                "_category_pages",
                new=AsyncMock(return_value=["page-1", "page-2", "page-3"]),
            ), patch.object(
                winpy,
                "_products_from_listing",
                new=AsyncMock(
                    side_effect=[
                        [{"name": "Uno", "url": "https://www.winpy.cl/venta/uno"}],
                        [{"name": "Tres", "url": "https://www.winpy.cl/venta/tres"}],
                    ]
                ),
            ):
                category, complete, error = await winpy._collect_category(
                    asyncio.Semaphore(1),
                    browser,
                    category_name="CPU",
                    category_url="https://www.winpy.cl/cpu",
                    products_to_scrape=products,
                    seen=seen,
                    ready_timeout=5,
                )
            self.assertEqual(category, "CPU")
            self.assertFalse(complete)
            self.assertEqual(error, "page_2_timeout")
            self.assertEqual([item["name"] for item in products], ["Uno", "Tres"])

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()

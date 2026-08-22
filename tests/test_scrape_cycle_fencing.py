import unittest
from types import SimpleNamespace
from unittest.mock import patch

from ScrapDB import match_products


class FenceClient:
    def __init__(self, value):
        self.value = value
        self.params = None

    def rpc(self, name, params):
        self.name = name
        self.params = params
        return self

    def execute(self):
        return SimpleNamespace(data=self.value)


class ScrapeCycleFenceTests(unittest.TestCase):
    def test_valid_fence_allows_publication(self):
        client = FenceClient(True)
        with (
            patch.object(match_products, "SCRAPE_CYCLE_DATE", "2026-08-12"),
            patch.object(match_products, "SCRAPE_FENCING_TOKEN", "token"),
            patch.object(match_products, "get_supabase", return_value=client),
        ):
            self.assertTrue(match_products.assert_scrape_cycle_fence("test"))
        self.assertEqual(client.name, "assert_scrape_cycle_fence")

    def test_stale_fence_aborts_publication(self):
        with (
            patch.object(match_products, "SCRAPE_CYCLE_DATE", "2026-08-12"),
            patch.object(match_products, "SCRAPE_FENCING_TOKEN", "stale"),
            patch.object(match_products, "get_supabase", return_value=FenceClient(False)),
        ):
            with self.assertRaises(RuntimeError):
                match_products.assert_scrape_cycle_fence("markout")

    def test_half_configured_fence_aborts(self):
        with (
            patch.object(match_products, "SCRAPE_CYCLE_DATE", "2026-08-12"),
            patch.object(match_products, "SCRAPE_FENCING_TOKEN", ""),
        ):
            with self.assertRaises(RuntimeError):
                match_products.assert_scrape_cycle_fence("startup")


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
import uuid
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
            match_products.should_allow_stock_markout(0, 0, {"success": True, "json_count": 0}),
            (False, "empty_output"),
        )
        self.assertEqual(
            match_products.should_allow_stock_markout(4, 4, {"success": True, "json_count": 4}),
            (True, "healthy_scrape"),
        )
        self.assertEqual(
            match_products.should_allow_stock_markout(100, 1, {"success": True, "json_count": 100}),
            (False, "low_match_rate"),
        )
        self.assertEqual(
            match_products.should_allow_stock_markout(100, 40, {"success": False}),
            (False, "scraper_failed"),
        )
        self.assertEqual(
            match_products.should_allow_stock_markout(100, 90, {"success": True, "json_count": 100}),
            (True, "healthy_scrape"),
        )

    def test_stock_markout_fails_closed_for_missing_partial_or_suspect_runs(self):
        self.assertEqual(
            match_products.should_allow_stock_markout(10, 10, None),
            (False, "missing_scraper_telemetry"),
        )
        self.assertEqual(
            match_products.should_allow_stock_markout(
                10,
                10,
                {"success": True, "json_count": 10, "partial": True},
            ),
            (False, "partial_output"),
        )
        self.assertEqual(
            match_products.should_allow_stock_markout(
                10,
                10,
                {"success": True, "json_count": 10},
                anomaly_count=1,
            ),
            (False, "price_anomalies"),
        )
        self.assertEqual(
            match_products.should_allow_stock_markout(
                10,
                10,
                {"success": True, "json_count": 10},
                input_error_count=1,
            ),
            (False, "processing_errors"),
        )

    def test_two_snapshot_markout_only_returns_old_unseen_products(self):
        active_rows = [
            {"SpecId": "old-unseen", "LastSeenAt": "2026-07-08T08:00:00Z"},
            {"SpecId": "recent-unseen", "LastSeenAt": "2026-07-09T10:00:00Z"},
            {"SpecId": "old-seen", "LastSeenAt": "2026-07-08T08:00:00Z"},
            {"SpecId": "unknown", "LastSeenAt": None},
        ]
        self.assertEqual(
            match_products.ids_missing_from_two_healthy_snapshots(
                active_rows,
                {"old-seen"},
                "2026-07-09T09:00:00Z",
            ),
            {"old-unseen"},
        )

    def test_price_anomalies_are_recorded_as_seen_before_quarantine(self):
        import inspect

        source = inspect.getsource(match_products.process_daily_scraps)
        seen_position = source.index("seen_ids_today.add(spec_id)")
        anomaly_position = source.index("anomaly_reason = detect_price_anomaly")
        self.assertLess(seen_position, anomaly_position)
        self.assertIn("seen_ids_today,\n                    previous_snapshot_started_at", source)

    def test_raw_rows_keep_scraped_name_and_candidates_reference_raw_id(self):
        item = {
            "store_name": "Tienda",
            "scraped_name": "GPU de prueba",
            "part #": "ABC-123",
            "type": "VideoCard",
            "price": "$100.000",
            "url": "https://example.test/product",
        }
        raw_row = match_products.build_raw_row(item, 1, "Tienda", "matched", parsed_price=100000)
        candidate = match_products.build_match_candidate_row(
            1,
            "Tienda",
            item,
            "spec-id",
            "GpuSpecifications",
            "part_number_exact",
            1,
            raw_id=raw_row["id"],
        )

        uuid.UUID(raw_row["id"])
        self.assertEqual(raw_row["scraped_name"], "GPU de prueba")
        self.assertEqual(candidate["raw_id"], raw_row["id"])

    def test_matching_database_errors_propagate_instead_of_becoming_unmatched(self):
        original_lookup = match_products.lookup_exact_part_number

        def fail_lookup(table_name, candidate):
            raise RuntimeError("database unavailable")

        try:
            match_products.lookup_exact_part_number = fail_lookup
            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                match_products.find_spec_match("GpuSpecifications", "ABC-123")
        finally:
            match_products.lookup_exact_part_number = original_lookup

    def test_matching_does_not_use_fuzzy_ilike(self):
        source = (SCRAPDB / "match_products.py").read_text(encoding="utf-8")
        self.assertNotIn(".ilike(", source)
        self.assertNotIn("part_number_fuzzy", source)

    def test_stock_markout_is_one_atomic_update_not_a_per_product_loop(self):
        import inspect

        source = inspect.getsource(match_products.mark_products_out_of_stock)
        self.assertIn('.in_("SpecId", normalized_ids)', source)

    def test_exact_matching_batches_part_number_variants(self):
        source = (SCRAPDB / "match_products.py").read_text(encoding="utf-8")
        self.assertIn('.in_("MetaPartNumber", variants)', source)
        self.assertNotIn('.eq("MetaPartNumber", variant)', source)

    def test_previous_price_lookup_is_cached_for_anomaly_detection(self):
        class Response:
            data = [{"Price": 100000}]

        calls = []
        original_execute = match_products.execute_db_request
        match_products.PREVIOUS_PRICE_CACHE.clear()

        def fake_execute(label, request_factory, attempts=1):
            calls.append(label)
            return Response()

        try:
            match_products.execute_db_request = fake_execute
            self.assertEqual(
                match_products.detect_price_anomaly(1, "spec-1", "GpuSpecifications", 500000),
                "price_spike:100000->500000",
            )
            self.assertEqual(
                match_products.detect_price_anomaly(1, "spec-1", "GpuSpecifications", 500000),
                "price_spike:100000->500000",
            )
        finally:
            match_products.execute_db_request = original_execute
            match_products.PREVIOUS_PRICE_CACHE.clear()

        self.assertEqual(len(calls), 1)

    def test_remote_protocol_errors_are_transient(self):
        class RemoteProtocolError(Exception):
            pass

        error = RemoteProtocolError(
            "<ConnectionTerminated error_code:0, last_stream_id:19999, additional_data:None>"
        )
        self.assertTrue(match_products.is_transient_db_error(error))

    def test_wrapped_httpx_errors_are_transient(self):
        class RemoteProtocolError(Exception):
            pass

        try:
            try:
                raise RemoteProtocolError("<ConnectionTerminated error_code:0>")
            except RemoteProtocolError as cause:
                raise RuntimeError("postgrest request failed") from cause
        except RuntimeError as wrapped:
            self.assertTrue(match_products.is_transient_db_error(wrapped))

    def test_schema_errors_are_not_transient_network_errors(self):
        self.assertFalse(match_products.is_transient_db_error(Exception("undefined column ProductPricing.Foo")))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from pydantic import ValidationError

from ScrapDB.raw_offer import (
    LegacyAdaptationError,
    RawOffer,
    adapt_legacy_offer,
    adapt_legacy_tree,
    discover_scraper_scripts,
    export_legacy_run,
    write_ndjson_gzip,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures" / "raw_offer"
FIXED_FETCHED_AT = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)


def load_fixture(name: str):
    return json.loads((FIXTURES_ROOT / name).read_text(encoding="utf-8"))


class RawOfferContractTests(unittest.TestCase):
    def test_contract_has_exact_roadmap_fields(self):
        self.assertEqual(
            list(RawOffer.model_fields),
            [
                "runId",
                "storeId",
                "category",
                "sourceListingId",
                "merchantSku",
                "mpns",
                "gtins",
                "brand",
                "name",
                "cashPrice",
                "cardPrice",
                "normalPrice",
                "currency",
                "availability",
                "url",
                "imageUrl",
                "fetchedAt",
                "payloadHash",
            ],
        )

    def test_fixture_registry_covers_every_discovered_scraper(self):
        registry = load_fixture("scrapers.json")
        discovered = discover_scraper_scripts(REPO_ROOT / "ScrapDB" / "PythonsScrap")
        self.assertEqual(len(discovered), 24)
        self.assertEqual(
            {path.name for path in discovered},
            {entry["script"] for entry in registry},
        )

        for entry in registry:
            with self.subTest(scraper=entry["script"]):
                slug = entry["outputStore"].casefold()
                offer = adapt_legacy_offer(
                    {
                        "store_name": entry["storeName"],
                        "scraped_name": f"Producto contractual {entry['storeName']}",
                        "scraped_brand": "Marca Demo",
                        "type": "CPU",
                        "part #": f"MPN-{slug}",
                        "price": "$99.990",
                        "url": f"https://example.cl/catalogo/{slug}",
                        "image_url": "N/A",
                    },
                    run_id="contract-all-scrapers",
                    store_id=entry["outputStore"],
                    fetched_at=FIXED_FETCHED_AT,
                    source_path=Path(entry["outputStore"]) / f"{entry['script']}.json",
                )
                self.assertEqual(offer.storeId, entry["outputStore"])
                self.assertEqual(offer.normalPrice, 99990)
                self.assertEqual(offer.currency, "CLP")

    def test_legacy_scenarios_cover_prices_identifiers_stock_and_errors(self):
        for scenario in load_fixture("scenarios.json"):
            with self.subTest(scenario=scenario["name"]):
                if "errorCode" in scenario:
                    with self.assertRaises(LegacyAdaptationError) as context:
                        adapt_legacy_offer(
                            scenario["payload"],
                            run_id="scenario-run",
                            fetched_at=FIXED_FETCHED_AT,
                        )
                    self.assertEqual(context.exception.code, scenario["errorCode"])
                    continue

                offer = adapt_legacy_offer(
                    scenario["payload"],
                    run_id="scenario-run",
                    fetched_at=FIXED_FETCHED_AT,
                )
                for field_name, expected in scenario["expected"].items():
                    self.assertEqual(getattr(offer, field_name), expected)

    def test_payload_hash_is_stable_and_changes_with_source_payload(self):
        payload = load_fixture("scenarios.json")[0]["payload"]
        first = adapt_legacy_offer(payload, run_id="hash-run", fetched_at=FIXED_FETCHED_AT)
        second = adapt_legacy_offer(payload, run_id="hash-run", fetched_at=FIXED_FETCHED_AT)
        changed = adapt_legacy_offer(
            {**payload, "price": "$130.990"},
            run_id="hash-run",
            fetched_at=FIXED_FETCHED_AT,
        )
        self.assertEqual(first.payloadHash, second.payloadHash)
        self.assertNotEqual(first.payloadHash, changed.payloadHash)
        self.assertRegex(first.payloadHash, r"^[0-9a-f]{64}$")

    def test_model_rejects_wrong_currency_prices_urls_dates_and_hashes(self):
        offer = adapt_legacy_offer(
            load_fixture("scenarios.json")[0]["payload"],
            run_id="strict-run",
            fetched_at=FIXED_FETCHED_AT,
        )
        valid = offer.model_dump(mode="json")
        mutations = {
            "currency": {**valid, "currency": "USD"},
            "zero_price": {**valid, "normalPrice": 0},
            "invalid_url": {**valid, "url": "javascript:alert(1)"},
            "naive_date": {**valid, "fetchedAt": "2026-07-10T12:00:00"},
            "invalid_hash": {**valid, "payloadHash": "not-a-sha256"},
            "descriptive_mpn": {
                **valid,
                "mpns": [
                    "Procesador para equipos de escritorio con multiples nucleos y graficos"
                ],
            },
        }
        for name, value in mutations.items():
            with self.subTest(mutation=name):
                with self.assertRaises(ValidationError):
                    RawOffer.model_validate(value)

    def test_ndjson_gzip_is_reproducible_and_round_trips(self):
        offer = adapt_legacy_offer(
            load_fixture("scenarios.json")[1]["payload"],
            run_id="gzip-run",
            fetched_at=FIXED_FETCHED_AT,
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            first_path = write_ndjson_gzip([offer], root / "first.ndjson.gz")
            second_path = write_ndjson_gzip([offer], root / "second.ndjson.gz")

            self.assertEqual(first_path.read_bytes(), second_path.read_bytes())
            with gzip.open(first_path, "rt", encoding="utf-8") as input_file:
                rows = [json.loads(line) for line in input_file if line.strip()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["runId"], "gzip-run")
            self.assertEqual(rows[0]["cashPrice"], 599990)

    def test_tree_adapter_quarantines_errors_and_export_writes_manifest(self):
        valid_payload = load_fixture("scenarios.json")[0]["payload"]
        invalid_payload = load_fixture("scenarios.json")[4]["payload"]
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            input_root = root / "outputs"
            store_root = input_root / "DemoStore"
            store_root.mkdir(parents=True)
            (store_root / "valid.json").write_text(
                json.dumps(valid_payload), encoding="utf-8"
            )
            (store_root / "invalid.json").write_text(
                json.dumps(invalid_payload), encoding="utf-8"
            )
            (store_root / "broken.json").write_text("{", encoding="utf-8")

            batch = adapt_legacy_tree(
                input_root, run_id="batch-run", fetched_at=FIXED_FETCHED_AT
            )
            self.assertEqual(batch.rawCount, 3)
            self.assertEqual(len(batch.offers), 1)
            self.assertEqual(len(batch.issues), 2)
            self.assertEqual(
                {issue.code for issue in batch.issues}, {"invalid_json", "invalid_price"}
            )

            result = export_legacy_run(
                input_root,
                root / "raw-runs",
                run_id="batch-run",
                fetched_at=FIXED_FETCHED_AT,
            )
            self.assertEqual(result.offerCount, 1)
            self.assertEqual(result.errorCount, 2)
            self.assertTrue(Path(result.ndjsonPath).is_file())
            self.assertTrue(Path(result.errorsPath).is_file())
            self.assertTrue(Path(result.manifestPath).is_file())
            self.assertRegex(result.compressedSha256, r"^[0-9a-f]{64}$")


if __name__ == "__main__":
    unittest.main()

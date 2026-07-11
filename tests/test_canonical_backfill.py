from __future__ import annotations

import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence
from unittest.mock import patch

from ScrapDB.canonical_backfill import (
    CATEGORY_CONFIGS,
    CanonicalBackfill,
    CheckpointFile,
    RawOfferMatchIndex,
    RawOfferMigrator,
    SnapshotGateway,
    build_product_rows,
    checkpoint_fingerprint,
    main,
    match_raw_offer,
    resolve_categories,
)
from ScrapDB.raw_offer import RawOffer


NOW = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
CPU = CATEGORY_CONFIGS[0]


def raw_offer(**changes: Any) -> RawOffer:
    payload: dict[str, Any] = {
        "runId": "17bb466d-52bd-4662-bdb8-a2982d26de03",
        "storeId": "Demo Store",
        "category": "CPU",
        "sourceListingId": "listing-1",
        "merchantSku": None,
        "mpns": [],
        "gtins": [],
        "brand": "AMD",
        "name": "AMD Ryzen 7 7800X3D",
        "cashPrice": 399990,
        "cardPrice": None,
        "normalPrice": 419990,
        "currency": "CLP",
        "availability": "available",
        "url": "https://store.example/product/7800x3d?utm_source=test",
        "imageUrl": None,
        "fetchedAt": NOW.isoformat(),
        "payloadHash": "a" * 64,
    }
    payload.update(changes)
    return RawOffer.model_validate(payload)


class FakeGateway:
    def __init__(
        self,
        *,
        specs: Mapping[str, Sequence[dict[str, Any]]] | None = None,
        pricing: Mapping[str, Sequence[dict[str, Any]]] | None = None,
        match_catalog: tuple[
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
            list[dict[str, Any]],
        ] | None = None,
    ):
        self.specs = {key: list(value) for key, value in (specs or {}).items()}
        self.pricing = {key: list(value) for key, value in (pricing or {}).items()}
        self.match_catalog = match_catalog or ([], [], [], [])
        self.stores = {1: "Demo Store"}
        self.writes: list[tuple[str, list[dict[str, Any]], str]] = []
        self.tables: dict[str, list[dict[str, Any]]] = {}
        self.iter_calls: list[tuple[str, int, str | None]] = []

    def iter_specifications(
        self, spec_table: str, batch_size: int, start_after: str | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        self.iter_calls.append((spec_table, batch_size, start_after))
        rows = sorted(self.specs.get(spec_table, []), key=lambda row: str(row.get("Id")))
        if start_after:
            rows = [row for row in rows if str(row.get("Id")) > start_after]
        for offset in range(0, len(rows), batch_size):
            yield rows[offset : offset + batch_size]

    def load_pricing_for_specs(
        self, spec_table: str, spec_ids: Sequence[str], batch_size: int
    ) -> list[dict[str, Any]]:
        selected = set(spec_ids)
        return [
            dict(row)
            for row in self.pricing.get(spec_table, [])
            if str(row.get("SpecId")) in selected
        ]

    def load_stores(self, batch_size: int) -> dict[int, str]:
        return dict(self.stores)

    def upsert_rows(
        self, table: str, rows: Sequence[dict[str, Any]], on_conflict: str, batch_size: int
    ) -> None:
        copied = [dict(row) for row in rows]
        self.writes.append((table, copied, on_conflict))
        keys = [part.strip() for part in on_conflict.split(",")]
        current = self.tables.setdefault(table, [])
        for row in copied:
            identity = tuple(row.get(key) for key in keys)
            current[:] = [
                existing
                for existing in current
                if tuple(existing.get(key) for key in keys) != identity
            ]
            current.append(row)

    def load_rows_by_values(
        self,
        table: str,
        value_column: str,
        values: Sequence[Any],
        columns: str,
        batch_size: int,
        equals: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        accepted = {str(value) for value in values}
        return [
            dict(row)
            for row in self.tables.get(table, [])
            if str(row.get(value_column)) in accepted
            and all(row.get(key) == value for key, value in (equals or {}).items())
        ]

    def load_match_catalog(
        self, categories: Sequence[str], batch_size: int
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        return self.match_catalog


def match_catalog() -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    products = [
        {"id": "p1", "category": "cpu", "brand": "AMD", "name": "AMD Ryzen 7 7800X3D"},
        {"id": "p2", "category": "cpu", "brand": "Intel", "name": "Intel Core i5 14600K"},
    ]
    identifiers = [
        {"product_id": "p1", "identifier_type": "gtin", "normalized_value": "1234567890123"},
        {"product_id": "p1", "identifier_type": "mpn", "normalized_value": "100-100000910WOF"},
        {"product_id": "p2", "identifier_type": "mpn", "normalized_value": "BX8071514600K"},
    ]
    listings = [
        {
            "store_id": 1,
            "product_id": "p1",
            "source_listing_id": "stable-listing",
            "merchant_sku": "SKU-7800",
            "url": "https://store.example/product/7800x3d",
        }
    ]
    refs = [
        {"product_id": "p1", "spec_table_name": "CPUSpecifications", "spec_id": "legacy-p1"},
        {"product_id": "p2", "spec_table_name": "CPUSpecifications", "spec_id": "legacy-p2"},
    ]
    return products, identifiers, listings, refs


class CanonicalBackfillTests(unittest.TestCase):
    def test_feature_flags_default_to_first_wave_and_can_enable_all_essential(self):
        self.assertEqual(
            [category.key for category in resolve_categories(None)],
            ["CPU", "GPU", "Motherboard"],
        )
        self.assertEqual(len(resolve_categories(None, all_essential=True)), 8)
        self.assertEqual(
            [category.canonical_slug for category in resolve_categories(None, all_essential=True)],
            [
                "cpu",
                "gpu",
                "motherboard",
                "ram",
                "internalstorage",
                "powersupply",
                "case",
                "cpucooler",
            ],
        )
        self.assertEqual(
            [category.key for category in resolve_categories("ram,psu,cooler")],
            ["Memory", "PowerSupply", "CPUCooler"],
        )
        with self.assertRaises(ValueError):
            resolve_categories("cpu,televisores")

    def test_product_backfill_splits_mpn_and_gtin_without_authorizing_images(self):
        product, identifiers, _, provenance, fallback = build_product_rows(
            CPU,
            {
                "Id": "legacy-1",
                "MetaName": "AMD Ryzen 7",
                "MetaPartNumber": "['100-ABC', '100-DEF']",
                "EAN": "1234567890123",
                "ImageUrl": "https://images.example/cpu.jpg",
            },
        )
        self.assertFalse(fallback)
        self.assertFalse(product["image_authorized"])
        self.assertEqual(
            {(row["identifier_type"], row["normalized_value"]) for row in identifiers},
            {("mpn", "100ABC"), ("mpn", "100DEF"), ("ean", "1234567890123")},
        )
        self.assertTrue(any(row["asset_type"] == "image" for row in provenance))

    def test_matching_precedence_is_gtin_then_mpn_then_persistent_listing(self):
        index = RawOfferMatchIndex.from_rows(*match_catalog())
        decision = match_raw_offer(
            raw_offer(gtins=["1234567890123"], mpns=["BX8071514600K"]),
            1,
            CPU,
            index,
        )
        self.assertEqual((decision.status, decision.method, decision.product_id), ("matched", "exact_gtin", "p1"))

        decision = match_raw_offer(
            raw_offer(
                gtins=["9999999999999"],
                mpns=["BX8071514600K"],
                brand="Intel",
            ),
            1,
            CPU,
            index,
        )
        self.assertEqual((decision.method, decision.product_id), ("exact_mpn", "p2"))

        decision = match_raw_offer(
            raw_offer(
                sourceListingId="stable-listing",
                merchantSku="SKU-7800",
                url="https://store.example/product/7800x3d/?utm_campaign=ignored",
            ),
            1,
            CPU,
            index,
        )
        self.assertEqual((decision.method, decision.product_id), ("persistent_sku", "p1"))

    def test_gtin_is_globally_unambiguous_even_outside_enabled_category(self):
        products, identifiers, listings, refs = match_catalog()
        products.append(
            {"id": "p3", "category": "gpu", "brand": "AMD", "name": "GPU Demo"}
        )
        identifiers.append(
            {
                "product_id": "p3",
                "identifier_type": "gtin",
                "normalized_value": "1234567890123",
            }
        )
        decision = match_raw_offer(
            raw_offer(gtins=["1234567890123"]),
            1,
            CPU,
            RawOfferMatchIndex.from_rows(products, identifiers, listings, refs),
        )
        self.assertEqual((decision.status, decision.method), ("candidate", "exact_gtin"))
        self.assertEqual(set(decision.candidate_product_ids), {"p1", "p3"})

    def test_mpn_requires_an_unambiguous_exact_brand_pair(self):
        products, identifiers, listings, refs = match_catalog()
        identifiers.append(
            {
                "product_id": "p2",
                "identifier_type": "mpn",
                "normalized_value": "100-100000910WOF",
            }
        )
        index = RawOfferMatchIndex.from_rows(products, identifiers, listings, refs)

        amd = match_raw_offer(
            raw_offer(mpns=["100-100000910WOF"], brand="AMD"), 1, CPU, index
        )
        self.assertEqual((amd.status, amd.product_id), ("matched", "p1"))

        missing_brand = match_raw_offer(
            raw_offer(mpns=["100-100000910WOF"], brand=None), 1, CPU, index
        )
        self.assertEqual((missing_brand.status, missing_brand.reason), ("candidate", "mpn_requires_brand"))

        conflicting_brand = match_raw_offer(
            raw_offer(mpns=["100-100000910WOF"], brand="NVIDIA"), 1, CPU, index
        )
        self.assertEqual(
            (conflicting_brand.status, conflicting_brand.reason),
            ("candidate", "mpn_brand_conflict"),
        )

    def test_ambiguous_exact_and_fuzzy_matches_never_auto_publish(self):
        products, identifiers, listings, refs = match_catalog()
        identifiers.append(
            {"product_id": "p2", "identifier_type": "gtin", "normalized_value": "1234567890123"}
        )
        index = RawOfferMatchIndex.from_rows(products, identifiers, listings, refs)
        ambiguous = match_raw_offer(raw_offer(gtins=["1234567890123"]), 1, CPU, index)
        self.assertEqual(ambiguous.status, "candidate")
        self.assertIsNone(ambiguous.product_id)

        fuzzy = match_raw_offer(
            raw_offer(
                sourceListingId="new-fuzzy-listing",
                name="AMD Ryzen 7 7800X3D Procesador",
                brand="AMD",
                url="https://store.example/new-fuzzy-listing",
            ),
            1,
            CPU,
            index,
        )
        self.assertEqual((fuzzy.status, fuzzy.method), ("candidate", "fuzzy_candidate"))
        self.assertNotEqual(fuzzy.status, "matched")

    def test_dry_run_does_not_write_remote_or_create_checkpoint(self):
        gateway = FakeGateway(
            specs={"CPUSpecifications": [{"Id": "1", "MetaName": "CPU Demo"}]},
            pricing={
                "CPUSpecifications": [
                    {
                        "SpecId": "1",
                        "StoreId": 1,
                        "Price": 99990,
                        "StockStatus": True,
                        "Url": "https://store.example/cpu",
                        "LastSeenAt": NOW.isoformat(),
                    }
                ]
            },
        )
        report = CanonicalBackfill(gateway, [CPU], apply=False, now=NOW).run()
        self.assertEqual(report.mode, "dry-run")
        self.assertEqual(report.categories[0].offers_planned, 1)
        self.assertEqual(gateway.writes, [])

    def test_cli_dry_run_uses_only_explicit_local_snapshot(self):
        _, _, legacy_ref, _, _ = build_product_rows(
            CPU, {"Id": "legacy-1", "MetaName": "CPU Demo"}
        )
        snapshot = {
            "snapshotFormatVersion": 1,
            "specifications": {
                "CPUSpecifications": [{"Id": "legacy-1", "MetaName": "CPU Demo"}]
            },
            "ProductPricing": [],
            "Stores": [{"Id": 1, "Name": "Demo Store"}],
            "canonical": {
                "products": [],
                "product_identifiers": [],
                "merchant_listings": [],
                "offers": [],
                "source_provenance": [],
                "legacy_product_refs": [legacy_ref],
                "scrape_runs": [],
            },
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "snapshot.json"
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            output = io.StringIO()
            with patch("ScrapDB.canonical_backfill.create_gateway") as remote_gateway:
                with redirect_stdout(output):
                    code = main(
                        [
                            "--snapshot",
                            str(path),
                            "--categories",
                            "CPU",
                            "--compare",
                        ]
                    )
            remote_gateway.assert_not_called()
            self.assertEqual(code, 0)
            report = json.loads(output.getvalue())
            self.assertEqual(report["mode"], "dry-run")
            self.assertEqual(
                report["categories"][0]["comparison"]["legacy_refs_found"], 1
            )

    def test_snapshot_gateway_is_read_only(self):
        gateway = SnapshotGateway(
            {
                "snapshotFormatVersion": 1,
                "specifications": {},
                "ProductPricing": [],
                "Stores": {"1": "Demo Store"},
                "canonical": {},
            }
        )
        with self.assertRaises(RuntimeError):
            gateway.upsert_rows("products", [{"id": "p1"}], "id", 10)

    def test_apply_checkpoint_is_atomic_and_resume_skips_completed_category(self):
        gateway = FakeGateway(specs={"CPUSpecifications": [{"Id": "1", "MetaName": "CPU Demo"}]})
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "checkpoint.json"
            fingerprint = checkpoint_fingerprint([CPU], 48)
            checkpoint = CheckpointFile(path, fingerprint)
            first = CanonicalBackfill(
                gateway,
                [CPU],
                apply=True,
                now=NOW,
                checkpoint=checkpoint,
            ).run()
            state = json.loads(path.read_text(encoding="utf-8"))
            self.assertTrue(state["categories"]["CPUSpecifications"]["completed"])
            self.assertEqual(first.categories[0].canonical_products_planned, 1)

            resumed_gateway = FakeGateway(specs=gateway.specs)
            resumed = CanonicalBackfill(
                resumed_gateway,
                [CPU],
                apply=True,
                now=NOW,
                checkpoint=CheckpointFile(path, fingerprint, resume=True),
            ).run()
            self.assertTrue(resumed.categories[0].checkpoint_completed_before_run)
            self.assertEqual(resumed_gateway.iter_calls, [])
            self.assertEqual(resumed_gateway.writes, [])

            with self.assertRaises(ValueError):
                CheckpointFile(path, checkpoint_fingerprint([CPU], 24), resume=True)
            with self.assertRaises(ValueError):
                CheckpointFile(
                    path,
                    checkpoint_fingerprint([CPU], 48, dual_write=True),
                    resume=True,
                )

    def test_raw_offer_dry_run_plans_matches_but_performs_no_writes(self):
        gateway = FakeGateway(match_catalog=match_catalog())
        report = RawOfferMigrator(
            gateway,
            [CPU],
            apply=False,
            dual_write=True,
            now=NOW,
        ).run([raw_offer(mpns=["100-100000910WOF"])])
        self.assertEqual(report["matched"]["exact_mpn"], 1)
        self.assertEqual(report["legacyRowsPlanned"], 1)
        self.assertEqual(gateway.writes, [])

    def test_raw_offer_apply_is_idempotent_and_dual_write_uses_exact_matches_only(self):
        gateway = FakeGateway(match_catalog=match_catalog())
        migrator = RawOfferMigrator(
            gateway,
            [CPU],
            apply=True,
            dual_write=True,
            compare=True,
            now=NOW,
        )
        exact = raw_offer(mpns=["100-100000910WOF"])
        candidate = raw_offer(
            sourceListingId="candidate-1",
            name="AMD Ryzen 7 7800X3D Procesador",
            brand="AMD",
            url="https://store.example/candidate",
        )
        first = migrator.run([exact, candidate])
        second = migrator.run([exact, candidate])

        self.assertEqual(first["legacyRowsPlanned"], 1)
        self.assertEqual(first["candidates"], 1)
        self.assertEqual(first["comparison"]["missing_offers"], 0)
        self.assertEqual(len(gateway.tables["merchant_listings"]), 2)
        self.assertEqual(len(gateway.tables["offers"]), 2)
        self.assertEqual(len(gateway.tables["ProductPricing"]), 1)
        self.assertEqual(second["offersPlanned"], 2)
        candidate_listing = next(
            row for row in gateway.tables["merchant_listings"] if row["source_listing_id"] == "candidate-1"
        )
        candidate_offer = next(
            row
            for row in gateway.tables["offers"]
            if row["merchant_listing_id"] == candidate_listing["id"]
        )
        self.assertEqual(candidate_listing["match_status"], "candidate")
        self.assertEqual(candidate_offer["public_state"], "suspect")
        self.assertIsNone(candidate_offer["normal_price"])
        self.assertEqual(candidate_offer["published_price"], 419990)

    def test_raw_offer_checkpoint_tracks_validated_offer_count_and_artifact_hash(self):
        gateway = FakeGateway(match_catalog=match_catalog())
        with tempfile.TemporaryDirectory() as temporary_directory:
            checkpoint = CheckpointFile(
                Path(temporary_directory) / "checkpoint.json",
                checkpoint_fingerprint([CPU], 48),
            )
            report = RawOfferMigrator(
                gateway,
                [CPU],
                apply=True,
                batch_size=1,
                now=NOW,
            ).run(
                [
                    raw_offer(mpns=["100-100000910WOF"]),
                    raw_offer(
                        sourceListingId="second",
                        mpns=["BX8071514600K"],
                        brand="Intel",
                        name="Intel Core i5 14600K",
                        url="https://store.example/second",
                    ),
                ],
                checkpoint=checkpoint,
                artifact_hash="b" * 64,
            )
            self.assertEqual(report["rawOffersSeen"], 2)
            self.assertEqual(checkpoint.raw_after_count("b" * 64), 2)
            self.assertTrue(checkpoint.raw_completed("b" * 64))
            with self.assertRaises(ValueError):
                checkpoint.raw_after_count("c" * 64)

    def test_raw_offer_replay_cannot_overwrite_a_newer_canonical_offer(self):
        gateway = FakeGateway(match_catalog=match_catalog())
        migrator = RawOfferMigrator(gateway, [CPU], apply=True, dual_write=True, now=NOW)
        current = raw_offer(mpns=["100-100000910WOF"])
        migrator.run([current])
        older = raw_offer(
            mpns=["100-100000910WOF"],
            fetchedAt=(NOW - timedelta(hours=24)).isoformat(),
            cashPrice=299990,
            payloadHash="d" * 64,
        )
        report = migrator.run([older])

        self.assertEqual(report["offersPlanned"], 0)
        self.assertEqual(report["skipped"]["older_than_existing"], 1)
        self.assertEqual(gateway.tables["offers"][0]["captured_at"], NOW.isoformat().replace("+00:00", "Z"))
        self.assertEqual(gateway.tables["offers"][0]["cash_price"], 399990)

    def test_dual_write_cannot_overwrite_newer_legacy_pricing(self):
        gateway = FakeGateway(match_catalog=match_catalog())
        gateway.tables["ProductPricing"] = [
            {
                "SpecId": "legacy-p1",
                "SpecTableName": "CPUSpecifications",
                "StoreId": 1,
                "Price": 450000,
                "LastUpdated": (NOW + timedelta(minutes=5)).isoformat(),
            }
        ]
        report = RawOfferMigrator(
            gateway,
            [CPU],
            apply=True,
            dual_write=True,
            now=NOW,
        ).run([raw_offer(mpns=["100-100000910WOF"])])

        self.assertEqual(report["legacyRowsPlanned"], 0)
        self.assertEqual(report["skipped"]["legacy_older_than_existing"], 1)
        self.assertEqual(gateway.tables["ProductPricing"][0]["Price"], 450000)
        self.assertEqual(len(gateway.tables["offers"]), 1)


if __name__ == "__main__":
    unittest.main()

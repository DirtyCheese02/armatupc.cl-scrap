from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inventory_provenance import build_inventory


class ProvenanceInventoryTests(unittest.TestCase):
    def test_inventory_is_aggregate_and_defaults_to_not_monetizable(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            pcpp_root = root / "SpecDB" / "ScrapedDataPCPP" / "CPU"
            pcpp_root.mkdir(parents=True)
            (pcpp_root / "cpu.json").write_text("{}", encoding="utf-8")

            scrapers_root = root / "ScrapDB" / "PythonsScrap"
            scrapers_root.mkdir(parents=True)
            (scrapers_root / "Scrap_Demo.py").write_text(
                'output_dir = "ScrapDB/Outputs/Demo"\n', encoding="utf-8"
            )
            output_root = root / "ScrapDB" / "Outputs" / "Demo"
            output_root.mkdir(parents=True)
            (output_root / "offer.json").write_text("{}", encoding="utf-8")

            records = build_inventory(root)
            self.assertEqual(len(records), 2)
            by_id = {record.sourceId: record for record in records}
            self.assertEqual(by_id["catalog:pcpartpicker"].fileCount, 1)
            self.assertEqual(by_id["catalog:pcpartpicker"].capturePolicy, "suspended_pending_written_permission")
            self.assertEqual(by_id["merchant:demo"].fileCount, 1)
            self.assertFalse(by_id["merchant:demo"].monetizationEligible)
            self.assertEqual(by_id["merchant:demo"].permissionStatus, "not_documented")

    def test_documented_permission_registry_can_mark_a_merchant_eligible(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            scrapers_root = root / "ScrapDB" / "PythonsScrap"
            scrapers_root.mkdir(parents=True)
            (scrapers_root / "Scrap_Demo.py").write_text(
                'output_dir = "ScrapDB/Outputs/Demo"\n', encoding="utf-8"
            )
            registry_path = root / "permissions.json"
            registry_path.write_text(
                json.dumps(
                    {
                        "merchant:demo": {
                            "permissionStatus": "written_permission",
                            "permissionReference": "email-2026-07-10",
                            "permissionRecordedAt": "2026-07-10T00:00:00Z",
                            "monetizationEligible": True,
                        }
                    }
                ),
                encoding="utf-8",
            )

            records = build_inventory(root, permission_registry=registry_path)
            merchant = next(record for record in records if record.sourceId == "merchant:demo")
            self.assertTrue(merchant.monetizationEligible)
            self.assertEqual(merchant.permissionReference, "email-2026-07-10")

    def test_eligibility_flag_without_evidence_remains_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            scrapers_root = root / "ScrapDB" / "PythonsScrap"
            scrapers_root.mkdir(parents=True)
            (scrapers_root / "Scrap_Demo.py").write_text(
                'output_dir = "ScrapDB/Outputs/Demo"\n', encoding="utf-8"
            )
            registry_path = root / "permissions.json"
            registry_path.write_text(
                json.dumps(
                    {
                        "merchant:demo": {
                            "permissionStatus": "written_permission",
                            "monetizationEligible": True,
                        }
                    }
                ),
                encoding="utf-8",
            )

            records = build_inventory(root, permission_registry=registry_path)
            merchant = next(record for record in records if record.sourceId == "merchant:demo")
            self.assertFalse(merchant.monetizationEligible)


if __name__ == "__main__":
    unittest.main()

import json
import os
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRAPDB = ROOT / "ScrapDB"
sys.path.insert(0, str(SCRAPDB))

import run_all_scrapers
import scraper_retry_manifest


class ScraperTelemetryTest(unittest.TestCase):
    def test_runner_infers_clean_output_dir_assignment(self):
        inferred = run_all_scrapers._infer_output_dir(
            ROOT / "ScrapDB" / "PythonsScrap" / "Scrap_TecnoMaster.py"
        )
        self.assertEqual(inferred, ROOT / "ScrapDB" / "Outputs" / "TecnoMaster")

    def test_github_shards_resolve_the_same_scrape_run_id(self):
        env = {
            "GITHUB_REPOSITORY": "owner/repo",
            "GITHUB_WORKFLOW": "ScrapDB Daily",
            "GITHUB_RUN_ID": "12345",
            "GITHUB_RUN_ATTEMPT": "1",
        }
        with patch.dict(os.environ, env, clear=True):
            first = run_all_scrapers._resolve_scrape_run_id()
            second = run_all_scrapers._resolve_scrape_run_id()

        self.assertEqual(first, second)
        uuid.UUID(first)

    def test_manifest_preserves_run_relationship_and_matcher_summary(self):
        scrape_run_id = str(uuid.uuid4())
        parent_run_id = str(uuid.uuid4())
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            logs = root / "logs" / "shard"
            outputs = root / "outputs"
            logs.mkdir(parents=True)
            outputs.mkdir()
            (logs / "summary.json").write_text(
                json.dumps(
                    {
                        "run_id": "20260710_090000",
                        "scrape_run_id": scrape_run_id,
                        "parent_scrape_run_id": parent_run_id,
                        "source": "retry",
                        "run_started_at_utc": "2026-07-10T09:00:00+00:00",
                        "scraper_results": [
                            {
                                "name": "Scrap_CentralGamer.py",
                                "success": True,
                                "json_count": 12,
                                "output_complete": True,
                                "partial": False,
                                "started_at_utc": "2026-07-10T09:00:01+00:00",
                                "finished_at_utc": "2026-07-10T09:01:00+00:00",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            manifest = scraper_retry_manifest.build_manifest(
                log_roots=[root / "logs"],
                outputs_root=outputs,
                prune_failed=False,
            )

        self.assertEqual(manifest["scrape_run_id"], scrape_run_id)
        self.assertEqual(manifest["parent_scrape_run_id"], parent_run_id)
        self.assertEqual(manifest["source"], "retry")
        self.assertEqual(manifest["scraper_results"][0]["name"], "Scrap_CentralGamer.py")
        self.assertTrue(manifest["scraper_results"][0]["output_complete"])

    def test_partial_scraper_is_retryable_without_being_pruned_as_hard_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            logs = root / "logs"
            outputs = root / "outputs"
            logs.mkdir()
            outputs.mkdir()
            (logs / "summary.json").write_text(
                json.dumps({
                    "run_id": "partial",
                    "scrape_run_id": str(uuid.uuid4()),
                    "run_started_at_utc": "2026-07-11T10:00:00+00:00",
                    "scraper_results": [{
                        "name": "Scrap_Winpy.py", "success": True, "partial": True,
                        "output_complete": False, "json_count": 420,
                        "health_status": "partial_success", "failed_categories": ["UPS"],
                    }],
                }), encoding="utf-8"
            )
            manifest = scraper_retry_manifest.build_manifest(
                log_roots=[logs], outputs_root=outputs, prune_failed=True
            )
        self.assertIn("Scrap_Winpy.py", manifest["failed_scrapers"])
        self.assertIn("Scrap_Winpy.py", manifest["partial_scrapers"])
        self.assertEqual(manifest["hard_failed_scrapers"], [])
        self.assertEqual(manifest["pruned_output_dirs"], [])

    def test_daily_and_retry_workflows_gate_canonical_dual_write(self):
        for workflow_name in ("scrapdb-daily.yml", "scrapdb-retry-failed.yml"):
            workflow = (ROOT / ".github" / "workflows" / workflow_name).read_text(
                encoding="utf-8"
            )
            with self.subTest(workflow=workflow_name):
                self.assertIn("vars.CANONICAL_DUAL_WRITE_ENABLED == 'true'", workflow)
                self.assertIn("SUPABASE_SERVICE_ROLE_KEY", workflow)
                self.assertIn("python -m ScrapDB.raw_offer", workflow)
                self.assertIn("python -m ScrapDB.canonical_backfill", workflow)
                self.assertIn("--raw-offers-only", workflow)
                self.assertIn("--dual-write", workflow)
                self.assertIn("CANONICAL_DUAL_WRITE_CATEGORIES", workflow)
                self.assertIn(
                    "CPU,GPU,Motherboard,Memory,Storage,PowerSupply,Case,CPUCooler",
                    workflow,
                )
                self.assertIn('--categories "$CANONICAL_CATEGORIES"', workflow)

    def test_github_outputs_include_scrape_run_ids(self):
        scrape_run_id = str(uuid.uuid4())
        parent_run_id = str(uuid.uuid4())
        manifest = {
            "failed_scrapers": [],
            "successful_scrapers": ["Scrap_CentralGamer.py"],
            "failed_output_dirs": [],
            "successful_output_dirs": ["ScrapDB/Outputs/CentralGamer"],
            "remaining_json_count": 1,
            "scrape_run_id": scrape_run_id,
            "parent_scrape_run_id": parent_run_id,
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "github-output.txt"
            with patch.dict(os.environ, {"GITHUB_OUTPUT": str(output_path)}):
                scraper_retry_manifest.write_github_outputs(manifest)
            output = output_path.read_text(encoding="utf-8")

        self.assertIn(f"scrape_run_id={scrape_run_id}", output)
        self.assertIn(f"parent_scrape_run_id={parent_run_id}", output)


if __name__ == "__main__":
    unittest.main()

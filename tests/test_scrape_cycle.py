import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ScrapDB"))

from scrape_cycle import watchdog_action


class ScrapeCycleWatchdogTests(unittest.TestCase):
    def test_missing_cycle_runs_full_fallback(self):
        self.assertEqual(watchdog_action(None), "full")

    def test_complete_cycle_skips(self):
        self.assertEqual(watchdog_action({"state": "complete"}), "skip")

    def test_partial_cycle_retries_only_failures(self):
        self.assertEqual(watchdog_action({"state": "partial"}), "retry")

    def test_active_lease_waits(self):
        now = datetime.now(timezone.utc)
        status = {
            "state": "running",
            "lease_expires_at": (now + timedelta(minutes=5)).isoformat(),
        }
        self.assertEqual(watchdog_action(status, now), "wait")

    def test_expired_lease_runs_full_fallback(self):
        now = datetime.now(timezone.utc)
        status = {
            "state": "running",
            "lease_expires_at": (now - timedelta(seconds=1)).isoformat(),
        }
        self.assertEqual(watchdog_action(status, now), "full")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ScrapDB import database_maintenance


class DatabaseMaintenanceTests(unittest.TestCase):
    def test_rpc_retries_transient_failures(self):
        request = Mock()
        request.execute.side_effect = [
            RuntimeError("temporary 522"),
            RuntimeError("temporary 522"),
            SimpleNamespace(data=3),
        ]
        client = Mock()
        client.rpc.return_value = request

        with patch.object(database_maintenance.time, "sleep") as sleep:
            response = database_maintenance._rpc(client, "example", {"p_limit": 5})

        self.assertEqual(response.data, 3)
        self.assertEqual(request.execute.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [2, 4])

    def test_post_run_expires_purges_and_refreshes(self):
        responses = [
            SimpleNamespace(data=2),
            SimpleNamespace(data=0),
            SimpleNamespace(data={"rawDeleted": 2, "issuesClosed": 1}),
            SimpleNamespace(data={"rawDeleted": 0, "issuesClosed": 0}),
            SimpleNamespace(data=7),
        ]

        tuesday = database_maintenance.dt.datetime(2026, 8, 18, tzinfo=database_maintenance.ZoneInfo("America/Santiago"))
        with (
            patch.object(database_maintenance, "_client", return_value=Mock()),
            patch.object(database_maintenance, "_rpc", side_effect=responses) as rpc,
            patch.object(database_maintenance.dt, "datetime", wraps=database_maintenance.dt.datetime) as datetime_mock,
        ):
            datetime_mock.now.return_value = tuesday
            result = database_maintenance.post_run(batch_size=2, max_batches=5)

        self.assertEqual(
            result,
            {
                "offersExpired": 2,
                "rawDeleted": 2,
                "issuesClosed": 1,
                "dailyStatsRefreshed": 7,
                "weeklyReportsRefreshed": 0,
            },
        )
        self.assertEqual(
            [call.args[1] for call in rpc.call_args_list],
            [
                "expire_canonical_offers",
                "expire_canonical_offers",
                "purge_scrape_diagnostics",
                "purge_scrape_diagnostics",
                "refresh_daily_product_stats",
            ],
        )

    def test_post_run_refreshes_weekly_reports_on_monday(self):
        responses = [
            SimpleNamespace(data=0),
            SimpleNamespace(data={"rawDeleted": 0, "issuesClosed": 0}),
            SimpleNamespace(data=7),
            SimpleNamespace(data={"reportsWritten": 8}),
        ]
        monday = database_maintenance.dt.datetime(2026, 8, 24, tzinfo=database_maintenance.ZoneInfo("America/Santiago"))
        with (
            patch.object(database_maintenance, "_client", return_value=Mock()),
            patch.object(database_maintenance, "_rpc", side_effect=responses) as rpc,
            patch.object(database_maintenance.dt, "datetime", wraps=database_maintenance.dt.datetime) as datetime_mock,
        ):
            datetime_mock.now.return_value = monday
            result = database_maintenance.post_run(batch_size=5000, max_batches=5)

        self.assertEqual(result["weeklyReportsRefreshed"], 8)
        self.assertEqual(rpc.call_args_list[-1].args[1], "refresh_weekly_category_market_reports")
        self.assertEqual(rpc.call_args_list[-1].args[2], {"p_week_end": "2026-08-24"})


if __name__ == "__main__":
    unittest.main()

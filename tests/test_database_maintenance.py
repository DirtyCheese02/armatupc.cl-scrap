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

        with (
            patch.object(database_maintenance, "_client", return_value=Mock()),
            patch.object(database_maintenance, "_rpc", side_effect=responses) as rpc,
        ):
            result = database_maintenance.post_run(batch_size=2, max_batches=5)

        self.assertEqual(
            result,
            {
                "offersExpired": 2,
                "rawDeleted": 2,
                "issuesClosed": 1,
                "dailyStatsRefreshed": 7,
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


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import argparse
import json
import os
import time

from dotenv import load_dotenv
from supabase import create_client


def _client():
    load_dotenv()
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
    return create_client(url, key)


def _rpc(client, name: str, params: dict, *, attempts: int = 4):
    for attempt in range(1, attempts + 1):
        try:
            return client.rpc(name, params).execute()
        except Exception:
            if attempt == attempts:
                raise
            time.sleep(2**attempt)


def post_run(*, batch_size: int = 5000, max_batches: int = 100) -> dict[str, int]:
    client = _client()
    offers_expired = 0
    for _ in range(max_batches):
        response = _rpc(client, "expire_canonical_offers", {"p_limit": batch_size})
        expired = int(response.data or 0)
        offers_expired += expired
        if expired < batch_size:
            break
    raw_deleted = 0
    issues_closed = 0
    for _ in range(max_batches):
        response = _rpc(client, "purge_scrape_diagnostics", {"p_limit": batch_size})
        payload = response.data or {}
        deleted = int(payload.get("rawDeleted") or 0)
        closed = int(payload.get("issuesClosed") or 0)
        raw_deleted += deleted
        issues_closed += closed
        if deleted < batch_size and closed < batch_size:
            break
    stats_response = _rpc(client, "refresh_daily_product_stats", {})
    stats_refreshed = int(stats_response.data or 0)
    result = {
        "offersExpired": offers_expired,
        "rawDeleted": raw_deleted,
        "issuesClosed": issues_closed,
        "dailyStatsRefreshed": stats_refreshed,
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Run bounded ScrapDB database maintenance tasks.")
    parser.add_argument("command", choices=("post-run",))
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--max-batches", type=int, default=100)
    args = parser.parse_args()
    if args.batch_size < 1 or args.batch_size > 10000 or args.max_batches < 1:
        parser.error("batch-size must be 1..10000 and max-batches must be positive")
    post_run(batch_size=args.batch_size, max_batches=args.max_batches)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

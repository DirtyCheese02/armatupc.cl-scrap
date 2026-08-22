from __future__ import annotations

import argparse
import json
import os
import socket
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from supabase import Client, create_client

BASE_DIR = Path(__file__).resolve().parent
SANTIAGO = ZoneInfo("America/Santiago")


def cycle_date_today() -> str:
    return datetime.now(SANTIAGO).date().isoformat()


def get_client() -> Client:
    load_dotenv(BASE_DIR / ".env")
    load_dotenv(BASE_DIR.parent / ".env")
    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
    return create_client(url, key)


def rpc_payload(client: Client, name: str, params: dict[str, Any]) -> Any:
    response = client.rpc(name, params).execute()
    return response.data


def write_github_outputs(values: dict[str, Any]) -> None:
    path = os.environ.get("GITHUB_OUTPUT", "").strip()
    if not path:
        return
    with open(path, "a", encoding="utf-8") as output:
        for key, value in values.items():
            if isinstance(value, bool):
                rendered = str(value).lower()
            elif value is None:
                rendered = ""
            else:
                rendered = str(value)
            output.write(f"{key}={rendered}\n")


def parse_json_array(raw: str | None) -> list[str]:
    if not raw:
        return []
    value = json.loads(raw)
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("expected a JSON array of strings")
    return value


def watchdog_action(status: dict[str, Any] | None, now: datetime | None = None) -> str:
    if not status:
        return "full"
    state = str(status.get("state") or "")
    if state == "complete":
        return "skip"
    if state == "partial":
        return "retry"
    if state == "failed":
        return "full"
    lease_raw = status.get("lease_expires_at") or status.get("leaseExpiresAt")
    if not lease_raw:
        return "full"
    lease = datetime.fromisoformat(str(lease_raw).replace("Z", "+00:00"))
    current = now or datetime.now(timezone.utc)
    return "wait" if lease > current else "full"


def command_claim(args: argparse.Namespace) -> int:
    expected = parse_json_array(args.expected_scrapers)
    result = rpc_payload(
        get_client(),
        "claim_scrape_cycle",
        {
            "p_cycle_date": args.date,
            "p_origin": args.origin,
            "p_owner": args.owner,
            "p_lease_seconds": args.lease_seconds,
            "p_expected_scrapers": expected,
        },
    )
    result = result or {}
    outputs = {
        "claimed": bool(result.get("claimed")),
        "fencing_token": result.get("fencingToken"),
        "state": result.get("state"),
    }
    write_github_outputs(outputs)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if outputs["claimed"] else 3


def command_heartbeat(args: argparse.Namespace) -> int:
    ok = bool(
        rpc_payload(
            get_client(),
            "heartbeat_scrape_cycle",
            {
                "p_cycle_date": args.date,
                "p_fencing_token": args.token,
                "p_lease_seconds": args.lease_seconds,
                "p_metadata": json.loads(args.metadata or "{}"),
            },
        )
    )
    print(json.dumps({"heartbeat": ok}))
    return 0 if ok else 4


def command_assert(args: argparse.Namespace) -> int:
    ok = bool(
        rpc_payload(
            get_client(),
            "assert_scrape_cycle_fence",
            {"p_cycle_date": args.date, "p_fencing_token": args.token},
        )
    )
    print(json.dumps({"valid": ok}))
    return 0 if ok else 4


def command_finalize(args: argparse.Namespace) -> int:
    ok = bool(
        rpc_payload(
            get_client(),
            "finalize_scrape_cycle",
            {
                "p_cycle_date": args.date,
                "p_fencing_token": args.token,
                "p_state": args.state,
                "p_completed_scrapers": parse_json_array(args.completed),
                "p_partial_scrapers": parse_json_array(args.partial),
                "p_failed_scrapers": parse_json_array(args.failed),
                "p_metadata": json.loads(args.metadata or "{}"),
            },
        )
    )
    print(json.dumps({"finalized": ok, "state": args.state}))
    return 0 if ok else 4


def command_status(args: argparse.Namespace) -> int:
    status = rpc_payload(
        get_client(), "get_scrape_cycle_status", {"p_cycle_date": args.date}
    )
    action = watchdog_action(status)
    failed = [
        *list((status or {}).get("failed_scrapers") or []),
        *list((status or {}).get("partial_scrapers") or []),
    ]
    write_github_outputs(
        {
            "action": action,
            "state": (status or {}).get("state"),
            "failed_scrapers_csv": ",".join(dict.fromkeys(map(str, failed))),
        }
    )
    print(json.dumps({"action": action, "cycle": status}, ensure_ascii=False))
    return 0


def command_finalize_manifest(args: argparse.Namespace) -> int:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    completed = list(manifest.get("successful_scrapers") or [])
    partial = list(manifest.get("partial_scrapers") or [])
    failed = list(manifest.get("hard_failed_scrapers") or [])
    state = "partial" if partial or failed else "complete"
    args.state = state
    args.completed = json.dumps(completed)
    args.partial = json.dumps(partial)
    args.failed = json.dumps(failed)
    args.metadata = json.dumps(
        {
            "scrapeRunId": manifest.get("scrape_run_id"),
            "manifestGeneratedAt": manifest.get("generated_at_utc"),
        }
    )
    return command_finalize(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Coordinate a fenced daily scrape cycle")
    parser.set_defaults(date=cycle_date_today())
    subparsers = parser.add_subparsers(dest="command", required=True)

    claim = subparsers.add_parser("claim")
    claim.add_argument("--date", default=cycle_date_today())
    claim.add_argument("--origin", choices=("home", "github", "retry"), required=True)
    claim.add_argument("--owner", default=socket.gethostname())
    claim.add_argument("--lease-seconds", type=int, default=1800)
    claim.add_argument("--expected-scrapers", default="[]")
    claim.set_defaults(handler=command_claim)

    for name, handler in (("heartbeat", command_heartbeat), ("assert", command_assert)):
        child = subparsers.add_parser(name)
        child.add_argument("--date", default=cycle_date_today())
        child.add_argument("--token", required=True)
        if name == "heartbeat":
            child.add_argument("--lease-seconds", type=int, default=1800)
            child.add_argument("--metadata", default="{}")
        child.set_defaults(handler=handler)

    finalize = subparsers.add_parser("finalize")
    finalize.add_argument("--date", default=cycle_date_today())
    finalize.add_argument("--token", required=True)
    finalize.add_argument("--state", choices=("partial", "complete", "failed"), required=True)
    finalize.add_argument("--completed", default="[]")
    finalize.add_argument("--partial", default="[]")
    finalize.add_argument("--failed", default="[]")
    finalize.add_argument("--metadata", default="{}")
    finalize.set_defaults(handler=command_finalize)

    status = subparsers.add_parser("status")
    status.add_argument("--date", default=cycle_date_today())
    status.set_defaults(handler=command_status)

    finalize_manifest = subparsers.add_parser("finalize-manifest")
    finalize_manifest.add_argument("--date", default=cycle_date_today())
    finalize_manifest.add_argument("--token", required=True)
    finalize_manifest.add_argument("--manifest", required=True)
    finalize_manifest.set_defaults(handler=command_finalize_manifest)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())

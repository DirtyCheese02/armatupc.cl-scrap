from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCRAPDB_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRAPDB_DIR.parent
SCRAPERS_DIR = SCRAPDB_DIR / "PythonsScrap"
DEFAULT_OUTPUTS_ROOT = SCRAPDB_DIR / "Outputs"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _repo_relative(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _resolve_repo_path(value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def _safe_child_path(base: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(base.resolve())
    except ValueError:
        return False
    return candidate.resolve() != base.resolve()


def _find_first_literal(patterns: tuple[str, ...], content: str) -> str | None:
    for pattern in patterns:
        match = re.search(pattern, content, flags=re.MULTILINE)
        if match:
            return match.group(1).strip().replace("\\", "/")
    return None


def infer_output_dir(script_name: str) -> Path | None:
    script_path = SCRAPERS_DIR / script_name
    if not script_path.exists():
        return None

    try:
        content = script_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None

    candidate = _find_first_literal(
        (
            r'output_dir\s*=\s*"([^"]+)"',
            r"output_dir\s*=\s*'([^']+)'",
            r'clean_output_dir\(\s*"([^"]+)"\s*\)',
            r"clean_output_dir\(\s*'([^']+)'\s*\)",
        ),
        content,
    )
    if not candidate:
        return None

    path = Path(candidate)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def _load_summary(summary_path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[retry-manifest] Skipping invalid summary {summary_path}: {exc}")
        return None


def _item_started_at(summary: dict[str, Any], item: dict[str, Any]) -> str:
    return (
        str(item.get("started_at_utc") or "")
        or str(summary.get("run_started_at_utc") or "")
        or str(summary.get("run_id") or "")
    )


def collect_scraper_results(log_roots: list[Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summaries: list[dict[str, Any]] = []
    latest_by_name: dict[str, dict[str, Any]] = {}
    interrupted_by_name: dict[str, dict[str, Any]] = {}

    for root in log_roots:
        if not root.exists():
            print(f"[retry-manifest] Logs root not found: {root}")
            continue

        for summary_path in sorted(root.rglob("summary.json")):
            summary = _load_summary(summary_path)
            if not summary:
                continue

            summaries.append(
                {
                    "path": _repo_relative(summary_path),
                    "run_id": summary.get("run_id"),
                    "scrape_run_id": summary.get("scrape_run_id"),
                    "parent_scrape_run_id": summary.get("parent_scrape_run_id"),
                    "source": summary.get("source"),
                    "run_started_at_utc": summary.get("run_started_at_utc"),
                    "final_exit_code": summary.get("final_exit_code"),
                    "scraper_failures": summary.get("scraper_failures"),
                }
            )

            completed_names = {
                str(item.get("name") or "").lower()
                for item in summary.get("scraper_results") or []
                if item.get("name")
            }
            for expected_name in summary.get("expected_scrapers") or []:
                normalized_expected = str(expected_name).lower()
                if not normalized_expected or normalized_expected in completed_names:
                    continue
                interrupted_by_name[normalized_expected] = {
                    "name": str(expected_name),
                    "success": False,
                    "return_code": -9,
                    "timed_out": True,
                    "failure_reason": "runner_interrupted_before_summary",
                    "partial": True,
                    "output_complete": False,
                    "started_at_utc": summary.get("run_started_at_utc"),
                    "finished_at_utc": None,
                    "duration_seconds": None,
                    "_summary_path": _repo_relative(summary_path),
                    "_summary_run_id": summary.get("run_id"),
                    "_scrape_run_id": summary.get("scrape_run_id"),
                    "_parent_scrape_run_id": summary.get("parent_scrape_run_id"),
                    "_sort_started_at": str(summary.get("run_started_at_utc") or ""),
                }

            for item in summary.get("scraper_results") or []:
                name = item.get("name")
                if not name:
                    continue

                normalized_name = str(name).lower()
                enriched = dict(item)
                enriched["_summary_path"] = _repo_relative(summary_path)
                enriched["_summary_run_id"] = summary.get("run_id")
                enriched["_scrape_run_id"] = summary.get("scrape_run_id")
                enriched["_parent_scrape_run_id"] = summary.get("parent_scrape_run_id")
                enriched["_sort_started_at"] = _item_started_at(summary, item)

                previous = latest_by_name.get(normalized_name)
                if previous is None or enriched["_sort_started_at"] >= previous["_sort_started_at"]:
                    latest_by_name[normalized_name] = enriched

    for normalized_name, interrupted in interrupted_by_name.items():
        if normalized_name not in latest_by_name:
            latest_by_name[normalized_name] = interrupted

    return summaries, [latest_by_name[key] for key in sorted(latest_by_name)]


def _manifest_entry(item: dict[str, Any]) -> dict[str, Any]:
    name = str(item.get("name"))
    output_dir = infer_output_dir(name)
    return {
        "name": name,
        "output_dir": _repo_relative(output_dir),
        "json_count": item.get("json_count"),
        "success": bool(item.get("success")),
        "return_code": item.get("return_code"),
        "timed_out": bool(item.get("timed_out")),
        "failure_reason": item.get("failure_reason"),
        "partial": bool(item.get("partial")),
        "health_status": item.get("health_status"),
        "expected_categories": item.get("expected_categories") or [],
        "completed_categories": item.get("completed_categories") or [],
        "failed_categories": item.get("failed_categories") or [],
        "blocked_reason": item.get("blocked_reason"),
        "output_complete": item.get("output_complete"),
        "started_at_utc": item.get("started_at_utc"),
        "finished_at_utc": item.get("finished_at_utc"),
        "duration_seconds": item.get("duration_seconds"),
        "headless": item.get("headless"),
        "used_headful_retry": item.get("used_headful_retry"),
        "headful_retry_attempted": item.get("headful_retry_attempted"),
        "headful_retry_success": item.get("headful_retry_success"),
        "headful_retry_return_code": item.get("headful_retry_return_code"),
        "headful_retry_json_count": item.get("headful_retry_json_count"),
        "log_file": item.get("log_file"),
        "summary_path": item.get("_summary_path"),
        "summary_run_id": item.get("_summary_run_id"),
        "scrape_run_id": item.get("_scrape_run_id"),
        "parent_scrape_run_id": item.get("_parent_scrape_run_id"),
    }


def _coalesced_run_id(summaries: list[dict[str, Any]], key: str, *, fallback_seed: str) -> tuple[str | None, list[str]]:
    values = sorted({str(item.get(key)) for item in summaries if item.get(key)})
    if not values:
        if key == "parent_scrape_run_id":
            return None, []
        return str(uuid.uuid5(uuid.NAMESPACE_URL, fallback_seed)), []
    if len(values) == 1:
        return values[0], values
    aggregate = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{fallback_seed}:{':'.join(values)}"))
    return aggregate, values


def _delete_output_dirs(entries: list[dict[str, Any]], outputs_root: Path) -> list[dict[str, Any]]:
    pruned: list[dict[str, Any]] = []
    outputs_root = outputs_root.resolve()

    for entry in entries:
        output_dir_raw = entry.get("output_dir")
        if not output_dir_raw:
            pruned.append({"name": entry["name"], "output_dir": None, "status": "unknown_output_dir"})
            continue

        output_dir = _resolve_repo_path(output_dir_raw)
        if not _safe_child_path(outputs_root, output_dir):
            pruned.append(
                {
                    "name": entry["name"],
                    "output_dir": output_dir_raw,
                    "status": "skipped_unsafe_path",
                }
            )
            continue

        if not output_dir.exists():
            pruned.append({"name": entry["name"], "output_dir": output_dir_raw, "status": "not_found"})
            continue

        shutil.rmtree(output_dir)
        pruned.append({"name": entry["name"], "output_dir": output_dir_raw, "status": "deleted"})
        print(f"[retry-manifest] Deleted failed output dir for {entry['name']}: {output_dir_raw}")

    return pruned


def _count_json_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for _ in path.rglob("*.json"))


def _csv(values: list[str]) -> str:
    return ",".join(values)


def write_github_outputs(manifest: dict[str, Any]) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return

    winpy_failed_categories: list[str] = []
    for entry in [*(manifest.get("failed") or []), *(manifest.get("partial") or [])]:
        if str(entry.get("name") or "").casefold() != "scrap_winpy.py":
            continue
        winpy_failed_categories = sorted(
            {str(category) for category in entry.get("failed_categories") or [] if category}
        )
        break

    outputs = {
        "failed_scrapers_csv": _csv(manifest["failed_scrapers"]),
        "successful_scrapers_csv": _csv(manifest["successful_scrapers"]),
        "failed_output_dirs_csv": _csv(manifest["failed_output_dirs"]),
        "successful_output_dirs_csv": _csv(manifest["successful_output_dirs"]),
        "failed_count": str(len(manifest["failed_scrapers"])),
        "successful_count": str(len(manifest["successful_scrapers"])),
        "remaining_json_count": str(manifest["remaining_json_count"]),
        "winpy_failed_categories_csv": _csv(winpy_failed_categories),
        "scrape_run_id": str(manifest.get("scrape_run_id") or ""),
        "parent_scrape_run_id": str(manifest.get("parent_scrape_run_id") or ""),
        "has_failures": "true" if manifest["failed_scrapers"] else "false",
        "has_remaining_json": "true" if manifest["remaining_json_count"] > 0 else "false",
    }

    with Path(output_path).open("a", encoding="utf-8") as handle:
        for key, value in outputs.items():
            handle.write(f"{key}={value}\n")


def build_manifest(
    *,
    log_roots: list[Path],
    outputs_root: Path,
    prune_failed: bool,
) -> dict[str, Any]:
    summaries, results = collect_scraper_results(log_roots)
    if not summaries:
        raise RuntimeError(f"No summary.json files found under: {', '.join(str(root) for root in log_roots)}")

    entries = [_manifest_entry(item) for item in results]
    failed = [entry for entry in entries if not entry["success"]]
    partial = [entry for entry in entries if entry["success"] and entry["partial"]]
    successful = [entry for entry in entries if entry["success"] and not entry["partial"]]
    retryable = [*failed, *partial]

    summary_seed = "armatupc:manifest:" + ":".join(
        sorted(str(item.get("path") or item.get("run_id") or "") for item in summaries)
    )
    scrape_run_id, source_scrape_run_ids = _coalesced_run_id(
        summaries,
        "scrape_run_id",
        fallback_seed=summary_seed,
    )
    parent_scrape_run_id, source_parent_run_ids = _coalesced_run_id(
        summaries,
        "parent_scrape_run_id",
        fallback_seed=f"{summary_seed}:parent",
    )

    pruned_output_dirs = _delete_output_dirs(failed, outputs_root) if prune_failed else []

    manifest = {
        "generated_at_utc": _utc_now(),
        "scrape_run_id": scrape_run_id,
        "parent_scrape_run_id": parent_scrape_run_id,
        "source_scrape_run_ids": source_scrape_run_ids,
        "source_parent_scrape_run_ids": source_parent_run_ids,
        "source": next((item.get("source") for item in summaries if item.get("source")), "scraper"),
        "log_roots": [_repo_relative(root) for root in log_roots],
        "outputs_root": _repo_relative(outputs_root),
        "summary_files": summaries,
        "failed_scrapers": sorted(entry["name"] for entry in retryable),
        "hard_failed_scrapers": sorted(entry["name"] for entry in failed),
        "partial_scrapers": sorted(entry["name"] for entry in partial),
        "successful_scrapers": sorted(entry["name"] for entry in successful),
        "failed_output_dirs": sorted(entry["output_dir"] for entry in failed if entry.get("output_dir")),
        "successful_output_dirs": sorted(entry["output_dir"] for entry in successful if entry.get("output_dir")),
        "failed": sorted(failed, key=lambda item: item["name"].lower()),
        "partial": sorted(partial, key=lambda item: item["name"].lower()),
        "successful": sorted(successful, key=lambda item: item["name"].lower()),
        "scraper_results": sorted(entries, key=lambda item: item["name"].lower()),
        "pruned_output_dirs": pruned_output_dirs,
        "remaining_json_count": _count_json_files(outputs_root),
    }
    return manifest


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[retry-manifest] Wrote manifest: {path}")
    print(f"[retry-manifest] Failed scrapers: {_csv(manifest['failed_scrapers']) or '(none)'}")
    print(f"[retry-manifest] Remaining JSON count: {manifest['remaining_json_count']}")


def command_build(args: argparse.Namespace) -> int:
    log_roots = [_resolve_repo_path(value) for value in args.logs_root]
    outputs_root = _resolve_repo_path(args.outputs_root)
    manifest = build_manifest(
        log_roots=log_roots,
        outputs_root=outputs_root,
        prune_failed=args.prune_failed,
    )
    _write_manifest(_resolve_repo_path(args.manifest), manifest)
    write_github_outputs(manifest)
    return 0


def command_emit_outputs(args: argparse.Namespace) -> int:
    manifest_path = _resolve_repo_path(args.manifest)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    write_github_outputs(manifest)
    print(f"[retry-manifest] Emitted GitHub outputs from {manifest_path}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ScrapDB retry manifests from scraper summaries.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build", help="Build a retry manifest from summary.json files.")
    build.add_argument(
        "--logs-root",
        action="append",
        default=[],
        help="Root containing RunLogs artifacts. Can be passed multiple times.",
    )
    build.add_argument("--outputs-root", default="ScrapDB/Outputs")
    build.add_argument("--manifest", default="ScrapDB/RunLogs/retry_manifest.json")
    build.add_argument("--prune-failed", action="store_true")
    build.set_defaults(func=command_build)

    emit = subparsers.add_parser("emit-outputs", help="Emit GitHub step outputs from an existing manifest.")
    emit.add_argument("--manifest", default="ScrapDB/RunLogs/retry_manifest.json")
    emit.set_defaults(func=command_emit_outputs)

    args = parser.parse_args()
    if args.command == "build" and not args.logs_root:
        args.logs_root = ["ScrapDB/RunLogs"]
    return args


def main() -> int:
    args = parse_args()
    try:
        return args.func(args)
    except Exception as exc:
        print(f"[retry-manifest] ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

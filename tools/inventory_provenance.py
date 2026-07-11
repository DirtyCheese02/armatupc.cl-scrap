"""Build a compact provenance and permissions inventory for local datasets.

The command never copies product payloads or images.  It emits one aggregate
row per merchant scraper plus one row for the historical PCPartPicker dataset,
making undocumented sources explicit before monetization or publication.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


ELIGIBLE_PERMISSION_STATUSES = frozenset(
    {
        "first_party",
        "written_permission",
        "merchant_feed_agreement",
        "licensed",
        "public_domain",
    }
)


@dataclass(frozen=True)
class ProvenanceRecord:
    sourceId: str
    sourceName: str
    sourceType: str
    datasetKind: str
    sourceRoot: str
    scraperScript: str | None
    fileCount: int
    totalBytes: int
    firstModifiedAt: str | None
    lastModifiedAt: str | None
    permissionStatus: str
    permissionReference: str | None
    permissionRecordedAt: str | None
    monetizationEligible: bool
    capturePolicy: str
    notes: str


def _source_id(prefix: str, name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.casefold()).strip("-")
    return f"{prefix}:{slug}"


def _utc_iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def _dataset_stats(root: Path) -> tuple[int, int, str | None, str | None]:
    if not root.exists():
        return 0, 0, None, None

    count = 0
    total_bytes = 0
    first_modified: float | None = None
    last_modified: float | None = None
    for path in root.rglob("*.json"):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        count += 1
        total_bytes += stat.st_size
        first_modified = (
            stat.st_mtime if first_modified is None else min(first_modified, stat.st_mtime)
        )
        last_modified = (
            stat.st_mtime if last_modified is None else max(last_modified, stat.st_mtime)
        )

    return (
        count,
        total_bytes,
        _utc_iso(first_modified) if first_modified is not None else None,
        _utc_iso(last_modified) if last_modified is not None else None,
    )


def _infer_output_root(repository_root: Path, scraper_path: Path) -> Path:
    content = scraper_path.read_text(encoding="utf-8", errors="ignore")
    patterns = (
        r"output_dir\s*=\s*[\"']([^\"']+)[\"']",
        r"clean_output_dir\(\s*[\"']([^\"']+)[\"']\s*\)",
    )
    for pattern in patterns:
        match = re.search(pattern, content)
        if match:
            candidate = Path(match.group(1).replace("\\", "/"))
            return candidate if candidate.is_absolute() else repository_root / candidate
    return repository_root / "ScrapDB" / "Outputs" / scraper_path.stem.removeprefix("Scrap_")


def _load_permissions(path: Path | None) -> dict[str, Mapping[str, Any]]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("permission registry must be a JSON object keyed by sourceId")
    result: dict[str, Mapping[str, Any]] = {}
    for source_id, value in payload.items():
        if not isinstance(value, dict):
            raise ValueError(f"permission registry entry {source_id!r} must be an object")
        result[str(source_id)] = value
    return result


def _permission_values(
    source_id: str, registry: Mapping[str, Mapping[str, Any]]
) -> tuple[str, str | None, str | None, bool]:
    entry = registry.get(source_id, {})
    status = str(entry.get("permissionStatus") or "not_documented")
    reference = entry.get("permissionReference")
    recorded_at = entry.get("permissionRecordedAt")
    eligible = (
        bool(entry.get("monetizationEligible", False))
        and status in ELIGIBLE_PERMISSION_STATUSES
        and bool(reference)
        and bool(recorded_at)
    )
    return (
        status,
        str(reference) if reference else None,
        str(recorded_at) if recorded_at else None,
        eligible,
    )


def build_inventory(
    repository_root: str | Path,
    *,
    permission_registry: str | Path | None = None,
) -> list[ProvenanceRecord]:
    root = Path(repository_root).resolve()
    registry_path = Path(permission_registry) if permission_registry is not None else None
    registry = _load_permissions(registry_path)
    records: list[ProvenanceRecord] = []

    pcpp_root = root / "SpecDB" / "ScrapedDataPCPP"
    pcpp_source_id = "catalog:pcpartpicker"
    file_count, total_bytes, first_modified, last_modified = _dataset_stats(pcpp_root)
    permission_status, permission_reference, permission_recorded_at, eligible = _permission_values(
        pcpp_source_id, registry
    )
    records.append(
        ProvenanceRecord(
            sourceId=pcpp_source_id,
            sourceName="PCPartPicker",
            sourceType="third_party_catalog",
            datasetKind="specifications_and_image_references",
            sourceRoot=str(pcpp_root.relative_to(root)),
            scraperScript="SpecDB/Scrap_PCPP.py",
            fileCount=file_count,
            totalBytes=total_bytes,
            firstModifiedAt=first_modified,
            lastModifiedAt=last_modified,
            permissionStatus=permission_status,
            permissionReference=permission_reference,
            permissionRecordedAt=permission_recorded_at,
            monetizationEligible=eligible,
            capturePolicy="suspended_pending_written_permission",
            notes="Historical files are migration/read-only input; do not expand or monetize without documented permission.",
        )
    )

    scrapers_root = root / "ScrapDB" / "PythonsScrap"
    for scraper_path in sorted(scrapers_root.glob("Scrap_*.py"), key=lambda path: path.name.casefold()):
        output_root = _infer_output_root(root, scraper_path)
        store_name = output_root.name
        source_id = _source_id("merchant", store_name)
        file_count, total_bytes, first_modified, last_modified = _dataset_stats(output_root)
        permission_status, permission_reference, permission_recorded_at, eligible = _permission_values(
            source_id, registry
        )
        records.append(
            ProvenanceRecord(
                sourceId=source_id,
                sourceName=store_name,
                sourceType="merchant_website",
                datasetKind="offers_and_remote_image_references",
                sourceRoot=str(output_root.relative_to(root)),
                scraperScript=str(scraper_path.relative_to(root)),
                fileCount=file_count,
                totalBytes=total_bytes,
                firstModifiedAt=first_modified,
                lastModifiedAt=last_modified,
                permissionStatus=permission_status,
                permissionReference=permission_reference,
                permissionRecordedAt=permission_recorded_at,
                monetizationEligible=eligible,
                capturePolicy="active_offer_observation",
                notes="Price facts and image-use permission must be assessed separately with the merchant.",
            )
        )

    return records


def _write_json(records: Sequence[ProvenanceRecord], output) -> None:
    json.dump([asdict(record) for record in records], output, ensure_ascii=False, indent=2)
    output.write("\n")


def _write_csv(records: Sequence[ProvenanceRecord], output) -> None:
    field_names = [field.name for field in fields(ProvenanceRecord)]
    writer = csv.DictWriter(output, fieldnames=field_names, lineterminator="\n")
    writer.writeheader()
    writer.writerows(asdict(record) for record in records)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inventory dataset and image provenance by source.")
    parser.add_argument("--root", default=".", help="Repository root")
    parser.add_argument("--format", choices=("json", "csv"), default="json")
    parser.add_argument("--output", default="-", help="Output path, or - for stdout")
    parser.add_argument(
        "--permissions",
        default=None,
        help="Optional JSON registry keyed by sourceId with documented permissions",
    )
    args = parser.parse_args(argv)

    records = build_inventory(args.root, permission_registry=args.permissions)
    if args.output == "-":
        output = sys.stdout
        close_output = False
    else:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output = output_path.open("w", encoding="utf-8", newline="")
        close_output = True

    try:
        if args.format == "csv":
            _write_csv(records, output)
        else:
            _write_json(records, output)
    finally:
        if close_output:
            output.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

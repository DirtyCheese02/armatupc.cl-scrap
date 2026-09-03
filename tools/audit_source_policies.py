from __future__ import annotations

import json
import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRAPDB_DIR = REPO_ROOT / "ScrapDB"
sys.path.insert(0, str(SCRAPDB_DIR))

from source_policy import (  # noqa: E402
    check_robots_access,
    load_source_registry,
    public_audit_row,
    validate_registry_coverage,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-robots", action="store_true")
    args = parser.parse_args()
    registry = load_source_registry()
    scrapers = sorted(
        path.name for path in (SCRAPDB_DIR / "PythonsScrap").glob("Scrap_*.py")
    )
    issues = validate_registry_coverage(scrapers, registry)
    rows = [public_audit_row(registry[name]) for name in scrapers if name in registry]
    if args.check_robots:
        for row in rows:
            allowed, reason = check_robots_access(registry[row["script"]])
            row["robotsCheck"] = {"allowed": allowed, "reason": reason}
    report = {
        "registryValid": not issues,
        "coverageIssues": issues,
        "sourceCount": len(rows),
        "automationAllowedCount": sum(row["automationAllowed"] for row in rows),
        "reviewRequiredCount": sum(
            row["automationStatus"] == "review_required" for row in rows
        ),
        "undocumentedImagePermissionCount": sum(
            row["imagePermission"] == "not_documented" for row in rows
        ),
        "sources": rows,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    robots_failed = args.check_robots and any(
        not row.get("robotsCheck", {}).get("allowed", False) for row in rows
    )
    return 0 if not issues and not robots_failed else 1


if __name__ == "__main__":
    raise SystemExit(main())

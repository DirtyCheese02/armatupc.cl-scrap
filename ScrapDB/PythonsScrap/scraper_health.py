from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


VALID_STATUSES = {"success", "partial_success", "failed"}


def write_scraper_health(
    *,
    status: str,
    expected_categories: Iterable[str],
    completed_categories: Iterable[str],
    failed_categories: Iterable[str] = (),
    product_count: int = 0,
    errors: Iterable[dict[str, Any]] = (),
    blocked_reason: str | None = None,
) -> Path | None:
    """Write the optional machine-readable health sidecar consumed by the runner."""

    if status not in VALID_STATUSES:
        raise ValueError(f"invalid scraper health status: {status}")
    destination_raw = os.environ.get("SCRAPER_HEALTH_FILE", "").strip()
    if not destination_raw:
        return None

    destination = Path(destination_raw)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "status": status,
        "expected_categories": sorted(set(expected_categories)),
        "completed_categories": sorted(set(completed_categories)),
        "failed_categories": sorted(set(failed_categories)),
        "product_count": max(int(product_count), 0),
        "errors": list(errors),
        "blocked_reason": blocked_reason,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(destination)
    return destination

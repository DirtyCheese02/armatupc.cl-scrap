"""Fail-closed source policy helpers for merchant scrapers.

The registry documents what may be collected from every merchant.  A pending
entry is deliberately not treated as permission.  Operators can run in
``audit`` mode while completing the registry; ``enforce`` mode skips every
source that has not been explicitly approved.
"""

from __future__ import annotations

import json
import urllib.robotparser
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


REGISTRY_PATH = Path(__file__).resolve().parent / "source_registry.json"
ALLOWED_AUTOMATION_STATUSES = frozenset({"allowed", "feed", "api"})
VALID_AUTOMATION_STATUSES = ALLOWED_AUTOMATION_STATUSES | frozenset(
    {"review_required", "denied", "suspended"}
)


@dataclass(frozen=True)
class SourcePolicy:
    script: str
    source_name: str
    base_url: str
    robots_url: str
    terms_url: str | None
    automation_status: str
    allowed_fields: tuple[str, ...]
    image_permission: str
    text_permission: str
    request_delay_seconds: float
    permission_reference: str | None
    reviewed_at: str | None

    @property
    def automation_allowed(self) -> bool:
        return self.automation_status in ALLOWED_AUTOMATION_STATUSES


def load_source_registry(path: str | Path = REGISTRY_PATH) -> dict[str, SourcePolicy]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("source registry must be a JSON array")
    result: dict[str, SourcePolicy] = {}
    for raw in payload:
        if not isinstance(raw, dict):
            raise ValueError("source registry rows must be objects")
        script = str(raw.get("script") or "").strip()
        if not script or script in result:
            raise ValueError(f"invalid or duplicate source registry script: {script!r}")
        status = str(raw.get("automationStatus") or "").strip()
        if status not in VALID_AUTOMATION_STATUSES:
            raise ValueError(f"invalid automation status for {script}: {status!r}")
        delay = float(raw.get("requestDelaySeconds") or 0)
        if delay < 0.25:
            raise ValueError(f"request delay for {script} must be at least 0.25 seconds")
        base_url = str(raw.get("baseUrl") or "").strip()
        robots_url = str(raw.get("robotsUrl") or "").strip()
        if not base_url.startswith("https://") or not robots_url.startswith("https://"):
            raise ValueError(f"https baseUrl and robotsUrl are required for {script}")
        result[script] = SourcePolicy(
            script=script,
            source_name=str(raw.get("sourceName") or "").strip(),
            base_url=base_url,
            robots_url=robots_url,
            terms_url=str(raw["termsUrl"]).strip() if raw.get("termsUrl") else None,
            automation_status=status,
            allowed_fields=tuple(str(value) for value in raw.get("allowedFields") or ()),
            image_permission=str(raw.get("imagePermission") or "not_documented"),
            text_permission=str(raw.get("textPermission") or "not_documented"),
            request_delay_seconds=delay,
            permission_reference=(
                str(raw["permissionReference"]).strip()
                if raw.get("permissionReference")
                else None
            ),
            reviewed_at=str(raw["reviewedAt"]).strip() if raw.get("reviewedAt") else None,
        )
    return result


def validate_registry_coverage(
    scraper_names: Iterable[str], registry: dict[str, SourcePolicy]
) -> list[str]:
    expected = set(scraper_names)
    actual = set(registry)
    issues = [f"missing:{name}" for name in sorted(expected - actual)]
    issues.extend(f"orphan:{name}" for name in sorted(actual - expected))
    return issues


def policy_decision(policy: SourcePolicy | None, mode: str) -> tuple[bool, str]:
    normalized_mode = mode.strip().lower() or "audit"
    if policy is None:
        return False, "source_not_registered"
    if policy.automation_status in {"denied", "suspended"}:
        return False, f"source_{policy.automation_status}"
    if normalized_mode == "enforce" and not policy.automation_allowed:
        return False, "source_approval_required"
    return True, "source_approved" if policy.automation_allowed else "source_audit_pending"


def public_audit_row(policy: SourcePolicy) -> dict[str, Any]:
    return {
        "script": policy.script,
        "sourceName": policy.source_name,
        "baseUrl": policy.base_url,
        "robotsUrl": policy.robots_url,
        "termsUrl": policy.terms_url,
        "automationStatus": policy.automation_status,
        "automationAllowed": policy.automation_allowed,
        "allowedFields": list(policy.allowed_fields),
        "imagePermission": policy.image_permission,
        "textPermission": policy.text_permission,
        "requestDelaySeconds": policy.request_delay_seconds,
        "permissionReference": policy.permission_reference,
        "reviewedAt": policy.reviewed_at,
    }


def check_robots_access(
    policy: SourcePolicy,
    user_agent: str = "ArmaTuPCDataBot/1.0",
) -> tuple[bool, str]:
    """Check the registered entry URL against the source robots policy.

    This is a technical crawl check, not proof of contractual permission.
    Network errors fail closed so enforce mode cannot silently proceed.
    """
    parser = urllib.robotparser.RobotFileParser(policy.robots_url)
    try:
        request = urllib.request.Request(
            policy.robots_url,
            headers={"User-Agent": user_agent, "Accept": "text/plain"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read(256_000).decode("utf-8", errors="replace")
        parser.parse(body.splitlines())
    except Exception as exc:  # pragma: no cover - depends on merchant network
        return False, f"robots_unavailable:{type(exc).__name__}"
    return (
        (True, "robots_allowed")
        if parser.can_fetch(user_agent, policy.base_url)
        else (False, "robots_disallowed")
    )

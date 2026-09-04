from pathlib import Path

from ScrapDB.source_policy import (
    load_source_registry,
    policy_decision,
    validate_registry_coverage,
)


def test_every_merchant_scraper_has_exactly_one_policy():
    repo_root = Path(__file__).resolve().parents[1]
    names = [
        path.name
        for path in (repo_root / "ScrapDB" / "PythonsScrap").glob("Scrap_*.py")
    ]
    registry = load_source_registry()
    assert len(names) == 24
    assert validate_registry_coverage(names, registry) == []


def test_pending_sources_are_visible_in_audit_and_blocked_in_enforce():
    policy = next(iter(load_source_registry().values()))
    assert policy.automation_status == "review_required"
    assert policy_decision(policy, "audit") == (True, "source_audit_pending")
    assert policy_decision(policy, "enforce") == (
        False,
        "source_approval_required",
    )


def test_unregistered_source_is_always_blocked():
    assert policy_decision(None, "audit") == (False, "source_not_registered")


def test_spdigital_is_suspended_until_an_authorized_integration_exists():
    policy = load_source_registry()["Scrap_SPDigital.py"]
    assert policy.automation_status == "suspended"
    assert policy_decision(policy, "audit") == (False, "source_suspended")
    assert policy_decision(policy, "enforce") == (False, "source_suspended")

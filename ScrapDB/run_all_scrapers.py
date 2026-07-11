from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRAPDB_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRAPDB_DIR.parent
SCRAPERS_DIR = SCRAPDB_DIR / "PythonsScrap"
MATCH_SCRIPT = SCRAPDB_DIR / "match_products.py"
RUN_LOGS_DIR = SCRAPDB_DIR / "RunLogs"


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_scrape_run_id() -> str:
    explicit = os.environ.get("SCRAPE_RUN_ID", "").strip()
    if explicit:
        return explicit

    github_run_id = os.environ.get("GITHUB_RUN_ID", "").strip()
    if github_run_id:
        identity = ":".join(
            (
                os.environ.get("GITHUB_REPOSITORY", "local"),
                os.environ.get("GITHUB_WORKFLOW", "scrape"),
                github_run_id,
                os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
            )
        )
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"armatupc:{identity}"))

    return str(uuid.uuid4())


def _parse_timeout_minutes(env_name: str, default_value: int) -> int:
    raw = os.environ.get(env_name)
    if not raw:
        return default_value

    try:
        value = int(raw)
    except ValueError:
        print(f"[WARN] {env_name}={raw!r} is not an integer. Using {default_value}.")
        return default_value

    if value <= 0:
        print(f"[WARN] {env_name}={raw!r} must be > 0. Using {default_value}.")
        return default_value

    return value


def _parse_bool(raw: str | None, default_value: bool) -> bool:
    if raw is None:
        return default_value
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _parse_csv_env(env_name: str) -> set[str]:
    raw = os.environ.get(env_name, "")
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


def _parse_timeout_overrides(env_name: str) -> dict[str, int]:
    raw = os.environ.get(env_name, "")
    overrides: dict[str, int] = {}
    for item in raw.split(","):
        if not item.strip():
            continue
        if "=" not in item:
            print(f"[WARN] Ignoring invalid {env_name} item {item!r}. Expected name=minutes.")
            continue
        name, value = item.split("=", 1)
        name = name.strip().lower()
        if not name:
            continue
        try:
            minutes = int(value.strip())
        except ValueError:
            print(f"[WARN] Ignoring invalid timeout override {item!r}.")
            continue
        if minutes <= 0:
            print(f"[WARN] Ignoring non-positive timeout override {item!r}.")
            continue
        overrides[name] = minutes
    return overrides


def _discover_scrapers() -> list[Path]:
    excluded_raw = os.environ.get("SCRAPER_EXCLUDE", "")
    excluded = {name.strip().lower() for name in excluded_raw.split(",") if name.strip()}
    included = _parse_csv_env("SCRAPER_INCLUDE")
    scraper_name_pattern = re.compile(r"^scrap_.*\.py$", re.IGNORECASE)

    scrapers = []
    for script in SCRAPERS_DIR.iterdir():
        if not script.is_file():
            continue
        if not scraper_name_pattern.match(script.name):
            continue
        if script.name.startswith("__"):
            continue
        script_name_l = script.name.lower()
        if included and script_name_l not in included:
            continue
        if script_name_l in excluded:
            continue
        scrapers.append(script)

    return sorted(scrapers, key=lambda item: item.name.lower())


def _infer_output_dir(script_path: Path) -> Path | None:
    try:
        content = script_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None

    match = re.search(r'output_dir\s*=\s*"([^"]+)"', content)
    if not match:
        return None

    candidate = match.group(1).strip().replace("\\", "/")
    if not candidate:
        return None

    path_candidate = Path(candidate)
    if path_candidate.is_absolute():
        return path_candidate
    return REPO_ROOT / path_candidate


def _count_json_files(path: Path | None) -> int | None:
    if path is None:
        return None
    if not path.exists():
        return 0
    return len(list(path.glob("*.json")))


def _build_command(script_path: Path, use_xvfb: bool) -> list[str]:
    base_command = [sys.executable, str(script_path)]
    if not use_xvfb:
        return base_command

    xvfb_path = shutil.which("xvfb-run")
    if not xvfb_path:
        print("[WARN] SCRAP_USE_XVFB is enabled but xvfb-run is not available. Running without xvfb.")
        return base_command

    return [xvfb_path, "-a", *base_command]


def _run_python_script(
    script_path: Path,
    log_path: Path,
    timeout_minutes: int,
    extra_env: dict[str, str] | None = None,
    use_xvfb: bool = False,
) -> dict[str, Any]:
    started_at = _utc_iso_now()
    command = _build_command(script_path, use_xvfb)

    result: dict[str, Any] = {
        "name": script_path.name,
        "path": str(script_path),
        "command": command,
        "started_at_utc": started_at,
        "finished_at_utc": None,
        "duration_seconds": None,
        "return_code": None,
        "timed_out": False,
        "success": False,
        "log_file": str(log_path),
    }

    run_started = datetime.now(timezone.utc)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write(f"# Command: {' '.join(command)}\n")
        log_file.write(f"# StartedAtUTC: {started_at}\n")
        log_file.write(f"# TimeoutMinutes: {timeout_minutes}\n\n")
        log_file.flush()

        if not script_path.exists():
            log_file.write("Script not found.\n")
            result["return_code"] = -1
        else:
            env = os.environ.copy()
            env.setdefault("PYTHONUNBUFFERED", "1")
            env.setdefault("PYTHONIOENCODING", "utf-8")
            if extra_env:
                env.update(extra_env)

            try:
                completed = subprocess.run(
                    command,
                    cwd=REPO_ROOT,
                    env=env,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=timeout_minutes * 60,
                    check=False,
                )
                result["return_code"] = completed.returncode
            except subprocess.TimeoutExpired:
                result["timed_out"] = True
                result["return_code"] = -9
                log_file.write(f"\nProcess timed out after {timeout_minutes} minutes.\n")

    run_finished = datetime.now(timezone.utc)
    duration = (run_finished - run_started).total_seconds()

    result["finished_at_utc"] = run_finished.isoformat()
    result["duration_seconds"] = round(duration, 2)
    result["success"] = (result["return_code"] == 0) and (not result["timed_out"])

    return result


def main() -> int:
    run_started = datetime.now(timezone.utc)
    run_id = run_started.strftime("%Y%m%d_%H%M%S")
    shard_name = re.sub(r"[^a-zA-Z0-9_-]+", "-", os.environ.get("SCRAPE_SHARD_NAME", "").strip()).strip("-")
    if shard_name:
        run_id = f"{run_id}_{shard_name}"
    scrape_run_id = _resolve_scrape_run_id()
    parent_scrape_run_id = os.environ.get("PARENT_SCRAPE_RUN_ID", "").strip() or None
    scrape_source = os.environ.get("SCRAPE_SOURCE", "scraper").strip() or "scraper"
    run_dir = RUN_LOGS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    scraper_timeout_minutes = _parse_timeout_minutes("SCRAPER_TIMEOUT_MINUTES", 90)
    match_timeout_minutes = _parse_timeout_minutes("MATCH_TIMEOUT_MINUTES", 60)
    scraper_timeout_overrides = _parse_timeout_overrides("SCRAPER_TIMEOUT_OVERRIDES")
    run_match_products = _parse_bool(os.environ.get("RUN_MATCH_PRODUCTS"), True)
    default_headless = _parse_bool(os.environ.get("SCRAP_HEADLESS"), True)
    use_xvfb = _parse_bool(os.environ.get("SCRAP_USE_XVFB"), True)
    retry_on_empty = _parse_bool(os.environ.get("SCRAPER_RETRY_ON_EMPTY"), True)
    retry_headful_on_fail = _parse_bool(os.environ.get("SCRAPER_RETRY_HEADFUL_ON_FAIL"), True)
    headful_scrapers = _parse_csv_env("SCRAPER_HEADFUL")
    headless_scrapers = _parse_csv_env("SCRAPER_HEADLESS")
    no_headful_retry_scrapers = _parse_csv_env("SCRAPER_NO_HEADFUL_RETRY")
    required_non_empty_scrapers = _parse_csv_env("SCRAPER_REQUIRE_NON_EMPTY")

    scrapers = _discover_scrapers()
    print(f"Discovered {len(scrapers)} scraper(s) in {SCRAPERS_DIR}.")

    scraper_results: list[dict[str, Any]] = []
    for index, scraper_path in enumerate(scrapers, start=1):
        script_name = scraper_path.name
        script_headless = default_headless
        script_name_l = script_name.lower()
        if script_name_l in headful_scrapers:
            script_headless = False
        if script_name_l in headless_scrapers:
            script_headless = True

        script_timeout_minutes = scraper_timeout_overrides.get(script_name_l, scraper_timeout_minutes)
        output_dir = _infer_output_dir(scraper_path)
        print(
            f"[{index}/{len(scrapers)}] Running {script_name} "
            f"(headless={'1' if script_headless else '0'}, timeout={script_timeout_minutes}m)..."
        )
        result = _run_python_script(
            script_path=scraper_path,
            log_path=run_dir / f"{scraper_path.stem}.log",
            timeout_minutes=script_timeout_minutes,
            extra_env={
                "SCRAP_HEADLESS": "1" if script_headless else "0",
                "SCRAPE_RUN_ID": scrape_run_id,
            },
            use_xvfb=use_xvfb and (not script_headless),
        )
        result["headless"] = script_headless
        result["used_headful_retry"] = False
        result["json_count"] = _count_json_files(output_dir)

        retry_headful_allowed = script_name_l not in no_headful_retry_scrapers

        if retry_headful_on_fail and retry_headful_allowed and script_headless and not result["success"]:
            print(
                f"[{index}/{len(scrapers)}] {script_name} failed in headless. "
                "Retrying in headful mode..."
            )
            retry_result = _run_python_script(
                script_path=scraper_path,
                log_path=run_dir / f"{scraper_path.stem}_headful_retry.log",
                timeout_minutes=script_timeout_minutes,
                extra_env={
                    "SCRAP_HEADLESS": "0",
                    "SCRAPE_RUN_ID": scrape_run_id,
                },
                use_xvfb=use_xvfb,
            )
            retry_result["headless"] = False
            retry_result["used_headful_retry"] = True
            retry_result["json_count"] = _count_json_files(output_dir)
            if retry_result["success"]:
                result = retry_result
            else:
                result["headful_retry_attempted"] = True
                result["headful_retry_success"] = retry_result["success"]
                result["headful_retry_return_code"] = retry_result["return_code"]
                result["headful_retry_json_count"] = retry_result["json_count"]

        if (
            retry_on_empty
            and retry_headful_allowed
            and script_headless
            and result["success"]
            and result["json_count"] == 0
        ):
            print(
                f"[{index}/{len(scrapers)}] {script_name} produced 0 JSON in headless. "
                "Retrying in headful mode..."
            )
            retry_result = _run_python_script(
                script_path=scraper_path,
                log_path=run_dir / f"{scraper_path.stem}_headful_retry.log",
                timeout_minutes=script_timeout_minutes,
                extra_env={
                    "SCRAP_HEADLESS": "0",
                    "SCRAPE_RUN_ID": scrape_run_id,
                },
                use_xvfb=use_xvfb,
            )
            retry_result["headless"] = False
            retry_result["used_headful_retry"] = True
            retry_result["json_count"] = _count_json_files(output_dir)
            if retry_result["success"] and (retry_result["json_count"] or 0) > 0:
                result = retry_result
            else:
                result["headful_retry_attempted"] = True
                result["headful_retry_success"] = retry_result["success"]
                result["headful_retry_return_code"] = retry_result["return_code"]
                result["headful_retry_json_count"] = retry_result["json_count"]

        if script_name_l in required_non_empty_scrapers and result["json_count"] == 0:
            result["success"] = False
            result["failure_reason"] = "empty_output_required"
            print(
                f"[{index}/{len(scrapers)}] {script_name} is required to produce JSON "
                "but finished with 0 files."
            )

        result["output_dir"] = str(output_dir) if output_dir else None
        result["output_complete"] = bool(result["success"] and (result["json_count"] or 0) > 0)
        result["partial"] = not result["output_complete"]
        scraper_results.append(result)

        status = "OK" if result["success"] else "FAILED"
        print(
            f"[{index}/{len(scrapers)}] {scraper_path.name} => {status} "
            f"(return_code={result['return_code']}, duration={result['duration_seconds']}s)"
        )

    pre_match_summary = {
        "run_id": run_id,
        "scrape_run_id": scrape_run_id,
        "parent_scrape_run_id": parent_scrape_run_id,
        "source": scrape_source,
        "shard_name": shard_name or None,
        "run_started_at_utc": run_started.isoformat(),
        "scraper_timeout_minutes": scraper_timeout_minutes,
        "scraper_timeout_overrides": scraper_timeout_overrides,
        "match_timeout_minutes": match_timeout_minutes,
        "run_match_products": run_match_products,
        "scraper_count": len(scrapers),
        "scraper_results": scraper_results,
    }
    pre_match_summary_path = run_dir / "scraper_summary_pre_match.json"
    pre_match_summary_path.write_text(json.dumps(pre_match_summary, indent=2, ensure_ascii=False), encoding="utf-8")

    if run_match_products:
        print("Running match_products.py...")
        match_result = _run_python_script(
            script_path=MATCH_SCRIPT,
            log_path=run_dir / "match_products.log",
            timeout_minutes=match_timeout_minutes,
            extra_env={
                "SCRAPE_RUN_ID": scrape_run_id,
                "SCRAPER_SUMMARY_PATH": str(pre_match_summary_path),
            },
        )

        if match_result["success"]:
            print(
                f"match_products.py => OK "
                f"(return_code={match_result['return_code']}, duration={match_result['duration_seconds']}s)"
            )
        else:
            print(
                f"match_products.py => FAILED "
                f"(return_code={match_result['return_code']}, duration={match_result['duration_seconds']}s)"
            )
    else:
        print("Skipping match_products.py because RUN_MATCH_PRODUCTS=0.")
        match_result = {
            "name": MATCH_SCRIPT.name,
            "path": str(MATCH_SCRIPT),
            "skipped": True,
            "success": True,
            "return_code": 0,
            "timed_out": False,
            "duration_seconds": 0,
            "log_file": None,
        }

    scraper_failures = [item for item in scraper_results if not item["success"]]

    if not match_result["success"]:
        final_exit_code = 1
    elif scraper_failures:
        final_exit_code = 2
    else:
        final_exit_code = 0

    run_finished = datetime.now(timezone.utc)
    summary = {
        "run_id": run_id,
        "scrape_run_id": scrape_run_id,
        "parent_scrape_run_id": parent_scrape_run_id,
        "source": scrape_source,
        "shard_name": shard_name or None,
        "run_started_at_utc": run_started.isoformat(),
        "run_finished_at_utc": run_finished.isoformat(),
        "run_duration_seconds": round((run_finished - run_started).total_seconds(), 2),
        "scraper_timeout_minutes": scraper_timeout_minutes,
        "scraper_timeout_overrides": scraper_timeout_overrides,
        "match_timeout_minutes": match_timeout_minutes,
        "run_match_products": run_match_products,
        "scraper_count": len(scrapers),
        "scraper_failures": len(scraper_failures),
        "pre_match_summary_path": str(pre_match_summary_path),
        "scraper_results": scraper_results,
        "match_result": match_result,
        "final_exit_code": final_exit_code,
    }

    summary_path = run_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Run logs: {run_dir}")
    print(f"Summary: {summary_path}")

    if final_exit_code == 0:
        print("Final status: SUCCESS")
    elif final_exit_code == 2:
        print("Final status: PARTIAL_SUCCESS (some scrapers failed)")
    else:
        print("Final status: FAILED (match step failed)")

    return final_exit_code


if __name__ == "__main__":
    raise SystemExit(main())

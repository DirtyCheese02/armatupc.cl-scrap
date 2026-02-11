from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
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


def _discover_scrapers() -> list[Path]:
    excluded_raw = os.environ.get("SCRAPER_EXCLUDE", "")
    excluded = {name.strip() for name in excluded_raw.split(",") if name.strip()}
    scraper_name_pattern = re.compile(r"^scrap_.*\.py$", re.IGNORECASE)

    scrapers = []
    for script in SCRAPERS_DIR.iterdir():
        if not script.is_file():
            continue
        if not scraper_name_pattern.match(script.name):
            continue
        if script.name.startswith("__"):
            continue
        if script.name in excluded:
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
    run_dir = RUN_LOGS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    scraper_timeout_minutes = _parse_timeout_minutes("SCRAPER_TIMEOUT_MINUTES", 90)
    match_timeout_minutes = _parse_timeout_minutes("MATCH_TIMEOUT_MINUTES", 60)
    default_headless = _parse_bool(os.environ.get("SCRAP_HEADLESS"), True)
    use_xvfb = _parse_bool(os.environ.get("SCRAP_USE_XVFB"), True)
    retry_on_empty = _parse_bool(os.environ.get("SCRAPER_RETRY_ON_EMPTY"), True)
    headful_scrapers = _parse_csv_env("SCRAPER_HEADFUL")
    headless_scrapers = _parse_csv_env("SCRAPER_HEADLESS")

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

        output_dir = _infer_output_dir(scraper_path)
        print(
            f"[{index}/{len(scrapers)}] Running {script_name} "
            f"(headless={'1' if script_headless else '0'})..."
        )
        result = _run_python_script(
            script_path=scraper_path,
            log_path=run_dir / f"{scraper_path.stem}.log",
            timeout_minutes=scraper_timeout_minutes,
            extra_env={"SCRAP_HEADLESS": "1" if script_headless else "0"},
            use_xvfb=use_xvfb and (not script_headless),
        )
        result["headless"] = script_headless
        result["used_headful_retry"] = False
        result["json_count"] = _count_json_files(output_dir)

        if (
            retry_on_empty
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
                timeout_minutes=scraper_timeout_minutes,
                extra_env={"SCRAP_HEADLESS": "0"},
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

        scraper_results.append(result)

        status = "OK" if result["success"] else "FAILED"
        print(
            f"[{index}/{len(scrapers)}] {scraper_path.name} => {status} "
            f"(return_code={result['return_code']}, duration={result['duration_seconds']}s)"
        )

    print("Running match_products.py...")
    match_result = _run_python_script(
        script_path=MATCH_SCRIPT,
        log_path=run_dir / "match_products.log",
        timeout_minutes=match_timeout_minutes,
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
        "run_started_at_utc": run_started.isoformat(),
        "run_finished_at_utc": run_finished.isoformat(),
        "run_duration_seconds": round((run_finished - run_started).total_seconds(), 2),
        "scraper_timeout_minutes": scraper_timeout_minutes,
        "match_timeout_minutes": match_timeout_minutes,
        "scraper_count": len(scrapers),
        "scraper_failures": len(scraper_failures),
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

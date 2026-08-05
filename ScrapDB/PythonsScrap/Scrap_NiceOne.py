from __future__ import annotations

import os
import re
from typing import Any

from api_scraper_utils import (
    build_prestashop_xhr_url,
    exit_code_from_count,
    fetch_json,
    html_to_text,
    make_session,
    pick_part_number,
    run_prestashop_xhr_store,
)
from scraper_health import write_scraper_health


CATEGORY_URL_MAP = {
    "OperatingSystem": "https://n1g.cl/Home/38-software",
    "UPS": "https://n1g.cl/Home/94-ups",
    "Headphones": "https://n1g.cl/Home/32-parlantes-audio",
    "Storage": [
        "https://n1g.cl/Home/55-discos-hdd",
        "https://n1g.cl/Home/54-discos-ssd",
        "https://n1g.cl/Home/56-discos-25",
    ],
    "Monitor": "https://n1g.cl/Home/28-monitores",
    "CPUCooler_Air": "https://n1g.cl/Home/61-disipador-por-aire",
    "CPUCooler_Liquid": "https://n1g.cl/Home/62-watercooling",
    "CaseFan": "https://n1g.cl/Home/63-ventiladores",
    "ThermalCompound": "https://n1g.cl/Home/64-pasta-disipadora",
    "PowerSupply": [
        "https://n1g.cl/Home/57-fuentes-certificadas-modular",
        "https://n1g.cl/Home/58-fuentes-certificadas-no-modular",
    ],
    "Case": "https://n1g.cl/Home/24-gabinetes",
    "Memory": [
        "https://n1g.cl/Home/77-ddr5-pc",
        "https://n1g.cl/Home/33-placas-madre",
    ],
    "CPU": "https://n1g.cl/Home/34-procesadores",
    "VideoCard": [
        "https://n1g.cl/Home/130-intel",
        "https://n1g.cl/Home/110-nvidia",
        "https://n1g.cl/Home/111-amd",
    ],
    "Motherboard": "https://n1g.cl/Home/33-placas-madre",
    "NetworkAdapter": "https://n1g.cl/Home/76-adaptadores-de-red",
    "Mouse_Keyboard": "https://n1g.cl/Home/29-mouse-teclados",
}

NICEONE_GENERIC_PART_RE = re.compile(
    r"^(?:"
    r"\d+(?:X\d+)*(?:MM|CM|ML|GB|TB|W|HZ|MHZ|GHZ|BIT|MAH|MB/S)?|"
    r"LGA\d+(?:/\d+)?|"
    r"V\d+(?:\.\d+)?"
    r")$",
    re.IGNORECASE,
)
NICEONE_GENERIC_PART_VALUES = {
    "A3-MATX",
    "ATX",
    "E-ATX",
    "ITX",
    "M-ATX",
    "MATX",
    "MINI-ITX",
    "CH260",
}

def is_generic_niceone_part_number(part_number: str | None) -> bool:
    if not part_number:
        return True
    compact = re.sub(r"\s+", "", part_number).upper()
    return compact in NICEONE_GENERIC_PART_VALUES or bool(NICEONE_GENERIC_PART_RE.fullmatch(compact))


def pick_niceone_part_number(product: dict[str, Any]) -> str | None:
    reference = html_to_text(product.get("reference"))
    primary_values = []
    if reference and not re.fullmatch(r"\d{4,7}", reference):
        primary_values.append(reference)

    part_number = pick_part_number(
        primary_values,
        [product.get("name")],
    )
    if is_generic_niceone_part_number(part_number):
        return None
    return part_number


def niceone_connectivity_probe(timeout: int = 10, retries: int = 2) -> tuple[bool, str | None]:
    session = make_session("https://n1g.cl/Home/")
    session.headers.update(
        {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        }
    )
    probe_url = build_prestashop_xhr_url(CATEGORY_URL_MAP["CPU"])
    try:
        payload, _ = fetch_json(
            session,
            probe_url,
            retries=retries,
            timeout=timeout,
        )
    except Exception as exc:
        return False, str(exc)[:500]
    if not isinstance(payload, dict) or not isinstance(payload.get("products"), list):
        return False, "unexpected_probe_payload"
    return True, None


def is_explicit_access_block(error: str | None) -> bool:
    return bool(error and re.search(r"\b403\b|forbidden|access denied", error, re.IGNORECASE))


def main() -> int:
    request_timeout = int(os.environ.get("NICEONE_REQUEST_TIMEOUT", "20") or "20")
    request_retries = int(os.environ.get("NICEONE_REQUEST_RETRIES", "3") or "3")
    probe_ok, probe_error = niceone_connectivity_probe(
        timeout=min(request_timeout, 10),
        retries=min(request_retries, 2),
    )
    if not probe_ok:
        if is_explicit_access_block(probe_error):
            print(f"NiceOne public catalog is blocked; stopping early: {probe_error}")
            write_scraper_health(
                status="failed",
                expected_categories=CATEGORY_URL_MAP,
                completed_categories=(),
                failed_categories=CATEGORY_URL_MAP,
                product_count=0,
                errors=({"category": "*", "error": probe_error or "access_blocked"},),
                blocked_reason="public_catalog_access_blocked",
            )
            return 1
        # Timeouts can be route-specific, so non-definitive connectivity
        # failures still allow every category one independent attempt.
        print(
            "NiceOne connectivity probe failed; continuing with independent "
            f"category requests: {probe_error}"
        )

    output_dir = "ScrapDB/Outputs/NiceOne"
    category_status: dict[str, bool] = {}
    saved_count = run_prestashop_xhr_store(
        store_name="NiceOne",
        base_url="https://n1g.cl/Home/",
        category_url_map=CATEGORY_URL_MAP,
        output_dir=output_dir,
        output_prefix="NO",
        part_number_picker=pick_niceone_part_number,
        request_retries=request_retries,
        request_timeout=request_timeout,
        category_status=category_status,
    )
    completed_categories = {
        category for category, completed in category_status.items() if completed
    }
    failed_categories = set(CATEGORY_URL_MAP) - completed_categories
    write_scraper_health(
        status=(
            "failed"
            if saved_count == 0
            else ("partial_success" if failed_categories else "success")
        ),
        expected_categories=CATEGORY_URL_MAP,
        completed_categories=completed_categories,
        failed_categories=failed_categories,
        product_count=saved_count,
        errors=(
            ({"category": "*", "error": probe_error or "connectivity_probe_failed"},)
            if not probe_ok
            else ()
        ),
        blocked_reason=None if saved_count > 0 else "store_connect_timeout",
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

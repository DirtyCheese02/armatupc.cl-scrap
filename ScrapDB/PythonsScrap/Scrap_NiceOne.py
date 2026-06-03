from __future__ import annotations

import re
from typing import Any

from api_scraper_utils import html_to_text, pick_part_number, exit_code_from_count, run_prestashop_xhr_store


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


def main() -> int:
    output_dir = "ScrapDB/Outputs/NiceOne"
    saved_count = run_prestashop_xhr_store(
        store_name="NiceOne",
        base_url="https://n1g.cl/Home/",
        category_url_map=CATEGORY_URL_MAP,
        output_dir=output_dir,
        output_prefix="NO",
        part_number_picker=pick_niceone_part_number,
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

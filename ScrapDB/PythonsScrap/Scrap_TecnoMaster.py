from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from api_scraper_utils import (
    brand_from_wc,
    clean_output_dir,
    clean_part_number,
    exit_code_from_count,
    fetch_json,
    first_image_from_wc,
    html_to_text,
    make_session,
    normalize_price,
    write_product_json,
)


BASE_URL = "https://tecno-master.cl"
CATEGORY_QUERIES = {
    "OperatingSystem": [{"category": 630}],
    "UPS": [{"category": 614}],
    "Headphones": [{"category": 575}],
    "Mouse_Keyboard": [{"category": 584}],
    "Mouse": [{"category": 585}],
    "Keyboard": [{"category": 576}],
    "Storage": [{"category": "578,579,591"}],
    "ExternalStorage": [{"category": "588,595"}],
    "Monitor": [{"category": 594}],
    "CPUCooler_CaseFan": [{"category": 581}],
    "PowerSupply": [{"category": 583}],
    "Case": [{"category": 587}],
    "Memory": [{"category": 597}],
    "CPU": [{"category": 592}],
    "VideoCard": [{"category": 600}],
    "Motherboard": [{"category": 589}],
    "Webcam": [{"category": 593}],
    "NetworkAdapter": [{"category": 596}],
}

BRACKETED_CODE_PATTERN = re.compile(r"\[([^\[\]]{4,80})\]")
PARENTHETICAL_CODE_PATTERN = re.compile(r"\(([^\(\)]{4,80})\)")


def is_weak_store_sku(value: str | None) -> bool:
    sku = clean_part_number(value)
    if not sku:
        return True
    compact = re.sub(r"[^A-Za-z0-9]", "", sku)
    if compact.isdigit():
        return True
    return bool(re.fullmatch(r"\d{3,6}-?M?", sku, flags=re.IGNORECASE))


def is_usable_name_code(value: str | None) -> bool:
    code = clean_part_number(value)
    if not code or "," in code or ":" in code:
        return False
    if re.fullmatch(r"\d+\s*x\s*\d+\s*(?:gb|tb|mb)", code, flags=re.IGNORECASE):
        return False
    if re.fullmatch(r"\d+\s*(?:gb|tb|mb)", code, flags=re.IGNORECASE):
        return False
    if len(code.split()) > 4:
        return False

    compact = re.sub(r"[^A-Za-z0-9]", "", code)
    if compact.isdigit():
        return len(compact) >= 7
    return any(char.isdigit() for char in compact) and any(char.isalpha() for char in compact)


def code_from_name_pattern(name: str, pattern: re.Pattern[str]) -> str | None:
    for match in reversed(pattern.findall(name)):
        candidate = clean_part_number(match)
        if candidate and is_usable_name_code(candidate):
            return candidate
    return None


def extract_tecno_master_part_number(product: dict[str, Any], name: str) -> str | None:
    bracketed_part = code_from_name_pattern(name, BRACKETED_CODE_PATTERN)
    if bracketed_part:
        return bracketed_part

    sku = clean_part_number(product.get("sku"))
    if sku and not is_weak_store_sku(sku):
        return sku

    parenthetical_part = code_from_name_pattern(name, PARENTHETICAL_CODE_PATTERN)
    if parenthetical_part:
        return parenthetical_part

    return None


def product_to_output(product: dict[str, Any], category_name: str) -> dict[str, Any] | None:
    url = product.get("permalink") or ""
    name = html_to_text(product.get("name"))
    if not url or not name:
        return None

    part_number = extract_tecno_master_part_number(product, name)
    if not part_number:
        return None

    prices = product.get("prices") or {}
    return {
        "store_name": "TecnoMaster",
        "scraped_name": name,
        "scraped_brand": brand_from_wc(product),
        "type": category_name,
        "part #": part_number,
        "price": normalize_price(prices.get("price")),
        "url": url,
        "image_url": first_image_from_wc(product),
    }


def scrape_tecno_master() -> int:
    output_path = clean_output_dir("ScrapDB/Outputs/TecnoMaster")
    session = make_session(BASE_URL)
    api_url = urljoin(BASE_URL, "/wp-json/wc/store/v1/products")
    saved_count = 0
    skipped_without_part = 0
    seen_urls: set[str] = set()

    for category_name, query_list in CATEGORY_QUERIES.items():
        for query in query_list:
            page = 1
            while True:
                params = {"per_page": 100, "page": page, **query}
                products, response = fetch_json(session, api_url, params=params)
                if not isinstance(products, list):
                    raise RuntimeError(f"Unexpected TecnoMaster response: {products!r}")

                total_pages = int(response.headers.get("X-WP-TotalPages", "1") or "1")
                print(f"TecnoMaster {category_name} page {page}/{total_pages}: {len(products)} products")

                for product in products:
                    url = product.get("permalink") or ""
                    if not url or url in seen_urls:
                        continue

                    data = product_to_output(product, category_name)
                    if not data:
                        skipped_without_part += 1
                        continue

                    seen_urls.add(url)
                    write_product_json(output_path, "TM", url, data)
                    saved_count += 1

                if page >= total_pages:
                    break
                page += 1

    print(
        f"TecnoMaster scraping finished. Saved {saved_count} JSON files; "
        f"skipped {skipped_without_part} products without part number."
    )
    return saved_count


def main() -> int:
    return exit_code_from_count(scrape_tecno_master())


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import html
import os
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


BASE_URL = "https://www.tecnoshopping.cl"
CATEGORY_QUERIES = {
    "UPS": [{"category": 102}],
    "Headphones": [{"category": 256}],
    "Mouse": [{"category": 329}],
    "Storage": [{"category": "234,380"}],
    "Monitor": [{"category": 27}],
    "CPUCooler_CaseFan": [{"category": "387,382"}],
    "PowerSupply": [{"category": 304}],
    "Case": [{"category": 383}],
    "Memory": [{"category": "241,232,379"}],
    "CPU": [{"category": 153}],
    "VideoCard": [{"category": 158}],
    "Motherboard": [{"category": 155}],
    "NetworkAdapter": [{"category": 381}],
}

EMPTY_PART_VALUES = {"", "N/A", "NA", "NONE", "NULL", "SIN SKU", "SKU NO INFORMADO"}
GENERIC_PART_PREFIXES = (
    "DDR",
    "GDDR",
    "USB",
    "SATA",
    "PCIE",
    "PCI-E",
    "NVME",
    "HDMI",
    "ARGB",
    "RGB",
    "ATX",
    "MATX",
    "M-ATX",
    "ITX",
    "AM4",
    "AM5",
    "LGA",
)


def configured_max_products() -> int:
    raw_value = os.environ.get("TECNOSHOPPING_MAX_PRODUCTS") or os.environ.get("SCRAPER_MAX_PRODUCTS") or "0"
    try:
        return max(0, int(raw_value))
    except ValueError:
        return 0


def clean_tecnoshopping_part_number(value: Any) -> str | None:
    part_number = clean_part_number(value)
    if not part_number:
        return None

    part_number = html.unescape(part_number).strip(" '\"\t\r\n")
    if part_number.upper() in EMPTY_PART_VALUES:
        return None

    compact = re.sub(r"[^A-Za-z0-9]", "", part_number)
    if len(compact) < 3 or compact.isdigit():
        return None
    if not any(char.isalpha() for char in compact) or not any(char.isdigit() for char in compact):
        return None

    normalized = part_number.upper().replace(" ", "")
    if normalized.startswith(GENERIC_PART_PREFIXES):
        return None
    if re.fullmatch(r"\d+\s*(?:gb|tb|mb|w|hz|mhz|ghz|dpi|mm|cm|in|inch)", part_number, re.IGNORECASE):
        return None

    return part_number


def product_to_output(product: dict[str, Any], category_name: str) -> dict[str, Any] | None:
    url = product.get("permalink") or ""
    name = html_to_text(product.get("name"))
    if not url or not name:
        return None

    part_number = clean_tecnoshopping_part_number(product.get("sku"))
    if not part_number:
        return None

    prices = product.get("prices") or {}
    return {
        "store_name": "TecnoShopping",
        "scraped_name": name,
        "scraped_brand": brand_from_wc(product),
        "type": category_name,
        "part #": part_number,
        "price": normalize_price(prices.get("price")),
        "url": url,
        "image_url": first_image_from_wc(product),
    }


def scrape_tecnoshopping() -> int:
    output_dir = "ScrapDB/Outputs/TecnoShopping"
    output_path = clean_output_dir(output_dir)
    session = make_session(BASE_URL)
    api_url = urljoin(BASE_URL, "/wp-json/wc/store/v1/products")
    max_products = configured_max_products()
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
                    raise RuntimeError(f"Unexpected TecnoShopping response: {products!r}")

                total_pages = int(response.headers.get("X-WP-TotalPages", "1") or "1")
                print(f"TecnoShopping {category_name} page {page}/{total_pages}: {len(products)} products")

                for product in products:
                    url = product.get("permalink") or ""
                    if not url or url in seen_urls:
                        continue

                    data = product_to_output(product, category_name)
                    if not data:
                        skipped_without_part += 1
                        continue

                    seen_urls.add(url)
                    write_product_json(output_path, "TSP", url, data)
                    saved_count += 1

                    if max_products and saved_count >= max_products:
                        print(
                            f"TecnoShopping reached TECNOSHOPPING_MAX_PRODUCTS={max_products}; "
                            "stopping early."
                        )
                        print(
                            f"TecnoShopping scraping finished. Saved {saved_count} JSON files; "
                            f"skipped {skipped_without_part} products without usable part number."
                        )
                        return saved_count

                if page >= total_pages:
                    break
                page += 1

    print(
        f"TecnoShopping scraping finished. Saved {saved_count} JSON files; "
        f"skipped {skipped_without_part} products without usable part number."
    )
    return saved_count


def main() -> int:
    return exit_code_from_count(scrape_tecnoshopping())


if __name__ == "__main__":
    raise SystemExit(main())

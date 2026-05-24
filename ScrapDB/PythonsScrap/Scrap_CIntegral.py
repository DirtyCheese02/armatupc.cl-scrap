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
    infer_part_number_from_name,
    make_session,
    normalize_price,
    write_product_json,
)


BASE_URL = "https://cintegral.cl"
CATEGORY_QUERIES = {
    "OperatingSystem": [{"category": 125}],
    "UPS": [{"category": 141}],
    "Headphones": [{"category": 139}],
    "Mouse": [{"category": 123}],
    "Keyboard": [{"category": 121}],
    "Mouse_Keyboard": [{"category": 122}],
    "Storage": [{"category": "181,183"}],
    "ExternalStorage": [{"category": 182}],
    "Monitor": [{"category": "96,160"}],
    "CPUCooler_CaseFan": [{"category": 364}],
    "PowerSupply": [{"category": 116}],
    "Case": [{"category": 115}],
    "Memory": [{"category": 118}],
    "CPU": [{"category": 114}],
    "VideoCard": [{"category": 119}],
    "Motherboard": [{"category": 117}],
    "Webcam": [{"category": 188}],
    "NetworkAdapter": [{"category": "142,145"}],
}

SKU_PREFIXES = (
    "AMIFI26FFE",
    "PUPAPC",
    "SOSLIC",
    "ETVPN",
    "ETVAS",
    "EMBAS",
    "EMBGI",
    "DPRAM",
    "DPRIN",
    "PMEKI",
    "PMEAD",
    "GFUGI",
    "GFUAD",
    "GFUHU",
    "GKSGI",
    "GKSXT",
    "GKSCL",
    "QTFSA",
    "QTFAS",
    "QTFHP",
    "JSDKI",
    "JDETO",
    "AKIKE",
    "AKIHP",
    "ATEHP",
    "ATEMO",
    "ATEKE",
    "ATEGE",
    "AMIFI",
    "AMIHP",
    "AMITR",
    "AMIML",
    "AAUHP",
    "AAUPO",
    "AAUDI",
    "AAUAL",
    "AAULO",
    "AAUMO",
    "AMOHP",
    "AMOKE",
    "PUPAP",
    "GUPAP",
    "SOSMI",
    "SIMLI",
    "ERETH",
    "EREAO",
    "AVECOM",
    "PTRTP",
    "AADKE",
    "AADTP",
    "ACAGE",
    "AUDMLB",
    "PUP",
    "GUP",
    "SIM",
    "ERE",
)
CATEGORY_PREFIXES = (
    "SOS",
    "SIM",
    "GUP",
    "PUP",
    "AMI",
    "AAU",
    "AUD",
    "AMO",
    "ATE",
    "AKI",
    "JSD",
    "JDE",
    "QTF",
    "ERE",
    "AVE",
    "GFU",
    "GKS",
    "PME",
    "DPR",
    "ETV",
    "EMB",
    "PTR",
    "AAD",
    "ACA",
)
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
NAME_CODE_PATTERN = re.compile(
    r"\b(?=[A-Z0-9][A-Z0-9._/#-]{3,39}\b)"
    r"(?=[A-Z0-9._/#-]*[A-Z])"
    r"(?=[A-Z0-9._/#-]*\d)"
    r"[A-Z0-9][A-Z0-9._/#-]*\b"
)
NAMED_MODEL_PATTERNS = (
    re.compile(r"\b(ECAM\s*\d{3,5})\b", re.IGNORECASE),
)
CATEGORY_NAME_FILTERS = {
    "Webcam": re.compile(r"\b(?:webcam|c[aá]mara\s+web|ecam)\b", re.IGNORECASE),
}


def configured_max_products() -> int:
    raw_value = os.environ.get("CINTEGRAL_MAX_PRODUCTS") or os.environ.get("SCRAPER_MAX_PRODUCTS") or "0"
    try:
        return max(0, int(raw_value))
    except ValueError:
        return 0


def is_usable_part_number(value: Any) -> str | None:
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
    if re.match(r"^110[A-Z]{2,}\d+", normalized):
        return None
    if re.fullmatch(r"\d+\s*(?:gb|tb|mb|w|hz|mhz|ghz|dpi|mm|cm|in|inch|p)", part_number, re.IGNORECASE):
        return None

    return part_number


def strip_store_prefix(sku: str) -> list[str]:
    candidates = []
    upper_sku = sku.upper()
    for prefix in SKU_PREFIXES:
        if upper_sku.startswith(prefix):
            candidate = sku[len(prefix) :].lstrip("-_ ")
            if candidate:
                candidates.append(candidate)
    return candidates


def looks_like_store_prefixed_sku(sku: str) -> bool:
    upper_sku = sku.upper()
    return any(upper_sku.startswith(prefix) for prefix in CATEGORY_PREFIXES)


def part_from_name(name: str) -> str | None:
    name_text = html_to_text(name)
    for pattern in NAMED_MODEL_PATTERNS:
        match = pattern.search(name_text)
        if match:
            part_number = is_usable_part_number(match.group(1))
            if part_number:
                return part_number

    inferred = infer_part_number_from_name(name)
    if inferred:
        part_number = is_usable_part_number(inferred)
        if part_number:
            return part_number

    for candidate in sorted(set(NAME_CODE_PATTERN.findall(name_text.upper())), key=len, reverse=True):
        part_number = is_usable_part_number(candidate)
        if part_number:
            return part_number
    return None


def clean_cintegral_part_number(sku: Any, name: str) -> str | None:
    raw_sku = html_to_text(sku)
    for candidate in strip_store_prefix(raw_sku):
        part_number = is_usable_part_number(candidate)
        if part_number:
            return part_number

    name_part = part_from_name(name)
    if name_part:
        return name_part

    part_number = is_usable_part_number(raw_sku)
    if part_number and not looks_like_store_prefixed_sku(raw_sku):
        return part_number

    return None


def product_to_output(product: dict[str, Any], category_name: str) -> dict[str, Any] | None:
    url = product.get("permalink") or ""
    name = html_to_text(product.get("name"))
    if not url or not name:
        return None

    name_filter = CATEGORY_NAME_FILTERS.get(category_name)
    if name_filter and not name_filter.search(name):
        return None

    part_number = clean_cintegral_part_number(product.get("sku"), name)
    if not part_number:
        return None

    prices = product.get("prices") or {}
    return {
        "store_name": "CIntegral",
        "scraped_name": name,
        "scraped_brand": brand_from_wc(product),
        "type": category_name,
        "part #": part_number,
        "price": normalize_price(prices.get("price")),
        "url": url,
        "image_url": first_image_from_wc(product),
    }


def scrape_cintegral() -> int:
    output_dir = "ScrapDB/Outputs/CIntegral"
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
                    raise RuntimeError(f"Unexpected CIntegral response: {products!r}")

                total_pages = int(response.headers.get("X-WP-TotalPages", "1") or "1")
                print(f"CIntegral {category_name} page {page}/{total_pages}: {len(products)} products")

                for product in products:
                    url = product.get("permalink") or ""
                    if not url or url in seen_urls:
                        continue

                    data = product_to_output(product, category_name)
                    if not data:
                        skipped_without_part += 1
                        continue

                    seen_urls.add(url)
                    write_product_json(output_path, "CI", url, data)
                    saved_count += 1

                    if max_products and saved_count >= max_products:
                        print(f"CIntegral reached CINTEGRAL_MAX_PRODUCTS={max_products}; stopping early.")
                        print(
                            f"CIntegral scraping finished. Saved {saved_count} JSON files; "
                            f"skipped {skipped_without_part} products without usable part number."
                        )
                        return saved_count

                if page >= total_pages:
                    break
                page += 1

    print(
        f"CIntegral scraping finished. Saved {saved_count} JSON files; "
        f"skipped {skipped_without_part} products without usable part number."
    )
    return saved_count


def main() -> int:
    return exit_code_from_count(scrape_cintegral())


if __name__ == "__main__":
    raise SystemExit(main())

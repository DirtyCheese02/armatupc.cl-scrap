from __future__ import annotations

import re
import unicodedata
from typing import Any

from bs4 import BeautifulSoup

from api_scraper_utils import (
    clean_part_number,
    exit_code_from_count,
    fetch_text,
    html_to_text,
    make_session,
    pick_part_number,
    run_woocommerce_store,
)


CATEGORY_QUERIES = {
    "UPS": [{"category": 1810}],
    "Headphones": [{"category": 989}],
    "Mouse": [{"category": 127}],
    "Keyboard": [{"category": 142}],
    "Storage": [{"category": 1029}],
    "Monitor": [{"category": 1055}],
    "CPUCooler_ThermalCompound": [{"category": 1769}],
    "CaseFan": [{"category": 1050}],
    "PowerSupply": [{"category": 1048}],
    "Case": [{"category": 1004}],
    "Memory": [{"category": 1047}],
    "CPU": [{"category": 982}],
    "VideoCard": [{"category": 1045}],
    "Motherboard": [{"category": 1046}],
    "Webcam": [{"category": 1788}],
}

DETAIL_SESSION = make_session("https://centralgamer.cl")
DETAIL_PART_NUMBER_CACHE: dict[str, str | None] = {}
INTERNAL_SKU_RE = re.compile(r"^CG[A-Z0-9]{6,}$", re.IGNORECASE)


def normalize_label(value: Any) -> str:
    text = html_to_text(value).casefold()
    text = unicodedata.normalize("NFKD", text)
    return "".join(char for char in text if not unicodedata.combining(char))


def is_part_number_label(value: Any) -> bool:
    label = normalize_label(value)
    return (
        "mpn" in label
        or "numero de parte" in label
        or ("part" in label and "number" in label)
        or "part #" in label
    )


def is_internal_centralgamer_sku(value: Any) -> bool:
    compact = re.sub(r"[^A-Za-z0-9]", "", html_to_text(value))
    return bool(INTERNAL_SKU_RE.fullmatch(compact))


def extract_attribute_part_number(product: dict[str, Any]) -> str | None:
    for attribute in product.get("attributes") or []:
        if not is_part_number_label(attribute.get("name")):
            continue
        terms = attribute.get("terms") or attribute.get("values") or attribute.get("options") or []
        for term in terms:
            if isinstance(term, dict):
                value = term.get("name") or term.get("value") or term.get("slug")
            else:
                value = term
            cleaned = clean_part_number(value)
            if cleaned:
                return cleaned
    return None


def extract_part_number_from_product_meta(html_content: str) -> str | None:
    soup = BeautifulSoup(html_content, "lxml")
    for selector in (
        ".product_meta .part-number",
        ".entry-product-meta .part-number",
        ".part_number_wrapper .meta-content",
        "[class*='part_number'] .meta-content",
    ):
        cleaned = clean_part_number(soup.select_one(selector))
        if cleaned:
            return cleaned

    for item in soup.select(".product_meta .meta-item, .entry-product-meta .meta-item"):
        label = item.select_one(".meta-label, label, dt, strong")
        if not is_part_number_label(label):
            continue
        content = item.select_one(".meta-content, dd, .part-number")
        cleaned = clean_part_number(content)
        if cleaned:
            return cleaned

    return None


def fetch_detail_part_number(url: str) -> str | None:
    if not url:
        return None
    if url not in DETAIL_PART_NUMBER_CACHE:
        try:
            html_content = fetch_text(DETAIL_SESSION, url)
            DETAIL_PART_NUMBER_CACHE[url] = extract_part_number_from_product_meta(html_content)
        except Exception as exc:
            print(f"CentralGamer: failed to fetch product detail part number for {url}: {exc}")
            DETAIL_PART_NUMBER_CACHE[url] = None
    return DETAIL_PART_NUMBER_CACHE[url]


def product_fallback_values(product: dict[str, Any]) -> list[Any]:
    image_texts: list[Any] = []
    for image in product.get("images") or []:
        image_texts.extend([image.get("alt"), image.get("name")])
    return [
        *image_texts,
        product.get("short_description"),
        product.get("description"),
        product.get("name"),
    ]


def pick_centralgamer_part_number(product: dict[str, Any]) -> str | None:
    attribute_part_number = extract_attribute_part_number(product)
    if attribute_part_number:
        return attribute_part_number

    sku = product.get("sku")
    fallback_values = product_fallback_values(product)
    if not is_internal_centralgamer_sku(sku):
        part_number = pick_part_number(
            [sku],
            fallback_values,
        )
        if part_number:
            return part_number

    detail_part_number = fetch_detail_part_number(html_to_text(product.get("permalink")))
    if detail_part_number:
        return detail_part_number

    return pick_part_number(
        [],
        fallback_values,
    )


def main() -> int:
    output_dir = "ScrapDB/Outputs/CentralGamer"
    saved_count = run_woocommerce_store(
        store_name="CentralGamer",
        base_url="https://centralgamer.cl",
        category_queries=CATEGORY_QUERIES,
        output_dir=output_dir,
        output_prefix="CG",
        part_number_picker=pick_centralgamer_part_number,
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

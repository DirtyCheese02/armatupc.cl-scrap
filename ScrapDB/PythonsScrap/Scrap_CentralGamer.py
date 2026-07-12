from __future__ import annotations

import re
import unicodedata
from typing import Any

from bs4 import BeautifulSoup

from api_scraper_utils import (
    absolute_url,
    build_woocommerce_page_url,
    clean_part_number,
    exit_code_from_count,
    fetch_text,
    html_to_text,
    make_session,
    normalize_price,
    pick_part_number,
    run_woocommerce_store,
    selected_attr,
    selected_text,
)
from scraper_health import write_scraper_health


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

CATEGORY_LISTING_URLS = {
    "UPS": "https://centralgamer.cl/componentes-pc/energia-y-proteccion/",
    "Headphones": "https://centralgamer.cl/perifericos/audifonos-gamer/",
    "Mouse": "https://centralgamer.cl/perifericos/mouse-gamer/",
    "Keyboard": "https://centralgamer.cl/perifericos/teclado-gamer/",
    "Storage": "https://centralgamer.cl/componentes-pc/almacenamiento/",
    "Monitor": "https://centralgamer.cl/monitores/monitores-gamer/",
    "CPUCooler_ThermalCompound": "https://centralgamer.cl/componentes-pc/refrigeracion-pc/",
    "CaseFan": "https://centralgamer.cl/componentes-pc/refrigeracion-pc/",
    "PowerSupply": "https://centralgamer.cl/componentes-pc/fuentes-de-poder/",
    "Case": "https://centralgamer.cl/componentes-pc/gabinetes-gamer/",
    "Memory": "https://centralgamer.cl/componentes-pc/memorias-ram/",
    "CPU": "https://centralgamer.cl/componentes-pc/procesadores/",
    "VideoCard": "https://centralgamer.cl/componentes-pc/tarjetas-de-video/",
    "Motherboard": "https://centralgamer.cl/componentes-pc/placas-madre/",
    "Webcam": "https://centralgamer.cl/perifericos/streaming/",
}

DETAIL_SESSION = make_session("https://centralgamer.cl")
DETAIL_PART_NUMBER_CACHE: dict[str, str | None] = {}
INTERNAL_SKU_RE = re.compile(r"^CG[A-Z0-9]{6,}$", re.IGNORECASE)


def valid_part_number(value: Any) -> str | None:
    cleaned = clean_part_number(value)
    if not cleaned or len(cleaned) > 128 or len(cleaned.split()) > 12:
        return None
    return cleaned


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
            cleaned = valid_part_number(value)
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
        cleaned = valid_part_number(soup.select_one(selector))
        if cleaned:
            return cleaned

    for item in soup.select(".product_meta .meta-item, .entry-product-meta .meta-item"):
        label = item.select_one(".meta-label, label, dt, strong")
        if not is_part_number_label(label):
            continue
        content = item.select_one(".meta-content, dd, .part-number")
        cleaned = valid_part_number(content)
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
        if valid_part_number(part_number):
            return valid_part_number(part_number)

    detail_part_number = fetch_detail_part_number(html_to_text(product.get("permalink")))
    if valid_part_number(detail_part_number):
        return valid_part_number(detail_part_number)

    return valid_part_number(pick_part_number([], fallback_values))


def parse_centralgamer_product(soup: BeautifulSoup, url: str, category_name: str, base_url: str):
    name = selected_text(soup, ("h1.product_title", "h1"))
    if not name:
        return None
    part_number = extract_part_number_from_product_meta(str(soup))
    if not valid_part_number(part_number):
        part_number = valid_part_number(selected_text(soup, ("span.sku", ".sku")))
    if not part_number:
        part_number = valid_part_number(pick_part_number([], [name]))
    if not part_number:
        return None

    price = selected_text(
        soup,
        (
            "table tr:first-child td:nth-child(3) .woocommerce-Price-amount",
            ".summary .price ins .woocommerce-Price-amount",
            ".summary .price .woocommerce-Price-amount",
        ),
    )
    if normalize_price(price) in {"N/A", "0"}:
        price = selected_attr(soup, "meta[property='product:price:amount']", "content")
    image = selected_attr(
        soup,
        ("img.wp-post-image", ".woocommerce-product-gallery__image img", "meta[property='og:image']"),
        "src",
    ) or selected_attr(soup, "meta[property='og:image']", "content")
    brand = selected_text(soup, (".product_meta .posted_in a", "[itemprop='brand']")) or "N/A"
    normalized_price = normalize_price(price)
    if normalized_price in {"N/A", "0"}:
        return None
    return {
        "store_name": "CentralGamer",
        "scraped_name": name,
        "scraped_brand": brand,
        "type": category_name,
        "part #": part_number,
        "price": normalized_price,
        "cash_price": normalized_price,
        "availability": "available",
        "url": url,
        "image_url": absolute_url(base_url, image),
    }


def main() -> int:
    output_dir = "ScrapDB/Outputs/CentralGamer"
    saved_count = run_woocommerce_store(
        store_name="CentralGamer",
        base_url="https://centralgamer.cl",
        category_queries=CATEGORY_QUERIES,
        output_dir=output_dir,
        output_prefix="CG",
        part_number_picker=pick_centralgamer_part_number,
        category_listing_urls=CATEGORY_LISTING_URLS,
        html_fallback_config={
            "product_link_selectors": (
                "a.woocommerce-LoopProduct-link",
                "a.woocommerce-loop-product__link",
                "h3.wd-entities-title a[href]",
                ".product-grid-item a[href*='/producto/']",
            ),
            "pagination_selectors": ("ul.page-numbers a", "ul.page-numbers span"),
            "page_url_builder": build_woocommerce_page_url,
            "parse_product": parse_centralgamer_product,
            "product_url_pattern": None,
        },
    )
    write_scraper_health(
        status="success" if saved_count else "failed",
        expected_categories=CATEGORY_QUERIES,
        completed_categories=CATEGORY_QUERIES if saved_count else (),
        failed_categories=() if saved_count else CATEGORY_QUERIES,
        product_count=saved_count,
        blocked_reason=None if saved_count else "api_and_html_unavailable",
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

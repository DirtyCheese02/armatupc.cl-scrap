from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from bs4 import BeautifulSoup

from api_scraper_utils import (
    absolute_url,
    build_woocommerce_page_url,
    exit_code_from_count,
    normalize_price,
    run_woocommerce_store,
    selected_attr,
    selected_text,
)
from scraper_health import write_scraper_health


BASE_URL = "https://www.dazbogstore.cl"
CATEGORY_QUERIES = {
    "Mouse": [{"category": 722}],
    "Monitor": [{"category": 703}],
    "CPUCooler": [{"category": 714}],
    "PowerSupply": [{"category": 683}],
    "CPU": [{"category": 685}],
    "VideoCard": [{"category": 684}],
    "Motherboard": [{"category": 705}],
}
CATEGORY_LISTING_URLS = {
    "Mouse": f"{BASE_URL}/product-category/perifericos/",
    "Monitor": f"{BASE_URL}/product-category/monitores/",
    "CPUCooler": f"{BASE_URL}/product-category/cooler-cpu/",
    "PowerSupply": f"{BASE_URL}/product-category/psu/",
    "CPU": f"{BASE_URL}/product-category/cpu/",
    "VideoCard": f"{BASE_URL}/product-category/gpus/",
    "Motherboard": f"{BASE_URL}/product-category/placas-madre/",
}


def _json_ld_nodes(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        graph = value.get("@graph")
        if graph is not None:
            yield from _json_ld_nodes(graph)
    elif isinstance(value, list):
        for item in value:
            yield from _json_ld_nodes(item)


def _product_json_ld(soup: BeautifulSoup) -> dict[str, Any] | None:
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            payload = json.loads(script.get_text())
        except (TypeError, ValueError):
            continue
        for node in _json_ld_nodes(payload):
            node_type = node.get("@type")
            types = node_type if isinstance(node_type, list) else [node_type]
            if "Product" in types:
                return node
    return None


def _offer_from_product(product: dict[str, Any]) -> dict[str, Any]:
    offers = product.get("offers") or {}
    if isinstance(offers, list):
        return next((offer for offer in offers if isinstance(offer, dict)), {})
    return offers if isinstance(offers, dict) else {}


def parse_dazbog_product(
    soup: BeautifulSoup,
    url: str,
    category_name: str,
    base_url: str,
) -> dict[str, Any] | None:
    product = _product_json_ld(soup) or {}
    offer = _offer_from_product(product)
    name = str(product.get("name") or selected_text(soup, ("h1.product_title", "h1"))).strip()
    part_number = str(product.get("sku") or selected_text(soup, (".product_meta .sku", ".sku"))).strip()
    price = normalize_price(offer.get("price"))
    if not name or not part_number or price in {"N/A", "0"}:
        return None

    brand_raw = product.get("brand") or {}
    brand = brand_raw.get("name") if isinstance(brand_raw, dict) else brand_raw
    image_raw = product.get("image")
    if isinstance(image_raw, list):
        image_raw = image_raw[0] if image_raw else None
    image = image_raw or selected_attr(
        soup,
        (".woocommerce-product-gallery img", "img.wp-post-image"),
        "src",
    )
    availability_raw = str(offer.get("availability") or "").casefold()
    availability = "unavailable" if "outofstock" in availability_raw else "available"
    return {
        "store_name": "DazbogStore",
        "scraped_name": name,
        "scraped_brand": str(brand or "N/A").strip(),
        "type": category_name,
        "part #": part_number,
        "price": price,
        "availability": availability,
        "url": url,
        "image_url": absolute_url(base_url, image),
    }


def main() -> int:
    output_dir = "ScrapDB/Outputs/DazbogStore"
    category_status: dict[str, bool] = {}
    saved_count = run_woocommerce_store(
        store_name="DazbogStore",
        base_url=BASE_URL,
        category_queries=CATEGORY_QUERIES,
        output_dir=output_dir,
        output_prefix="DZB",
        category_listing_urls=CATEGORY_LISTING_URLS,
        html_fallback_config={
            "product_link_selectors": (
                ".wd-product a.product-image-link[href]",
                ".product-grid-item a[href*='/product/']",
            ),
            "pagination_selectors": (".woocommerce-pagination .page-numbers", ".page-numbers"),
            "page_url_builder": build_woocommerce_page_url,
            "parse_product": parse_dazbog_product,
            "product_url_pattern": r"/product/",
        },
        category_status=category_status,
    )
    failed_categories = {name for name, complete in category_status.items() if not complete}
    completed_categories = set(CATEGORY_QUERIES) - failed_categories
    status = "failed" if saved_count == 0 else ("partial_success" if failed_categories else "success")
    write_scraper_health(
        status=status,
        expected_categories=CATEGORY_QUERIES,
        completed_categories=completed_categories,
        failed_categories=failed_categories,
        product_count=saved_count,
        errors=(
            {"category": category, "error": "api_and_html_unavailable"}
            for category in sorted(failed_categories)
        ),
        blocked_reason="api_and_html_unavailable" if saved_count == 0 else None,
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

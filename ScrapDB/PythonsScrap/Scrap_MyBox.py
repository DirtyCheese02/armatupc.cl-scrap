from __future__ import annotations

import json
import os
import re
from pathlib import Path

from api_scraper_utils import (
    absolute_url,
    build_query_page_url,
    clean_output_dir,
    exit_code_from_count,
    fetch_text_with_referer,
    make_session,
    normalize_price,
    pick_part_number,
    run_prestashop_xhr_store,
    selected_attr,
    selected_text,
)
from browser_fallback_utils import (
    browser_fallback_enabled,
    probe_browser_category,
    run_browser_fallback_store,
)
from scraper_health import write_scraper_health


CATEGORY_URL_MAP = {
    "OperatingSystem": "https://mybox.cl/29-software",
    "UPS": "https://mybox.cl/91-respaldo-energetico-ups",
    "Headphones": "https://mybox.cl/21-audifonos-headset",
    "Mouse_Keyboard": "https://mybox.cl/20-teclados-mouse",
    "Storage": "https://mybox.cl/67-almacenamiento",
    "Monitor": "https://mybox.cl/28-pantallas-y-monitores",
    "CPUCooler": "https://mybox.cl/92-enfriamiento-refrigeracion",
    "CaseFan": "https://mybox.cl/89-ventiladores-fans",
    "PowerSupply": "https://mybox.cl/63-fuentes-de-poder",
    "Case": "https://mybox.cl/62-gabinetes",
    "Memory": "https://mybox.cl/66-memoria-ram",
    "CPU": "https://mybox.cl/64-procesador",
    "VideoCard": "https://mybox.cl/68-tarjeta-de-video",
    "Motherboard": "https://mybox.cl/65-placa-madre",
    "Webcam": "https://mybox.cl/23-webcam",
    "NetworkAdapter": "https://mybox.cl/90-redes-conectividad?q=Categor%C3%ADas-Adaptadores+de+Red",
}

MYBOX_LISTING_READY_SELECTORS = (
    "//div[contains(@class,'products')]",
    "//a[contains(@class,'product-thumbnail')]",
)

MYBOX_PRODUCT_CONFIG = {
    "ready_selectors": (
        "//h1[@itemprop='name']",
        "//h1",
        "//*[contains(@class,'product-reference')]",
    ),
    "name_selectors": (
        "//h1[contains(@class,'page-title') and @itemprop='name']/span",
        "//h1[@itemprop='name']/span",
        "//h1[@itemprop='name']",
        "//h1",
    ),
    "part_selectors": (
        "//span[@itemprop='sku']",
        "//*[@itemprop='sku']",
        "//*[contains(@class,'product-reference')]//span",
        "//*[contains(@class,'product-reference')]",
    ),
    "price_selectors": (
        "//span[contains(@class,'product-price')]",
        "//*[contains(@class,'current-price')]//*[@itemprop='price']",
        "//*[@itemprop='price']",
        "//*[contains(@class,'product-prices')]//*[contains(@class,'price')]",
    ),
    "image_selectors": (
        "//div[contains(@class,'swiper-slide-active')]/img",
        "//div[contains(@class,'product-cover')]//img",
        "//img[@itemprop='image']",
    ),
    "brand_selectors": (),
}


def clean_mybox_part_number(value: str) -> str:
    return pick_part_number([value], (), allow_name_fallback=False) or ""


def mybox_connectivity_probe(timeout: int = 10) -> tuple[bool, str | None]:
    session = make_session("https://mybox.cl")
    try:
        fetch_text_with_referer(
            session,
            CATEGORY_URL_MAP["CPU"],
            "https://mybox.cl",
            retries=1,
            timeout=timeout,
        )
    except Exception as exc:
        return False, str(exc)[:500]
    return True, None


def is_explicit_access_block(error: str | None) -> bool:
    return bool(error and re.search(r"\b403\b|forbidden|access denied", error, re.IGNORECASE))


def output_categories(output_dir: str) -> set[str]:
    categories: set[str] = set()
    for path in Path(output_dir).glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        category = str(payload.get("type") or "").strip()
        if category:
            categories.add(category)
    return categories


def parse_mybox_product(soup, url: str, category_name: str, base_url: str):
    name = selected_text(
        soup,
        (
            "h1.h1.page-title[itemprop='name'] span",
            "h1[itemprop='name'] span",
            "h1[itemprop='name']",
            "h1.h1",
            "h1",
        ),
    )
    if not name:
        return None

    sku = selected_text(
        soup,
        (
            "span[itemprop='sku']",
            ".product-reference span",
            ".product-reference",
        ),
    )
    part_number = pick_part_number([sku], [name], allow_name_fallback=False) or "N/A"
    image = selected_attr(
        soup,
        (
            "div.swiper-slide-active img",
            ".product-cover img",
            "img.js-qv-product-cover",
            ".images-container img",
            "img[itemprop='image']",
        ),
        "src",
    )

    return {
        "store_name": "MyBox",
        "scraped_name": name,
        "scraped_brand": "N/A",
        "type": category_name,
        "part #": part_number,
        "price": normalize_price(
            selected_text(
                soup,
                (
                    "span.product-price",
                    ".current-price span[itemprop='price']",
                    ".current-price .product-price",
                    ".product-prices .price",
                ),
            )
        ),
        "url": url,
        "image_url": absolute_url(base_url, image),
    }


def main() -> int:
    output_dir = "ScrapDB/Outputs/MyBox"
    clean_output_dir(output_dir)
    force_browser = os.environ.get("SCRAPER_FORCE_BROWSER_FALLBACK", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    if force_browser:
        probe_ok, probe_error = True, None
    else:
        probe_ok, probe_error = mybox_connectivity_probe()
    requests_blocked = not probe_ok and is_explicit_access_block(probe_error)

    if force_browser or requests_blocked:
        saved_count = 0
        reason = "forced browser mode" if force_browser else f"request access block: {probe_error}"
        print(f"MyBox requests path skipped ({reason}).")
    else:
        saved_count = run_prestashop_xhr_store(
            store_name="MyBox",
            base_url="https://mybox.cl",
            category_url_map=CATEGORY_URL_MAP,
            output_dir=output_dir,
            output_prefix="MyB",
            html_fallback_config={
                "product_link_selectors": (
                    "div.products div.product a.product-thumbnail",
                    "article.product-miniature a.product-thumbnail",
                    "a.thumbnail.product-thumbnail",
                    ".product-miniature a[href]",
                ),
                "pagination_selectors": (
                    "nav.pagination a",
                    "ul.page-list a",
                    "a.js-search-link",
                ),
                "page_url_builder": lambda url, page: build_query_page_url(url, page, "page"),
                "parse_product": parse_mybox_product,
                "product_url_pattern": r"\.html(?:$|\?)",
            },
        )
    print(f"MyBox requests path saved {saved_count} JSON files.")

    if browser_fallback_enabled(saved_count):
        if force_browser or requests_blocked:
            browser_ok, browser_error = probe_browser_category(
                CATEGORY_URL_MAP["CPU"],
                MYBOX_LISTING_READY_SELECTORS,
                timeout_seconds=20,
            )
            if not browser_ok:
                print(f"MyBox browser probe failed; stopping early: {browser_error}")
                write_scraper_health(
                    status="failed",
                    expected_categories=CATEGORY_URL_MAP,
                    completed_categories=(),
                    failed_categories=CATEGORY_URL_MAP,
                    product_count=0,
                    errors=({"category": "*", "error": browser_error or probe_error or "blocked"},),
                    blocked_reason="public_catalog_access_blocked",
                )
                return 1
        print("MyBox starting browser fallback.")
        saved_count = run_browser_fallback_store(
            store_name="MyBox",
            category_url_map=CATEGORY_URL_MAP,
            output_dir=output_dir,
            output_prefix="MyB",
            listing_config={
                "link_selector": "//div[contains(@class,'products')]/div[contains(@class,'product')]//a[contains(@class,'product-thumbnail')]",
                "pagination_selector": "//nav[@class='pagination']/ul/li",
                "page_url_builder": lambda url, page: build_query_page_url(url, page, "page"),
                "ready_selectors": MYBOX_LISTING_READY_SELECTORS,
            },
            product_config={
                **MYBOX_PRODUCT_CONFIG,
                "clean_part_number": clean_mybox_part_number,
                "clean_price": normalize_price,
            },
        )
        print(f"MyBox browser fallback saved {saved_count} JSON files.")

    completed_categories = output_categories(output_dir)
    failed_categories = set(CATEGORY_URL_MAP) - completed_categories
    write_scraper_health(
        status="failed" if saved_count == 0 else ("partial_success" if failed_categories else "success"),
        expected_categories=CATEGORY_URL_MAP,
        completed_categories=completed_categories,
        failed_categories=failed_categories,
        product_count=saved_count,
        errors=({"category": "*", "error": probe_error},) if probe_error else (),
        blocked_reason="public_catalog_unavailable" if saved_count == 0 else None,
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

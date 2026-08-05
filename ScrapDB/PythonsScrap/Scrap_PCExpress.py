from __future__ import annotations

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from api_scraper_utils import (
    clean_output_dir,
    exit_code_from_count,
    fetch_text_with_referer,
    html_to_text,
    make_session,
    normalize_price,
    pick_part_number,
    write_product_json,
)
from scraper_health import write_scraper_health


BASE_URL = "https://tienda.pc-express.cl"
MAX_DETAIL_WORKERS = 4
REQUEST_DELAY_SECONDS = 0.35

CATEGORY_URL_MAP = {
    "Case": [
        f"{BASE_URL}/index.php?route=product/category&path=460_462_119&limit=100",
        f"{BASE_URL}/index.php?route=product/category&path=460_462_280&limit=100",
        f"{BASE_URL}/index.php?route=product/category&path=460_462_120&limit=100",
        f"{BASE_URL}/index.php?route=product/category&path=460_462_278&limit=100",
    ],
    "CaseFan": f"{BASE_URL}/index.php?route=product/category&path=460_462_170&limit=100",
    "Mouse_Keyboard": f"{BASE_URL}/index.php?route=product/category&path=460_74&limit=100",
    "Motherboard": f"{BASE_URL}/index.php?route=product/category&path=460_472&limit=100",
    "CPU_CPUCooler_ThermalCompound": f"{BASE_URL}/index.php?route=product/category&path=460_473&limit=100",
    "VideoCard": f"{BASE_URL}/index.php?route=product/category&path=460_475&limit=100",
    "Memory": f"{BASE_URL}/index.php?route=product/category&path=72_126&limit=100",
    "Storage": [
        f"{BASE_URL}/index.php?route=product/category&path=62_413&limit=100",
        f"{BASE_URL}/index.php?route=product/category&path=62_331&limit=100",
    ],
    "ExternalStorage": f"{BASE_URL}/index.php?route=product/category&path=62_102&limit=100",
    "Monitor": [
        f"{BASE_URL}/index.php?route=product/category&path=73_523_128&limit=100",
        f"{BASE_URL}/index.php?route=product/category&path=73_523_171&limit=100",
    ],
    "PowerSupply": f"{BASE_URL}/index.php?route=product/category&path=460_461&limit=100",
    "UPS": f"{BASE_URL}/index.php?route=product/category&path=82&limit=100",
    "Webcam": f"{BASE_URL}/index.php?route=product/category&path=417&limit=100",
}


def clean_pce_part_number(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    # An explicit "P/N" followed by marketing copy was previously accepted
    # as one long identifier. Real PC Express MPNs do not contain sentences.
    if len(value) > 128 or len(value.split()) > 3:
        return None
    return value


def _page_url(url: str, page: int) -> str:
    if page <= 1:
        return url
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["page"] = str(page)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _page_count(soup: BeautifulSoup) -> int:
    pages = []
    for element in soup.select("ul.pagination li, ul.pagination a, .pagination .page-item"):
        text = html_to_text(element)
        if text.isdigit():
            pages.append(int(text))
    return max(pages, default=1)


def parse_listing(html: str, category: str) -> tuple[list[dict[str, str]], int]:
    soup = BeautifulSoup(html, "lxml")
    products: list[dict[str, str]] = []
    seen: set[str] = set()
    for card in soup.select(".product-list__item"):
        link = (
            card.select_one(".product-list__image a[href]")
            or card.select_one(".product-list__name a[href]")
            or card.select_one("a.product-list__btn[href]")
        )
        if link is None:
            continue
        url = urljoin(BASE_URL, str(link.get("href") or ""))
        if not url or url in seen:
            continue
        seen.add(url)
        name = html_to_text(card.select_one(".product-list__name")) or html_to_text(link.get("title"))
        image = card.select_one(".product-list__image img")
        card_text = html_to_text(card).casefold()
        explicitly_unavailable = any(
            marker in card_text
            for marker in ("sin stock", "sin existencias", "producto agotado")
        )
        has_cart_action = card.select_one("button[onclick*='cart.add'], .product-list__btn") is not None
        products.append(
            {
                "type": category,
                "url": url,
                "name": name,
                "brand": html_to_text(card.select_one(".product-list__manufacturer")),
                "price": normalize_price(html_to_text(card.select_one(".product-list__price"))),
                "image_url": urljoin(
                    BASE_URL,
                    str((image.get("data-src") or image.get("src") or "") if image else ""),
                ),
                "availability": (
                    "unavailable"
                    if explicitly_unavailable
                    else ("available" if has_cart_action else "unknown")
                ),
            }
        )
    if not products and "product-list" not in html:
        raise RuntimeError("unexpected_listing_html")
    return products, _page_count(soup)


def product_from_listing(product: dict[str, str]) -> dict[str, Any] | None:
    """Build a publishable record without opening the detail page.

    PC Express includes the manufacturer part number in its listing title and
    exposes an add-to-cart/OOS state on the card.  Persisting these records as
    pages are discovered avoids losing the whole store when detail requests
    later become slow or unavailable in GitHub Actions.
    """
    name = product.get("name", "").strip()
    part_number = clean_pce_part_number(
        pick_part_number((), (name,), allow_name_fallback=True)
    )
    price = normalize_price(product.get("price"))
    availability = product.get("availability")
    if (
        not name
        or not part_number
        or len(part_number) > 128
        or price in {"N/A", "0"}
        or availability not in {"available", "unavailable"}
    ):
        return None
    return {
        "store_name": "PC Express",
        "scraped_name": name,
        "scraped_brand": product.get("brand") or "N/A",
        "type": product["type"],
        "part #": part_number,
        "price": price,
        "availability": availability,
        "url": product["url"],
        "image_url": product.get("image_url") or "N/A",
    }


def _brand_from_detail(soup: BeautifulSoup, fallback: str) -> str:
    for link in soup.select("a[href*='manufacturer/info'], .product-manufacturer a"):
        value = html_to_text(link)
        if value:
            return value
    return fallback or "N/A"


def parse_product_detail(
    html: str,
    product: dict[str, str],
) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "lxml")
    name = html_to_text(soup.select_one("h1.rm-product-page__title, h1")) or product["name"]
    part_number = clean_pce_part_number(
        pick_part_number((), (name,), allow_name_fallback=True)
    )
    if not name or not part_number or len(part_number) > 128:
        return None

    price = normalize_price(
        html_to_text(
            soup.select_one(
                ".rm-product-page__prices h3.text-primary, "
                ".rm-product-page__prices h3, .product-list__price"
            )
        )
        or product.get("price")
    )
    if price in {"N/A", "0"}:
        return None

    image = soup.select_one(
        ".rm-product-page__gallery img, .product-image img, img[data-zoom-image], img[title]"
    )
    image_url = ""
    if image is not None:
        image_url = str(
            image.get("data-zoom-image")
            or image.get("data-large-image")
            or image.get("data-src")
            or image.get("src")
            or ""
        )

    page_text = html_to_text(soup).casefold()
    explicitly_unavailable = (
        "stock web sin stock" in page_text
        or "producto agotado" in page_text
        or "sin existencias" in page_text
    )
    return {
        "store_name": "PC Express",
        "scraped_name": name,
        "scraped_brand": _brand_from_detail(soup, product.get("brand", "")),
        "type": product["type"],
        "part #": part_number,
        "price": price,
        "availability": "unavailable" if explicitly_unavailable else "available",
        "url": product["url"],
        "image_url": urljoin(BASE_URL, image_url or product.get("image_url") or "") or "N/A",
    }


def _fetch_detail(product: dict[str, str]) -> dict[str, Any] | None:
    time.sleep(REQUEST_DELAY_SECONDS)
    session = make_session(BASE_URL)
    html = fetch_text_with_referer(
        session,
        product["url"],
        BASE_URL,
        retries=3,
        timeout=30,
    )
    return parse_product_detail(html, product)


def scrape_pc_express() -> int:
    output_path = clean_output_dir("ScrapDB/Outputs/PCExpress")
    session = make_session(BASE_URL)
    expected = set(CATEGORY_URL_MAP)
    completed: set[str] = set()
    failed: set[str] = set()
    errors: list[dict[str, str]] = []
    discovered: dict[tuple[str, str], dict[str, str]] = {}
    saved_urls: set[str] = set()
    max_products = int(os.environ.get("PCEXPRESS_MAX_PRODUCTS", "0") or "0")

    def remember(products: list[dict[str, str]]) -> None:
        for product in products:
            discovered.setdefault((product["type"], product["url"]), product)
            data = product_from_listing(product)
            below_limit = not max_products or len(saved_urls) < max_products
            if data and data["url"] not in saved_urls and below_limit:
                write_product_json(output_path, "PCE", data["url"], data)
                saved_urls.add(data["url"])

    for category, raw_urls in CATEGORY_URL_MAP.items():
        urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
        category_complete = True
        for category_url in urls:
            try:
                first_html = fetch_text_with_referer(
                    session, category_url, BASE_URL, retries=3, timeout=30
                )
                first_products, pages = parse_listing(first_html, category)
                remember(first_products)
                print(f"PC Express {category} page 1/{pages}: {len(first_products)} products")
                for page in range(2, pages + 1):
                    time.sleep(REQUEST_DELAY_SECONDS)
                    page_url = _page_url(category_url, page)
                    page_html = fetch_text_with_referer(
                        session, page_url, category_url, retries=3, timeout=30
                    )
                    page_products, _ = parse_listing(page_html, category)
                    remember(page_products)
                    print(f"PC Express {category} page {page}/{pages}: {len(page_products)} products")
            except Exception as exc:
                category_complete = False
                errors.append({"category": category, "url": category_url, "error": str(exc)[:500]})
                print(f"PC Express {category} listing failed: {exc}")
        if category_complete:
            completed.add(category)
        else:
            failed.add(category)

    products = [
        product
        for product in discovered.values()
        if product["url"] not in saved_urls
    ]
    if max_products:
        products = products[: max(0, max_products - len(saved_urls))]

    saved_count = len(saved_urls)
    with ThreadPoolExecutor(max_workers=MAX_DETAIL_WORKERS) as executor:
        futures = {executor.submit(_fetch_detail, product): product for product in products}
        for future in as_completed(futures):
            product = futures[future]
            try:
                data = future.result()
                if not data:
                    raise RuntimeError("missing_name_part_number_or_price")
                write_product_json(output_path, "PCE", data["url"], data)
                saved_count += 1
            except Exception as exc:
                # The listing snapshot is still complete when one optional
                # detail enrichment cannot produce an MPN or price. Keep the
                # issue for review without making the whole category partial.
                errors.append(
                    {"category": product["type"], "url": product["url"], "error": str(exc)[:500]}
                )

    status = "failed" if saved_count == 0 else ("partial_success" if failed else "success")
    write_scraper_health(
        status=status,
        expected_categories=expected,
        completed_categories=completed,
        failed_categories=failed,
        product_count=saved_count,
        errors=errors[:100],
        blocked_reason="store_html_unavailable" if saved_count == 0 else None,
    )
    print(f"PC Express scraping finished. Saved {saved_count}; failed={sorted(failed)}")
    return saved_count


def main() -> int:
    return exit_code_from_count(scrape_pc_express())


if __name__ == "__main__":
    raise SystemExit(main())

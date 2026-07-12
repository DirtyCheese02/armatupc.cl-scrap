from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from api_scraper_utils import (
    clean_output_dir,
    clean_part_number,
    exit_code_from_count,
    fetch_text,
    html_to_text,
    make_session,
    normalize_price,
    write_product_json,
)
from scraper_health import write_scraper_health


BASE_URL = "https://www.ctman.cl"
REQUEST_DELAY_SECONDS = 0.5
MAX_DETAIL_WORKERS = 4

CATEGORY_URL_MAP = {
    "OperatingSystem": "https://www.ctman.cl/collections/software/types/software",
    "UPS": "https://www.ctman.cl/collections/ups-respaldo-de-energia/types/ups",
    "Headphones": [
        "https://www.ctman.cl/collections/perifericos-de-pc/types/audifonos",
        "https://www.ctman.cl/collections/gaming/types/audifonos",
    ],
    "Mouse": [
        "https://www.ctman.cl/collections/perifericos-de-pc/types/mouse",
        "https://www.ctman.cl/collections/gaming/types/mouse",
    ],
    "Keyboard": [
        "https://www.ctman.cl/collections/perifericos-de-pc/types/teclados",
        "https://www.ctman.cl/collections/gaming/types/teclados",
    ],
    "Storage": [
        "https://www.ctman.cl/collections/almacenamiento/types/ssd",
        "https://www.ctman.cl/collections/almacenamiento/types/disco-duro",
    ],
    "ExternalStorage": [
        "https://www.ctman.cl/collections/almacenamiento/types/discos-duros-externos",
        "https://www.ctman.cl/collections/almacenamiento/types/ssds-externos",
        "https://www.ctman.cl/collections/almacenamiento/types/tarjeta-de-memoria-flash",
    ],
    "Monitor": "https://www.ctman.cl/collections/monitores/types/monitores",
    "CPUCooler": "https://www.ctman.cl/collections/repuestos-y-componentes/types/coolers-para-pc",
    "PowerSupply": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/fuentes-de-poder",
        "https://www.ctman.cl/collections/pc-escritorio/types/fuentes-de-poder",
    ],
    "Case": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/gabinetes",
        "https://www.ctman.cl/collections/pc-escritorio/types/gabinetes",
        "https://www.ctman.cl/collections/gaming/types/gabinetes",
    ],
    "Memory": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/memorias-ram",
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/memorias-ram-para-laptops",
    ],
    "CPU": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/procesadores",
        "https://www.ctman.cl/collections/gaming/types/procesadores",
        "https://www.ctman.cl/collections/servidores/types/procesadores",
    ],
    "VideoCard": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/tarjetas-de-video",
        "https://www.ctman.cl/collections/gaming/types/tarjetas-de-video",
    ],
    "Motherboard": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/placas-madre",
        "https://www.ctman.cl/collections/pc-escritorio/types/placas-madre",
        "https://www.ctman.cl/collections/gaming/types/placas-madre",
    ],
    "Webcam": "https://www.ctman.cl/collections/electronica-audio-y-video/types/camaras-web",
}


def _build_page_url(url: str, page_number: int) -> str:
    if page_number <= 1:
        return url
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["page"] = str(page_number)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _page_count(soup: BeautifulSoup) -> int:
    values = []
    for element in soup.select(".pagination a, .pagination span"):
        text = html_to_text(element)
        if text.isdigit():
            values.append(int(text))
    return max(values, default=1)


def _listing_products(html: str, category: str) -> tuple[list[dict[str, str]], int]:
    soup = BeautifulSoup(html, "lxml")
    page_text = html_to_text(soup).casefold()
    if "just a moment" in page_text or "checking your browser" in page_text or "verificación de seguridad" in page_text:
        raise RuntimeError("blocked_html_response")
    products: list[dict[str, str]] = []
    for card in soup.select(".product-item"):
        link = card.select_one("a.product-title-link[href]") or card.select_one("a[href*='/products/']")
        if link is None:
            continue
        url = urljoin(BASE_URL, str(link.get("href") or ""))
        name = html_to_text(link) or html_to_text(card.select_one(".product-name"))
        image = card.select_one("img[src]")
        if not url or not name:
            continue
        products.append(
            {
                "type": category,
                "url": url,
                "name": name,
                "image_url": urljoin(BASE_URL, str(image.get("src") or "")) if image else "",
            }
        )
    if not products and soup.select_one(".total-products, .pagination") is None:
        raise RuntimeError("unexpected_empty_listing_html")
    return products, _page_count(soup)


def _iter_json_ld(value: Any):
    if isinstance(value, list):
        for item in value:
            yield from _iter_json_ld(item)
    elif isinstance(value, dict):
        yield value
        if "@graph" in value:
            yield from _iter_json_ld(value["@graph"])


def _product_json_ld(soup: BeautifulSoup) -> dict[str, Any]:
    for script in soup.select("script[type='application/ld+json']"):
        try:
            payload = json.loads(script.string or script.get_text() or "null")
        except (TypeError, ValueError):
            continue
        for item in _iter_json_ld(payload):
            kind = item.get("@type")
            if kind == "Product" or (isinstance(kind, list) and "Product" in kind):
                return item
    return {}


def _meta(soup: BeautifulSoup, key: str) -> str:
    element = soup.select_one(f"meta[property='{key}'], meta[name='{key}']")
    return str(element.get("content") or "").strip() if element else ""


def _brand(value: Any) -> str:
    if isinstance(value, dict):
        return html_to_text(value.get("name"))
    return html_to_text(value)


def _offer(value: Any) -> dict[str, Any]:
    if isinstance(value, list):
        return next((item for item in value if isinstance(item, dict)), {})
    return value if isinstance(value, dict) else {}


def _detail_product(product: dict[str, str]) -> dict[str, Any] | None:
    time.sleep(REQUEST_DELAY_SECONDS)
    session = make_session(BASE_URL)
    html = fetch_text(session, product["url"], retries=3, timeout=30)
    soup = BeautifulSoup(html, "lxml")
    structured = _product_json_ld(soup)
    offer = _offer(structured.get("offers"))

    name = html_to_text(structured.get("name")) or _meta(soup, "og:title") or product["name"]
    part_number = clean_part_number(structured.get("mpn") or structured.get("sku"))
    if not part_number:
        part_number = clean_part_number(
            html_to_text(soup.select_one(".product-sku, [itemprop='sku'], [data-sku]"))
        )
    if not part_number:
        slug = urlsplit(product["url"]).path.rstrip("/").split("/")[-1]
        candidate = slug.split("-", 1)[-1] if "-" in slug else slug
        part_number = clean_part_number(candidate)
    if not part_number or len(part_number) > 128:
        return None

    cash_price = normalize_price(_meta(soup, "product:price:amount") or offer.get("price"))
    if cash_price in {"N/A", "0"}:
        return None
    availability_raw = str(offer.get("availability") or _meta(soup, "product:availability")).lower()
    availability = "unavailable" if "outofstock" in availability_raw or "out of stock" in availability_raw else "available"
    image = structured.get("image")
    if isinstance(image, list):
        image = image[0] if image else ""

    return {
        "store_name": "CTMan",
        "scraped_name": name,
        "scraped_brand": _brand(structured.get("brand")) or _meta(soup, "product:brand") or "N/A",
        "type": product["type"],
        "part #": part_number,
        "price": cash_price,
        "cash_price": cash_price,
        "availability": availability,
        "url": product["url"],
        "image_url": html_to_text(image) or _meta(soup, "og:image") or product["image_url"] or "N/A",
    }


def scrape_ctman() -> int:
    output_dir = clean_output_dir("ScrapDB/Outputs/CTMan")
    expected = set(CATEGORY_URL_MAP)
    completed: set[str] = set()
    failed: set[str] = set()
    errors: list[dict[str, Any]] = []
    discovered: dict[str, dict[str, str]] = {}
    session = make_session(BASE_URL)

    for category, raw_urls in CATEGORY_URL_MAP.items():
        category_ok = True
        urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
        for category_url in urls:
            try:
                first_html = fetch_text(session, category_url, retries=3, timeout=30)
                first_products, pages = _listing_products(first_html, category)
                for item in first_products:
                    discovered.setdefault(item["url"], item)
                print(f"CTMan {category}: page 1/{pages}, {len(first_products)} product cards")
                for page_number in range(2, pages + 1):
                    time.sleep(REQUEST_DELAY_SECONDS)
                    page_html = fetch_text(session, _build_page_url(category_url, page_number), retries=3, timeout=30)
                    products, _ = _listing_products(page_html, category)
                    for item in products:
                        discovered.setdefault(item["url"], item)
                    print(f"CTMan {category}: page {page_number}/{pages}, {len(products)} product cards")
            except Exception as exc:
                category_ok = False
                errors.append({"category": category, "url": category_url, "error": str(exc)[:500]})
                print(f"CTMan {category}: failed listing {category_url}: {exc}")
        (completed if category_ok else failed).add(category)

    saved_count = 0
    with ThreadPoolExecutor(max_workers=MAX_DETAIL_WORKERS) as executor:
        futures = {executor.submit(_detail_product, product): product for product in discovered.values()}
        for future in as_completed(futures):
            product = futures[future]
            try:
                data = future.result()
            except Exception as exc:
                errors.append({"category": product["type"], "url": product["url"], "error": str(exc)[:500]})
                continue
            if not data:
                continue
            write_product_json(output_dir, "CTM", data["url"], data)
            saved_count += 1

    status = "failed" if saved_count == 0 else ("partial_success" if failed else "success")
    write_scraper_health(
        status=status,
        expected_categories=expected,
        completed_categories=completed,
        failed_categories=failed,
        product_count=saved_count,
        errors=errors[:100],
        blocked_reason="html_unavailable" if saved_count == 0 else None,
    )
    print(f"CTMan scraping finished. Saved {saved_count} JSON files; failed categories={sorted(failed)}")
    return saved_count


def main() -> int:
    return exit_code_from_count(scrape_ctman())


if __name__ == "__main__":
    raise SystemExit(main())

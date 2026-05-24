from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Iterable

from bs4 import BeautifulSoup

from api_scraper_utils import (
    absolute_url,
    build_query_page_url,
    clean_output_dir,
    clean_part_number,
    exit_code_from_count,
    html_to_text,
    infer_part_number_from_name,
    make_session,
    normalize_price,
    page_numbers_from_soup,
    product_links_from_soup,
    write_product_json,
)


BASE_URL = "https://www.eylstore.cl"

CATEGORY_URL_MAP = {
    "Headphones": "https://www.eylstore.cl/categorias/audifonos",
    "Mouse": "https://www.eylstore.cl/categorias/mouse",
    "Keyboard": "https://www.eylstore.cl/categorias/teclados",
    "Storage": "https://www.eylstore.cl/categorias/almacenamiento",
    "Monitor": "https://www.eylstore.cl/categorias/monitores",
    "CPUCooler_CaseFan": "https://www.eylstore.cl/categorias/refrigeracion",
    "PowerSupply": "https://www.eylstore.cl/categorias/fuentes-de-poder",
    "Case": "https://www.eylstore.cl/categorias/gabinetes",
    "Memory": "https://www.eylstore.cl/categorias/memorias-ram",
    "CPU": "https://www.eylstore.cl/categorias/procesadores",
    "VideoCard": "https://www.eylstore.cl/categorias/tarjetas-de-video",
    "Motherboard": "https://www.eylstore.cl/categorias/placas-madres",
}

BRACKETED_CODE_PATTERN = re.compile(r"\[([^\[\]]{4,80})\]")
PARENTHETICAL_CODE_PATTERN = re.compile(r"\(([^\(\)]{4,80})\)")
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
GENERIC_PART_VALUES = {
    "SKU NO INFORMADO",
    "NO INFORMADO",
    "SIN SKU",
    "N/A",
    "NA",
    "NONE",
    "NULL",
}


def clean_eyl_text(value: Any) -> str:
    text = html_to_text(value)
    if not text:
        return ""
    if not any(marker in text for marker in ("Ã", "Â", "â€", "â€“", "â€”")):
        return text
    try:
        return text.encode("cp1252").decode("utf-8")
    except UnicodeError:
        return text


def fetch_eyl_html(session: Any, url: str, referer: str | None = None) -> str:
    old_accept = session.headers.get("Accept")
    old_referer = session.headers.get("Referer")
    session.headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    if referer:
        session.headers["Referer"] = referer

    try:
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = session.get(url, timeout=30)
                response.raise_for_status()
                response.encoding = "utf-8"
                return response.text
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    time.sleep(1.5 * attempt)
        raise RuntimeError(f"Failed to fetch HTML from {url}: {last_error}") from last_error
    finally:
        if old_accept is None:
            session.headers.pop("Accept", None)
        else:
            session.headers["Accept"] = old_accept
        if old_referer is None:
            session.headers.pop("Referer", None)
        else:
            session.headers["Referer"] = old_referer


def configured_max_products() -> int:
    raw_value = os.environ.get("EYLSTORE_MAX_PRODUCTS") or os.environ.get("SCRAPER_MAX_PRODUCTS") or "0"
    try:
        return max(0, int(raw_value))
    except ValueError:
        return 0


def json_ld_objects(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            if isinstance(child, (dict, list)):
                yield from json_ld_objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from json_ld_objects(child)


def product_json_ld(soup: BeautifulSoup) -> dict[str, Any]:
    for script in soup.select('script[type="application/ld+json"]'):
        raw_json = script.string or script.get_text()
        if not raw_json or "Product" not in raw_json:
            continue
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        for item in json_ld_objects(payload):
            item_type = item.get("@type")
            if item_type == "Product" or (isinstance(item_type, list) and "Product" in item_type):
                return item
    return {}


def is_usable_part_number(value: Any, *, allow_plain_model: bool = True) -> str | None:
    part_number = clean_part_number(value)
    if not part_number:
        return None

    upper = part_number.upper()
    if upper in GENERIC_PART_VALUES:
        return None

    compact = re.sub(r"[^A-Za-z0-9]", "", part_number)
    if len(compact) < 4 or compact.isdigit():
        return None
    if not any(char.isalpha() for char in compact) or not any(char.isdigit() for char in compact):
        return None

    normalized = upper.replace(" ", "")
    if normalized.startswith(GENERIC_PART_PREFIXES):
        return None
    if re.fullmatch(r"\d+\s*x\s*\d+\s*(?:gb|tb|mb)", upper, flags=re.IGNORECASE):
        return None
    if re.fullmatch(r"\d+\s*(?:gb|tb|mb|w|hz|mhz|ghz)", upper, flags=re.IGNORECASE):
        return None

    if not allow_plain_model and not re.search(r"[-/_ ]", part_number) and len(compact) < 8:
        return None

    return part_number


def code_from_pattern(name: str, pattern: re.Pattern[str]) -> str | None:
    for raw_candidate in reversed(pattern.findall(name or "")):
        candidate = is_usable_part_number(raw_candidate)
        if candidate:
            return candidate
    return None


def visible_code_from_label(soup: BeautifulSoup, label_text: str) -> str | None:
    label_pattern = re.compile(rf"^\s*{label_text}\s*:?\s*$", re.IGNORECASE)
    for label in soup.find_all(string=label_pattern):
        label_element = getattr(label, "parent", None)
        if label_element is None:
            continue
        for sibling in label_element.next_siblings:
            text = html_to_text(sibling)
            if text:
                return text
    return None


def infer_name_part_number(name: str) -> str | None:
    inferred = infer_part_number_from_name(name)
    return is_usable_part_number(inferred, allow_plain_model=False)


def extract_part_number(product: dict[str, Any], soup: BeautifulSoup, name: str) -> str | None:
    for value in (product.get("mpn"), product.get("sku")):
        part_number = is_usable_part_number(value)
        if part_number:
            return part_number

    for extractor in (
        lambda: code_from_pattern(name, BRACKETED_CODE_PATTERN),
        lambda: code_from_pattern(name, PARENTHETICAL_CODE_PATTERN),
        lambda: is_usable_part_number(visible_code_from_label(soup, "C[oó]d"), allow_plain_model=False),
        lambda: infer_name_part_number(name),
    ):
        part_number = extractor()
        if part_number:
            return part_number

    return None


def brand_from_product(product: dict[str, Any]) -> str:
    brand = product.get("brand")
    if isinstance(brand, dict):
        return clean_eyl_text(brand.get("name")) or "N/A"
    return clean_eyl_text(brand) or "N/A"


def price_from_product(product: dict[str, Any], soup: BeautifulSoup) -> str:
    offers = product.get("offers")
    if isinstance(offers, dict) and offers.get("price") is not None:
        return normalize_price(offers.get("price"))
    if isinstance(offers, list):
        for offer in offers:
            if isinstance(offer, dict) and offer.get("price") is not None:
                return normalize_price(offer.get("price"))

    text = html_to_text(soup)
    match = re.search(r"\$\s*[\d.]+", text)
    return normalize_price(match.group(0)) if match else "N/A"


def image_from_product(product: dict[str, Any]) -> str:
    images = product.get("image")
    if isinstance(images, list) and images:
        return absolute_url(BASE_URL, images[0])
    if isinstance(images, str):
        return absolute_url(BASE_URL, images)
    return "N/A"


def parse_product(soup: BeautifulSoup, url: str, category_name: str) -> dict[str, Any] | None:
    product = product_json_ld(soup)
    name = clean_eyl_text(product.get("alternateName") or product.get("name"))
    if not name:
        title = soup.select_one("h1")
        name = clean_eyl_text(title)
    if not name:
        return None

    part_number = extract_part_number(product, soup, name)
    if not part_number:
        return None

    return {
        "store_name": "EYLStore",
        "scraped_name": name,
        "scraped_brand": brand_from_product(product),
        "type": category_name,
        "part #": part_number,
        "price": price_from_product(product, soup),
        "url": url,
        "image_url": image_from_product(product),
    }


def scrape_eyl_store() -> int:
    output_dir = "ScrapDB/Outputs/EYLStore"
    output_path = clean_output_dir(output_dir)
    session = make_session(BASE_URL)
    request_delay = float(os.environ.get("HTML_REQUEST_DELAY_SECONDS", "0.25"))
    max_products = configured_max_products()
    saved_count = 0
    skipped_without_part = 0
    seen_urls: set[str] = set()

    for category_name, category_url in CATEGORY_URL_MAP.items():
        try:
            first_html = fetch_eyl_html(session, category_url, BASE_URL)
            first_soup = BeautifulSoup(first_html, "html.parser")
            page_numbers = page_numbers_from_soup(first_soup, "a[href*='page=']")
            total_pages = max(page_numbers) if page_numbers else 1

            for page in range(1, total_pages + 1):
                page_url = build_query_page_url(category_url, page, "page")
                if page == 1:
                    soup = first_soup
                else:
                    page_html = fetch_eyl_html(session, page_url, category_url)
                    soup = BeautifulSoup(page_html, "html.parser")

                links = product_links_from_soup(soup, BASE_URL, "a[href^='/producto/']")
                print(f"EYLStore {category_name} page {page}/{total_pages}: {len(links)} product links")

                for url in links:
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                    try:
                        product_html = fetch_eyl_html(session, url, page_url)
                        product_soup = BeautifulSoup(product_html, "html.parser")
                        data = parse_product(product_soup, url, category_name)
                        if not data:
                            skipped_without_part += 1
                            continue

                        write_product_json(output_path, "EYL", url, data)
                        saved_count += 1

                        if max_products and saved_count >= max_products:
                            print(
                                f"EYLStore reached EYLSTORE_MAX_PRODUCTS={max_products}; stopping early."
                            )
                            print(
                                f"EYLStore scraping finished. Saved {saved_count} JSON files; "
                                f"skipped {skipped_without_part} products without usable part number."
                            )
                            return saved_count
                    except Exception as exc:
                        print(f"EYLStore {category_name}: error scraping product {url}: {exc}")

                if request_delay:
                    time.sleep(request_delay)
        except Exception as exc:
            print(f"EYLStore {category_name}: error scraping {category_url}: {exc}")

    print(
        f"EYLStore scraping finished. Saved {saved_count} JSON files; "
        f"skipped {skipped_without_part} products without usable part number."
    )
    return saved_count


def main() -> int:
    return exit_code_from_count(scrape_eyl_store())


if __name__ == "__main__":
    raise SystemExit(main())

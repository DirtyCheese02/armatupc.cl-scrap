from __future__ import annotations

import html
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
    fetch_text_with_referer,
    html_to_text,
    infer_part_number_from_name,
    make_session,
    normalize_price,
    page_numbers_from_soup,
    selected_attr,
    selected_text,
    write_product_json,
)


BASE_URL = "https://invasiongamer.com"
CATEGORY_URL_MAP = {
    "Headphones": "https://invasiongamer.com/audifonos",
    "Mouse": "https://invasiongamer.com/mouse",
    "Keyboard": "https://invasiongamer.com/teclados",
    "Storage": "https://invasiongamer.com/componentes-pc/ssd-y-almacenamiento",
    "Monitor": "https://invasiongamer.com/monitores",
    "CPUCooler_CaseFan": "https://invasiongamer.com/componentes-pc/refrigeracion",
    "PowerSupply": "https://invasiongamer.com/componentes-pc/fuentes-de-poder",
    "Case": "https://invasiongamer.com/componentes-pc/gabinetes",
    "Memory": "https://invasiongamer.com/memorias-ram",
    "CPU": "https://invasiongamer.com/procesadores",
    "VideoCard": "https://invasiongamer.com/componentes-pc/tarjetas-de-video",
    "Motherboard": "https://invasiongamer.com/placas-madres",
    "Webcam": "https://invasiongamer.com/webcams",
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
BRACKETED_CODE_PATTERN = re.compile(r"\[([^\[\]]{3,80})\]")
PARENTHETICAL_CODE_PATTERN = re.compile(r"\(([^\(\)]{3,80})\)")
SHORT_MODEL_PATTERN = re.compile(
    r"\b(?=[A-Z0-9][A-Z0-9._/-]{2,30}\b)"
    r"(?=[A-Z0-9._/-]*[A-Z])"
    r"(?=[A-Z0-9._/-]*\d)"
    r"[A-Z0-9][A-Z0-9._/-]*\b"
)


def configured_max_products() -> int:
    raw_value = os.environ.get("INVASIONGAMER_MAX_PRODUCTS") or os.environ.get("SCRAPER_MAX_PRODUCTS") or "0"
    try:
        return max(0, int(raw_value))
    except ValueError:
        return 0


def normalize_invasion_price(value: Any) -> str:
    text = html_to_text(value)
    if not text:
        return "N/A"

    first_currency = re.search(r"\$\s*[\d.]+", text)
    if first_currency:
        return normalize_price(first_currency.group(0))

    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        return str(int(float(text)))

    return normalize_price(text)


def first_srcset_url(value: str) -> str:
    if not value:
        return ""
    return value.split(",")[0].strip().split(" ")[0].strip()


def image_from_card(card: Any) -> str:
    image_url = selected_attr(card, ("img.product-block__image", "img"), "data-src")
    if not image_url:
        image_url = selected_attr(card, ("img.product-block__image", "img"), "src")
    if not image_url:
        image_url = first_srcset_url(selected_attr(card, ("source[srcset]", "img[srcset]"), "srcset"))
    return absolute_url(BASE_URL, image_url) if image_url else "N/A"


def product_cards_from_category(soup: BeautifulSoup, category_name: str) -> list[dict[str, str]]:
    cards: list[dict[str, str]] = []
    for card in soup.select(".product-block"):
        url = absolute_url(
            BASE_URL,
            selected_attr(card, ("a.product-block__anchor", "a.product-block__name", "a[href]"), "href"),
        )
        if url == "N/A":
            continue

        cards.append(
            {
                "category": category_name,
                "url": url,
                "name": selected_text(card, ".product-block__name"),
                "brand": selected_text(card, ".product-block__brand") or "N/A",
                "price": normalize_invasion_price(
                    selected_text(
                        card,
                        (
                            ".product-block__price",
                            ".product-block__price--discount",
                            ".product-block__pricing",
                        ),
                    )
                    or html_to_text(card)
                ),
                "image_url": image_from_card(card),
            }
        )
    return cards


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


def script_field(soup: BeautifulSoup, field_name: str) -> str:
    pattern = re.compile(rf'"{re.escape(field_name)}"\s*:\s*"([^"]*)"')
    for script in soup.select('script[type="application/ld+json"]'):
        raw_text = script.string or script.get_text()
        match = pattern.search(raw_text or "")
        if match:
            return html.unescape(match.group(1))
    return ""


def brand_from_product(product: dict[str, Any], fallback: str) -> str:
    brand = product.get("brand")
    if isinstance(brand, dict):
        return html_to_text(brand.get("name")) or fallback or "N/A"
    return html_to_text(brand) or fallback or "N/A"


def price_from_product(product: dict[str, Any], soup: BeautifulSoup, fallback: str) -> str:
    if fallback and fallback != "N/A":
        return fallback

    for form in soup.select(".product-main__wrapper form.product-form[data-price], form.product-form[data-price]"):
        price = normalize_invasion_price(form.get("data-price"))
        if price != "N/A":
            return price

    offers = product.get("offers")
    if isinstance(offers, dict) and offers.get("price") is not None:
        return normalize_invasion_price(offers.get("price"))
    if isinstance(offers, list):
        for offer in offers:
            if isinstance(offer, dict) and offer.get("price") is not None:
                return normalize_invasion_price(offer.get("price"))

    return fallback or "N/A"


def image_from_product(product: dict[str, Any], soup: BeautifulSoup, fallback: str) -> str:
    image = product.get("image") or script_field(soup, "image")
    if isinstance(image, list) and image:
        return absolute_url(BASE_URL, image[0])
    if isinstance(image, str) and image:
        return absolute_url(BASE_URL, image)
    return fallback or "N/A"


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
    if re.fullmatch(r"\d+\s*(?:gb|tb|mb|w|hz|mhz|ghz|dpi)", part_number, flags=re.IGNORECASE):
        return None
    if re.fullmatch(r"\d+\s*(?:mm|cm|in|inch|pulgadas?)", part_number, flags=re.IGNORECASE):
        return None

    return part_number


def code_from_text(value: Any, brand: str = "") -> str | None:
    text = html_to_text(value).upper()
    if not text:
        return None

    brand_text = html_to_text(brand).upper()
    if brand_text:
        for brand_word in brand_text.split():
            if len(brand_word) > 2:
                text = re.sub(rf"\b{re.escape(brand_word)}\b", " ", text)

    candidates = []
    inferred = infer_part_number_from_name(text)
    if inferred:
        candidates.append(inferred)
    candidates.extend(SHORT_MODEL_PATTERN.findall(text))

    for candidate in sorted(set(candidates), key=len, reverse=True):
        part_number = is_usable_part_number(candidate)
        if part_number:
            return part_number
    return None


def part_from_model_text(value: Any, brand: str = "") -> str | None:
    text = html_to_text(value)
    if not text:
        return None

    brand_text = html_to_text(brand)
    if brand_text:
        for brand_word in brand_text.split():
            if len(brand_word) > 2:
                text = re.sub(rf"\b{re.escape(brand_word)}\b", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip()

    whole_model = is_usable_part_number(text)
    if whole_model and len(whole_model.split()) <= 5:
        return whole_model

    return code_from_text(text)


def model_from_specs(soup: BeautifulSoup, brand: str) -> str | None:
    for row in soup.select("tr"):
        cells = [html_to_text(cell) for cell in row.find_all(["th", "td"])]
        if len(cells) >= 2 and "modelo" in cells[0].lower():
            return part_from_model_text(cells[1], brand)

        row_text = html_to_text(row)
        match = re.match(r"Modelo\s+(.+)$", row_text, flags=re.IGNORECASE)
        if match:
            return part_from_model_text(match.group(1), brand)
    return None


def code_from_pattern(name: str, pattern: re.Pattern[str]) -> str | None:
    for raw_candidate in reversed(pattern.findall(name or "")):
        candidate = is_usable_part_number(raw_candidate) or code_from_text(raw_candidate)
        if candidate:
            return candidate
    return None


def extract_part_number(
    product: dict[str, Any],
    soup: BeautifulSoup,
    name: str,
    brand: str,
    category_name: str,
) -> str | None:
    for value in (product.get("mpn"), product.get("sku"), script_field(soup, "sku")):
        part_number = is_usable_part_number(value)
        if part_number:
            return part_number

    spec_model = model_from_specs(soup, brand)
    if spec_model:
        return spec_model

    if category_name == "CPU":
        return None

    for extractor in (
        lambda: code_from_pattern(name, BRACKETED_CODE_PATTERN),
        lambda: code_from_pattern(name, PARENTHETICAL_CODE_PATTERN),
        lambda: code_from_text(name, brand),
    ):
        part_number = extractor()
        if part_number:
            return part_number

    return None


def parse_product(soup: BeautifulSoup, card_data: dict[str, str]) -> dict[str, Any] | None:
    product = product_json_ld(soup)
    name = html_to_text(product.get("name"))
    if not name:
        title = soup.select_one("h1")
        name = html_to_text(title)
    if not name:
        name = card_data["name"]
    if not name:
        return None

    brand = brand_from_product(product, card_data.get("brand", "N/A"))
    part_number = extract_part_number(product, soup, name, brand, card_data["category"])
    if not part_number:
        return None

    url = product.get("url") or card_data["url"]
    return {
        "store_name": "InvasionGamer",
        "scraped_name": name,
        "scraped_brand": brand,
        "type": card_data["category"],
        "part #": part_number,
        "price": price_from_product(product, soup, card_data.get("price", "N/A")),
        "url": url,
        "image_url": image_from_product(product, soup, card_data.get("image_url", "N/A")),
    }


def scrape_invasion_gamer() -> int:
    output_dir = "ScrapDB/Outputs/InvasionGamer"
    output_path = clean_output_dir(output_dir)
    session = make_session(BASE_URL)
    request_delay = float(os.environ.get("HTML_REQUEST_DELAY_SECONDS", "0.25"))
    max_products = configured_max_products()
    saved_count = 0
    skipped_without_part = 0
    seen_urls: set[str] = set()

    for category_name, category_url in CATEGORY_URL_MAP.items():
        try:
            first_html = fetch_text_with_referer(session, category_url, BASE_URL)
            first_soup = BeautifulSoup(first_html, "html.parser")
            page_numbers = page_numbers_from_soup(first_soup, "a[href*='page=']")
            total_pages = max(page_numbers) if page_numbers else 1

            for page in range(1, total_pages + 1):
                page_url = build_query_page_url(category_url, page, "page")
                if page == 1:
                    soup = first_soup
                else:
                    page_html = fetch_text_with_referer(session, page_url, category_url)
                    soup = BeautifulSoup(page_html, "html.parser")

                cards = product_cards_from_category(soup, category_name)
                print(f"InvasionGamer {category_name} page {page}/{total_pages}: {len(cards)} product cards")

                for card_data in cards:
                    url = card_data["url"]
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                    try:
                        product_html = fetch_text_with_referer(session, url, page_url)
                        product_soup = BeautifulSoup(product_html, "html.parser")
                        data = parse_product(product_soup, card_data)
                        if not data:
                            skipped_without_part += 1
                            continue

                        write_product_json(output_path, "IG", url, data)
                        saved_count += 1

                        if max_products and saved_count >= max_products:
                            print(
                                f"InvasionGamer reached INVASIONGAMER_MAX_PRODUCTS={max_products}; "
                                "stopping early."
                            )
                            print(
                                f"InvasionGamer scraping finished. Saved {saved_count} JSON files; "
                                f"skipped {skipped_without_part} products without usable part number."
                            )
                            return saved_count
                    except Exception as exc:
                        print(f"InvasionGamer {category_name}: error scraping product {url}: {exc}")

                if request_delay:
                    time.sleep(request_delay)
        except Exception as exc:
            print(f"InvasionGamer {category_name}: error scraping {category_url}: {exc}")

    print(
        f"InvasionGamer scraping finished. Saved {saved_count} JSON files; "
        f"skipped {skipped_without_part} products without usable part number."
    )
    return saved_count


def main() -> int:
    return exit_code_from_count(scrape_invasion_gamer())


if __name__ == "__main__":
    raise SystemExit(main())

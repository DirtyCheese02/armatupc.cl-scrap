from __future__ import annotations

import html
import os
import re
from typing import Any
from urllib.parse import urljoin

from api_scraper_utils import (
    absolute_url,
    brand_from_wc,
    build_woocommerce_page_url,
    clean_output_dir,
    clean_part_number,
    exit_code_from_count,
    fetch_json,
    first_image_from_wc,
    html_to_text,
    make_session,
    normalize_price,
    scrape_html_listing_categories,
    selected_attr,
    selected_text,
    write_product_json,
)
from scraper_health import write_scraper_health


BASE_URL = "https://www.tecnoshopping.cl"
API_PATHS = (
    "/wp-json/wc/store/v1/products",
    "/?rest_route=/wc/store/v1/products",
)
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

CATEGORY_HTML_URL_MAP = {
    "UPS": "https://www.tecnoshopping.cl/ups/",
    "Headphones": "https://www.tecnoshopping.cl/audifonos/",
    "Mouse": "https://www.tecnoshopping.cl/accesorios/mouse/",
    "Storage": [
        "https://www.tecnoshopping.cl/almacenamiento/discos-internos/",
        "https://www.tecnoshopping.cl/servidores/discos/",
    ],
    "Monitor": "https://www.tecnoshopping.cl/computacion/monitores/",
    "CPUCooler_CaseFan": [
        "https://www.tecnoshopping.cl/computacion/componentes/refrigeracion/",
        "https://www.tecnoshopping.cl/servidores/ventiladores/",
    ],
    "PowerSupply": "https://www.tecnoshopping.cl/computacion/componentes/fuentes-de-poder/",
    "Case": "https://www.tecnoshopping.cl/gabinetes/",
    "Memory": [
        "https://www.tecnoshopping.cl/computacion/componentes/memorias-ram/",
        "https://www.tecnoshopping.cl/servidores/memorias-servidores/",
    ],
    "CPU": "https://www.tecnoshopping.cl/computacion/componentes/procesadores/",
    "VideoCard": "https://www.tecnoshopping.cl/computacion/componentes/tarjetas-graficas/",
    "Motherboard": "https://www.tecnoshopping.cl/computacion/componentes/placas-madre/",
    "NetworkAdapter": "https://www.tecnoshopping.cl/servidores/tarjetas-de-red/",
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


def parse_tecnoshopping_html_product(soup, url: str, category_name: str, base_url: str) -> dict[str, Any] | None:
    name = selected_text(soup, ("h1.product_title", "h1.entry-title", "h1"))
    part_number = clean_tecnoshopping_part_number(
        selected_text(soup, (".product_meta .sku", ".sku", "[itemprop='sku']"))
    )
    if not name or not part_number:
        return None

    price = normalize_price(
        selected_text(
            soup,
            (
                ".summary .custom-price-display .woocommerce-Price-amount",
                ".summary .price_transferencia .woocommerce-Price-amount",
                ".summary .woocommerce-Price-amount",
            ),
        )
    )
    if price in {"N/A", "0"}:
        return None

    stock_text = selected_text(soup, (".summary .stock", "p.stock", ".stock"))
    unavailable = bool(re.search(r"sin existencias|agotad|sin stock", stock_text, re.IGNORECASE))
    image_url = selected_attr(
        soup,
        (".woocommerce-product-gallery__image img", "img.wp-post-image", "meta[property='og:image']"),
        "data-large_image",
    ) or selected_attr(
        soup,
        (".woocommerce-product-gallery__image img", "img.wp-post-image", "meta[property='og:image']"),
        "src",
    ) or selected_attr(soup, "meta[property='og:image']", "content")

    brand = "N/A"
    product_meta = soup.select_one(".product_meta")
    if product_meta is not None:
        meta_text = html_to_text(product_meta)
        match = re.search(r"Marca:\s*([^|]+)$", meta_text, re.IGNORECASE)
        if match:
            brand = html_to_text(match.group(1)) or "N/A"

    return {
        "store_name": "TecnoShopping",
        "scraped_name": name,
        "scraped_brand": brand,
        "type": category_name,
        "part #": part_number,
        "price": price,
        "availability": "unavailable" if unavailable else "available",
        "url": url,
        "image_url": absolute_url(base_url, image_url) or "N/A",
    }


def scrape_tecnoshopping_html(output_path, categories: set[str]) -> tuple[int, set[str], set[str]]:
    session = make_session(BASE_URL)
    completed: set[str] = set()
    failed: set[str] = set()
    saved_total = 0
    seen: set[tuple[str, str]] = set()
    for category in sorted(categories):
        saved = scrape_html_listing_categories(
            session=session,
            store_name="TecnoShopping",
            base_url=BASE_URL,
            category_url_map={category: CATEGORY_HTML_URL_MAP[category]},
            output_path=output_path,
            output_prefix="TSP",
            product_link_selectors=(
                "li.product a.woocommerce-LoopProduct-link[href]",
                ".products .product a.woocommerce-loop-product__link[href]",
            ),
            pagination_selectors=(".woocommerce-pagination a.page-numbers", "a.page-numbers"),
            page_url_builder=build_woocommerce_page_url,
            parse_product=parse_tecnoshopping_html_product,
            seen=seen,
        )
        saved_total += saved
        if saved:
            completed.add(category)
        else:
            failed.add(category)
    return saved_total, completed, failed


def _api_candidates() -> list[str]:
    return [urljoin(BASE_URL, path) for path in API_PATHS]


def _select_api_url(session: Any) -> str:
    errors = []
    for api_url in _api_candidates():
        try:
            payload, _ = fetch_json(
                session,
                api_url,
                params={"per_page": 1, "page": 1},
                retries=3,
                timeout=30,
            )
            if isinstance(payload, list):
                print(f"TecnoShopping API selected: {api_url}")
                return api_url
            errors.append(f"{api_url}: unexpected payload {type(payload).__name__}")
        except Exception as exc:
            errors.append(f"{api_url}: {exc}")
    raise RuntimeError("TecnoShopping APIs unavailable: " + " | ".join(errors))


def _fetch_product_page(
    session: Any,
    preferred_api_url: str,
    *,
    params: dict[str, Any],
) -> tuple[list[dict[str, Any]], Any, str]:
    candidates = [preferred_api_url, *[url for url in _api_candidates() if url != preferred_api_url]]
    errors = []
    for api_url in candidates:
        try:
            payload, response = fetch_json(
                session,
                api_url,
                params=params,
                retries=3,
                timeout=30,
            )
            if not isinstance(payload, list):
                raise RuntimeError(f"unexpected payload {type(payload).__name__}")
            return payload, response, api_url
        except Exception as exc:
            errors.append(f"{api_url}: {exc}")
    raise RuntimeError("TecnoShopping product page unavailable: " + " | ".join(errors))


def scrape_tecnoshopping() -> int:
    output_dir = "ScrapDB/Outputs/TecnoShopping"
    output_path = clean_output_dir(output_dir)
    session = make_session(BASE_URL)
    max_products = configured_max_products()
    saved_count = 0
    skipped_without_part = 0
    seen_urls: set[str] = set()
    expected_categories = set(CATEGORY_QUERIES)
    completed_categories: set[str] = set()
    failed_categories: set[str] = set()
    errors: list[dict[str, str]] = []

    try:
        api_url = _select_api_url(session)
    except Exception as exc:
        print(f"{exc}; TecnoShopping switching to public category HTML.")
        saved_count, completed_categories, failed_categories = scrape_tecnoshopping_html(
            output_path,
            expected_categories,
        )
        write_scraper_health(
            status="failed" if saved_count == 0 else ("partial_success" if failed_categories else "success"),
            expected_categories=expected_categories,
            completed_categories=completed_categories,
            failed_categories=failed_categories,
            product_count=saved_count,
            errors=({"category": "*", "error": str(exc)[:500]},),
            blocked_reason="store_api_and_html_unavailable" if saved_count == 0 else None,
        )
        return saved_count

    for category_name, query_list in CATEGORY_QUERIES.items():
        category_complete = True
        for query in query_list:
            page = 1
            while True:
                params = {"per_page": 100, "page": page, **query}
                try:
                    products, response, api_url = _fetch_product_page(
                        session,
                        api_url,
                        params=params,
                    )
                except Exception as exc:
                    category_complete = False
                    errors.append(
                        {
                            "category": category_name,
                            "error": str(exc)[:500],
                        }
                    )
                    print(f"TecnoShopping {category_name} page {page} failed: {exc}")
                    break

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
        if category_complete:
            completed_categories.add(category_name)
        else:
            failed_categories.add(category_name)

    if failed_categories:
        print(f"TecnoShopping retrying failed API categories through HTML: {sorted(failed_categories)}")
        fallback_count, fallback_completed, fallback_failed = scrape_tecnoshopping_html(
            output_path,
            set(failed_categories),
        )
        saved_count += fallback_count
        completed_categories.update(fallback_completed)
        failed_categories = fallback_failed

    print(
        f"TecnoShopping scraping finished. Saved {saved_count} JSON files; "
        f"skipped {skipped_without_part} products without usable part number."
    )
    write_scraper_health(
        status="failed" if saved_count == 0 else ("partial_success" if failed_categories else "success"),
        expected_categories=expected_categories,
        completed_categories=completed_categories,
        failed_categories=failed_categories,
        product_count=saved_count,
        errors=errors,
        blocked_reason="store_api_unavailable" if saved_count == 0 else None,
    )
    return saved_count


def main() -> int:
    return exit_code_from_count(scrape_tecnoshopping())


if __name__ == "__main__":
    raise SystemExit(main())

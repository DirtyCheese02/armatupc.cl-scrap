from __future__ import annotations

import html
import os
import re
import time
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from api_scraper_utils import (
    absolute_url,
    brand_from_wc,
    build_woocommerce_page_url,
    clean_output_dir,
    clean_part_number,
    exit_code_from_count,
    fetch_json,
    fetch_text_with_referer,
    first_image_from_wc,
    html_to_text,
    make_session,
    normalize_price,
    page_numbers_from_soup,
    product_links_from_soup,
    selected_attr,
    selected_text,
    write_product_json,
)
from browser_fallback_utils import browser_fallback_enabled, run_browser_fallback_store


BASE_URL = "https://infosep.cl"
CATEGORY_QUERIES = {
    "OperatingSystem": [{"category": 456}],
    "UPS": [{"category": 247}],
    "Headphones": [{"category": "147,184"}],
    "Mouse": [{"category": "156,158"}],
    "Keyboard": [{"category": "312,189"}],
    "Mouse_Keyboard": [{"category": "222,183"}],
    "Storage": [{"category": "404,191,255,378,406,407"}],
    "ExternalStorage": [{"category": "405,226"}],
    "Monitor": [{"category": "160,159"}],
    "CPUCooler": [{"category": "418,419"}],
    "ThermalCompound": [{"category": 248}],
    "PowerSupply": [{"category": "187,411,235"}],
    "Case": [{"category": "179,216"}],
    "Memory": [{"category": 129}],
    "CPU": [{"category": 509}],
    "Motherboard": [{"category": "208,415"}],
    "Webcam": [{"category": 215}],
    "NetworkAdapter": [{"category": "458,214"}],
}
CATEGORY_URL_MAP = {
    "OperatingSystem": "https://infosep.cl/categoria-producto/software/windows-server-2022-std-rock-ams-hp/",
    "UPS": "https://infosep.cl/categoria-producto/partes-y-piezas/ups-respaldo-de-energia/",
    "Headphones": [
        "https://infosep.cl/categoria-producto/accesorios/audifonos/",
        "https://infosep.cl/categoria-producto/gamer/audifonos-gamer/",
    ],
    "Mouse": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/mouse-2/",
        "https://infosep.cl/categoria-producto/gamer/mouse-gamer/",
    ],
    "Keyboard": [
        "https://infosep.cl/categoria-producto/accesorios/accesorios-de-escritorio/teclado/",
        "https://infosep.cl/categoria-producto/gamer/teclado-gamer/",
    ],
    "Mouse_Keyboard": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/kit-teclado-y-mouse/",
        "https://infosep.cl/categoria-producto/gamer/teclado-y-mouse-gamer/",
    ],
    "Storage": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/almacenamiento/disco-interno/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/disco-hdd/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/discos-ssd/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/discos-ssd-m2/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/almacenamiento/disco-vigilancia/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/memorias-sd/",
    ],
    "ExternalStorage": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/almacenamiento/disco-externo/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/discos-externos-25/",
    ],
    "Monitor": [
        "https://infosep.cl/categoria-producto/monitores/",
        "https://infosep.cl/categoria-producto/gamer/monitor-gamer/",
    ],
    "CPUCooler": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/partes-de-computador/tarjeta-madre/cooler-liquido/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/partes-de-computador/tarjeta-madre/ventilador-de-cpu/",
    ],
    "ThermalCompound": "https://infosep.cl/categoria-producto/accesorios/pasta-disipadora/",
    "PowerSupply": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/fuente-de-poder-pc/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/partes-de-computador/fuentes-de-poder/",
        "https://infosep.cl/categoria-producto/gamer/fuentes-gamer/",
    ],
    "Case": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/gabinetes/",
        "https://infosep.cl/categoria-producto/gamer/gabinetes-gamer/",
    ],
    "Memory": "https://infosep.cl/categoria-producto/partes-y-piezas/memorias-pc-notebook/",
    "CPU": "https://infosep.cl/categoria-producto/partes-y-piezas/procesadores/",
    "Motherboard": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/placas-madres/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/partes-de-computador/tarjeta-madre/tarjeta-madre-asus/",
    ],
    "Webcam": "https://infosep.cl/categoria-producto/accesorios/camara-web/",
    "NetworkAdapter": [
        "https://infosep.cl/categoria-producto/servidores/redes-servidores/adaptador-de-red/",
        "https://infosep.cl/categoria-producto/accesorios/adaptadores/",
    ],
}


def configured_max_products() -> int:
    raw_value = os.environ.get("INFOSEP_MAX_PRODUCTS") or os.environ.get("SCRAPER_MAX_PRODUCTS") or "0"
    try:
        return max(0, int(raw_value))
    except ValueError:
        return 0


def clean_infosep_part_number(value: Any) -> str | None:
    part_number = clean_part_number(value)
    if not part_number:
        return None

    part_number = html.unescape(part_number).replace("\u2011", "-").strip()
    part_number = re.sub(r"\s+", " ", part_number)

    internal_prefix = re.match(r"^\d{4,6}\s*[-\u2013\u2014]\s*(.+)$", part_number)
    if internal_prefix:
        part_number = re.sub(r"\s+", "", internal_prefix.group(1).strip())

    compact = re.sub(r"[^A-Za-z0-9]", "", part_number)
    if len(compact) < 3 or compact.isdigit():
        return None
    if part_number.upper() in {"N/A", "NA", "NONE", "NULL", "SIN SKU", "SKU NO INFORMADO"}:
        return None

    return part_number


def normalize_infosep_price(value: Any) -> str:
    text = html_to_text(value)
    if not text:
        return "N/A"

    prices: list[int] = []
    for match in re.finditer(r"\$\s*([0-9][0-9.\s,]*)", text):
        digits = re.sub(r"\D", "", match.group(1))
        if not digits:
            continue
        price = int(digits)
        if 0 < price < 100_000_000:
            prices.append(price)

    if prices:
        return str(min(prices))

    return normalize_price(value)


def product_to_output(product: dict[str, Any], category_name: str) -> dict[str, Any] | None:
    url = product.get("permalink") or ""
    name = html_to_text(product.get("name"))
    if not url or not name:
        return None

    part_number = clean_infosep_part_number(product.get("sku"))
    if not part_number:
        return None

    prices = product.get("prices") or {}
    return {
        "store_name": "InfoSep",
        "scraped_name": name,
        "scraped_brand": brand_from_wc(product),
        "type": category_name,
        "part #": part_number,
        "price": normalize_infosep_price(prices.get("price")),
        "url": url,
        "image_url": first_image_from_wc(product),
    }


def parse_infosep_html_product(
    soup: BeautifulSoup,
    url: str,
    category_name: str,
    base_url: str,
) -> dict[str, Any] | None:
    name = selected_text(soup, ("h1.product_title", "h1.entry-title", "h1"))
    if not name:
        return None

    sku = selected_text(soup, (".sku_wrapper .sku", "span.sku", ".product_meta .sku"))
    part_number = clean_infosep_part_number(sku)
    if not part_number:
        return None

    price = selected_text(
        soup,
        (
            "p.price ins .woocommerce-Price-amount",
            "p.price ins",
            ".summary .price ins .woocommerce-Price-amount",
            ".summary .price ins",
            "p.price .woocommerce-Price-amount",
            "p.price",
            ".summary .price",
            ".price",
        ),
    )
    image = selected_attr(
        soup,
        (
            ".woocommerce-product-gallery__image img",
            "img.wp-post-image",
            ".product-image-summary img",
        ),
        "src",
    )

    return {
        "store_name": "InfoSep",
        "scraped_name": name,
        "scraped_brand": "N/A",
        "type": category_name,
        "part #": part_number,
        "price": normalize_infosep_price(price),
        "url": url,
        "image_url": absolute_url(base_url, image),
    }


def scrape_infosep_html(output_dir: str) -> int:
    output_path = clean_output_dir(output_dir)
    session = make_session(BASE_URL)
    max_products = configured_max_products()
    saved_count = 0
    seen: set[tuple[str, str]] = set()
    request_delay = float(os.environ.get("HTML_REQUEST_DELAY_SECONDS", "0.25"))
    link_selectors = (
        "a.product-image-link[href*='/producto/']",
        "h3.wd-entities-title a[href*='/producto/']",
        ".product-grid-item a[href*='/producto/']",
    )
    pagination_selectors = (
        "nav.woocommerce-pagination a",
        "ul.page-numbers a",
        ".page-numbers a",
    )

    for category_name, raw_urls in CATEGORY_URL_MAP.items():
        urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
        for category_url in urls:
            try:
                first_html = fetch_text_with_referer(session, category_url, BASE_URL)
                first_soup = BeautifulSoup(first_html, "html.parser")
                page_numbers = page_numbers_from_soup(first_soup, pagination_selectors)
                total_pages = max(page_numbers) if page_numbers else 1

                for page in range(1, total_pages + 1):
                    page_url = build_woocommerce_page_url(category_url, page)
                    if page == 1:
                        soup = first_soup
                    else:
                        page_html = fetch_text_with_referer(session, page_url, category_url)
                        soup = BeautifulSoup(page_html, "html.parser")

                    links = product_links_from_soup(
                        soup,
                        BASE_URL,
                        link_selectors,
                        url_pattern=r"/producto/",
                    )
                    print(
                        f"InfoSep {category_name} HTML page {page}/{total_pages}: "
                        f"{len(links)} product links"
                    )

                    for url in links:
                        identity = (category_name, url)
                        if identity in seen:
                            continue
                        seen.add(identity)

                        try:
                            product_html = fetch_text_with_referer(session, url, page_url)
                            product_soup = BeautifulSoup(product_html, "html.parser")
                            data = parse_infosep_html_product(
                                product_soup,
                                url,
                                category_name,
                                BASE_URL,
                            )
                            if not data:
                                continue

                            write_product_json(output_path, "IS", url, data)
                            saved_count += 1

                            if max_products and saved_count >= max_products:
                                print(f"InfoSep reached INFOSEP_MAX_PRODUCTS={max_products}; stopping early.")
                                return saved_count
                        except Exception as exc:
                            print(f"InfoSep {category_name}: error scraping product {url}: {exc}")

                    if request_delay:
                        time.sleep(request_delay)
            except Exception as exc:
                print(f"InfoSep {category_name}: HTML fallback failed for {category_url}: {exc}")

    return saved_count


def scrape_infosep_requests(output_dir: str) -> int:
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
                    raise RuntimeError(f"Unexpected InfoSep response: {products!r}")

                total_pages = int(response.headers.get("X-WP-TotalPages", "1") or "1")
                print(f"InfoSep {category_name} page {page}/{total_pages}: {len(products)} products")

                for product in products:
                    url = product.get("permalink") or ""
                    if not url or url in seen_urls:
                        continue

                    data = product_to_output(product, category_name)
                    if not data:
                        skipped_without_part += 1
                        continue

                    seen_urls.add(url)
                    write_product_json(output_path, "IS", url, data)
                    saved_count += 1

                    if max_products and saved_count >= max_products:
                        print(f"InfoSep reached INFOSEP_MAX_PRODUCTS={max_products}; stopping early.")
                        print(
                            f"InfoSep scraping finished. Saved {saved_count} JSON files; "
                            f"skipped {skipped_without_part} products without usable part number."
                        )
                        return saved_count

                if page >= total_pages:
                    break
                page += 1

    print(
        f"InfoSep scraping finished. Saved {saved_count} JSON files; "
        f"skipped {skipped_without_part} products without usable part number."
    )
    return saved_count


def main() -> int:
    output_dir = "ScrapDB/Outputs/InfoSep"
    force_browser = os.environ.get("SCRAPER_FORCE_BROWSER_FALLBACK", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    if force_browser:
        saved_count = 0
        print("InfoSep requests and HTML paths skipped; browser fallback will be used.")
    else:
        try:
            saved_count = scrape_infosep_requests(output_dir)
            print(f"InfoSep requests path saved {saved_count} JSON files.")
        except Exception as exc:
            saved_count = 0
            print(f"InfoSep requests path failed, HTML fallback will be tried: {exc}")

        if saved_count == 0:
            try:
                saved_count = scrape_infosep_html(output_dir)
                print(f"InfoSep HTML fallback saved {saved_count} JSON files.")
            except Exception as exc:
                saved_count = 0
                print(f"InfoSep HTML fallback failed, browser fallback will be tried: {exc}")

    if browser_fallback_enabled(saved_count):
        print("InfoSep starting browser fallback.")
        saved_count = run_browser_fallback_store(
            store_name="InfoSep",
            category_url_map=CATEGORY_URL_MAP,
            output_dir=output_dir,
            output_prefix="IS",
            listing_config={
                "link_selector": (
                    "//a[contains(@href,'/producto/') and "
                    "(contains(concat(' ', normalize-space(@class), ' '), ' product-image-link ') or "
                    "ancestor::h3[contains(concat(' ', normalize-space(@class), ' '), ' wd-entities-title ')])]"
                ),
                "pagination_selector": (
                    "//nav[contains(@class,'woocommerce-pagination')]//a|"
                    "//ul[contains(@class,'page-numbers')]//a|"
                    "//*[contains(concat(' ', normalize-space(@class), ' '), ' page-numbers ')]//a"
                ),
                "page_url_builder": build_woocommerce_page_url,
                "ready_selectors": (
                    "//*[contains(concat(' ', normalize-space(@class), ' '), ' products ')]"
                    "//*[contains(concat(' ', normalize-space(@class), ' '), ' product ')]",
                    "//*[contains(concat(' ', normalize-space(@class), ' '), ' product-grid-item ')]",
                    "//a[contains(@href,'/producto/') and contains(concat(' ', normalize-space(@class), ' '), ' product-image-link ')]",
                ),
            },
            product_config={
                "ready_selectors": (
                    "//h1[contains(@class,'product_title')]",
                    "//h1[contains(@class,'entry-title')]",
                    "//span[contains(concat(' ', normalize-space(@class), ' '), ' sku ')]",
                ),
                "name_selectors": (
                    "//h1[contains(@class,'product_title')]",
                    "//h1[contains(@class,'entry-title')]",
                    "//h1",
                ),
                "part_selectors": (
                    "//*[contains(concat(' ', normalize-space(@class), ' '), ' sku_wrapper ')]"
                    "//*[contains(concat(' ', normalize-space(@class), ' '), ' sku ')]",
                    "//span[contains(concat(' ', normalize-space(@class), ' '), ' sku ')]",
                    "//*[contains(concat(' ', normalize-space(@class), ' '), ' product_meta ')]"
                    "//*[contains(concat(' ', normalize-space(@class), ' '), ' sku ')]",
                ),
                "price_selectors": (
                    "//p[contains(@class,'price')]//ins//*[contains(@class,'woocommerce-Price-amount')]",
                    "//p[contains(@class,'price')]//ins",
                    "//*[contains(@class,'summary')]//*[contains(@class,'price')]//ins//*[contains(@class,'woocommerce-Price-amount')]",
                    "//*[contains(@class,'summary')]//*[contains(@class,'price')]//ins",
                    "//p[contains(@class,'price')]//*[contains(@class,'woocommerce-Price-amount')]",
                    "//p[contains(@class,'price')]",
                    "//*[contains(@class,'summary')]//*[contains(@class,'price')]",
                    "//*[contains(@class,'price')]",
                ),
                "image_selectors": (
                    "//*[contains(@class,'woocommerce-product-gallery__image')]//img",
                    "//img[contains(@class,'wp-post-image')]",
                    "//*[contains(@class,'product-image-summary')]//img",
                ),
                "brand_selectors": (),
                "clean_part_number": clean_infosep_part_number,
                "clean_price": normalize_infosep_price,
            },
        )
        print(f"InfoSep browser fallback saved {saved_count} JSON files.")

    print(f"InfoSep scraping finished. Saved {saved_count} JSON files.")
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import html
import os
import re
from typing import Any
from urllib.parse import urljoin

from api_scraper_utils import (
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
        "price": normalize_price(prices.get("price")),
        "url": url,
        "image_url": first_image_from_wc(product),
    }


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
    running_headful = os.environ.get("SCRAP_HEADLESS", "1").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }

    if force_browser or running_headful:
        saved_count = 0
        print("InfoSep requests path skipped; browser fallback will be used.")
    else:
        try:
            saved_count = scrape_infosep_requests(output_dir)
            print(f"InfoSep requests path saved {saved_count} JSON files.")
        except Exception as exc:
            saved_count = 0
            print(f"InfoSep requests path failed, browser fallback will be tried: {exc}")

    if browser_fallback_enabled(saved_count):
        print("InfoSep starting browser fallback.")
        saved_count = run_browser_fallback_store(
            store_name="InfoSep",
            category_url_map=CATEGORY_URL_MAP,
            output_dir=output_dir,
            output_prefix="IS",
            listing_config={
                "link_selector": (
                    ".products .product a.product-image-link[href*='/producto/'], "
                    ".products .product .wd-entities-title a[href*='/producto/']"
                ),
                "pagination_selector": (
                    "nav.woocommerce-pagination a, ul.page-numbers a, .page-numbers a"
                ),
                "page_url_builder": build_woocommerce_page_url,
                "ready_selectors": (
                    ".products .product",
                    ".product-grid-item",
                    "a.product-image-link[href*='/producto/']",
                ),
            },
            product_config={
                "ready_selectors": ("h1.product_title", "h1.entry-title", ".sku_wrapper .sku", "span.sku"),
                "name_selectors": ("h1.product_title", "h1.entry-title", "h1"),
                "part_selectors": (".sku_wrapper .sku", "span.sku", ".product_meta .sku"),
                "price_selectors": ("p.price", ".summary .price", ".price"),
                "image_selectors": (
                    ".woocommerce-product-gallery__image img",
                    "img.wp-post-image",
                    ".product-image-summary img",
                ),
                "brand_selectors": (),
                "clean_part_number": clean_infosep_part_number,
                "clean_price": normalize_price,
            },
        )
        print(f"InfoSep browser fallback saved {saved_count} JSON files.")

    print(f"InfoSep scraping finished. Saved {saved_count} JSON files.")
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

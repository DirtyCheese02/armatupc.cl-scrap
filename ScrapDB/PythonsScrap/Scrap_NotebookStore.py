from __future__ import annotations

import os
import time
from typing import Any

from bs4 import BeautifulSoup

from api_scraper_utils import (
    absolute_url,
    build_query_page_url,
    clean_output_dir,
    clean_part_number,
    exit_code_from_count,
    fetch_text_with_referer,
    html_to_text,
    make_session,
    normalize_price,
    page_numbers_from_soup,
    selected_attr,
    selected_text,
    write_product_json,
)


BASE_URL = "https://notebookstore.cl"

CATEGORY_URL_MAP = {
    "OperatingSystem": "https://notebookstore.cl/software-servicios/software/sistema-operativo",
    "UPS": "https://notebookstore.cl/ups/respaldo-de-energia",
    "Headphones": "https://notebookstore.cl/audio-video-y-foto/audio-y-video/audifonos-y-headset",
    "Mouse": "https://notebookstore.cl/equipos/perifericos/mouse",
    "Keyboard": "https://notebookstore.cl/equipos/perifericos/teclados",
    "Mouse_Keyboard": "https://notebookstore.cl/equipos/perifericos/combos-de-teclado-y-mouse",
    "Storage": [
        "https://notebookstore.cl/equipos/almacenamiento/discos-de-estado-solido",
        "https://notebookstore.cl/equipos/almacenamiento/discos-duros-internos",
    ],
    "ExternalStorage": "https://notebookstore.cl/equipos/almacenamiento/discos-duros-externos",
    "Monitor": "https://notebookstore.cl/audio-video-y-foto/monitores-proyectores/monitores",
    "CPUCooler_CaseFan": (
        "https://notebookstore.cl/equipos/componentes-informaticos/"
        "ventiladores-y-sistemas-de-enfriamiento"
    ),
    "PowerSupply": "https://notebookstore.cl/equipos/componentes-informaticos/fuentes-de-poder",
    "Case": "https://notebookstore.cl/cajas/gabinetes",
    "Memory": [
        "https://notebookstore.cl/equipos/memorias/ram-para-pc-y-servidores",
        "https://notebookstore.cl/equipos/memorias/ram-para-notebooks",
    ],
    "CPU": "https://notebookstore.cl/equipos/componentes-informaticos/procesadores",
    "VideoCard": "https://notebookstore.cl/equipos/componentes-informaticos/tarjetas-de-video",
    "Motherboard": "https://notebookstore.cl/equipos/componentes-informaticos/tarjetas-y-placas-madre",
    "Webcam": "https://notebookstore.cl/audio-video-y-foto/camaras-videocamaras/camaras-web",
    "NetworkAdapter": "https://notebookstore.cl/redes/redes/adaptadores-y-controladoras",
}

EMPTY_SKU_VALUES = {"", "N/A", "NA", "NONE", "NULL", "SIN SKU", "SKU NO INFORMADO"}


def configured_max_products() -> int:
    raw_value = os.environ.get("NOTEBOOKSTORE_MAX_PRODUCTS") or os.environ.get("SCRAPER_MAX_PRODUCTS") or "0"
    try:
        return max(0, int(raw_value))
    except ValueError:
        return 0


def first_srcset_url(value: str) -> str:
    if not value:
        return ""
    return value.split(",")[0].strip().split(" ")[0].strip()


def image_from_card(card: Any) -> str:
    image_url = selected_attr(card, ("img.product-block__image", "img"), "src")
    if not image_url:
        image_url = first_srcset_url(selected_attr(card, ("source[srcset]", "img[srcset]"), "srcset"))
    return absolute_url(BASE_URL, image_url) if image_url else "N/A"


def parse_product_card(card: Any, category_name: str) -> dict[str, Any] | None:
    part_number = clean_part_number(selected_text(card, ".product-block__sku"))
    if not part_number or part_number.upper() in EMPTY_SKU_VALUES:
        return None

    name = selected_text(card, ".product-block__name")
    url = absolute_url(
        BASE_URL,
        selected_attr(card, ("a.product-block__anchor", "a.product-block__name", "a[href]"), "href"),
    )
    if not name or url == "N/A":
        return None

    brand = selected_text(card, ".product-block__brand") or "N/A"
    price = normalize_price(selected_text(card, ".product-block__price"))

    return {
        "store_name": "NotebookStore",
        "scraped_name": name,
        "scraped_brand": brand,
        "type": category_name,
        "part #": part_number,
        "price": price,
        "url": url,
        "image_url": image_from_card(card),
    }


def category_urls(raw_urls: Any) -> list[str]:
    if isinstance(raw_urls, list):
        return raw_urls
    return [raw_urls]


def scrape_notebook_store() -> int:
    output_dir = "ScrapDB/Outputs/NotebookStore"
    output_path = clean_output_dir(output_dir)
    session = make_session(BASE_URL)
    request_delay = float(os.environ.get("HTML_REQUEST_DELAY_SECONDS", "0.25"))
    max_products = configured_max_products()
    saved_count = 0
    skipped_without_part = 0
    seen_urls: set[str] = set()

    for category_name, raw_urls in CATEGORY_URL_MAP.items():
        for category_url in category_urls(raw_urls):
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

                    cards = soup.select(".product-block")
                    print(
                        f"NotebookStore {category_name} page {page}/{total_pages}: "
                        f"{len(cards)} product cards"
                    )

                    for card in cards:
                        data = parse_product_card(card, category_name)
                        if not data:
                            skipped_without_part += 1
                            continue

                        url = data["url"]
                        if url in seen_urls:
                            continue
                        seen_urls.add(url)

                        write_product_json(output_path, "NBS", url, data)
                        saved_count += 1

                        if max_products and saved_count >= max_products:
                            print(
                                f"NotebookStore reached NOTEBOOKSTORE_MAX_PRODUCTS={max_products}; "
                                "stopping early."
                            )
                            print(
                                f"NotebookStore scraping finished. Saved {saved_count} JSON files; "
                                f"skipped {skipped_without_part} products without part number."
                            )
                            return saved_count

                    if request_delay:
                        time.sleep(request_delay)
            except Exception as exc:
                print(f"NotebookStore {category_name}: error scraping {category_url}: {exc}")

    print(
        f"NotebookStore scraping finished. Saved {saved_count} JSON files; "
        f"skipped {skipped_without_part} products without part number."
    )
    return saved_count


def main() -> int:
    return exit_code_from_count(scrape_notebook_store())


if __name__ == "__main__":
    raise SystemExit(main())

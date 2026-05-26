from __future__ import annotations

import os
import time
from pathlib import Path

from bs4 import BeautifulSoup

from api_scraper_utils import (
    absolute_url,
    build_query_page_url,
    clean_output_dir,
    exit_code_from_count,
    fetch_text_with_referer,
    make_session,
    normalize_price,
    pick_part_number,
    product_links_from_soup,
    selected_attr,
    selected_text,
    write_product_json,
)


CATEGORY_URL_MAP = {
    "OperatingSystem": [
        "https://tecnomas.cl/productos/categorias/Microsoft",
        "https://tecnomas.cl/productos/categorias/Software",
    ],
    "UPS": "https://tecnomas.cl/productos/categorias/UPS",
    "Headphones": "https://tecnomas.cl/productos/categorias/Audio",
    "Mouse_Keyboard": "https://tecnomas.cl/productos/categorias/Teclados y Mouse",
    "Storage_ExternalStorage": [
        "https://tecnomas.cl/productos/categorias/Almacenamiento",
        "https://tecnomas.cl/productos/categorias/Almacenamiento Externo",
    ],
    "Monitor": "https://tecnomas.cl/productos/categorias/Monitores",
    "PowerSupply": "https://tecnomas.cl/productos/categorias/Fuentes de Poder",
    "Case": "https://tecnomas.cl/productos/categorias/Gabinetes",
    "Memory": "https://tecnomas.cl/productos/categorias/RAM",
    "CPU": "https://tecnomas.cl/productos/categorias/Procesadores",
    "VideoCard": "https://tecnomas.cl/productos/categorias/Tarjetas de Video",
    "Motherboard": "https://tecnomas.cl/productos/categorias/Placas Madre",
    "Webcam": "https://tecnomas.cl/productos/categorias/Webcam",
    "NetworkAdapter": "https://tecnomas.cl/productos/categorias/Tarjetas de Red",
    "CPUCooler_CaseFan": "https://tecnomas.cl/productos/categorias/Ventiladores y Sistemas de Enfriamiento",
}


def configured_max_pages() -> int:
    raw_value = os.environ.get("TECNOMAS_MAX_PAGES") or "100"
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 100


def parse_tecnomas_product(soup, url: str, category_name: str, base_url: str):
    name = selected_text(soup, ("h1[id^='name-']", "h1"))
    sku = selected_text(soup, ("h2[id^='sku-']",))
    part_number = pick_part_number([sku], [name])
    if not name or not part_number:
        return None

    image = selected_attr(
        soup,
        (
            "div.swiper-zoom-container img",
            ".swiper-slide-active img",
            "img",
        ),
        "src",
    )

    return {
        "store_name": "TecnoMas",
        "scraped_name": name,
        "scraped_brand": selected_text(soup, ("a[id^='brand-']",)) or "N/A",
        "type": category_name,
        "part #": part_number,
        "price": normalize_price(selected_text(soup, ("span[id^='wire-transfer-price-']",))),
        "url": url,
        "image_url": absolute_url(base_url, image),
    }


def scrape_tecnomas_categories(
    *,
    session,
    base_url: str,
    output_path: str | Path,
) -> int:
    saved_count = 0
    seen: set[tuple[str, str]] = set()
    request_delay = float(os.environ.get("HTML_REQUEST_DELAY_SECONDS", "0.25"))
    max_pages = configured_max_pages()

    for category_name, raw_urls in CATEGORY_URL_MAP.items():
        urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
        for category_url in urls:
            previous_page_links: set[str] = set()

            for page in range(1, max_pages + 1):
                page_url = build_query_page_url(category_url, page, "pagina")
                try:
                    html_content = fetch_text_with_referer(session, page_url, category_url if page > 1 else base_url)
                    soup = BeautifulSoup(html_content, "html.parser")
                    links = product_links_from_soup(
                        soup,
                        base_url,
                        "a[href*='/producto/']",
                        url_pattern=r"/producto/",
                    )
                except Exception as exc:
                    print(f"TecnoMas {category_name}: error scraping page {page_url}: {exc}")
                    break

                current_page_links = set(links)
                print(f"TecnoMas {category_name} HTML page {page}: {len(links)} product links")

                if not links:
                    break
                if page > 1 and current_page_links == previous_page_links:
                    print(f"TecnoMas {category_name} HTML page {page}: duplicate page detected; stopping.")
                    break

                previous_page_links = current_page_links

                for url in links:
                    identity = (category_name, url)
                    if identity in seen:
                        continue
                    seen.add(identity)

                    try:
                        product_html = fetch_text_with_referer(session, url, page_url)
                        product_soup = BeautifulSoup(product_html, "html.parser")
                        data = parse_tecnomas_product(product_soup, url, category_name, base_url)
                        if not data:
                            continue
                        write_product_json(output_path, "TM", url, data)
                        saved_count += 1
                    except Exception as exc:
                        print(f"TecnoMas {category_name}: error scraping product {url}: {exc}")

                if request_delay:
                    time.sleep(request_delay)

            else:
                print(f"TecnoMas {category_name}: reached TECNOMAS_MAX_PAGES={max_pages}.")

    return saved_count


def main() -> int:
    output_dir = "ScrapDB/Outputs/TecnoMas"
    output_path = clean_output_dir(output_dir)
    session = make_session("https://tecnomas.cl")
    saved_count = scrape_tecnomas_categories(
        session=session,
        base_url="https://tecnomas.cl",
        output_path=output_path,
    )
    print(f"TecnoMas scraping finished. Saved {saved_count} JSON files.")
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

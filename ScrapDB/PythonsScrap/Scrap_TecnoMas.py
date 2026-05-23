from __future__ import annotations

from api_scraper_utils import (
    absolute_url,
    build_query_page_url,
    clean_output_dir,
    exit_code_from_count,
    make_session,
    normalize_price,
    pick_part_number,
    scrape_html_listing_categories,
    selected_attr,
    selected_text,
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


def main() -> int:
    output_dir = "ScrapDB/Outputs/TecnoMas"
    output_path = clean_output_dir(output_dir)
    session = make_session("https://tecnomas.cl")
    saved_count = scrape_html_listing_categories(
        session=session,
        store_name="TecnoMas",
        base_url="https://tecnomas.cl",
        category_url_map=CATEGORY_URL_MAP,
        output_path=output_path,
        output_prefix="TM",
        product_link_selectors="a[href*='/producto/']",
        pagination_selectors=("button", "a[href*='pagina=']"),
        page_url_builder=lambda url, page: build_query_page_url(url, page, "pagina"),
        parse_product=parse_tecnomas_product,
        product_url_pattern=r"/producto/",
    )
    print(f"TecnoMas scraping finished. Saved {saved_count} JSON files.")
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

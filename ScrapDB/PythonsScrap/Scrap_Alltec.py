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


BASE_URL = "https://www.alltec.cl"

CATEGORY_URL_MAP = {
    "OperatingSystem": "https://www.alltec.cl/86-sistemas-operativos",
    "UPS": "https://www.alltec.cl/122-ups",
    "Mouse": [
        "https://www.alltec.cl/24-mouse",
        "https://www.alltec.cl/70-gamer",
        "https://www.alltec.cl/69-inalambrico",
    ],
    "Keyboard": [
        "https://www.alltec.cl/60-estandar",
        "https://www.alltec.cl/62-gamer",
        "https://www.alltec.cl/61-inalambricos",
    ],
    "Storage": "https://www.alltec.cl/34-ssd",
    "ExternalStorage": "https://www.alltec.cl/59-memorias-flash-microsdsdcompac-flash",
    "Monitor": "https://www.alltec.cl/27-monitores",
    "CPUCooler": "https://www.alltec.cl/93-cpu-cooler",
    "CaseFan": "https://www.alltec.cl/91-chassis-fan-ventiladores",
    "PowerSupply": [
        "https://www.alltec.cl/18-fuentes-de-poder",
        "https://www.alltec.cl/80-potencia-realcertificadas",
    ],
    "Case": [
        "https://www.alltec.cl/16-gabinetes",
        "https://www.alltec.cl/81-sin-fuente-de-poder",
    ],
    "Memory": [
        "https://www.alltec.cl/37-ddr4",
        "https://www.alltec.cl/117-ddr5",
    ],
    "CPU": [
        "https://www.alltec.cl/29-intel",
        "https://www.alltec.cl/28-amd",
    ],
    "VideoCard": [
        "https://www.alltec.cl/63-amd",
        "https://www.alltec.cl/64-nvidia",
    ],
    "Motherboard": [
        "https://www.alltec.cl/17-placas-madre",
        "https://www.alltec.cl/31-para-amd",
        "https://www.alltec.cl/79-para-intel",
    ],
    "Webcam": "https://www.alltec.cl/97-webcam",
    "NetworkAdapter": "https://www.alltec.cl/49-tarjetas-de-red-pci-pcie-usb-wireless",
}


def parse_alltec_product(soup, url: str, category_name: str, base_url: str) -> dict | None:
    name = selected_text(soup, ("h1[itemprop='name']", "h1"))
    raw_part_number = selected_text(soup, ("#product_reference span[itemprop='sku']", "#product_reference"))
    part_number = pick_part_number([raw_part_number], [name], allow_name_fallback=True)
    if not name or not part_number:
        return None

    image_url = selected_attr(
        soup,
        (
            "#bigpic",
            "#image-block img",
            ".pb-left-column img",
            "img[itemprop='image']",
        ),
        "src",
    )

    return {
        "store_name": "Alltec",
        "scraped_name": name,
        "scraped_brand": "N/A",
        "type": category_name,
        "part #": part_number,
        "price": normalize_price(selected_text(soup, ("#our_price_display", ".price[itemprop='price']", ".price"))),
        "url": url,
        "image_url": absolute_url(base_url, image_url),
    }


def main() -> int:
    output_dir = "ScrapDB/Outputs/Alltec"
    output_path = clean_output_dir(output_dir)
    saved_count = scrape_html_listing_categories(
        session=make_session(BASE_URL),
        store_name="Alltec",
        base_url=BASE_URL,
        category_url_map=CATEGORY_URL_MAP,
        output_path=output_path,
        output_prefix="ALT",
        product_link_selectors=(
            "#center_column ul.product_list a.product-name",
            "#center_column .product-container a.product-name",
        ),
        pagination_selectors=("#pagination a", "ul.pagination a", ".pagination a"),
        page_url_builder=lambda url, page: build_query_page_url(url, page, "p"),
        parse_product=parse_alltec_product,
        product_url_pattern=r"/[^/]+/\d+-",
    )
    print(f"Alltec scraping finished. Saved {saved_count} JSON files.")
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

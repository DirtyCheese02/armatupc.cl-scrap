from __future__ import annotations

from api_scraper_utils import (
    absolute_url,
    build_query_page_url,
    exit_code_from_count,
    normalize_price,
    pick_part_number,
    run_prestashop_xhr_store,
    selected_attr,
    selected_text,
)


CATEGORY_URL_MAP = {
    "OperatingSystem": "https://mybox.cl/29-software",
    "UPS": "https://mybox.cl/91-respaldo-energetico-ups",
    "Headphones": "https://mybox.cl/21-audifonos-headset",
    "Mouse_Keyboard": "https://mybox.cl/20-teclados-mouse",
    "Storage": "https://mybox.cl/67-almacenamiento",
    "Monitor": "https://mybox.cl/28-pantallas-y-monitores",
    "CPUCooler": "https://mybox.cl/92-enfriamiento-refrigeracion",
    "CaseFan": "https://mybox.cl/89-ventiladores-fans",
    "PowerSupply": "https://mybox.cl/63-fuentes-de-poder",
    "Case": "https://mybox.cl/62-gabinetes",
    "Memory": "https://mybox.cl/66-memoria-ram",
    "CPU": "https://mybox.cl/64-procesador",
    "VideoCard": "https://mybox.cl/68-tarjeta-de-video",
    "Motherboard": "https://mybox.cl/65-placa-madre",
    "Webcam": "https://mybox.cl/23-webcam",
    "NetworkAdapter": "https://mybox.cl/90-redes-conectividad?q=Categor%C3%ADas-Adaptadores+de+Red",
}


def parse_mybox_product(soup, url: str, category_name: str, base_url: str):
    name = selected_text(
        soup,
        (
            "h1[itemprop='name'] span",
            "h1[itemprop='name']",
            "h1.h1",
            "h1",
        ),
    )
    if not name:
        return None

    sku = selected_text(
        soup,
        (
            "span[itemprop='sku']",
            ".product-reference span",
            ".product-reference",
        ),
    )
    part_number = pick_part_number([sku], [name], allow_name_fallback=False) or "N/A"
    image = selected_attr(
        soup,
        (
            "div.swiper-slide-active img",
            ".product-cover img",
            "img.js-qv-product-cover",
            ".images-container img",
            "img[itemprop='image']",
        ),
        "src",
    )

    return {
        "store_name": "MyBox",
        "scraped_name": name,
        "scraped_brand": "N/A",
        "type": category_name,
        "part #": part_number,
        "price": normalize_price(
            selected_text(
                soup,
                (
                    "span.product-price",
                    ".current-price span[itemprop='price']",
                    ".current-price .product-price",
                    ".product-prices .price",
                ),
            )
        ),
        "url": url,
        "image_url": absolute_url(base_url, image),
    }


def main() -> int:
    output_dir = "ScrapDB/Outputs/MyBox"
    saved_count = run_prestashop_xhr_store(
        store_name="MyBox",
        base_url="https://mybox.cl",
        category_url_map=CATEGORY_URL_MAP,
        output_dir=output_dir,
        output_prefix="MyB",
        html_fallback_config={
            "product_link_selectors": (
                "div.products div.product a.product-thumbnail",
                "article.product-miniature a.product-thumbnail",
                "a.thumbnail.product-thumbnail",
                ".product-miniature a[href]",
            ),
            "pagination_selectors": (
                "nav.pagination a",
                "ul.page-list a",
                "a.js-search-link",
            ),
            "page_url_builder": lambda url, page: build_query_page_url(url, page, "page"),
            "parse_product": parse_mybox_product,
            "product_url_pattern": r"\.html(?:$|\?)",
        },
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

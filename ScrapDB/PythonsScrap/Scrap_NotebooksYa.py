from __future__ import annotations

import os

from api_scraper_utils import (
    absolute_url,
    build_woocommerce_page_url,
    exit_code_from_count,
    normalize_price,
    pick_part_number,
    run_woocommerce_store,
    selected_attr,
    selected_text,
)
from browser_fallback_utils import browser_fallback_enabled, run_browser_fallback_store


PARTES_Y_PIEZAS_ATTRIBUTE = "pa_producto-partes-y-piezas"

CATEGORY_QUERIES = {
    "OperatingSystem": [{"category": 709}],
    "UPS": [{"category": 710}],
    "Headphones": [{"category": 701}],
    "Mouse_Keyboard": [{"category": 706}],
    "Storage_ExternalStorage": [{"category": 689}],
    "Monitor": [{"category": 723}],
    "CPUCooler": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "refrigeracion",
        }
    ],
    "PowerSupply": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "fuente-de-poder",
        }
    ],
    "Case": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "gabinetes",
        }
    ],
    "Memory": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "memoria-ram-para-pc",
        }
    ],
    "CPU": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "procesadores",
        }
    ],
    "VideoCard": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "tarjeta-de-video",
        }
    ],
    "Motherboard": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "placa-madre",
        }
    ],
    "Webcam": [{"category": 1505}],
}

CATEGORY_LISTING_URLS = {
    "OperatingSystem": "https://notebooksya.cl/product-category/software-ya/",
    "UPS": "https://notebooksya.cl/product-category/ups-ya/",
    "Headphones": "https://notebooksya.cl/product-category/audio-y-video-ya/audifonos-ya/",
    "Mouse_Keyboard": "https://notebooksya.cl/product-category/teclados-mouse-ya/",
    "Storage_ExternalStorage": "https://notebooksya.cl/product-category/almacenamiento-ya/",
    "Monitor": "https://notebooksya.cl/product-category/monitores-gamer/",
    "CPUCooler": "https://notebooksya.cl/product-category/partes-y-piezas-ya/?filter_producto-partes-y-piezas=refrigeracion",
    "PowerSupply": "https://notebooksya.cl/product-category/partes-y-piezas-ya/?filter_producto-partes-y-piezas=fuente-de-poder",
    "Case": "https://notebooksya.cl/product-category/partes-y-piezas-ya/?filter_producto-partes-y-piezas=gabinetes",
    "Memory": "https://notebooksya.cl/product-category/partes-y-piezas-ya/?filter_producto-partes-y-piezas=memoria-ram-para-pc",
    "CPU": "https://notebooksya.cl/product-category/partes-y-piezas-ya/?filter_producto-partes-y-piezas=procesadores",
    "VideoCard": "https://notebooksya.cl/product-category/partes-y-piezas-ya/?filter_producto-partes-y-piezas=tarjeta-de-video",
    "Motherboard": "https://notebooksya.cl/product-category/partes-y-piezas-ya/?filter_producto-partes-y-piezas=placa-madre",
    "Webcam": "https://notebooksya.cl/product-category/audio-y-video-ya/webcam-audio-y-video-ya/",
}


def clean_notebooksya_part_number(value: str) -> str:
    return pick_part_number([value], (value,), allow_name_fallback=False) or ""


def parse_notebooksya_product(soup, url: str, category_name: str, base_url: str):
    name = selected_text(soup, ("h1.product_title", "h1"))
    if not name:
        return None

    sku = selected_text(soup, ("span.sku", ".sku"))
    part_number = pick_part_number([sku], [name])
    if not part_number:
        return None

    price = selected_text(
        soup,
        (
            "p.wds-price ins span",
            "p.wds-price > span",
            "p.price ins span",
            "p.price .woocommerce-Price-amount",
            ".summary .price .woocommerce-Price-amount",
        ),
    )
    image = selected_attr(
        soup,
        (
            "img.wp-post-image",
            ".woocommerce-product-gallery__image img",
            ".woocommerce-product-gallery img",
        ),
        "src",
    )

    return {
        "store_name": "NotebooksYa",
        "scraped_name": name,
        "scraped_brand": "N/A",
        "type": category_name,
        "part #": part_number,
        "price": normalize_price(price),
        "url": url,
        "image_url": absolute_url(base_url, image),
    }


def main() -> int:
    output_dir = "ScrapDB/Outputs/NotebooksYa"
    force_browser = os.environ.get("SCRAPER_FORCE_BROWSER_FALLBACK", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    if force_browser:
        saved_count = 0
        print("NotebooksYa requests path skipped because SCRAPER_FORCE_BROWSER_FALLBACK is enabled.")
    else:
        saved_count = run_woocommerce_store(
            store_name="NotebooksYa",
            base_url="https://notebooksya.cl",
            category_queries=CATEGORY_QUERIES,
            output_dir=output_dir,
            output_prefix="NYa",
            category_listing_urls=CATEGORY_LISTING_URLS,
            html_fallback_config={
                "product_link_selectors": "a.woocommerce-loop-product__link",
                "pagination_selectors": ("ul.page-numbers a", "ul.page-numbers span"),
                "page_url_builder": build_woocommerce_page_url,
                "parse_product": parse_notebooksya_product,
                "product_url_pattern": r"/producto/",
            },
        )
    print(f"NotebooksYa requests path saved {saved_count} JSON files.")

    if browser_fallback_enabled(saved_count):
        print("NotebooksYa starting browser fallback.")
        saved_count = run_browser_fallback_store(
            store_name="NotebooksYa",
            category_url_map=CATEGORY_LISTING_URLS,
            output_dir=output_dir,
            output_prefix="NYa",
            listing_config={
                "link_selector": "//a[contains(@class, 'woocommerce-loop-product__link')]",
                "pagination_selector": "//ul[@class='page-numbers']/li",
                "page_url_builder": build_woocommerce_page_url,
                "ready_selectors": (
                    "//a[contains(@class, 'woocommerce-loop-product__link')]",
                    "//ul[contains(@class,'products')]",
                ),
            },
            product_config={
                "ready_selectors": ("//span[@class='sku']", "//h1[contains(@class,'product_title')]"),
                "name_selectors": ("//h1[contains(@class,'product_title')]", "//h1"),
                "part_selectors": ("//span[@class='sku']",),
                "price_selectors": (
                    "//p[@class='wds-price']/ins/span",
                    "//p[@class='wds-price']/span",
                    "//p[contains(@class,'price')]//span[contains(@class,'woocommerce-Price-amount')]",
                ),
                "image_selectors": (
                    "//img[contains(@class,'wp-post-image')]",
                    "//div[contains(@class,'woocommerce-product-gallery')]//img",
                ),
                "brand_selectors": (),
                "clean_part_number": clean_notebooksya_part_number,
                "clean_price": normalize_price,
            },
        )
        print(f"NotebooksYa browser fallback saved {saved_count} JSON files.")

    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

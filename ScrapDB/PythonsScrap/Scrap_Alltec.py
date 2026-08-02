from __future__ import annotations

import os

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
from browser_fallback_utils import browser_fallback_enabled, run_browser_fallback_store


BASE_URL = "https://www.alltec.cl"
UNAVAILABLE_PATTERNS = (
    r"ya no se encuentra disponible",
    r"no se encuentra disponible",
    r"sin stock",
    r"agotad[oa]",
)

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


def is_alltec_unavailable(soup) -> bool:
    availability_text = selected_text(soup, ("#availability_value", "#availability_statut"))
    return any(pattern in availability_text.lower() for pattern in (
        "ya no se encuentra disponible",
        "no se encuentra disponible",
        "sin stock",
        "agotado",
        "agotada",
    ))


def parse_alltec_product(soup, url: str, category_name: str, base_url: str) -> dict | None:
    unavailable = is_alltec_unavailable(soup)

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

    result = {
        "store_name": "Alltec",
        "scraped_name": name,
        "scraped_brand": "N/A",
        "type": category_name,
        "part #": part_number,
        "price": normalize_price(selected_text(soup, ("#our_price_display", ".price[itemprop='price']", ".price"))),
        "url": url,
        "image_url": absolute_url(base_url, image_url),
    }
    if unavailable:
        # An explicit OOS listing is useful evidence. Publishing it allows the
        # matcher to update this one product immediately without interpreting a
        # missing/partial category as a store-wide markout.
        result["availability"] = "unavailable"
        print(f"Alltec kept explicitly unavailable product: {url}")
    else:
        result["availability"] = "available"
    return result


def clean_alltec_part_number(value: str) -> str:
    return pick_part_number([value], (), allow_name_fallback=False) or ""


def main() -> int:
    output_dir = "ScrapDB/Outputs/Alltec"
    force_browser = os.environ.get("SCRAPER_FORCE_BROWSER_FALLBACK", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    if force_browser:
        saved_count = 0
        print("Alltec requests path skipped; browser fallback will be used.")
    else:
        try:
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
            print(f"Alltec requests path saved {saved_count} JSON files.")
        except Exception as exc:
            saved_count = 0
            print(f"Alltec requests path failed, browser fallback will be used: {exc}")

    if browser_fallback_enabled(saved_count):
        print("Alltec starting browser fallback.")
        os.environ.setdefault("BROWSER_FALLBACK_COLLECTOR_CONCURRENCY", "1")
        os.environ.setdefault("BROWSER_FALLBACK_SCRAPER_CONCURRENCY", "1")
        saved_count = run_browser_fallback_store(
            store_name="Alltec",
            category_url_map=CATEGORY_URL_MAP,
            output_dir=output_dir,
            output_prefix="ALT",
            listing_config={
                "link_selector": (
                    "//div[@id='center_column']//ul[contains(@class,'product_list')]"
                    "//a[contains(@class,'product-name')]"
                ),
                "pagination_selector": (
                    "//div[@id='pagination']//a|//ul[contains(@class,'pagination')]//a"
                ),
                "page_url_builder": lambda url, page: build_query_page_url(url, page, "p"),
                "ready_selectors": (
                    "//div[@id='center_column']//ul[contains(@class,'product_list')]",
                    "//div[@id='center_column']//a[contains(@class,'product-name')]",
                ),
            },
            product_config={
                "ready_selectors": ("//h1[@itemprop='name']", "//p[@id='product_reference']"),
                "name_selectors": ("//h1[@itemprop='name']", "//h1"),
                "part_selectors": ("//p[@id='product_reference']//span[@itemprop='sku']",),
                "price_selectors": ("//*[@id='our_price_display']", "//*[contains(@class,'price')]"),
                "image_selectors": ("//*[@id='bigpic']", "//*[@id='image-block']//img", "//img[@itemprop='image']"),
                "brand_selectors": (),
                "unavailable_selectors": ("//*[@id='availability_value']", "//*[@id='availability_statut']"),
                "unavailable_patterns": UNAVAILABLE_PATTERNS,
                "clean_part_number": clean_alltec_part_number,
                "clean_price": normalize_price,
            },
        )
        print(f"Alltec browser fallback saved {saved_count} JSON files.")

    print(f"Alltec scraping finished. Saved {saved_count} JSON files.")
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

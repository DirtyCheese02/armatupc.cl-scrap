from __future__ import annotations

import asyncio
import json
import os
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydoll.browser import Chrome

from api_scraper_utils import clean_output_dir, clean_part_number, exit_code_from_count, normalize_price, write_product_json
from browser_fallback_utils import _env_int, _make_browser_options


BASE_URL = "https://www.ctman.cl"

CATEGORY_URL_MAP = {
    "OperatingSystem": "https://www.ctman.cl/collections/software/types/software",
    "UPS": "https://www.ctman.cl/collections/ups-respaldo-de-energia/types/ups",
    "Headphones": [
        "https://www.ctman.cl/collections/perifericos-de-pc/types/audifonos",
        "https://www.ctman.cl/collections/gaming/types/audifonos",
    ],
    "Mouse": [
        "https://www.ctman.cl/collections/perifericos-de-pc/types/mouse",
        "https://www.ctman.cl/collections/gaming/types/mouse",
    ],
    "Keyboard": [
        "https://www.ctman.cl/collections/perifericos-de-pc/types/teclados",
        "https://www.ctman.cl/collections/gaming/types/teclados",
    ],
    "Storage": [
        "https://www.ctman.cl/collections/almacenamiento/types/ssd",
        "https://www.ctman.cl/collections/almacenamiento/types/disco-duro",
    ],
    "ExternalStorage": [
        "https://www.ctman.cl/collections/almacenamiento/types/discos-duros-externos",
        "https://www.ctman.cl/collections/almacenamiento/types/ssds-externos",
        "https://www.ctman.cl/collections/almacenamiento/types/tarjeta-de-memoria-flash",
    ],
    "Monitor": "https://www.ctman.cl/collections/monitores/types/monitores",
    "CPUCooler": "https://www.ctman.cl/collections/repuestos-y-componentes/types/coolers-para-pc",
    "PowerSupply": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/fuentes-de-poder",
        "https://www.ctman.cl/collections/pc-escritorio/types/fuentes-de-poder",
    ],
    "Case": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/gabinetes",
        "https://www.ctman.cl/collections/pc-escritorio/types/gabinetes",
        "https://www.ctman.cl/collections/gaming/types/gabinetes",
    ],
    "Memory": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/memorias-ram",
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/memorias-ram-para-laptops",
    ],
    "CPU": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/procesadores",
        "https://www.ctman.cl/collections/gaming/types/procesadores",
        "https://www.ctman.cl/collections/servidores/types/procesadores",
    ],
    "VideoCard": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/tarjetas-de-video",
        "https://www.ctman.cl/collections/gaming/types/tarjetas-de-video",
    ],
    "Motherboard": [
        "https://www.ctman.cl/collections/repuestos-y-componentes/types/placas-madre",
        "https://www.ctman.cl/collections/pc-escritorio/types/placas-madre",
        "https://www.ctman.cl/collections/gaming/types/placas-madre",
    ],
    "Webcam": "https://www.ctman.cl/collections/electronica-audio-y-video/types/camaras-web",
}


def _value(result: dict[str, Any] | None) -> str:
    return (((result or {}).get("result") or {}).get("result") or {}).get("value") or ""


async def _json_from_page(page: Any, script: str) -> Any:
    raw_value = _value(await page.execute_script(script))
    return json.loads(raw_value or "null")


def _build_page_url(url: str, page_number: int) -> str:
    if page_number <= 1:
        return url
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["page"] = str(page_number)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


async def _wait_for_category(page: Any, timeout_seconds: int) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        state = await _json_from_page(
            page,
            r"""
return JSON.stringify({
  title: document.title || "",
  body: (document.body && document.body.innerText || "").slice(0, 600),
  productCount: document.querySelectorAll(".product-item").length,
  hasTotal: Boolean(document.querySelector(".total-products")),
  hasPagination: Boolean(document.querySelector(".pagination"))
});
""",
        )
        body = f"{state.get('title', '')} {state.get('body', '')}".lower()
        if state.get("productCount") or state.get("hasTotal") or state.get("hasPagination"):
            return True
        if "página no encontrada" in body or "pagina no encontrada" in body:
            return True
        await asyncio.sleep(1)
    return False


async def _category_page_count(page: Any) -> int:
    data = await _json_from_page(
        page,
        r"""
const clean = (value) => (value || "").replace(/\s+/g, " ").trim();
const pageNumbers = [...document.querySelectorAll(".pagination *")]
  .map((element) => clean(element.textContent))
  .filter((text) => /^\d+$/.test(text))
  .map((text) => Number(text));
return JSON.stringify(Math.max(1, ...pageNumbers));
""",
    )
    try:
        return max(int(data or 1), 1)
    except (TypeError, ValueError):
        return 1


async def _products_from_listing(page: Any) -> list[dict[str, str]]:
    return await _json_from_page(
        page,
        r"""
const clean = (value) => (value || "").replace(/\s+/g, " ").trim();
const products = [...document.querySelectorAll(".product-item")].map((card) => {
  const titleLink = card.querySelector("a.product-title-link");
  const imageLink = card.querySelector("a.product-image-link");
  const image = card.querySelector("img.front-image, img.back-image, .product-image img");
  return {
    name: clean(titleLink?.innerText || card.querySelector(".product-name")?.innerText),
    part_number: clean(card.querySelector(".product-sku")?.innerText),
    price: clean(card.querySelector(".bootic-price")?.innerText || card.querySelector("#price-on-img")?.innerText),
    url: titleLink?.href || imageLink?.href || "",
    image_url: image?.src || ""
  };
}).filter((product) => product.name && product.url);
return JSON.stringify(products);
""",
    ) or []


async def _scrape_ctman_async() -> int:
    output_dir = "ScrapDB/Outputs/CTMan"
    output_path = clean_output_dir(output_dir)
    options = _make_browser_options()
    browser = Chrome(options=options)
    await browser.start()
    page = None
    try:
        page = await browser.new_tab()
        ready_timeout = _env_int("CTMAN_PAGE_READY_TIMEOUT", 30)
        max_products = int(os.environ.get("BROWSER_FALLBACK_MAX_PRODUCTS", "0") or "0")
        saved_count = 0
        seen_urls: set[str] = set()

        for category_name, raw_urls in CATEGORY_URL_MAP.items():
            urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
            for category_url in urls:
                await page.go_to(category_url)
                if not await _wait_for_category(page, ready_timeout):
                    print(f"CTMan {category_name}: timed out waiting for {category_url}")
                    continue

                total_pages = await _category_page_count(page)
                print(f"CTMan {category_name}: {total_pages} page(s) from {category_url}")

                for page_number in range(1, total_pages + 1):
                    if page_number > 1:
                        await page.go_to(_build_page_url(category_url, page_number))
                        if not await _wait_for_category(page, ready_timeout):
                            print(f"CTMan {category_name}: timed out waiting for page {page_number}")
                            continue

                    products = await _products_from_listing(page)
                    print(
                        f"CTMan {category_name} page {page_number}/{total_pages}: "
                        f"{len(products)} product cards"
                    )

                    for product in products:
                        url = product.get("url") or ""
                        if not url or url in seen_urls:
                            continue

                        part_number = clean_part_number(product.get("part_number"))
                        if not part_number:
                            continue

                        data = {
                            "store_name": "CTMan",
                            "scraped_name": product["name"],
                            "scraped_brand": "N/A",
                            "type": category_name,
                            "part #": part_number,
                            "price": normalize_price(product.get("price")),
                            "url": url,
                            "image_url": product.get("image_url") or "N/A",
                        }
                        write_product_json(output_path, "CTM", url, data)
                        seen_urls.add(url)
                        saved_count += 1

                        if max_products > 0 and saved_count >= max_products:
                            print(f"CTMan scraping stopped at BROWSER_FALLBACK_MAX_PRODUCTS={max_products}.")
                            return saved_count

        print(f"CTMan scraping finished. Saved {saved_count} JSON files.")
        return saved_count
    finally:
        if page is not None:
            await page.close()
        await browser.stop()


def main() -> int:
    return exit_code_from_count(asyncio.run(_scrape_ctman_async()))


if __name__ == "__main__":
    raise SystemExit(main())

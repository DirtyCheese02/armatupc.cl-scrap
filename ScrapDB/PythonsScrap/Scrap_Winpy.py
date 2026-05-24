from __future__ import annotations

import asyncio
import json
import os
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

from pydoll.browser import Chrome

from api_scraper_utils import clean_output_dir, clean_part_number, exit_code_from_count, normalize_price, write_product_json
from browser_fallback_utils import _env_int, _make_browser_options


BASE_URL = "https://www.winpy.cl"

CATEGORY_URL_MAP = {
    "OperatingSystem": "https://www.winpy.cl/software/sistemas-operativos/",
    "UPS": "https://www.winpy.cl/energia/ups/",
    "Headphones": "https://www.winpy.cl/accesorios/audifonos/",
    "Mouse": "https://www.winpy.cl/accesorios/mouse/",
    "Keyboard": "https://www.winpy.cl/accesorios/teclados/",
    "Storage": [
        "https://www.winpy.cl/almacenamiento/disco-estado-solido/",
        "https://www.winpy.cl/almacenamiento/disco-duro-pc-s/",
    ],
    "ExternalStorage": "https://www.winpy.cl/almacenamiento/discos-portatiles/",
    "Monitor": "https://www.winpy.cl/monitores/",
    "CPUCooler": [
        "https://www.winpy.cl/partes-y-piezas/disipadores/?filtro=refrigeracion-por-aire",
        "https://www.winpy.cl/partes-y-piezas/disipadores/?filtro=refrigeracion-liquida",
    ],
    "CaseFan": "https://www.winpy.cl/partes-y-piezas/disipadores/?filtro=ventilador",
    "ThermalCompound": "https://www.winpy.cl/partes-y-piezas/disipadores/?filtro=pasta-termica",
    "PowerSupply": "https://www.winpy.cl/partes-y-piezas/fuente-de-poder/",
    "Case": "https://www.winpy.cl/partes-y-piezas/gabinetes/",
    "Memory": "https://www.winpy.cl/memorias/",
    "CPU": "https://www.winpy.cl/partes-y-piezas/procesadores/",
    "VideoCard": "https://www.winpy.cl/partes-y-piezas/tarjetas-graficas/",
    "Motherboard": "https://www.winpy.cl/partes-y-piezas/placas-madres/",
    "Webcam": "https://www.winpy.cl/electronica/camaras-web/",
    "NetworkAdapter": "https://www.winpy.cl/redes/tarjetas-de-red/",
}


def _clean_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _value(result: dict[str, Any] | None) -> str:
    return (((result or {}).get("result") or {}).get("result") or {}).get("value") or ""


async def _json_from_page(page: Any, script: str) -> Any:
    raw_value = _value(await page.execute_script(script))
    return json.loads(raw_value or "null")


async def _wait_for_category(page: Any, timeout_seconds: int) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        state = await _json_from_page(
            page,
            r"""
return JSON.stringify({
  title: document.title || "",
  body: (document.body && document.body.innerText || "").slice(0, 700),
  productCount: document.querySelectorAll("section#productos article").length,
  hasPager: Boolean(document.querySelector(".paginador"))
});
""",
        )
        page_text = f"{state.get('title', '')} {state.get('body', '')}".lower()
        if state.get("productCount") or state.get("hasPager"):
            return True
        if "pagina no encontrada" in page_text or "página no encontrada" in page_text:
            return True
        await asyncio.sleep(1)
    return False


async def _wait_for_product(page: Any, timeout_seconds: int) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while asyncio.get_running_loop().time() < deadline:
        state = await _json_from_page(
            page,
            r"""
return JSON.stringify({
  hasName: Boolean(document.querySelector("h1[itemprop='name'], h1")),
  hasSku: Boolean(document.querySelector("span[itemprop='sku'], span.sku")),
  hasPrice: Boolean(document.querySelector(".price-oferta h2, [itemprop='lowPrice'], .price-normal h3"))
});
""",
        )
        if state.get("hasName") and (state.get("hasSku") or state.get("hasPrice")):
            return True
        await asyncio.sleep(1)
    return False


async def _category_pages(page: Any, category_url: str) -> list[str]:
    raw_pages = await _json_from_page(
        page,
        r"""
const urls = [location.href];
document.querySelectorAll(".paginador a[href]").forEach((link) => urls.push(link.href));
return JSON.stringify([...new Set(urls)]);
""",
    ) or []
    pages: list[str] = []
    category_path = urlsplit(category_url).path.rstrip("/")
    for raw_url in raw_pages:
        if not raw_url:
            continue
        absolute = urljoin(BASE_URL, raw_url)
        parsed = urlsplit(absolute)
        if parsed.netloc and parsed.netloc != "www.winpy.cl":
            continue
        if not parsed.path.rstrip("/").startswith(category_path):
            continue
        if absolute not in pages:
            pages.append(absolute)
    return pages or [category_url]


async def _products_from_listing(page: Any) -> list[dict[str, str]]:
    return await _json_from_page(
        page,
        r"""
const clean = (value) => (value || "").replace(/\s+/g, " ").trim();
const firstPrice = (text) => {
  const match = clean(text).match(/\$\s*[\d.]+/);
  return match ? match[0] : "";
};
const products = [...document.querySelectorAll("section#productos article")].map((card) => {
  const links = [...card.querySelectorAll("a[href*='/venta/']")];
  const modelLink =
    card.querySelector(".model a[href*='/venta/']") ||
    links.find((link) => clean(link.textContent).length > 20) ||
    links[0];
  const image = card.querySelector("img[src]");
  const brandLink = links.find((link) => {
    const text = clean(link.textContent);
    return link !== modelLink && text && text.length <= 35 && !/\$/.test(text);
  });
  return {
    name: clean(modelLink?.textContent || modelLink?.getAttribute("title") || card.querySelector(".model")?.textContent),
    brand: clean(brandLink?.textContent),
    price: firstPrice(card.querySelector("h2")?.textContent || card.textContent),
    url: modelLink?.href || "",
    image_url: image?.src || ""
  };
}).filter((product) => product.name && product.url);
return JSON.stringify(products);
""",
    ) or []


async def _collect_category(
    sem: asyncio.Semaphore,
    browser: Chrome,
    *,
    category_name: str,
    category_url: str,
    products_to_scrape: list[dict[str, str]],
    seen: set[tuple[str, str]],
    ready_timeout: int,
) -> None:
    async with sem:
        page = await browser.new_tab()
        try:
            print(f"[Winpy] collector starting: {category_name} -> {category_url}")
            category_ready = False
            for attempt in range(1, 3):
                await page.go_to(category_url)
                category_ready = await _wait_for_category(page, ready_timeout)
                if category_ready:
                    break
                print(f"[Winpy] {category_name}: category load retry {attempt}/2.")
                await asyncio.sleep(3)
            if not category_ready:
                print(f"[Winpy] {category_name}: timed out waiting for category.")
                return

            page_urls = await _category_pages(page, category_url)
            print(f"[Winpy] {category_name}: {len(page_urls)} page(s) detected.")

            for index, page_url in enumerate(page_urls, start=1):
                if index > 1:
                    page_ready = False
                    for attempt in range(1, 3):
                        await page.go_to(page_url)
                        page_ready = await _wait_for_category(page, ready_timeout)
                        if page_ready:
                            break
                        print(
                            f"[Winpy] {category_name}: page {index} load retry "
                            f"{attempt}/2."
                        )
                        await asyncio.sleep(3)
                    if not page_ready:
                        print(f"[Winpy] {category_name}: timed out waiting for page {index}.")
                        continue

                products = await _products_from_listing(page)
                new_count = 0
                for product in products:
                    url = _clean_url(product.get("url", ""))
                    identity = (category_name, url)
                    if not url or identity in seen:
                        continue
                    product["url"] = url
                    product["type"] = category_name
                    products_to_scrape.append(product)
                    seen.add(identity)
                    new_count += 1
                print(
                    f"[Winpy] {category_name} page {index}/{len(page_urls)}: "
                    f"{new_count} new links ({len(products)} cards)"
                )
        except Exception as exc:
            print(f"[Winpy] collector error {category_name}: {exc}")
        finally:
            await page.close()


async def _product_detail(page: Any) -> dict[str, str]:
    return await _json_from_page(
        page,
        r"""
const clean = (value) => (value || "").replace(/\s+/g, " ").trim();
const priceText = clean(
  document.querySelector(".price-oferta h2")?.textContent ||
  document.querySelector("[itemprop='lowPrice']")?.textContent ||
  document.querySelector(".price-normal h3")?.textContent ||
  ""
);
const name = clean(document.querySelector("h1[itemprop='name']")?.textContent || document.querySelector("h1")?.textContent);
const brand =
  clean(document.querySelector("img[src*='/logos/']")?.alt) ||
  clean(document.querySelector("[itemprop='brand']")?.textContent);
const sku = clean(document.querySelector("span[itemprop='sku']")?.textContent || document.querySelector("span.sku")?.textContent);
const namePrefix = name.slice(0, 30).toLowerCase();
const image =
  [...document.querySelectorAll("img[src*='/files/']")]
    .find((img) => clean(img.alt).toLowerCase().startsWith(namePrefix))?.src ||
  [...document.querySelectorAll("img[src*='/files/']")]
    .find((img) => !/banner|logo|home-/i.test(img.src + " " + img.alt))?.src ||
  "";
return JSON.stringify({name, brand, sku, price: priceText, image_url: image});
""",
    ) or {}


def _part_from_title(product_name: str, page_title: str) -> str | None:
    if "|" not in page_title:
        return None
    candidate = page_title.rsplit("|", 1)[-1]
    cleaned = clean_part_number(candidate)
    if not cleaned:
        return None
    if cleaned.lower() in product_name.lower():
        return None
    return cleaned


async def _scrape_product(
    sem: asyncio.Semaphore,
    browser: Chrome,
    *,
    product: dict[str, str],
    output_path: str,
    ready_timeout: int,
) -> bool:
    async with sem:
        page = await browser.new_tab()
        try:
            await page.go_to(product["url"])
            await _wait_for_product(page, ready_timeout)
            detail = await _product_detail(page)
            title = _value(await page.execute_script("return document.title || ''"))

            name = detail.get("name") or product.get("name") or ""
            part_number = clean_part_number(detail.get("sku"))
            if not part_number:
                part_number = _part_from_title(name, title)
            if not name or not part_number:
                print(f"[Winpy] skipped without SKU: {product['url']}")
                return False

            data = {
                "store_name": "Winpy",
                "scraped_name": name,
                "scraped_brand": detail.get("brand") or product.get("brand") or "N/A",
                "type": product["type"],
                "part #": part_number,
                "price": normalize_price(detail.get("price") or product.get("price")),
                "url": product["url"],
                "image_url": detail.get("image_url") or product.get("image_url") or "N/A",
            }
            write_product_json(output_path, "W", product["url"], data)
            return True
        except Exception as exc:
            print(f"[Winpy] product error {product.get('url')}: {exc}")
            return False
        finally:
            await page.close()


async def _scrape_winpy_async() -> int:
    output_dir = "ScrapDB/Outputs/Winpy"
    clean_output_dir(output_dir)

    options = _make_browser_options()
    browser = Chrome(options=options)
    await browser.start()
    try:
        collector_concurrency = _env_int("WINPY_COLLECTOR_CONCURRENCY", 2)
        scraper_concurrency = _env_int("WINPY_SCRAPER_CONCURRENCY", 3)
        ready_timeout = _env_int("WINPY_PAGE_READY_TIMEOUT", 45)
        products_to_scrape: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()

        sem_collector = asyncio.Semaphore(collector_concurrency)
        collect_tasks = []
        for category_name, raw_urls in CATEGORY_URL_MAP.items():
            urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
            for category_url in urls:
                collect_tasks.append(
                    _collect_category(
                        sem_collector,
                        browser,
                        category_name=category_name,
                        category_url=category_url,
                        products_to_scrape=products_to_scrape,
                        seen=seen,
                        ready_timeout=ready_timeout,
                    )
                )
        await asyncio.gather(*collect_tasks)

        print(f"[Winpy] collected {len(products_to_scrape)} product/category pairs.")
        max_products = int(os.environ.get("BROWSER_FALLBACK_MAX_PRODUCTS", "0") or "0")
        if max_products > 0:
            products_to_scrape = products_to_scrape[:max_products]
            print(f"[Winpy] limited to BROWSER_FALLBACK_MAX_PRODUCTS={max_products}.")

        sem_scraper = asyncio.Semaphore(scraper_concurrency)
        saved_count = 0
        chunk_size = 80
        for index in range(0, len(products_to_scrape), chunk_size):
            chunk = products_to_scrape[index : index + chunk_size]
            results = await asyncio.gather(
                *[
                    _scrape_product(
                        sem_scraper,
                        browser,
                        product=product,
                        output_path=output_dir,
                        ready_timeout=ready_timeout,
                    )
                    for product in chunk
                ]
            )
            saved_count += sum(1 for result in results if result)
            print(f"[Winpy] saved {saved_count} JSON files so far.")

        print(f"[Winpy] scraping finished. Saved {saved_count} JSON files.")
        return saved_count
    finally:
        await browser.stop()


def main() -> int:
    return exit_code_from_count(asyncio.run(_scrape_winpy_async()))


if __name__ == "__main__":
    raise SystemExit(main())

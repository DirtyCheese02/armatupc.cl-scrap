from __future__ import annotations

import asyncio
import json
import os
import re
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

from pydoll.browser import Chrome

from api_scraper_utils import clean_output_dir, clean_part_number, exit_code_from_count, normalize_price, write_product_json
from browser_fallback_utils import _env_int, _make_browser_options
from scraper_health import write_scraper_health


BASE_URL = "https://www.winpy.cl"
EMPTY_PART_VALUES = {"", "N/A", "NA", "NONE", "NULL", "SIN SKU", "SKU NO INFORMADO", "NO INFORMADO"}
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


def _env_nonnegative_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        print(f"[Winpy] {name}={raw!r} is invalid. Using {default}.")
        return default


def _active_category_url_map() -> dict[str, Any]:
    raw = os.environ.get("WINPY_CATEGORY_INCLUDE", "").strip()
    if not raw:
        return dict(CATEGORY_URL_MAP)

    requested = {value.strip() for value in raw.split(",") if value.strip()}
    unknown = sorted(requested - set(CATEGORY_URL_MAP))
    if unknown:
        print(f"[Winpy] Ignoring unknown WINPY_CATEGORY_INCLUDE values: {', '.join(unknown)}")
    selected = {
        category_name: raw_urls
        for category_name, raw_urls in CATEGORY_URL_MAP.items()
        if category_name in requested
    }
    if not selected:
        raise ValueError("WINPY_CATEGORY_INCLUDE did not contain a valid Winpy category")
    print(f"[Winpy] Scoped retry categories: {', '.join(selected)}")
    return selected


def _clean_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _value(result: dict[str, Any] | None) -> str:
    return (((result or {}).get("result") or {}).get("result") or {}).get("value") or ""


def _clean_winpy_part_number(value: Any) -> str | None:
    part_number = clean_part_number(value)
    if not part_number:
        return None

    part_number = re.sub(r"\s+", " ", part_number).strip(" \t\r\n,;|()[]{}")
    if part_number.upper() in EMPTY_PART_VALUES:
        return None

    return part_number


async def _json_from_page(page: Any, script: str) -> Any:
    raw_value = _value(await page.execute_script(script))
    return json.loads(raw_value or "null")


async def _start_browser(options: Any) -> Chrome:
    browser = Chrome(options=options)
    await browser.start()
    return browser


async def _stop_browser(browser: Chrome | None, *, timeout_seconds: int = 20) -> None:
    if browser is None:
        return
    try:
        await asyncio.wait_for(browser.stop(), timeout=timeout_seconds)
    except TimeoutError:
        print(f"[Winpy] browser stop timed out after {timeout_seconds}s; continuing with a fresh session.")
    except Exception as exc:
        print(f"[Winpy] browser stop warning: {exc}")


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
  hasSku: Boolean(
    document.querySelector("span[itemprop='sku'], span.sku, #info-product") ||
    document.querySelector("[data-flix-mpn]") ||
    /SKU\s*:/i.test(document.body?.innerText || "")
  ),
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


async def _open_ready_category_tab(
    browser: Chrome,
    url: str,
    *,
    category_name: str,
    ready_timeout: int,
    label: str,
    attempts: int = 3,
) -> Any | None:
    for attempt in range(1, attempts + 1):
        page = await browser.new_tab()
        try:
            await page.go_to(url)
            if await _wait_for_category(page, ready_timeout):
                return page
        except Exception as exc:
            print(f"[Winpy] {category_name}: {label} attempt {attempt}/{attempts} error: {exc}")
        try:
            await page.close()
        except Exception:
            pass
        if attempt < attempts:
            print(f"[Winpy] {category_name}: {label} load retry {attempt}/{attempts}.")
            await asyncio.sleep(5 * attempt)
    return None


async def _collect_category(
    sem: asyncio.Semaphore,
    browser: Chrome,
    *,
    category_name: str,
    category_url: str,
    products_to_scrape: list[dict[str, str]],
    seen: set[tuple[str, str]],
    ready_timeout: int,
    navigation_delay: int = 0,
) -> tuple[str, bool, str | None]:
    async with sem:
        page = None
        page_errors: list[str] = []
        try:
            print(f"[Winpy] collector starting: {category_name} -> {category_url}")
            page = await _open_ready_category_tab(
                browser,
                category_url,
                category_name=category_name,
                ready_timeout=ready_timeout,
                label="category",
            )
            if page is None:
                print(f"[Winpy] {category_name}: timed out waiting for category.")
                return category_name, False, "category_timeout"

            page_urls = await _category_pages(page, category_url)
            print(f"[Winpy] {category_name}: {len(page_urls)} page(s) detected.")

            for index, page_url in enumerate(page_urls, start=1):
                if index > 1:
                    if navigation_delay:
                        await asyncio.sleep(navigation_delay)
                    if page is not None:
                        try:
                            await page.close()
                        except Exception:
                            pass
                    page = await _open_ready_category_tab(
                        browser,
                        page_url,
                        category_name=category_name,
                        ready_timeout=ready_timeout,
                        label=f"page {index}",
                        # The complete category is retried later with a fresh
                        # browser. Repeating a poisoned secondary navigation
                        # three times here only delays that safer recovery.
                        attempts=1,
                    )
                    if page is None:
                        print(f"[Winpy] {category_name}: timed out waiting for page {index}.")
                        page_errors.append(f"page_{index}_timeout")
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
            return (
                category_name,
                not page_errors,
                ",".join(page_errors) if page_errors else None,
            )
        except Exception as exc:
            print(f"[Winpy] collector error {category_name}: {exc}")
            return category_name, False, str(exc)[:500]
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception as exc:
                    print(f"[Winpy] collector close warning {category_name}: {exc}")


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
const textSku = (text) => {
  const match = clean(text).match(/\bSKU\s*:\s*([^|]+?)(?:\s+WhatsApp|$)/i);
  return clean(match?.[1] || "");
};
const scripts = [...document.scripts].map((script) => script.textContent || "").join("\n");
const itemIdMatch = scripts.match(/item_id\s*:\s*["']([^"']+)["']/i);
const sku = clean(
  document.querySelector("span[itemprop='sku']")?.textContent ||
  document.querySelector("#info-product span.sku")?.textContent ||
  document.querySelector("span.sku")?.textContent ||
  document.querySelector("[data-flix-mpn]")?.getAttribute("data-flix-mpn") ||
  textSku(document.querySelector("#info-product")?.textContent) ||
  textSku(document.body?.innerText) ||
  itemIdMatch?.[1] ||
  ""
);
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
    return _clean_winpy_part_number(candidate)


async def _scrape_product(
    sem: asyncio.Semaphore,
    browser: Chrome,
    *,
    product: dict[str, str],
    output_path: str,
    ready_timeout: int,
) -> bool:
    async with sem:
        page = None
        try:
            page = await browser.new_tab()
            await page.go_to(product["url"])
            await _wait_for_product(page, ready_timeout)
            detail = await _product_detail(page)
            title = _value(await page.execute_script("return document.title || ''"))

            name = detail.get("name") or product.get("name") or ""
            part_number = _clean_winpy_part_number(detail.get("sku"))
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
            if page is not None:
                try:
                    await page.close()
                except Exception as exc:
                    print(f"[Winpy] page close warning {product.get('url')}: {exc}")


async def _scrape_winpy_async() -> int:
    output_dir = "ScrapDB/Outputs/Winpy"
    clean_output_dir(output_dir)

    options = _make_browser_options()
    browser: Chrome | None = await _start_browser(options)
    try:
        active_category_map = _active_category_url_map()
        collector_concurrency = _env_int(
            "WINPY_COLLECTOR_CONCURRENCY",
            _env_int("BROWSER_FALLBACK_COLLECTOR_CONCURRENCY", 2),
        )
        scraper_concurrency = _env_int(
            "WINPY_SCRAPER_CONCURRENCY",
            _env_int("BROWSER_FALLBACK_SCRAPER_CONCURRENCY", 2),
        )
        ready_timeout = _env_int("WINPY_PAGE_READY_TIMEOUT", 45)
        product_ready_timeout = _env_int("WINPY_PRODUCT_READY_TIMEOUT", 20)
        listing_navigation_delay = _env_nonnegative_int(
            "WINPY_LISTING_NAVIGATION_DELAY_SECONDS",
            8,
        )
        products_to_scrape: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()

        sem_collector = asyncio.Semaphore(collector_concurrency)
        collect_tasks = []
        collect_specs: list[tuple[str, str]] = []
        for category_name, raw_urls in active_category_map.items():
            urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
            for category_url in urls:
                collect_specs.append((category_name, category_url))
                collect_tasks.append(
                    _collect_category(
                        sem_collector,
                        browser,
                        category_name=category_name,
                        category_url=category_url,
                        products_to_scrape=products_to_scrape,
                        seen=seen,
                        ready_timeout=ready_timeout,
                        navigation_delay=listing_navigation_delay,
                    )
                )
        collection_results = await asyncio.gather(*collect_tasks)
        result_by_spec = dict(zip(collect_specs, collection_results))
        retry_passes = _env_nonnegative_int("WINPY_CATEGORY_RETRY_PASSES", 2)
        retry_timeout = max(ready_timeout, _env_int("WINPY_RETRY_PAGE_READY_TIMEOUT", 75))
        # Failed categories are retried sequentially. Cloudflare becomes less
        # reliable when several fresh tabs navigate at the same time, which was
        # leaving the same eight categories partial on every Actions run.
        retry_sem = asyncio.Semaphore(1)
        for retry_pass in range(1, retry_passes + 1):
            failed_specs = [spec for spec, result in result_by_spec.items() if not result[1]]
            if not failed_specs:
                break
            cooldown = min(30, 10 * retry_pass)
            print(
                f"[Winpy] retry pass {retry_pass}/{retry_passes}: "
                f"{len(failed_specs)} failed category source(s), cooldown={cooldown}s."
            )
            await asyncio.sleep(cooldown)
            retry_results = await asyncio.gather(
                *[
                    _collect_category(
                        retry_sem,
                        browser,
                        category_name=category_name,
                        category_url=category_url,
                        products_to_scrape=products_to_scrape,
                        seen=seen,
                        ready_timeout=retry_timeout,
                        navigation_delay=listing_navigation_delay,
                    )
                    for category_name, category_url in failed_specs
                ]
            )
            result_by_spec.update(dict(zip(failed_specs, retry_results)))
        collection_results = [result_by_spec[spec] for spec in collect_specs]
        failed_categories = {name for name, ok, _ in collection_results if not ok}
        completed_categories = set(active_category_map) - failed_categories
        health_errors = [
            {"category": name, "error": error or "unknown"}
            for name, ok, error in collection_results
            if not ok
        ]

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
                        ready_timeout=product_ready_timeout,
                    )
                    for product in chunk
                ],
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, Exception):
                    print(f"[Winpy] product task error: {result}")
                elif result:
                    saved_count += 1
            print(f"[Winpy] saved {saved_count} JSON files so far.")
            write_scraper_health(
                status="partial_success",
                expected_categories=active_category_map,
                completed_categories=completed_categories,
                failed_categories=failed_categories,
                product_count=saved_count,
                errors=health_errors,
                blocked_reason=None,
            )

        print(f"[Winpy] scraping finished. Saved {saved_count} JSON files.")
        write_scraper_health(
            status="failed" if saved_count == 0 else ("partial_success" if failed_categories else "success"),
            expected_categories=active_category_map,
            completed_categories=completed_categories,
            failed_categories=failed_categories,
            product_count=saved_count,
            errors=health_errors,
            blocked_reason="browser_unavailable" if saved_count == 0 else None,
        )
        return saved_count
    finally:
        await _stop_browser(browser)


def main() -> int:
    try:
        count = asyncio.run(_scrape_winpy_async())
    except Exception as exc:
        write_scraper_health(
            status="failed",
            expected_categories=CATEGORY_URL_MAP,
            completed_categories=(),
            failed_categories=CATEGORY_URL_MAP,
            errors=({"category": "*", "error": str(exc)[:500]},),
            blocked_reason="browser_start_or_runtime_failure",
        )
        raise
    return exit_code_from_count(count)


if __name__ == "__main__":
    raise SystemExit(main())

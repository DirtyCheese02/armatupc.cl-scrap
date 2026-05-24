from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import re
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin

from pydoll.browser import Chrome
from pydoll.browser.options import ChromiumOptions


def browser_fallback_enabled(saved_count: int) -> bool:
    forced = os.environ.get("SCRAPER_FORCE_BROWSER_FALLBACK", "").strip().lower()
    return saved_count == 0 or forced in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        print(f"[BrowserFallback] {name}={raw!r} is invalid. Using {default}.")
        return default
    return max(value, 1)


def _make_browser_options() -> ChromiumOptions:
    options = ChromiumOptions()
    options.headless = os.environ.get("SCRAP_HEADLESS", "1").lower() not in (
        "0",
        "false",
        "no",
        "off",
    )
    options.start_timeout = int(os.environ.get("SCRAP_BROWSER_START_TIMEOUT", "45"))
    chrome_binary = os.environ.get("CHROME_BINARY_PATH")
    if chrome_binary:
        options.binary_location = chrome_binary
    options.add_argument("--window-size=1280,720")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    print(
        f"[BrowserFallback] headless={options.headless} "
        f"binary={'auto' if not chrome_binary else chrome_binary} "
        f"start_timeout={options.start_timeout}s"
    )
    return options


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _element_text(element: Any) -> str:
    if element is None:
        return ""
    value = await _maybe_await(getattr(element, "text", ""))
    return re.sub(r"\s+", " ", str(value or "")).strip()


async def _element_attr(element: Any, attr: str) -> str:
    if element is None:
        return ""
    getter = getattr(element, "get_attribute", None)
    if getter is None:
        return ""
    value = await _maybe_await(getter(attr))
    return str(value or "").strip()


async def _first_text(page: Any, selectors: tuple[str, ...]) -> str:
    for selector in selectors:
        try:
            element = await page.query(selector)
            text = await _element_text(element)
            if text:
                return text
        except Exception:
            continue
    return ""


async def _first_attr(page: Any, selectors: tuple[str, ...], attr: str) -> str:
    for selector in selectors:
        try:
            element = await page.query(selector)
            value = await _element_attr(element, attr)
            if value:
                return value
        except Exception:
            continue
    return ""


async def _wait_for_any(page: Any, selectors: tuple[str, ...], timeout_seconds: int) -> bool:
    end_time = asyncio.get_running_loop().time() + timeout_seconds
    while asyncio.get_running_loop().time() < end_time:
        for selector in selectors:
            try:
                element = await page.query(selector)
                if element is not None:
                    return True
            except Exception:
                continue
        await asyncio.sleep(1)
    return False


async def _page_count(page: Any, pagination_selector: str) -> int:
    try:
        elements = await page.query(pagination_selector, find_all=True)
    except Exception:
        return 1

    pages: list[int] = []
    for element in elements or []:
        text = await _element_text(element)
        if text.isdigit():
            pages.append(int(text))
    return max(pages) if pages else 1


async def _collect_category_links(
    sem: asyncio.Semaphore,
    browser: Chrome,
    *,
    store_name: str,
    category_name: str,
    category_url: str,
    link_selector: str,
    pagination_selector: str,
    page_url_builder: Callable[[str, int], str],
    ready_selectors: tuple[str, ...],
    links_to_scrape: list[tuple[str, str]],
    seen_links: set[tuple[str, str]],
) -> None:
    async with sem:
        page = await browser.new_tab()
        try:
            print(f"[BrowserFallback] {store_name} collector starting: {category_name}")
            await page.go_to(category_url)
            await _wait_for_any(page, ready_selectors, timeout_seconds=20)
            total_pages = await _page_count(page, pagination_selector)
            print(f"[BrowserFallback] {store_name} {category_name}: {total_pages} pages")

            for page_number in range(1, total_pages + 1):
                if page_number != 1:
                    next_page_url = page_url_builder(category_url, page_number)
                    await page.go_to(next_page_url)
                    await _wait_for_any(page, ready_selectors, timeout_seconds=20)

                try:
                    elements = await page.query(link_selector, find_all=True)
                except Exception:
                    elements = []

                new_count = 0
                for element in elements or []:
                    href = await _element_attr(element, "href")
                    if not href:
                        continue
                    full_url = urljoin(category_url, href)
                    identity = (category_name, full_url)
                    if identity in seen_links:
                        continue
                    seen_links.add(identity)
                    links_to_scrape.append(identity)
                    new_count += 1
                print(
                    f"[BrowserFallback] {store_name} {category_name} "
                    f"page {page_number}: {new_count} new links"
                )
        except Exception as exc:
            print(f"[BrowserFallback] {store_name} collector error {category_name}: {exc}")
        finally:
            await page.close()


async def _scrape_product(
    sem: asyncio.Semaphore,
    browser: Chrome,
    *,
    store_name: str,
    category_name: str,
    url: str,
    output_path: Path,
    output_prefix: str,
    product_config: dict[str, Any],
) -> bool:
    async with sem:
        page = await browser.new_tab()
        try:
            await page.go_to(url)
            await _wait_for_any(page, tuple(product_config["ready_selectors"]), timeout_seconds=20)

            name = await _first_text(page, tuple(product_config["name_selectors"]))
            part_number = await _first_text(page, tuple(product_config["part_selectors"]))
            price = await _first_text(page, tuple(product_config["price_selectors"]))
            image_url = await _first_attr(page, tuple(product_config["image_selectors"]), "src")
            brand = "N/A"
            if product_config.get("brand_selectors"):
                brand = await _first_text(page, tuple(product_config["brand_selectors"])) or "N/A"

            part_number = product_config["clean_part_number"](part_number)
            price = product_config["clean_price"](price)

            if not name or not part_number:
                return False

            data = {
                "store_name": store_name,
                "scraped_name": name,
                "scraped_brand": brand,
                "type": category_name,
                "part #": part_number,
                "price": price,
                "url": url,
                "image_url": image_url or "N/A",
            }
            file_path = output_path / f"{output_prefix}_{hashlib.md5(url.encode()).hexdigest()}.json"
            file_path.write_text(json.dumps(data, ensure_ascii=False, indent=4), encoding="utf-8")
            return True
        except Exception as exc:
            print(f"[BrowserFallback] {store_name} product error {url}: {exc}")
            return False
        finally:
            await page.close()


async def _run_browser_fallback_async(
    *,
    store_name: str,
    category_url_map: dict[str, Any],
    output_dir: str,
    output_prefix: str,
    listing_config: dict[str, Any],
    product_config: dict[str, Any],
) -> int:
    options = _make_browser_options()
    browser = Chrome(options=options)
    await browser.start()
    try:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        for file_path in output_path.glob("*.json"):
            file_path.unlink()

        collector_concurrency = _env_int("BROWSER_FALLBACK_COLLECTOR_CONCURRENCY", 2)
        scraper_concurrency = _env_int("BROWSER_FALLBACK_SCRAPER_CONCURRENCY", 2)
        links_to_scrape: list[tuple[str, str]] = []
        seen_links: set[tuple[str, str]] = set()

        sem_collector = asyncio.Semaphore(collector_concurrency)
        collect_tasks = []
        for category_name, raw_urls in category_url_map.items():
            urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
            for category_url in urls:
                collect_tasks.append(
                    _collect_category_links(
                        sem_collector,
                        browser,
                        store_name=store_name,
                        category_name=category_name,
                        category_url=category_url,
                        link_selector=listing_config["link_selector"],
                        pagination_selector=listing_config["pagination_selector"],
                        page_url_builder=listing_config["page_url_builder"],
                        ready_selectors=tuple(listing_config["ready_selectors"]),
                        links_to_scrape=links_to_scrape,
                        seen_links=seen_links,
                    )
                )
        await asyncio.gather(*collect_tasks)
        print(f"[BrowserFallback] {store_name}: collected {len(links_to_scrape)} product links")

        max_products = int(os.environ.get("BROWSER_FALLBACK_MAX_PRODUCTS", "0") or "0")
        if max_products > 0:
            links_to_scrape = links_to_scrape[:max_products]
            print(f"[BrowserFallback] {store_name}: limited to {max_products} products")

        sem_scraper = asyncio.Semaphore(scraper_concurrency)
        saved_count = 0
        chunk_size = 50
        for index in range(0, len(links_to_scrape), chunk_size):
            chunk = links_to_scrape[index : index + chunk_size]
            scrape_tasks = [
                _scrape_product(
                    sem_scraper,
                    browser,
                    store_name=store_name,
                    category_name=category_name,
                    url=url,
                    output_path=output_path,
                    output_prefix=output_prefix,
                    product_config=product_config,
                )
                for category_name, url in chunk
            ]
            results = await asyncio.gather(*scrape_tasks)
            saved_count += sum(1 for item in results if item)
            print(f"[BrowserFallback] {store_name}: saved {saved_count} JSON so far")

        return saved_count
    finally:
        await browser.stop()


def run_browser_fallback_store(
    *,
    store_name: str,
    category_url_map: dict[str, Any],
    output_dir: str,
    output_prefix: str,
    listing_config: dict[str, Any],
    product_config: dict[str, Any],
) -> int:
    return asyncio.run(
        _run_browser_fallback_async(
            store_name=store_name,
            category_url_map=category_url_map,
            output_dir=output_dir,
            output_prefix=output_prefix,
            listing_config=listing_config,
            product_config=product_config,
        )
    )

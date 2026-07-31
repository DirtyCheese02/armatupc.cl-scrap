from __future__ import annotations

import asyncio
import html
import json
import os
import re
import time
import unicodedata
from typing import Any
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup
from pydoll.browser import Chrome

from api_scraper_utils import (
    absolute_url,
    brand_from_wc,
    build_woocommerce_page_url,
    clean_output_dir,
    clean_part_number,
    exit_code_from_count,
    fetch_json,
    fetch_text_with_referer,
    first_image_from_wc,
    html_to_text,
    make_session,
    normalize_price,
    page_numbers_from_soup,
    product_links_from_soup,
    selected_attr,
    selected_text,
    write_product_json,
)
from browser_fallback_utils import _env_int, _make_browser_options, browser_fallback_enabled


BASE_URL = "https://infosep.cl"
CATEGORY_QUERIES = {
    "OperatingSystem": [{"category": 456}],
    "UPS": [{"category": 247}],
    "Headphones": [{"category": "147,184"}],
    "Mouse": [{"category": "156,158"}],
    "Keyboard": [{"category": "312,189"}],
    "Mouse_Keyboard": [{"category": "222,183"}],
    "Storage": [{"category": "404,191,255,378,406,407"}],
    "ExternalStorage": [{"category": "405,226"}],
    "Monitor": [{"category": "160,159"}],
    "CPUCooler": [{"category": "418,419"}],
    "ThermalCompound": [{"category": 248}],
    "PowerSupply": [{"category": "187,411,235"}],
    "Case": [{"category": "179,216"}],
    "Memory": [{"category": 129}],
    "CPU": [{"category": 509}],
    "Motherboard": [{"category": "208,415"}],
    "Webcam": [{"category": 215}],
    "NetworkAdapter": [{"category": "458,214"}],
}
CATEGORY_URL_MAP = {
    "OperatingSystem": "https://infosep.cl/categoria-producto/software/windows-server-2022-std-rock-ams-hp/",
    "UPS": "https://infosep.cl/categoria-producto/partes-y-piezas/ups-respaldo-de-energia/",
    "Headphones": [
        "https://infosep.cl/categoria-producto/accesorios/audifonos/",
        "https://infosep.cl/categoria-producto/gamer/audifonos-gamer/",
    ],
    "Mouse": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/mouse-2/",
        "https://infosep.cl/categoria-producto/gamer/mouse-gamer/",
    ],
    "Keyboard": [
        "https://infosep.cl/categoria-producto/accesorios/accesorios-de-escritorio/teclado/",
        "https://infosep.cl/categoria-producto/gamer/teclado-gamer/",
    ],
    "Mouse_Keyboard": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/kit-teclado-y-mouse/",
        "https://infosep.cl/categoria-producto/gamer/teclado-y-mouse-gamer/",
    ],
    "Storage": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/almacenamiento/disco-interno/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/disco-hdd/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/discos-ssd/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/discos-ssd-m2/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/almacenamiento/disco-vigilancia/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/memorias-sd/",
    ],
    "ExternalStorage": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/almacenamiento/disco-externo/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/discos-externos-25/",
    ],
    "Monitor": [
        "https://infosep.cl/categoria-producto/monitores/",
        "https://infosep.cl/categoria-producto/gamer/monitor-gamer/",
    ],
    "CPUCooler": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/partes-de-computador/tarjeta-madre/cooler-liquido/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/partes-de-computador/tarjeta-madre/ventilador-de-cpu/",
    ],
    "ThermalCompound": "https://infosep.cl/categoria-producto/accesorios/pasta-disipadora/",
    "PowerSupply": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/fuente-de-poder-pc/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/partes-de-computador/fuentes-de-poder/",
        "https://infosep.cl/categoria-producto/gamer/fuentes-gamer/",
    ],
    "Case": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/gabinetes/",
        "https://infosep.cl/categoria-producto/gamer/gabinetes-gamer/",
    ],
    "Memory": "https://infosep.cl/categoria-producto/partes-y-piezas/memorias-pc-notebook/",
    "CPU": "https://infosep.cl/categoria-producto/partes-y-piezas/procesadores/",
    "Motherboard": [
        "https://infosep.cl/categoria-producto/partes-y-piezas/placas-madres/",
        "https://infosep.cl/categoria-producto/partes-y-piezas/partes-de-computador/tarjeta-madre/tarjeta-madre-asus/",
    ],
    "Webcam": "https://infosep.cl/categoria-producto/accesorios/camara-web/",
    "NetworkAdapter": [
        "https://infosep.cl/categoria-producto/servidores/redes-servidores/adaptador-de-red/",
        "https://infosep.cl/categoria-producto/accesorios/adaptadores/",
    ],
}


def configured_max_products() -> int:
    raw_value = os.environ.get("INFOSEP_MAX_PRODUCTS") or os.environ.get("SCRAPER_MAX_PRODUCTS") or "0"
    try:
        return max(0, int(raw_value))
    except ValueError:
        return 0


def clean_infosep_part_number(value: Any) -> str | None:
    part_number = clean_part_number(value)
    if not part_number:
        return None

    part_number = html.unescape(part_number).replace("\u2011", "-").strip()
    part_number = re.sub(r"\s+", " ", part_number)

    internal_prefix = re.match(r"^\d{4,6}\s*[-\u2013\u2014]\s*(.+)$", part_number)
    if internal_prefix:
        part_number = re.sub(r"\s+", "", internal_prefix.group(1).strip())

    compact = re.sub(r"[^A-Za-z0-9]", "", part_number)
    if len(compact) < 3 or compact.isdigit():
        return None
    if part_number.upper() in {"N/A", "NA", "NONE", "NULL", "SIN SKU", "SKU NO INFORMADO"}:
        return None

    return part_number


def normalize_infosep_price(value: Any) -> str:
    text = html_to_text(value)
    if not text:
        return "N/A"

    prices: list[int] = []
    for match in re.finditer(r"\$\s*([0-9][0-9.\s,]*)", text):
        digits = re.sub(r"\D", "", match.group(1))
        if not digits:
            continue
        price = int(digits)
        if 0 < price < 100_000_000:
            prices.append(price)

    if prices:
        return str(min(prices))

    return normalize_price(value)


def normalize_infosep_availability(*values: Any) -> str:
    """Return an explicit public stock state without guessing from price or markup."""

    unavailable = False
    available = False

    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            unavailable = unavailable or not value
            available = available or value
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            unavailable = unavailable or value <= 0
            available = available or value > 0
            continue

        if isinstance(value, (list, tuple, set)):
            text = " ".join(str(item) for item in value)
        else:
            text = html_to_text(value)
        folded = unicodedata.normalize("NFKD", text.casefold())
        folded = folded.encode("ascii", "ignore").decode("ascii")
        folded = re.sub(r"[^a-z0-9]+", " ", folded).strip()
        if not folded:
            continue

        if any(
            marker in folded
            for marker in (
                "sin existencias",
                "sin stock",
                "out of stock",
                "agotado",
                "no disponible",
                "stock agotado",
            )
        ):
            unavailable = True
        elif any(
            marker in folded
            for marker in (
                "hay existencias",
                "en stock",
                "in stock",
                "disponible",
            )
        ):
            available = True

    # A negative signal wins if InfoSep returns contradictory markup/API fields.
    if unavailable:
        return "unavailable"
    if available:
        return "available"
    return "unknown"


def infosep_html_availability(soup: BeautifulSoup) -> str:
    stock_element = soup.select_one(
        ".summary p.stock, p.stock, .wd_single_product_stock_status .stock, "
        ".wd_single_product_stock_status"
    )
    if stock_element is None:
        return "unknown"
    return normalize_infosep_availability(
        stock_element.get_text(" ", strip=True),
        stock_element.get("class", []),
    )


def product_to_output(product: dict[str, Any], category_name: str) -> dict[str, Any] | None:
    url = product.get("permalink") or ""
    name = html_to_text(product.get("name"))
    if not url or not name:
        return None

    part_number = clean_infosep_part_number(product.get("sku"))
    if not part_number:
        return None

    prices = product.get("prices") or {}
    normalized_price = normalize_infosep_price(prices.get("price"))
    if normalized_price in {"N/A", "0"}:
        return None
    return {
        "store_name": "InfoSep",
        "scraped_name": name,
        "scraped_brand": brand_from_wc(product),
        "type": category_name,
        "part #": part_number,
        "price": normalized_price,
        "availability": normalize_infosep_availability(
            product.get("is_in_stock"),
            product.get("stock_status"),
            product.get("availability_html"),
        ),
        "url": url,
        "image_url": first_image_from_wc(product),
    }


def parse_infosep_html_product(
    soup: BeautifulSoup,
    url: str,
    category_name: str,
    base_url: str,
) -> dict[str, Any] | None:
    name = selected_text(soup, ("h1.product_title", "h1.entry-title", "h1"))
    if not name:
        return None

    sku = selected_text(soup, (".sku_wrapper .sku", "span.sku", ".product_meta .sku"))
    part_number = clean_infosep_part_number(sku)
    if not part_number:
        return None

    price = selected_text(
        soup,
        (
            "p.price ins .woocommerce-Price-amount",
            "p.price ins",
            ".summary .price ins .woocommerce-Price-amount",
            ".summary .price ins",
            "p.price .woocommerce-Price-amount",
            "p.price",
            ".summary .price",
            ".price",
        ),
    )
    image = selected_attr(
        soup,
        (
            ".woocommerce-product-gallery__image img",
            "img.wp-post-image",
            ".product-image-summary img",
        ),
        "src",
    )
    normalized_price = normalize_infosep_price(price)
    if normalized_price in {"N/A", "0"}:
        return None

    return {
        "store_name": "InfoSep",
        "scraped_name": name,
        "scraped_brand": "N/A",
        "type": category_name,
        "part #": part_number,
        "price": normalized_price,
        "availability": infosep_html_availability(soup),
        "url": url,
        "image_url": absolute_url(base_url, image),
    }


def scrape_infosep_html(output_dir: str) -> int:
    output_path = clean_output_dir(output_dir)
    session = make_session(BASE_URL)
    max_products = configured_max_products()
    saved_count = 0
    seen: set[tuple[str, str]] = set()
    request_delay = float(os.environ.get("HTML_REQUEST_DELAY_SECONDS", "0.25"))
    link_selectors = (
        "a.product-image-link[href*='/producto/']",
        "h3.wd-entities-title a[href*='/producto/']",
        ".product-grid-item a[href*='/producto/']",
    )
    pagination_selectors = (
        "nav.woocommerce-pagination a",
        "ul.page-numbers a",
        ".page-numbers a",
    )

    for category_name, raw_urls in CATEGORY_URL_MAP.items():
        urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
        for category_url in urls:
            try:
                first_html = fetch_text_with_referer(session, category_url, BASE_URL)
                first_soup = BeautifulSoup(first_html, "html.parser")
                page_numbers = page_numbers_from_soup(first_soup, pagination_selectors)
                total_pages = max(page_numbers) if page_numbers else 1

                for page in range(1, total_pages + 1):
                    page_url = build_woocommerce_page_url(category_url, page)
                    if page == 1:
                        soup = first_soup
                    else:
                        page_html = fetch_text_with_referer(session, page_url, category_url)
                        soup = BeautifulSoup(page_html, "html.parser")

                    links = product_links_from_soup(
                        soup,
                        BASE_URL,
                        link_selectors,
                        url_pattern=r"/producto/",
                    )
                    print(
                        f"InfoSep {category_name} HTML page {page}/{total_pages}: "
                        f"{len(links)} product links"
                    )

                    for url in links:
                        identity = (category_name, url)
                        if identity in seen:
                            continue
                        seen.add(identity)

                        try:
                            product_html = fetch_text_with_referer(session, url, page_url)
                            product_soup = BeautifulSoup(product_html, "html.parser")
                            data = parse_infosep_html_product(
                                product_soup,
                                url,
                                category_name,
                                BASE_URL,
                            )
                            if not data:
                                continue

                            write_product_json(output_path, "IS", url, data)
                            saved_count += 1

                            if max_products and saved_count >= max_products:
                                print(f"InfoSep reached INFOSEP_MAX_PRODUCTS={max_products}; stopping early.")
                                return saved_count
                        except Exception as exc:
                            print(f"InfoSep {category_name}: error scraping product {url}: {exc}")

                    if request_delay:
                        time.sleep(request_delay)
            except Exception as exc:
                print(f"InfoSep {category_name}: HTML fallback failed for {category_url}: {exc}")

    return saved_count


def scrape_infosep_requests(output_dir: str) -> int:
    output_path = clean_output_dir(output_dir)
    session = make_session(BASE_URL)
    api_url = urljoin(BASE_URL, "/wp-json/wc/store/v1/products")
    max_products = configured_max_products()
    saved_count = 0
    skipped_without_part = 0
    seen_urls: set[str] = set()

    for category_name, query_list in CATEGORY_QUERIES.items():
        for query in query_list:
            page = 1
            while True:
                params = {"per_page": 100, "page": page, **query}
                products, response = fetch_json(session, api_url, params=params)
                if not isinstance(products, list):
                    raise RuntimeError(f"Unexpected InfoSep response: {products!r}")

                total_pages = int(response.headers.get("X-WP-TotalPages", "1") or "1")
                print(f"InfoSep {category_name} page {page}/{total_pages}: {len(products)} products")

                for product in products:
                    url = product.get("permalink") or ""
                    if not url or url in seen_urls:
                        continue

                    data = product_to_output(product, category_name)
                    if not data:
                        skipped_without_part += 1
                        continue

                    seen_urls.add(url)
                    write_product_json(output_path, "IS", url, data)
                    saved_count += 1

                    if max_products and saved_count >= max_products:
                        print(f"InfoSep reached INFOSEP_MAX_PRODUCTS={max_products}; stopping early.")
                        print(
                            f"InfoSep scraping finished. Saved {saved_count} JSON files; "
                            f"skipped {skipped_without_part} products without usable part number."
                        )
                        return saved_count

                if page >= total_pages:
                    break
                page += 1

    print(
        f"InfoSep scraping finished. Saved {saved_count} JSON files; "
        f"skipped {skipped_without_part} products without usable part number."
    )
    return saved_count


def _pydoll_value(result: dict[str, Any] | None) -> str:
    return (((result or {}).get("result") or {}).get("result") or {}).get("value") or ""


async def _json_from_page(page: Any, script: str) -> Any:
    raw_value = _pydoll_value(await page.execute_script(script))
    return json.loads(raw_value or "null")


async def _infosep_browser_api_page(
    page: Any,
    params: dict[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    api_url = f"{urljoin(BASE_URL, '/wp-json/wc/store/v1/products')}?{urlencode(params)}"
    result = await _json_from_page(
        page,
        f"""
const url = {json.dumps(api_url)};
try {{
  const response = await fetch(url, {{
    credentials: "include",
    headers: {{ Accept: "application/json" }}
  }});
  const body = await response.text();
  let products = null;
  try {{ products = JSON.parse(body); }} catch (_) {{}}
  return JSON.stringify({{
    ok: response.ok && Array.isArray(products),
    status: response.status,
    totalPages: Number(response.headers.get("X-WP-TotalPages") || "1"),
    products,
    bodySample: body.slice(0, 160)
  }});
}} catch (error) {{
  return JSON.stringify({{ ok: false, status: 0, error: String(error) }});
}}
""",
    ) or {}
    if not result.get("ok"):
        raise RuntimeError(
            "browser Store API failed "
            f"status={result.get('status')} "
            f"error={result.get('error') or result.get('bodySample') or 'unknown'}"
        )
    products = result.get("products")
    if not isinstance(products, list):
        raise RuntimeError("browser Store API returned a non-list payload")
    try:
        total_pages = max(1, int(result.get("totalPages") or 1))
    except (TypeError, ValueError):
        total_pages = 1
    return products, total_pages


async def _scrape_infosep_browser_store_api(
    page: Any,
    output_path: str,
    max_products: int,
) -> tuple[int, bool]:
    """Use the authorized browser session for fast same-origin Store API calls."""

    saved_count = 0
    skipped_without_part = 0
    seen_urls: set[str] = set()

    for category_name, query_list in CATEGORY_QUERIES.items():
        for query in query_list:
            page_number = 1
            total_pages = 1
            while page_number <= total_pages:
                try:
                    products, total_pages = await _infosep_browser_api_page(
                        page,
                        {"per_page": 100, "page": page_number, **query},
                    )
                except Exception as exc:
                    print(
                        f"InfoSep browser Store API failed for {category_name} "
                        f"page {page_number}: {exc}"
                    )
                    return saved_count, False

                print(
                    f"InfoSep browser Store API {category_name} "
                    f"page {page_number}/{total_pages}: {len(products)} products"
                )
                for product in products:
                    url = product.get("permalink") or ""
                    if not url or url in seen_urls:
                        continue
                    data = product_to_output(product, category_name)
                    if not data:
                        skipped_without_part += 1
                        continue
                    seen_urls.add(url)
                    write_product_json(output_path, "IS", url, data)
                    saved_count += 1
                    if max_products and saved_count >= max_products:
                        print(
                            f"InfoSep browser Store API reached max products={max_products}."
                        )
                        return saved_count, True
                page_number += 1

    print(
        "InfoSep browser Store API finished. "
        f"Saved {saved_count}; skipped {skipped_without_part} without usable part number."
    )
    return saved_count, saved_count > 0


async def _wait_for_infosep_category(page: Any, timeout_seconds: int) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    last_state: dict[str, Any] = {}
    while asyncio.get_running_loop().time() < deadline:
        last_state = await _json_from_page(
            page,
            r"""
const clean = (value) => (value || "").replace(/\s+/g, " ").trim();
return JSON.stringify({
  title: document.title || "",
  body: clean(document.body?.innerText || "").slice(0, 500),
  productCount: document.querySelectorAll(
    ".product-grid-item, .wd-product, li.product, .products .product, a.product-image-link[href*='/producto/'], h3.wd-entities-title a[href*='/producto/']"
  ).length,
  paginationCount: document.querySelectorAll("nav.woocommerce-pagination a, .page-numbers a, a.page-numbers").length
});
""",
        ) or {}
        body = f"{last_state.get('title', '')} {last_state.get('body', '')}".lower()
        if last_state.get("productCount"):
            return last_state
        if "access denied" in body or "forbidden" in body or "403" in body:
            return last_state
        await asyncio.sleep(1)
    return last_state


async def _infosep_page_count(page: Any) -> int:
    value = await _json_from_page(
        page,
        r"""
const numbers = new Set([1]);
for (const element of document.querySelectorAll("nav.woocommerce-pagination a, .page-numbers a, a.page-numbers, link[rel='next']")) {
  const text = (element.textContent || "").trim();
  if (/^\d+$/.test(text)) numbers.add(Number(text));
  const href = element.href || element.getAttribute("href") || "";
  for (const match of href.matchAll(/\/page\/(\d+)\/?/g)) numbers.add(Number(match[1]));
}
return JSON.stringify(Math.max(...numbers));
""",
    )
    try:
        return max(1, int(value or 1))
    except (TypeError, ValueError):
        return 1


async def _infosep_listing_links(page: Any) -> list[str]:
    return await _json_from_page(
        page,
        r"""
const urls = new Set();
const selectors = [
  "a.product-image-link[href*='/producto/']",
  "h3.wd-entities-title a[href*='/producto/']",
  ".product-grid-item a[href*='/producto/']",
  ".wd-product a[href*='/producto/']",
  "li.product a[href*='/producto/']",
  "a[href*='/producto/']"
];
for (const selector of selectors) {
  for (const link of document.querySelectorAll(selector)) {
    const href = link.href || "";
    if (!href.includes("/producto/")) continue;
    if (link.classList.contains("open-quick-view")) continue;
    if (href.includes("#")) continue;
    urls.add(href.split("?")[0]);
  }
}
return JSON.stringify([...urls]);
""",
    ) or []


async def _wait_for_infosep_product(page: Any, timeout_seconds: int) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    last_state: dict[str, Any] = {}
    while asyncio.get_running_loop().time() < deadline:
        last_state = await _json_from_page(
            page,
            r"""
const clean = (value) => (value || "").replace(/\s+/g, " ").trim();
return JSON.stringify({
  title: document.title || "",
  body: clean(document.body?.innerText || "").slice(0, 600),
  hasName: Boolean(document.querySelector("h1.product_title, h1.entry-title, h1")),
  hasSku: Boolean(document.querySelector(".sku_wrapper .sku, span.sku, .product_meta .sku")),
  hasPrice: Boolean(document.querySelector("p.price, .summary .price, .price"))
});
""",
        ) or {}
        body = f"{last_state.get('title', '')} {last_state.get('body', '')}".lower()
        if last_state.get("hasName") and (last_state.get("hasSku") or last_state.get("hasPrice")):
            return last_state
        if "access denied" in body or "forbidden" in body or "403" in body:
            return last_state
        await asyncio.sleep(1)
    return last_state


async def _infosep_product_detail(page: Any) -> dict[str, str]:
    return await _json_from_page(
        page,
        r"""
const clean = (value) => (value || "").replace(/\s+/g, " ").trim();
const text = document.body?.innerText || "";
const firstMatch = (...patterns) => {
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match?.[1]) return clean(match[1]);
  }
  return "";
};
const image =
  document.querySelector(".woocommerce-product-gallery__image img[src]")?.src ||
  document.querySelector("img.wp-post-image[src]")?.src ||
  document.querySelector(".product-image-summary img[src]")?.src ||
  document.querySelector("meta[property='og:image']")?.content ||
  "";
return JSON.stringify({
  name: clean(
    document.querySelector("h1.product_title")?.textContent ||
    document.querySelector("h1.entry-title")?.textContent ||
    document.querySelector("h1")?.textContent
  ),
  part_number: clean(
    document.querySelector(".sku_wrapper .sku")?.textContent ||
    document.querySelector("span.sku")?.textContent ||
    document.querySelector(".product_meta .sku")?.textContent ||
    firstMatch(/\bSKU\s*:?\s*([^\n\r|]+)/i, /\bMPN\s*:?\s*([^\n\r|]+)/i, /\bModelo\s*:?\s*([^\n\r|]+)/i)
  ),
  price: clean(
    document.querySelector("p.price ins .woocommerce-Price-amount")?.textContent ||
    document.querySelector("p.price ins")?.textContent ||
    document.querySelector(".summary .price ins .woocommerce-Price-amount")?.textContent ||
    document.querySelector(".summary .price ins")?.textContent ||
    document.querySelector("p.price .woocommerce-Price-amount")?.textContent ||
    document.querySelector("p.price")?.textContent ||
    document.querySelector(".summary .price")?.textContent ||
    document.querySelector(".price")?.textContent
  ),
  stock_text: clean(
    document.querySelector(".summary p.stock")?.textContent ||
    document.querySelector("p.stock")?.textContent ||
    document.querySelector(".wd_single_product_stock_status .stock")?.textContent ||
    document.querySelector(".wd_single_product_stock_status")?.textContent
  ),
  stock_class: [
    ...(document.querySelector(".summary p.stock")?.classList || []),
    ...(document.querySelector("p.stock")?.classList || []),
    ...(document.querySelector(".wd_single_product_stock_status .stock")?.classList || [])
  ].join(" "),
  image_url: image
});
""",
    ) or {}


async def _scrape_infosep_product_browser(
    sem: asyncio.Semaphore,
    browser: Chrome,
    *,
    category_name: str,
    url: str,
    output_path: str,
    ready_timeout: int,
) -> bool:
    async with sem:
        page = None
        try:
            page = await browser.new_tab()
            await page.go_to(url)
            state = await _wait_for_infosep_product(page, ready_timeout)
            if not state.get("hasName"):
                print(
                    f"InfoSep browser product not ready: {url} "
                    f"title={state.get('title', '')!r} body={state.get('body', '')[:160]!r}"
                )
                return False

            detail = await _infosep_product_detail(page)
            name = html_to_text(detail.get("name"))
            part_number = clean_infosep_part_number(detail.get("part_number"))
            if not name or not part_number:
                print(f"InfoSep browser skipped without name/SKU: {url}")
                return False

            data = {
                "store_name": "InfoSep",
                "scraped_name": name,
                "scraped_brand": "N/A",
                "type": category_name,
                "part #": part_number,
                "price": normalize_infosep_price(detail.get("price")),
                "availability": normalize_infosep_availability(
                    detail.get("stock_text"),
                    detail.get("stock_class"),
                ),
                "url": url,
                "image_url": absolute_url(BASE_URL, detail.get("image_url")),
            }
            write_product_json(output_path, "IS", url, data)
            return True
        except Exception as exc:
            print(f"InfoSep browser product error {url}: {exc}")
            return False
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception as exc:
                    print(f"InfoSep browser product close warning {url}: {exc}")


async def _scrape_infosep_browser_async(output_dir: str) -> int:
    output_path = clean_output_dir(output_dir)
    options = _make_browser_options()
    browser = Chrome(options=options)
    await browser.start()
    page = None
    try:
        page = await browser.new_tab()
        ready_timeout = _env_int("INFOSEP_BROWSER_READY_TIMEOUT", 25)
        product_timeout = _env_int("INFOSEP_BROWSER_PRODUCT_TIMEOUT", 15)
        scraper_concurrency = _env_int("INFOSEP_BROWSER_SCRAPER_CONCURRENCY", 4)
        chunk_size = _env_int("INFOSEP_BROWSER_CHUNK_SIZE", max(40, scraper_concurrency * 10))
        print(
            "InfoSep browser settings: "
            f"category_timeout={ready_timeout}s "
            f"product_timeout={product_timeout}s "
            f"scraper_concurrency={scraper_concurrency} "
            f"chunk_size={chunk_size}"
        )
        max_products = configured_max_products() or int(os.environ.get("BROWSER_FALLBACK_MAX_PRODUCTS", "0") or "0")

        # GitHub runner IPs can receive 403 through requests while a normal
        # browser session is accepted. Reuse that accepted session to call the
        # same public Store API and avoid navigating roughly one page per item.
        await page.go_to(BASE_URL)
        browser_api_count, browser_api_complete = await _scrape_infosep_browser_store_api(
            page,
            str(output_path),
            max_products,
        )
        if browser_api_complete:
            return browser_api_count
        print(
            "InfoSep browser Store API incomplete; falling back to category and product pages."
        )

        links: list[tuple[str, str]] = []
        seen_urls: set[str] = set()

        for category_name, raw_urls in CATEGORY_URL_MAP.items():
            urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
            for category_url in urls:
                await page.go_to(category_url)
                state = await _wait_for_infosep_category(page, ready_timeout)
                total_pages = await _infosep_page_count(page) if state.get("productCount") else 1
                print(
                    f"InfoSep browser {category_name}: {total_pages} page(s) from {category_url} "
                    f"products_seen={state.get('productCount', 0)} title={state.get('title', '')!r}"
                )
                if not state.get("productCount"):
                    print(f"InfoSep browser category body sample: {state.get('body', '')[:180]!r}")
                    continue

                for page_number in range(1, total_pages + 1):
                    if page_number > 1:
                        await page.go_to(build_woocommerce_page_url(category_url, page_number))
                        state = await _wait_for_infosep_category(page, ready_timeout)
                        if not state.get("productCount"):
                            print(f"InfoSep browser {category_name} page {page_number}: no products after wait")
                            continue

                    page_links = await _infosep_listing_links(page)
                    new_count = 0
                    for url in page_links:
                        if url in seen_urls:
                            continue
                        seen_urls.add(url)
                        links.append((category_name, url))
                        new_count += 1
                    print(
                        f"InfoSep browser {category_name} page {page_number}/{total_pages}: "
                        f"{new_count} new links"
                    )

                    if max_products and len(links) >= max_products:
                        links = links[:max_products]
                        print(f"InfoSep browser reached max products={max_products}; stopping collection.")
                        break
                if max_products and len(links) >= max_products:
                    break
            if max_products and len(links) >= max_products:
                break

        print(f"InfoSep browser collected {len(links)} product links")
        if not links:
            return 0

        sem = asyncio.Semaphore(scraper_concurrency)
        saved_count = 0
        for index in range(0, len(links), chunk_size):
            chunk = links[index : index + chunk_size]
            results = await asyncio.gather(
                *(
                    _scrape_infosep_product_browser(
                        sem,
                        browser,
                        category_name=category_name,
                        url=url,
                        output_path=str(output_path),
                        ready_timeout=product_timeout,
                    )
                    for category_name, url in chunk
                ),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, Exception):
                    print(f"InfoSep browser product task error: {result}")
                elif result:
                    saved_count += 1
            print(f"InfoSep browser saved {saved_count} JSON so far")

        return saved_count
    finally:
        if page is not None:
            try:
                await page.close()
            except Exception as exc:
                print(f"InfoSep browser collector close warning: {exc}")
        try:
            await browser.stop()
        except Exception as exc:
            print(f"InfoSep browser stop warning: {exc}")


def scrape_infosep_browser(output_dir: str) -> int:
    return asyncio.run(_scrape_infosep_browser_async(output_dir))


def main() -> int:
    output_dir = "ScrapDB/Outputs/InfoSep"
    force_browser = os.environ.get("SCRAPER_FORCE_BROWSER_FALLBACK", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    if force_browser:
        saved_count = 0
        print("InfoSep requests and HTML paths skipped; browser fallback will be used.")
    else:
        try:
            saved_count = scrape_infosep_requests(output_dir)
            print(f"InfoSep requests path saved {saved_count} JSON files.")
        except Exception as exc:
            saved_count = 0
            print(f"InfoSep requests path failed, HTML fallback will be tried: {exc}")

        if saved_count == 0:
            try:
                saved_count = scrape_infosep_html(output_dir)
                print(f"InfoSep HTML fallback saved {saved_count} JSON files.")
            except Exception as exc:
                saved_count = 0
                print(f"InfoSep HTML fallback failed, browser fallback will be tried: {exc}")

    if browser_fallback_enabled(saved_count):
        print("InfoSep starting browser fallback.")
        saved_count = scrape_infosep_browser(output_dir)
        print(f"InfoSep browser fallback saved {saved_count} JSON files.")

    print(f"InfoSep scraping finished. Saved {saved_count} JSON files.")
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

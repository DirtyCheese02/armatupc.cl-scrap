from __future__ import annotations

import hashlib
import html
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
}

PART_NUMBER_PATTERNS = [
    re.compile(r"\bMPN\s*:?\s*([^,()|;\n\r<]+)", re.IGNORECASE),
    re.compile(r"N[uú]mero de parte\s*:?\s*([^|,;\n\r<]+)", re.IGNORECASE),
    re.compile(r"\bPart\s*(?:number|#)\s*:?\s*([^|,;\n\r<]+)", re.IGNORECASE),
    re.compile(r"\bModelo\s*:?\s*([^|,;\n\r<]+)", re.IGNORECASE),
]

NAME_PART_PATTERN = re.compile(
    r"\b(?=[A-Z0-9][A-Z0-9._/-]{3,39}\b)"
    r"(?=[A-Z0-9._/-]*[A-Z])"
    r"(?=[A-Z0-9._/-]*\d)"
    r"[A-Z0-9][A-Z0-9._/-]*\b"
)

NAME_PART_BLACKLIST_PREFIXES = (
    "DDR",
    "USB",
    "SATA",
    "PCIE",
    "PCI-E",
    "NVME",
    "HDMI",
    "ARGB",
    "RGB",
    "ATX",
    "MATX",
    "M-ATX",
    "ITX",
)


def make_session(base_url: str | None = None) -> requests.Session:
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    if base_url:
        session.headers["Referer"] = base_url
    return session


def fetch_json(
    session: requests.Session,
    url: str,
    *,
    method: str = "GET",
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    retries: int = 3,
    timeout: int = 30,
) -> tuple[Any, requests.Response]:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = session.request(
                method,
                url,
                params=params,
                json=json_body,
                data=data,
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json(), response
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < retries:
                delay = 1.5 * attempt
                response = getattr(exc, "response", None)
                if response is not None and response.status_code in (403, 429):
                    delay = 10 * attempt
                time.sleep(delay)

    raise RuntimeError(f"Failed to fetch JSON from {url}: {last_error}") from last_error


def fetch_text(
    session: requests.Session,
    url: str,
    *,
    retries: int = 3,
    timeout: int = 30,
) -> str:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            if attempt < retries:
                delay = 1.5 * attempt
                response = getattr(exc, "response", None)
                if response is not None and response.status_code in (403, 429):
                    delay = 10 * attempt
                time.sleep(delay)

    raise RuntimeError(f"Failed to fetch HTML from {url}: {last_error}") from last_error


def clean_output_dir(output_dir: str | Path) -> Path:
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    for file_path in path.glob("*.json"):
        file_path.unlink()
    return path


def write_product_json(output_dir: str | Path, prefix: str, url: str, data: dict[str, Any]) -> Path:
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    file_path = path / f"{prefix}_{hashlib.md5(url.encode()).hexdigest()}.json"
    file_path.write_text(json.dumps(data, ensure_ascii=False, indent=4), encoding="utf-8")
    return file_path


def html_to_text(value: Any) -> str:
    if value is None:
        return ""
    text = html.unescape(str(value)).replace("\xa0", " ")
    if "<" in text and ">" in text:
        text = BeautifulSoup(text, "lxml").get_text(" ")
    return re.sub(r"\s+", " ", text).strip()


def clean_part_number(value: Any) -> str | None:
    text = html_to_text(value)
    if not text:
        return None
    text = re.sub(
        r"^(?:sku|mpn|part\s*(?:number|#)|n[uú]mero de parte|modelo|referencia)\s*:?\s*",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = text.strip(" \t\r\n,;|()[]{}")
    if not text or text.upper() in {"N/A", "NA", "NONE", "NULL", "ERROR", "SIN SKU"}:
        return None
    return text


def extract_explicit_part_number(*values: Any) -> str | None:
    for value in values:
        text = html_to_text(value)
        if not text:
            continue
        for pattern in PART_NUMBER_PATTERNS:
            match = pattern.search(text)
            if match:
                cleaned = clean_part_number(match.group(1))
                if cleaned:
                    return cleaned
    return None


def infer_part_number_from_name(value: Any) -> str | None:
    text = html_to_text(value).upper()
    if not text:
        return None
    candidates = []
    for candidate in NAME_PART_PATTERN.findall(text):
        if candidate.startswith(NAME_PART_BLACKLIST_PREFIXES):
            continue
        if candidate.count("-") > 5:
            continue
        candidates.append(candidate)
    return max(candidates, key=len) if candidates else None


def pick_part_number(
    primary_values: list[Any] | tuple[Any, ...] = (),
    fallback_values: list[Any] | tuple[Any, ...] = (),
    *,
    allow_name_fallback: bool = True,
) -> str | None:
    for value in primary_values:
        cleaned = clean_part_number(value)
        if cleaned:
            return cleaned

    explicit = extract_explicit_part_number(*fallback_values)
    if explicit:
        return explicit

    if allow_name_fallback:
        for value in fallback_values:
            inferred = infer_part_number_from_name(value)
            if inferred:
                return inferred

    return None


def normalize_price(value: Any) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, (int, float)):
        return str(int(value))
    text = html_to_text(value)
    digits = re.sub(r"\D", "", text)
    return digits or "N/A"


def absolute_url(base_url: str, value: Any) -> str:
    if not value:
        return "N/A"
    return urljoin(base_url, html.unescape(str(value)).strip())


def first_image_from_wc(product: dict[str, Any]) -> str:
    images = product.get("images") or []
    if not images:
        return "N/A"
    return images[0].get("src") or images[0].get("thumbnail") or "N/A"


def brand_from_wc(product: dict[str, Any]) -> str:
    brands = product.get("brands") or []
    if brands and brands[0].get("name"):
        return html_to_text(brands[0]["name"])

    for attr in product.get("attributes") or []:
        name = html_to_text(attr.get("name")).lower()
        if "marca" not in name and "brand" not in name:
            continue
        terms = attr.get("terms") or attr.get("values") or []
        if terms:
            first = terms[0]
            if isinstance(first, dict):
                return html_to_text(first.get("name") or first.get("value")) or "N/A"
            return html_to_text(first) or "N/A"

    return "N/A"


def run_woocommerce_store(
    *,
    store_name: str,
    base_url: str,
    category_queries: dict[str, list[dict[str, Any]]],
    output_dir: str,
    output_prefix: str,
) -> int:
    output_path = clean_output_dir(output_dir)
    session = make_session(base_url)
    api_url = urljoin(base_url, "/wp-json/wc/store/v1/products")
    saved_count = 0
    skipped_without_part = 0
    seen: set[tuple[str, str]] = set()

    for category_name, query_list in category_queries.items():
        for query in query_list:
            page = 1
            while True:
                params = {"per_page": 100, "page": page, **query}
                products, response = fetch_json(session, api_url, params=params)
                if not isinstance(products, list):
                    raise RuntimeError(f"Unexpected WooCommerce response for {store_name}: {products!r}")

                total_pages = int(response.headers.get("X-WP-TotalPages", "1") or "1")
                print(f"{store_name} {category_name} page {page}/{total_pages}: {len(products)} products")

                for product in products:
                    url = product.get("permalink") or ""
                    name = html_to_text(product.get("name"))
                    if not url or not name:
                        continue

                    identity = (category_name, url)
                    if identity in seen:
                        continue
                    seen.add(identity)

                    images = product.get("images") or []
                    image_texts = []
                    for image in images:
                        image_texts.extend([image.get("alt"), image.get("name")])
                    part_number = pick_part_number(
                        [product.get("sku")],
                        [
                            *image_texts,
                            product.get("short_description"),
                            product.get("description"),
                            name,
                        ],
                    )
                    if not part_number:
                        skipped_without_part += 1
                        continue

                    prices = product.get("prices") or {}
                    data = {
                        "store_name": store_name,
                        "scraped_name": name,
                        "scraped_brand": brand_from_wc(product),
                        "type": category_name,
                        "part #": part_number,
                        "price": normalize_price(prices.get("price")),
                        "url": url,
                        "image_url": first_image_from_wc(product),
                    }
                    write_product_json(output_path, output_prefix, url, data)
                    saved_count += 1

                if page >= total_pages:
                    break
                page += 1

    print(
        f"{store_name} scraping finished. Saved {saved_count} JSON files; "
        f"skipped {skipped_without_part} products without part number."
    )
    return saved_count


def add_or_replace_query_params(url: str, params: dict[str, Any]) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    for key, value in params.items():
        query[key] = [str(value)]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def _decode_filter_value(raw_value: str) -> list[str] | str:
    value = raw_value.replace("%%", "%").replace("$%", "%").replace("$", "")
    previous = None
    while previous != value:
        previous = value
        value = html.unescape(value)
        value = unquote(value)
    value = value.replace("%", "")

    if value.startswith("[") and value.endswith("]"):
        items = re.findall(r'"([^"]+)"|(\d+)', value)
        flattened = [left or right for left, right in items if left or right]
        if flattened:
            return flattened
    return value


def parse_url_filters(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    filters: dict[str, Any] = {}
    for key, values in parse_qs(parsed.query, keep_blank_values=True).items():
        if not key.startswith("filtro_") and not key.startswith("filter_"):
            continue
        if not values:
            continue
        filters[key] = _decode_filter_value(values[0])
    return filters


def parse_sphinx_form(html_content: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html_content, "lxml")
    form = soup.find("form", id="filtroShop")
    if form is None:
        return None

    params: dict[str, Any] = {}
    for input_tag in form.find_all("input"):
        name = input_tag.get("name")
        if not name:
            continue
        params[name] = input_tag.get("value", "")
    return params


def run_sphinx_store(
    *,
    store_name: str,
    base_url: str,
    service_url: str,
    category_url_map: dict[str, Any],
    output_dir: str,
    output_prefix: str,
) -> int:
    output_path = clean_output_dir(output_dir)
    session = make_session(base_url)
    session.headers.update({"Origin": base_url, "Referer": base_url})
    request_delay = float(os.environ.get("SPHINX_REQUEST_DELAY_SECONDS", "0.75"))
    saved_count = 0

    for category_name, raw_urls in category_url_map.items():
        urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
        for category_url in urls:
            try:
                if "/producto/" in urlparse(category_url).path:
                    print(f"{store_name} {category_name}: skipped non-listing URL {category_url}")
                    continue

                html_content = fetch_text(session, category_url)
                base_params = parse_sphinx_form(html_content)
                if not base_params:
                    print(f"{store_name} {category_name}: skipped non-listing URL {category_url}")
                    continue

                base_params.update(parse_url_filters(category_url))
                base_params["page"] = "1"
                first_response, _ = fetch_json(
                    session,
                    service_url,
                    method="POST",
                    json_body=base_params,
                )
                result = first_response.get("resultado", {})
                products_info = result.get("productos") or {}
                items = result.get("items") or []
                total_items = int(products_info.get("count") or len(items) or 0)
                page_size = max(len(items), 1)
                total_pages = max(math.ceil(total_items / page_size), 1)

                for page in range(1, total_pages + 1):
                    if page == 1:
                        page_items = items
                    else:
                        params = {**base_params, "page": str(page)}
                        response, _ = fetch_json(
                            session,
                            service_url,
                            method="POST",
                            json_body=params,
                        )
                        page_items = (response.get("resultado") or {}).get("items") or []
                    if request_delay:
                        time.sleep(request_delay)

                    print(
                        f"{store_name} {category_name} page {page}/{total_pages}: "
                        f"{len(page_items)} products"
                    )
                    for item in page_items:
                        url = absolute_url(base_url, item.get("url"))
                        name = html_to_text(item.get("nombre"))
                        if not name or url == "N/A":
                            continue

                        part_number = pick_part_number(
                            [item.get("partno"), item.get("codigo")],
                            [name, item.get("texto")],
                        ) or "N/A"
                        image_url = absolute_url(base_url, item.get("foto") or item.get("fotoMini"))
                        data = {
                            "store_name": store_name,
                            "scraped_name": name,
                            "scraped_brand": html_to_text(item.get("marca")) or "N/A",
                            "type": category_name,
                            "part #": part_number,
                            "price": normalize_price(item.get("precio")),
                            "url": url,
                            "image_url": image_url,
                        }
                        write_product_json(output_path, output_prefix, url, data)
                        saved_count += 1
            except Exception as exc:
                print(f"{store_name} {category_name}: error scraping {category_url}: {exc}")

    print(f"{store_name} scraping finished. Saved {saved_count} JSON files.")
    return saved_count


def build_prestashop_xhr_url(url: str) -> str:
    return add_or_replace_query_params(url, {"from-xhr": "1"})


def image_from_prestashop(product: dict[str, Any]) -> str:
    cover = product.get("cover") or {}
    for key in ("large", "medium", "small"):
        value = cover.get(key)
        if isinstance(value, dict) and value.get("url"):
            return value["url"]

    by_size = cover.get("bySize") or {}
    for key in ("large_default", "medium_default", "home_default", "small_default"):
        value = by_size.get(key)
        if isinstance(value, dict) and value.get("url"):
            return value["url"]
    return "N/A"


def run_prestashop_xhr_store(
    *,
    store_name: str,
    base_url: str,
    category_url_map: dict[str, Any],
    output_dir: str,
    output_prefix: str,
) -> int:
    output_path = clean_output_dir(output_dir)
    session = make_session(base_url)
    session.headers.update(
        {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": base_url,
        }
    )
    saved_count = 0

    for category_name, raw_urls in category_url_map.items():
        urls = raw_urls if isinstance(raw_urls, list) else [raw_urls]
        for category_url in urls:
            try:
                next_urls = [build_prestashop_xhr_url(category_url)]
                visited_pages: set[str] = set()
                page_number = 1

                while next_urls:
                    page_url = next_urls.pop(0)
                    if page_url in visited_pages:
                        continue
                    visited_pages.add(page_url)

                    payload, _ = fetch_json(session, page_url)
                    products = payload.get("products") or []
                    pagination = payload.get("pagination") or {}
                    total_pages = int(pagination.get("pages_count") or page_number)
                    print(
                        f"{store_name} {category_name} page {page_number}/{total_pages}: "
                        f"{len(products)} products"
                    )

                    for product in products:
                        url = product.get("url") or product.get("canonical_url") or ""
                        name = html_to_text(product.get("name"))
                        if not url or not name:
                            continue

                        part_number = pick_part_number(
                            [product.get("reference")],
                            [product.get("description_short"), name],
                        ) or "N/A"
                        price_value = product.get("price_amount")
                        if price_value is None:
                            price_value = product.get("price")
                        data = {
                            "store_name": store_name,
                            "scraped_name": name,
                            "scraped_brand": html_to_text(
                                product.get("manufacturer_name") or product.get("manufacturer")
                            ) or "N/A",
                            "type": category_name,
                            "part #": part_number,
                            "price": normalize_price(price_value),
                            "url": url,
                            "image_url": image_from_prestashop(product),
                        }
                        write_product_json(output_path, output_prefix, url, data)
                        saved_count += 1

                    pages = pagination.get("pages") or {}
                    page_iter = pages.values() if isinstance(pages, dict) else pages
                    for page in page_iter:
                        if not page.get("clickable") or page.get("current"):
                            continue
                        next_url = page.get("url")
                        if next_url:
                            next_urls.append(build_prestashop_xhr_url(next_url))

                    page_number += 1
            except Exception as exc:
                print(f"{store_name} {category_name}: error scraping {category_url}: {exc}")

    print(f"{store_name} scraping finished. Saved {saved_count} JSON files.")
    return saved_count


def exit_code_from_count(count: int) -> int:
    return 0 if count > 0 else 1

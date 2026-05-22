from __future__ import annotations

import hashlib
import html
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import requests


STORE_NAME = "Centrale"
STORE_API_URL = "https://centrale.cl/wp-json/wc/store/v1/products"
PER_PAGE = 100
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("CENTRALE_REQUEST_TIMEOUT_SECONDS", "30"))
REQUEST_RETRIES = int(os.environ.get("CENTRALE_REQUEST_RETRIES", "3"))

CATEGORY_ID_MAP: dict[str, tuple[int, ...]] = {
    "OperatingSystem": (756,),
    "UPS": (760,),
    "Headphones": (739,),
    "Mouse": (12605,),
    "Keyboard": (833,),
    "Storage": (747,),
    "ExternalStorage": (719,),
    "Monitor": (12077,),
    "CPUCooler": (46947, 46949),
    "CaseFan": (46948,),
    "ThermalCompound": (45567,),
    "PowerSupply": (734,),
    "Case": (732,),
    "Memory": (746,),
    "CPU": (773,),
    "VideoCard": (765,),
    "Motherboard": (749,),
    "Webcam": (12057,),
    "NetworkAdapter": (725,),
}

HEADERS = {
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    ),
}


def normalize_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def clean_part_number(raw_value: str) -> str:
    return normalize_text(raw_value).strip(" .;:-")


def extract_part_number(product: dict[str, Any]) -> str | None:
    images = product.get("images") or []
    for image in images:
        if not isinstance(image, dict):
            continue
        alt = normalize_text(image.get("alt"))
        match = re.search(r"\bMPN\s*:?\s*([^,()]+)", alt, flags=re.IGNORECASE)
        if match:
            return clean_part_number(match.group(1))

    searchable_text = "\n".join(
        normalize_text(product.get(field))
        for field in ("short_description", "description")
    )

    patterns = [
        r"N[u\u00fa]mero de parte\s*:?\s*([^|,;\n\r<]+)",
        r"\bMPN\s*:?\s*([^|,;\n\r<]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, searchable_text, flags=re.IGNORECASE)
        if match:
            return clean_part_number(match.group(1))

    return None


def extract_brand(product: dict[str, Any]) -> str:
    brands = product.get("brands") or []
    for brand in brands:
        if isinstance(brand, dict):
            name = normalize_text(brand.get("name"))
            if name:
                return name
    return "N/A"


def extract_image_url(product: dict[str, Any]) -> str:
    images = product.get("images") or []
    for image in images:
        if isinstance(image, dict):
            src = str(image.get("src") or "").strip()
            if src:
                return src
    return "N/A"


def fetch_page(
    session: requests.Session,
    category_ids: tuple[int, ...],
    page_number: int,
) -> tuple[list[dict[str, Any]], int]:
    params = {
        "category": ",".join(str(category_id) for category_id in category_ids),
        "per_page": str(PER_PAGE),
        "page": str(page_number),
    }

    last_error: Exception | None = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            response = session.get(
                STORE_API_URL,
                params=params,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            total_pages = int(response.headers.get("X-WP-TotalPages") or "1")
            data = response.json()
            if not isinstance(data, list):
                raise ValueError(f"Unexpected API payload: {type(data).__name__}")
            return data, total_pages
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < REQUEST_RETRIES:
                time.sleep(attempt * 2)

    raise RuntimeError(
        f"Failed to fetch category={params['category']} page={page_number}: {last_error}"
    )


def product_to_output(product: dict[str, Any], category_name: str) -> dict[str, Any] | None:
    part_number = extract_part_number(product)
    if not part_number:
        return None

    price = product.get("prices", {}).get("price")
    permalink = str(product.get("permalink") or "").strip()
    if not price or not permalink:
        return None

    return {
        "store_name": STORE_NAME,
        "scraped_name": normalize_text(product.get("name")),
        "scraped_brand": extract_brand(product),
        "type": category_name,
        "part #": part_number,
        "price": str(price).strip(),
        "url": permalink,
        "image_url": extract_image_url(product),
    }


def write_product(output_dir: Path, product_data: dict[str, Any]) -> None:
    url = product_data["url"]
    filename = f"C_{hashlib.md5(url.encode()).hexdigest()}.json"
    with (output_dir / filename).open("w", encoding="utf-8") as output_file:
        json.dump(product_data, output_file, ensure_ascii=False, indent=4)


def clean_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for path in output_dir.glob("*.json"):
        path.unlink()


def main() -> int:
    output_dir = "ScrapDB/Outputs/Centrale"
    output_path = Path(output_dir)
    print("Cleaning previous Centrale outputs...")
    clean_output_dir(output_path)

    saved_count = 0
    skipped_count = 0
    seen_urls: set[str] = set()

    with requests.Session() as session:
        for category_name, category_ids in CATEGORY_ID_MAP.items():
            print(f"[Centrale] Collecting {category_name} ({category_ids})")
            first_page, total_pages = fetch_page(session, category_ids, 1)
            pages = [(1, first_page)]

            print(
                f"   {category_name}: {total_pages} API page(s), "
                f"{len(first_page)} products on page 1"
            )

            for page_number in range(2, total_pages + 1):
                products, _ = fetch_page(session, category_ids, page_number)
                pages.append((page_number, products))

            category_saved = 0
            category_skipped = 0
            for page_number, products in pages:
                page_saved = 0
                for product in products:
                    product_data = product_to_output(product, category_name)
                    if not product_data:
                        skipped_count += 1
                        category_skipped += 1
                        continue

                    url = product_data["url"]
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                    write_product(output_path, product_data)
                    saved_count += 1
                    category_saved += 1
                    page_saved += 1

                print(f"   {category_name} page {page_number}: saved {page_saved}")

            print(
                f"   {category_name}: saved {category_saved}, "
                f"skipped without required fields {category_skipped}"
            )

    print(
        f"Centrale scraping finished. Saved {saved_count} product JSON files; "
        f"skipped {skipped_count} products without required fields."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

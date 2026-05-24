from __future__ import annotations

import json
import os
import re
from pathlib import Path

from api_scraper_utils import (
    build_woocommerce_page_url,
    exit_code_from_count,
    normalize_price,
    pick_part_number,
    run_woocommerce_store,
)
from browser_fallback_utils import browser_fallback_enabled, run_browser_fallback_store


CATEGORY_QUERIES = {
    "OperatingSystem": [{"category": 512}],
    "Headphones": [{"category": 20}],
    "Mouse_Keyboard": [{"category": "33,24"}],
    "Storage": [{"category": "85,539,540"}],
    "Monitor": [{"category": 27}],
    "CPUCooler": [{"category": "414,415"}],
    "CaseFan": [{"category": 403}],
    "ThermalCompound": [{"category": 508}],
    "PowerSupply": [{"category": 44}],
    "Case": [{"category": 40}],
    "Memory": [{"category": "45,301,362"}],
    "CPU": [{"category": "253,255"}],
    "VideoCard": [{"category": 19}],
    "Motherboard": [{"category": "101,102"}],
    "Webcam": [{"category": 31}],
}

CATEGORY_LISTING_URLS = {
    "OperatingSystem": "https://trulustore.cl/categoria-producto/software/",
    "Headphones": "https://trulustore.cl/categoria-producto/perifericos/audifonos-y-accesorios/",
    "Mouse_Keyboard": [
        "https://trulustore.cl/categoria-producto/perifericos/mouse/",
        "https://trulustore.cl/categoria-producto/perifericos/teclados/",
    ],
    "Storage": [
        "https://trulustore.cl/categoria-producto/componentes-pc/almacenamiento/",
        "https://trulustore.cl/categoria-producto/componentes-pc/almacenamiento/disco-ssd-2-5/",
        "https://trulustore.cl/categoria-producto/componentes-pc/almacenamiento/disco-ssd-m-2/",
    ],
    "Monitor": "https://trulustore.cl/categoria-producto/monitores-gamer/",
    "CPUCooler": [
        "https://trulustore.cl/categoria-producto/componentes-pc/refrigeracion/refrigeracion-aire/",
        "https://trulustore.cl/categoria-producto/componentes-pc/refrigeracion/refrigeracion-liquida/",
    ],
    "CaseFan": "https://trulustore.cl/categoria-producto/componentes-pc/refrigeracion/ventiladores/",
    "ThermalCompound": "https://trulustore.cl/categoria-producto/componentes-pc/refrigeracion/pastas-disipadora/",
    "PowerSupply": "https://trulustore.cl/categoria-producto/componentes-pc/fuentes-de-poder/",
    "Case": "https://trulustore.cl/categoria-producto/componentes-pc/gabinetes/",
    "Memory": [
        "https://trulustore.cl/categoria-producto/componentes-pc/memoria-ram/",
        "https://trulustore.cl/categoria-producto/componentes-pc/memoria-ram/ddr4/",
        "https://trulustore.cl/categoria-producto/componentes-pc/memoria-ram/ddr5/",
    ],
    "CPU": [
        "https://trulustore.cl/categoria-producto/componentes-pc/procesadores/amd-procesadores/",
        "https://trulustore.cl/categoria-producto/componentes-pc/procesadores/intel-procesadores/",
    ],
    "VideoCard": "https://trulustore.cl/categoria-producto/componentes-pc/tarjetas-de-video/",
    "Motherboard": [
        "https://trulustore.cl/categoria-producto/componentes-pc/placas-madre/amd/",
        "https://trulustore.cl/categoria-producto/componentes-pc/placas-madre/intel/",
    ],
    "Webcam": "https://trulustore.cl/categoria-producto/perifericos/camaras-web/",
}

SKU_PREFIX_TAGS = {
    "ACC",
    "AU",
    "AUD",
    "CBL",
    "CPU",
    "FAN",
    "GAB",
    "GPU",
    "MB",
    "MEM",
    "MIC",
    "MON",
    "MOU",
    "PSU",
    "REF",
    "SSD",
    "SW",
    "TCL",
    "WC",
}


def clean_trulu_part_number(value: str) -> str:
    part_number = pick_part_number([value], (), allow_name_fallback=False) or ""
    match = re.match(r"^([A-Z]{2,8})-(.+)$", part_number)
    if match and match.group(1).upper() in SKU_PREFIX_TAGS:
        return match.group(2).strip()
    return part_number


def normalize_trulu_output_parts(output_dir: str) -> int:
    changed_count = 0
    for file_path in Path(output_dir).glob("*.json"):
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        original_part = data.get("part #")
        cleaned_part = clean_trulu_part_number(str(original_part or ""))
        if not cleaned_part or cleaned_part == original_part:
            continue

        data["part #"] = cleaned_part
        file_path.write_text(json.dumps(data, ensure_ascii=False, indent=4), encoding="utf-8")
        changed_count += 1

    return changed_count


def clean_trulu_price(value: str) -> str:
    match = re.search(r"\$\s*([\d.]+)", value or "")
    if match:
        return normalize_price(match.group(1))
    return normalize_price(value)


def main() -> int:
    output_dir = "ScrapDB/Outputs/TruluStore"
    force_browser = os.environ.get("SCRAPER_FORCE_BROWSER_FALLBACK", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    if force_browser:
        saved_count = 0
        print("TruluStore requests path skipped because SCRAPER_FORCE_BROWSER_FALLBACK is enabled.")
    else:
        try:
            saved_count = run_woocommerce_store(
                store_name="TruluStore",
                base_url="https://trulustore.cl",
                category_queries=CATEGORY_QUERIES,
                output_dir=output_dir,
                output_prefix="TS",
            )
        except Exception as exc:
            saved_count = 0
            print(f"TruluStore requests path failed, browser fallback will be tried: {exc}")
    print(f"TruluStore requests path saved {saved_count} JSON files.")

    if browser_fallback_enabled(saved_count):
        print("TruluStore starting browser fallback.")
        saved_count = run_browser_fallback_store(
            store_name="TruluStore",
            category_url_map=CATEGORY_LISTING_URLS,
            output_dir=output_dir,
            output_prefix="TS",
            listing_config={
                "link_selector": "//a[contains(@class,'woocommerce-loop-product__link') or contains(@class,'woocommerce-LoopProduct-link')]",
                "pagination_selector": "//ul[contains(@class,'page-numbers')]/li",
                "page_url_builder": build_woocommerce_page_url,
                "ready_selectors": (
                    "//a[contains(@class,'woocommerce-loop-product__link') or contains(@class,'woocommerce-LoopProduct-link')]",
                    "//ul[contains(@class,'products')]",
                ),
            },
            product_config={
                "ready_selectors": ("//span[contains(@class,'sku')]", "//h1[contains(@class,'product_title')]"),
                "name_selectors": ("//h1[contains(@class,'product_title')]", "//h1[contains(@class,'entry-title')]", "//h1"),
                "part_selectors": ("//span[contains(@class,'sku')]",),
                "price_selectors": (
                    "//p[contains(@class,'price')]",
                    "//div[contains(@class,'summary')]//*[contains(@class,'price')]",
                ),
                "image_selectors": (
                    "//div[contains(@class,'woocommerce-product-gallery__image')]//img",
                    "//img[contains(@class,'wp-post-image')]",
                ),
                "brand_selectors": ("//span[contains(@class,'posted_in') and contains(., 'Marca:')]/a[1]",),
                "clean_part_number": clean_trulu_part_number,
                "clean_price": clean_trulu_price,
            },
        )
        print(f"TruluStore browser fallback saved {saved_count} JSON files.")

    normalized_count = normalize_trulu_output_parts(output_dir)
    if normalized_count:
        print(f"TruluStore normalized {normalized_count} prefixed SKU values.")

    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

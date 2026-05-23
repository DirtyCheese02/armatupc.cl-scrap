from __future__ import annotations

from api_scraper_utils import exit_code_from_count, run_woocommerce_store


CATEGORY_QUERIES = {
    "UPS": [{"category": 1810}],
    "Headphones": [{"category": 989}],
    "Mouse": [{"category": 127}],
    "Keyboard": [{"category": 142}],
    "Storage": [{"category": 1029}],
    "Monitor": [{"category": 1055}],
    "CPUCooler_ThermalCompound": [{"category": 1769}],
    "CaseFan": [{"category": 1050}],
    "PowerSupply": [{"category": 1048}],
    "Case": [{"category": 1004}],
    "Memory": [{"category": 1047}],
    "CPU": [{"category": 982}],
    "VideoCard": [{"category": 1045}],
    "Motherboard": [{"category": 1046}],
    "Webcam": [{"category": 1788}],
}


def main() -> int:
    output_dir = "ScrapDB/Outputs/CentralGamer"
    saved_count = run_woocommerce_store(
        store_name="CentralGamer",
        base_url="https://centralgamer.cl",
        category_queries=CATEGORY_QUERIES,
        output_dir=output_dir,
        output_prefix="CG",
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

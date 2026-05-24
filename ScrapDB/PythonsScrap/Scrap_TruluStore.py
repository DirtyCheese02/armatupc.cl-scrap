from __future__ import annotations

from api_scraper_utils import exit_code_from_count, run_woocommerce_store


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


def main() -> int:
    output_dir = "ScrapDB/Outputs/TruluStore"
    saved_count = run_woocommerce_store(
        store_name="TruluStore",
        base_url="https://trulustore.cl",
        category_queries=CATEGORY_QUERIES,
        output_dir=output_dir,
        output_prefix="TS",
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

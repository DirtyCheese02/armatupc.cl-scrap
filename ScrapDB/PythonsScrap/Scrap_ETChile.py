from __future__ import annotations

from api_scraper_utils import exit_code_from_count, run_woocommerce_store


CATEGORY_QUERIES = {
    "Headphones": [{"category": 114}],
    "Mouse": [{"category": 160}],
    "Keyboard": [{"category": 118}],
    "Storage": [{"category": 71}],
    "ExternalStorage": [{"category": 172}],
    "Monitor": [{"category": 163}],
    "CPUCooler_Liquid": [{"category": 121}],
    "ThermalCompound": [{"category": 343}],
    "PowerSupply": [{"category": 119}],
    "Case": [{"category": 117}],
    "Memory": [{"category": 122}],
    "CPU": [{"category": 133}],
    "VideoCard": [{"category": 173}],
    "Motherboard": [{"category": 148}],
    "Webcam": [{"category": 229}],
    "NetworkAdapter": [{"category": 390}],
    "CPUCooler_CaseFan": [{"category": 147}],
}


def main() -> int:
    output_dir = "ScrapDB/Outputs/ETChile"
    saved_count = run_woocommerce_store(
        store_name="ETChile",
        base_url="https://etchile.net",
        category_queries=CATEGORY_QUERIES,
        output_dir=output_dir,
        output_prefix="ETC",
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

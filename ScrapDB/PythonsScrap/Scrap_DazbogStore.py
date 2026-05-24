from __future__ import annotations

from api_scraper_utils import exit_code_from_count, run_woocommerce_store


CATEGORY_QUERIES = {
    "Mouse": [{"category": 722}],
    "Monitor": [{"category": 703}],
    "CPUCooler": [{"category": 714}],
    "PowerSupply": [{"category": 683}],
    "CPU": [{"category": 685}],
    "VideoCard": [{"category": 684}],
    "Motherboard": [{"category": 705}],
}


def main() -> int:
    output_dir = "ScrapDB/Outputs/DazbogStore"
    saved_count = run_woocommerce_store(
        store_name="DazbogStore",
        base_url="https://www.dazbogstore.cl",
        category_queries=CATEGORY_QUERIES,
        output_dir=output_dir,
        output_prefix="DZB",
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

from api_scraper_utils import exit_code_from_count, run_woocommerce_store


PARTES_Y_PIEZAS_ATTRIBUTE = "pa_producto-partes-y-piezas"

CATEGORY_QUERIES = {
    "OperatingSystem": [{"category": 709}],
    "UPS": [{"category": 710}],
    "Headphones": [{"category": 701}],
    "Mouse_Keyboard": [{"category": 706}],
    "Storage_ExternalStorage": [{"category": 689}],
    "Monitor": [{"category": 723}],
    "CPUCooler": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "refrigeracion",
        }
    ],
    "PowerSupply": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "fuente-de-poder",
        }
    ],
    "Case": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "gabinetes",
        }
    ],
    "Memory": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "memoria-ram-para-pc",
        }
    ],
    "CPU": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "procesadores",
        }
    ],
    "VideoCard": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "tarjeta-de-video",
        }
    ],
    "Motherboard": [
        {
            "category": 698,
            "attributes[0][attribute]": PARTES_Y_PIEZAS_ATTRIBUTE,
            "attributes[0][slug]": "placa-madre",
        }
    ],
    "Webcam": [{"category": 1505}],
}


def main() -> int:
    output_dir = "ScrapDB/Outputs/NotebooksYa"
    saved_count = run_woocommerce_store(
        store_name="NotebooksYa",
        base_url="https://notebooksya.cl",
        category_queries=CATEGORY_QUERIES,
        output_dir=output_dir,
        output_prefix="NYa",
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

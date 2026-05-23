from __future__ import annotations

from api_scraper_utils import exit_code_from_count, run_prestashop_xhr_store


CATEGORY_URL_MAP = {
    "OperatingSystem": "https://n1g.cl/Home/38-software",
    "UPS": "https://n1g.cl/Home/94-ups",
    "Headphones": "https://n1g.cl/Home/32-parlantes-audio",
    "Storage": [
        "https://n1g.cl/Home/55-discos-hdd",
        "https://n1g.cl/Home/54-discos-ssd",
        "https://n1g.cl/Home/56-discos-25",
    ],
    "Monitor": "https://n1g.cl/Home/28-monitores",
    "CPUCooler_Air": "https://n1g.cl/Home/61-disipador-por-aire",
    "CPUCooler_Liquid": "https://n1g.cl/Home/62-watercooling",
    "CaseFan": "https://n1g.cl/Home/63-ventiladores",
    "ThermalCompound": "https://n1g.cl/Home/64-pasta-disipadora",
    "PowerSupply": [
        "https://n1g.cl/Home/57-fuentes-certificadas-modular",
        "https://n1g.cl/Home/58-fuentes-certificadas-no-modular",
    ],
    "Case": "https://n1g.cl/Home/24-gabinetes",
    "Memory": [
        "https://n1g.cl/Home/77-ddr5-pc",
        "https://n1g.cl/Home/33-placas-madre",
    ],
    "CPU": "https://n1g.cl/Home/34-procesadores",
    "VideoCard": [
        "https://n1g.cl/Home/130-intel",
        "https://n1g.cl/Home/110-nvidia",
        "https://n1g.cl/Home/111-amd",
    ],
    "Motherboard": "https://n1g.cl/Home/33-placas-madre",
    "NetworkAdapter": "https://n1g.cl/Home/76-adaptadores-de-red",
    "Mouse_Keyboard": "https://n1g.cl/Home/29-mouse-teclados",
}


def main() -> int:
    output_dir = "ScrapDB/Outputs/NiceOne"
    saved_count = run_prestashop_xhr_store(
        store_name="NiceOne",
        base_url="https://n1g.cl/Home/",
        category_url_map=CATEGORY_URL_MAP,
        output_dir=output_dir,
        output_prefix="NO",
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

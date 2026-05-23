from __future__ import annotations

from api_scraper_utils import exit_code_from_count, run_prestashop_xhr_store


CATEGORY_URL_MAP = {
    "OperatingSystem": "https://mybox.cl/29-software",
    "UPS": "https://mybox.cl/91-respaldo-energetico-ups",
    "Headphones": "https://mybox.cl/21-audifonos-headset",
    "Mouse_Keyboard": "https://mybox.cl/20-teclados-mouse",
    "Storage": "https://mybox.cl/67-almacenamiento",
    "Monitor": "https://mybox.cl/28-pantallas-y-monitores",
    "CPUCooler": "https://mybox.cl/92-enfriamiento-refrigeracion",
    "CaseFan": "https://mybox.cl/89-ventiladores-fans",
    "PowerSupply": "https://mybox.cl/63-fuentes-de-poder",
    "Case": "https://mybox.cl/62-gabinetes",
    "Memory": "https://mybox.cl/66-memoria-ram",
    "CPU": "https://mybox.cl/64-procesador",
    "VideoCard": "https://mybox.cl/68-tarjeta-de-video",
    "Motherboard": "https://mybox.cl/65-placa-madre",
    "Webcam": "https://mybox.cl/23-webcam",
    "NetworkAdapter": "https://mybox.cl/90-redes-conectividad?q=Categor%C3%ADas-Adaptadores+de+Red",
}


def main() -> int:
    output_dir = "ScrapDB/Outputs/MyBox"
    saved_count = run_prestashop_xhr_store(
        store_name="MyBox",
        base_url="https://mybox.cl",
        category_url_map=CATEGORY_URL_MAP,
        output_dir=output_dir,
        output_prefix="MyB",
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

from api_scraper_utils import exit_code_from_count, run_sphinx_store


CATEGORY_URL_MAP = {
    "Case": "https://www.myshop.cl/partes-y-piezas-gabinetes",
    "CaseFan": "https://www.myshop.cl/partes-y-piezas-refrigeracion?filtro_categoria=[%%22148%%22]",
    "CPU": "https://www.myshop.cl/partes-y-piezas-procesadores",
    "CPUCooler": "https://www.myshop.cl/partes-y-piezas-refrigeracion?filtro_categoria=[%%22151%%22%%2C%%22150%%22]",
    "ExternalStorage": "https://www.myshop.cl/almacenamiento-almacenamiento-externo",
    "Headphones": "https://www.myshop.cl/audio-video-audifonos",
    "Keyboard": "https://www.myshop.cl/partes-y-piezas-teclados",
    "Memory": "https://www.myshop.cl/partes-y-piezas-memorias-ram-memorias-pc",
    "Monitor": "https://www.myshop.cl/monitor-monitores",
    "Motherboard": "https://www.myshop.cl/partes-y-piezas-placas-madres",
    "Mouse": "https://www.myshop.cl/partes-y-piezas-mouse",
    "OperatingSystem": "https://www.myshop.cl/partes-y-piezas-software",
    "PowerSupply": "https://www.myshop.cl/partes-y-piezas-fuentes-de-poder",
    "Storage": [
        "https://www.myshop.cl/partes-y-piezas-discos-ssd-internos",
        "https://www.myshop.cl/almacenamiento-discos-hdd-internos",
    ],
    "ThermalCompound": "https://www.myshop.cl/partes-y-piezas-refrigeracion?filtro_categoria=[%%22149%%22]",
    "UPS": "https://www.myshop.cl/empresas-ups",
    "VideoCard": "https://www.myshop.cl/partes-y-piezas-tarjetas-de-video",
    "Webcam": "https://www.myshop.cl/gamer-streaming-webcam",
}


def main() -> int:
    output_dir = "ScrapDB/Outputs/MyShop"
    saved_count = run_sphinx_store(
        store_name="MyShop",
        base_url="https://www.myshop.cl",
        service_url="https://www.myshop.cl/servicio/producto",
        category_url_map=CATEGORY_URL_MAP,
        output_dir=output_dir,
        output_prefix="MyS",
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

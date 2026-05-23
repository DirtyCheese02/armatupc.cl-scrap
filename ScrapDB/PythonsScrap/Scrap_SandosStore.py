from __future__ import annotations

from api_scraper_utils import exit_code_from_count, run_sphinx_store


CATEGORY_URL_MAP = {
    "Case": "https://sandos.cl/componentes-gabinetes?filtro_categoria=[%%2229%%22%%2C%%22142%%22%%2C%%2239%%22]",
    "CaseFan": "https://sandos.cl/componentes-gabinetes?filtro_categoria=[$%22143$%22]",
    "CPU": "https://sandos.cl/componentes-procesador",
    "CPUCooler_Air": "https://sandos.cl/componentes-refrigeracion-y-ventilacion-refrigeracion-aire",
    "CPUCooler_Liquid": "https://sandos.cl/componentes-refrigeracion-y-ventilacion-refrigeracion-liquida",
    "ExternalStorage": "https://sandos.cl/almacenamiento-almacenamiento-externo?filtro_categoria=[%%22148%%22%%2C%%22147%%22]",
    "Headphones": "https://sandos.cl/audio-y-video-audifonos",
    "Keyboard": "https://www.sandos.cl/computadores-y-tablets-perifericos-teclados",
    "Memory": "https://sandos.cl/memorias-memorias-ram-memorias-ram-pc",
    "Monitor": "https://sandos.cl/monitores-y-pantallas-monitores?filtro_categoria=[%%22101%%22%%2C%%22169%%22]",
    "Motherboard": "https://sandos.cl/componentes-placa-madre",
    "Mouse": "https://www.sandos.cl/buscar?texto=mouse&filtro_categoria=[%%2276%%22]",
    "OperatingSystem": "https://www.sandos.cl/hogar-y-oficina-software-sistema-operativo-y-aplicaciones",
    "PowerSupply": "https://sandos.cl/componentes-fuente-de-poder",
    "Storage": "https://sandos.cl/almacenamiento-almacenamiento-interno?filtro_categoria=[%%22149%%22%%2C%%22146%%22%%2C%%2220%%22]",
    "ThermalCompound": "https://sandos.cl/componentes-refrigeracion-y-ventilacion-pasta-termica",
    "UPS": "https://www.sandos.cl/hogar-y-oficina-ups-y-energia-ups-y-respaldo-de-energia",
    "VideoCard": "https://sandos.cl/componentes-tarjeta-de-video",
    "Webcam": "https://www.sandos.cl/buscar?texto=webcam",
    "NetworkAdapter": "https://sandos.cl/producto/hpe-broadcom-bcm57416-adaptador-de-red-ocp-30-125510gbase-t-x-2-para-proliant-dl325-gen10-dl345-gen10-dl360-gen10-dl365-gen10-xl220n-gen10-p14917",
}


def main() -> int:
    output_dir = "ScrapDB/Outputs/Sandos"
    saved_count = run_sphinx_store(
        store_name="Sandos",
        base_url="https://sandos.cl",
        service_url="https://sandos.cl/servicio/producto",
        category_url_map=CATEGORY_URL_MAP,
        output_dir=output_dir,
        output_prefix="SS",
    )
    return exit_code_from_count(saved_count)


if __name__ == "__main__":
    raise SystemExit(main())

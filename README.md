# ArmaTuPC Scraping (Minimal)

Proyecto para scrapear tiendas, generar JSONs por producto y hacer matching contra Supabase.

## Estructura relevante

- `ScrapDB/PythonsScrap/`: scrapers individuales (`Scrap_*.py`)
- `ScrapDB/Outputs/`: JSONs generados por cada tienda
- `ScrapDB/match_products.py`: matching por part number + actualización de precios/imagenes en Supabase
- `ScrapDB/run_all_scrapers.py`: orquestador (corre todos los scrapers y luego matching)
- `SpecDB/`: scraping/carga de base de especificaciones (flujo aparte)

## Requisitos

- Python 3.12+
- Chrome/Chromium
- Cuenta Supabase con tablas configuradas

## Variables de entorno

No subas `.env` al repo.

### `ScrapDB/.env`

```env
SUPABASE_URL=https://your-project-ref.supabase.co
SUPABASE_KEY=your-service-role-key
```

### `SpecDB/.env`

```env
SUPABASE_URL=https://your-project-ref.supabase.co
SUPABASE_KEY=your-service-role-key
```

## Instalación

```bash
pip install -r requirements.txt
```

## Ejecución local

### Correr todo (scrapers + matching)

```bash
python ScrapDB/run_all_scrapers.py
```

### Variables opcionales del runner

- `SCRAP_HEADLESS`: `1` o `0`
- `SCRAP_USE_XVFB`: `1` o `0`
- `SCRAPER_RETRY_ON_EMPTY`: `1` o `0`
- `SCRAPER_HEADFUL`: lista CSV de scrapers forzados en headful (ej: `Scrap_MyShop.py,Scrap_SandosStore.py`)
- `SCRAPER_HEADLESS`: lista CSV de scrapers forzados en headless
- `SCRAPER_EXCLUDE`: lista CSV de scrapers a excluir
- `SCRAPER_TIMEOUT_MINUTES`: timeout por scraper
- `MATCH_TIMEOUT_MINUTES`: timeout de matching

## GitHub Actions

Workflow: `.github/workflows/scrapdb-daily.yml`

- Trigger manual: `workflow_dispatch`
- Trigger automático: `cron: 0 10 * * *` (UTC)

Nota: al usar UTC fijo, la hora local en Chile puede variar cuando cambia horario de verano/invierno.

### Secrets requeridos (GitHub)

- `SUPABASE_URL`
- `SUPABASE_KEY`

Usa valores sin comillas.

## Seguridad antes de hacer público el repo

1. Asegúrate de no trackear ningún `.env`.
2. Rota cualquier key que haya estado expuesta antes.
3. Verifica el historial si en algún commit antiguo se subieron secretos.

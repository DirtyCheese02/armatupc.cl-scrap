# ArmaTuPC.cl Scrap

Pipeline de scraping y matching para precios de tiendas chilenas.

## Flujo principal

1. `ScrapDB/run_all_scrapers.py` descubre y ejecuta `ScrapDB/PythonsScrap/Scrap_*.py`.
2. Cada scraper escribe JSON en `ScrapDB/Outputs/<Tienda>/`.
3. El runner escribe logs en `ScrapDB/RunLogs/<timestamp>/`.
4. Antes del matching, el runner genera `scraper_summary_pre_match.json` con exito, duracion y cantidad de JSON por scraper.
5. `ScrapDB/match_products.py` lee outputs, matchea contra PCPP, publica precios e historial, y registra calidad de datos.

## Comando local

```powershell
python ScrapDB/run_all_scrapers.py
```

Variables utiles:

- `RUN_MATCH_PRODUCTS=0`: corre scrapers sin publicar precios.
- `SCRAPER_INCLUDE=Scrap_SPDigital.py,Scrap_KDtec.py`: limita tiendas.
- `SCRAPER_EXCLUDE=Scrap_InfoSep.py`: excluye tiendas.
- `SCRAPER_TIMEOUT_MINUTES=90`: timeout base por scraper.
- `MATCH_TIMEOUT_MINUTES=60`: timeout del matcher.
- `SCRAPER_REQUIRE_NON_EMPTY=Scrap_SPDigital.py`: falla si el scraper no genera JSON.
- `STOCK_MARKOUT_MIN_RAW_COUNT=20`: minimo de productos brutos para permitir marcar faltantes como sin stock.
- `STOCK_MARKOUT_MIN_MATCH_RATE=0.05`: tasa minima de match para permitir marcar faltantes como sin stock.

## Stock safety

`match_products.py` solo marca productos como no disponibles cuando la tienda tuvo un scrape saludable:

- el scraper no fallo,
- el output no esta vacio,
- la cantidad de productos supera `STOCK_MARKOUT_MIN_RAW_COUNT`,
- la tasa de match supera `STOCK_MARKOUT_MIN_MATCH_RATE`.

Si una tienda falla, queda vacia o trae pocos resultados, se actualizan metricas, pero no se borran stocks vigentes.

## Calidad de datos

Con la migracion `20260626_data_quality_foundation.sql`, el matcher alimenta:

- `scrape_runs`: resumen global por corrida.
- `scraper_store_runs`: metricas por tienda.
- `scraped_products_raw`: cola accionable de productos crudos, matcheados, no matcheados, invalidos o con anomalia.
- `match_candidates`: candidatos seleccionados o rechazados.
- `match_overrides`: overrides manuales aprobados para casos recurrentes.

Si esas tablas todavia no existen, el matcher sigue funcionando y omite las escrituras opcionales.

## Matching

Orden actual:

1. override manual aprobado en `match_overrides`,
2. part number exacto normalizado contra `MetaPartNumber`,
3. fallback fuzzy por `ilike`.

Los no matcheados siguen quedando en `ScrapDB/unmatched_log.txt`, pero la fuente de trabajo deberia ser `scraped_products_raw`.

## Anomalias de precio

El matcher no publica precios que parezcan imposibles:

- bajo `MIN_REASONABLE_PUBLISH_PRICE` (default: `1000` CLP),
- subida mayor a `PRICE_ANOMALY_MAX_MULTIPLIER` (default: `4x`),
- caida menor a `PRICE_ANOMALY_MIN_MULTIPLIER` (default: `0.25x`).

Esos casos quedan como `price_anomaly` en `scraped_products_raw`.

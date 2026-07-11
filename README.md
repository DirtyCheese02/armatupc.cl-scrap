# ArmaTuPC.cl Scrap

Pipeline de scraping y matching para precios de tiendas chilenas.

Runtime local y CI: Python 3.12. Para una instalacion reproducible usa `python -m pip install -r requirements.lock`; `requirements.txt` conserva la lista corta de dependencias directas.

## Flujo principal

1. `ScrapDB/run_all_scrapers.py` descubre y ejecuta `ScrapDB/PythonsScrap/Scrap_*.py`.
2. Cada scraper escribe JSON en `ScrapDB/Outputs/<Tienda>/`.
3. El runner escribe logs en `ScrapDB/RunLogs/<timestamp>/`.
4. Antes del matching, el runner genera `scraper_summary_pre_match.json` con exito, duracion y cantidad de JSON por scraper.
5. `ScrapDB/match_products.py` lee outputs, matchea contra el catalogo legacy durante la migracion canonica, publica precios e historial, y registra calidad de datos.

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

## Stock safety

`match_products.py` solo marca productos como no disponibles despues de dos snapshots completos y saludables consecutivos:

- el scraper no fallo,
- el output no esta vacio ni parcial,
- existe telemetria de la corrida,
- hubo al menos un match y el match rate fue de 80% o mas,
- no hubo errores de matching ni anomalias pendientes.

Si una tienda falla, queda vacia, parcial o sospechosa, se actualizan metricas, pero no se modifican stocks vigentes.

## Politica de fuentes

Las capturas nuevas desde PCPartPicker estan suspendidas. `SpecDB/Scrap_PCPP.py` termina sin navegar por defecto y nunca intenta resolver automaticamente Turnstile u otros challenges. Una futura reactivacion exige simultaneamente `PCPP_CAPTURE_ENABLED=1` y `PCPP_PERMISSION_REFERENCE` con la referencia verificable del permiso escrito o acuerdo de datos.

Los archivos historicos se conservan en modo de migracion/lectura. No deben incorporarse nuevas especificaciones ni imagenes de una fuente sin registrar procedencia y permiso.

### Archivo de especificaciones legacy

Los 69.514 JSON historicos ya no se versionan de forma individual. `tools/archive_specifications.py` los convierte en un NDJSON gzip determinista, verifica cada ruta y SHA-256 contra el archivo resultante y solo despues permite eliminar la carpeta de origen:

```powershell
python tools/archive_specifications.py --remove-source
```

El manifiesto versionado queda en `SpecDB/specifications-archive-manifest.json`. El archivo comprimido se guarda localmente en `SpecDB/Archives/pcpartpicker-specifications.ndjson.gz`, esta ignorado por Git y no debe subirse ni monetizarse mientras la procedencia siga como `unverified`. Para reconstruirlo desde otra copia de los JSON, el SHA-256 esperado es `8deba9bc2ca622a504aba35f34d9b81e9fc32eefc64c85f888771f9fbf099f7e`.

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
2. part number exacto normalizado contra `MetaPartNumber`.

Los no matcheados siguen quedando en `ScrapDB/unmatched_log.txt`, pero la fuente de trabajo deberia ser `scraped_products_raw`.

## Anomalias de precio

El matcher no publica precios que parezcan imposibles:

- bajo `MIN_REASONABLE_PUBLISH_PRICE` (default: `1000` CLP),
- subida mayor a `PRICE_ANOMALY_MAX_MULTIPLIER` (default: `4x`),
- caida menor a `PRICE_ANOMALY_MIN_MULTIPLIER` (default: `0.25x`).

Esos casos quedan como `price_anomaly` en `scraped_products_raw`.

## Contrato canonico `RawOffer`

`ScrapDB/raw_offer.py` agrega una frontera Pydantic estricta sin cambiar todavia los 24 scrapers. El adaptador convierte el JSON legacy, separa transferencia, tarjeta y precio publicado, normaliza multiples MPN/GTIN y rechaza precios cero, URLs invalidas, fechas sin zona horaria y errores de fuente.

Para convertir un snapshot local sin publicarlo:

```powershell
python -m ScrapDB.raw_offer --input ScrapDB/Outputs --output ScrapDB/RawRuns --run-id <scrape-run-id> --strict
```

La salida principal es `<run-id>.ndjson.gz`. Los registros invalidos quedan indexados en `<run-id>.errors.json` para revision; nunca se transforman silenciosamente en ofertas disponibles. `ScrapDB/RawRuns/` es un artefacto local ignorado por Git.

Los fixtures contractuales de `tests/fixtures/raw_offer/` deben mantenerse sincronizados con cada `Scrap_*.py`. El test falla si aparece o desaparece un scraper sin actualizar el registro.

## Backfill del catalogo canonico

`ScrapDB/canonical_backfill.py` migra de forma aditiva; no cambia las lecturas web ni elimina filas legacy. El comando es dry-run por defecto, parte solo con CPU, GPU y placa madre, y en ese modo **nunca crea un cliente Supabase**. Para que el plan sea reproducible, el dry-run exige un snapshot JSON local con el formato de `canonical_backfill_snapshot.example.json`:

```powershell
python -m ScrapDB.canonical_backfill --snapshot canonical_backfill_snapshot.json
python -m ScrapDB.canonical_backfill --snapshot canonical_backfill_snapshot.json --categories CPU,GPU,Motherboard --compare --comparison-report ScrapDB/RunLogs/canonical-compare.json
```

El snapshot contiene las tablas legacy `specifications.*`, `ProductPricing` y `Stores`, junto con una seccion `canonical` opcional para comparar ambas lecturas. Debe obtenerse mediante un proceso de exportacion autorizado y guardarse fuera de Git si contiene datos reales. El archivo de ejemplo solo documenta la estructura y no contiene catalogo.

Para escribir se exige `--apply` y `SUPABASE_SERVICE_ROLE_KEY`. Los upserts usan IDs deterministas, se guardan checkpoints atomicos despues de cada lote completo y una corrida interrumpida se reanuda explicitamente:

```powershell
python -m ScrapDB.canonical_backfill --apply --checkpoint ScrapDB/RunLogs/canonical-checkpoint.json
python -m ScrapDB.canonical_backfill --apply --resume --checkpoint ScrapDB/RunLogs/canonical-checkpoint.json
```

Cuando la primera ola supere sus gates, `--all-essential` habilita RAM, almacenamiento, PSU, gabinete y cooler ademas de CPU/GPU/placa. Multiples MPN almacenados en texto se separan en filas y las imagenes migradas permanecen con `image_authorized=false`.

Un artefacto `RawOffer` se puede evaluar sin publicar:

```powershell
python -m ScrapDB.canonical_backfill --snapshot canonical_backfill_snapshot.json --raw-offers ScrapDB/RawRuns/<run-id>.ndjson.gz --raw-offers-only --compare
```

El matching aplica GTIN exacto global no ambiguo, luego la pareja exacta marca+MPN no ambigua y finalmente SKU, ID o URL persistente de la misma tienda. Una colision global, conflicto de categoria/marca o similitud de nombre queda en estado `candidate`; el matching fuzzy nunca obtiene `match_status=matched`. Durante los 14 dias de validacion, `--dual-write` mantiene `ProductPricing` solo para matches exactos, datos frescos y disponibilidad explicita; no cambia el ranking ni activa nuevas lecturas. Se debe revisar el reporte de comparacion antes de cambiar cualquier feature flag web.

### Dual-write automatico en GitHub Actions

Los workflows diario y de retry generan `RawOffer` y publican en ambos modelos solo cuando el repositorio tiene:

- el secret de Actions `SUPABASE_SERVICE_ROLE_KEY`;
- la variable de Actions `CANONICAL_DUAL_WRITE_ENABLED=true`.

Sin la variable, los pasos canonicos se omiten y el pipeline legacy sigue funcionando. Los artefactos `RawRuns`, checkpoints y reportes de comparacion se conservan 14 dias junto con los logs del workflow. La primera ola automatica esta limitada a CPU, GPU y placa madre.

## Inventario de procedencia y permisos

La herramienta `tools/inventory_provenance.py` genera un inventario agregado por fuente, sin copiar payloads ni imagenes:

```powershell
python tools/inventory_provenance.py --format csv --output provenance.csv
python tools/inventory_provenance.py --permissions source_permissions.json --format json
```

Por defecto toda fuente queda como `not_documented` y no elegible para monetizacion. La herramienta solo acepta elegibilidad cuando hay un estado permitido, una referencia y una fecha de respaldo. `source_permissions.example.json` documenta el formato; no constituye un permiso real.

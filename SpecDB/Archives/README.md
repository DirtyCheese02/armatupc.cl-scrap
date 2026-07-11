# Archivo consolidado de especificaciones

`tools/archive_specifications.py` empaqueta los JSON legacy en un NDJSON gzip determinista y verifica ruta/hash 1:1 antes de permitir su eliminación.

El archivo contiene capturas históricas de PCPartPicker con permiso aún no documentado. Por eso se mantiene local, ignorado por Git y fuera de monetización. `SpecDB/specifications-archive-manifest.json` conserva conteos y SHA-256 para comprobar una futura carga privada autorizada.

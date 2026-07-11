from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _read_json(file_path: Path) -> tuple[bytes, Any]:
    raw = file_path.read_bytes()
    return raw, json.loads(raw.decode("utf-8-sig"))


def _verified_unlink(file_path: Path, source: Path) -> None:
    if not _inside(file_path, source):
        raise ValueError(f"Se rechazó eliminar una ruta fuera de {source}: {file_path}")
    file_path.unlink()


def archive_specifications(source: Path, output: Path, manifest_path: Path, remove_source: bool = False) -> dict[str, Any]:
    source = source.resolve()
    output = output.resolve()
    manifest_path = manifest_path.resolve()
    workspace = Path(__file__).resolve().parents[1]
    if not source.is_dir() or not _inside(source, workspace):
        raise ValueError("El directorio fuente debe existir dentro del repositorio.")
    if not _inside(output, workspace) or not _inside(manifest_path, workspace):
        raise ValueError("Los artefactos deben permanecer dentro del repositorio.")
    if output.exists():
        raise FileExistsError(f"El archivo ya existe: {output}")

    files = sorted(source.rglob("*.json"), key=lambda item: item.relative_to(source).as_posix())
    if not files:
        raise ValueError("No se encontraron JSON de especificaciones.")
    output.parent.mkdir(parents=True, exist_ok=True)
    categories: Counter[str] = Counter()
    expected: dict[str, str] = {}

    worker_count = min(32, max(4, (os.cpu_count() or 4) * 2))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        loaded_files = executor.map(_read_json, files)
        with output.open("wb") as raw_output:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw_output, mtime=0, compresslevel=9) as compressed:
                for file_path, (raw, payload) in zip(files, loaded_files, strict=True):
                    if not _inside(file_path, source):
                        raise ValueError(f"Ruta fuera del directorio fuente: {file_path}")
                    relative = file_path.relative_to(source).as_posix()
                    payload_hash = _sha256(raw)
                    category = relative.split("/", 1)[0]
                    categories[category] += 1
                    expected[relative] = payload_hash
                    row = {
                        "sourcePath": relative,
                        "category": category,
                        "payloadSha256": payload_hash,
                        "payload": payload,
                    }
                    compressed.write((json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"))

    observed: dict[str, str] = {}
    with gzip.open(output, "rt", encoding="utf-8") as archived:
        for line_number, line in enumerate(archived, start=1):
            row = json.loads(line)
            relative = str(row.get("sourcePath", ""))
            payload_hash = str(row.get("payloadSha256", ""))
            if not relative or relative in observed:
                raise ValueError(f"Fila {line_number}: sourcePath ausente o duplicado.")
            if expected.get(relative) != payload_hash:
                raise ValueError(f"Fila {line_number}: hash no coincide para {relative}.")
            observed[relative] = payload_hash
    if observed != expected:
        raise ValueError("El contenido del archivo no coincide 1:1 con los JSON fuente.")

    archive_hash = _sha256(output.read_bytes())
    manifest = {
        "format": "armatupc-specifications-ndjson-gzip-v1",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "source": "PCPartPicker legacy capture",
        "permissionStatus": "unverified",
        "monetizationEligible": False,
        "recordCount": len(files),
        "categories": dict(sorted(categories.items())),
        "archiveFile": output.relative_to(workspace).as_posix(),
        "archiveBytes": output.stat().st_size,
        "archiveSha256": archive_hash,
        "sourceRemovedAfterVerification": bool(remove_source),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if remove_source:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            list(executor.map(lambda item: _verified_unlink(item, source), files))
        for directory in sorted((path for path in source.rglob("*") if path.is_dir()), reverse=True):
            if _inside(directory, source) and not any(directory.iterdir()):
                directory.rmdir()
        if source.exists() and not any(source.iterdir()):
            source.rmdir()

    return manifest


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Consolida especificaciones JSON en NDJSON gzip verificable.")
    parser.add_argument("--source", type=Path, default=root / "SpecDB" / "ScrapedDataPCPP")
    parser.add_argument("--output", type=Path, default=root / "SpecDB" / "Archives" / "pcpartpicker-specifications.ndjson.gz")
    parser.add_argument("--manifest", type=Path, default=root / "SpecDB" / "specifications-archive-manifest.json")
    parser.add_argument("--remove-source", action="store_true", help="Elimina cada JSON sólo después de verificar el archivo 1:1.")
    args = parser.parse_args()
    manifest = archive_specifications(args.source, args.output, args.manifest, args.remove_source)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

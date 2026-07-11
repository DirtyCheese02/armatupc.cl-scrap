"""Canonical raw-offer contract and additive legacy JSON adapter.

The current scrapers keep writing one legacy JSON file per product.  This
module is deliberately independent from the runner and matcher so stores can
move to the canonical contract incrementally.  It provides:

* a strict Pydantic ``RawOffer`` model;
* deterministic adaptation of the legacy product JSON shape;
* quarantinable per-file adaptation errors; and
* atomic, reproducible NDJSON gzip exports for one scrape run.

Run ``python -m ScrapDB.raw_offer --help`` from the repository root to convert
an existing ``ScrapDB/Outputs`` snapshot without publishing it.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Iterable, Literal, Mapping, Sequence

from pydantic import (
    AnyHttpUrl,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
MAX_CLP_PRICE = 2_147_483_647
PLACEHOLDER_VALUES = frozenset(
    {
        "",
        "-",
        "--",
        "n/a",
        "na",
        "none",
        "null",
        "sin dato",
        "sin información",
        "sin informacion",
        "error",
    }
)
HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
GTIN_LENGTHS = frozenset({8, 12, 13, 14})

PositiveCLP = Annotated[int, Field(strict=True, gt=0, le=MAX_CLP_PRICE)]


class Availability(str, Enum):
    """Availability as stated explicitly by the merchant source."""

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class RawOffer(BaseModel):
    """Validated scraper boundary shared by all merchant adapters.

    Field names intentionally match the TypeScript contract in the product
    roadmap.  ``normalPrice`` also carries a generic legacy ``price`` when the
    scraper does not identify a payment method; consumers must label that case
    as "precio publicado", not as a cash or card price.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        use_enum_values=True,
    )

    runId: str
    storeId: str
    category: str
    sourceListingId: str
    merchantSku: str | None = None
    mpns: list[str] = Field(default_factory=list)
    gtins: list[str] = Field(default_factory=list)
    brand: str | None = None
    name: str
    cashPrice: PositiveCLP | None = None
    cardPrice: PositiveCLP | None = None
    normalPrice: PositiveCLP | None = None
    currency: Literal["CLP"] = "CLP"
    availability: Availability = Availability.UNKNOWN
    url: AnyHttpUrl
    imageUrl: AnyHttpUrl | None = None
    fetchedAt: AwareDatetime
    payloadHash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("runId", "storeId", "category", "sourceListingId", "name")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned or cleaned.casefold() in PLACEHOLDER_VALUES:
            raise ValueError("must contain meaningful text")
        if len(cleaned) > 512:
            raise ValueError("must contain at most 512 characters")
        return cleaned

    @field_validator("merchantSku", "brand")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned or cleaned.casefold() in PLACEHOLDER_VALUES:
            return None
        if len(cleaned) > 512:
            raise ValueError("must contain at most 512 characters")
        return cleaned

    @field_validator("mpns")
    @classmethod
    def validate_mpns(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_value in values:
            value = raw_value.strip().strip("'\"")
            if not value or value.casefold() in PLACEHOLDER_VALUES:
                continue
            value = re.sub(r"\s+", "", value).upper()
            if len(value) > 128:
                raise ValueError("MPN must contain at most 128 characters")
            if value not in seen:
                normalized.append(value)
                seen.add(value)
        return normalized

    @field_validator("gtins")
    @classmethod
    def validate_gtins(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for raw_value in values:
            value = re.sub(r"\D", "", raw_value)
            if not value:
                continue
            if len(value) not in GTIN_LENGTHS:
                raise ValueError("GTIN must contain 8, 12, 13, or 14 digits")
            if value not in seen:
                normalized.append(value)
                seen.add(value)
        return normalized

    @field_validator("payloadHash")
    @classmethod
    def normalize_payload_hash(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not HASH_PATTERN.fullmatch(normalized):
            raise ValueError("payloadHash must be a lowercase SHA-256 digest")
        return normalized

    @model_validator(mode="after")
    def require_a_price(self) -> "RawOffer":
        if self.cashPrice is None and self.cardPrice is None and self.normalPrice is None:
            raise ValueError("at least one positive CLP price is required")
        return self


class LegacyAdaptationError(ValueError):
    """A legacy row that must be quarantined instead of published."""

    def __init__(self, code: str, message: str, *, source: str | None = None) -> None:
        self.code = code
        self.source = source
        self.message = message
        prefix = f"{source}: " if source else ""
        super().__init__(f"{prefix}{code}: {message}")


@dataclass(frozen=True)
class AdaptationIssue:
    sourcePath: str
    code: str
    message: str


@dataclass(frozen=True)
class AdaptationBatch:
    rawCount: int
    offers: tuple[RawOffer, ...]
    issues: tuple[AdaptationIssue, ...]


@dataclass(frozen=True)
class ExportResult:
    runId: str
    rawCount: int
    offerCount: int
    errorCount: int
    ndjsonPath: str
    errorsPath: str
    manifestPath: str
    compressedSha256: str


def _normalized_key(value: Any) -> str:
    folded = unicodedata.normalize("NFKD", str(value).casefold())
    ascii_value = folded.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", ascii_value)


def _lookup(payload: Mapping[str, Any], aliases: Sequence[str]) -> tuple[bool, Any]:
    normalized = {_normalized_key(key): value for key, value in payload.items()}
    for alias in aliases:
        key = _normalized_key(alias)
        if key in normalized:
            return True, normalized[key]
    return False, None


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    if not text or text.casefold() in PLACEHOLDER_VALUES:
        return None
    return text


def _required_legacy_text(
    payload: Mapping[str, Any], aliases: Sequence[str], field_name: str, source: str | None
) -> str:
    _, value = _lookup(payload, aliases)
    cleaned = _clean_optional_text(value)
    if cleaned is None:
        raise LegacyAdaptationError(
            "missing_field", f"{field_name} is required", source=source
        )
    return cleaned


def _parse_price(value: Any, field_name: str, source: str | None) -> int:
    if isinstance(value, bool) or value is None:
        raise LegacyAdaptationError(
            "invalid_price", f"{field_name} must be a positive CLP integer", source=source
        )

    if isinstance(value, int):
        price = value
    elif isinstance(value, float):
        if not value.is_integer():
            raise LegacyAdaptationError(
                "invalid_price", f"{field_name} cannot contain CLP decimals", source=source
            )
        price = int(value)
    else:
        text = _clean_optional_text(value)
        if text is None:
            raise LegacyAdaptationError(
                "invalid_price", f"{field_name} is empty or unavailable", source=source
            )
        digits = re.sub(r"\D", "", text)
        if not digits:
            raise LegacyAdaptationError(
                "invalid_price", f"{field_name} has no numeric CLP value", source=source
            )
        price = int(digits)

    if price <= 0 or price > MAX_CLP_PRICE:
        raise LegacyAdaptationError(
            "invalid_price",
            f"{field_name} must be between 1 and {MAX_CLP_PRICE} CLP",
            source=source,
        )
    return price


def _read_price(
    payload: Mapping[str, Any], aliases: Sequence[str], field_name: str, source: str | None
) -> tuple[bool, int | None]:
    present, value = _lookup(payload, aliases)
    if not present:
        return False, None
    return True, _parse_price(value, field_name, source)


def _identifier_parts(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        result: list[str] = []
        for item in value:
            result.extend(_identifier_parts(item))
        return result

    text = _clean_optional_text(value)
    if text is None:
        return []
    text = text.strip("[](){}")
    return [part.strip().strip("'\"") for part in re.split(r"[,;|\n\r]+", text) if part.strip()]


def _collect_identifiers(payload: Mapping[str, Any], aliases: Sequence[str]) -> list[str]:
    normalized = {_normalized_key(key): value for key, value in payload.items()}
    values: list[str] = []
    for alias in aliases:
        key = _normalized_key(alias)
        if key in normalized:
            values.extend(_identifier_parts(normalized[key]))
    return values


def _parse_availability(payload: Mapping[str, Any], source: str | None) -> Availability:
    present, value = _lookup(
        payload,
        (
            "availability",
            "stock_status",
            "stockStatus",
            "is_available",
            "isAvailable",
            "in_stock",
            "inStock",
            "available",
            "stock",
        ),
    )
    if not present or value is None:
        return Availability.UNKNOWN
    if isinstance(value, bool):
        return Availability.AVAILABLE if value else Availability.UNAVAILABLE
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return Availability.AVAILABLE if value > 0 else Availability.UNAVAILABLE

    normalized = _normalized_key(value)
    if normalized in {
        "available",
        "instock",
        "enstock",
        "disponible",
        "true",
        "yes",
        "si",
        "1",
    }:
        return Availability.AVAILABLE
    if normalized in {
        "unavailable",
        "outofstock",
        "agotado",
        "sinstock",
        "nodisponible",
        "false",
        "no",
        "0",
    }:
        return Availability.UNAVAILABLE
    if normalized in {"unknown", "consultar", "ask", "porconfirmar"}:
        return Availability.UNKNOWN
    if normalized in {"error", "failed", "timeout", "exception"}:
        raise LegacyAdaptationError(
            "source_error",
            "source error is not an availability state and must be quarantined",
            source=source,
        )
    return Availability.UNKNOWN


def _validate_legacy_currency(payload: Mapping[str, Any], source: str | None) -> None:
    present, value = _lookup(payload, ("currency", "currency_code", "moneda"))
    if not present or value is None:
        return
    currency = _normalized_key(value)
    if currency not in {"clp", "peso", "pesos", "pesochileno", "pesoschilenos"}:
        raise LegacyAdaptationError(
            "invalid_currency", "only explicitly identified CLP offers are accepted", source=source
        )


def _derive_store_id(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    if not normalized:
        raise LegacyAdaptationError("invalid_store", "could not derive storeId")
    return normalized


def _canonical_payload_hash(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _source_listing_id(
    payload: Mapping[str, Any], merchant_sku: str | None, url: str, source_path: Path | None
) -> str:
    _, explicit = _lookup(
        payload,
        ("sourceListingId", "source_listing_id", "listing_id", "product_id", "variant_id", "id"),
    )
    explicit_text = _clean_optional_text(explicit)
    if explicit_text:
        return explicit_text
    if merchant_sku:
        return merchant_sku
    if source_path is not None and source_path.stem:
        return source_path.stem
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]


def adapt_legacy_offer(
    payload: Mapping[str, Any],
    *,
    run_id: str,
    store_id: str | None = None,
    fetched_at: datetime | str | None = None,
    source_path: str | Path | None = None,
) -> RawOffer:
    """Convert one current scraper JSON object into a strict ``RawOffer``.

    Invalid rows raise ``LegacyAdaptationError`` and should be retained for
    review.  They are never converted into zero-priced or implicitly available
    offers.
    """

    if not isinstance(payload, Mapping):
        raise LegacyAdaptationError("invalid_payload", "legacy payload must be a JSON object")

    source = str(source_path) if source_path is not None else None
    source_path_value = Path(source_path) if source_path is not None else None
    _validate_legacy_currency(payload, source)
    store_name = _required_legacy_text(
        payload,
        ("store_name", "storeName", "merchant", "merchant_name", "store"),
        "store_name",
        source,
    )
    resolved_store_id = str(store_id).strip() if store_id is not None else _derive_store_id(store_name)
    category = _required_legacy_text(
        payload, ("type", "category", "category_name", "product_type"), "category", source
    )
    name = _required_legacy_text(
        payload,
        ("scraped_name", "name", "title", "product_name", "productName", "nombre"),
        "name",
        source,
    )
    url = _required_legacy_text(payload, ("url", "product_url", "permalink", "link"), "url", source)

    _, brand_value = _lookup(payload, ("scraped_brand", "brand", "manufacturer", "marca"))
    brand = _clean_optional_text(brand_value)
    _, sku_value = _lookup(
        payload, ("merchantSku", "merchant_sku", "store_sku", "sku", "variant_sku")
    )
    merchant_sku = _clean_optional_text(sku_value)

    mpns = _collect_identifiers(
        payload,
        ("mpns", "mpn", "part #", "part_number", "partNumber", "manufacturer_part_number"),
    )
    gtins = _collect_identifiers(
        payload, ("gtins", "gtin", "ean", "ean13", "upc", "barcode", "codigo_barra")
    )

    cash_present, cash_price = _read_price(
        payload,
        (
            "cashPrice",
            "cash_price",
            "transferPrice",
            "transfer_price",
            "bank_transfer_price",
            "precio_transferencia",
            "precio_efectivo",
        ),
        "cashPrice",
        source,
    )
    card_present, card_price = _read_price(
        payload,
        ("cardPrice", "card_price", "credit_price", "precio_tarjeta"),
        "cardPrice",
        source,
    )
    normal_present, normal_price = _read_price(
        payload,
        (
            "normalPrice",
            "normal_price",
            "regular_price",
            "list_price",
            "published_price",
            "precio_publicado",
            "precio_normal",
        ),
        "normalPrice",
        source,
    )
    generic_present, generic_price = _read_price(
        payload, ("price", "precio", "current_price", "sale_price"), "price", source
    )

    if generic_present:
        _, price_type_value = _lookup(payload, ("price_type", "priceType", "payment_method"))
        price_type = _normalized_key(price_type_value) if price_type_value is not None else ""
        if price_type in {"cash", "transfer", "banktransfer", "efectivo", "transferencia"}:
            if not cash_present:
                cash_price = generic_price
        elif price_type in {"card", "credit", "tarjeta", "credito"}:
            if not card_present:
                card_price = generic_price
        elif not normal_present:
            # Unknown payment type: this remains a published/normal price.
            normal_price = generic_price

    if not any((cash_present, card_present, normal_present, generic_present)):
        raise LegacyAdaptationError(
            "missing_price", "at least one source price is required", source=source
        )

    _, image_value = _lookup(payload, ("imageUrl", "image_url", "image", "image_src"))
    image_url = _clean_optional_text(image_value)
    _, fetched_value = _lookup(payload, ("fetchedAt", "fetched_at", "scraped_at", "captured_at"))
    captured_at = fetched_value if fetched_value is not None else fetched_at
    if captured_at is None:
        captured_at = datetime.now(timezone.utc)

    raw_offer_data = {
        "runId": str(run_id),
        "storeId": resolved_store_id,
        "category": category,
        "sourceListingId": _source_listing_id(payload, merchant_sku, url, source_path_value),
        "merchantSku": merchant_sku,
        "mpns": mpns,
        "gtins": gtins,
        "brand": brand,
        "name": name,
        "cashPrice": cash_price,
        "cardPrice": card_price,
        "normalPrice": normal_price,
        "currency": "CLP",
        "availability": _parse_availability(payload, source),
        "url": url,
        "imageUrl": image_url,
        "fetchedAt": captured_at,
        "payloadHash": _canonical_payload_hash(payload),
    }
    try:
        return RawOffer.model_validate(raw_offer_data)
    except ValidationError as exc:
        raise LegacyAdaptationError("validation_error", str(exc), source=source) from exc


def discover_scraper_scripts(scrapers_dir: str | Path) -> list[Path]:
    """Return every current ``Scrap_*.py`` adapter without importing it."""

    root = Path(scrapers_dir)
    return sorted(root.glob("Scrap_*.py"), key=lambda path: path.name.casefold())


def discover_legacy_files(input_root: str | Path) -> list[Path]:
    return sorted(Path(input_root).rglob("*.json"), key=lambda path: str(path).casefold())


def adapt_legacy_tree(
    input_root: str | Path,
    *,
    run_id: str,
    fetched_at: datetime | str | None = None,
) -> AdaptationBatch:
    """Adapt every per-product legacy JSON file below an output root."""

    root = Path(input_root)
    files = discover_legacy_files(root)
    offers: list[RawOffer] = []
    issues: list[AdaptationIssue] = []

    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            relative_path = path.relative_to(root)
            file_fetched_at: datetime | str
            if fetched_at is not None:
                file_fetched_at = fetched_at
            else:
                file_fetched_at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
            offer = adapt_legacy_offer(
                payload,
                run_id=run_id,
                store_id=_derive_store_id(path.parent.name),
                fetched_at=file_fetched_at,
                source_path=relative_path,
            )
            offers.append(offer)
        except (OSError, UnicodeError, json.JSONDecodeError, LegacyAdaptationError) as exc:
            code = exc.code if isinstance(exc, LegacyAdaptationError) else "invalid_json"
            issues.append(
                AdaptationIssue(
                    sourcePath=str(path.relative_to(root)),
                    code=code,
                    message=str(exc),
                )
            )

    return AdaptationBatch(rawCount=len(files), offers=tuple(offers), issues=tuple(issues))


def write_ndjson_gzip(offers: Iterable[RawOffer], output_path: str | Path) -> Path:
    """Atomically write validated offers as reproducible UTF-8 NDJSON gzip."""

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")

    try:
        with temporary.open("wb") as raw_file:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                compresslevel=9,
                fileobj=raw_file,
                mtime=0,
            ) as compressed:
                with io.TextIOWrapper(compressed, encoding="utf-8", newline="\n") as text_file:
                    for offer in offers:
                        if not isinstance(offer, RawOffer):
                            raise TypeError("write_ndjson_gzip accepts RawOffer instances only")
                        text_file.write(offer.model_dump_json(exclude_none=False))
                        text_file.write("\n")
        temporary.replace(destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    return destination


def _safe_run_filename(run_id: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9_.-]+", "-", run_id.strip()).strip(".-")
    if not value:
        raise ValueError("run_id cannot produce an empty artifact filename")
    return value[:160]


def export_legacy_run(
    input_root: str | Path,
    output_root: str | Path,
    *,
    run_id: str,
    fetched_at: datetime | str | None = None,
) -> ExportResult:
    """Create a canonical run artifact plus a small manifest and error index."""

    batch = adapt_legacy_tree(input_root, run_id=run_id, fetched_at=fetched_at)
    output_dir = Path(output_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = _safe_run_filename(run_id)
    ndjson_path = write_ndjson_gzip(batch.offers, output_dir / f"{stem}.ndjson.gz")
    compressed_sha = hashlib.sha256(ndjson_path.read_bytes()).hexdigest()

    errors_path = output_dir / f"{stem}.errors.json"
    errors_path.write_text(
        json.dumps([asdict(issue) for issue in batch.issues], ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    manifest_path = output_dir / f"{stem}.manifest.json"
    result = ExportResult(
        runId=run_id,
        rawCount=batch.rawCount,
        offerCount=len(batch.offers),
        errorCount=len(batch.issues),
        ndjsonPath=str(ndjson_path),
        errorsPath=str(errors_path),
        manifestPath=str(manifest_path),
        compressedSha256=compressed_sha,
    )
    manifest_path.write_text(
        json.dumps(asdict(result), ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return result


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Adapt legacy scraper product JSON files to RawOffer NDJSON gzip."
    )
    parser.add_argument("--input", default="ScrapDB/Outputs", help="Legacy output root")
    parser.add_argument("--output", default="ScrapDB/RawRuns", help="Artifact output directory")
    parser.add_argument("--run-id", default=_utc_run_id(), help="Parent scrape run identifier")
    parser.add_argument(
        "--fetched-at",
        default=None,
        help="Optional timezone-aware ISO timestamp applied to every input file",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return a non-zero exit code when any legacy row is quarantined",
    )
    args = parser.parse_args(argv)

    result = export_legacy_run(
        args.input,
        args.output,
        run_id=args.run_id,
        fetched_at=args.fetched_at,
    )
    print(json.dumps(asdict(result), ensure_ascii=False))
    return 1 if args.strict and result.errorCount else 0


if __name__ == "__main__":
    raise SystemExit(main())

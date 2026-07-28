"""Backfill incremental del catalogo legacy al modelo canonico.

La herramienta es deliberadamente aditiva y segura por defecto:

* sin ``--apply`` solo lee y construye un plan (dry-run);
* migra categorias habilitadas, CPU/GPU/Motherboard por defecto;
* usa UUID deterministas y upserts por lotes para poder reintentarla;
* conserva las referencias y URLs legacy sin cambiar las lecturas web;
* una asociacion legacy nunca se convierte en un match difuso.

El cliente Supabase se crea unicamente desde ``main``. Las funciones de
transformacion y ``CanonicalBackfill`` no requieren red y se prueban con un
gateway falso.
"""

from __future__ import annotations

import argparse
import ast
import gzip
import hashlib
import json
import os
import re
import sys
import unicodedata
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Protocol, Sequence
from urllib.parse import parse_qsl, urlencode, urlparse, urlsplit, urlunsplit

from dotenv import load_dotenv

try:
    from ScrapDB.raw_offer import RawOffer
except ModuleNotFoundError:  # Permite ``python ScrapDB/canonical_backfill.py``.
    from raw_offer import RawOffer


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_TTL_HOURS = 48
DEFAULT_BATCH_SIZE = 250
DEFAULT_COMPARISON_DAYS = 14
MAX_MISMATCH_SAMPLES = 50
CHECKPOINT_VERSION = 1
SNAPSHOT_VERSION = 1
FUZZY_CANDIDATE_THRESHOLD = 0.72
FUZZY_AMBIGUITY_GAP = 0.08

# These namespaces must never change: stable IDs make the backfill idempotent
# even when an earlier invocation stopped between table upserts.
PRODUCT_NAMESPACE = uuid.UUID("9614f011-5616-4c02-a28f-eb3f72b9373c")
LISTING_NAMESPACE = uuid.UUID("4503c3ee-29d2-4fd9-9edf-b73709ec3c63")
OFFER_NAMESPACE = uuid.UUID("6f976d14-73d0-4bf5-a1e2-f57acef59795")
PROVENANCE_NAMESPACE = uuid.UUID("a0ae6b0e-3955-4ca1-a095-6e76de5696c5")


@dataclass(frozen=True)
class CategoryConfig:
    key: str
    spec_table: str
    canonical_slug: str
    legacy_slug: str


CATEGORY_CONFIGS: tuple[CategoryConfig, ...] = (
    CategoryConfig("CPU", "CPUSpecifications", "cpu", "cpu"),
    CategoryConfig("GPU", "GpuSpecifications", "gpu", "gpu"),
    CategoryConfig("Motherboard", "MotherboardSpecifications", "motherboard", "motherboard"),
    CategoryConfig("Memory", "RamSpecifications", "ram", "ram"),
    CategoryConfig("Storage", "InternalStorageSpecifications", "internalstorage", "internalstorage"),
    CategoryConfig("PowerSupply", "PowerSupplySpecifications", "powersupply", "powersupply"),
    CategoryConfig("Case", "CaseSpecifications", "case", "case"),
    CategoryConfig("CPUCooler", "CpuCoolerSpecifications", "cpucooler", "cpucooler"),
)

FIRST_WAVE_KEYS = ("CPU", "GPU", "Motherboard")
ESSENTIAL_KEYS = tuple(config.key for config in CATEGORY_CONFIGS)


def _category_aliases() -> dict[str, CategoryConfig]:
    aliases: dict[str, CategoryConfig] = {}
    extra = {
        "processor": "CPU",
        "procesador": "CPU",
        "videocard": "GPU",
        "graphics": "GPU",
        "placamadre": "Motherboard",
        "motherboards": "Motherboard",
        "ram": "Memory",
        "memory": "Memory",
        "almacenamiento": "Storage",
        "internalstorage": "Storage",
        "psu": "PowerSupply",
        "powersupply": "PowerSupply",
        "gabinete": "Case",
        "cooler": "CPUCooler",
        "cpucooler": "CPUCooler",
    }
    by_key = {config.key: config for config in CATEGORY_CONFIGS}
    for config in CATEGORY_CONFIGS:
        for value in (config.key, config.spec_table, config.canonical_slug, config.legacy_slug):
            aliases[normalize_flag(value)] = config
    for alias, key in extra.items():
        aliases[normalize_flag(alias)] = by_key[key]
    return aliases


def normalize_flag(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def resolve_categories(raw: str | Sequence[str] | None, all_essential: bool = False) -> list[CategoryConfig]:
    """Resolve category feature flags, preserving the configured order."""

    if all_essential:
        requested = list(ESSENTIAL_KEYS)
    elif raw is None or (isinstance(raw, str) and not raw.strip()):
        requested = list(FIRST_WAVE_KEYS)
    elif isinstance(raw, str):
        requested = [part for part in re.split(r"[,;\s]+", raw) if part]
    else:
        requested = [str(part) for part in raw if str(part).strip()]

    aliases = _category_aliases()
    selected: list[CategoryConfig] = []
    seen: set[str] = set()
    unknown: list[str] = []
    for value in requested:
        config = aliases.get(normalize_flag(value))
        if not config:
            unknown.append(value)
            continue
        if config.key not in seen:
            selected.append(config)
            seen.add(config.key)
    if unknown:
        raise ValueError(
            "Categorias desconocidas: "
            + ", ".join(unknown)
            + ". Validas: "
            + ", ".join(ESSENTIAL_KEYS)
        )
    if not selected:
        raise ValueError("Debe habilitar al menos una categoria canonica.")
    return selected


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalized_identifier(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").strip().upper())


def normalized_gtin(value: Any) -> str:
    digits = re.sub(r"\D+", "", str(value or ""))
    return digits if len(digits) in {8, 12, 13, 14} else ""


def normalize_match_url(value: Any) -> str:
    """Canonicalize a merchant URL without turning it into a fuzzy signal."""

    text = text_or_none(value)
    if not text:
        return ""
    try:
        parsed = urlsplit(text)
    except ValueError:
        return ""
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        return ""
    host = parsed.hostname.lower()
    try:
        port = parsed.port
    except ValueError:
        return ""
    if port and not ((parsed.scheme.lower() == "http" and port == 80) or (parsed.scheme.lower() == "https" and port == 443)):
        host = f"{host}:{port}"
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    ignored = {"fbclid", "gclid", "ref", "source"}
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in ignored and not key.lower().startswith("utm_")
    ]
    query.sort()
    return urlunsplit((parsed.scheme.lower(), host, path, urlencode(query), ""))


def parse_multiple_identifiers(raw: Any) -> list[str]:
    """Parse legacy MPN values, including Python/JSON arrays stored as text."""

    if raw is None:
        return []
    values: Iterable[Any]
    if isinstance(raw, (list, tuple, set)):
        values = raw
    else:
        text = str(raw).strip()
        if not text:
            return []
        parsed: Any = None
        if text[:1] in ("[", "(", "{"):
            try:
                parsed = json.loads(text)
            except (json.JSONDecodeError, TypeError):
                try:
                    parsed = ast.literal_eval(text)
                except (ValueError, SyntaxError):
                    parsed = None
        if isinstance(parsed, Mapping):
            values = parsed.values()
        elif isinstance(parsed, (list, tuple, set)):
            values = parsed
        elif parsed is not None and not isinstance(parsed, (dict, list, tuple, set)):
            values = [parsed]
        else:
            values = re.split(r"[,;|\r\n]+", text)

    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        candidate = str(value or "").strip().strip("'\"")
        normalized = normalized_identifier(candidate)
        if not candidate or not normalized or normalized in seen:
            continue
        output.append(candidate)
        seen.add(normalized)
    return output


def extract_spec_identifiers(spec: Mapping[str, Any]) -> dict[str, list[str]]:
    """Split identifiers stored in legacy scalar/array text fields."""

    result: dict[str, list[str]] = {"mpn": parse_multiple_identifiers(spec.get("MetaPartNumber"))}
    gtin_sources = {
        "gtin": ("GTIN", "Gtin", "MetaGTIN", "MetaGtin", "Barcode"),
        "ean": ("EAN", "Ean", "EAN13", "MetaEAN"),
        "upc": ("UPC", "Upc", "MetaUPC"),
    }
    seen_gtins: set[str] = set()
    for identifier_type, keys in gtin_sources.items():
        values: list[str] = []
        for key in keys:
            for raw_value in parse_multiple_identifiers(spec.get(key)):
                value = normalized_gtin(raw_value)
                if value and value not in seen_gtins:
                    values.append(value)
                    seen_gtins.add(value)
        if values:
            result[identifier_type] = values
    return result


def text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def positive_price(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(round(float(value)))
    except (TypeError, ValueError, OverflowError):
        return None
    return number if number > 0 else None


def stock_is_available(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    normalized = normalize_flag(value)
    return normalized in {"true", "t", "1", "yes", "si", "instock", "available", "disponible"}


def is_http_url(value: Any) -> bool:
    text = text_or_none(value)
    if not text:
        return False
    parsed = urlparse(text)
    return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc)


def stable_uuid(namespace: uuid.UUID, *parts: Any) -> str:
    material = "\x1f".join(str(part).strip() for part in parts)
    return str(uuid.uuid5(namespace, material))


def canonical_json_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def slugify(value: Any) -> str:
    normalized = unicodedata.normalize("NFD", str(value or ""))
    ascii_text = "".join(char for char in normalized if unicodedata.category(char) != "Mn")
    return re.sub(r"(^-|-$)+", "", re.sub(r"[^a-z0-9]+", "-", ascii_text.lower()))


def legacy_product_url(category_slug: str, name: str, spec_id: str) -> str:
    product_slug = slugify(name) or "producto"
    fragment = str(spec_id).replace("-", "").lower()[-8:]
    return f"/producto/{category_slug}/{product_slug}-{fragment}"


def canonical_product_name(spec: Mapping[str, Any], spec_id: str, mpns: Sequence[str]) -> tuple[str, bool]:
    explicit = text_or_none(spec.get("MetaName"))
    if explicit:
        return explicit, False
    brand = text_or_none(spec.get("MetaManufacturer"))
    model = text_or_none(spec.get("Model"))
    derived = " ".join(part for part in (brand, model) if part)
    if derived:
        return derived, True
    if mpns:
        return mpns[0], True
    return f"Componente legado {spec_id}", True


PRODUCT_METADATA_KEYS = {
    "Id",
    "MetaName",
    "MetaManufacturer",
    "MetaPartNumber",
    "ImageUrl",
    "pcpp_link",
    "LastUpdated",
}


def canonical_specs(spec: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in spec.items()
        if key not in PRODUCT_METADATA_KEYS and value is not None and value != ""
    }


def build_product_rows(
    config: CategoryConfig,
    spec: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any], list[dict[str, Any]], bool]:
    spec_id = str(spec.get("Id") or "").strip()
    if not spec_id:
        raise ValueError(f"{config.spec_table}: fila sin Id")
    product_id = stable_uuid(PRODUCT_NAMESPACE, config.spec_table, spec_id)
    identifier_values = extract_spec_identifiers(spec)
    mpns = identifier_values.get("mpn", [])
    name, used_fallback = canonical_product_name(spec, spec_id, mpns)
    brand = text_or_none(spec.get("MetaManufacturer"))
    model = text_or_none(spec.get("Model"))
    image_url = text_or_none(spec.get("ImageUrl"))
    source_url = text_or_none(spec.get("pcpp_link"))

    product = {
        "id": product_id,
        "category": config.canonical_slug,
        "brand": brand,
        "model": model,
        "name": name,
        "specs": canonical_specs(spec),
        "specs_version": 1,
        "image_url": image_url,
        "image_authorized": False,
        "status": "active",
    }
    identifiers: list[dict[str, Any]] = []
    for identifier_type, values in identifier_values.items():
        for index, value in enumerate(values):
            normalized = normalized_gtin(value) if identifier_type in {"gtin", "ean", "upc"} else normalized_identifier(value)
            identifiers.append(
                {
                    "id": stable_uuid(
                        PROVENANCE_NAMESPACE,
                        "identifier-row",
                        product_id,
                        identifier_type,
                        normalized,
                    ),
                    "product_id": product_id,
                    "identifier_type": identifier_type,
                    "value": value,
                    "normalized_value": normalized,
                    "is_primary": index == 0,
                }
            )
    legacy_ref = {
        "id": stable_uuid(PROVENANCE_NAMESPACE, "legacy-ref", config.spec_table, spec_id),
        "product_id": product_id,
        "spec_table_name": config.spec_table,
        "spec_id": spec_id,
        "category_slug": config.legacy_slug,
        "legacy_url": legacy_product_url(config.legacy_slug, name, spec_id),
    }
    provenance = [
        {
            "id": stable_uuid(PROVENANCE_NAMESPACE, "specs", product_id),
            "product_id": product_id,
            "merchant_listing_id": None,
            "asset_type": "specs",
            "source_name": "legacy specifications import",
            "source_url": source_url,
            "permission_status": "unknown",
            "permission_evidence": "Migrated from specifications.*; permission must be reviewed before monetization.",
        }
    ]
    if identifiers:
        provenance.append(
            {
                "id": stable_uuid(PROVENANCE_NAMESPACE, "identifier", product_id),
                "product_id": product_id,
                "merchant_listing_id": None,
                "asset_type": "identifier",
                "source_name": "legacy specifications import",
                "source_url": source_url,
                "permission_status": "unknown",
                "permission_evidence": "Legacy MetaPartNumber split and normalized; source permission not established.",
            }
        )
    if image_url:
        provenance.append(
            {
                "id": stable_uuid(PROVENANCE_NAMESPACE, "image", product_id),
                "product_id": product_id,
                "merchant_listing_id": None,
                "asset_type": "image",
                "source_name": "legacy specifications import",
                "source_url": image_url,
                "permission_status": "unknown",
                "permission_evidence": "image_authorized=false until written permission or an owned replacement exists.",
            }
        )
    return product, identifiers, legacy_ref, provenance, used_fallback


def build_pricing_rows(
    config: CategoryConfig,
    spec: Mapping[str, Any],
    pricing: Mapping[str, Any],
    now: datetime,
    ttl_hours: int,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]:
    """Translate one ProductPricing row; never publishes zero or stale prices."""

    spec_id = str(spec.get("Id") or "").strip()
    store_id_raw = pricing.get("StoreId")
    try:
        store_id = int(store_id_raw)
    except (TypeError, ValueError):
        return None, None, "invalid_store_id"
    direct_url = text_or_none(pricing.get("Url"))
    if not is_http_url(direct_url):
        return None, None, "invalid_or_missing_url"

    product_id = stable_uuid(PRODUCT_NAMESPACE, config.spec_table, spec_id)
    source_listing_id = f"legacy:{config.spec_table}:{spec_id}"
    listing_id = stable_uuid(LISTING_NAMESPACE, store_id, source_listing_id)
    offer_id = stable_uuid(OFFER_NAMESPACE, listing_id)
    mpns = parse_multiple_identifiers(spec.get("MetaPartNumber"))
    name, _ = canonical_product_name(spec, spec_id, mpns)
    captured = parse_datetime(pricing.get("LastSeenAt")) or parse_datetime(pricing.get("LastUpdated"))
    has_known_capture = captured is not None
    captured = captured or now
    expires = captured + timedelta(hours=ttl_hours)
    price = positive_price(pricing.get("Price"))
    in_stock = stock_is_available(pricing.get("StockStatus"))

    if not in_stock:
        public_state = "unavailable"
        confidence = 1.0
    elif price is None or not has_known_capture:
        public_state = "suspect"
        confidence = 0.0
    elif expires <= now:
        public_state = "stale"
        confidence = 0.5
    else:
        public_state = "available"
        confidence = 1.0

    payload_hash = canonical_json_hash(
        {
            "SpecTableName": config.spec_table,
            "SpecId": spec_id,
            "StoreId": store_id,
            "Price": pricing.get("Price"),
            "StockStatus": pricing.get("StockStatus"),
            "Url": direct_url,
            "LastSeenAt": pricing.get("LastSeenAt"),
            "LastUpdated": pricing.get("LastUpdated"),
        }
    )
    listing = {
        "id": listing_id,
        "store_id": store_id,
        "product_id": product_id,
        "source_listing_id": source_listing_id,
        "merchant_sku": None,
        "name": name,
        "brand": text_or_none(spec.get("MetaManufacturer")),
        "url": direct_url,
        "image_url": text_or_none(spec.get("ImageUrl")),
        "match_status": "matched",
        "match_method": "manual",
        "match_confidence": 1.0,
        "consecutive_missing_complete": 0,
        "last_seen_at": iso_utc(captured) if has_known_capture else None,
        "last_seen_run_id": None,
        "payload_hash": payload_hash,
    }
    offer = {
        "id": offer_id,
        "merchant_listing_id": listing_id,
        "cash_price": None,
        "card_price": None,
        "normal_price": None,
        "published_price": price,
        "currency": "CLP",
        "public_state": public_state,
        "confidence": confidence,
        "captured_at": iso_utc(captured),
        "expires_at": iso_utc(expires),
        "last_seen_run_id": None,
        "payload_hash": payload_hash,
    }
    return listing, offer, None


@dataclass(frozen=True)
class ProductProfile:
    product_id: str
    category: str
    brand: str | None
    name: str


@dataclass(frozen=True)
class MatchDecision:
    status: str
    method: str
    product_id: str | None
    confidence: float | None
    candidate_product_ids: tuple[str, ...] = ()
    reason: str = ""


@dataclass
class RawOfferMatchIndex:
    identifiers: dict[tuple[str, str], set[str]] = field(default_factory=dict)
    persistent_skus: dict[tuple[int, str], set[str]] = field(default_factory=dict)
    persistent_urls: dict[tuple[int, str], set[str]] = field(default_factory=dict)
    persistent_source_ids: dict[tuple[int, str], set[str]] = field(default_factory=dict)
    profiles: list[ProductProfile] = field(default_factory=list)
    profiles_by_id: dict[str, ProductProfile] = field(default_factory=dict)
    fuzzy_token_postings: dict[tuple[str, str], set[str]] = field(default_factory=dict)
    product_categories: dict[str, str] = field(default_factory=dict)
    legacy_refs: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    @classmethod
    def from_rows(
        cls,
        products: Sequence[Mapping[str, Any]],
        identifiers: Sequence[Mapping[str, Any]],
        listings: Sequence[Mapping[str, Any]],
        legacy_refs: Sequence[Mapping[str, Any]] = (),
    ) -> "RawOfferMatchIndex":
        index = cls()
        valid_products = {str(row.get("id")) for row in products if row.get("id")}
        index.product_categories = {
            str(row["id"]): str(row.get("category") or "")
            for row in products
            if row.get("id")
        }
        index.profiles = [
            ProductProfile(
                product_id=str(row["id"]),
                category=str(row.get("category") or ""),
                brand=text_or_none(row.get("brand")),
                name=str(row.get("name") or ""),
            )
            for row in products
            if row.get("id") and text_or_none(row.get("name"))
        ]
        index.profiles_by_id = {profile.product_id: profile for profile in index.profiles}
        for profile in index.profiles:
            for token in set(_normalized_fuzzy_text(profile.name).split()):
                if len(token) >= 3:
                    index.fuzzy_token_postings.setdefault(
                        (profile.category, token), set()
                    ).add(profile.product_id)
        for row in identifiers:
            product_id = str(row.get("product_id") or "")
            identifier_type = str(row.get("identifier_type") or "").lower()
            value = str(row.get("normalized_value") or row.get("value") or "")
            if product_id not in valid_products:
                continue
            if identifier_type in {"gtin", "ean", "upc"}:
                normalized = normalized_gtin(value)
                group = "gtin"
            elif identifier_type == "mpn":
                normalized = normalized_identifier(value)
                group = "mpn"
            else:
                continue
            if normalized:
                index.identifiers.setdefault((group, normalized), set()).add(product_id)

        for row in listings:
            product_id = str(row.get("product_id") or "")
            try:
                store_id = int(row.get("store_id"))
            except (TypeError, ValueError):
                continue
            if product_id not in valid_products:
                continue
            sku = normalized_identifier(row.get("merchant_sku"))
            url = normalize_match_url(row.get("url"))
            source_id = normalized_identifier(row.get("source_listing_id"))
            if sku:
                index.persistent_skus.setdefault((store_id, sku), set()).add(product_id)
            if url:
                index.persistent_urls.setdefault((store_id, url), set()).add(product_id)
            if source_id:
                index.persistent_source_ids.setdefault((store_id, source_id), set()).add(product_id)

        for row in legacy_refs:
            product_id = str(row.get("product_id") or "")
            if product_id in valid_products:
                index.legacy_refs.setdefault(product_id, []).append(dict(row))
        return index


def _exact_decision(
    product_ids: set[str], method: str, reason: str, confidence: float
) -> MatchDecision | None:
    if not product_ids:
        return None
    ordered = tuple(sorted(product_ids))
    if len(ordered) == 1:
        return MatchDecision("matched", method, ordered[0], confidence, ordered, reason)
    return MatchDecision(
        "candidate",
        method,
        None,
        min(confidence, 0.80),
        ordered,
        f"ambiguous_{reason}",
    )


def _global_exact_decision(
    product_ids: set[str],
    method: str,
    reason: str,
    confidence: float,
    category: CategoryConfig,
    index: RawOfferMatchIndex,
) -> MatchDecision | None:
    """Publish an exact identifier only when it is globally unambiguous.

    Filtering collisions by the incoming category would hide a duplicate GTIN
    or brand+MPN pair elsewhere in the catalog.  A category conflict therefore
    remains a review candidate instead of becoming an automatic association.
    """

    decision = _exact_decision(product_ids, method, reason, confidence)
    if decision is None or decision.status != "matched" or not decision.product_id:
        return decision
    if index.product_categories.get(decision.product_id) == category.canonical_slug:
        return decision
    return MatchDecision(
        "candidate",
        method,
        None,
        min(confidence, 0.80),
        decision.candidate_product_ids,
        f"{reason}_category_conflict",
    )


def _global_mpn_decision(
    product_ids: set[str],
    category: CategoryConfig,
    index: RawOfferMatchIndex,
) -> MatchDecision | None:
    """Match a globally unique MPN without using merchant brand/category.

    MPN collisions remain review-only.  When the scraper category is wrong,
    the unique canonical product is authoritative and the correction is
    exposed in telemetry instead of blocking publication.
    """

    decision = _exact_decision(product_ids, "exact_mpn", "mpn", 0.99)
    if decision is None or decision.status != "matched" or not decision.product_id:
        return decision
    if index.product_categories.get(decision.product_id) == category.canonical_slug:
        return MatchDecision(
            decision.status,
            decision.method,
            decision.product_id,
            decision.confidence,
            decision.candidate_product_ids,
            "exact_mpn",
        )
    return MatchDecision(
        decision.status,
        decision.method,
        decision.product_id,
        decision.confidence,
        decision.candidate_product_ids,
        "category_corrected_from_mpn",
    )


def _normalized_fuzzy_text(value: Any) -> str:
    folded = unicodedata.normalize("NFKD", str(value or "").casefold())
    ascii_value = folded.encode("ascii", "ignore").decode("ascii")
    return " ".join(re.findall(r"[a-z0-9]+", ascii_value))


def _fuzzy_score(raw_name: str, product_name: str) -> float:
    left = _normalized_fuzzy_text(raw_name)
    right = _normalized_fuzzy_text(product_name)
    if not left or not right:
        return 0.0
    sequence = SequenceMatcher(None, left, right, autojunk=False).ratio()
    left_tokens = set(left.split())
    right_tokens = set(right.split())
    union = left_tokens | right_tokens
    jaccard = len(left_tokens & right_tokens) / len(union) if union else 0.0
    return round(sequence * 0.65 + jaccard * 0.35, 4)


def match_raw_offer(
    offer: RawOffer,
    store_id: int,
    category: CategoryConfig,
    index: RawOfferMatchIndex,
    *,
    fuzzy_threshold: float = FUZZY_CANDIDATE_THRESHOLD,
) -> MatchDecision:
    """Apply exact MPN/persistent associations; every other signal is review-only."""

    mpn_hits: set[str] = set()
    for value in offer.mpns:
        normalized = normalized_identifier(value)
        if normalized:
            mpn_hits.update(index.identifiers.get(("mpn", normalized), set()))
    if mpn_hits:
        decision = _global_mpn_decision(mpn_hits, category, index)
        if decision:
            return decision

    persistent_hits: set[str] = set()
    sku = normalized_identifier(offer.merchantSku)
    if sku:
        persistent_hits.update(index.persistent_skus.get((store_id, sku), set()))
    source_id = normalized_identifier(offer.sourceListingId)
    if source_id:
        persistent_hits.update(index.persistent_source_ids.get((store_id, source_id), set()))
    url = normalize_match_url(str(offer.url))
    if url:
        persistent_hits.update(index.persistent_urls.get((store_id, url), set()))
    decision = _global_exact_decision(
        persistent_hits,
        "persistent_sku",
        "persistent_listing",
        0.98,
        category,
        index,
    )
    if decision:
        return decision

    normalized_brand = _normalized_fuzzy_text(offer.brand)
    scored: list[tuple[float, str]] = []
    raw_tokens = {
        token
        for token in _normalized_fuzzy_text(offer.name).split()
        if len(token) >= 3
    }
    postings = sorted(
        (
            index.fuzzy_token_postings.get((category.canonical_slug, token), set())
            for token in raw_tokens
        ),
        key=len,
    )
    candidate_ids: set[str] = set()
    for posting in postings:
        if not posting:
            continue
        if not candidate_ids:
            candidate_ids.update(posting)
        elif len(candidate_ids | posting) <= 2_000:
            candidate_ids.update(posting)
        if len(candidate_ids) >= 2_000:
            break
    # A generic token shared by thousands of products is not useful evidence;
    # leaving it unmatched is safer and avoids an unbounded O(raw*catalog) scan.
    if len(candidate_ids) > 2_000:
        candidate_ids.clear()
    for product_id in candidate_ids:
        profile = index.profiles_by_id[product_id]
        profile_brand = _normalized_fuzzy_text(profile.brand)
        if normalized_brand and profile_brand and normalized_brand != profile_brand:
            continue
        score = _fuzzy_score(offer.name, profile.name)
        if score >= fuzzy_threshold:
            scored.append((score, profile.product_id))
    scored.sort(key=lambda item: (-item[0], item[1]))
    if scored:
        best_score, best_id = scored[0]
        close = tuple(
            product_id
            for score, product_id in scored
            if best_score - score < FUZZY_AMBIGUITY_GAP
        )
        candidate_id = best_id if len(close) == 1 else None
        # This status is invariant: fuzzy evidence is review-only and can never publish.
        return MatchDecision(
            "candidate",
            "fuzzy_candidate",
            candidate_id,
            min(best_score, 0.89),
            close,
            "fuzzy_review_required",
        )
    reason = "mpn_not_found" if offer.mpns else "no_exact_match"
    return MatchDecision("unmatched", "none", None, None, (), reason)


def resolve_raw_category(value: str, enabled: Sequence[CategoryConfig]) -> CategoryConfig | None:
    resolved = _category_aliases().get(normalize_flag(value))
    if not resolved:
        return None
    enabled_by_key = {config.key: config for config in enabled}
    return enabled_by_key.get(resolved.key)


def resolve_raw_store_id(value: str, stores: Mapping[int, str]) -> int | None:
    text = str(value or "").strip()
    try:
        numeric = int(text)
    except ValueError:
        numeric = None
    if numeric is not None and numeric in stores:
        return numeric
    normalized = normalize_flag(text)
    matches = [store_id for store_id, name in stores.items() if normalize_flag(name) == normalized]
    return matches[0] if len(matches) == 1 else None


def parse_optional_uuid(value: Any) -> str | None:
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError, AttributeError):
        return None


def build_raw_offer_rows(
    offer: RawOffer,
    store_id: int,
    category: CategoryConfig,
    decision: MatchDecision,
    now: datetime,
    ttl_hours: int,
    known_run_ids: set[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    listing_id = stable_uuid(LISTING_NAMESPACE, store_id, offer.sourceListingId)
    captured = offer.fetchedAt.astimezone(timezone.utc)
    expires = captured + timedelta(hours=ttl_hours)
    availability = str(offer.availability)
    if availability == "unavailable":
        public_state = "unavailable"
        confidence = 1.0 if decision.status == "matched" else 0.0
    elif captured < now - timedelta(hours=ttl_hours):
        public_state = "stale"
        confidence = 0.5 if decision.status == "matched" else 0.0
    elif availability == "available" and decision.status == "matched":
        public_state = "available"
        confidence = decision.confidence or 0.0
    else:
        public_state = "suspect"
        confidence = 0.0
    run_id = parse_optional_uuid(offer.runId)
    if not known_run_ids or run_id not in known_run_ids:
        run_id = None
    listing = {
        "id": listing_id,
        "store_id": store_id,
        "product_id": decision.product_id,
        "source_listing_id": offer.sourceListingId,
        "merchant_sku": offer.merchantSku,
        "name": offer.name,
        "brand": offer.brand,
        "url": str(offer.url),
        "image_url": str(offer.imageUrl) if offer.imageUrl else None,
        "match_status": decision.status,
        "match_method": decision.method,
        "match_confidence": decision.confidence,
        "consecutive_missing_complete": 0,
        "last_seen_at": iso_utc(captured),
        "last_seen_run_id": run_id,
        "payload_hash": offer.payloadHash,
    }
    canonical_offer = {
        "id": stable_uuid(OFFER_NAMESPACE, listing_id),
        "merchant_listing_id": listing_id,
        "cash_price": offer.cashPrice,
        "card_price": offer.cardPrice,
        # RawOffer keeps the legacy generic channel in normalPrice too. Until
        # adapters identify it unambiguously, the conservative public label is
        # "precio publicado", never a claimed normal/card/cash method.
        "normal_price": None,
        "published_price": offer.normalPrice,
        "currency": "CLP",
        "public_state": public_state,
        "confidence": confidence,
        "captured_at": iso_utc(captured),
        "expires_at": iso_utc(expires),
        "last_seen_run_id": run_id,
        "payload_hash": offer.payloadHash,
    }
    return listing, canonical_offer


class CheckpointFile:
    """Atomic local cursor state. It never writes unless the caller is in apply mode."""

    def __init__(self, path: Path, fingerprint: str, *, resume: bool = False):
        self.path = path
        self.fingerprint = fingerprint
        self.state: dict[str, Any] = {
            "version": CHECKPOINT_VERSION,
            "fingerprint": fingerprint,
            "categories": {},
        }
        if resume:
            if not path.is_file():
                raise ValueError(f"No existe checkpoint para --resume: {path}")
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as error:
                raise ValueError(f"Checkpoint invalido: {path}: {error}") from error
            if loaded.get("version") != CHECKPOINT_VERSION:
                raise ValueError("Version de checkpoint incompatible.")
            if loaded.get("fingerprint") != fingerprint:
                raise ValueError("El checkpoint pertenece a otra seleccion/configuracion.")
            if not isinstance(loaded.get("categories"), dict):
                raise ValueError("Checkpoint sin mapa de categorias valido.")
            self.state = loaded
        else:
            self._persist()

    def _persist(self) -> None:
        self.state["updatedAt"] = iso_utc(utc_now())
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.tmp")
        temporary.write_text(
            json.dumps(self.state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.path)

    def category_state(self, spec_table: str) -> dict[str, Any]:
        value = self.state["categories"].get(spec_table, {})
        return value if isinstance(value, dict) else {}

    def after_id(self, spec_table: str) -> str | None:
        return text_or_none(self.category_state(spec_table).get("afterId"))

    def completed(self, spec_table: str) -> bool:
        return self.category_state(spec_table).get("completed") is True

    def save(self, spec_table: str, after_id: str | None, *, completed: bool) -> None:
        self.state["categories"][spec_table] = {
            "afterId": after_id,
            "completed": completed,
        }
        self._persist()

    def raw_after_count(self, artifact_hash: str) -> int:
        state = self.state.get("rawOffers")
        if not isinstance(state, dict):
            return 0
        if state.get("artifactSha256") != artifact_hash:
            raise ValueError("El checkpoint RawOffer pertenece a otro artefacto.")
        try:
            count = int(state.get("afterOfferCount", 0))
        except (TypeError, ValueError) as error:
            raise ValueError("Checkpoint RawOffer con cursor invalido.") from error
        if count < 0:
            raise ValueError("Checkpoint RawOffer con cursor negativo.")
        return count

    def raw_completed(self, artifact_hash: str) -> bool:
        state = self.state.get("rawOffers")
        if not isinstance(state, dict):
            return False
        if state.get("artifactSha256") != artifact_hash:
            raise ValueError("El checkpoint RawOffer pertenece a otro artefacto.")
        return state.get("completed") is True

    def save_raw(self, artifact_hash: str, after_offer_count: int, *, completed: bool) -> None:
        self.state["rawOffers"] = {
            "artifactSha256": artifact_hash,
            "afterOfferCount": after_offer_count,
            "completed": completed,
        }
        self._persist()


def checkpoint_fingerprint(
    categories: Sequence[CategoryConfig],
    ttl_hours: int,
    *,
    dual_write: bool = False,
) -> str:
    return canonical_json_hash(
        {
            "checkpointVersion": CHECKPOINT_VERSION,
            "categories": [config.spec_table for config in categories],
            "ttlHours": ttl_hours,
            "dualWriteLegacy": dual_write,
        }
    )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as input_file:
            for block in iter(lambda: input_file.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as error:
        raise ValueError(f"No se pudo leer {path}: {error}") from error
    return digest.hexdigest()


class BackfillGateway(Protocol):
    def iter_specifications(
        self, spec_table: str, batch_size: int, start_after: str | None = None
    ) -> Iterator[list[dict[str, Any]]]: ...

    def load_pricing_for_specs(
        self, spec_table: str, spec_ids: Sequence[str], batch_size: int
    ) -> list[dict[str, Any]]: ...

    def load_stores(self, batch_size: int) -> dict[int, str]: ...

    def upsert_rows(
        self, table: str, rows: Sequence[dict[str, Any]], on_conflict: str, batch_size: int
    ) -> None: ...

    def load_rows_by_values(
        self,
        table: str,
        value_column: str,
        values: Sequence[Any],
        columns: str,
        batch_size: int,
        equals: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]: ...

    def load_match_catalog(
        self, categories: Sequence[str], batch_size: int
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]: ...


def chunks(values: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


class SnapshotGateway:
    """Read-only gateway for an explicit, local backfill snapshot.

    Dry-runs use this adapter and therefore cannot create a Supabase client or
    mutate remote state.  The snapshot may include current canonical rows so
    ``--compare`` can produce a dual-read report offline.
    """

    TABLE_NAMES = (
        "products",
        "product_identifiers",
        "merchant_listings",
        "offers",
        "source_provenance",
        "legacy_product_refs",
        "scrape_runs",
    )

    def __init__(self, payload: Mapping[str, Any]):
        if payload.get("snapshotFormatVersion") != SNAPSHOT_VERSION:
            raise ValueError(
                f"snapshotFormatVersion debe ser {SNAPSHOT_VERSION}."
            )
        specifications = payload.get("specifications", {})
        if not isinstance(specifications, Mapping):
            raise ValueError("El snapshot requiere un objeto specifications.")
        self.specifications = {
            str(table): self._rows(rows, f"specifications.{table}")
            for table, rows in specifications.items()
        }
        self.pricing = self._rows(
            payload.get("ProductPricing", payload.get("productPricing", [])),
            "ProductPricing",
        )
        canonical = payload.get("canonical", {})
        if not isinstance(canonical, Mapping):
            raise ValueError("canonical debe ser un objeto cuando esta presente.")
        self.tables = {
            table: self._rows(canonical.get(table, payload.get(table, [])), table)
            for table in self.TABLE_NAMES
        }
        self.stores = self._parse_stores(payload.get("Stores", payload.get("stores", [])))

    @staticmethod
    def _rows(value: Any, label: str) -> list[dict[str, Any]]:
        if value is None:
            return []
        if not isinstance(value, list) or any(not isinstance(row, Mapping) for row in value):
            raise ValueError(f"{label} debe ser un arreglo de objetos.")
        return [dict(row) for row in value]

    @staticmethod
    def _parse_stores(value: Any) -> dict[int, str]:
        if isinstance(value, Mapping):
            rows: list[Mapping[str, Any]] = []
            for key, item in value.items():
                if isinstance(item, Mapping):
                    rows.append({"Id": key, **item})
                else:
                    rows.append({"Id": key, "Name": item})
        elif isinstance(value, list) and all(isinstance(row, Mapping) for row in value):
            rows = list(value)
        else:
            raise ValueError("Stores/stores debe ser un arreglo u objeto.")
        stores: dict[int, str] = {}
        for row in rows:
            try:
                store_id = int(row.get("Id", row.get("id")))
            except (TypeError, ValueError):
                raise ValueError("El snapshot contiene una tienda sin Id entero.") from None
            name = text_or_none(row.get("Name", row.get("name")))
            if not name:
                raise ValueError(f"La tienda {store_id} no tiene Name.")
            if store_id in stores:
                raise ValueError(f"La tienda {store_id} esta duplicada en el snapshot.")
            stores[store_id] = name
        return stores

    @classmethod
    def from_path(cls, path: Path) -> "SnapshotGateway":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ValueError(f"Snapshot invalido {path}: {error}") from error
        if not isinstance(payload, Mapping):
            raise ValueError("El snapshot debe ser un objeto JSON.")
        return cls(payload)

    def iter_specifications(
        self, spec_table: str, batch_size: int, start_after: str | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        rows = sorted(
            self.specifications.get(spec_table, []),
            key=lambda row: str(row.get("Id") or ""),
        )
        if start_after:
            rows = [row for row in rows if str(row.get("Id") or "") > start_after]
        for batch in chunks(rows, batch_size):
            yield [dict(row) for row in batch]

    def load_pricing_for_specs(
        self, spec_table: str, spec_ids: Sequence[str], batch_size: int
    ) -> list[dict[str, Any]]:
        del batch_size
        accepted = {str(value) for value in spec_ids}
        return [
            dict(row)
            for row in self.pricing
            if str(row.get("SpecTableName") or "") == spec_table
            and str(row.get("SpecId") or "") in accepted
        ]

    def load_stores(self, batch_size: int) -> dict[int, str]:
        del batch_size
        return dict(self.stores)

    def upsert_rows(
        self, table: str, rows: Sequence[dict[str, Any]], on_conflict: str, batch_size: int
    ) -> None:
        del table, rows, on_conflict, batch_size
        raise RuntimeError("Un snapshot local es de solo lectura; use --apply para escribir.")

    def load_rows_by_values(
        self,
        table: str,
        value_column: str,
        values: Sequence[Any],
        columns: str,
        batch_size: int,
        equals: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        del columns, batch_size
        accepted = {str(value) for value in values}
        return [
            dict(row)
            for row in self.tables.get(table, [])
            if str(row.get(value_column)) in accepted
            and all(row.get(key) == value for key, value in (equals or {}).items())
        ]

    def load_match_catalog(
        self, categories: Sequence[str], batch_size: int
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        del categories, batch_size
        # Exact MPN uniqueness is global, so the index intentionally includes
        # every active product, not only the rollout categories.
        products = [
            dict(row)
            for row in self.tables["products"]
            if row.get("status", "active") == "active"
        ]
        product_ids = {str(row.get("id")) for row in products if row.get("id")}

        def related(table: str) -> list[dict[str, Any]]:
            return [
                dict(row)
                for row in self.tables[table]
                if str(row.get("product_id") or "") in product_ids
            ]

        return (
            products,
            related("product_identifiers"),
            related("merchant_listings"),
            related("legacy_product_refs"),
        )


class SupabaseGateway:
    """Thin batched adapter around supabase-py/PostgREST."""

    def __init__(self, client: Any):
        self.client = client

    @staticmethod
    def _data(response: Any) -> list[dict[str, Any]]:
        return list(getattr(response, "data", None) or [])

    def _paginate(self, query_factory: Any, batch_size: int) -> Iterator[list[dict[str, Any]]]:
        offset = 0
        while True:
            query = query_factory().range(offset, offset + batch_size - 1)
            rows = self._data(query.execute())
            if not rows:
                break
            yield rows
            if len(rows) < batch_size:
                break
            offset += batch_size

    def iter_specifications(
        self, spec_table: str, batch_size: int, start_after: str | None = None
    ) -> Iterator[list[dict[str, Any]]]:
        def factory() -> Any:
            query = self.client.schema("specifications").from_(spec_table).select("*").order("Id")
            return query.gt("Id", start_after) if start_after else query

        yield from self._paginate(factory, batch_size)

    def load_pricing_for_specs(
        self, spec_table: str, spec_ids: Sequence[str], batch_size: int
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for id_batch in chunks(list(spec_ids), batch_size):
            output.extend(
                row
                for page in self._paginate(
                    lambda id_batch=id_batch: self.client.table("ProductPricing")
                    .select(
                        "SpecId,SpecTableName,StoreId,Price,StockStatus,Url,AffiliateUrl,"
                        "LastUpdated,LastSeenAt,StockConfidence"
                    )
                    .eq("SpecTableName", spec_table)
                    .in_("SpecId", list(id_batch))
                    .order("SpecId"),
                    batch_size,
                )
                for row in page
            )
        return output

    def load_stores(self, batch_size: int) -> dict[int, str]:
        stores: dict[int, str] = {}
        for page in self._paginate(lambda: self.client.table("Stores").select("Id,Name").order("Id"), batch_size):
            for row in page:
                try:
                    stores[int(row["Id"])] = str(row.get("Name") or f"Tienda {row['Id']}")
                except (KeyError, TypeError, ValueError):
                    continue
        return stores

    def upsert_rows(
        self, table: str, rows: Sequence[dict[str, Any]], on_conflict: str, batch_size: int
    ) -> None:
        for batch in chunks(list(rows), batch_size):
            self.client.table(table).upsert(list(batch), on_conflict=on_conflict).execute()

    def load_rows_by_values(
        self,
        table: str,
        value_column: str,
        values: Sequence[Any],
        columns: str,
        batch_size: int,
        equals: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for value_batch in chunks(list(values), batch_size):
            def factory(value_batch: Sequence[Any] = value_batch) -> Any:
                query = self.client.table(table).select(columns).in_(value_column, list(value_batch))
                for key, value in (equals or {}).items():
                    query = query.eq(key, value)
                return query.order(value_column)

            for page in self._paginate(factory, batch_size):
                output.extend(page)
        return output

    def load_match_catalog(
        self, categories: Sequence[str], batch_size: int
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        # Do not restrict this query to rollout categories. Exact MPN
        # uniqueness is a global catalog invariant; hiding an identifier
        # collision in another category would create a false match.
        del categories
        products = [
            row
            for page in self._paginate(
                lambda: self.client.table("products")
                .select("id,category,brand,name")
                .eq("status", "active")
                .order("id"),
                batch_size,
            )
            for row in page
        ]
        product_ids = [row["id"] for row in products]
        if not product_ids:
            return products, [], [], []
        identifiers = self.load_rows_by_values(
            "product_identifiers",
            "product_id",
            product_ids,
            "product_id,identifier_type,value,normalized_value",
            batch_size,
        )
        listings = self.load_rows_by_values(
            "merchant_listings",
            "product_id",
            product_ids,
            "store_id,product_id,source_listing_id,merchant_sku,url",
            batch_size,
        )
        refs = self.load_rows_by_values(
            "legacy_product_refs",
            "product_id",
            product_ids,
            "product_id,spec_table_name,spec_id",
            batch_size,
        )
        return products, identifiers, listings, refs


@dataclass
class CategoryReport:
    category: str
    spec_table: str
    legacy_products: int = 0
    canonical_products_planned: int = 0
    identifiers_planned: int = 0
    legacy_prices_seen: int = 0
    listings_planned: int = 0
    offers_planned: int = 0
    available_offers: int = 0
    unavailable_offers: int = 0
    stale_offers: int = 0
    suspect_offers: int = 0
    fallback_names: int = 0
    skipped_rows: int = 0
    skipped_reasons: dict[str, int] = field(default_factory=dict)
    comparison: dict[str, Any] | None = None
    resumed_after_id: str | None = None
    checkpoint_completed_before_run: bool = False

    def skip(self, reason: str) -> None:
        self.skipped_rows += 1
        self.skipped_reasons[reason] = self.skipped_reasons.get(reason, 0) + 1


@dataclass
class BackfillReport:
    mode: str
    generated_at: str
    category_flags: list[str]
    batch_size: int
    ttl_hours: int
    comparison_window_days: int
    stores_cached: int
    categories: list[CategoryReport] = field(default_factory=list)
    raw_offers: dict[str, Any] | None = None
    reads_switched: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "safety": {
                "default_is_dry_run": True,
                "fuzzy_auto_publish": False,
                "legacy_reads_changed": self.reads_switched,
                "images_authorized_by_backfill": False,
            },
        }


UPSERT_CONFLICTS = {
    "products": "id",
    "product_identifiers": "product_id,identifier_type,normalized_value",
    "legacy_product_refs": "spec_table_name,spec_id",
    "source_provenance": "id",
    "merchant_listings": "store_id,source_listing_id",
    "offers": "merchant_listing_id",
}


class CanonicalBackfill:
    def __init__(
        self,
        gateway: BackfillGateway,
        categories: Sequence[CategoryConfig],
        *,
        apply: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
        ttl_hours: int = DEFAULT_TTL_HOURS,
        compare: bool = False,
        comparison_days: int = DEFAULT_COMPARISON_DAYS,
        now: datetime | None = None,
        checkpoint: CheckpointFile | None = None,
    ):
        if batch_size < 1:
            raise ValueError("batch_size debe ser mayor que cero")
        if ttl_hours < 1:
            raise ValueError("ttl_hours debe ser mayor que cero")
        if comparison_days < 1:
            raise ValueError("comparison_days debe ser mayor que cero")
        if checkpoint is not None and not apply:
            raise ValueError("Los checkpoints solo se actualizan junto con --apply.")
        self.gateway = gateway
        self.categories = list(categories)
        self.apply = apply
        self.batch_size = batch_size
        self.ttl_hours = ttl_hours
        self.compare = compare
        self.comparison_days = comparison_days
        self.now = (now or utc_now()).astimezone(timezone.utc)
        self.checkpoint = checkpoint
        self.stores = gateway.load_stores(batch_size)

    def _write(self, table: str, rows: Sequence[dict[str, Any]]) -> None:
        if self.apply and rows:
            self.gateway.upsert_rows(table, rows, UPSERT_CONFLICTS[table], self.batch_size)

    @staticmethod
    def _deduplicate(rows: Sequence[dict[str, Any]], key: str) -> list[dict[str, Any]]:
        return list({str(row[key]): row for row in rows}.values())

    def _compare_batch(
        self,
        config: CategoryConfig,
        spec_ids: Sequence[str],
        listings: Sequence[dict[str, Any]],
        offers: Sequence[dict[str, Any]],
        accumulator: dict[str, Any],
    ) -> None:
        refs = self.gateway.load_rows_by_values(
            "legacy_product_refs",
            "spec_id",
            spec_ids,
            "spec_id,product_id,spec_table_name",
            self.batch_size,
            equals={"spec_table_name": config.spec_table},
        )
        expected_listing_ids = [row["id"] for row in listings]
        expected_offer_ids = [row["id"] for row in offers]
        actual_listings = self.gateway.load_rows_by_values(
            "merchant_listings",
            "id",
            expected_listing_ids,
            "id,product_id,match_status,match_method",
            self.batch_size,
        ) if expected_listing_ids else []
        actual_offers = self.gateway.load_rows_by_values(
            "offers",
            "id",
            expected_offer_ids,
            "id,published_price,public_state,captured_at,expires_at",
            self.batch_size,
        ) if expected_offer_ids else []

        ref_ids = {str(row.get("spec_id")) for row in refs}
        listing_ids = {str(row.get("id")) for row in actual_listings}
        actual_offer_map = {str(row.get("id")): row for row in actual_offers}
        accumulator["legacy_refs_expected"] += len(spec_ids)
        accumulator["legacy_refs_found"] += sum(str(spec_id) in ref_ids for spec_id in spec_ids)
        accumulator["listings_expected"] += len(expected_listing_ids)
        accumulator["listings_found"] += sum(str(value) in listing_ids for value in expected_listing_ids)
        accumulator["offers_expected"] += len(expected_offer_ids)
        accumulator["offers_found"] += sum(str(value) in actual_offer_map for value in expected_offer_ids)
        for expected in offers:
            actual = actual_offer_map.get(str(expected["id"]))
            if not actual:
                self._sample(accumulator, {"kind": "missing_offer", "id": expected["id"]})
                continue
            if actual.get("published_price") != expected.get("published_price"):
                accumulator["price_mismatches"] += 1
                self._sample(
                    accumulator,
                    {
                        "kind": "price",
                        "id": expected["id"],
                        "legacy": expected.get("published_price"),
                        "canonical": actual.get("published_price"),
                    },
                )
            if actual.get("public_state") != expected.get("public_state"):
                accumulator["state_mismatches"] += 1
                self._sample(
                    accumulator,
                    {
                        "kind": "state",
                        "id": expected["id"],
                        "legacy": expected.get("public_state"),
                        "canonical": actual.get("public_state"),
                    },
                )

    @staticmethod
    def _sample(accumulator: dict[str, Any], item: dict[str, Any]) -> None:
        if len(accumulator["mismatch_samples"]) < MAX_MISMATCH_SAMPLES:
            accumulator["mismatch_samples"].append(item)

    def run(self) -> BackfillReport:
        report = BackfillReport(
            mode="apply" if self.apply else "dry-run",
            generated_at=iso_utc(self.now),
            category_flags=[config.key for config in self.categories],
            batch_size=self.batch_size,
            ttl_hours=self.ttl_hours,
            comparison_window_days=self.comparison_days,
            stores_cached=len(self.stores),
        )
        for config in self.categories:
            category_report = CategoryReport(config.key, config.spec_table)
            start_after = self.checkpoint.after_id(config.spec_table) if self.checkpoint else None
            category_report.resumed_after_id = start_after
            if self.checkpoint and self.checkpoint.completed(config.spec_table):
                category_report.checkpoint_completed_before_run = True
                report.categories.append(category_report)
                continue
            comparison = {
                "window_started_at": iso_utc(self.now - timedelta(days=self.comparison_days)),
                "window_days": self.comparison_days,
                "legacy_refs_expected": 0,
                "legacy_refs_found": 0,
                "listings_expected": 0,
                "listings_found": 0,
                "offers_expected": 0,
                "offers_found": 0,
                "price_mismatches": 0,
                "state_mismatches": 0,
                "mismatch_samples": [],
            }
            last_processed_id = start_after
            for spec_batch in self.gateway.iter_specifications(
                config.spec_table, self.batch_size, start_after=start_after
            ):
                products: list[dict[str, Any]] = []
                identifiers: list[dict[str, Any]] = []
                refs: list[dict[str, Any]] = []
                provenance: list[dict[str, Any]] = []
                specs_by_id: dict[str, dict[str, Any]] = {}
                for spec in spec_batch:
                    spec_id = str(spec.get("Id") or "").strip()
                    category_report.legacy_products += 1
                    if not spec_id:
                        category_report.skip("missing_spec_id")
                        continue
                    try:
                        product, product_ids, legacy_ref, source_rows, fallback = build_product_rows(config, spec)
                    except (TypeError, ValueError):
                        category_report.skip("invalid_specification")
                        continue
                    specs_by_id[spec_id] = dict(spec)
                    products.append(product)
                    identifiers.extend(product_ids)
                    refs.append(legacy_ref)
                    provenance.extend(source_rows)
                    category_report.fallback_names += int(fallback)

                spec_ids = list(specs_by_id)
                pricing_rows = self.gateway.load_pricing_for_specs(config.spec_table, spec_ids, self.batch_size)
                listings_by_id: dict[str, dict[str, Any]] = {}
                offers_by_id: dict[str, dict[str, Any]] = {}
                for pricing in pricing_rows:
                    category_report.legacy_prices_seen += 1
                    spec = specs_by_id.get(str(pricing.get("SpecId") or ""))
                    if not spec:
                        category_report.skip("pricing_without_loaded_spec")
                        continue
                    listing, offer, error = build_pricing_rows(
                        config, spec, pricing, self.now, self.ttl_hours
                    )
                    if error or not listing or not offer:
                        category_report.skip(error or "invalid_pricing")
                        continue
                    if listing["store_id"] not in self.stores:
                        category_report.skip("unknown_store")
                        continue
                    listings_by_id[listing["id"]] = listing
                    offers_by_id[offer["id"]] = offer

                listings = list(listings_by_id.values())
                offers = list(offers_by_id.values())
                category_report.canonical_products_planned += len(products)
                category_report.identifiers_planned += len(identifiers)
                category_report.listings_planned += len(listings)
                category_report.offers_planned += len(offers)
                for offer in offers:
                    state_key = f"{offer['public_state']}_offers"
                    setattr(category_report, state_key, getattr(category_report, state_key) + 1)

                # Parent rows always precede children. Every ID is deterministic,
                # so a retry after a partial failure converges without duplicates.
                self._write("products", products)
                self._write("product_identifiers", identifiers)
                self._write("legacy_product_refs", refs)
                self._write("source_provenance", provenance)
                self._write("merchant_listings", listings)
                self._write("offers", offers)

                valid_batch_ids = [str(row.get("Id")) for row in spec_batch if row.get("Id")]
                if valid_batch_ids:
                    last_processed_id = valid_batch_ids[-1]
                    if self.checkpoint:
                        self.checkpoint.save(
                            config.spec_table,
                            last_processed_id,
                            completed=False,
                        )

                if self.compare:
                    self._compare_batch(config, spec_ids, listings, offers, comparison)

            if self.compare:
                category_report.comparison = comparison
            if self.checkpoint:
                self.checkpoint.save(config.spec_table, last_processed_id, completed=True)
            report.categories.append(category_report)
        return report


def iter_raw_offer_file(path: Path) -> Iterator[RawOffer]:
    """Stream a RawOffer NDJSON/NDJSON.GZ artifact and fail closed per line."""

    opener = gzip.open if path.name.lower().endswith(".gz") else open
    try:
        with opener(path, "rt", encoding="utf-8") as input_file:
            for line_number, line in enumerate(input_file, start=1):
                if not line.strip():
                    continue
                try:
                    yield RawOffer.model_validate_json(line)
                except Exception as error:
                    raise ValueError(
                        f"RawOffer invalido en {path}:{line_number}: {error}"
                    ) from error
    except OSError as error:
        raise ValueError(f"No se pudo leer {path}: {error}") from error


def skip_raw_offers(values: Iterable[RawOffer], count: int) -> Iterator[RawOffer]:
    iterator = iter(values)
    for _ in range(count):
        try:
            next(iterator)
        except StopIteration as error:
            raise ValueError("El cursor RawOffer excede el contenido del artefacto.") from error
    yield from iterator


def iterable_batches(values: Iterable[Any], size: int) -> Iterator[list[Any]]:
    batch: list[Any] = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


class RawOfferMigrator:
    """Publish validated raw offers into canonical staging/listing tables."""

    def __init__(
        self,
        gateway: BackfillGateway,
        categories: Sequence[CategoryConfig],
        *,
        apply: bool = False,
        dual_write: bool = False,
        compare: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
        ttl_hours: int = DEFAULT_TTL_HOURS,
        now: datetime | None = None,
    ):
        if batch_size < 1:
            raise ValueError("batch_size debe ser mayor que cero")
        if ttl_hours < 1:
            raise ValueError("ttl_hours debe ser mayor que cero")
        self.gateway = gateway
        self.categories = list(categories)
        self.apply = apply
        self.dual_write = dual_write
        self.compare = compare
        self.batch_size = batch_size
        self.ttl_hours = ttl_hours
        self.now = (now or utc_now()).astimezone(timezone.utc)
        self.stores = gateway.load_stores(batch_size)
        products, identifiers, listings, refs = gateway.load_match_catalog(
            [config.canonical_slug for config in categories], batch_size
        )
        self.index = RawOfferMatchIndex.from_rows(products, identifiers, listings, refs)

    def _write(self, table: str, rows: Sequence[dict[str, Any]]) -> None:
        if not self.apply or not rows:
            return
        conflict = UPSERT_CONFLICTS[table]
        self.gateway.upsert_rows(table, rows, conflict, self.batch_size)

    def _legacy_row(
        self,
        raw_offer: RawOffer,
        store_id: int,
        category: CategoryConfig,
        decision: MatchDecision,
    ) -> dict[str, Any] | None:
        if decision.status != "matched" or not decision.product_id:
            return None
        if str(raw_offer.availability) not in {"available", "unavailable"}:
            return None
        if raw_offer.fetchedAt.astimezone(timezone.utc) < self.now - timedelta(hours=self.ttl_hours):
            return None
        canonical_slug = self.index.product_categories.get(decision.product_id)
        canonical_category = next(
            (
                config
                for config in CATEGORY_CONFIGS
                if config.canonical_slug == canonical_slug
            ),
            None,
        )
        resolved_category = canonical_category or category
        refs = [
            row
            for row in self.index.legacy_refs.get(decision.product_id, [])
            if row.get("spec_table_name") == resolved_category.spec_table
        ]
        if len(refs) != 1:
            return None
        prices = [
            value
            for value in (raw_offer.cashPrice, raw_offer.cardPrice, raw_offer.normalPrice)
            if value is not None
        ]
        return {
            "SpecId": refs[0]["spec_id"],
            "SpecTableName": resolved_category.spec_table,
            "StoreId": store_id,
            "Price": min(prices),
            "StockStatus": str(raw_offer.availability) == "available",
            "Url": str(raw_offer.url),
            "LastUpdated": iso_utc(raw_offer.fetchedAt),
            "LastSeenAt": iso_utc(raw_offer.fetchedAt),
            "StockConfidence": 1.0,
        }

    def _compare_rows(
        self,
        listings: Sequence[dict[str, Any]],
        offers: Sequence[dict[str, Any]],
        report: dict[str, Any],
    ) -> None:
        expected_listing_ids = [row["id"] for row in listings]
        expected_offer_ids = [row["id"] for row in offers]
        actual_listings = self.gateway.load_rows_by_values(
            "merchant_listings",
            "id",
            expected_listing_ids,
            "id,product_id,match_status,match_method",
            self.batch_size,
        ) if expected_listing_ids else []
        actual_offers = self.gateway.load_rows_by_values(
            "offers",
            "id",
            expected_offer_ids,
            "id,cash_price,card_price,normal_price,published_price,public_state",
            self.batch_size,
        ) if expected_offer_ids else []
        actual_listing_map = {str(row.get("id")): row for row in actual_listings}
        actual_offer_map = {str(row.get("id")): row for row in actual_offers}
        report["comparison"]["listings_expected"] += len(listings)
        report["comparison"]["offers_expected"] += len(offers)
        for expected in listings:
            actual = actual_listing_map.get(str(expected["id"]))
            if not actual:
                report["comparison"]["missing_listings"] += 1
                continue
            if any(
                actual.get(field) != expected.get(field)
                for field in ("product_id", "match_status", "match_method")
            ):
                report["comparison"]["listing_mismatches"] += 1
        for expected in offers:
            actual = actual_offer_map.get(str(expected["id"]))
            if not actual:
                report["comparison"]["missing_offers"] += 1
                continue
            if any(
                actual.get(field) != expected.get(field)
                for field in (
                    "cash_price",
                    "card_price",
                    "normal_price",
                    "published_price",
                    "public_state",
                )
            ):
                report["comparison"]["offer_mismatches"] += 1

    def _blocked_replays(
        self,
        offers: Sequence[dict[str, Any]],
        report: dict[str, Any],
    ) -> set[str]:
        if not offers:
            return set()
        existing_rows = self.gateway.load_rows_by_values(
            "offers",
            "id",
            [row["id"] for row in offers],
            "id,merchant_listing_id,captured_at,payload_hash",
            self.batch_size,
        )
        incoming_by_id = {str(row["id"]): row for row in offers}
        blocked_listing_ids: set[str] = set()
        for existing in existing_rows:
            incoming = incoming_by_id.get(str(existing.get("id")))
            if not incoming:
                continue
            existing_capture = parse_datetime(existing.get("captured_at"))
            incoming_capture = parse_datetime(incoming.get("captured_at"))
            reason: str | None = None
            if not existing_capture or not incoming_capture:
                reason = "invalid_capture_for_replay_check"
            elif existing_capture > incoming_capture:
                reason = "older_than_existing"
            elif (
                existing_capture == incoming_capture
                and existing.get("payload_hash") != incoming.get("payload_hash")
            ):
                reason = "same_timestamp_payload_conflict"
            if reason:
                listing_id = str(incoming["merchant_listing_id"])
                blocked_listing_ids.add(listing_id)
                report["skipped"][reason] = report["skipped"].get(reason, 0) + 1
        return blocked_listing_ids

    def _filter_legacy_replays(
        self,
        legacy_rows: Sequence[dict[str, Any]],
        report: dict[str, Any],
    ) -> list[dict[str, Any]]:
        safe_rows: list[dict[str, Any]] = []
        by_table: dict[str, list[dict[str, Any]]] = {}
        for row in legacy_rows:
            by_table.setdefault(str(row["SpecTableName"]), []).append(row)
        for spec_table, planned in by_table.items():
            existing_rows = self.gateway.load_rows_by_values(
                "ProductPricing",
                "SpecId",
                [row["SpecId"] for row in planned],
                "SpecId,SpecTableName,StoreId,Price,StockStatus,Url,LastUpdated",
                self.batch_size,
                equals={"SpecTableName": spec_table},
            )
            existing_by_key = {
                (str(row.get("SpecId")), row.get("StoreId")): row
                for row in existing_rows
            }
            for incoming in planned:
                existing = existing_by_key.get(
                    (str(incoming["SpecId"]), incoming["StoreId"])
                )
                if not existing:
                    safe_rows.append(incoming)
                    continue
                existing_capture = parse_datetime(existing.get("LastUpdated"))
                incoming_capture = parse_datetime(incoming.get("LastUpdated"))
                if not existing_capture or not incoming_capture:
                    reason = "legacy_invalid_capture_for_replay_check"
                elif existing_capture > incoming_capture:
                    reason = "legacy_older_than_existing"
                elif existing_capture == incoming_capture and any(
                    existing.get(field) != incoming.get(field)
                    for field in ("Price", "StockStatus", "Url")
                ):
                    reason = "legacy_same_timestamp_payload_conflict"
                else:
                    safe_rows.append(incoming)
                    continue
                report["skipped"][reason] = report["skipped"].get(reason, 0) + 1
        return safe_rows

    def run(
        self,
        raw_offers: Iterable[RawOffer],
        *,
        checkpoint: CheckpointFile | None = None,
        artifact_hash: str | None = None,
        processed_start: int = 0,
    ) -> dict[str, Any]:
        if checkpoint and (not self.apply or not artifact_hash):
            raise ValueError("Checkpoint RawOffer requiere apply y hash del artefacto.")
        report: dict[str, Any] = {
            "mode": "apply" if self.apply else "dry-run",
            "dualWriteLegacy": self.dual_write,
            "categories": [config.key for config in self.categories],
            "rawOffersSeen": 0,
            "listingsPlanned": 0,
            "offersPlanned": 0,
            "legacyRowsPlanned": 0,
            "matched": {"exact_gtin": 0, "exact_mpn": 0, "persistent_sku": 0},
            "telemetry": {
                "exact_mpn": 0,
                "ambiguous_mpn": 0,
                "mpn_not_found": 0,
                "category_corrected_from_mpn": 0,
            },
            "candidates": 0,
            "unmatched": 0,
            "skipped": {},
            "candidateSamples": [],
            "comparison": {
                "enabled": self.compare,
                "listings_expected": 0,
                "offers_expected": 0,
                "missing_listings": 0,
                "missing_offers": 0,
                "listing_mismatches": 0,
                "offer_mismatches": 0,
            },
            "safety": {
                "fuzzyAutoPublish": False,
                "unmatchedPublicOffers": False,
                "zeroPricesAccepted": False,
            },
            "resumedAfterOfferCount": processed_start,
            "checkpointCompletedBeforeRun": False,
        }
        processed_count = processed_start
        for raw_batch in iterable_batches(raw_offers, self.batch_size):
            listings: list[dict[str, Any]] = []
            offers: list[dict[str, Any]] = []
            provenance: list[dict[str, Any]] = []
            legacy_rows_by_listing: list[tuple[str, dict[str, Any]]] = []
            possible_run_ids = sorted(
                {
                    run_id
                    for run_id in (parse_optional_uuid(raw_offer.runId) for raw_offer in raw_batch)
                    if run_id
                }
            )
            known_run_ids = {
                str(row["id"])
                for row in self.gateway.load_rows_by_values(
                    "scrape_runs",
                    "id",
                    possible_run_ids,
                    "id",
                    self.batch_size,
                )
                if row.get("id")
            } if possible_run_ids else set()
            for raw_offer in raw_batch:
                report["rawOffersSeen"] += 1
                category = resolve_raw_category(raw_offer.category, self.categories)
                if not category:
                    key = "category_not_enabled"
                    report["skipped"][key] = report["skipped"].get(key, 0) + 1
                    continue
                store_id = resolve_raw_store_id(raw_offer.storeId, self.stores)
                if store_id is None:
                    key = "unknown_or_ambiguous_store"
                    report["skipped"][key] = report["skipped"].get(key, 0) + 1
                    continue
                decision = match_raw_offer(raw_offer, store_id, category, self.index)
                listing, offer = build_raw_offer_rows(
                    raw_offer,
                    store_id,
                    category,
                    decision,
                    self.now,
                    self.ttl_hours,
                    known_run_ids,
                )
                listings.append(listing)
                offers.append(offer)
                provenance.append(
                    {
                        "id": stable_uuid(PROVENANCE_NAMESPACE, "raw-listing", listing["id"]),
                        "product_id": None,
                        "merchant_listing_id": listing["id"],
                        "asset_type": "listing",
                        "source_name": self.stores[store_id],
                        "source_url": str(raw_offer.url),
                        "permission_status": "unknown",
                        "permission_evidence": "Raw merchant listing; commercial/image permission pending review.",
                    }
                )
                if decision.status == "matched":
                    report["matched"][decision.method] += 1
                    if decision.method == "exact_mpn":
                        report["telemetry"]["exact_mpn"] += 1
                    if decision.reason == "category_corrected_from_mpn":
                        report["telemetry"]["category_corrected_from_mpn"] += 1
                elif decision.status == "candidate":
                    report["candidates"] += 1
                    if decision.reason == "ambiguous_mpn":
                        report["telemetry"]["ambiguous_mpn"] += 1
                    if len(report["candidateSamples"]) < MAX_MISMATCH_SAMPLES:
                        report["candidateSamples"].append(
                            {
                                "storeId": store_id,
                                "sourceListingId": raw_offer.sourceListingId,
                                "method": decision.method,
                                "candidateProductIds": list(decision.candidate_product_ids),
                                "reason": decision.reason,
                            }
                        )
                else:
                    report["unmatched"] += 1
                    if decision.reason == "mpn_not_found":
                        report["telemetry"]["mpn_not_found"] += 1
                if self.dual_write:
                    legacy_row = self._legacy_row(raw_offer, store_id, category, decision)
                    if legacy_row:
                        legacy_rows_by_listing.append((listing["id"], legacy_row))

            blocked_listing_ids = self._blocked_replays(offers, report)
            if blocked_listing_ids:
                listings = [row for row in listings if row["id"] not in blocked_listing_ids]
                offers = [
                    row
                    for row in offers
                    if row["merchant_listing_id"] not in blocked_listing_ids
                ]
                provenance = [
                    row
                    for row in provenance
                    if row["merchant_listing_id"] not in blocked_listing_ids
                ]
            legacy_rows = [
                row
                for listing_id, row in legacy_rows_by_listing
                if listing_id not in blocked_listing_ids
            ]
            if self.dual_write:
                legacy_rows = self._filter_legacy_replays(legacy_rows, report)

            report["listingsPlanned"] += len(listings)
            report["offersPlanned"] += len(offers)
            report["legacyRowsPlanned"] += len(legacy_rows)
            # Candidates and unmatched rows are retained, but canonical public views
            # exclude them because only match_status=matched is publishable.
            self._write("merchant_listings", listings)
            self._write("offers", offers)
            self._write("source_provenance", provenance)
            if self.dual_write and self.apply and legacy_rows:
                self.gateway.upsert_rows(
                    "ProductPricing",
                    legacy_rows,
                    "SpecId,SpecTableName,StoreId",
                    self.batch_size,
                )
            if self.compare:
                self._compare_rows(listings, offers, report)
            processed_count += len(raw_batch)
            if checkpoint and artifact_hash:
                checkpoint.save_raw(artifact_hash, processed_count, completed=False)
        if checkpoint and artifact_hash:
            checkpoint.save_raw(artifact_hash, processed_count, completed=True)
        return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill legacy -> catalogo canonico. Dry-run salvo que se entregue --apply."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Ejecuta upserts. Sin este flag no se escribe en Supabase.",
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        help=(
            "Snapshot JSON local de legacy+canonico para dry-run. "
            "Es obligatorio sin --apply y nunca abre Supabase."
        ),
    )
    parser.add_argument(
        "--categories",
        default=os.environ.get("CANONICAL_BACKFILL_CATEGORIES", ",".join(FIRST_WAVE_KEYS)),
        help="Feature flags separados por coma (default: CPU,GPU,Motherboard).",
    )
    parser.add_argument(
        "--all-essential",
        action="store_true",
        help="Habilita las ocho categorias esenciales.",
    )
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--ttl-hours", type=int, default=DEFAULT_TTL_HOURS)
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=Path("ScrapDB/RunLogs/canonical-backfill-checkpoint.json"),
        help="Cursor atomico local. Solo se crea/actualiza junto con --apply.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continua desde un checkpoint compatible (requiere --apply).",
    )
    parser.add_argument(
        "--raw-offers",
        type=Path,
        help="NDJSON o NDJSON.GZ validado con el contrato RawOffer.",
    )
    parser.add_argument(
        "--raw-offers-only",
        action="store_true",
        help="Omite el backfill de specifications.* y procesa solo --raw-offers.",
    )
    parser.add_argument(
        "--dual-write",
        action="store_true",
        help="Con RawOffer, mantiene tambien ProductPricing para rollback legacy.",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compara por lotes el resultado legacy esperado con las tablas canonicas actuales.",
    )
    parser.add_argument("--comparison-days", type=int, default=DEFAULT_COMPARISON_DAYS)
    parser.add_argument(
        "--comparison-report",
        type=Path,
        help="Guarda el reporte JSON. Requiere --compare; las lecturas web no se modifican.",
    )
    return parser


def create_gateway(apply: bool, *, require_service_role: bool = False) -> SupabaseGateway:
    if not apply:
        raise RuntimeError(
            "Supabase solo se habilita con --apply; use --snapshot para un dry-run offline."
        )
    # Existing scraper deployments use ScrapDB/.env. Root .env remains a
    # fallback for local execution.
    load_dotenv(BASE_DIR / ".env")
    load_dotenv(BASE_DIR.parent / ".env")
    url = os.environ.get("SUPABASE_URL")
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    key = service_key or os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY/SUPABASE_KEY son obligatorios.")
    if (apply or require_service_role) and not service_key:
        raise RuntimeError(
            "Esta operacion exige SUPABASE_SERVICE_ROLE_KEY; los identificadores privados "
            "y las escrituras nunca usan una clave publica."
        )
    from supabase import create_client

    return SupabaseGateway(create_client(url, key))


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.comparison_report and not args.compare:
        parser.error("--comparison-report requiere --compare")
    if args.apply and args.snapshot:
        parser.error("--snapshot es solo para dry-run; no se combina con --apply")
    if not args.apply and not args.snapshot:
        parser.error("el dry-run requiere --snapshot y nunca consulta Supabase")
    if args.resume and not args.apply:
        parser.error("--resume requiere --apply")
    if args.raw_offers_only and not args.raw_offers:
        parser.error("--raw-offers-only requiere --raw-offers")
    if args.dual_write and not args.raw_offers:
        parser.error("--dual-write requiere --raw-offers")
    if args.batch_size < 1 or args.ttl_hours < 1 or args.comparison_days < 1:
        parser.error("batch-size, ttl-hours y comparison-days deben ser mayores que cero")
    try:
        categories = resolve_categories(args.categories, args.all_essential)
        gateway: BackfillGateway
        if args.apply:
            gateway = create_gateway(True, require_service_role=True)
        else:
            gateway = SnapshotGateway.from_path(args.snapshot)
        checkpoint = None
        if args.apply:
            checkpoint = CheckpointFile(
                args.checkpoint,
                checkpoint_fingerprint(
                    categories,
                    args.ttl_hours,
                    dual_write=args.dual_write,
                ),
                resume=args.resume,
            )
        if args.raw_offers_only:
            report_object = BackfillReport(
                mode="apply" if args.apply else "dry-run",
                generated_at=iso_utc(utc_now()),
                category_flags=[config.key for config in categories],
                batch_size=args.batch_size,
                ttl_hours=args.ttl_hours,
                comparison_window_days=args.comparison_days,
                stores_cached=len(gateway.load_stores(args.batch_size)),
            )
        else:
            runner = CanonicalBackfill(
                gateway,
                categories,
                apply=args.apply,
                batch_size=args.batch_size,
                ttl_hours=args.ttl_hours,
                compare=args.compare,
                comparison_days=args.comparison_days,
                checkpoint=checkpoint,
            )
            report_object = runner.run()
        if args.raw_offers:
            raw_runner = RawOfferMigrator(
                gateway,
                categories,
                apply=args.apply,
                dual_write=args.dual_write,
                compare=args.compare,
                batch_size=args.batch_size,
                ttl_hours=args.ttl_hours,
            )
            artifact_hash = file_sha256(args.raw_offers)
            raw_after = checkpoint.raw_after_count(artifact_hash) if checkpoint else 0
            if checkpoint and checkpoint.raw_completed(artifact_hash):
                raw_report = raw_runner.run([], processed_start=raw_after)
                raw_report["checkpointCompletedBeforeRun"] = True
            else:
                raw_report = raw_runner.run(
                    skip_raw_offers(iter_raw_offer_file(args.raw_offers), raw_after),
                    checkpoint=checkpoint,
                    artifact_hash=artifact_hash if checkpoint else None,
                    processed_start=raw_after,
                )
            report_object.raw_offers = raw_report
        report = report_object.to_dict()
    except (OSError, RuntimeError, ValueError) as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 2

    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if args.comparison_report:
        args.comparison_report.parent.mkdir(parents=True, exist_ok=True)
        args.comparison_report.write_text(rendered + "\n", encoding="utf-8")
        print(f"[OK] Reporte guardado en {args.comparison_report}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import os
import json
import hashlib
import re
import requests
import time
import uuid as uuid_lib
from io import BytesIO
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from PIL import Image

# ================= CONFIGURACIÓN =================
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
supabase = None
DB_REQUEST_COUNT = 0
MANUAL_OVERRIDE_CACHE = {}
SPEC_MATCH_CACHE = {}
CANONICAL_MPN_CACHE = {}
CANONICAL_MPN_REASON_CACHE = {}
PREVIOUS_PRICE_CACHE = {}

# Schemas
SPECIFICATIONS_SCHEMA = "specifications"

SCRAP_OUTPUT_DIR = BASE_DIR / "Outputs"
LOG_FILE = BASE_DIR / "unmatched_log.txt"
MAX_DB_INTEGER = 2_147_483_647
MAX_REASONABLE_CLP_PRICE = 100_000_000
MIN_REASONABLE_PUBLISH_PRICE = int(os.environ.get("MIN_REASONABLE_PUBLISH_PRICE", "1000"))
PRICE_ANOMALY_MAX_MULTIPLIER = float(os.environ.get("PRICE_ANOMALY_MAX_MULTIPLIER", "4"))
PRICE_ANOMALY_MIN_MULTIPLIER = float(os.environ.get("PRICE_ANOMALY_MIN_MULTIPLIER", "0.25"))
MIN_STOCK_MARKOUT_MATCH_RATE = float(os.environ.get("MIN_STOCK_MARKOUT_MATCH_RATE", "0.80"))
OFFER_TTL_HOURS = int(os.environ.get("OFFER_TTL_HOURS", "48"))
SCRAPER_SUMMARY_PATH = os.environ.get("SCRAPER_SUMMARY_PATH", "").strip()
SCRAPE_RUN_ID = os.environ.get("SCRAPE_RUN_ID", "").strip() or str(uuid_lib.uuid4())
DB_RETRY_ATTEMPTS = 4
DB_RETRY_BASE_DELAY_SECONDS = 2
DB_CLIENT_REFRESH_EVERY = int(os.environ.get("DB_CLIENT_REFRESH_EVERY", "5000"))
OPTIONAL_DB_TABLES_DISABLED = set()
PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED = True
STORE_QUALITY_COLUMNS_ENABLED = True
TRANSIENT_DB_ERROR_MARKERS = (
    "server disconnected",
    "remote protocol error",
    "remoteprotocolerror",
    "connectionterminated",
    "connection terminated",
    "connection reset",
    "connection aborted",
    "connection refused",
    "network is unreachable",
    "temporarily unavailable",
    "timeout",
    "timed out",
    "read timeout",
    "write timeout",
    "connect timeout",
    "pool timeout",
    "max retries exceeded",
)
OPTIONAL_SCHEMA_ERROR_MARKERS = (
    "does not exist",
    "could not find the",
    "schema cache",
    "pgrst",
    "undefined column",
    "undefined table",
)

# Mapeo de categorías a tablas
CATEGORY_TO_TABLE = {
    "CPUCooler_Air": "CpuCoolerSpecifications",
    "CPUCooler_Liquid": "CpuCoolerSpecifications",
    "NetworkAdapter": ["WiredNetworkAdapterSpecifications", "WirelessNetworkAdapterSpecifications"],
    "Case": "CaseSpecifications",
    "CaseFan": "CaseFanSpecifications",
    "CPU": "CPUSpecifications",
    "CPUCooler": "CpuCoolerSpecifications",
    "ExternalStorage": "ExternalStorageSpecifications",
    "FanController": "FanControllerSpecifications",
    "Headphones": "HeadphoneSpecifications",
    "Keyboard": "KeyboardSpecifications",
    "Memory": "RamSpecifications",
    "Monitor": "MonitorSpecifications",
    "Motherboard": "MotherboardSpecifications",
    "Mouse": "MouseSpecifications",
    "OperatingSystem": "OperatingSystemSpecifications",
    "OpticalDrive": "OpticalDriveSpecifications",
    "PowerSupply": "PowerSupplySpecifications",
    "SoundCard": "SoundCardSpecifications",
    "Speakers": "SpeakersSpecifications",
    "Storage": "InternalStorageSpecifications",
    "ThermalCompound": "ThermalPasteSpecifications",
    "UPS": "UpsSpecifications",
    "VideoCard": "GpuSpecifications",
    "Webcam": "WebcamSpecifications",
    "WiredNetworkAdapter": "WiredNetworkAdapterSpecifications",
    "WirelessNetworkAdapter": "WirelessNetworkAdapterSpecifications",
    "CPU_CPUCooler_ThermalCompound": ["CPUSpecifications", "CpuCoolerSpecifications", "ThermalPasteSpecifications"],
    "CPUCooler_CaseFan": ["CpuCoolerSpecifications", "CaseFanSpecifications"],
    "Mouse_Keyboard": ["MouseSpecifications", "KeyboardSpecifications"],
    "Storage_ExternalStorage": ["InternalStorageSpecifications", "ExternalStorageSpecifications"],
    "CPUCooler_ThermalCompound": ["CpuCoolerSpecifications", "ThermalPasteSpecifications"]
}

ESSENTIAL_SPEC_TABLES = {
    "CPUSpecifications",
    "CpuCoolerSpecifications",
    "CaseSpecifications",
    "GpuSpecifications",
    "InternalStorageSpecifications",
    "MotherboardSpecifications",
    "PowerSupplySpecifications",
    "RamSpecifications",
}
INVALID_MPN_VALUES = {
    "NA",
    "NONE",
    "NULL",
    "UNKNOWN",
    "NOTAVAILABLE",
    "NOAPLICA",
    "SINMPN",
    "SINSKU",
}


# ================= FUNCIONES =================

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def get_supabase():
    global supabase
    if supabase is not None:
        return supabase

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL y SUPABASE_KEY son obligatorios para procesar scraps.")

    supabase = create_client(url, key)
    return supabase

def reset_supabase_client(reason=""):
    global supabase
    if supabase is None:
        return
    supabase = None
    if reason:
        print(f"   [DB] Reiniciando cliente Supabase: {reason}")

def maybe_refresh_supabase_client():
    if DB_CLIENT_REFRESH_EVERY <= 0:
        return
    if DB_REQUEST_COUNT > 0 and DB_REQUEST_COUNT % DB_CLIENT_REFRESH_EVERY == 0:
        reset_supabase_client(f"{DB_REQUEST_COUNT} requests acumuladas")

def normalize_key(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())

def normalize_part_number(value):
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").strip().upper())

def valid_part_number(value):
    raw = str(value or "").strip()
    normalized = normalize_part_number(raw)
    words = re.findall(r"[A-Za-z0-9]+", raw)
    return bool(
        raw
        and len(raw) <= 128
        and not (len(raw) >= 48 and len(words) >= 6)
        and normalized
        and normalized not in INVALID_MPN_VALUES
    )

def normalized_part_number_candidates(raw_val):
    return [
        normalize_part_number(candidate)
        for candidate in parse_part_numbers(raw_val)
        if valid_part_number(candidate)
    ]

def exact_part_number_variants(candidate):
    raw = str(candidate or "").strip()
    normalized = normalize_part_number(raw)
    variants = [
        raw,
        raw.upper(),
        normalized,
        f"['{raw}']",
        f'["{raw}"]',
        f"['{raw.upper()}']",
        f'["{raw.upper()}"]',
        f"['{normalized}']",
        f'["{normalized}"]',
    ]
    return [variant for variant in dict.fromkeys(variants) if variant]

def first_text(item, *keys):
    for key in keys:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def explicit_stock_status(item):
    """Return False only for explicit OOS evidence; preserve legacy unknown as available."""
    aliases = {
        "availability",
        "stockstatus",
        "isavailable",
        "isinstock",
        "instock",
        "available",
        "stock",
    }
    present = False
    value = None
    for key, candidate in item.items():
        if normalize_key(key) in aliases:
            present = True
            value = candidate
            break

    if not present or value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value > 0

    normalized = normalize_key(value)
    if normalized in {
        "unavailable",
        "outofstock",
        "agotado",
        "sinexistencias",
        "sinstock",
        "nodisponible",
        "false",
        "no",
        "0",
    }:
        return False
    if normalized in {
        "available",
        "instock",
        "enstock",
        "disponible",
        "hayexistencias",
        "true",
        "yes",
        "si",
        "1",
    }:
        return True
    return None


def preferred_product_snapshot(existing, candidate):
    """Prefer an available duplicate, then the lowest price within the same stock state."""
    if existing is None:
        return candidate
    if candidate["stock_status"] != existing["stock_status"]:
        return candidate if candidate["stock_status"] else existing
    return candidate if candidate["price_int"] < existing["price_int"] else existing

def chunked(items, size):
    for index in range(0, len(items), size):
        yield items[index:index + size]

def parse_part_numbers(raw_val):
    if not raw_val: return []
    if isinstance(raw_val, list):
        return [str(v).strip() for v in raw_val if v]
    s = str(raw_val).strip()
    if s.startswith("[") and s.endswith("]"):
        content = s[1:-1]
        parts = []
        for p in content.split(','):
            clean_p = p.strip().strip("'").strip('"')
            if clean_p:
                parts.append(clean_p)
        return parts
    return [s]

def _valid_price_candidate(digits):
    if not digits:
        return None
    digits = re.sub(r"\D", "", str(digits))
    if not digits:
        return None
    try:
        price = int(digits)
    except ValueError:
        return None
    if price <= 0 or price > MAX_DB_INTEGER or price > MAX_REASONABLE_CLP_PRICE:
        return None
    return price

def _prices_from_long_digit_run(digits):
    candidates = []
    if len(set(digits)) == 1:
        return candidates

    # Some scraper price blocks can collapse into strings like
    # 165000165000147990147990. Recover adjacent repeated prices first.
    for size in range(4, 9):
        pattern = re.compile(rf"(\d{{{size}}})\1")
        for match in pattern.finditer(digits):
            candidate = _valid_price_candidate(match.group(1))
            if candidate is not None:
                candidates.append(candidate)

    return candidates

def parse_price_to_int(raw_price):
    if raw_price is None:
        return None
    if isinstance(raw_price, (int, float)):
        return _valid_price_candidate(str(int(raw_price)))

    text = str(raw_price).strip()
    if not text:
        return None

    candidates = []
    for currency_match in re.finditer(r"\$\s*([0-9][0-9.\s,]*)", text):
        candidate = _valid_price_candidate(currency_match.group(1))
        if candidate is not None:
            candidates.append(candidate)

    for token in re.findall(r"\d[\d.,]*", text):
        digits = re.sub(r"\D", "", token)
        candidate = _valid_price_candidate(digits)
        if candidate is not None:
            candidates.append(candidate)
        elif len(digits) > 10:
            candidates.extend(_prices_from_long_digit_run(digits))

    return min(candidates) if candidates else None

def error_chain_text(error):
    texts = []
    queue = [error]
    seen = set()
    while queue:
        current = queue.pop(0)
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        texts.append(f"{current.__class__.__module__}.{current.__class__.__name__}")
        texts.append(str(current))
        queue.append(getattr(current, "__cause__", None))
        queue.append(getattr(current, "__context__", None))
    return " ".join(texts).lower()

def is_transient_db_error(error):
    message = error_chain_text(error)
    compact_message = re.sub(r"[^a-z0-9]+", "", message)
    for marker in TRANSIENT_DB_ERROR_MARKERS:
        marker_text = marker.lower()
        marker_compact = re.sub(r"[^a-z0-9]+", "", marker_text)
        if marker_text in message or marker_compact in compact_message:
            return True
    return False

def is_optional_schema_error(error):
    message = str(error).lower()
    return any(marker in message for marker in OPTIONAL_SCHEMA_ERROR_MARKERS)

def execute_db_request(label, request_factory, attempts=DB_RETRY_ATTEMPTS):
    global DB_REQUEST_COUNT
    for attempt in range(1, attempts + 1):
        try:
            maybe_refresh_supabase_client()
            DB_REQUEST_COUNT += 1
            return request_factory().execute()
        except Exception as error:
            if attempt >= attempts or not is_transient_db_error(error):
                raise

            delay = DB_RETRY_BASE_DELAY_SECONDS * attempt
            reset_supabase_client(f"error transitorio en {label}")
            print(f"   [WARN] DB retry {attempt}/{attempts} en {label}: {error}. Reintentando en {delay}s...")
            time.sleep(delay)

def optional_db_request(table_name, label, request_factory):
    if table_name in OPTIONAL_DB_TABLES_DISABLED:
        return None
    try:
        return execute_db_request(label, request_factory)
    except Exception as error:
        if is_optional_schema_error(error):
            OPTIONAL_DB_TABLES_DISABLED.add(table_name)
            print(f"   [WARN] Tabla opcional '{table_name}' no disponible; se omite desde ahora.")
            return None
        raise

def optional_insert_rows(table_name, rows, chunk_size=250):
    if not rows:
        return True
    for batch in chunked(rows, chunk_size):
        result = optional_db_request(
            table_name,
            f"{table_name} insert {len(batch)} rows",
            lambda batch=batch: get_supabase().table(table_name).insert(batch),
        )
        if result is None:
            return False
    return True

def record_scrape_issues(raw_rows, chunk_size=250):
    issues = []
    for row in raw_rows:
        if row.get("match_status") not in {"unmatched", "price_anomaly", "invalid"}:
            continue
        identity = "|".join((
            str(row.get("store_name") or "").strip().casefold(),
            str(row.get("scraped_category") or "").strip().casefold(),
            str(row.get("source_url") or "").strip().casefold(),
            str(row.get("normalized_part_number") or "").strip().casefold(),
        ))
        issues.append({
            "fingerprint": hashlib.sha256(identity.encode("utf-8")).hexdigest(),
            "store_id": row.get("store_id"),
            "store_name": row.get("store_name"),
            "scraped_category": row.get("scraped_category"),
            "source_url": row.get("source_url"),
            "scraped_name": row.get("scraped_name"),
            "scraped_part_number": row.get("scraped_part_number"),
            "normalized_part_number": row.get("normalized_part_number"),
            "issue_type": row.get("match_status"),
            "anomaly_reason": row.get("anomaly_reason"),
            "scrape_run_id": SCRAPE_RUN_ID,
        })
    for batch in chunked(issues, chunk_size):
        result = optional_db_request(
            "scrape_issue_queue",
            f"record_scrape_issues {len(batch)} rows",
            lambda batch=batch: get_supabase().rpc("record_scrape_issues", {"p_issues": batch}),
        )
        if result is None:
            return False
    return True

def record_legacy_offer_change(spec_id, spec_table_name, store_id, price, stock_status):
    feature_key = "record_legacy_offer_change"
    if feature_key not in OPTIONAL_DB_TABLES_DISABLED:
        try:
            return execute_db_request(
                f"PriceHistory change {store_id} {spec_id}",
                lambda: get_supabase().rpc("record_legacy_offer_change", {
                    "p_spec_id": spec_id,
                    "p_spec_table_name": spec_table_name,
                    "p_store_id": store_id,
                    "p_price": price,
                    "p_stock_status": stock_status,
                    "p_recorded_at": now_iso(),
                }),
            )
        except Exception as error:
            if not is_optional_schema_error(error):
                raise
            OPTIONAL_DB_TABLES_DISABLED.add(feature_key)
            print("   [WARN] RPC record_legacy_offer_change no disponible; usando historial legacy.")
    if price is None or int(price) <= 0:
        return None
    return execute_db_request(
        f"PriceHistory legacy insert {store_id} {spec_id}",
        lambda: get_supabase().table("PriceHistory").insert({
            "SpecId": spec_id,
            "SpecTableName": spec_table_name,
            "StoreId": store_id,
            "Price": price,
            "RecordedAt": now_iso(),
        }),
    )

def get_or_create_store(store_name):
    res = execute_db_request(
        f"Stores select {store_name}",
        lambda: get_supabase().table("Stores").select("Id").eq("Name", store_name),
    )
    if res.data:
        return res.data[0]['Id']
    else:
        res = execute_db_request(
            f"Stores insert {store_name}",
            lambda: get_supabase().table("Stores").insert({"Name": store_name}),
        )
        return res.data[0]['Id']

def load_scraper_summary(path_value):
    if not path_value:
        return {}
    path = Path(path_value)
    if not path.exists():
        print(f"   [WARN] SCRAPER_SUMMARY_PATH no existe: {path}")
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as error:
        print(f"   [WARN] No se pudo leer resumen de scrapers: {error}")
        return {}

def build_scraper_result_map(summary):
    result_map = {}
    for result in summary.get("scraper_results", []) or []:
        name = str(result.get("name") or "")
        stem = Path(name).stem
        inferred = re.sub(r"^scrap_", "", stem, flags=re.IGNORECASE)
        for key in (name, stem, inferred):
            normalized = normalize_key(key)
            if normalized:
                result_map[normalized] = result
    return result_map

def scraper_result_for_store(store_name, scraper_result_map):
    key = normalize_key(store_name)
    if key in scraper_result_map:
        return scraper_result_map[key]
    for result_key, result in scraper_result_map.items():
        if key and (key in result_key or result_key in key):
            return result
    return None

def create_scrape_run(summary):
    metadata = dict(summary or {})
    metadata["quality_policy"] = {
        "offer_ttl_hours": OFFER_TTL_HOURS,
        "minimum_stock_markout_match_rate": MIN_STOCK_MARKOUT_MATCH_RATE,
        "missing_snapshots_before_markout": 2,
    }
    optional_db_request(
        "scrape_runs",
        f"scrape_runs upsert {SCRAPE_RUN_ID}",
        lambda: get_supabase().table("scrape_runs").upsert({
            "id": SCRAPE_RUN_ID,
            "source": os.environ.get("SCRAPE_SOURCE") or summary.get("source") or "scraper",
            "status": "running",
            "started_at": summary.get("run_started_at_utc") or now_iso(),
            "scraper_count": int(summary.get("scraper_count") or 0),
            "summary_path": SCRAPER_SUMMARY_PATH or None,
            "metadata": metadata,
            "updated_at": now_iso(),
        }, on_conflict="id"),
    )

def finalize_scrape_run(totals, status):
    optional_db_request(
        "scrape_runs",
        f"scrape_runs finalize {SCRAPE_RUN_ID}",
        lambda: get_supabase().table("scrape_runs").update({
            "status": status,
            "finished_at": now_iso(),
            "duration_seconds": totals.get("duration_seconds"),
            "store_count": totals["store_count"],
            "raw_count": totals["raw_count"],
            "matched_count": totals["matched_count"],
            "unmatched_count": totals["unmatched_count"],
            "anomaly_count": totals["anomaly_count"],
            "error_count": totals["error_count"],
            "match_rate": totals["match_rate"],
            "updated_at": now_iso(),
        }).eq("id", SCRAPE_RUN_ID),
    )

def create_store_run(store_id, store_name, scraper_result, metrics):
    metadata = dict(scraper_result or {})
    metadata["matching_telemetry"] = {
        "exact_mpn": metrics.get("exact_mpn_count", 0),
        "ambiguous_mpn": metrics.get("ambiguous_mpn_count", 0),
        "mpn_not_found": metrics.get("mpn_not_found_count", 0),
        "category_corrected_from_mpn": metrics.get(
            "category_corrected_from_mpn_count", 0
        ),
    }
    result = optional_db_request(
        "scraper_store_runs",
        f"scraper_store_runs insert {store_name}",
        lambda: get_supabase().table("scraper_store_runs").insert({
            "scrape_run_id": SCRAPE_RUN_ID,
            "store_id": store_id,
            "store_name": store_name,
            "scraper_name": (scraper_result or {}).get("name"),
            "status": metrics["status"],
            "started_at": (scraper_result or {}).get("started_at_utc"),
            "finished_at": (scraper_result or {}).get("finished_at_utc"),
            "duration_seconds": (scraper_result or {}).get("duration_seconds"),
            "raw_count": metrics["raw_count"],
            "matched_count": metrics["matched_count"],
            "unmatched_count": metrics["unmatched_count"],
            "anomaly_count": metrics["anomaly_count"],
            "error_count": metrics["error_count"],
            "output_empty": metrics["output_empty"],
            "match_rate": metrics["match_rate"],
            "stock_markout_allowed": metrics["stock_markout_allowed"],
            "stock_markout_reason": metrics["stock_markout_reason"],
            "log_file": (scraper_result or {}).get("log_file"),
            "metadata": metadata,
        }),
    )
    if result and getattr(result, "data", None):
        return result.data[0].get("id")
    return None

def scraper_result_is_partial(scraper_result, raw_count):
    if not scraper_result:
        return True
    if scraper_result.get("partial") is True or scraper_result.get("output_complete") is False:
        return True
    if scraper_result.get("timed_out"):
        return True
    if scraper_result.get("failure_reason"):
        return True

    expected_json_count = scraper_result.get("json_count")
    if isinstance(expected_json_count, int) and expected_json_count > raw_count:
        return True
    return False


def should_allow_stock_markout(
    raw_count,
    matched_count,
    scraper_result=None,
    *,
    anomaly_count=0,
    error_count=0,
    input_error_count=0,
):
    if not scraper_result:
        return False, "missing_scraper_telemetry"
    if not scraper_result.get("success", False):
        return False, "scraper_failed"
    if raw_count <= 0:
        return False, "empty_output"
    if matched_count <= 0:
        return False, "zero_matches"
    if scraper_result_is_partial(scraper_result, raw_count):
        return False, "partial_output"
    if anomaly_count > 0:
        return False, "price_anomalies"
    if error_count > 0 or input_error_count > 0:
        return False, "processing_errors"

    match_rate = matched_count / raw_count
    if match_rate < MIN_STOCK_MARKOUT_MATCH_RATE:
        return False, "low_match_rate"
    return True, "healthy_scrape"

def find_manual_override(tables, store_name, raw_type, part_number):
    if isinstance(tables, str):
        target_tables = {tables}
    else:
        target_tables = set(tables)
    target_table_key = tuple(sorted(target_tables))

    for candidate in parse_part_numbers(part_number):
        normalized = normalize_part_number(candidate)
        if not normalized:
            continue
        cache_key = (target_table_key, store_name, raw_type, normalized)
        if cache_key in MANUAL_OVERRIDE_CACHE:
            cached = MANUAL_OVERRIDE_CACHE[cache_key]
            if cached[0] and cached[1]:
                return cached
            continue
        result = optional_db_request(
            "match_overrides",
            f"match_overrides lookup {store_name} {candidate}",
            lambda normalized=normalized: get_supabase().table("match_overrides")
                .select("spec_id,spec_table_name,confidence")
                .eq("scraped_store_name", store_name)
                .eq("normalized_part_number", normalized)
                .eq("status", "approved")
                .limit(1),
        )
        if not result or not result.data:
            MANUAL_OVERRIDE_CACHE[cache_key] = (None, None, None, None)
            continue
        row = result.data[0]
        spec_table_name = row.get("spec_table_name")
        if row.get("spec_id") and spec_table_name:
            match = (row.get("spec_id"), spec_table_name, "manual_override", row.get("confidence") or 1)
            MANUAL_OVERRIDE_CACHE[cache_key] = match
            return match
        MANUAL_OVERRIDE_CACHE[cache_key] = (None, None, None, None)
    return None, None, None, None

def prefetch_canonical_mpn_matches(part_numbers):
    """Load exact MPN matches in batches from the canonical identity tables."""
    requested = {
        normalized
        for part_number in part_numbers
        for normalized in normalized_part_number_candidates(part_number)
        if normalized not in CANONICAL_MPN_CACHE
    }
    if not requested:
        return

    identifier_rows = []
    for batch in chunked(sorted(requested), 200):
        response = execute_db_request(
            f"product_identifiers MPN prefetch {len(batch)}",
            lambda batch=batch: get_supabase().table("product_identifiers")
                .select("product_id,normalized_value")
                .eq("identifier_type", "mpn")
                .in_("normalized_value", batch),
        )
        identifier_rows.extend(response.data or [])

    product_ids_by_mpn = {normalized: set() for normalized in requested}
    for row in identifier_rows:
        normalized = normalize_part_number(row.get("normalized_value"))
        product_id = str(row.get("product_id") or "")
        if normalized in product_ids_by_mpn and product_id:
            product_ids_by_mpn[normalized].add(product_id)

    candidate_product_ids = sorted(
        {
            product_id
            for product_ids in product_ids_by_mpn.values()
            for product_id in product_ids
        }
    )
    active_product_ids = set()
    for batch in chunked(candidate_product_ids, 200):
        response = execute_db_request(
            f"products active MPN prefetch {len(batch)}",
            lambda batch=batch: get_supabase().table("products")
                .select("id")
                .in_("id", batch)
                .eq("status", "active"),
        )
        active_product_ids.update(
            str(row.get("id"))
            for row in (response.data or [])
            if row.get("id")
        )

    refs_by_product = {}
    for batch in chunked(sorted(active_product_ids), 200):
        response = execute_db_request(
            f"legacy_product_refs MPN prefetch {len(batch)}",
            lambda batch=batch: get_supabase().table("legacy_product_refs")
                .select("product_id,spec_table_name,spec_id")
                .in_("product_id", batch),
        )
        for row in response.data or []:
            product_id = str(row.get("product_id") or "")
            spec_table = row.get("spec_table_name")
            spec_id = row.get("spec_id")
            if product_id and spec_table and spec_id:
                refs_by_product.setdefault(product_id, set()).add(
                    (str(spec_id), str(spec_table))
                )

    for normalized in requested:
        product_ids = product_ids_by_mpn[normalized] & active_product_ids
        if not product_ids:
            CANONICAL_MPN_CACHE[normalized] = (None, None, None, None)
            CANONICAL_MPN_REASON_CACHE[normalized] = "mpn_not_found"
            continue
        if len(product_ids) != 1:
            CANONICAL_MPN_CACHE[normalized] = (None, None, None, None)
            CANONICAL_MPN_REASON_CACHE[normalized] = "ambiguous_mpn"
            continue
        product_id = next(iter(product_ids))
        refs = refs_by_product.get(product_id, set())
        if len(refs) != 1:
            CANONICAL_MPN_CACHE[normalized] = (None, None, None, None)
            CANONICAL_MPN_REASON_CACHE[normalized] = "canonical_legacy_ref_not_unique"
            continue
        spec_id, spec_table = next(iter(refs))
        CANONICAL_MPN_CACHE[normalized] = (
            spec_id,
            spec_table,
            "exact_mpn",
            1,
        )
        CANONICAL_MPN_REASON_CACHE[normalized] = "exact_mpn"

def lookup_canonical_mpn(candidate):
    normalized = normalize_part_number(candidate)
    if not valid_part_number(candidate):
        return None, None, None, None
    if normalized not in CANONICAL_MPN_CACHE:
        prefetch_canonical_mpn_matches([candidate])
    return CANONICAL_MPN_CACHE.get(normalized, (None, None, None, None))

def canonical_mpn_reason(part_number):
    reasons = [
        CANONICAL_MPN_REASON_CACHE.get(normalized)
        for normalized in normalized_part_number_candidates(part_number)
    ]
    if "ambiguous_mpn" in reasons:
        return "ambiguous_mpn"
    if "canonical_legacy_ref_not_unique" in reasons:
        return "canonical_legacy_ref_not_unique"
    if reasons and all(reason == "mpn_not_found" for reason in reasons):
        return "mpn_not_found"
    return next((reason for reason in reasons if reason), "mpn_not_found")

def lookup_exact_part_number(table_name, candidate):
    normalized_candidate = normalize_part_number(candidate)
    if not normalized_candidate:
        return None, None, None, None
    cache_key = (table_name, normalized_candidate)
    if cache_key in SPEC_MATCH_CACHE:
        return SPEC_MATCH_CACHE[cache_key]

    variants = exact_part_number_variants(candidate)
    if not variants:
        SPEC_MATCH_CACHE[cache_key] = (None, None, None, None)
        return SPEC_MATCH_CACHE[cache_key]

    res = execute_db_request(
        f"{table_name} exact lookup {normalized_candidate}",
        lambda table_name=table_name, variants=variants: get_supabase().schema(SPECIFICATIONS_SCHEMA).from_(table_name)
            .select("Id,MetaPartNumber")
            .in_("MetaPartNumber", variants)
            .limit(max(5, len(variants))),
    )
    for row in res.data or []:
        row_parts = normalized_part_number_candidates(row.get("MetaPartNumber"))
        if normalized_candidate in row_parts:
            match = (row['Id'], table_name, "part_number_exact", 1)
            SPEC_MATCH_CACHE[cache_key] = match
            return match

    SPEC_MATCH_CACHE[cache_key] = (None, None, None, None)
    return SPEC_MATCH_CACHE[cache_key]

def find_spec_match(tables, part_number, store_name=None, raw_type=None):
    if isinstance(tables, str): target_tables = [tables]
    else: target_tables = tables

    if store_name:
        override_id, override_table, override_method, override_score = find_manual_override(
            target_tables,
            store_name,
            raw_type,
            part_number,
        )
        if override_id and override_table:
            return override_id, override_table, override_method, override_score

    candidates = parse_part_numbers(part_number)
    if not candidates: return None, None, None, None

    canonical_ambiguous = False
    canonical_matches = set()
    for candidate in candidates:
        spec_id, found_table, method, score = lookup_canonical_mpn(candidate)
        if spec_id and found_table:
            canonical_matches.add((spec_id, found_table, method, score))
        if CANONICAL_MPN_REASON_CACHE.get(normalize_part_number(candidate)) == "ambiguous_mpn":
            canonical_ambiguous = True
    if len(canonical_matches) == 1 and not canonical_ambiguous:
        return next(iter(canonical_matches))
    if len(canonical_matches) > 1:
        canonical_ambiguous = True
        for candidate in candidates:
            normalized = normalize_part_number(candidate)
            if normalized:
                CANONICAL_MPN_REASON_CACHE[normalized] = "ambiguous_mpn"

    # Essential categories are fully represented by the canonical identity
    # tables.  Missing/ambiguous MPNs must remain unmatched rather than being
    # rescued by a category-scoped legacy lookup.
    if canonical_ambiguous or any(
        table_name in ESSENTIAL_SPEC_TABLES for table_name in target_tables
    ):
        return None, None, None, None

    # Preserve legacy exact matching for non-essential categories that have not
    # yet been migrated to product_identifiers/legacy_product_refs.
    for table_name in target_tables:
        for candidate in candidates:
            spec_id, found_table, method, score = lookup_exact_part_number(table_name, candidate)
            if spec_id and found_table:
                return spec_id, found_table, method, score
    return None, None, None, None

def find_spec_id(tables, part_number):
    spec_id, table_name, _, _ = find_spec_match(tables, part_number)
    return spec_id, table_name

def detect_price_anomaly(store_id, spec_id, spec_table_name, price_int):
    if price_int < MIN_REASONABLE_PUBLISH_PRICE:
        return f"price_below_minimum:{price_int}"

    cache_key = (store_id, spec_table_name, spec_id)
    if cache_key in PREVIOUS_PRICE_CACHE:
        previous_price = PREVIOUS_PRICE_CACHE[cache_key]
    else:
        result = execute_db_request(
            f"ProductPricing previous price {store_id} {spec_id}",
            lambda: get_supabase().table("ProductPricing")
                .select("Price")
                .eq("StoreId", store_id)
                .eq("SpecId", spec_id)
                .eq("SpecTableName", spec_table_name)
                .limit(1),
        )
        previous_price = result.data[0].get("Price") if result.data else None
        PREVIOUS_PRICE_CACHE[cache_key] = previous_price
    if not isinstance(previous_price, (int, float)) or previous_price <= 0:
        return None

    ratio = price_int / previous_price
    if ratio >= PRICE_ANOMALY_MAX_MULTIPLIER:
        return f"price_spike:{previous_price}->{price_int}"
    if ratio <= PRICE_ANOMALY_MIN_MULTIPLIER:
        return f"price_drop:{previous_price}->{price_int}"
    return None

def build_product_pricing_payload(spec_id, data, store_id):
    timestamp = now_iso()
    payload = {
        "SpecId": spec_id,
        "SpecTableName": data["table"],
        "StoreId": store_id,
        "Price": data["price_int"],
        "StockStatus": data.get("stock_status", True),
        "Url": data["url"],
        "LastUpdated": timestamp,
    }
    if PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED:
        payload.update({
            "AffiliateUrl": data.get("affiliate_url"),
            "LastSeenAt": timestamp,
            "StockConfidence": (
                "confirmed" if data.get("stock_status", True) else "explicit_unavailable"
            ),
        })
        if not data.get("stock_status", True):
            payload["LastConfirmedOutOfStockAt"] = timestamp
    return payload


def upsert_product_pricing(store_name, spec_id, data, store_id):
    global PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED
    payload = build_product_pricing_payload(spec_id, data, store_id)
    try:
        return execute_db_request(
            f"ProductPricing upsert {store_name} {spec_id}",
            lambda payload=payload: get_supabase().table("ProductPricing").upsert(
                payload,
                on_conflict="SpecId, SpecTableName, StoreId",
            ),
        )
    except Exception as error:
        if PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED and is_optional_schema_error(error):
            PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED = False
            print("   [WARN] ProductPricing sin columnas nuevas; reintentando upsert legacy.")
            return upsert_product_pricing(store_name, spec_id, data, store_id)
        raise

def mark_products_out_of_stock(store_name, store_id, spec_ids):
    global PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED
    normalized_ids = sorted({str(spec_id) for spec_id in spec_ids if spec_id})
    if not normalized_ids:
        return None
    payload = {
        "StockStatus": False,
        "LastUpdated": now_iso(),
    }
    if PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED:
        payload.update({
            "StockConfidence": "missing_from_healthy_scrape",
            "LastConfirmedOutOfStockAt": now_iso(),
        })
    try:
        return execute_db_request(
            f"ProductPricing atomic stock update {store_name} {len(normalized_ids)} products",
            lambda payload=payload: get_supabase().table("ProductPricing").update(payload)
                .eq("StoreId", store_id)
                .eq("StockStatus", True)
                .in_("SpecId", normalized_ids),
        )
    except Exception as error:
        if PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED and is_optional_schema_error(error):
            PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED = False
            print("   [WARN] ProductPricing sin columnas nuevas; reintentando stock legacy.")
            return mark_products_out_of_stock(store_name, store_id, normalized_ids)
        raise


def mark_product_out_of_stock(store_name, store_id, spec_id):
    return mark_products_out_of_stock(store_name, store_id, [spec_id])


def parse_utc_datetime(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def get_previous_healthy_snapshot_started_at(store_id):
    result = optional_db_request(
        "scraper_store_runs",
        f"scraper_store_runs previous healthy snapshot {store_id}",
        lambda: get_supabase().table("scraper_store_runs")
            .select("started_at,created_at")
            .eq("store_id", store_id)
            .eq("status", "success")
            .eq("output_empty", False)
            .eq("anomaly_count", 0)
            .eq("error_count", 0)
            .gte("match_rate", MIN_STOCK_MARKOUT_MATCH_RATE)
            .neq("scrape_run_id", SCRAPE_RUN_ID)
            .order("created_at", desc=True)
            .limit(1),
    )
    if not result or not result.data:
        return None
    row = result.data[0]
    return parse_utc_datetime(row.get("started_at") or row.get("created_at"))


def ids_missing_from_two_healthy_snapshots(active_rows, seen_ids, previous_snapshot_started_at):
    cutoff = parse_utc_datetime(previous_snapshot_started_at)
    if cutoff is None:
        return set()

    missing_ids = set()
    for row in active_rows or []:
        spec_id = row.get("SpecId")
        if not spec_id or spec_id in seen_ids:
            continue
        last_seen_at = parse_utc_datetime(row.get("LastSeenAt"))
        if last_seen_at is not None and last_seen_at < cutoff:
            missing_ids.add(spec_id)
    return missing_ids


def load_active_product_presence(store_name, store_id):
    global PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED
    if not PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED:
        return None
    try:
        result = execute_db_request(
            f"ProductPricing active presence select {store_name}",
            lambda: get_supabase().table("ProductPricing")
                .select("SpecId,SpecTableName,Price,LastSeenAt")
                .eq("StoreId", store_id)
                .eq("StockStatus", True),
        )
        return result.data or []
    except Exception as error:
        if is_optional_schema_error(error):
            PRODUCT_PRICING_EXTENDED_COLUMNS_ENABLED = False
            print("   [WARN] ProductPricing sin LastSeenAt; markout deshabilitado por seguridad.")
            return None
        raise


def update_store_run_markout_outcome(store_run_id, allowed, reason):
    if not store_run_id:
        return
    optional_db_request(
        "scraper_store_runs",
        f"scraper_store_runs markout outcome {store_run_id}",
        lambda: get_supabase().table("scraper_store_runs").update({
            "stock_markout_allowed": allowed,
            "stock_markout_reason": reason,
            "updated_at": now_iso(),
        }).eq("id", store_run_id),
    )


def update_store_run_processing_outcome(store_run_id, metrics):
    """Persist late publication errors discovered after the store run was created."""
    if not store_run_id:
        return
    optional_db_request(
        "scraper_store_runs",
        f"scraper_store_runs processing outcome {store_run_id}",
        lambda: get_supabase().table("scraper_store_runs").update({
            "status": metrics["status"],
            "error_count": metrics["error_count"],
            "stock_markout_allowed": metrics["stock_markout_allowed"],
            "stock_markout_reason": metrics["stock_markout_reason"],
            "updated_at": now_iso(),
        }).eq("id", store_run_id),
    )


def finalize_scrape_run_after_fatal_error(error):
    """Fail closed so a matcher crash never leaves the parent run as running."""
    optional_db_request(
        "scrape_runs",
        f"scrape_runs fatal finalize {SCRAPE_RUN_ID}",
        lambda: get_supabase().table("scrape_runs").update({
            "status": "failed",
            "finished_at": now_iso(),
            "updated_at": now_iso(),
            "error_count": 1,
        }).eq("id", SCRAPE_RUN_ID),
    )

def update_store_scrape_status(store_id, metrics):
    global STORE_QUALITY_COLUMNS_ENABLED
    payload = {
        "LastScrapedAt": now_iso(),
    }
    if STORE_QUALITY_COLUMNS_ENABLED:
        payload.update({
            "LastScrapeStatus": metrics["status"],
            "LastScrapeMatchRate": metrics["match_rate"],
            "LastScrapeRawCount": metrics["raw_count"],
            "LastScrapeMatchedCount": metrics["matched_count"],
            "LastScrapeUnmatchedCount": metrics["unmatched_count"],
            "LastScrapeRunId": SCRAPE_RUN_ID,
        })
        if metrics.get("snapshot_healthy"):
            payload["LastSuccessfulScrapedAt"] = now_iso()
    try:
        return execute_db_request(
            f"Stores scrape status update {store_id}",
            lambda payload=payload: get_supabase().table("Stores").update(payload).eq("Id", store_id),
        )
    except Exception as error:
        if STORE_QUALITY_COLUMNS_ENABLED and is_optional_schema_error(error):
            STORE_QUALITY_COLUMNS_ENABLED = False
            print("   [WARN] Stores sin columnas de calidad; reintentando update legacy.")
            return update_store_scrape_status(store_id, metrics)
        raise

def download_and_convert_image(image_url):
    """
    Descarga una imagen desde una URL y la convierte a formato WebP.
    Retorna: (bytes_webp, error_message)
    """
    try:
        # Descargar imagen
        response = requests.get(image_url, timeout=10)
        response.raise_for_status()
        
        # Abrir imagen con Pillow
        img = Image.open(BytesIO(response.content))
        
        # Convertir a RGB si es necesario (para PNGs con transparencia)
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Convertir a WebP
        output = BytesIO()
        img.save(output, format='WEBP', quality=85, method=6)
        output.seek(0)
        
        return output.read(), None
    except Exception as e:
        return None, str(e)

def upload_to_supabase_storage(image_bytes, filename):
    """
    Sube una imagen a Supabase Storage en el bucket 'ProductsImages'.
    Retorna: URL pública de la imagen o None si falla.
    """
    try:
        bucket_name = "ProductsImages"
        
        # Subir archivo
        result = get_supabase().storage.from_(bucket_name).upload(
            path=filename,
            file=image_bytes,
            file_options={"content-type": "image/webp"}
        )
        
        # Obtener URL pública
        public_url = get_supabase().storage.from_(bucket_name).get_public_url(filename)
        return public_url
    except Exception as e:
        print(f"   ⚠️  Error subiendo imagen: {e}")
        return None

def process_product_image(spec_id, table_name, image_url):
    """
    Procesa la imagen de un producto:
    1. Verifica si ya tiene imagen en la tabla de especificaciones
    2. Si no tiene, descarga, convierte a WebP y sube a Supabase
    3. Actualiza el campo ImageUrl en la tabla de especificaciones
    """
    try:
        # Verificar si ya tiene imagen
        existing = execute_db_request(
            f"{table_name} image lookup {spec_id}",
            lambda: get_supabase().schema(SPECIFICATIONS_SCHEMA).from_(table_name)
                .select("ImageUrl")
                .eq("Id", spec_id)
                .limit(1),
        )
        
        if not existing.data:
            return False
        
        current_image_url = existing.data[0].get('ImageUrl')
        
        # Si ya tiene imagen, no hacer nada
        if current_image_url:
            return True
        
        # Descargar y convertir imagen
        webp_bytes, error = download_and_convert_image(image_url)
        if error:
            print(f"   ⚠️  Error descargando imagen: {error}")
            return False
        
        # Generar nombre único para el archivo
        filename = f"{spec_id}.webp"
        
        # Subir a Supabase Storage
        public_url = upload_to_supabase_storage(webp_bytes, filename)
        if not public_url:
            return False
        
        # Actualizar ImageUrl en la tabla de especificaciones
        execute_db_request(
            f"{table_name} image update {spec_id}",
            lambda: get_supabase().schema(SPECIFICATIONS_SCHEMA).from_(table_name).update({
                "ImageUrl": public_url
            }).eq("Id", spec_id),
        )
        
        print(f"   ✅ Imagen procesada y subida para {spec_id}")
        return True
        
    except Exception as e:
        print(f"   ⚠️  Error procesando imagen: {e}")
        return False

# ================= PROCESO PRINCIPAL =================

def process_daily_scraps():
    print("🚀 Iniciando procesamiento (Con Deduplicación y Precio Mínimo)...")
    
    with open(LOG_FILE, 'w', encoding='utf-8') as log:
        log.write(f"--- Reporte de No Match: {datetime.now()} ---\n")

    store_batches = {} 

    if not os.path.exists(SCRAP_OUTPUT_DIR):
        print("❌ Directorio no encontrado.")
        return

    # 1. Lectura de Archivos
    for root, dirs, files in os.walk(SCRAP_OUTPUT_DIR):
        for filename in files:
            if filename.endswith(".json"):
                filepath = os.path.join(root, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        content = json.load(f)
                        if isinstance(content, dict): content = [content]
                        
                        for item in content:
                            s_name = item.get("store_name")
                            if s_name:
                                if s_name not in store_batches: store_batches[s_name] = []
                                item["_source_file"] = filename
                                store_batches[s_name].append(item)
                except Exception as e:
                    print(f"❌ Error en {filename}: {e}")

    # 2. Procesamiento por Tienda
    for store_name, items in store_batches.items():
        print(f"\n🔵 Tienda: {store_name} - Items brutos: {len(items)}")
        store_id = get_or_create_store(store_name)
        
        # --- FASE A: Deduplicación en Memoria ---
        # Usaremos un diccionario donde la clave sea el SpecId (el producto único)
        # y el valor sea el item con el MENOR precio encontrado.
        unique_products_today = {} # { "UUID-XXX": {data_del_item_mas_barato} }
        
        # Lista para logs de error que escribiremos después
        unmatched_buffer = []

        print("   🔍 Analizando y deduplicando...")
        for item in items:
            raw_type = item.get("type")
            part_num = item.get("part #")
            price = item.get("price")
            url = item.get("url")
            source_file = item.get("_source_file", "unknown")
            
            if not raw_type or not part_num or not price: continue

            target_tables = CATEGORY_TO_TABLE.get(raw_type)
            if not target_tables: continue
            
            # Buscamos ID
            spec_id, found_table = find_spec_id(target_tables, part_num)
            
            if spec_id and found_table:
                price_int = parse_price_to_int(price)
                if price_int is None:
                    continue

                # LÓGICA DE PRECIO MÍNIMO:
                if spec_id in unique_products_today:
                    # Ya vimos este producto hoy. ¿El nuevo es más barato?
                    existing_price = unique_products_today[spec_id]['price_int']
                    if price_int < existing_price:
                        # Reemplazamos con el más barato
                        unique_products_today[spec_id] = {
                            "spec_id": spec_id,
                            "table": found_table,
                            "price_int": price_int,
                            "url": url,
                            "image_url": item.get("image_url"),
                        }
                else:
                    # Primera vez que vemos este producto hoy
                    unique_products_today[spec_id] = {
                        "spec_id": spec_id,
                        "table": found_table,
                        "price_int": price_int,
                        "url": url,
                        "image_url": item.get("image_url"),
                    }
            else:
                unmatched_buffer.append(f"[{source_file}] {url} | TYPE: {raw_type} | PN: {part_num}")

        # Escribir logs de no encontrados
        if unmatched_buffer:
            with open(LOG_FILE, 'a', encoding='utf-8') as log:
                for entry in unmatched_buffer:
                    log.write(entry + "\n")

        print(f"   💾 Insertando {len(unique_products_today)} productos únicos en DB...")

        # --- FASE B: Inserción en Base de Datos ---
        # Ahora recorremos la lista limpia (sin duplicados, precio mínimo garantizado)
        
        found_ids_today = set()

        for spec_id, data in unique_products_today.items():
            found_ids_today.add(spec_id)
            
            # 1. Upsert ProductPricing (Estado Actual)
            execute_db_request(
                f"ProductPricing upsert {store_name} {spec_id}",
                lambda spec_id=spec_id, data=data: supabase.table("ProductPricing").upsert({
                    "SpecId": spec_id,
                    "SpecTableName": data["table"],
                    "StoreId": store_id,
                    "Price": data["price_int"],
                    "StockStatus": True,
                    "Url": data["url"],
                    "LastUpdated": datetime.now().isoformat()
                }, on_conflict="SpecId, SpecTableName, StoreId"),
            )
            
            # 2. Insert PriceHistory (Nueva entrada siempre)
            # Como ya deduplicamos, esto solo insertará 1 vez por producto por ejecución.
            execute_db_request(
                f"PriceHistory insert {store_name} {spec_id}",
                lambda spec_id=spec_id, data=data: supabase.table("PriceHistory").insert({
                    "SpecId": spec_id,
                    "SpecTableName": data["table"],
                    "StoreId": store_id,
                    "Price": data["price_int"],
                    "RecordedAt": datetime.now().isoformat()
                }),
            )
            
            # 3. Procesar imagen del producto si existe y no es N/A
            if "image_url" in data and data["image_url"] != "N/A":
                process_product_image(spec_id, data["table"], data["image_url"])

        # --- FASE C: Stock Agotado ---
        print("   🔄 Verificando stock agotado...")
        active_products = execute_db_request(
            f"ProductPricing active select {store_name}",
            lambda: supabase.table("ProductPricing")
                .select("SpecId")
                .eq("StoreId", store_id)
                .eq("StockStatus", True),
        )
            
        active_ids_db = {row['SpecId'] for row in active_products.data}
        missing_ids = active_ids_db - found_ids_today
        
        if missing_ids:
            print(f"   📉 {len(missing_ids)} productos marcados como NO DISPONIBLES.")
            for missing in missing_ids:
                execute_db_request(
                    f"ProductPricing stock update {store_name} {missing}",
                    lambda missing=missing: supabase.table("ProductPricing").update({
                        "StockStatus": False,
                        "LastUpdated": datetime.now().isoformat()
                    }).eq("SpecId", missing).eq("StoreId", store_id),
                )

        execute_db_request(
            f"Stores LastScrapedAt update {store_name}",
            lambda: supabase.table("Stores").update({"LastScrapedAt": datetime.now().isoformat()}).eq("Id", store_id),
        )

    print(f"\n🏁 Listo. Logs en '{LOG_FILE}'.")

def build_raw_row(item, store_id, store_name, match_status, parsed_price=None, matched_spec_id=None, matched_table=None, anomaly_reason=None, store_run_id=None):
    return {
        "id": str(uuid_lib.uuid4()),
        "scrape_run_id": SCRAPE_RUN_ID,
        "scraper_store_run_id": store_run_id,
        "store_id": store_id,
        "store_name": store_name,
        "source_file": item.get("_source_file"),
        "source_url": item.get("url"),
        "scraped_category": item.get("type"),
        "scraped_name": first_text(item, "scraped_name", "name", "title", "product_name", "productName", "nombre"),
        "scraped_part_number": first_text(item, "part #", "part_number", "sku", "mpn"),
        "normalized_part_number": normalize_part_number(first_text(item, "part #", "part_number", "sku", "mpn")),
        "raw_price": str(item.get("price") or ""),
        "parsed_price": parsed_price,
        "raw_payload": item,
        "match_status": match_status,
        "matched_spec_table_name": matched_table,
        "matched_spec_id": matched_spec_id,
        "anomaly_reason": anomaly_reason,
    }

def build_match_candidate_row(store_id, store_name, item, spec_id, spec_table_name, method, score, status="selected", raw_id=None):
    return {
        "raw_id": raw_id,
        "scrape_run_id": SCRAPE_RUN_ID,
        "store_id": store_id,
        "store_name": store_name,
        "scraped_category": item.get("type"),
        "scraped_part_number": first_text(item, "part #", "part_number", "sku", "mpn"),
        "spec_table_name": spec_table_name,
        "spec_id": spec_id,
        "match_method": method or "unknown",
        "score": score,
        "status": status,
    }

def process_daily_scraps():
    run_started = datetime.now(timezone.utc)
    print(f"[match] Iniciando procesamiento. scrape_run_id={SCRAPE_RUN_ID}")

    get_supabase()
    scraper_summary = load_scraper_summary(SCRAPER_SUMMARY_PATH)
    scraper_result_map = build_scraper_result_map(scraper_summary)
    create_scrape_run(scraper_summary)

    with open(LOG_FILE, 'w', encoding='utf-8') as log:
        log.write(f"--- Reporte de No Match: {now_iso()} ---\n")

    store_batches = {}
    totals = {
        "store_count": 0,
        "raw_count": 0,
        "matched_count": 0,
        "unmatched_count": 0,
        "anomaly_count": 0,
        "error_count": 0,
        "match_rate": 0,
        "duration_seconds": None,
        "warning_store_count": 0,
    }
    input_error_count = 0

    if not os.path.exists(SCRAP_OUTPUT_DIR):
        print("[match] Directorio de outputs no encontrado.")
        totals["duration_seconds"] = round((datetime.now(timezone.utc) - run_started).total_seconds(), 2)
        finalize_scrape_run(totals, "failed")
        return

    for root, dirs, files in os.walk(SCRAP_OUTPUT_DIR):
        for filename in files:
            if not filename.endswith(".json"):
                continue
            filepath = os.path.join(root, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                    if isinstance(content, dict):
                        content = [content]

                    for item in content:
                        s_name = item.get("store_name")
                        if s_name:
                            store_batches.setdefault(s_name, [])
                            item["_source_file"] = filename
                            store_batches[s_name].append(item)
                        else:
                            totals["error_count"] += 1
                            input_error_count += 1
                            print(f"[match] Item sin store_name en {filename}; markout deshabilitado.")
            except Exception as error:
                totals["error_count"] += 1
                input_error_count += 1
                print(f"[match] Error leyendo {filename}: {error}")

    prefetch_canonical_mpn_matches(
        first_text(item, "part #", "part_number", "sku", "mpn")
        for items in store_batches.values()
        for item in items
    )
    print(
        "[match] Indice MPN canonico preparado: "
        f"{len(CANONICAL_MPN_CACHE)} identificadores consultados."
    )

    for store_name, items in store_batches.items():
        raw_count = len(items)
        print(f"\n[match] Tienda: {store_name} - items brutos: {raw_count}")
        store_id = get_or_create_store(store_name)
        scraper_result = scraper_result_for_store(store_name, scraper_result_map)

        unique_products_today = {}
        seen_ids_today = set()
        unmatched_buffer = []
        raw_rows = []
        candidate_rows = []
        matched_count = 0
        unmatched_count = 0
        anomaly_count = 0
        error_count = 0
        exact_mpn_count = 0
        ambiguous_mpn_count = 0
        mpn_not_found_count = 0
        category_corrected_from_mpn_count = 0

        for item in items:
            raw_type = item.get("type")
            part_num = first_text(item, "part #", "part_number", "sku", "mpn")
            price = item.get("price")
            url = item.get("url")
            source_file = item.get("_source_file", "unknown")
            price_int = parse_price_to_int(price)

            if not raw_type or not part_num or price_int is None:
                error_count += 1
                raw_rows.append(build_raw_row(item, store_id, store_name, "invalid", parsed_price=price_int))
                continue

            target_tables = CATEGORY_TO_TABLE.get(raw_type)
            if not target_tables:
                unmatched_count += 1
                raw_rows.append(build_raw_row(item, store_id, store_name, "unmatched", parsed_price=price_int))
                unmatched_buffer.append(f"[{source_file}] {url} | TYPE: {raw_type} | PN: {part_num} | reason: unknown_category")
                continue

            spec_id, found_table, match_method, match_score = find_spec_match(
                target_tables,
                part_num,
                store_name=store_name,
                raw_type=raw_type,
            )

            if not spec_id or not found_table:
                mpn_reason = canonical_mpn_reason(part_num)
                if mpn_reason == "ambiguous_mpn":
                    ambiguous_mpn_count += 1
                elif mpn_reason == "mpn_not_found":
                    mpn_not_found_count += 1
                unmatched_count += 1
                raw_rows.append(build_raw_row(item, store_id, store_name, "unmatched", parsed_price=price_int))
                unmatched_buffer.append(
                    f"[{source_file}] {url} | TYPE: {raw_type} | "
                    f"PN: {part_num} | reason: {mpn_reason}"
                )
                continue

            if match_method == "exact_mpn":
                exact_mpn_count += 1
                normalized_targets = (
                    {target_tables}
                    if isinstance(target_tables, str)
                    else set(target_tables)
                )
                if found_table not in normalized_targets:
                    category_corrected_from_mpn_count += 1

            # A matched product was present in this snapshot even when its price is
            # quarantined. Anomalies must never be interpreted as stock absence.
            seen_ids_today.add(spec_id)
            scraped_stock_status = explicit_stock_status(item)

            # An explicit store-level OOS signal is authoritative and must be
            # published immediately. It is still a successful exact match and a
            # seen listing, so it cannot be confused with a partial-snapshot absence.
            if scraped_stock_status is False:
                matched_count += 1
                product_data = {
                    "spec_id": spec_id,
                    "table": found_table,
                    "price_int": price_int,
                    "url": url,
                    "affiliate_url": item.get("affiliate_url") or item.get("affiliateUrl"),
                    "image_url": item.get("image_url"),
                    "match_method": match_method,
                    "match_score": match_score,
                    "stock_status": False,
                }
                unique_products_today[spec_id] = preferred_product_snapshot(
                    unique_products_today.get(spec_id), product_data
                )
                continue

            anomaly_reason = detect_price_anomaly(store_id, spec_id, found_table, price_int)
            if anomaly_reason:
                anomaly_count += 1
                raw_row = build_raw_row(
                    item,
                    store_id,
                    store_name,
                    "price_anomaly",
                    parsed_price=price_int,
                    matched_spec_id=spec_id,
                    matched_table=found_table,
                    anomaly_reason=anomaly_reason,
                )
                raw_rows.append(raw_row)
                candidate_rows.append(
                    build_match_candidate_row(
                        store_id,
                        store_name,
                        item,
                        spec_id,
                        found_table,
                        match_method,
                        match_score,
                        status="rejected",
                        raw_id=raw_row["id"],
                    )
                )
                continue

            # Ordinary exact matches are represented by ProductPricing and the
            # canonical listing. Full payloads are reserved for short-lived issues.
            matched_count += 1

            product_data = {
                "spec_id": spec_id,
                "table": found_table,
                "price_int": price_int,
                "url": url,
                "affiliate_url": item.get("affiliate_url") or item.get("affiliateUrl"),
                "image_url": item.get("image_url"),
                "match_method": match_method,
                "match_score": match_score,
                "stock_status": True,
            }
            unique_products_today[spec_id] = preferred_product_snapshot(
                unique_products_today.get(spec_id), product_data
            )

        match_rate = matched_count / raw_count if raw_count else 0
        snapshot_healthy, stock_markout_reason = should_allow_stock_markout(
            raw_count,
            matched_count,
            scraper_result,
            anomaly_count=anomaly_count,
            error_count=error_count,
            input_error_count=input_error_count,
        )
        previous_snapshot_started_at = None
        stock_markout_allowed = snapshot_healthy
        if snapshot_healthy:
            previous_snapshot_started_at = get_previous_healthy_snapshot_started_at(store_id)
            if previous_snapshot_started_at is None:
                stock_markout_allowed = False
                stock_markout_reason = "awaiting_second_healthy_snapshot"

        status = "success" if snapshot_healthy else (
            "failed" if scraper_result and not scraper_result.get("success", False) else "warning"
        )
        store_metrics = {
            "raw_count": raw_count,
            "matched_count": matched_count,
            "unmatched_count": unmatched_count,
            "anomaly_count": anomaly_count,
            "error_count": error_count,
            "output_empty": raw_count == 0,
            "match_rate": match_rate,
            "stock_markout_allowed": stock_markout_allowed,
            "stock_markout_reason": stock_markout_reason,
            "status": status,
            "snapshot_healthy": snapshot_healthy,
            "exact_mpn_count": exact_mpn_count,
            "ambiguous_mpn_count": ambiguous_mpn_count,
            "mpn_not_found_count": mpn_not_found_count,
            "category_corrected_from_mpn_count": category_corrected_from_mpn_count,
        }
        store_run_id = create_store_run(store_id, store_name, scraper_result, store_metrics)
        if store_run_id:
            for row in raw_rows:
                row["scraper_store_run_id"] = store_run_id

        raw_rows_inserted = optional_insert_rows("scraped_products_raw", raw_rows)
        record_scrape_issues(raw_rows)
        if raw_rows_inserted:
            optional_insert_rows("match_candidates", candidate_rows)
        elif candidate_rows:
            print("   [WARN] Se omiten match_candidates porque scraped_products_raw no esta disponible.")

        if unmatched_buffer:
            with open(LOG_FILE, 'a', encoding='utf-8') as log:
                for entry in unmatched_buffer:
                    log.write(entry + "\n")

        print(
            f"[match] {store_name}: raw={raw_count}, matched={matched_count}, "
            f"unmatched={unmatched_count}, anomalies={anomaly_count}, match_rate={match_rate:.2%}"
        )
        print(f"[match] Stock markout: {stock_markout_allowed} ({stock_markout_reason})")
        print(f"[match] Insertando {len(unique_products_today)} productos unicos en DB.")

        publication_error_count = 0
        for spec_id, data in unique_products_today.items():
            try:
                upsert_product_pricing(store_name, spec_id, data, store_id)
                record_legacy_offer_change(
                    spec_id,
                    data["table"],
                    store_id,
                    data["price_int"],
                    data.get("stock_status", True),
                )

                if data.get("image_url") and data["image_url"] != "N/A":
                    process_product_image(spec_id, data["table"], data["image_url"])
            except Exception as error:
                publication_error_count += 1
                print(
                    f"[match] Error publicando {store_name}/{spec_id}; "
                    f"se conserva el resto de la tienda: {error}"
                )

        if publication_error_count:
            error_count += publication_error_count
            stock_markout_allowed = False
            stock_markout_reason = "publication_errors"
            status = "warning"
            snapshot_healthy = False
            print(
                f"[match] {store_name}: {publication_error_count} errores de publicacion; "
                "markout deshabilitado."
            )

        if stock_markout_allowed:
            active_rows = load_active_product_presence(store_name, store_id)
            if active_rows is None:
                stock_markout_allowed = False
                stock_markout_reason = "last_seen_unavailable"
            else:
                missing_ids = ids_missing_from_two_healthy_snapshots(
                    active_rows,
                    seen_ids_today,
                    previous_snapshot_started_at,
                )
                if missing_ids:
                    print(
                        f"[match] {len(missing_ids)} productos ausentes en dos snapshots saludables; "
                        f"se marcan no disponibles para {store_name}."
                    )
                    # A single PostgREST update maps to one atomic SQL statement.
                    # If it fails, no loop can leave a partially marked-out store.
                    mark_products_out_of_stock(store_name, store_id, missing_ids)
                    active_by_id = {str(row.get("SpecId")): row for row in active_rows}
                    for missing_id in missing_ids:
                        previous = active_by_id.get(str(missing_id), {})
                        spec_table_name = previous.get("SpecTableName")
                        if spec_table_name:
                            record_legacy_offer_change(
                                missing_id,
                                spec_table_name,
                                store_id,
                                previous.get("Price"),
                                False,
                            )
        else:
            print(f"[match] Se omite marcado sin stock para {store_name}: {stock_markout_reason}.")

        store_metrics["stock_markout_allowed"] = stock_markout_allowed
        store_metrics["stock_markout_reason"] = stock_markout_reason
        store_metrics["error_count"] = error_count
        store_metrics["status"] = status
        store_metrics["snapshot_healthy"] = snapshot_healthy
        update_store_run_processing_outcome(store_run_id, store_metrics)
        update_store_scrape_status(store_id, store_metrics)

        totals["store_count"] += 1
        totals["raw_count"] += raw_count
        totals["matched_count"] += matched_count
        totals["unmatched_count"] += unmatched_count
        totals["anomaly_count"] += anomaly_count
        totals["error_count"] += error_count
        totals["exact_mpn_count"] = totals.get("exact_mpn_count", 0) + exact_mpn_count
        totals["ambiguous_mpn_count"] = totals.get("ambiguous_mpn_count", 0) + ambiguous_mpn_count
        totals["mpn_not_found_count"] = totals.get("mpn_not_found_count", 0) + mpn_not_found_count
        totals["category_corrected_from_mpn_count"] = (
            totals.get("category_corrected_from_mpn_count", 0)
            + category_corrected_from_mpn_count
        )
        if not snapshot_healthy:
            totals["warning_store_count"] += 1

    totals["match_rate"] = totals["matched_count"] / totals["raw_count"] if totals["raw_count"] else 0
    totals["duration_seconds"] = round((datetime.now(timezone.utc) - run_started).total_seconds(), 2)
    final_status = "success"
    if totals["error_count"] or totals["anomaly_count"] or totals["warning_store_count"]:
        final_status = "partial_success"
    if totals["raw_count"] == 0:
        final_status = "failed"
    finalize_scrape_run(totals, final_status)
    print(f"\n[match] Listo. Logs en '{LOG_FILE}'.")

if __name__ == "__main__":
    try:
        process_daily_scraps()
    except Exception as error:
        print(f"[match] Error fatal: {error}")
        finalize_scrape_run_after_fatal_error(error)
        raise

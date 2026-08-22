#!/usr/bin/env bash
set -Eeuo pipefail

RUNTIME_ROOT="${RUNTIME_ROOT:-/srv/armatupc/scraper}"
RELEASE_ROOT="${RELEASE_ROOT:-${RUNTIME_ROOT}/current}"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${RUNTIME_ROOT}/artifacts}"
LOG_ROOT="${LOG_ROOT:-${RUNTIME_ROOT}/logs}"
IMAGE="${SCRAPER_IMAGE:-armatupc-scraper:current}"
SOURCE_RUN_DIR="${SOURCE_RUN_DIR:-}"
SCRAPER_INCLUDE_OVERRIDE="${SCRAPER_INCLUDE_OVERRIDE:-}"

normalize_scraper_include() {
  local raw="$1"
  local token
  local normalized=()
  IFS=',' read -ra tokens <<< "${raw}"
  for token in "${tokens[@]}"; do
    token="$(xargs <<< "${token}")"
    [[ -z "${token}" ]] && continue
    if [[ "${token}" == *.py ]]; then
      normalized+=("${token}")
    elif [[ "${token}" == Scrap_* ]]; then
      normalized+=("${token}.py")
    else
      normalized+=("Scrap_${token}.py")
    fi
  done
  local IFS=','
  echo "${normalized[*]}"
}

if [[ -z "${SOURCE_RUN_DIR}" ]]; then
  SOURCE_RUN_DIR="$({
    find "${ARTIFACT_ROOT}" -mindepth 1 -maxdepth 1 -type d \
      -name '*-home' -printf '%T@ %p\n' 2>/dev/null || true
  } | sort -nr | awk 'NR == 1 {print $2}')"
fi

SOURCE_MANIFEST="${SOURCE_RUN_DIR}/merged/RunLogs/retry-manifest.json"
if [[ -z "${SOURCE_RUN_DIR}" || ! -f "${SOURCE_MANIFEST}" ]]; then
  echo "No completed local daily manifest was found." >&2
  exit 1
fi

if [[ -n "${SCRAPER_INCLUDE_OVERRIDE}" ]]; then
  SCRAPER_INCLUDE="$(normalize_scraper_include "${SCRAPER_INCLUDE_OVERRIDE}")"
else
  SCRAPER_INCLUDE="$(jq -r '.failed_scrapers | join(",")' "${SOURCE_MANIFEST}")"
fi

if [[ -z "${SCRAPER_INCLUDE}" ]]; then
  echo "The source run has no retryable scrapers."
  exit 0
fi

SOURCE_SCRAPE_RUN_ID="$(jq -r '.scrape_run_id // empty' "${SOURCE_MANIFEST}")"
RUN_ID="$(date -u +%Y%m%d_%H%M%S)-home-retry"
RUN_DIR="${ARTIFACT_ROOT}/${RUN_ID}"
mkdir -p "${RUN_DIR}/Outputs" "${RUN_DIR}/RunLogs" "${LOG_ROOT}"
exec > >(tee -a "${LOG_ROOT}/${RUN_ID}.log") 2>&1

echo "Source run: ${SOURCE_RUN_DIR}"
echo "Retrying: ${SCRAPER_INCLUDE}"
echo "Shadow mode: retry outputs will not be matched or published."

docker run --rm \
  --name "armatupc-${RUN_ID}" \
  --memory 6g --cpus 4 \
  -e SCRAP_HEADLESS=1 \
  -e SCRAP_USE_XVFB=1 \
  -e SCRAPER_RETRY_ON_EMPTY=1 \
  -e SCRAPER_RETRY_HEADFUL_ON_FAIL=1 \
  -e SCRAPER_INCLUDE="${SCRAPER_INCLUDE}" \
  -e SCRAPER_HEADFUL="Scrap_Alltec.py,Scrap_MyBox.py,Scrap_NotebooksYa.py,Scrap_TruluStore.py,Scrap_InfoSep.py,Scrap_Winpy.py" \
  -e SCRAPER_NO_HEADFUL_RETRY="Scrap_NiceOne.py,Scrap_CentralGamer.py,Scrap_CTMan.py,Scrap_DazbogStore.py,Scrap_TecnoShopping.py,Scrap_PCExpress.py" \
  -e SCRAPER_REQUIRE_NON_EMPTY="${SCRAPER_INCLUDE}" \
  -e RUN_MATCH_PRODUCTS=0 \
  -e SCRAPE_SOURCE=retry \
  -e SCRAPE_SHARD_NAME=local-retry \
  -e PARENT_SCRAPE_RUN_ID="${SOURCE_SCRAPE_RUN_ID}" \
  -e SCRAPER_TIMEOUT_MINUTES=90 \
  -e MATCH_TIMEOUT_MINUTES=60 \
  -e WINPY_CATEGORY_RETRY_PASSES=1 \
  -e WINPY_COLLECTOR_CONCURRENCY=1 \
  -e WINPY_SCRAPER_CONCURRENCY=2 \
  -e WINPY_LISTING_NAVIGATION_DELAY_SECONDS=8 \
  -e BROWSER_FALLBACK_COLLECTOR_CONCURRENCY=1 \
  -e BROWSER_FALLBACK_SCRAPER_CONCURRENCY=1 \
  -e SCRAP_BROWSER_START_TIMEOUT=90 \
  -e CHROME_BINARY_PATH=/usr/bin/chromium \
  -v "${RUN_DIR}/Outputs:/app/ScrapDB/Outputs" \
  -v "${RUN_DIR}/RunLogs:/app/ScrapDB/RunLogs" \
  "${IMAGE}" || true

SUMMARY_FILE="$({
  find "${RUN_DIR}/RunLogs" -mindepth 2 -maxdepth 2 -type f -name summary.json \
    -printf '%T@ %p\n' 2>/dev/null || true
} | sort -nr | awk 'NR == 1 {print $2}')"
if [[ -z "${SUMMARY_FILE}" || ! -f "${SUMMARY_FILE}" ]]; then
  echo "Retry runner did not produce a summary." >&2
  exit 1
fi
DISCOVERED_COUNT="$(jq -r '.scraper_count // (.scraper_results | length) // 0' "${SUMMARY_FILE}")"
if [[ "${DISCOVERED_COUNT}" -eq 0 ]]; then
  echo "Retry runner discovered zero scrapers for ${SCRAPER_INCLUDE}; refusing false success." >&2
  exit 1
fi

docker run --rm \
  -v "${RUN_DIR}/Outputs:/app/ScrapDB/Outputs" \
  -v "${RUN_DIR}/RunLogs:/app/ScrapDB/RunLogs" \
  "${IMAGE}" python ScrapDB/scraper_retry_manifest.py build \
    --logs-root ScrapDB/RunLogs \
    --outputs-root ScrapDB/Outputs \
    --manifest ScrapDB/RunLogs/retry-manifest.json \
    --prune-failed

jq --arg run_id "${RUN_ID}" --arg source_run "${SOURCE_RUN_DIR}" \
  '. + {localRunId:$run_id,sourceLocalRun:$source_run,publishEnabled:false}' \
  "${RUN_DIR}/RunLogs/retry-manifest.json" \
  > /srv/armatupc/specdb-ai/imports/scrape-retry-status.json.tmp
mv /srv/armatupc/specdb-ai/imports/scrape-retry-status.json.tmp \
  /srv/armatupc/specdb-ai/imports/scrape-retry-status.json

echo "Local retry completed: ${RUN_DIR}"

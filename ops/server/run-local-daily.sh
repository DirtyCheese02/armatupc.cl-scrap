#!/usr/bin/env bash
set -Eeuo pipefail

RUNTIME_ROOT="${RUNTIME_ROOT:-/srv/armatupc/scraper}"
RELEASE_ROOT="${RELEASE_ROOT:-${RUNTIME_ROOT}/current}"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${RUNTIME_ROOT}/artifacts}"
LOG_ROOT="${LOG_ROOT:-${RUNTIME_ROOT}/logs}"
ENV_FILE="${ENV_FILE:-/etc/armatupc/credentials/scraper.env}"
IMAGE="${SCRAPER_IMAGE:-armatupc-scraper:current}"
PUBLISH_ENABLED="${PUBLISH_ENABLED:-0}"
CYCLE_DATE="$(TZ=America/Santiago date +%F)"
RUN_ID="$(date -u +%Y%m%d_%H%M%S)-home"
RUN_DIR="${ARTIFACT_ROOT}/${RUN_ID}"
OWNER="home:$(hostname):${RUN_ID}"
TOKEN=""
HEARTBEAT_PID=""

mkdir -p "${RUN_DIR}/shards" "${RUN_DIR}/merged/Outputs" \
  "${RUN_DIR}/merged/RunLogs" "${RUN_DIR}/merged/RawRuns" "${LOG_ROOT}"
touch "${RUN_DIR}/merged/unmatched_log.txt"
exec > >(tee -a "${LOG_ROOT}/${RUN_ID}.log") 2>&1

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}; local publication remains unavailable." >&2
  if [[ "${PUBLISH_ENABLED}" == "1" ]]; then
    exit 1
  fi
fi

coordinator() {
  docker run --rm \
    --env-file "${ENV_FILE}" \
    -v "${RELEASE_ROOT}:/app:ro" \
    -v "${RUN_DIR}/merged/Outputs:/app/ScrapDB/Outputs" \
    -v "${RUN_DIR}/merged/RunLogs:/app/ScrapDB/RunLogs" \
    -w /app \
    "${IMAGE}" \
    python -m ScrapDB.scrape_cycle "$@"
}

finalize_failure() {
  local exit_code=$?
  set +e
  if [[ -n "${HEARTBEAT_PID}" ]]; then
    kill "${HEARTBEAT_PID}" 2>/dev/null || true
  fi
  if [[ -n "${TOKEN}" && -f "${ENV_FILE}" ]]; then
    coordinator finalize \
      --date "${CYCLE_DATE}" --token "${TOKEN}" --state failed \
      --metadata "{\"runId\":\"${RUN_ID}\",\"exitCode\":${exit_code}}" || true
  fi
  exit "${exit_code}"
}
trap finalize_failure ERR

if [[ "${PUBLISH_ENABLED}" == "1" ]]; then
  claim_json="$(coordinator claim \
    --date "${CYCLE_DATE}" --origin home --owner "${OWNER}" \
    --lease-seconds 21600)"
  TOKEN="$(jq -r '.fencingToken // empty' <<<"${claim_json}")"
  if [[ -z "${TOKEN}" ]]; then
    echo "Daily cycle is already owned; exiting without scraping."
    exit 0
  fi
  (
    while sleep 300; do
      coordinator heartbeat --date "${CYCLE_DATE}" --token "${TOKEN}" \
        --lease-seconds 21600 --metadata "{\"runId\":\"${RUN_ID}\"}" || exit 1
    done
  ) &
  HEARTBEAT_PID=$!
else
  echo "Shadow mode: outputs will not be matched or published."
fi

declare -A SHARDS=(
  [api-fast]="Scrap_Centrale.py,Scrap_CentralGamer.py,Scrap_CIntegral.py,Scrap_DazbogStore.py,Scrap_ETChile.py,Scrap_MyShop.py,Scrap_SandosStore.py,Scrap_TecnoMaster.py,Scrap_TecnoShopping.py,Scrap_InvasionGamer.py"
  [html-mid]="Scrap_EYLStore.py,Scrap_NotebookStore.py,Scrap_TecnoMas.py,Scrap_InfoSep.py"
  [browser-blocked]="Scrap_Alltec.py,Scrap_CTMan.py,Scrap_MyBox.py,Scrap_NotebooksYa.py,Scrap_TruluStore.py"
  [legacy-kdtec]="Scrap_KDtec.py"
  [legacy-pcexpress]="Scrap_PCExpress.py"
  [legacy-spdigital]="Scrap_SPDigital.py"
  [legacy-winpy]="Scrap_Winpy.py"
  [legacy-niceone]="Scrap_NiceOne.py"
)

HEADFUL="Scrap_Alltec.py,Scrap_MyBox.py,Scrap_NotebooksYa.py,Scrap_TruluStore.py,Scrap_InfoSep.py,Scrap_Winpy.py"
NO_HEADFUL="Scrap_NiceOne.py,Scrap_CentralGamer.py,Scrap_CTMan.py,Scrap_DazbogStore.py,Scrap_TecnoShopping.py,Scrap_PCExpress.py"
REQUIRED="Scrap_Centrale.py,Scrap_CentralGamer.py,Scrap_ETChile.py,Scrap_NotebooksYa.py,Scrap_MyShop.py,Scrap_SandosStore.py,Scrap_MyBox.py,Scrap_NiceOne.py,Scrap_TecnoMas.py,Scrap_TruluStore.py,Scrap_DazbogStore.py,Scrap_TecnoMaster.py,Scrap_Alltec.py,Scrap_CTMan.py,Scrap_NotebookStore.py,Scrap_EYLStore.py,Scrap_InfoSep.py,Scrap_InvasionGamer.py,Scrap_CIntegral.py,Scrap_TecnoShopping.py"

run_shard() {
  local shard="$1"
  local include="$2"
  local shard_root="${RUN_DIR}/shards/${shard}"
  mkdir -p "${shard_root}/Outputs" "${shard_root}/RunLogs"
  docker run --rm \
    --name "armatupc-${RUN_ID}-${shard}" \
    --memory 4g --cpus 3 \
    -e SCRAP_HEADLESS=1 \
    -e SCRAP_USE_XVFB=1 \
    -e SCRAPER_RETRY_ON_EMPTY=1 \
    -e SCRAPER_RETRY_HEADFUL_ON_FAIL=1 \
    -e SCRAPER_INCLUDE="${include}" \
    -e SCRAPER_HEADFUL="${HEADFUL}" \
    -e SCRAPER_NO_HEADFUL_RETRY="${NO_HEADFUL}" \
    -e SCRAPER_REQUIRE_NON_EMPTY="${REQUIRED}" \
    -e RUN_MATCH_PRODUCTS=0 \
    -e SCRAPE_SOURCE=home \
    -e SCRAPE_SHARD_NAME="${shard}" \
    -e SCRAPER_TIMEOUT_MINUTES=90 \
    -e MATCH_TIMEOUT_MINUTES=60 \
    -e WINPY_CATEGORY_RETRY_PASSES=0 \
    -e BROWSER_FALLBACK_COLLECTOR_CONCURRENCY=1 \
    -e BROWSER_FALLBACK_SCRAPER_CONCURRENCY=1 \
    -e CHROME_BINARY_PATH=/usr/bin/chromium \
    -v "${shard_root}/Outputs:/app/ScrapDB/Outputs" \
    -v "${shard_root}/RunLogs:/app/ScrapDB/RunLogs" \
    "${IMAGE}" || true
}

active=0
for shard in "${!SHARDS[@]}"; do
  run_shard "${shard}" "${SHARDS[$shard]}" &
  ((active += 1))
  if (( active >= 3 )); then
    wait -n || true
    ((active -= 1))
  fi
done
wait || true

for shard_root in "${RUN_DIR}"/shards/*; do
  cp -a "${shard_root}/Outputs/." "${RUN_DIR}/merged/Outputs/" 2>/dev/null || true
  cp -a "${shard_root}/RunLogs/." "${RUN_DIR}/merged/RunLogs/" 2>/dev/null || true
done

docker run --rm \
  -v "${RUN_DIR}/merged/Outputs:/app/ScrapDB/Outputs" \
  -v "${RUN_DIR}/merged/RunLogs:/app/ScrapDB/RunLogs" \
  "${IMAGE}" python ScrapDB/scraper_retry_manifest.py build \
    --logs-root ScrapDB/RunLogs \
    --outputs-root ScrapDB/Outputs \
    --manifest ScrapDB/RunLogs/retry-manifest.json \
    --prune-failed

if [[ "${PUBLISH_ENABLED}" == "1" ]]; then
  docker run --rm \
    --env-file "${ENV_FILE}" \
    -e SCRAPE_SOURCE=home \
    -e SCRAPE_CYCLE_DATE="${CYCLE_DATE}" \
    -e SCRAPE_FENCING_TOKEN="${TOKEN}" \
    -e SCRAPER_SUMMARY_PATH=ScrapDB/RunLogs/retry-manifest.json \
    -v "${RUN_DIR}/merged/Outputs:/app/ScrapDB/Outputs" \
    -v "${RUN_DIR}/merged/RunLogs:/app/ScrapDB/RunLogs" \
    -v "${RUN_DIR}/merged/unmatched_log.txt:/app/ScrapDB/unmatched_log.txt" \
    "${IMAGE}" python ScrapDB/match_products.py

  if [[ -d /srv/armatupc/specdb-ai/imports ]]; then
    install -m 0644 "${RUN_DIR}/merged/unmatched_log.txt" \
      "/srv/armatupc/specdb-ai/imports/${RUN_ID}-unmatched.txt"
  fi

  coordinator assert --date "${CYCLE_DATE}" --token "${TOKEN}"

  if [[ "${CANONICAL_DUAL_WRITE_ENABLED:-0}" == "1" ]]; then
    docker run --rm \
      --env-file "${ENV_FILE}" \
      -e SCRAPE_RUN_ID="${RUN_ID}" \
      -e SCRAPE_CYCLE_DATE="${CYCLE_DATE}" \
      -e SCRAPE_FENCING_TOKEN="${TOKEN}" \
      -v "${RUN_DIR}/merged/Outputs:/app/ScrapDB/Outputs:ro" \
      -v "${RUN_DIR}/merged/RawRuns:/app/ScrapDB/RawRuns" \
      -v "${RUN_DIR}/merged/RunLogs:/app/ScrapDB/RunLogs" \
      "${IMAGE}" python -m ScrapDB.raw_offer \
        --input ScrapDB/Outputs \
        --output ScrapDB/RawRuns \
        --run-id "${RUN_ID}"

    docker run --rm \
      --env-file "${ENV_FILE}" \
      -e SCRAPE_RUN_ID="${RUN_ID}" \
      -e SCRAPE_CYCLE_DATE="${CYCLE_DATE}" \
      -e SCRAPE_FENCING_TOKEN="${TOKEN}" \
      -v "${RUN_DIR}/merged/RawRuns:/app/ScrapDB/RawRuns:ro" \
      -v "${RUN_DIR}/merged/RunLogs:/app/ScrapDB/RunLogs" \
      "${IMAGE}" python -m ScrapDB.canonical_backfill \
        --apply \
        --raw-offers "ScrapDB/RawRuns/${RUN_ID}.ndjson.gz" \
        --raw-offers-only \
        --dual-write \
        --categories "${CANONICAL_CATEGORIES:-CPU,GPU,Motherboard,Memory,Storage,PowerSupply,Case,CPUCooler}" \
        --compare \
        --checkpoint "ScrapDB/RunLogs/canonical-${RUN_ID}.checkpoint.json" \
        --comparison-report "ScrapDB/RunLogs/canonical-${RUN_ID}.comparison.json"
  fi

  coordinator assert --date "${CYCLE_DATE}" --token "${TOKEN}"
  docker run --rm \
    --env-file "${ENV_FILE}" \
    -e SCRAPE_CYCLE_DATE="${CYCLE_DATE}" \
    -e SCRAPE_FENCING_TOKEN="${TOKEN}" \
    -e SCRAPE_RUN_ID="${RUN_ID}" \
    -v "${RELEASE_ROOT}:/app:ro" \
    "${IMAGE}" python -m ScrapDB.database_maintenance post-run

  coordinator finalize-manifest \
    --date "${CYCLE_DATE}" --token "${TOKEN}" \
    --manifest /app/ScrapDB/RunLogs/retry-manifest.json
fi

if [[ -n "${HEARTBEAT_PID}" ]]; then
  kill "${HEARTBEAT_PID}" 2>/dev/null || true
fi
if [[ -d /srv/armatupc/specdb-ai/imports ]]; then
  jq --arg run_id "${RUN_ID}" --arg cycle_date "${CYCLE_DATE}" \
    --arg origin home --argjson publish_enabled "${PUBLISH_ENABLED}" \
    '. + {localRunId:$run_id,cycleDate:$cycle_date,origin:$origin,publishEnabled:($publish_enabled == 1)}' \
    "${RUN_DIR}/merged/RunLogs/retry-manifest.json" \
    > /srv/armatupc/specdb-ai/imports/scrape-status.json.tmp
  mv /srv/armatupc/specdb-ai/imports/scrape-status.json.tmp \
    /srv/armatupc/specdb-ai/imports/scrape-status.json
fi
trap - ERR
echo "Local daily completed: ${RUN_DIR}"

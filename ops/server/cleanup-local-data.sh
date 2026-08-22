#!/usr/bin/env bash
set -Eeuo pipefail

while IFS= read -r -d '' run_dir; do
  manifest="${run_dir}/merged/RunLogs/retry-manifest.json"
  retention_days=14
  if [[ ! -f "${manifest}" ]] || jq -e \
    '((.failed_scrapers // []) | length) > 0 or ((.partial_scrapers // []) | length) > 0' \
    "${manifest}" >/dev/null 2>&1; then
    retention_days=90
  fi

  if find "${run_dir}" -maxdepth 0 -mtime "+${retention_days}" -print -quit | grep -q .; then
    rm -rf -- "${run_dir}"
  fi
done < <(find /srv/armatupc/scraper/artifacts -mindepth 1 -maxdepth 1 -type d -print0)
find /srv/armatupc/scraper/logs -type f -mtime +30 -delete
find /srv/armatupc/specdb-ai/documents -type f -mtime +30 -delete
find /srv/armatupc/specdb-ai/backups -type f -name 'daily-*' -mtime +14 -delete
find /srv/armatupc/specdb-ai/backups -type f -name 'weekly-*' -mtime +56 -delete

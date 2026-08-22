#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GIT=(git -c "safe.directory=${REPO_ROOT}" -C "${REPO_ROOT}")
SHA="$("${GIT[@]}" rev-parse --short=12 HEAD 2>/dev/null || date -u +%Y%m%d%H%M)"
if [[ -n "$("${GIT[@]}" status --porcelain 2>/dev/null || true)" ]]; then
  TREE_SUFFIX="dirty-$(
    {
      "${GIT[@]}" diff --binary HEAD
      "${GIT[@]}" ls-files --others --exclude-standard -z \
        | while IFS= read -r -d '' path; do
            printf '%s\0' "${path}"
            sha256sum "${REPO_ROOT}/${path}"
          done
    } | sha256sum | cut -c1-10
  )"
else
  TREE_SUFFIX="clean"
fi
RELEASE_ID="${SHA}-${TREE_SUFFIX}"
RELEASE_DIR="/srv/armatupc/scraper/releases/${RELEASE_ID}"
IMAGE="armatupc-scraper:${RELEASE_ID}"

if [[ ${EUID} -ne 0 ]]; then
  echo "Run with sudo so Docker and the atomic runtime link remain root-managed." >&2
  exit 1
fi

docker build -f "${REPO_ROOT}/ops/server/Dockerfile.scraper" -t "${IMAGE}" "${REPO_ROOT}"
docker run --rm "${IMAGE}" python -m unittest discover -s tests -p 'test_*.py'
docker tag "${IMAGE}" armatupc-scraper:current

rm -rf "${RELEASE_DIR}"
mkdir -p "${RELEASE_DIR}"
tar -C "${REPO_ROOT}" \
  --exclude=.git \
  --exclude=ScrapDB/Outputs \
  --exclude=ScrapDB/RunLogs \
  --exclude=ScrapDB/RawRuns \
  --exclude='__pycache__' \
  -cf - . | tar -x -C "${RELEASE_DIR}"
ln -sfn "${RELEASE_DIR}" /srv/armatupc/scraper/current.next
mv -Tf /srv/armatupc/scraper/current.next /srv/armatupc/scraper/current
echo "Deployed ${RELEASE_ID} to ${RELEASE_DIR}"

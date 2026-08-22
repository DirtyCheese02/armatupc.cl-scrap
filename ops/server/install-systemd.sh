#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run this script with sudo." >&2
  exit 1
fi

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/systemd" && pwd)"
install -m 0644 "${SOURCE_DIR}"/*.service "${SOURCE_DIR}"/*.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now armatupc-maintenance.timer armatupc-scrape-daily.timer
systemctl list-timers 'armatupc-*' --no-pager

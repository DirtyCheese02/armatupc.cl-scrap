#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run this script with sudo." >&2
  exit 1
fi

TARGET_USER="${TARGET_USER:-moises}"
TARGET_HOME="$(getent passwd "${TARGET_USER}" | cut -d: -f6)"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y \
  ca-certificates curl gnupg git jq unzip tmux htop nvtop ufw fail2ban \
  build-essential python3 python3-venv python3-pip sqlite3 lsb-release \
  ubuntu-drivers-common

# Docker Engine official repository.
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
. /etc/os-release
cat >/etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${VERSION_CODENAME}
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF

# NVIDIA Container Toolkit official repository.
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
  | gpg --batch --yes --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
curl -fsSL https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
  | sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#' \
  >/etc/apt/sources.list.d/nvidia-container-toolkit.list

apt-get update
apt-get install -y \
  docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin \
  nvidia-container-toolkit

nvidia-ctk runtime configure --runtime=docker
systemctl enable --now docker containerd ssh fail2ban
systemctl restart docker

# Tailscale official installer adds the matching Ubuntu repository.
if ! command -v tailscale >/dev/null 2>&1; then
  tailscale_installer="$(mktemp)"
  curl -fsSL https://tailscale.com/install.sh -o "${tailscale_installer}"
  bash "${tailscale_installer}"
  rm -f "${tailscale_installer}"
fi
systemctl enable --now tailscaled

# Private runtime layout. Development repositories live in the user's home.
install -d -o "${TARGET_USER}" -g "${TARGET_USER}" \
  /srv/armatupc/scraper/releases \
  /srv/armatupc/scraper/artifacts \
  /srv/armatupc/scraper/logs \
  /srv/armatupc/specdb-ai/data \
  /srv/armatupc/specdb-ai/documents \
  /srv/armatupc/specdb-ai/exports \
  /srv/armatupc/specdb-ai/imports \
  /srv/armatupc/specdb-ai/backups \
  /srv/armatupc/models \
  /srv/armatupc/docker \
  "${TARGET_HOME}/src"
install -d -m 0700 -o root -g root /etc/armatupc/credentials

# Bound journald disk usage so application logs cannot fill the root volume.
install -d /etc/systemd/journald.conf.d
cat >/etc/systemd/journald.conf.d/armatupc-limits.conf <<'EOF'
[Journal]
SystemMaxUse=1G
RuntimeMaxUse=256M
MaxRetentionSec=30day
EOF
systemctl restart systemd-journald

# Keep LAN SSH during commissioning; services themselves remain private.
ufw default deny incoming
ufw default allow outgoing
ufw allow from 192.168.0.0/24 to any port 22 proto tcp comment 'commissioning LAN SSH'
ufw allow in on tailscale0
ufw --force enable

# Install the recommended server/compute driver. A reboot is required before
# validating Docker GPU access.
ubuntu-drivers install --gpgpu

# Codex is installed for the normal account, never for root.
sudo -u "${TARGET_USER}" env HOME="${TARGET_HOME}" bash -lc \
  'curl -fsSL https://chatgpt.com/codex/install.sh | sh'

echo
echo "Host bootstrap complete. Next actions:"
echo "  1. Reboot: sudo reboot"
echo "  2. Validate: nvidia-smi"
echo "  3. Authorize Tailscale: sudo tailscale up --ssh"
echo "  4. Authenticate Codex as ${TARGET_USER}: codex login --device-auth"

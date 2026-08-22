# ArmaTuPC home node

This directory contains the reproducible host and runtime configuration for
the private ArmaTuPC node. Production credentials never belong in this repo.

## Bootstrap

```bash
sudo TARGET_USER=moises bash ops/server/bootstrap-host.sh
sudo reboot
```

After reboot, validate `nvidia-smi`, authorize Tailscale with
`sudo tailscale up --ssh`, and authenticate Codex as the normal user with
`codex login --device-auth`.

## Security invariants

- Do not add the normal account to the `docker` group.
- Do not publish dashboard, SearXNG, llama.cpp, or Docker ports publicly.
- Store runtime credentials below `/etc/armatupc/credentials` with mode 0600.
- Local scrape releases are immutable directories below
  `/srv/armatupc/scraper/releases`; `current` is only an atomic symlink.
- The local scraper starts in shadow mode and cannot publish until the
  database lease migration and rollout gates have been completed.

## Shadow rollout

The installed timer runs at 05:00 in `America/Santiago`. Its service pins
`PUBLISH_ENABLED=0`, so it only creates local artifacts during the initial
seven-day comparison window:

```bash
systemctl list-timers 'armatupc-*'
journalctl -u armatupc-scrape-daily.service
```

Normal artifacts live for 14 days. Runs with failed or partial scrapers are
retained for 90 days. Enabling publication requires a separate credential
installation and service override after the rollout gate is approved.

Retry only the failed and partial scrapers from the latest completed local
daily without publishing anything:

```bash
sudo bash /srv/armatupc/scraper/current/ops/server/run-local-retry.sh
```

Use `SCRAPER_INCLUDE_OVERRIDE` to validate a selected subset without rerunning
healthy stores. The retry always creates a separate artifact directory and
never matches or publishes products. Short names such as `Winpy` are
normalized to `Scrap_Winpy.py`; the command aborts instead of reporting a
false success if the runner discovers zero scrapers.

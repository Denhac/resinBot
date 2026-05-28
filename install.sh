#!/usr/bin/env bash
set -euo pipefail

# Installs the resinBot systemd services (resinbot-server + resinbot-bot).
#
#   sudo ./install.sh
#
# Prompts for the service account to run as (default: resinbot) and creates it
# as a system account if it doesn't exist. Skip the prompt by presetting the user:
#   sudo SERVICE_USER=youruser ./install.sh

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_USER="resinbot"
ENV_DIR="/etc/resinbot"
ENV_FILE="$ENV_DIR/resinbot.env"
UNIT_DIR="/etc/systemd/system"

if [[ $EUID -ne 0 ]]; then
  echo "Run as root: sudo ./install.sh" >&2
  exit 1
fi

if [[ ! -x "$DIR/venv/bin/python3" ]]; then
  echo "No venv at $DIR/venv. Create it first:" >&2
  echo "  python3 -m venv venv && ./venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

# Determine the service account (prompt unless SERVICE_USER is preset).
if [[ -z "${SERVICE_USER:-}" ]]; then
  if [[ -t 0 ]]; then
    read -rp "Service account to run resinBot as [$DEFAULT_USER]: " SERVICE_USER || true
  fi
  SERVICE_USER="${SERVICE_USER:-$DEFAULT_USER}"
fi

# Create it as a system account if it doesn't already exist: no login, no password,
# no home (HOME=/nonexistent, the Debian convention for daemon accounts).
if id "$SERVICE_USER" &>/dev/null; then
  echo "Using existing account: $SERVICE_USER"
else
  echo "Creating system account: $SERVICE_USER"
  useradd --system --no-create-home --home-dir /nonexistent \
    --shell /usr/sbin/nologin --comment "resinBot service account" "$SERVICE_USER"
fi

# The service runs from $DIR, so the account must own/read it (and the venv).
chown -R "$SERVICE_USER" "$DIR"
if ! runuser -u "$SERVICE_USER" -- test -r "$DIR/printer_server.py"; then
  echo "WARNING: $SERVICE_USER cannot read $DIR — likely a parent-directory permission." >&2
  echo "         Move the app to a neutral path (e.g. /opt/resinbot) or loosen the path perms." >&2
fi

# Secrets file (root-only). systemd reads it as root before dropping to $SERVICE_USER.
install -d -m 755 "$ENV_DIR"
if [[ -f "$ENV_FILE" ]]; then
  echo "Keeping existing $ENV_FILE"
else
  install -m 600 "$DIR/systemd/resinbot.env.example" "$ENV_FILE"
  echo "Created $ENV_FILE — edit it and add your Slack tokens before starting the bot."
fi

# Raise the UDP receive-buffer ceiling so ffmpeg's large -buffer_size takes effect.
# The default (~180 KB) is smaller than a 1080p H.264 keyframe burst, so slices get
# dropped on the slow single-core Pi and screenshots come through streaked.
SYSCTL_FILE="/etc/sysctl.d/99-resinbot.conf"
echo "net.core.rmem_max = 8388608" > "$SYSCTL_FILE"
sysctl -w net.core.rmem_max=8388608 >/dev/null
echo "Set net.core.rmem_max=8388608 ($SYSCTL_FILE)"

# Render and install unit files with the resolved user + directory.
for unit in resinbot-server resinbot-bot; do
  sed -e "s|__USER__|$SERVICE_USER|g" -e "s|__DIR__|$DIR|g" \
    "$DIR/systemd/$unit.service" > "$UNIT_DIR/$unit.service"
  echo "Installed $UNIT_DIR/$unit.service (User=$SERVICE_USER)"
done

systemctl daemon-reload
systemctl enable resinbot-server.service resinbot-bot.service

echo
echo "Enabled on boot. Once $ENV_FILE has real tokens, start now with:"
echo "  sudo systemctl start resinbot-server resinbot-bot"
echo "Follow logs with:"
echo "  journalctl -u resinbot-server -u resinbot-bot -f"

#!/usr/bin/env bash
set -euo pipefail

# Deploys this checkout to the resinBot Pi over SSH, installing over the existing copy,
# then restarts the services and waits for them to come back up.
#
#   ./deploy.sh              # tracked files as they are on disk (uncommitted edits included)
#   ./deploy.sh <ref>        # a commit/branch/tag instead, e.g. roll back: ./deploy.sh HEAD~1
#   ./deploy.sh -n [<ref>]   # dry run: show what would change on the Pi, touch nothing
#
# Only git-tracked files are shipped, never venv/, __pycache__/ or the git-ignored
# systemd/resinbot.env that holds real tokens. The install dir is mirrored (files not in
# the release are deleted) except venv/, which is reused and only re-pip'd when
# requirements.txt changes. /etc/resinbot is never written: install.sh keeps an existing
# resinbot.env as-is, and a release needing a setting the file lacks is refused.
#
# The install dir and service account are read from the installed units. Deploys to the
# ssh host "resinBot", which must log in as root; override with DEPLOY_HOST=root@host.
# A fresh Pi needs ffmpeg, rsync and python3-venv from apt first.

HOST="${DEPLOY_HOST:-resinBot}"

# Runs on the Pi, with the release tarball on stdin.
remote() {
  set -euo pipefail
  local revision=$1 dry_run=$2
  local env_file=/etc/resinbot/resinbot.env
  local units=(resinbot-server.service resinbot-bot.service)
  if [[ $EUID -ne 0 ]]; then
    echo "Must log in to the Pi as root." >&2
    exit 1
  fi

  # Unpack into a scratch dir; the install dir isn't touched until the release checks out.
  local stage f
  stage=$(mktemp -d)
  trap "rm -rf '$stage'" EXIT
  chmod 755 "$stage"  # rsync -p copies this onto the install dir itself
  tar -x -f - -C "$stage"
  exec </dev/null
  for f in install.sh requirements.txt printer_server.py slack_bot.py systemd/resinbot.env.example; do
    if [[ ! -f $stage/$f ]]; then
      echo "Release has no $f; refusing to deploy it." >&2
      exit 1
    fi
  done
  echo "$revision" > "$stage/REVISION"

  # Install over the existing copy: take its dir and account from the installed unit, so a
  # deploy can't fork a second install. A fresh host gets install.sh's defaults.
  local dir user
  dir=$(systemctl show -p WorkingDirectory --value "${units[0]}")
  user=$(systemctl show -p User --value "${units[0]}")
  dir=${dir:-/opt/resinBot} user=${user:-resinbot}
  if [[ -n $(ls -A "$dir" 2>/dev/null) && ! -f $dir/printer_server.py ]]; then
    echo "$dir doesn't look like a resinBot install; refusing to overwrite it." >&2
    exit 1
  fi
  echo "Installing into $dir as $user, replacing: $(cat "$dir/REVISION" 2>/dev/null || echo unknown)"

  # install.sh only creates resinbot.env when it's missing, so a new required setting would
  # never reach the Pi. Refuse such a release rather than start it without the setting.
  if [[ -f $env_file ]]; then
    local missing
    missing=$(awk -F= 'NR == FNR { have[$1]; next }
      /^[A-Za-z_][A-Za-z0-9_]*=/ && !($1 in have) { print $1 }' "$env_file" "$stage/systemd/resinbot.env.example")
    if [[ -n $missing ]]; then
      echo "This release needs settings missing from $env_file:" $missing >&2
      echo "Add them (see systemd/resinbot.env.example), then re-run." >&2
      exit 1
    fi
  fi

  # Reuse the venv (rebuilding is slow on the Pi Zero): pip only runs when requirements.txt
  # changed, and before the code swap so a failed install leaves the running version intact.
  local need_pip=
  if [[ ! -x $dir/venv/bin/python3 ]] || ! cmp -s "$stage/requirements.txt" "$dir/requirements.txt"; then
    need_pip=1
  fi
  if [[ -n $need_pip && -z $dry_run ]]; then
    if [[ ! -x $dir/venv/bin/python3 ]]; then
      mkdir -p "$dir"
      python3 -m venv "$dir/venv"
    fi
    echo "Installing requirements.txt into $dir/venv..."
    "$dir/venv/bin/pip" install -q --disable-pip-version-check -r "$stage/requirements.txt"
  fi

  local changes
  changes=$(rsync -rlpc --delete --exclude=/venv/ --out-format='  %i %n%L' ${dry_run:+--dry-run} "$stage/" "$dir/")
  echo "Changes${dry_run:+ (dry run, nothing written)}:"
  echo "${changes:-  (none)}"
  if [[ -n $dry_run ]]; then
    [[ -z $need_pip ]] || echo "Would pip install requirements.txt into $dir/venv"
    echo "Would run install.sh (SERVICE_USER=$user) and restart ${units[*]}"
    exit 0
  fi

  echo "Running install.sh:"
  SERVICE_USER=$user "$dir/install.sh" 2>&1 | sed 's/^/  /'
  if grep -q replace-me "$env_file"; then
    echo "Not starting: put the Slack tokens in $env_file, then: systemctl restart ${units[*]}"
    exit 0
  fi

  local since bot_id http=000 start=$SECONDS
  since=$(date +%s)
  echo "Restarting ${units[*]}..."
  systemctl restart "${units[@]}"
  bot_id=$(systemctl show -p InvocationID --value "${units[1]}")

  # The single-core Pi Zero takes ~40s before the server answers and ~75s before the bot
  # connects to Slack. A crash shows as a unit leaving "active": Restart=on-failure holds it
  # in auto-restart for 5s, longer than one poll. (is-active passes if *any* unit is active,
  # hence one call per unit.)
  until [[ $http == [1-9]* && $(journalctl _SYSTEMD_INVOCATION_ID="$bot_id" -o cat) == *"Bolt app is running"* ]]; do
    if (( SECONDS - start > 240 )) || ! systemctl is-active -q "${units[0]}" || ! systemctl is-active -q "${units[1]}"; then
      echo "Services did not come up cleanly. Logs since the restart:" >&2
      journalctl -u "${units[0]}" -u "${units[1]}" --since "@$since" --no-pager >&2
      exit 1
    fi
    sleep 3
    [[ $http == [1-9]* ]] || http=$(curl -s -o /dev/null -m 5 -w '%{http_code}' http://127.0.0.1:5000/status || true)
  done
  [[ $http != 503 ]] || http+=", no printers discovered"
  echo "Deployed $revision"
  echo "Server answering (HTTP $http), bot connected to Slack, $(( SECONDS - start ))s after restart."
}

usage() {
  echo "Usage: $0 [-n|--dry-run] [<git ref>]" >&2
  exit 2
}

dry_run= ref=
for arg in "$@"; do
  case $arg in
    -n|--dry-run) dry_run=1 ;;
    -*) usage ;;
    *) [[ -z $ref ]] || usage; ref=$arg ;;
  esac
done

cd "$(dirname "${BASH_SOURCE[0]}")"  # git archive packs the current dir, so run from the repo root
if [[ -n $ref ]]; then
  tree=$(git rev-parse --verify --quiet "$ref^{commit}") || { echo "Not a commit: $ref" >&2; exit 1; }
  rev=$(git rev-parse --short "$tree")
else
  # Snapshot tracked files as they are on disk without touching the index or stash list;
  # prints nothing when there are no uncommitted changes.
  tree=$(git stash create)
  rev=$(git rev-parse --short HEAD)${tree:+-dirty}
  tree=${tree:-HEAD}
  untracked=$(git ls-files --others --exclude-standard)
  if [[ -n $untracked ]]; then
    echo "Not deploying untracked files:" >&2
    sed 's/^/  /' <<<"$untracked" >&2
  fi
fi
revision="$rev $(git log -1 --format=%s "${ref:-HEAD}")"

echo "Deploying $revision to $HOST${dry_run:+ (dry run)}"
git -c tar.umask=022 archive --format=tar "$tree" |
  ssh -o ConnectTimeout=15 "$HOST" "$(declare -f remote); remote $(printf '%q ' "$revision" "$dry_run")"

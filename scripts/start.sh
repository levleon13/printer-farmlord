#!/usr/bin/env bash
# Installs on first run, then starts the dashboard. With no printers configured, it opens a setup page in the browser.
set -euo pipefail
cd "$(dirname "$0")/.."
STEP="starting"
report() {
  # Print a prefilled GitHub issue link. Nothing is sent unless the user opens it and clicks Submit.
  local sys q
  sys="$(uname -srm) · $(python3 --version 2>&1 || echo 'no python3')"
  q="template=problem.yml&title=Install+failed&details=Failed+while+${STEP// /+}&system=${sys// /+}"
  printf '\nSomething went wrong while %s.\nReport it (opens GitHub; nothing private is included):\nhttps://github.com/levleon13/printer-farmlord/issues/new?%s\n' "$STEP" "$q" >&2
}
trap report ERR
if ! .venv/bin/python -c "import paho.mqtt" >/dev/null 2>&1; then
  printf 'First run: installing Printer Farmlord (about a minute)...\n'
  STEP="checking for Python 3"; command -v python3 >/dev/null
  STEP="creating the Python environment"; python3 -m venv .venv
  STEP="installing dependencies"
  .venv/bin/python -m pip install --quiet --upgrade pip
  .venv/bin/python -m pip install --quiet -r requirements.txt
fi
command -v ffmpeg >/dev/null 2>&1 || printf 'Tip: install ffmpeg to see printer cameras.\n' >&2
STEP="preparing the data folder"; mkdir -p .runtime && chmod 700 .runtime
# Android/Termux: keep running while the screen is off.
if command -v termux-wake-lock >/dev/null 2>&1; then termux-wake-lock; fi
trap - ERR
export AMS_TRACKER_DIR="$PWD/.runtime"
export AMS_STANDBY="${AMS_STANDBY:-0}"
exec .venv/bin/python -u ams_tracker.py run "$@"

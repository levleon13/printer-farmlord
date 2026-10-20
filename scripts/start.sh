#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ ! -f .runtime/config.json ]; then
  printf 'Run ./scripts/install.sh first.\n' >&2
  exit 1
fi
if [ ! -x .venv/bin/python ]; then
  printf 'Python environment missing. Run ./scripts/install.sh first.\n' >&2
  exit 1
fi
export AMS_TRACKER_DIR="$PWD/.runtime"
export AMS_STANDBY="${AMS_STANDBY:-0}"
exec .venv/bin/python -u ams_tracker.py run "$@"

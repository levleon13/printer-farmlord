#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
if ! command -v ffmpeg >/dev/null 2>&1; then
  printf 'Camera feeds require ffmpeg. Install it with your OS package manager.\n' >&2
fi
if [ ! -e .runtime/config.json ]; then
  .venv/bin/python scripts/setup.py
fi
printf '\nReady. Run ./scripts/start.sh\n'

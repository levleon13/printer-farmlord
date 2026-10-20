#!/usr/bin/env bash
# Optional: let a Mac/Linux OrcaSlicer reach printers behind an SSH-accessible host.
set -euo pipefail
: "${PRINT_ROOM_SSH_HOST:?Set PRINT_ROOM_SSH_HOST to your SSH host alias first}"
if ! command -v sshuttle >/dev/null 2>&1; then
  printf 'Install sshuttle on this computer first.\n' >&2
  exit 1
fi
mapfile_compat() {
  while IFS= read -r line; do ROUTES+=("$line"); done
}
ROUTES=()
mapfile_compat < <(ssh "$PRINT_ROOM_SSH_HOST" 'python3 -' <<'PY'
import ipaddress
import json
from pathlib import Path
config = Path.home() / 'print-room-kit' / '.runtime' / 'config.json'
for printer in json.loads(config.read_text())['printers']:
    address = ipaddress.ip_address(printer['ip'])
    if address.version != 4 or not address.is_private:
        raise SystemExit('Only private IPv4 printer addresses may be tunneled')
    print(f'{address}/32')
PY
)
if [ "${#ROUTES[@]}" -eq 0 ]; then
  printf 'No remote printers found.\n' >&2
  exit 1
fi
printf 'Routing %s printer(s) through %s. Keep this window open.\n' "${#ROUTES[@]}" "$PRINT_ROOM_SSH_HOST"
PYTHON_ARGS=()
if [ -n "${PRINT_ROOM_SSHUTTLE_PYTHON:-}" ]; then
  PYTHON_ARGS=(--python "$PRINT_ROOM_SSHUTTLE_PYTHON")
fi
exec sshuttle "${PYTHON_ARGS[@]}" --remote "$PRINT_ROOM_SSH_HOST" "${ROUTES[@]}"

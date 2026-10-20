# Printer Farmlord

Local Bambu H2/AMS print-room dashboard, drying tracker and controls, plus the edited OrcaSlicer profiles and the G-code bridge-anchor post-processor. The dashboard supports multiple printers, RFID spool tracking, drying requests, cameras, stock, alerts and print controls. Nothing in this repository connects to the original print room: add your own LAN credentials during setup.

## Fast setup

On the machine that can reach your printers (macOS, Linux, or Termux with Python 3 and `ffmpeg`):

```sh
git clone <YOUR_PRIVATE_REPO_URL>
cd printer-farmlord
./scripts/install.sh
./scripts/start.sh
```

The installer creates a virtual environment, installs `paho-mqtt`, and prompts for each printer's **label, LAN IP, serial, and LAN access code**. It writes those only to ignored `.runtime/config.json`. On each printer, enable LAN Only/Developer Mode and copy the details from its network screen. Open `http://127.0.0.1:8765` on the host. To open the dashboard on another device, choose LAN access during setup, set a password, and use the host's LAN IP on port 8765. A firewall may need to allow that port.

For Termux, first run `pkg install python ffmpeg`, then the same commands. To start on boot, install Termux:Boot and add a boot script that runs `cd /path/to/printer-farmlord && ./scripts/start.sh`; keep the path local to that phone. For any always-on host, use your service manager to supervise `scripts/start.sh`. `AMS_STANDBY=1 ./scripts/start.sh` starts telemetry-only mode; the normal start enables controls. Test the first printer before adding the rest.

Stop with Control-C. To add or edit printers later, use dashboard Settings. `.runtime/config.json` and every spool, log, recording and event file stay local. Back up `.runtime/` privately if you need to preserve history.

Optional store availability is off by default. To use it, add `stock_store_url` and a `stock_catalog` array to `.runtime/config.json`. Each catalog item needs `material`, six-digit `color`, numeric `product_id`, `sku`, and `expected_name`. Product and store IDs stay local. Restart the dashboard after changing the catalog.

## Orca profiles and print script

The repository contains the current edited 0.6 mm H2S/H2D machine, filament and process JSON profiles in `profiles/`. A separate 0.8 mm experimental filament profile is opt-in. The process profiles that use the bridge-anchor post-processor are listed in `profiles/bridge-fix-processes.json`.

Quit OrcaSlicer after saving your work. Locate its user profile directory (for example `~/Documents/OrcaSlicer/user/default` on a Mac with local presets). Preview, then install:

```sh
python3 scripts/install_orca_profiles.py --orca-user ~/Documents/OrcaSlicer/user/default --dry-run
python3 scripts/install_orca_profiles.py --orca-user ~/Documents/OrcaSlicer/user/default
```

The installer backs up colliding JSON files inside that user directory and writes the post-processing command with **your** Python and checkout paths. Reopen Orca. The experimental 0.8 mm profile requires `--include-experimental-0.8`; verify nozzle size and calibration before using it. Orca's stock profiles remain installed, so an AMS sync may still choose a stock filament: check the `- LL` preset before slicing. The AMS material picker and Orca slicer preset picker are separate catalogs.

The bridge-anchor script is `fix_bridge_anchor.py`. It modifies qualifying internal solid-infill loops in generated G-code. It requires relative extrusion and leaves unknown patterns untouched. Review sample slices and printer compatibility before relying on any imported profile or post-processor.

If Orca runs on a different network from the printers, set up SSH access to the dashboard host, install `sshuttle` locally, then run `PRINT_ROOM_SSH_HOST=your-ssh-alias ./scripts/connect_orca_remote.sh`. It reads the printer IP list from the remote checkout's private config. The remote checkout must be at `~/printer-farmlord`. For Termux, set `PRINT_ROOM_SSHUTTLE_PYTHON=/data/data/com.termux/files/usr/bin/python`. Keep the tunnel terminal open while using Orca.

## What is where

| Path | Purpose |
| --- | --- |
| `ams_tracker.py` | Dashboard, MQTT workers, drying, RFID, cameras, controls and inventory |
| `fix_bridge_anchor.py` | Orca G-code post-processing script |
| `profiles/` | Edited machine, filament and process JSON |
| `scripts/install.sh`, `scripts/start.sh` | Local runtime setup and launch |
| `scripts/setup.py` | Prompt for local printer credentials |
| `scripts/install_orca_profiles.py` | Profile import with backup and local path rewrite |
| `scripts/connect_orca_remote.sh` | Optional SSH route to remote printer LAN IPs |
| `scripts/privacy_check.py` | Prevent identifying data from entering a commit |
| `AGENTS.md` | Short instructions for a newly arrived coding agent |

## Safety and limits

Drying temperatures and timings are application defaults, not a guarantee for every spool. Confirm the spool maker's limits, AMS model, external power and current firmware. The printer's own safety interlocks take precedence; the dashboard does not bypass them. Camera feeds require `ffmpeg`; other dashboard features work without it. LAN credentials and dashboard password must never be committed. Run `python3 scripts/privacy_check.py` before each push.

This bundle is a portable snapshot of the active code and edited presets. It deliberately excludes live printer IDs, credentials, RFID tags, spool records, local paths, cloud metadata, screenshots, logs and prior working backups. It is not an export of printer firmware or Orca's built-in presets.

GitHub Actions repeats the privacy, syntax and Orca-import checks on each push and pull request. Keep the repository private even though these checks pass; print settings can still reveal workflow preferences.

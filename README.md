# Printer Farmlord

A self-hosted dashboard for a room full of Bambu Lab printers.

It talks to each printer directly over your LAN — no cloud account, no vendor
service in the middle. One page shows every machine: what it is printing, how
hot it is, which spools are loaded, how dry they are, and what needs attention.
From the same page you can start and stop AMS drying, preheat, move the
toolhead, change filament, watch the cameras and track stock.

Built for Bambu H2 series printers with AMS 2 Pro and AMS HT units. Other
models that speak the same LAN protocol may work, but are untested.

## What it does

**Drying.** Tracks how long every spool has spent above its drying temperature
and estimates how dry it is now, decaying over time with measured humidity.
Starts and stops AMS heaters on its own, respecting each material's safe
ceiling and the lowest common limit of everything in a unit. It reads the
printer's own refusal reasons rather than guessing, queues units when the
printer only has power for one heater, and tells you when a unit is wedged and
needs a power cycle instead of retrying forever.

**Spools.** Reads AMS RFID tags where they exist and lets you record what is in
a slot where they don't. Keeps a per-spool history of drying, exposure and
consumption that survives restarts and offline gaps.

**Printers.** Live status per machine, cameras, preheat and cool-down, print
pause/resume/stop, speed, lights, fans, manual jogging and homing. Decodes
Bambu's error and HMS codes into readable text.

**Attention.** Collects faults into one list you can acknowledge or snooze, so
a problem on one machine doesn't get lost among four.

## Requirements

- Python 3.9 or newer
- `ffmpeg` — only for camera feeds; everything else works without it
- A host on the same network as the printers: macOS, Linux, or Android via
  Termux. A spare phone works well and uses almost no power.
- Each printer in **LAN Only / Developer Mode**, which exposes the local MQTT
  interface this dashboard uses

## Install

```
git clone <this-repo-url>
cd printer-farmlord
./scripts/install.sh
./scripts/start.sh
```

`install.sh` creates a virtual environment, installs `paho-mqtt`, and then asks
for each printer's label, LAN IP, serial number and LAN access code. You will
find all three on the printer's own network screen.

Those details are written only to `.runtime/config.json`, which is gitignored
and never leaves the machine.

Then open **http://127.0.0.1:8765**.

On Termux, run `pkg install python ffmpeg` first, then the same commands.

Add one printer, confirm it connects, and only then add the rest. A wrong
access code looks the same as an unreachable printer, and it is much easier to
tell them apart with a single machine configured.

## Reaching it from other devices

By default the dashboard listens on localhost only. To open it from a phone or
another computer, choose LAN access during setup and set a password. It then
serves on your host's LAN address, port 8765. Your firewall may need to allow
that port.

The password is the only thing between your network and full control of the
printers. Treat it accordingly, and do not expose port 8765 to the internet.

## Keeping it running

`./scripts/start.sh` runs in the foreground; Control-C stops it.

For an always-on host, supervise that script with whatever your system uses —
`systemd` on Linux, `launchd` on macOS. On Android, install Termux:Boot and add
a boot script that changes into the checkout and runs `./scripts/start.sh`.

`AMS_STANDBY=1 ./scripts/start.sh` starts in telemetry-only mode: it watches and
records, but will not command the printers. Useful for a first run, or for a
second instance that should only observe.

## Optional: the bridge-anchor post-processor

`fix_bridge_anchor.py` is a standalone OrcaSlicer post-processing script,
independent of the dashboard.

On the layer below an internal bridge, Orca emits a closed loop of solid infill
that hugs the walls and prints at full speed over mostly air. This script finds
those loops and either slows them down or removes them.

To use it, open OrcaSlicer, go to **Others → Post-processing Scripts** and add:

```
/usr/bin/python3 /path/to/printer-farmlord/fix_bridge_anchor.py --mode slow
```

Use the full path to your own Python and your own checkout. `--mode slow`
retimes the loops and sets a fan override; `--mode delete` removes them
entirely.

It requires **relative extrusion**, which is Orca's default for Bambu printers,
and aborts untouched if it finds absolute extrusion. Detection is
shape-independent and deliberately conservative: anything it does not
positively recognise is left alone. Arc fitting (G2/G3) is handled.

Slice something familiar and compare the preview before trusting it on a long
print.

## Optional: slicing from a different network

If OrcaSlicer runs somewhere that cannot reach the printers directly, you can
route just the printer addresses through the dashboard host:

```
PRINT_ROOM_SSH_HOST=your-ssh-alias ./scripts/connect_orca_remote.sh
```

This needs SSH access to that host, `sshuttle` installed locally, and the
checkout at `~/printer-farmlord` on the far end. It reads the printer IPs from
the remote config. Keep the terminal open while you are slicing. On Termux, set
`PRINT_ROOM_SSHUTTLE_PYTHON` to the Termux Python path.

## Optional: stock tracking

Off by default. To enable it, add `stock_store_url` and a `stock_catalog` array
to `.runtime/config.json`. Each entry needs `material`, a six-digit `color`,
a numeric `product_id`, `sku` and `expected_name`. Restart afterwards.

## Privacy

Everything identifying stays in `.runtime/`, which is gitignored: credentials,
serials, spool records, RFID tags, logs, recordings and event history.

`scripts/privacy_check.py` scans for personal paths, private and CGNAT IP
ranges, emails, account UUIDs, printer serials and credential assignments. It
reports file and line numbers without echoing what it found, and fails closed.
Run it before every push:

```
python3 scripts/privacy_check.py
```

CI repeats that check, plus a syntax check, on every push and pull request.

If you fork this, back up `.runtime/` separately. It is the only copy of your
spool history.

## Safety

Drying temperatures and durations in this project are sensible defaults, not
guarantees for your filament. Confirm what your spool's manufacturer allows,
and what your AMS model supports with and without external power.

The printer's own interlocks always take precedence — this dashboard does not
and cannot bypass them. It will refuse to move the toolhead or operate the
filament cutter while a print is running, while the printer reports an
uncleared failure, or in the minutes after the printer boots and has not yet
established a position reference.

Nothing here is affiliated with or endorsed by Bambu Lab.

## Layout

| Path | Purpose |
| --- | --- |
| `ams_tracker.py` | The whole application: hub, per-printer MQTT workers, drying, RFID, cameras, controls, inventory, web UI |
| `fix_bridge_anchor.py` | Standalone Orca post-processing script |
| `scripts/install.sh` | Creates the virtualenv and runs setup |
| `scripts/setup.py` | Prompts for printer credentials, writes `.runtime/config.json` |
| `scripts/start.sh` | Launches the dashboard |
| `scripts/privacy_check.py` | Pre-commit scan for identifying data |
| `scripts/connect_orca_remote.sh` | Optional SSH route to the printer LAN |
| `.github/workflows/verify.yml` | CI: privacy and syntax checks |
| `AGENTS.md` | Orientation for a coding agent working on this repo |

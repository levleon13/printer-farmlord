# Agent handoff

This repo is the portable print-room system. Start with `README.md`, then run `python3 scripts/privacy_check.py` and `python3 -m py_compile ams_tracker.py fix_bridge_anchor.py scripts/*.py`. Never copy `.runtime/`, Orca `.info` files, logs, screenshots, RFID tags, printer serials, LAN access codes, personal paths or machine-specific SSH files into Git.

To set up: run `./scripts/install.sh`, enter each printer's LAN details locally, then `./scripts/start.sh`. The hub reads `.runtime/config.json` through `AMS_TRACKER_DIR`, spawns one worker per printer and serves port 8765. The central app is `ams_tracker.py`; avoid replacing it with old `work/` snapshots. Change code here, run syntax and privacy checks, test a local dashboard instance, then deploy to the host. Never test commands against a live printer without knowing its state.

For Orca, `profiles/` contains reviewed JSON only. Run `scripts/install_orca_profiles.py --dry-run` before import. The installer rewrites the bridge-fix command to this checkout and backs up collisions. Close Orca before writing its files. Preserve user edits and confirm nozzle size; the 0.8 profile is opt-in.

Before publishing, stage only an explicit allowlist and run `python3 scripts/privacy_check.py --staged`. Keep the remote private. Do not embed credentials in commits, examples, issue text, or tool output. If privacy check finds data, remove it before the first push and re-run the scan.

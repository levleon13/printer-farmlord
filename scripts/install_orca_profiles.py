#!/usr/bin/env python3
"""Install reviewed Orca JSON profiles with local post-processing paths."""

import argparse
import json
import pathlib
import shlex
import shutil
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROFILES = ROOT / "profiles"
PROCESS_WITH_BRIDGE_FIX = set(json.loads((PROFILES / "bridge-fix-processes.json").read_text()))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--orca-user", type=pathlib.Path, required=True,
                        help="OrcaSlicer user directory, e.g. ~/Documents/OrcaSlicer/user/default")
    parser.add_argument("--include-experimental-0.8", dest="include_experimental_0_8", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    target = args.orca_user.expanduser().resolve()
    backup = target / ("backup-before-printer-farmlord-" + time.strftime("%Y%m%d-%H%M%S"))
    kinds = ["machine", "filament", "process"]
    rows = []
    for kind in kinds:
        for source in sorted((PROFILES / kind).glob("*.json")):
            destination = target / kind / source.name
            data = json.loads(source.read_text())
            if kind == "process" and source.name in PROCESS_WITH_BRIDGE_FIX:
                data["post_process"] = [f"{shlex.quote(sys.executable)} {shlex.quote(str(ROOT / 'fix_bridge_anchor.py'))} --mode slow"]
            rows.append((destination, data))
    if args.include_experimental_0_8:
        for kind in ("filament", "process"):
            for source in sorted((PROFILES / "experimental-0.8" / kind).glob("*.json")):
                rows.append((target / kind / source.name, json.loads(source.read_text())))
    for destination, _ in rows:
        print(("Would install " if args.dry_run else "Installing ") + str(destination))
    if args.dry_run:
        return 0
    if not target.is_dir():
        parser.error("Orca user directory does not exist. Launch Orca once, then use the correct user directory.")
    for destination, data in rows:
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            saved = backup / destination.relative_to(target)
            saved.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(destination, saved)
        destination.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    print(f"Installed {len(rows)} profiles. Reopen OrcaSlicer. Backup: {backup if backup.exists() else 'none needed'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

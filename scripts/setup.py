#!/usr/bin/env python3
"""Interactive first-run configuration. Secrets only go to ignored .runtime/config.json."""

import getpass
import json
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUNTIME = ROOT / ".runtime"
CONFIG = RUNTIME / "config.json"


def ask(prompt, default=""):
    suffix = f" [{default}]" if default else ""
    return input(f"{prompt}{suffix}: ").strip() or default


def main():
    if CONFIG.exists():
        print(f"Existing {CONFIG} found. Edit it directly; setup will not overwrite it.")
        return 0
    print("Printer details are stored locally and ignored by Git.")
    printers = []
    while True:
        name = ask("Printer label (blank to finish)")
        if not name:
            break
        if name in {p["name"] for p in printers} or "/" in name or name in (".", ".."):
            print("Use a unique, simple label without slashes.")
            continue
        ip = ask("Printer LAN IP")
        serial = ask("Printer serial number")
        code = getpass.getpass("Printer LAN access code (hidden): ").strip()
        if not all((ip, serial, code)):
            print("All printer fields are required.")
            continue
        printers.append(dict(name=name, ip=ip, serial=serial, access_code=code))
    if not printers:
        print("No printers configured; nothing written.")
        return 1
    lan = ask("Expose dashboard on your LAN? yes/no", "no").lower() == "yes"
    password = getpass.getpass("Dashboard password (hidden): ").strip() if lan else ""
    if lan and not password:
        print("A password is required for LAN access.")
        return 1
    cfg = dict(printers=printers, lan_access=lan, lan_name="printroom")
    if password:
        cfg["auth"] = {"password": password}
    RUNTIME.mkdir(mode=0o700, exist_ok=True)
    old_umask = os.umask(0o077)
    try:
        with CONFIG.open("x") as out:
            json.dump(cfg, out, indent=2)
            out.write("\n")
    finally:
        os.umask(old_umask)
    CONFIG.chmod(0o600)
    print(f"Configured {len(printers)} printer(s). Run ./scripts/start.sh")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Fail closed on common personal or machine-specific data in publishable files."""

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
PATTERNS = {
    "personal path": re.compile(r"/(?:Users|home)/[^/\s\"']+"),
    "private IP": re.compile(r"\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3})\b"),
    "email": re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I),
    "account UUID": re.compile(r"\b[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\b", re.I),
    # Bambu serials: 15-16 uppercase letters and digits, e.g. 01P00A...
    "possible printer serial": re.compile(r"\b(?=[0-9A-Z]*\d)(?=[0-9A-Z]*[A-Z])[0-9A-Z]{15,16}\b"),
    # A credential key followed by a quoted value, in JSON, Python, YAML or shell style.
    "credential assignment": re.compile(
        r"(?:access_code|password|token|secret)[\"']?\s*[:=]\s*[\"'](?!REPLACE_|[\"'])", re.I),
}


def files(staged):
    if staged:
        p = subprocess.run(["git", "diff", "--cached", "--name-only", "--diff-filter=d", "-z"], cwd=ROOT,
                           capture_output=True, check=True)
        return [ROOT / x.decode() for x in p.stdout.split(b"\0") if x]
    # Everything Git would publish: tracked files plus new files that are not ignored.
    p = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT,
                       capture_output=True, check=True)
    return [ROOT / x.decode() for x in p.stdout.split(b"\0") if x and (ROOT / x.decode()).is_file()]


def read(path, staged):
    if not staged:
        return path.read_text()
    # Scan what will be committed, not the working tree.
    p = subprocess.run(["git", "show", ":" + path.relative_to(ROOT).as_posix()], cwd=ROOT,
                       capture_output=True, check=True)
    return p.stdout.decode()


def reviewed_image(path):
    # README art and screenshots can't be text-scanned; they are checked by eye before they are added.
    rel = path.relative_to(ROOT).as_posix()
    return rel.startswith(("docs/img/", "docs/screenshots/")) and rel.endswith(".webp")


def main():
    bad = []
    staged = "--staged" in sys.argv
    for path in files(staged):
        if reviewed_image(path):
            continue
        try:
            content = read(path, staged)
        except (OSError, UnicodeError, subprocess.CalledProcessError):
            bad.append((path, 0, "unreadable or binary"))
            continue
        for number, line in enumerate(content.splitlines(), 1):
            for label, pattern in PATTERNS.items():
                if pattern.search(line):
                    # Report location only; never echo a suspected secret.
                    bad.append((path, number, label))
    for path, number, label in bad:
        print(f"{path.relative_to(ROOT)}:{number}: {label}")
    print(f"Scanned {len(files('--staged' in sys.argv))} files; {len(bad)} potential leaks")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

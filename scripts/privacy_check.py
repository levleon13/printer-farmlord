#!/usr/bin/env python3
"""Fail closed on common personal or machine-specific data in publishable files."""

import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
PATTERNS = {
    "personal path": re.compile(r"/(?:Users|home)/[^/\s\"']+"),
    "private IP": re.compile(r"\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3})\b"),
    "email": re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I),
    "account UUID": re.compile(r"\b[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\b", re.I),
    "possible printer serial": re.compile(r"\b[A-F0-9]{15,16}\b", re.I),
    "credential assignment": re.compile(r'"(?:access_code|password|token)"\s*:\s*"(?!REPLACE_|\")')
}


def files(staged):
    if staged:
        p = subprocess.run(["git", "diff", "--cached", "--name-only", "-z"], cwd=ROOT,
                           capture_output=True, check=True)
        return [ROOT / x.decode() for x in p.stdout.split(b"\0") if x]
    return [p for p in ROOT.rglob("*") if p.is_file() and not any(
        part in {".git", ".venv", ".runtime", "__pycache__"} for part in p.relative_to(ROOT).parts)]


def main():
    bad = []
    for path in files("--staged" in sys.argv):
        try:
            content = path.read_text()
        except (OSError, UnicodeError):
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

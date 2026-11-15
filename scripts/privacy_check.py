#!/usr/bin/env python3
"""Fail closed on personal or office-specific data before anything reaches Git.

  privacy_check.py              every file Git would publish
  privacy_check.py --staged     what the next commit contains (pre-commit hook)
  privacy_check.py --history [REV_RANGE]
                                added lines and author/committer emails of every commit in the range
                                (default: all refs) — used by the pre-push hook

Besides the generic patterns below, it rejects every exact string in a private denylist that never
lives in the repo: $PRIVACY_DENYLIST or ~/.config/printer-farmlord/privacy-denylist.txt (one entry per
line: printer serials, access codes, office IPs and host names, people's names, Wi-Fi names...).
Findings print as file:line: label only; a suspected secret is never echoed.
"""

import os
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
    "Tailscale host name": re.compile(r"\b[\w-]+\.[\w-]+\.ts\.net\b", re.I),
    "MAC address": re.compile(r"\b[0-9a-f]{2}(?::[0-9a-f]{2}){5}\b", re.I),
    "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "API token": re.compile(r"\b(?:tskey-[\w-]{10,}|gh[pousr]_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9_-]{20,}|xox[abpr]-[\w-]{10,})"),
    "credential assignment": re.compile(
        r"(?:access_code|password|token|secret)[\"']?\s*[:=]\s*[\"'](?!REPLACE_|[\"'])", re.I),
}


ALLOWED_EMAIL = re.compile(r"@(?:users\.noreply\.github\.com|github\.com)$", re.I)


def denylist():
    path = pathlib.Path(os.environ.get("PRIVACY_DENYLIST") or
                        pathlib.Path.home() / ".config/printer-farmlord/privacy-denylist.txt")
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return []
    # Very short entries would match everywhere; the generic patterns cover those.
    return [x.strip().lower() for x in lines if len(x.strip()) >= 4 and not x.startswith("#")]


def findings(line, deny):
    hits = [label for label, pattern in PATTERNS.items() if pattern.search(line)]
    low = line.lower()
    if any(d in low for d in deny):
        hits.append("private denylist entry")
    return hits


def scan_history(rev_range):
    """Every line added by any commit in the range, plus its author and committer emails."""
    deny, bad = denylist(), []
    revs = subprocess.run(["git", "rev-list", *(rev_range or ["--all"])], cwd=ROOT,
                          capture_output=True, text=True, check=True).stdout.split()
    for rev in revs:
        meta = subprocess.run(["git", "show", "-s", "--format=%ae%n%ce", rev], cwd=ROOT,
                              capture_output=True, text=True, check=True).stdout.split()
        if any(not ALLOWED_EMAIL.search(e) for e in meta):
            bad.append((rev[:9], 0, "author/committer email is not a GitHub noreply address"))
        diff = subprocess.run(["git", "show", "--format=", "--no-color", "-U0", rev], cwd=ROOT,
                              capture_output=True, check=True).stdout.decode("utf-8", "replace")
        name = "?"
        for line in diff.splitlines():
            if line.startswith("+++ "):
                name = line[6:] if line.startswith("+++ b/") else line[4:]
            elif line.startswith("+") and not name.startswith(("docs/img/", "docs/screenshots/")):
                for label in findings(line[1:], deny):
                    bad.append((f"{rev[:9]} {name}", 0, label))
    for where, _, label in bad:
        print(f"{where}: {label}")
    print(f"Scanned {len(revs)} commits; {len(bad)} potential leaks")
    return 1 if bad else 0


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
    if "--history" in sys.argv:
        return scan_history(sys.argv[sys.argv.index("--history") + 1:])
    bad, deny = [], denylist()
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
            for label in findings(line, deny):
                # Report location only; never echo a suspected secret.
                bad.append((path, number, label))
    for path, number, label in bad:
        print(f"{path.relative_to(ROOT)}:{number}: {label}")
    print(f"Scanned {len(files('--staged' in sys.argv))} files; {len(bad)} potential leaks")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

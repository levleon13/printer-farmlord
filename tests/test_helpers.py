#!/usr/bin/env python3
"""Checks for the I/O helpers the efficiency work added (run: python3 tests/test_helpers.py)."""
import json
import os
import pathlib
import sys
import tempfile
import time

os.environ.setdefault("AMS_TRACKER_DIR", tempfile.mkdtemp())
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from farmlord import core  # noqa: E402


def check(name, cond):
    print(("ok   " if cond else "FAIL ") + name)
    return cond


def main():
    tmp = pathlib.Path(tempfile.mkdtemp())
    ok = True

    # tail_lines gives exactly what reading the whole file and slicing gives
    for lines, trailing in ((0, True), (1, True), (5, False), (39, True), (40, True), (41, False), (25000, True)):
        p = tmp / f"log{lines}{trailing}.jsonl"
        body = "\n".join(json.dumps({"i": i, "pad": "x" * (i % 97)}) for i in range(lines))
        p.write_text(body + ("\n" if trailing and lines else ""))
        for n in (0, 1, 40, 100):
            ok &= check(f"tail_lines {lines} lines, trailing newline {trailing}, n={n}",
                        core.tail_lines(str(p), n) == p.read_text().splitlines()[-n:])

    # jsave_changed skips only its own identical rewrite, never someone else's change
    p = str(tmp / "spools.json")
    ok &= check("first write happens", core.jsave_changed(p, {"a": 1}))
    ok &= check("identical rewrite is skipped", not core.jsave_changed(p, {"a": 1}))
    ok &= check("different content is written", core.jsave_changed(p, {"a": 2}))
    time.sleep(0.01)
    core.jsave(p, {"a": 2})                      # another writer, same content: mtime moves
    ok &= check("write after another writer is not skipped", core.jsave_changed(p, {"a": 2}))
    with open(p, "w") as f: f.write('{"a": 3}')    # edited behind our back
    ok &= check("external edit is overwritten", core.jsave_changed(p, {"a": 2}) and core.jload(p, None) == {"a": 2})
    os.remove(p)
    ok &= check("deleted file is rewritten", core.jsave_changed(p, {"a": 2}) and os.path.exists(p))
    ok &= check("file mode stays 0600", (os.stat(p).st_mode & 0o777) == 0o600)

    print("all passed" if ok else "FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

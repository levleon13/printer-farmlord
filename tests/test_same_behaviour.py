#!/usr/bin/env python3
"""Prove a refactor did not change what the dashboard computes or serves.

  python3 tests/test_same_behaviour.py OLD_DIR [NEW_DIR]

Both copies get the same made-up printer data and a frozen clock (see tests/bench.py). The test
compares parse_ams(), the dashboard poll (rev_data), the state API (report_data), the page bytes
and the UI version hash. NEW_DIR defaults to this checkout. Exit 0 = identical.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent


def snapshot(target, out):
    subprocess.run([sys.executable, str(HERE / "bench.py"), "--target", str(target), "--dump", str(out)],
                   check=True, capture_output=True, text=True)
    return json.loads(pathlib.Path(out).read_text())


def diff(a, b, path=""):
    if type(a) is not type(b):
        return [f"{path}: {type(a).__name__} -> {type(b).__name__}"]
    if isinstance(a, dict):
        out = []
        for k in sorted(set(a) | set(b)):
            if k not in a: out.append(f"{path}.{k}: added")
            elif k not in b: out.append(f"{path}.{k}: removed")
            else: out += diff(a[k], b[k], f"{path}.{k}")
        return out
    if isinstance(a, list):
        if len(a) != len(b): return [f"{path}: length {len(a)} -> {len(b)}"]
        return [d for i, (x, y) in enumerate(zip(a, b)) for d in diff(x, y, f"{path}[{i}]")]
    return [] if a == b else [f"{path}: changed"]


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    old = pathlib.Path(sys.argv[1]).resolve()
    new = pathlib.Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else HERE.parent
    with tempfile.TemporaryDirectory() as tmp:
        a = snapshot(old, pathlib.Path(tmp) / "old.json")
        b = snapshot(new, pathlib.Path(tmp) / "new.json")
    problems = diff(a, b)
    for p in problems[:60]:
        print("DIFF", p)
    print(f"{len(problems)} differences between {old.name} and {new.name}")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()

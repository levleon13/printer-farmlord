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

    # plate preview: finding the job's file and reading only the picture out of a .3mf
    from farmlord import preview as pv
    files = [("Bench part.gcode.3mf", 10), ("Other.3mf", 5), ("Calib.gcode.gcode.3mf", 7), ("notes.txt", 3), ("Bench part.gcode", 9)]
    ok &= check("preview: exact <job>.gcode.3mf wins", pv.preview_find_file(files, "Bench part") == ("Bench part.gcode.3mf", 10))
    ok &= check("preview: <job>.3mf", pv.preview_find_file(files, "Other") == ("Other.3mf", 5))
    ok &= check("preview: job name ending in .gcode", pv.preview_find_file(files, "Calib.gcode") == ("Calib.gcode.gcode.3mf", 7))
    ok &= check("preview: case-insensitive match", pv.preview_find_file(files, "bench PART") == ("Bench part.gcode.3mf", 10))
    ok &= check("preview: no match", pv.preview_find_file(files, "notes") == (None, 0))
    import io, zipfile, zlib, struct
    def png(tag):
        raw = b"\x00" + bytes(4)
        chunk = lambda k, d: struct.pack(">I", len(d)) + k + d + struct.pack(">I", zlib.crc32(k + d))
        return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0)) + chunk(b"tEXt", tag) + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("Metadata/plate_1.gcode", "G1 X0\n" * 200000)        # big part that must not be read
        z.writestr("Metadata/plate_1.png", png(b"one")); z.writestr("Metadata/plate_2.png", png(b"two"))
    blob = buf.getvalue(); reads = []
    def fetch(a, b): reads.append(b - a + 1); return blob[a:b + 1]
    got, inner = pv.preview_extract(fetch, len(blob), 2)
    ok &= check("preview: plate 2 picture extracted", inner == "Metadata/plate_2.png" and b"two" in got)
    ok &= check("preview: reads far less than the file", sum(reads) < len(blob) // 2)
    got, inner = pv.preview_extract(lambda a, b: blob[a:b + 1], len(blob), 7)
    ok &= check("preview: missing plate falls back to plate 1", inner == "Metadata/plate_1.png")
    try: pv.preview_extract(lambda a, b: blob[a:b + 1][:5], len(blob), 1); short = False
    except Exception: short = True
    ok &= check("preview: a short read from the printer is an error, not a bad picture", short)
    st = {"print_name": "Bench part", "print_state": "RUNNING", "job_id": "7", "job_started": "1000", "print_plate": 1}
    ok &= check("preview: key per job", pv.preview_key(st) == "7|1000|1|Bench part" and pv.preview_key(dict(st, print_state="IDLE")) is None)
    ok &= check("preview: same name sent again is a new job", pv.preview_key(dict(st, job_started="2000")) != pv.preview_key(st))
    pv.PREVIEW.clear(); pv.PREVIEW.update(key=pv.preview_key(st), ok=True, name="Bench part")
    ok &= check("preview: shown for its own job", pv.preview_info(st) is not None)
    ok &= check("preview: shown as the last print when idle", pv.preview_info(dict(st, print_state="IDLE")) is not None)
    ok &= check("preview: never shown for a different, newer job", pv.preview_info(dict(st, job_started="2000", print_name="Next")) is None)
    import farmlord.core as fc
    times = []
    for tries in (1, 2, 3, 4):
        pv.PREVIEW.update(key=pv.preview_key(st), ok=False, tries=tries, at=fc.now())
        waits = [w for w in (59, 61, 299, 301, 1799, 1801)
                 if not (tries > len(pv.PREVIEW_RETRY) or w < pv.PREVIEW_RETRY[min(tries, len(pv.PREVIEW_RETRY)) - 1])]
        times.append(waits[0] if waits else None)
    ok &= check("preview: retries after 1 min, 5 min, 30 min, then stops", times == [61, 301, 1801, None])

    print("all passed" if ok else "FAILED")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

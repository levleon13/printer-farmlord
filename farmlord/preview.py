"""Plate preview of the current (or last) print, read from the sliced file on the printer's USB drive.

The printer reports only the job name (subtask_name), task id and plate number. Orca and Bambu Studio
upload the job to the drive's root as "<job name>.gcode.3mf", a zip that holds the slicer's plate
picture at Metadata/plate_<n>.png. The worker finds that file over the printer's FTPS service and
reads just the zip directory and the picture with ranged reads (a few hundred KB, never the whole file).
Read-only: nothing is written to the printer, and nothing here moves or heats anything.
"""

from farmlord.core import *  # noqa: F401,F403

PREVIEW_PNG = P("preview.png")
PREVIEW_META = P("preview.json")
PREVIEW = {}                 # key, ok, name, file, plate, error, at, tries — mirrors preview.json
PREVIEW_RETRY = (60, 300, 1800)   # wait after the 1st, 2nd and 3rd failure for the same job; then give up until it changes
PREVIEW_MAX_PNG = 4 * 1024 * 1024
PREVIEW_MAX_READ = 12 * 1024 * 1024
PREVIEW_JOB_STATES = ("PREPARE", "RUNNING", "PAUSE", "FINISH", "FAILED")


def _ftps(cfg, path, extra=(), binary=False, timeout=40):
    import subprocess, urllib.parse
    url = f"ftps://{cfg['ip']}:990{urllib.parse.quote(path, safe='/')}"
    conf = "user = " + json.dumps("bblp:" + cfg["access_code"]) + "\n"   # via stdin: the code never appears in ps
    r = subprocess.run(["curl", "-k", "-sS", "--max-time", str(timeout), "-K", "-", *extra, url],
                       input=conf.encode(), capture_output=True, timeout=timeout + 10)
    if r.returncode:
        raise ValueError(f"USB file service failed (curl {r.returncode}: {r.stderr.decode(errors='replace').strip()[:120]})")
    return r.stdout if binary else r.stdout.decode("utf-8", "replace")


def _usb_root(cfg):
    """[(name, size)] of plain files in the drive's root."""
    files = []
    for line in _ftps(cfg, "/", ("--request", "LIST -a")).splitlines():
        parts = line.split(maxsplit=8)
        if len(parts) == 9 and parts[0].startswith("-") and parts[4].isdigit():
            files.append((parts[8], int(parts[4])))
    return files


def _job_stem(name):
    low = name.lower()
    for ext in (".gcode.3mf", ".3mf"):
        if low.endswith(ext): return name[:-len(ext)]
    return None


def preview_find_file(files, job):
    """The drive file for this job name: exact "<job>.gcode.3mf" first, then "<job>.3mf", then a case-insensitive match."""
    names = {n: s for n, s in files}
    for cand in (job + ".gcode.3mf", job + ".3mf", job):
        if cand in names and _job_stem(cand) is not None: return cand, names[cand]
    want = job.strip().lower()
    for n, s in files:
        stem = _job_stem(n)
        if stem is not None and stem.strip().lower() in (want, want + ".gcode"): return n, s
    return None, 0


class _RangeFile:
    """A read-only, seekable view of a remote file that fetches byte ranges on demand (enough for zipfile)."""

    def __init__(self, fetch, size, block=128 * 1024):
        self.fetch, self.size, self.block, self.pos, self.read_total = fetch, size, block, 0, 0
        self.cache = []          # [(start, bytes)]

    def seekable(self): return True
    def tell(self): return self.pos

    def seek(self, off, whence=0):
        self.pos = off if whence == 0 else self.pos + off if whence == 1 else self.size + off
        if self.pos < 0: raise ValueError("negative seek")
        return self.pos

    def _cached(self, a, b):
        for start, data in self.cache:
            if start <= a and b <= start + len(data): return data[a - start:b - start]
        return None

    def read(self, n=-1):
        a = self.pos; b = self.size if n is None or n < 0 else min(self.size, a + n)
        if a >= b: return b""
        out = self._cached(a, b)
        if out is None:
            end = min(self.size, max(b, a + self.block))
            if self.read_total + (end - a) > PREVIEW_MAX_READ: raise ValueError("preview read limit reached")
            data = self.fetch(a, end - 1)
            if len(data) < b - a: raise ValueError("short read from the printer")
            self.read_total += len(data); self.cache.append((a, data)); self.cache = self.cache[-8:]
            out = data[:b - a]
        self.pos = a + len(out)
        return out


def preview_extract(fetch, size, plate):
    """PNG bytes of the plate picture inside a .3mf, reading only what zipfile asks for."""
    import zipfile
    f = _RangeFile(fetch, size)
    with zipfile.ZipFile(f) as z:
        names = set(z.namelist())
        for cand in (f"Metadata/plate_{plate}.png", f"Metadata/plate_{plate}_small.png", f"Metadata/top_{plate}.png",
                     "Metadata/plate_1.png", "Metadata/thumbnail.png"):
            if cand in names:
                info = z.getinfo(cand)
                if info.file_size > PREVIEW_MAX_PNG: raise ValueError("plate picture is too large")
                png = z.read(cand)
                if not png.startswith(b"\x89PNG\r\n\x1a\n"): raise ValueError("plate picture is not a PNG")
                return png, cand
    raise ValueError("this file has no plate picture")


def _preview_save(**meta):
    global PREVIEW
    PREVIEW = dict(meta, at=now())
    jsave(PREVIEW_META, PREVIEW)


def preview_key(st):
    name = (st.get("print_name") or "").strip()
    if not name or st.get("print_state") not in PREVIEW_JOB_STATES: return None
    # Same job identity as the nozzle auto-resume uses: a re-sliced file sent under the same name
    # starts at a new time, so it gets a new key, a new fetch and a new picture URL.
    return f"{st.get('job_id') or ''}|{st.get('job_started') or ''}|{st.get('print_plate') or 1}|{name}"


def preview_tick():
    """Fetch the plate picture once per job. Retries a failed job a few times, spaced out, then waits for the next job."""
    st = jload(STATE, {}) or {}
    key = preview_key(st)
    if not key or STATUS.get("mqtt") != "connected": return
    if PREVIEW.get("key") == key:
        if PREVIEW.get("ok"): return
        tries = int(PREVIEW.get("tries") or 0)
        if tries > len(PREVIEW_RETRY) or now() - float(PREVIEW.get("at") or 0) < PREVIEW_RETRY[min(tries, len(PREVIEW_RETRY)) - 1]: return
    else:
        tries = 0
    cfg = jload(CONFIG, {}) or {}
    name = st["print_name"].strip()
    try: plate = int(st.get("print_plate") or 1)
    except (TypeError, ValueError): plate = 1
    if not (cfg.get("ip") and cfg.get("access_code")): return
    file = None
    try:
        file, size = preview_find_file(_usb_root(cfg), name)
        if not file: raise ValueError("the print file is not on the printer's USB drive")
        png, inner = preview_extract(lambda a, b: _ftps(cfg, "/" + file, ("--range", f"{a}-{b}"), binary=True), size, plate)
        tmp = PREVIEW_PNG + ".tmp"
        with open(tmp, "wb") as fh: fh.write(png)
        os.chmod(tmp, 0o600); os.replace(tmp, PREVIEW_PNG)
        _preview_save(key=key, ok=True, name=name, file=file, plate=plate, picture=inner, bytes=len(png))
        print(f"[{iso()}] preview: plate picture for {name!r} ({len(png)//1024} KB)", flush=True)
    except Exception as exc:
        _preview_save(key=key, ok=False, name=name, file=file, plate=plate, error=str(exc)[:200], tries=tries + 1)
        print(f"[{iso()}] preview: {name!r}: {exc}", flush=True)


def preview_loop():
    global PREVIEW
    PREVIEW = jload(PREVIEW_META, {}) or {}
    if PREVIEW.get("ok") and not os.path.exists(PREVIEW_PNG): PREVIEW = {}
    time.sleep(15)
    while True:
        try: preview_tick()
        except Exception as exc: print(f"[{iso()}] preview error {exc}", flush=True)
        time.sleep(10)


def preview_info(st):
    """What the page needs: a cache key for the picture URL, or None. The picture is offered only for the
    job the printer reports now (or, when it has no job, its last one), so a new job never shows the
    previous plate while its own picture is still being fetched."""
    if not PREVIEW.get("ok"): return None
    current = preview_key(st)
    if current is not None and current != PREVIEW.get("key"): return None
    return dict(key=hashlib.sha1(PREVIEW["key"].encode()).hexdigest()[:12], name=PREVIEW.get("name"))


def preview_png():
    try:
        with open(PREVIEW_PNG, "rb") as fh: return fh.read()
    except OSError:
        return None


__all__ = ['PREVIEW_PNG', 'PREVIEW_META', 'PREVIEW', 'PREVIEW_RETRY', 'PREVIEW_MAX_PNG', 'PREVIEW_MAX_READ', 'PREVIEW_JOB_STATES',
           '_ftps', '_usb_root', '_job_stem', 'preview_find_file', '_RangeFile', 'preview_extract', '_preview_save', 'preview_key',
           'preview_tick', 'preview_loop', 'preview_info', 'preview_png']

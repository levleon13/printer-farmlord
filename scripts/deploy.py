#!/usr/bin/env python3
"""Deploy this checkout's code to the print-room host in one step, and check it came up.

  python3 scripts/deploy.py              check, upload, back up, swap in, verify
  python3 scripts/deploy.py --dry-run    everything except the swap
  python3 scripts/deploy.py --rollback   put back the code from the most recent backup

Where to deploy comes from ~/.config/printer-farmlord/deploy.json (never part of the repo):
  {"ssh": ["-F", "~/.ssh/your-host.conf", "your-host"], "remote_dir": "~/ams-host"}

Only code moves: ams_tracker.py and farmlord/. The host's .runtime data, config and logs are never
touched. The swap is refused while a USB clear or an RFID scan/unload is running, the previous code is
saved under <remote_dir>/backups/code-<time>.tgz, and ams_tracker.py is written last so the running
hub restarts exactly once, into the complete new code.
"""
import hashlib
import io
import json
import os
import pathlib
import subprocess
import sys
import tarfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
CONF = pathlib.Path.home() / ".config/printer-farmlord/deploy.json"

REMOTE = r'''
import hashlib, io, json, os, pathlib, shutil, sys, tarfile, time, urllib.request
root = pathlib.Path(os.path.expanduser(ARGS["remote_dir"])); blob = root / ARGS["upload"]
data = blob.read_bytes(); blob.unlink()
def stop(msg): print(json.dumps({"ok": False, "error": msg})); sys.exit(0)
if hashlib.sha256(data).hexdigest() != ARGS["sha"]: stop("upload corrupted; nothing replaced")
if ARGS["mode"] == "deploy":
    try:
        job = json.load(urllib.request.urlopen("http://127.0.0.1:8765/api/storage_clear_status", timeout=10))
        if not job.get("done"): stop("a USB clear is running; nothing replaced")
    except OSError:
        pass                                    # hub down: nothing to protect
    busy = []
    for d in (root / "printers").iterdir() if (root / "printers").exists() else []:
        try: cycles = json.load(open(d / "rfid_heat_cycles.json")) or {}
        except (OSError, ValueError): cycles = {}
        busy += [f"{d.name}:{a}" for a, w in cycles.items()
                 if w.get("stage") in ("scan", "unload") or (w.get("stage") == "clear_outlet" and w.get("outlet_sent_at"))]
    if busy: stop("a tag read or nozzle unload is in progress (" + ", ".join(busy) + "); nothing replaced")
stage = root / ".deploy-staging"
shutil.rmtree(stage, ignore_errors=True); stage.mkdir(mode=0o700)
with tarfile.open(fileobj=io.BytesIO(data)) as t:
    for m in t.getmembers():
        if m.name.startswith(("/", "..")) or ".." in pathlib.PurePosixPath(m.name).parts: stop("unsafe path in upload")
    t.extractall(stage, filter="data") if sys.version_info >= (3, 12) else t.extractall(stage)
for f in [stage / "ams_tracker.py", *sorted((stage / "farmlord").rglob("*.py"))]:
    try: compile(f.read_text(), str(f), "exec")
    except SyntaxError as e: stop(f"syntax error in {f.name}, nothing replaced: {e}")
if ARGS["dry_run"]:
    shutil.rmtree(stage); print(json.dumps({"ok": True, "dry_run": True})); sys.exit(0)
import glob
logs = [root / "logs" / "host.log", *map(pathlib.Path, glob.glob(str(root / "printers" / "*" / "worker.log")))]
sizes = {str(l): l.stat().st_size for l in logs if l.exists()}
(root / "backups").mkdir(exist_ok=True)
backup = root / "backups" / ("code-" + time.strftime("%Y%m%d-%H%M%S") + ".tgz")
with tarfile.open(backup, "w:gz") as t:
    for name in ("ams_tracker.py", "farmlord"):
        if (root / name).exists(): t.add(root / name, arcname=name, filter=lambda i: None if "__pycache__" in i.name else i)
os.chmod(backup, 0o600)
old = root / ".farmlord-old"
shutil.rmtree(old, ignore_errors=True)
if (root / "farmlord").exists(): os.replace(root / "farmlord", old)
os.replace(stage / "farmlord", root / "farmlord")
for p in (root / "farmlord").rglob("*"):
    os.chmod(p, 0o700 if p.is_dir() else 0o600)
nxt = root / "ams_tracker.next.py"; shutil.copyfile(stage / "ams_tracker.py", nxt); nxt.chmod(0o600)
os.replace(nxt, root / "ams_tracker.py")            # last: this is what restarts the hub
shutil.rmtree(old, ignore_errors=True); shutil.rmtree(stage, ignore_errors=True)
print(json.dumps({"ok": True, "backup": backup.name, "at": time.time(), "sizes": sizes}))
'''

VERIFY = r'''
import glob, json, os, pathlib, time, urllib.request
root = pathlib.Path(os.path.expanduser(ARGS["remote_dir"]))
cfg = json.load(open(root / "config.json"))
def ok(url):
    try: return urllib.request.urlopen(url, timeout=8).status == 200
    except Exception: return False
hub = ok("http://127.0.0.1:8765/")
names = [p["name"] for p in cfg.get("printers", [])]
workers = {n: ok(f"http://127.0.0.1:8765/api/p/{urllib.request.quote(n)}/api/rev") for n in names}
errors = 0
for log in [root / "logs" / "host.log", *map(pathlib.Path, glob.glob(str(root / "printers" / "*" / "worker.log")))]:
    try:
        with open(log, "rb") as f:
            f.seek(ARGS["sizes"].get(str(log), 0)); new = f.read().decode(errors="replace")
    except OSError:
        continue
    errors += new.count("Traceback")
print(json.dumps({"hub": hub, "workers_up": sum(workers.values()), "workers": len(workers),
                  "down": [n for n, up in workers.items() if not up], "tracebacks": errors}))
'''

ROLLBACK = r'''
import json, os, pathlib, shutil, tarfile, time
root = pathlib.Path(os.path.expanduser(ARGS["remote_dir"]))
backups = sorted((root / "backups").glob("code-*.tgz"))
if not backups: print(json.dumps({"ok": False, "error": "no code backup found"})); raise SystemExit
import glob
sizes = {l: os.path.getsize(l) for l in [str(root / "logs" / "host.log"), *glob.glob(str(root / "printers" / "*" / "worker.log"))] if os.path.exists(l)}
src = backups[-1]; stage = root / ".rollback-staging"
shutil.rmtree(stage, ignore_errors=True); stage.mkdir(mode=0o700)
with tarfile.open(src) as t: t.extractall(stage)
old = root / ".farmlord-old"; shutil.rmtree(old, ignore_errors=True)
if (root / "farmlord").exists(): os.replace(root / "farmlord", old)
if (stage / "farmlord").exists(): os.replace(stage / "farmlord", root / "farmlord")
os.replace(stage / "ams_tracker.py", root / "ams_tracker.py")
shutil.rmtree(old, ignore_errors=True); shutil.rmtree(stage, ignore_errors=True)
print(json.dumps({"ok": True, "restored": src.name, "sizes": sizes}))
'''


def conf():
    try:
        c = json.loads(CONF.read_text())
    except (OSError, ValueError):
        sys.exit(f"Create {CONF} first (see the top of this script).")
    c["ssh"] = [os.path.expanduser(x) for x in c["ssh"]]
    c.setdefault("remote_dir", "~/ams-host")
    return c


def remote(c, script, args, stdin_bytes=None, upload=None):
    if upload is not None:
        # remote_dir comes from your own deploy.json; ~ is left for the remote shell to expand
        subprocess.run(["ssh", "-o", "ConnectTimeout=25", *c["ssh"], f"umask 077; cat > {c['remote_dir']}/{upload}"],
                       input=stdin_bytes, check=True)
    code = "ARGS = " + repr(args) + "\n" + script
    r = subprocess.run(["ssh", "-o", "ConnectTimeout=25", *c["ssh"], "python -"], input=code.encode(),
                       capture_output=True, check=False)
    if r.returncode:
        sys.exit("remote step failed:\n" + r.stderr.decode(errors="replace")[-2000:])
    return json.loads(r.stdout.decode().strip().splitlines()[-1])


def bundle():
    files = [ROOT / "ams_tracker.py"] + sorted(p for p in (ROOT / "farmlord").rglob("*")
                                              if p.is_file() and "__pycache__" not in p.parts)
    for f in files:
        if f.suffix == ".py":
            compile(f.read_text(), str(f), "exec")          # refuse to ship a syntax error
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        for f in files:
            info = t.gettarinfo(str(f), arcname=str(f.relative_to(ROOT)))
            info.uid = info.gid = 0; info.uname = info.gname = ""; info.mode = 0o600
            with open(f, "rb") as fh: t.addfile(info, fh)
    return buf.getvalue(), len(files)


def verify(c, sizes):
    print("waiting for the hub to restart …", flush=True)
    for attempt in range(12):
        time.sleep(10)
        v = remote(c, VERIFY, {"remote_dir": c["remote_dir"], "sizes": sizes})
        if v["hub"] and v["workers_up"] == v["workers"]:
            break
    print(f"hub {'up' if v['hub'] else 'DOWN'}; workers {v['workers_up']}/{v['workers']} up"
          + (f" (down: {', '.join(v['down'])})" if v["down"] else "") + f"; new tracebacks: {v['tracebacks']}")
    return v["hub"] and v["workers_up"] == v["workers"] and not v["tracebacks"]


def main():
    c = conf()
    if "--rollback" in sys.argv:
        r = remote(c, ROLLBACK, {"remote_dir": c["remote_dir"]})
        if not r["ok"]: sys.exit(r["error"])
        print("restored", r["restored"]); sys.exit(0 if verify(c, r["sizes"]) else 1)
    data, n = bundle()
    sha = hashlib.sha256(data).hexdigest()
    name = f".deploy-{int(time.time())}.tgz"
    print(f"uploading {n} files ({len(data) // 1024} KB) …", flush=True)
    r = remote(c, REMOTE, {"remote_dir": c["remote_dir"], "upload": name, "sha": sha, "mode": "deploy",
                           "dry_run": "--dry-run" in sys.argv}, stdin_bytes=data, upload=name)
    if not r["ok"]: sys.exit("not deployed: " + r["error"])
    if r.get("dry_run"):
        print("dry run: upload, checks and syntax all passed; nothing replaced"); return
    print("deployed; previous code saved as backups/" + r["backup"])
    if not verify(c, r["sizes"]):
        sys.exit("the new code did not come up cleanly — run: python3 scripts/deploy.py --rollback")


if __name__ == "__main__":
    main()

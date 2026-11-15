#!/usr/bin/env python3
"""Efficiency benchmark and behaviour snapshot for the dashboard, with no printer involved.

  python3 tests/bench.py                        benchmark this checkout
  python3 tests/bench.py --target DIR           benchmark another copy (a dir holding ams_tracker.py)
  python3 tests/bench.py --compare OLD NEW      run both and print a before/after table
  python3 tests/bench.py --dump FILE            write deterministic outputs (for tests/test_same_behaviour.py)

Everything runs on made-up data in a temporary directory. tests/guard/sitecustomize.py is loaded
into every process and blocks any connection that is not to this machine, and the fake printer
connection only records commands. The report ends with the number of blocked network attempts and
recorded printer commands; both must be 0 for idle work.

Two layers:
  in-process  each hot function: median time per call and file reads/writes per call
  end-to-end  a real hub + worker on 127.0.0.1: startup time, page/API latency, memory
"""

import argparse
import json
import os
import pathlib
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent
GUARD = HERE / "guard"
FROZEN = 1_800_000_000.0        # fixed clock for --dump, so outputs are comparable


# ---------------------------------------------------------------------------- made-up printer data
def tray(i, material, sub, color, uid, remain):
    return {"id": str(i), "tray_type": material, "tray_sub_brands": sub, "tray_color": color,
            "cols": [color], "tray_info_idx": "GFA00", "tray_id_name": "A00-K0", "tag_uid": uid,
            "tray_uuid": uid + uid, "remain": remain, "tray_weight": "1000", "tray_diameter": "1.75",
            "nozzle_temp_min": "190", "nozzle_temp_max": "230", "drying_temp": "55", "drying_time": "8",
            "state": 11, "ctype": 0, "bed_temp": "0", "bed_temp_type": "0", "total_len": 330000}


def unit(aid, temp="31.0", hum_raw="22", drying=False):
    mats = [("PLA", "PLA Basic", "FFFFFFFF"), ("PETG", "PETG HF", "000000FF"),
            ("PETG-CF", "PETG-CF", "333333FF"), ("TPU-AMS", "TPU for AMS", "5898DDFF")]
    trays = [tray(i, *mats[i], f"{aid:04d}{i:04d}00000000", 80 - 10 * i) for i in range(4)]
    return {"id": str(aid), "temp": temp, "humidity": "4", "humidity_raw": hum_raw, "info": "1003" if not drying else "1023",
            "dry_time": 0 if not drying else 300, "dry_sf_reason": [] if not drying else [6],
            "dry_setting": {"dry_temperature": -1 if not drying else 55, "dry_duration": -1 if not drying else 8,
                            "dry_filament": ""}, "tray": trays}


def report(units=2):
    return {"print": {
        "command": "push_status", "gcode_state": "IDLE", "print_error": 0, "hms": [], "mc_percent": 0,
        "nozzle_temper": 30.0, "nozzle_target_temper": 0.0, "bed_temper": 28.0, "bed_target_temper": 0.0,
        "ams_status": 0, "home_flag": 0, "nozzle_diameter": "0.4", "nozzle_type": "hardened_steel",
        "ams": {"ams": [unit(a) for a in range(units)], "ams_exist_bits": "3", "tray_exist_bits": "ff",
                "tray_is_bbl_bits": "ff", "tray_read_done_bits": "ff", "tray_reading_bits": "0",
                "tray_now": "255", "tray_pre": "255", "tray_tar": "255", "version": 1, "insert_flag": True,
                "power_on_flag": True},
        "device": {"extruder": {"state": 2, "info": [
            {"id": 0, "snow": 65535, "spre": 65535, "star": 65535, "info": 8, "temp": 30, "hnow": 0},
            {"id": 1, "snow": 65535, "spre": 65535, "star": 65535, "info": 8, "temp": 30, "hnow": 1}]},
            "nozzle": {"info": [{"id": 0, "diameter": 0.4, "type": "HS"}, {"id": 1, "diameter": 0.4, "type": "HS"}]},
            "bed_temp": 28, "ctc": {"info": {"temp": 30}}}}}


def write_fixture(root, spools=500, events=20000):
    """A worker directory as a real one looks after weeks of use: a big spool registry and events log."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "config.json").write_text(json.dumps({
        "name": "Bench-1", "ip": "127.0.0.1", "serial": "bench-serial-1", "access_code": "REPLACE_ME"}))
    reg = {}
    for i in range(spools):
        # Old spools that have since left the AMS, shaped like Tracker.new_spool() records.
        reg[f"S{i:06d}"] = {"id": f"S{i:06d}", "name": f"Bench spool {i}", "type": ["PLA", "PETG", "PETG-CF", "ABS"][i % 4],
                            "sub": "Bench", "color": "FFFFFF", "rfid": f"{i:016d}", "bambu": True,
                            "first_seen": FROZEN - 86400 * 30, "slot": None, "removed_at": FROZEN - 86400,
                            "dry_credit_h": float(i % 7), "exposure_h": float(i % 5), "last_dry_end": None, "history": []}
    (root / "spools.json").write_text(json.dumps(reg))
    with open(root / "events.jsonl", "w") as f:
        for i in range(events):
            f.write(json.dumps({"t": FROZEN - events + i, "time": "2026-01-01 00:00:00", "kind": "INFO",
                                "msg": f"bench event {i}"}) + "\n")


# ---------------------------------------------------------------------------- in-process layer
class Recorder:
    """Stands in for the MQTT client: records what would have been sent to a printer."""
    def __init__(self): self.sent = []
    def publish(self, topic, payload, *a, **k):
        self.sent.append(payload)
        return type("R", (), {"rc": 0})()


def modules_of(target):
    """The module objects that make up the app: the single file, or the file plus farmlord.*."""
    import ams_tracker as m
    mods = [m] + [v for k, v in sorted(sys.modules.items()) if k.startswith("farmlord.") and v]
    return mods


class NS:
    """Look a name up across all app modules (single file today, a package later)."""
    def __init__(self, mods): self.mods = mods
    def __getattr__(self, n):
        for m in self.mods:
            if hasattr(m, n): return getattr(m, n)
        raise AttributeError(n)
    def has(self, n): return any(hasattr(m, n) for m in self.mods)
    def set(self, n, v):
        for m in self.mods:
            if hasattr(m, n): setattr(m, n, v)


def instrument(mods, counts):
    """Count every JSON read, JSON write and log append, whichever module calls them."""
    for name in ("jload", "jsave", "append"):
        orig = next(getattr(m, name) for m in mods if hasattr(m, name))
        def wrap(*a, _o=orig, _n=name, **k):
            counts[_n] = counts.get(_n, 0) + 1
            return _o(*a, **k)
        for m in mods:
            if getattr(m, name, None) is orig: setattr(m, name, wrap)


def measure(fn, counts, repeat):
    """Median wall time per call (µs) and I/O per call, after one warm-up call."""
    fn()
    times, before = [], dict(counts)
    for _ in range(repeat):
        t = time.perf_counter(); fn(); times.append(time.perf_counter() - t)
    per = {k: round((counts.get(k, 0) - before.get(k, 0)) / repeat, 2) for k in ("jload", "jsave", "append")}
    return {"us": round(statistics.median(times) * 1e6, 1), **per}


def prime_state(a, rep):
    """What the MQTT handler adds to state.json besides the AMS data."""
    st = a.jload(a.STATE, {})
    pr = rep["print"]
    st.update(print_state=pr["gcode_state"], print_state_t=a.now(), t=a.now(), seen_since=a.now() - 3600,
              print_error=0, hms=[], ams_status=0)
    if a.has("update_nozzle_routing"): a.update_nozzle_routing(st, pr["device"]["extruder"])
    a.jsave(a.STATE, st)


def in_process(target, dump=None, repeat=60):
    os.chdir(target)
    sys.path.insert(0, str(target))
    mods = modules_of(target)
    a = NS(mods)
    counts = {}
    instrument(mods, counts)
    rec = Recorder()
    a.STATUS.update(mqtt="connected", since=0)
    a.MQTT.update(client=rec, topic_req="bench/request")
    rep = report()
    t = a.Tracker()
    t.update(*a.parse_ams(rep))
    prime_state(a, rep)

    out = {}
    if dump:
        snap = {"parse_ams": a.parse_ams(rep), "rev": a.rev_data(), "state": a.report_data(),
                "html_sha": __import__("hashlib").sha256(a.HTML.encode()).hexdigest(),
                "movement_sha": __import__("hashlib").sha256(a.MOVEMENT_HTML.encode()).hexdigest(),
                "ui_version": a.UI_VERSION}
        pathlib.Path(dump).write_text(json.dumps(snap, sort_keys=True, indent=1, default=str))
        return {"dumped": dump}

    out["mqtt message (unchanged AMS)"] = measure(lambda: t.update(*a.parse_ams(rep)), counts, repeat)
    out["dashboard poll  rev_data()"] = measure(a.rev_data, counts, repeat)
    out["state API  report_data()"] = measure(a.report_data, counts, max(5, repeat // 6))
    page = lambda: a.gzip_body(a.HTML.replace("configured store", a.store_name() or "configured store").encode())
    out["page build  GET / body"] = measure(page, counts, max(10, repeat // 3))
    sent_before = len(rec.sent)
    for tick in ("autodry_tick", "dry_pending_tick", "rfid_heat_cycle_tick", "rfid_retry_tick", "cool_read_tick",
                 "ams_wedged_tick", "pullback_retry_tick", "fan_timer_tick", "camera_resume_tick",
                 "plate_resume_tick", "nozzle_resume_tick", "cooldown_tick"):
        if a.has(tick):
            out[f"idle tick  {tick}"] = measure(getattr(a, tick), counts, max(10, repeat // 3))
    out["_printer_commands_sent_while_idle"] = len(rec.sent) - sent_before
    return out


# ---------------------------------------------------------------------------- end-to-end layer
def free(port):
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def get(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    t = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read(); code = r.status
    except urllib.error.HTTPError as e:
        body, code = e.read(), e.code
    return code, time.perf_counter() - t, body


def rss_kb(pid):
    """Resident memory of a process and its children (the hub plus its workers)."""
    out = subprocess.run(["ps", "-A", "-o", "pid=,ppid=,rss="], capture_output=True, text=True).stdout.split("\n")
    rows = [tuple(map(int, l.split())) for l in out if l.strip()]
    tree, total = {pid}, 0
    for _ in range(3):
        tree |= {p for p, pp, _ in rows if pp in tree}
    return sum(r for p, _, r in rows if p in tree)


def end_to_end(target, repeat=150):
    if not free(8765) or not free(8766):
        return {"skipped": "ports 8765/8766 are busy on this machine"}
    root = pathlib.Path(tempfile.mkdtemp(prefix="farmlord-e2e-"))
    try:
        (root / "config.json").write_text(json.dumps({"printers": [{
            "name": "Bench-1", "ip": "127.0.0.1", "serial": "bench-serial-1", "access_code": "REPLACE_ME"}],
            "lan_access": False}))
        write_fixture(root / "printers" / "Bench-1")
        env = dict(os.environ, AMS_TRACKER_DIR=str(root), AMS_STANDBY="0", PYTHONPATH=str(GUARD),
                   BENCH_NET_LOG=str(root / "net.log"))
        t0 = time.perf_counter()
        hub = subprocess.Popen([sys.executable, str(target / "ams_tracker.py"), "run"], cwd=target, env=env,
                               stdout=open(root / "hub.out", "w"), stderr=subprocess.STDOUT)
        ready = None
        while time.perf_counter() - t0 < 60:
            try:
                if get("http://127.0.0.1:8765/api/p/Bench-1/api/rev")[0] == 200:
                    ready = time.perf_counter() - t0; break
            except Exception:
                pass
            time.sleep(0.05)
        out = {"startup to first dashboard data (s)": round(ready, 2) if ready else "never"}
        if ready:
            def lat(path, headers=None, n=repeat):
                ts = []
                for _ in range(n):
                    code, dt, body = get("http://127.0.0.1:8765" + path, headers)
                    ts.append(dt)
                ts.sort()
                return {"p50_ms": round(ts[len(ts) // 2] * 1e3, 2), "p95_ms": round(ts[int(len(ts) * .95)] * 1e3, 2),
                        "bytes": len(body), "status": code}
            gz = {"Accept-Encoding": "gzip"}
            out["GET / (page, gzip)"] = lat("/", gz, 40)
            code, _, _ = get("http://127.0.0.1:8765/", gz)
            out["GET /api/p/<printer>/api/rev (dashboard poll)"] = lat("/api/p/Bench-1/api/rev", gz)
            out["GET /api/p/<printer>/api/state"] = lat("/api/p/Bench-1/api/state", gz, 40)
            out["GET /api/attention"] = lat("/api/attention", gz)
            time.sleep(1)
            out["memory hub+worker (MB)"] = round(rss_kb(hub.pid) / 1024, 1)
        hub.terminate()
        try: hub.wait(10)
        except subprocess.TimeoutExpired: hub.kill()
        subprocess.run(["pkill", "-f", f"{target}/ams_tracker.py serve"], capture_output=True)
        log = root / "net.log"
        out["network calls from request/tick code"] = len(log.read_text().splitlines()) if log.exists() else 0
        return out
    finally:
        shutil.rmtree(root, ignore_errors=True)


# ---------------------------------------------------------------------------- driver
def code_size(target):
    files = [target / "ams_tracker.py"] + sorted((target / "farmlord").rglob("*.py")) if (target / "farmlord").exists() \
        else [target / "ams_tracker.py"]
    text = [f.read_text() for f in files]
    lines = sum(t.count("\n") for t in text)
    longest = max(len(l) for t in text for l in t.splitlines())
    py = [f for f in files]
    web = sorted((target / "farmlord" / "web").glob("*")) if (target / "farmlord" / "web").exists() else []
    return {"python files": len(py), "python lines": lines, "python KB": round(sum(len(t) for t in text) / 1024),
            "longest line (chars)": longest, "web asset files": len(web)}


def child(target, mode, dump=None):
    """Run one layer in a fresh process so imports and caches start cold, as on the printer host."""
    holder = pathlib.Path(tempfile.mkdtemp(prefix="farmlord-bench-"))
    work = holder / "Bench-1"          # a fixed printer-folder name, so outputs do not depend on the temp path
    try:
        if mode == "inproc":
            write_fixture(work)
        env = dict(os.environ, AMS_TRACKER_DIR=str(work), AMS_STANDBY="0", PYTHONPATH=str(GUARD),
                   BENCH_NET_LOG=str(work / "net.log"))
        cmd = [sys.executable, str(pathlib.Path(__file__).resolve()), "--_child", mode, "--target", str(target)]
        if dump: cmd += ["--_dump", str(pathlib.Path(dump).resolve())]
        if dump: env["BENCH_FREEZE"] = "1"
        r = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=900)
        if r.returncode:
            raise SystemExit(f"{mode} run failed for {target}:\n{r.stderr[-4000:]}")
        res = json.loads(r.stdout.strip().splitlines()[-1])
        log = work / "net.log"
        if mode == "inproc":
            res["network calls from request/tick code"] = len(log.read_text().splitlines()) if log.exists() else 0
        return res
    finally:
        shutil.rmtree(holder, ignore_errors=True)


def run_all(target):
    return {"code": code_size(target), "in-process": child(target, "inproc"), "end-to-end": child(target, "e2e")}


def fmt(v):
    if isinstance(v, dict):
        if "us" in v: return f"{v['us']:>10.1f} µs  r{v['jload']:<5g} w{v['jsave']:<4g} log{v['append']:g}"
        if "p50_ms" in v: return f"p50 {v['p50_ms']:>7.2f} ms  p95 {v['p95_ms']:>7.2f} ms  {v['bytes']} B"
    return str(v)


def table(results):
    names = list(results)
    rows = []
    for section in ("code", "in-process", "end-to-end"):
        keys = []
        for n in names:
            for k in results[n][section]:
                if k not in keys: keys.append(k)
        rows.append(f"\n== {section} " + "=" * 60)
        for k in keys:
            vals = [fmt(results[n][section].get(k, "—")) for n in names]
            line = f"{k:46s} " + "  |  ".join(f"{v:42s}" for v in vals)
            old, new = (results[names[0]][section].get(k), results[names[-1]][section].get(k)) if len(names) > 1 else (None, None)
            for unit_key in ("us", "p50_ms"):
                if isinstance(old, dict) and isinstance(new, dict) and old.get(unit_key) and new.get(unit_key):
                    line += f"  {old[unit_key] / new[unit_key]:5.1f}x faster" if new[unit_key] < old[unit_key] \
                        else f"  {new[unit_key] / old[unit_key]:5.1f}x slower"
            rows.append(line)
    head = f"{'':46s} " + "  |  ".join(f"{n:42s}" for n in names)
    return head + "\n".join(rows) + "\n\n(r = JSON file reads, w = JSON file writes, log = event-log appends; per call)"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", default=str(REPO))
    ap.add_argument("--compare", nargs=2, metavar=("OLD", "NEW"))
    ap.add_argument("--dump")
    ap.add_argument("--json", help="also write raw results here")
    ap.add_argument("--_child"); ap.add_argument("--_dump")
    a = ap.parse_args()
    if a._child:
        if os.environ.get("BENCH_FREEZE"):
            time.time = lambda: FROZEN
        target = pathlib.Path(a.target).resolve()
        res = in_process(target, a._dump) if a._child == "inproc" else end_to_end(target)
        print(json.dumps(res, default=str)); return
    if a.dump:
        print(child(pathlib.Path(a.target).resolve(), "inproc", a.dump)); return
    targets = [pathlib.Path(p).resolve() for p in (a.compare or [a.target])]
    results = {}
    for t in targets:
        print(f"benchmarking {t.name} …", file=sys.stderr, flush=True)
        results[t.name if len(targets) == 1 or targets[0].name != targets[1].name else str(t)] = run_all(t)
    if a.json: pathlib.Path(a.json).write_text(json.dumps(results, indent=1))
    print(table(results))
    # A printer command from idle work is a hard failure. Network use is reported as a number.
    bad = [n for n, r in results.items() if r["in-process"].get("_printer_commands_sent_while_idle")]
    if bad:
        print(f"\nFAIL: idle work sent printer commands in: {', '.join(bad)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

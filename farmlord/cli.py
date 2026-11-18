"""AMS drying tracker for Bambu Lab printers (H2S / AMS 2 Pro / AMS HT).

Listens to the printer's local MQTT feed, records every AMS change
(spool inserted / removed / swapped / loaded to the nozzle, heater on/off,
humidity, temperature) and keeps a running "drying account" per spool:
how many hours it has spent at or above its material's target temperature,
how long it has been exposed to humid air since, and whether it is dry enough
to print.

Usage
  ams_tracker.py run                daemon: connect + log forever (launchd)
  ams_tracker.py report             human summary of every slot / spool
  ams_tracker.py report --json      same, machine-readable
  ams_tracker.py name <spool_id> "<name>" [material]   resolve an unknown spool
  ams_tracker.py events [N]         last N events
  ams_tracker.py dried <spool_id> <hours> <temp>       manually credit an external dryer session
  ams_tracker.py same <new_id> <old_id>               untagged spool re-inserted = an existing one

Files (next to this script unless AMS_TRACKER_DIR is set)
  config.json      printer ip / serial / access code  (you fill this in once)
  snapshots.jsonl  raw AMS state every time it changes (for later re-analysis)
  events.jsonl     detected events
  spools.json      spool registry (identity, drying account)
  state.json       last seen state (used to detect changes across restarts)
"""

from farmlord.core import *  # noqa: F401,F403
from farmlord.materials import *  # noqa: F401,F403
from farmlord.ams import *  # noqa: F401,F403
from farmlord.errors import *  # noqa: F401,F403
from farmlord.tracker import *  # noqa: F401,F403
from farmlord.actions import *  # noqa: F401,F403
from farmlord.drying import *  # noqa: F401,F403
from farmlord.rfid import *  # noqa: F401,F403
from farmlord.controls import *  # noqa: F401,F403
from farmlord.alerts import *  # noqa: F401,F403
from farmlord.camera import *  # noqa: F401,F403
from farmlord.preview import *  # noqa: F401,F403
from farmlord.spools import *  # noqa: F401,F403
from farmlord.views import *  # noqa: F401,F403
from farmlord.mqtt_client import *  # noqa: F401,F403
from farmlord.pages import *  # noqa: F401,F403
from farmlord.worker import *  # noqa: F401,F403
from farmlord.hub import *  # noqa: F401,F403

def report(as_json=False):
    spools = jload(SPOOLS, {}); state = jload(STATE, {})
    ams = state.get("ams", {}); slot_spool = state.get("slot_spool", {})
    rows = []
    for ams_id in sorted(ams, key=int):
        a = ams[ams_id]
        for tray_id in sorted(a["trays"]):
            key = slot_key(ams_id, tray_id); sid = slot_spool.get(key)
            row = dict(ams=ams_label(ams_id), slot=int(tray_id), ams_temp=a["temp"], humidity=a["humidity_idx"],
                       heating=a.get("heating"), spool=None)
            if sid and sid in spools:
                s = spools[sid]; row["spool"] = dict(id=sid, name=s.get("name"), type=s["type"], needs_name=not s.get("name"), **assess(s))
            rows.append(row)
    used = {}
    root = os.path.dirname(BASE)
    try: pnames = sorted(os.listdir(root))
    except Exception: pnames = [os.path.basename(BASE)]
    for pn in pnames:
        for sp2 in (jload(os.path.join(root, pn, "spools.json"), {}) or {}).values():
            ty = (sp2.get("type") or "").strip()
            if not ty: continue
            col = (sp2.get("color") or "")[:6]
            e = used.setdefault(ty, dict(type=ty, color="", colors=[], n=0))
            e["n"] += 1
            if col and not e["color"]: e["color"] = col
            if col and col not in e["colors"]: e["colors"].append(col)
    used = sorted(used.values(), key=lambda x: -x["n"])
    unknown = [s for s in spools.values() if not s.get("name") and not s.get("removed_at")]
    out = dict(generated=iso(), last_update=iso(state.get("t")) if state.get("t") else None, slots=rows,
               unnamed=[dict(id=s["id"], type=s["type"], slot=s["slot"]) for s in unknown])
    if as_json: print(json.dumps(out, indent=1)); return
    print(f"AMS drying report — {out['generated']} (data as of {out['last_update']})\n")
    for r in rows:
        head = f"{r['ams']} slot {r['slot']}  {r['ams_temp']}°C  hum {r['humidity']}{'  HEATING' if r['heating'] else ''}"
        if not r["spool"]: print(f"{head}\n    empty"); continue
        s = r["spool"]
        nm = s["name"] or f"UNNAMED {s['type']} ({s['id']})"
        print(f"{head}\n    {nm}: {s['status']}  — {s['have_h']}/{s['need_h']} h at ≥{s['target_temp']}°C"
              f"  | exposure {s['exposure_h']} h" + (f" | last dried {s['hours_since_dry']} h ago" if s['hours_since_dry'] is not None else " | never dried here"))
    if unknown:
        print("\nUnnamed spools — tell me what they are:")
        for s in unknown: print(f"  {s['id']}  {s['type']}  in {s['slot']}")

def run(): hub()

def main():
    ap = argparse.ArgumentParser(); sub = ap.add_subparsers(dest="cmd")
    rn = sub.add_parser("run"); rn.add_argument("--open", action="store_true")
    sv = sub.add_parser("serve"); sv.add_argument("--open", action="store_true"); sv.add_argument("--port", type=int)
    r = sub.add_parser("report"); r.add_argument("--json", action="store_true")
    n = sub.add_parser("name"); n.add_argument("spool_id"); n.add_argument("name"); n.add_argument("material", nargs="?")
    e = sub.add_parser("events"); e.add_argument("n", nargs="?", type=int, default=30)
    s2 = sub.add_parser("same"); s2.add_argument("new_id"); s2.add_argument("old_id")
    d = sub.add_parser("dried"); d.add_argument("spool_id"); d.add_argument("hours", type=float); d.add_argument("temp", type=float)
    a = ap.parse_args()
    if a.cmd == "run": hub(a.open)
    elif a.cmd == "serve": serve(a.open, a.port)
    elif a.cmd == "report": report(a.json)
    elif a.cmd == "name": print(do_name(a.spool_id, a.name, a.material))
    elif a.cmd == "same": print(do_same(a.new_id, a.old_id))
    elif a.cmd == "dried": print(do_dried(a.spool_id, a.hours, a.temp))
    elif a.cmd == "events":
        try: lines = tail_lines(EVENTS, a.n)
        except FileNotFoundError: lines = []
        for l in lines:
            e = json.loads(l); print(f"{e['time']}  {e['kind']:12s} {e.get('slot') or '':7s} {e.get('msg','')}")
    else: ap.print_help()

if __name__ == "__main__":
    main()


__all__ = ['report', 'run', 'main']

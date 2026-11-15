"""Low-level printer actions shared by everything that moves filament: G-code, busy flags, safety guard."""

from farmlord.core import *  # noqa: F401,F403
from farmlord.materials import *  # noqa: F401,F403
from farmlord.ams import *  # noqa: F401,F403
from farmlord.errors import *  # noqa: F401,F403
from farmlord.tracker import *  # noqa: F401,F403

FILAMENT_BUSY_UNTIL = 0

def mark_filament_busy(seconds):
    global FILAMENT_BUSY_UNTIL
    FILAMENT_BUSY_UNTIL = max(FILAMENT_BUSY_UNTIL, now()+seconds)
    stg=settings();stg["filament_busy_until"]=FILAMENT_BUSY_UNTIL;save_settings(stg)

def filament_busy(stg):
    return now()<max(FILAMENT_BUSY_UNTIL,stg.get("filament_busy_until",0))

def send_gcode(lines):
    c = MQTT["client"]
    if not c: return "printer not connected"
    c.publish(MQTT["topic_req"], json.dumps({"print": {"sequence_id": "0", "command": "gcode_line", "param": lines}}))
    return "ok"

def toolhead_safe(st, why="this"):
    """Refuse anything that moves the toolhead or works the filament cutter unless the
    machine is in a known-good state.

    Learned the hard way: an AMS unload (M620/T255) operates the H2D's filament cutter.
    Firing it at a freshly booted, un-homed printer drove the cutter with no valid XY
    reference — the gantry fought the limits and the printer reported
    07FF_4500_0002_0002 "cutting distance too large, XY motor may lose steps".
    """
    ps = st.get("print_state")
    if STATUS.get("mqtt") != "connected" or now()-st.get("t",0)>120 or now()-st.get("print_state_t",0)>120: return "fresh printer status is unavailable"
    if ps not in ("IDLE","FINISH","FAILED"): return "a print job or printer operation is active"
    if ps == "FAILED" and unresolved_print_fault(st): return "the printer shows an unresolved fault — clear it on the screen first"
    since = st.get("seen_since") or 0
    if since and now() - since < 300:
        return "the printer only just came online — waiting until it has homed"
    # home_flag is captured but its bit layout is not documented and real values do not
    # match the obvious x/y/z encoding, so it is recorded rather than trusted.
    return None

ACTFILE = P("activity.json")

def act_start(op, first):
    jsave(ACTFILE, dict(op=op, t=now(), done=False, steps=[dict(t=now(), text=first)]))

def act_step(text, done=False, outcome=None):
    a = jload(ACTFILE, None)
    if not a: a = dict(op=text, t=now(), done=False, steps=[])
    if a.get("steps") and a["steps"][-1]["text"] == text:
        a["steps"][-1]["t"] = now()
    else:
        a.setdefault("steps", []).append(dict(t=now(), text=text))
    a["steps"] = a["steps"][-8:]
    a["done"] = bool(done); a["t"] = now()
    a["outcome"] = outcome or ("success" if done else "running")
    if done: a["op"] = text
    jsave(ACTFILE, a)

def act_clear_tick():
    a = jload(ACTFILE, None)
    if not a: return
    if not a.get("done") and now() - a.get("t", 0) > 600:      # a sequence that died (restart, error)
        a["done"] = True; a["outcome"] = "failed"; a["op"] = "Operation stopped — no further progress"
        a.setdefault("steps", []).append(dict(t=now(), text="(stopped — no further progress)"))
        jsave(ACTFILE, a); return
    if a.get("done") and now() - a.get("t", 0) > 120:
        try: os.remove(ACTFILE)
        except Exception: pass


__all__ = ['FILAMENT_BUSY_UNTIL', 'mark_filament_busy', 'filament_busy', 'send_gcode', 'toolhead_safe', 'ACTFILE',
           'act_start', 'act_step', 'act_clear_tick']

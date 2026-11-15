"""Spool registry actions: naming, moving, removing and claiming spools."""

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

def move_candidates(hours=72):
    """Spools removed from any slot on any printer within the last N hours — likely candidates for 'did you move X here?'."""
    out = []
    root = os.path.dirname(BASE)            # .../printers
    me = os.path.basename(BASE)
    try: names = sorted(os.listdir(root))
    except Exception: names = [me]
    for pn in names:
        sp = jload(os.path.join(root, pn, "spools.json"), {})
        for sid, s in sp.items():
            if s.get("removed_at") and now() - s["removed_at"] < hours * 3600 and not s.get("provisional"):
                where = s.get("last_slot") or ""
                if not where and s.get("slot"):
                    a, tr = str(s["slot"]).split(":"); where = f"{ams_label(a)} slot {int(tr)+1}"
                out.append(dict(printer=pn, id=sid, name=s.get("name") or s.get("type"), type=s.get("type"),
                                pct=assess(s)["pct"], removed_h=round((now() - s["removed_at"]) / 3600, 1), where=where,
                                remain=s.get("remain"), rfid=(s.get("rfid") or "")[-4:]))
    return out

def do_claim(prov_id, from_printer, from_id):
    """User confirms: the unidentified spool in this slot is <from_id> moved from <from_printer>. Import its history."""
    sp = jload(SPOOLS, {}); st = jload(STATE, {})
    if prov_id not in sp: return "unknown spool"
    slot = sp[prov_id].get("slot")
    src_path = os.path.join(os.path.dirname(BASE), from_printer, "spools.json")
    src = jload(src_path, {})
    if from_id not in src: return "source spool not found"
    rec = dict(src[from_id]); rec.update(slot=slot, removed_at=None, manual=True, provisional=False)
    if from_printer != os.path.basename(BASE):
        src[from_id]["moved_to"] = os.path.basename(BASE); src[from_id]["removed_at"] = src[from_id].get("removed_at") or now()
        jsave(src_path, src)
        new_id = from_id if from_id not in sp else from_id + "M"
        sp[new_id] = rec
    else:
        new_id = from_id; sp[new_id] = rec
    sp.pop(prov_id, None)
    for k, v in list(st.get("slot_spool", {}).items()):
        if v == prov_id: st["slot_spool"][k] = new_id
    jsave(SPOOLS, sp); jsave(STATE, st)
    append(EVENTS, dict(t=now(), time=iso(), kind="SPOOL_MOVE", slot=slot, spool=new_id, msg=f"{rec.get('name') or rec.get('type')} moved here from {from_printer} (history kept)"))
    return "ok"

def do_name(sid, name, material=None):
    sp = jload(SPOOLS, {})
    if sid not in sp: return "unknown spool id"
    sp[sid]["name"] = name; sp[sid]["provisional"] = False
    if material: sp[sid]["type"] = material
    jsave(SPOOLS, sp); return "ok"

def do_same(new_id, old_id):
    sp = jload(SPOOLS, {}); st = jload(STATE, {})
    if new_id not in sp or old_id not in sp: return "unknown spool id"
    new, old = sp.pop(new_id), sp[old_id]
    old.update(slot=new["slot"], removed_at=None, manual=False)
    for k, v in list(st.get("slot_spool", {}).items()):
        if v == old_id: st["slot_spool"].pop(k)          # drop stale binding (e.g. still shown in the old slot)
    for k, v in list(st.get("slot_spool", {}).items()):
        if v == new_id: st["slot_spool"][k] = old_id
    jsave(SPOOLS, sp); jsave(STATE, st)
    append(EVENTS, dict(t=now(), time=iso(), kind="SPOOL_MERGE", spool=old_id, was=new_id, msg=f"{old.get('name') or old_id} re-inserted")); return "ok"

def do_set_spool(slot, mtype, name, dried_h=0.0, dried_temp=None, color=""):
    """Record a spool for drying without changing the printer or Orca profile."""
    sp = jload(SPOOLS, {}); st = jload(STATE, {})
    old = st.get("slot_spool", {}).get(slot)
    if old and old in sp: sp[old]["removed_at"] = now(); sp[old]["slot"] = None
    sid = "S" + hashlib.sha1(f"{slot}{now()}".encode()).hexdigest()[:6].upper()
    sp[sid] = dict(id=sid, name=name or mtype, type=mtype, sub="", color=(color or "").lstrip("#")[:6].upper(), rfid="", bambu=False, manual=True,
                   first_seen=now(), slot=slot, removed_at=None, dry_credit_h=0.0, exposure_h=0.0, last_dry_end=None, history=[])
    st.setdefault("slot_spool", {})[slot] = sid
    st.get("pending", {}).pop(slot, None)
    jsave(SPOOLS, sp); jsave(STATE, st)
    append(EVENTS, dict(t=now(), time=iso(), kind="SPOOL_SET", slot=slot, spool=sid, msg=f"{name or mtype} ({mtype}) set manually in {ams_label(slot.split(':')[0])} slot {int(slot.split(':')[1])+1}"))
    stg = settings()
    stg.setdefault("cool_read", {})[slot] = dict(since=now(), temp=0, quiet=True)   # still try the tag when cool
    save_settings(stg)
    append(EVENTS, dict(t=now(), time=iso(), kind="RFID", slot=slot,
                        msg="filament set by hand; the tag will still be read when the unit is cool (for the remaining %)"))
    append(EVENTS, dict(t=now(), time=iso(), kind="SPOOL_SET", slot=slot,
                        msg="Drying record saved; printer filament profile left unchanged"))
    if dried_h: do_dried(sid, dried_h, dried_temp or mat_rule(mtype)["temp"])
    return "ok"

def do_move_spool(sid, slot):
    """Rebind a spool to another slot, keeping its drying history. Marks it manual until the printer confirms it."""
    sp = jload(SPOOLS, {}); st = jload(STATE, {})
    if sid not in sp: return "unknown spool id"
    ss = st.setdefault("slot_spool", {})
    other = ss.get(slot)
    if other and other != sid:
        if sp.get(other, {}).get("manual") or True:
            sp[other]["removed_at"] = now(); sp[other]["slot"] = None
    for k, v in list(ss.items()):
        if v == sid: ss.pop(k)
    ss[slot] = sid; s = sp[sid]; s["slot"] = slot; s["removed_at"] = None; s["manual"] = True
    jsave(SPOOLS, sp); jsave(STATE, st)
    append(EVENTS, dict(t=now(), time=iso(), kind="SPOOL_MOVE", slot=slot, spool=sid,
                        msg=f"{s.get('name') or sid} moved to {ams_label(slot.split(':')[0])} slot {int(slot.split(':')[1])+1} (history kept)")); return "ok"

def do_remove_spool(sid):
    sp = jload(SPOOLS, {}); st = jload(STATE, {})
    if sid not in sp: return "unknown spool id"
    s = sp[sid]; s["removed_at"] = now(); slot = s.get("slot"); s["slot"] = None
    for k, v in list(st.get("slot_spool", {}).items()):
        if v == sid: st["slot_spool"].pop(k)
    jsave(SPOOLS, sp); jsave(STATE, st)
    append(EVENTS, dict(t=now(), time=iso(), kind="SPOOL_OUT", slot=slot, spool=sid, msg=f"{s.get('name') or sid} removed (manually)")); return "ok"


__all__ = ['move_candidates', 'do_claim', 'do_name', 'do_same', 'do_set_spool', 'do_move_spool', 'do_remove_spool']

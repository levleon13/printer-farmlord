"""Reading AMS units from printer reports: slots, drying bits, which nozzle holds which filament."""

from farmlord.core import *  # noqa: F401,F403
from farmlord.materials import *  # noqa: F401,F403

def ams_label(ams_id):
    i = int(ams_id)
    return f"HT-{i-128}" if i >= 128 else f"AMS-{i}"

# ---------------------------------------------------------------------------
# Parsing the printer's report
# ---------------------------------------------------------------------------
def ams_info_bits(info, start, count):
    try:
        value = int(info, 16) if isinstance(info, str) else int(info)
        return (value >> start) & ((1 << count)-1)
    except (TypeError, ValueError): return None

def ams_is_drying(a):
    status = ams_info_bits(a.get("info"), 4, 4)
    if status is None: status = a.get("dry_status")
    if status is not None: return status == 2
    return (a.get("dry_time") or 0) > 0

def parse_ams(report):
    """Return {ams_id: {temp, humidity_idx, humidity_raw, dry_time, trays:{tray_id:{...}}}}, tray_now"""
    pr = report.get("print", {})
    ams = pr.get("ams")
    if not isinstance(ams, dict) or "ams" not in ams: return None, None
    out = {}
    try: jsave(P("last_ams_raw.json"), ams)
    except Exception: pass
    try: exist = int(str(ams.get("tray_exist_bits")), 16)
    except Exception: exist = None
    def bits(name):
        try: return int(str(ams.get(name, "0")), 16)
        except Exception: return 0
    read_done, reading = bits("tray_read_done_bits"), bits("tray_reading_bits")
    for a in ams["ams"]:
        trays = {}
        uid = int(a.get("id", 0))
        for t in a.get("tray", []):
            ttype = t.get("tray_type") or ""
            ident = t.get("tray_uuid") or t.get("tag_uid") or ""
            if not ident.strip("0"): ident = ""   # all-zero uid = no RFID
            empty = not ttype and not ident
            tid = int(t.get("id", 0))
            bit = (uid * 4 + tid) if uid < 128 else (16 + (uid - 128))   # one presence/read bit per HT unit, starting at bit 16
            present = bool(exist >> bit & 1) if exist is not None else (t.get("state") not in (None, 0, 10))
            rd = bool(read_done >> bit & 1); rdg = bool(reading >> bit & 1)
            # freshly inserted, RFID not read yet (happens mid-print): don't trust the slot's remembered type
            pending = present and not ident and not rd
            if pending: empty = True
            elif present and not ident and not ttype: empty = False     # filament there, no tag, no type → unidentified spool (triggers a read)
            elif not present and exist is not None: empty = True
            trays[str(t.get("id"))] = dict(
                empty=empty, type=ttype, state=t.get("state"), present=present, present_known=exist is not None, read_done=rd, reading=rdg, pending=pending, remembered_type=ttype if pending else "", drying_temp=fnum(t.get("drying_temp")) or None,
                nozzle_max=fnum(t.get("nozzle_temp_max")) or None, sub=t.get("tray_sub_brands") or "",
                color=t.get("tray_color") or "", rfid=ident, remain=t.get("remain"),
                idx=t.get("tray_info_idx") or t.get("tray_id_name") or "")
        out[str(a.get("id"))] = dict(
            temp=fnum(a.get("temp")), humidity_idx=fnum(a.get("humidity")),
            humidity_raw=fnum(a.get("humidity_raw")), dry_time=fnum(a.get("dry_time")),
            dry_sf=([int(x) for x in (a.get("dry_sf_reason") or [])] if "dry_sf_reason" in a else None),
            dry_status=ams_info_bits(a.get("info"), 4, 4),
            info=a.get("info"), dry_setting=a.get("dry_setting"), trays=trays)
    PARSE_FLAGS["scan"] = bool(reading) or str(ams.get("ams_rfid_status") or 0) not in ("0", "None")
    return out, ams.get("tray_now")

PARSE_FLAGS = {"scan": False}

def slot_key(ams_id, tray_id): return f"{ams_id}:{tray_id}"

def ams_feeding_nozzle(state, aid):
    """The nozzle holding filament from this AMS, if any. The AMS cannot turn a spool to
    scan its tag while its own filament runs through the outlet into the toolhead; the
    firmware still ACKs ams_get_rfid with success and simply never scans."""
    aid = int(aid); route = state.get("nozzle_routing")
    if route is not None:
        info = route.get("info") or {}
        for nid, item in info.items():
            try: snow = int(item["snow"])
            except (KeyError, TypeError, ValueError): continue
            if not 0 <= snow <= 65535 or (snow & 255) == 255 or ((snow >> 8) & 255) != aid: continue
            try:
                if item.get("info") is not None and not (int(item["info"]) & 2): continue
            except (TypeError, ValueError): continue
            name = ("the right nozzle" if str(nid) == "0" else "the left nozzle") if len(info) > 1 else "the nozzle"
            return dict(slot=snow & 255, nozzle=name, nid=str(nid), multi=len(info) > 1)
        return None
    if len(state.get("env", {}).get("nozzles", [])) > 1: return None
    try: tray = int(state.get("tray_now"))
    except (TypeError, ValueError): return None
    if tray < 0 or tray in (254, 255): return None
    unit, slot = (tray, 0) if 128 <= tray <= 135 else divmod(tray, 4)
    return dict(slot=slot, nozzle="the nozzle") if unit == aid else None

def ams_name(ams_id):
    """The unit name as the printer screen shows it: AMS A, AMS B, AMS HT A."""
    i = int(ams_id)
    n = i - 128 if i >= 128 else i                     # the printer screen letters units: AMS A, AMS B, AMS HT A
    letter = chr(65 + n) if 0 <= n < 26 else str(n + 1)
    return f"AMS HT {letter}" if i >= 128 else f"AMS {letter}"

def update_nozzle_routing(state, extruder):
    """Retain incremental device.extruder routing separately from legacy tray_now."""
    if not isinstance(extruder, dict): return
    route = state.setdefault("nozzle_routing", {"info": {}})
    if "state" in extruder:
        route["state"] = extruder["state"]
    for item in extruder.get("info") or []:
        if not isinstance(item, dict) or item.get("id") is None: continue
        entry = route.setdefault("info", {}).setdefault(str(item["id"]), {})
        for key in ("snow", "info", "hnow"):
            if key in item: entry[key] = item[key]

def active_nozzle_source(state):
    """Bambu Studio V2: active extruder in state bits 4..7; snow = AMS<<8 | slot.
    Do not reinterpret the legacy tray number when V2 routing is unavailable.
    This is display-only; printer commands continue through their existing guards.
    """
    unknown = dict(status="unknown", key=None)
    route = state.get("nozzle_routing")
    if route is not None:
        try:
            active = (int(route["state"]) >> 4) & 15
            item = route.get("info", {}).get(str(active), {})
            if active == 15: return unknown
            snow = int(item["snow"])
            if not 0 <= snow <= 65535: return unknown
            aid, slot = (snow >> 8) & 255, snow & 255
            # The filament sensor distinguishes retained routing from a loaded nozzle.
            if item.get("info") is not None and not (int(item["info"]) & 2):
                return dict(status="empty", key=None)
            if slot == 255: return dict(status="empty", key=None)
            if aid in (254, 255): return dict(status="external", key=None)
        except (ValueError, TypeError, KeyError): return unknown
    else:
        # Only old single-extruder telemetry can use the legacy slot numbering.
        if len(state.get("env", {}).get("nozzles", [])) > 1: return unknown
        try: tray = int(state.get("tray_now"))
        except (ValueError, TypeError): return unknown
        if tray == 255: return dict(status="empty", key=None)
        if tray == 254: return dict(status="external", key=None)
        if tray < 0: return unknown
        aid, slot = (tray, 0) if 128 <= tray <= 135 else divmod(tray, 4)
    unit = state.get("ams", {}).get(str(aid), {})
    if str(slot) not in unit.get("trays", {}): return unknown
    return dict(status="loaded", key=f"{aid}:{slot}", ams_id=aid, slot=slot)


__all__ = ['ams_label', 'ams_info_bits', 'ams_is_drying', 'parse_ams', 'PARSE_FLAGS', 'slot_key',
           'ams_feeding_nozzle', 'ams_name', 'update_nozzle_routing', 'active_nozzle_source']

#!/usr/bin/env python3
"""
AMS drying tracker for Bambu Lab printers (H2S / AMS 2 Pro / AMS HT).

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

import json, os, ssl, sys, time, uuid, signal, hashlib, argparse, datetime as dt, math
import ipaddress, urllib.parse

STANDBY = os.environ.get("AMS_STANDBY", "1") != "0"

BASE = os.environ.get("AMS_TRACKER_DIR") or os.path.dirname(os.path.abspath(__file__))
P = lambda n: os.path.join(BASE, n)
CONFIG, SNAPS, EVENTS, SPOOLS, STATE = map(P, ["config.json", "snapshots.jsonl", "events.jsonl", "spools.json", "state.json"])

# ---------------------------------------------------------------------------
# Drying targets. hours = full dry from "damp"; temp = minimum useful temp.
# rewet_h = hours at humidity index >=3 that undo ~1 hour of drying credit.
# Tune freely. Keys are matched as substrings of the AMS tray_type (upper).
# ---------------------------------------------------------------------------
MATERIALS = {
    "PA-CF": dict(temp=80, hours=10, rewet_h=2),
    "PA6":   dict(temp=80, hours=10, rewet_h=2),
    "PAHT":  dict(temp=80, hours=12, rewet_h=2),   # Bambu PAHT-CF: 80 °C, 12 h
    "PA":    dict(temp=80, hours=10, rewet_h=2),
    "PC-CF": dict(temp=80, hours=8,  rewet_h=4),   # Inherit the existing generic PC drying profile.
    "PC":    dict(temp=80, hours=8,  rewet_h=4),
    "PETG-CF": dict(temp=65, hours=8, rewet_h=6),
    "PETG":  dict(temp=65, hours=6,  rewet_h=8),
    "TPU-AMS": dict(temp=70, hours=8, rewet_h=4), # Bambu TPU for AMS (68D): Bambu spec 70 °C
    "TPU":   dict(temp=50, hours=8,  rewet_h=4),   # soft TPU: 45–55 °C, cap 55 — softens and jams above
    "ASA":   dict(temp=65, hours=4,  rewet_h=12),
    "ABS":   dict(temp=65, hours=4,  rewet_h=12),
    "PLA":   dict(temp=55, hours=4,  rewet_h=24),
    "PVA":   dict(temp=55, hours=8,  rewet_h=1),
}
DEFAULT_MAT = dict(temp=65, hours=6, rewet_h=8)
TEMP_TOLERANCE = 5        # degC below target still counts (AMS sensor is air temp)
HEATER_DELTA = 8          # AMS temp this far above ambient => heater considered ON
HUMID_BAD_RH = 35         # % RH above this counts as exposure (AMS 2 Pro / HT report raw RH)
HUMID_BAD_IDX = 2         # fallback for old AMS: index 5 = driest, 1 = wettest; <= this counts as exposure
SNAPSHOT_MIN_INTERVAL = 60  # s; also written on every detected change


def now(): return time.time()
def iso(t=None): return dt.datetime.fromtimestamp(t or now()).strftime("%Y-%m-%d %H:%M:%S")
def jload(p, d):
    try:
        with open(p) as f: return json.load(f)
    except Exception: return d
def jsave(p, o):
    tmp = p + "." + uuid.uuid4().hex + ".tmp"
    # 0600: config files hold printer access codes and the dashboard password.
    with os.fdopen(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f: json.dump(o, f, indent=1)
    os.replace(tmp, p)

MAX_BODY = 256 * 1024
EXTRA_HOSTS = set()   # config.json "allowed_hosts": extra DNS names that may address the dashboard
LOGIN_FAILS = {}      # client ip -> [failures, locked_until]
def host_ok(host):
    """Refuse DNS rebinding: only IP literals, local/tailnet names and configured hosts may address this server."""
    h = (host or "").strip().lower()
    if h.startswith("["): h = h[1:].split("]", 1)[0]
    elif h.count(":") == 1: h = h.split(":", 1)[0]
    if not h: return False
    try: ipaddress.ip_address(h); return True
    except ValueError: pass
    return "." not in h or h.endswith((".local", ".localhost", ".ts.net", ".home.arpa", ".lan", ".internal")) or h in EXTRA_HOSTS
def json_post_ok(headers):
    """Other sites cannot send application/json here without a CORS preflight, which this server never grants."""
    if headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json": return False
    origin = headers.get("Origin")
    return not origin or urllib.parse.urlsplit(origin).netloc == headers.get("Host")
def read_body(headers, rfile, limit=MAX_BODY):
    try: n = int(headers.get("Content-Length") or 0)
    except ValueError: return None
    if n < 0 or n > limit: return None
    return rfile.read(n)
def printer_name_ok(base, name):
    """Printer names become directory names under BASE/printers; keep them there."""
    if not name or len(name) > 64 or name.startswith(".") or any(c in name for c in "/\\") or any(ord(c) < 32 for c in name): return False
    root = os.path.realpath(os.path.join(base, "printers"))
    return os.path.dirname(os.path.realpath(os.path.join(root, name))) == root
REPORT_URL = "https://github.com/levleon13/printer-farmlord/issues/new"
def scrub(text):
    """Blank out IPs, serial-like codes and home-folder names before anything leaves this machine."""
    import re
    text = re.sub(r"\b\d{1,3}(?:\.\d{1,3}){3}\b", "<ip>", str(text))
    text = re.sub(r"\b(?=[0-9A-Z]*\d)(?=[0-9A-Z]*[A-Z])[0-9A-Z]{12,16}\b", "<serial>", text)
    return re.sub(r"/(?:Users|home)/[^/\s]+", "~", text)[-1500:]
def report_url(title, details=""):
    """A prefilled GitHub issue; the user reviews it and clicks Submit, nothing is sent automatically."""
    import platform
    system = f"{platform.system()} {platform.release()} ({platform.machine()}) · Python {platform.python_version()}"
    return REPORT_URL + "?" + urllib.parse.urlencode(dict(template="problem.yml", title=title, details=scrub(details), system=system))
def store_name():
    n = str(jload(CONFIG, {}).get("stock_store_name") or "")
    return "".join(c for c in n if c.isalnum() or c in " .&-")[:40]
def public_printer_config(c):
    """Never send LAN access codes to the browser."""
    c = dict(c); c["access_code_set"] = bool(c.pop("access_code", "")); return c
def append(p, o):
    with open(p, "a") as f: f.write(json.dumps(o) + "\n")
def fnum(x, d=None):
    try: return float(x)
    except Exception: return d


def mat_rule(tray_type):
    t = (tray_type or "").upper()
    for k in sorted(MATERIALS, key=len, reverse=True):
        if k in t: return dict(MATERIALS[k], key=k)
    return dict(DEFAULT_MAT, key="?")


def spool_rule(s):
    """Material rule for a spool; a Bambu RFID drying temperature overrides the table's target."""
    r = mat_rule(s.get("type"))
    if s.get("tag_dry_temp"): r = dict(r, temp=int(s["tag_dry_temp"]), tag=True)
    return r


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


def dry_stop_reason_text(code):
    if code == 8:
        return "External drying power was lost mid-cycle. The AMS reported its power adapter unavailable when heating stopped (code 8)."
    return f"Printer reported at the stop: {DRY_REASON_TEXT[code]} (code {code})."


def dry_stop_firmware_reason(before, after):
    """Describe a new AMS restriction at heater-off."""
    new = set((after or {}).get("dry_sf") or []) - set((before or {}).get("dry_sf") or [])
    for code in (8, 1, 5, 3, 10, 0, 2, 7, 4):
        if code in new:
            return code, dry_stop_reason_text(code)
    return None, None


def recorded_dry_stop_reason(aid, ended_at):
    """Find a stop-time firmware reason for a cycle saved by an older version."""
    before = after = None
    try:
        with open(SNAPS) as snapshots:
            for line in snapshots:
                row = json.loads(line)
                stamp = row.get("t", 0)
                if stamp < ended_at - 180: continue
                if stamp > ended_at + 180: break
                unit = (row.get("ams") or {}).get(str(aid))
                if not unit: continue
                if unit.get("heating") and stamp <= ended_at:
                    before = unit
                elif before and not unit.get("heating") and after is None:
                    after = unit
    except (OSError, ValueError, TypeError):
        return None, None
    return dry_stop_firmware_reason(before, after) if before and after else (None, None)


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


def humid_exposed(a):
    """True when the AMS air is humid enough to re-wet filament."""
    if a.get("humidity_raw") is not None: return a["humidity_raw"] > HUMID_BAD_RH
    hi = a.get("humidity_idx")
    return hi is not None and hi <= HUMID_BAD_IDX


# ---------------------------------------------------------------------------
# Tracker state machine
# ---------------------------------------------------------------------------
class Tracker:
    def __init__(self):
        self.state = jload(STATE, {})
        self.spools = jload(SPOOLS, {})
        self.last_snapshot = 0
        self.ambient = self.state.get("ambient")  # slow-moving min AMS temp estimate

    # ---- spool registry ----
    def find_spool_for_slot(self, key, tray):
        """Match by RFID first, else by the spool previously bound to this slot if material matches."""
        if tray["rfid"]:
            for sid, s in self.spools.items():
                if s.get("rfid") == tray["rfid"]: return sid
        bound = self.state.get("slot_spool", {}).get(key)
        if bound and bound in self.spools:
            s = self.spools[bound]
            if not s.get("provisional") and (s.get("type") or "").upper() == tray["type"].upper() and not s.get("removed_at") and not (s.get("rfid") and tray["rfid"] and s["rfid"] != tray["rfid"]):
                return bound
            if s.get("provisional") and not s.get("removed_at") and not tray["rfid"]:
                return bound          # still the same unidentified spool
        return None

    def new_spool(self, key, tray, ams_id):
        sid = "S" + hashlib.sha1(f"{key}{now()}".encode()).hexdigest()[:6].upper()
        known = bool(tray["rfid"])
        self.spools[sid] = dict(
            id=sid, name=(f"{tray['sub'] or tray['type']}" if known else None),
            type=tray["type"], sub=tray["sub"], color=tray["color"], rfid=tray["rfid"],
            bambu=known, first_seen=now(), slot=key, removed_at=None,
            dry_credit_h=0.0, exposure_h=0.0, last_dry_end=None, history=[])
        self.event("SPOOL_NEW", key, spool=sid, type=tray["type"], sub=tray["sub"],
                   rfid=tray["rfid"], needs_name=not known,
                   msg=f"New spool in {ams_label(ams_id)} slot {key.split(':')[1]}: {tray['type']} "
                       + ("(Bambu RFID)" if known else "(no RFID read — type is the slot's memory, UNVERIFIED)"))
        return sid

    def event(self, kind, slot=None, **kw):
        e = dict(t=now(), time=iso(), kind=kind, slot=slot, **kw)
        append(EVENTS, e)
        print(f"[{e['time']}] {kind:12s} {slot or '':6s} {kw.get('msg','')}", flush=True)

    # ---- main update ----
    def _reload_if_changed(self):
        """The dashboard API edits spools.json/state.json from other threads/processes: pick those edits up."""
        try: m = (os.path.getmtime(SPOOLS), os.path.getmtime(STATE))
        except FileNotFoundError: return
        if m != getattr(self, "_mtimes", None):
            disk_sp = jload(SPOOLS, None); disk_st = jload(STATE, None)
            if disk_sp is not None: self.spools = disk_sp
            if disk_st is not None:
                for key in ("slot_spool", "pending"):
                    if key in disk_st: self.state[key] = disk_st[key]
            self._mtimes = m

    def _save(self):
        jsave(STATE, self.state); jsave(SPOOLS, self.spools)
        try: self._mtimes = (os.path.getmtime(SPOOLS), os.path.getmtime(STATE))
        except FileNotFoundError: pass

    def update(self, ams, tray_now):
        self._reload_if_changed()
        t = now()
        prev = self.state.get("ams", {})
        prev_t = self.state.get("t", t)
        gap_h = (t - prev_t) / 3600.0
        dt_h = max(0.0, min(t - prev_t, 3600)) / 3600.0  # normal accrual capped at 1 h
        changed = False
        if gap_h > 1.0:
            self._backfill_gap(prev, ams, gap_h, t); dt_h = 0.0

        # ambient estimate: min temp across non-heating units, slow decay
        temps = [a["temp"] for a in ams.values() if a["temp"] is not None]
        if temps:
            m = min(temps)
            self.ambient = m if self.ambient is None else min(self.ambient * 0.98 + m * 0.02, m + 3)

        slot_spool = self.state.setdefault("slot_spool", {})

        for ams_id, a in ams.items():
            pa = prev.get(ams_id, {})
            # ---- heater / drying detection ----
            heating = ams_is_drying(a)  # firmware state beats a leftover countdown or warm air
            was = pa.get("heating", False)
            cycle = pa.get("dry_cycle")
            history = list(pa.get("dry_cycles") or [])[-7:]
            remaining = a.get("dry_time")
            if history and history[-1].get("reason_code") in DRY_REASON_TEXT:
                code = history[-1]["reason_code"]
                history[-1] = dict(history[-1], reason=dry_stop_reason_text(code))
            if history and history[-1].get("legacy") and history[-1].get("outcome") == "stopped" and not history[-1].get("reason_checked"):
                code, stop_reason = recorded_dry_stop_reason(ams_id, history[-1].get("ended_at", 0))
                history[-1] = dict(history[-1], reason_checked=True)
                if stop_reason: history[-1].update(reason=stop_reason, reason_code=code)
            if not history and not heating and not was:
                # Preserve a previously observed result when upgrading; do not
                # invent outcomes for requests that never reached the heater.
                old = (jload(DRY_REQUESTS, {}) or {}).get(ams_id, {})
                left = fnum(old.get("last_dry_time"))
                if old.get("status") in ("completed", "cancelled") and old.get("last_observed") and left is not None:
                    outcome = "completed" if old["status"] == "completed" and left <= 1.5 else "stopped" if left > 1.5 else "unknown"
                    reason = ("The earlier dashboard observed the printer's countdown reach its end" if outcome == "completed" else
                              "The earlier dashboard recorded a stop with time remaining, but no cause was reported" if outcome == "stopped" else
                              "The earlier dashboard did not record enough detail to confirm the end")
                    history.append(dict(started_at=None, first_seen_at=old["last_observed"],
                                        ended_at=old.get("last") or t, planned_min=fnum(old.get("hours"), 0) * 60,
                                        last_remaining_min=left, outcome=outcome, reason=reason, legacy=True))
            if heating:
                if not was or not cycle:
                    duration = fnum((a.get("dry_setting") or {}).get("dry_duration"))
                    cycle = dict(started_at=t if not was else None, first_seen_at=t,
                                 planned_min=duration * 60 if duration and duration > 0 else None,
                                 first_remaining_min=remaining, last_remaining_min=remaining,
                                 telemetry_gap=False)
                else:
                    cycle = dict(cycle)
                    cycle["last_remaining_min"] = remaining
                    if t - prev_t > 120: cycle["telemetry_gap"] = True
                a["dry_cycle"] = cycle
            elif was:
                cycle = dict(cycle or dict(started_at=None, first_seen_at=prev_t,
                                           planned_min=None, first_remaining_min=pa.get("dry_time")))
                left = fnum(pa.get("dry_time"))
                gap = t - prev_t
                request = (jload(DRY_REQUESTS, {}) or {}).get(ams_id, {})
                stopped_here = request.get("status") in ("held", "stopped") and abs(t - request.get("last", 0)) < 300
                reason_code = None
                if stopped_here:
                    outcome, reason = "stopped", "Stopped from the dashboard before the timer finished"
                elif cycle.get("telemetry_gap") or gap > 120 or left is None:
                    outcome, reason = "unknown", "Readings were missing, so the end of this cycle cannot be confirmed"
                elif left <= 1.5:
                    outcome, reason = "completed", "The printer's drying countdown reached its end"
                else:
                    outcome = "stopped"
                    reason_code, firmware_reason = dry_stop_firmware_reason(pa, a)
                    reason = firmware_reason or "The heater stopped before the printer's drying countdown ended; the printer did not report why"
                history.append(dict(cycle, ended_at=t, outcome=outcome, reason=reason,
                                    last_remaining_min=left, reason_code=reason_code))
                history = history[-8:]
                self.event("DRY_RESULT", f"{ams_id}:*", outcome=outcome, remaining_min=left,
                           msg=f"{ams_label(ams_id)}: {reason}")
            if history: a["dry_cycles"] = history
            if heating != was:
                changed = True
                self.event("DRY_START" if heating else "DRY_STOP", f"{ams_id}:*",
                           temp=a["temp"], humidity=a["humidity_idx"],
                           msg=f"{ams_label(ams_id)} heater {'ON' if heating else 'OFF'} at {a['temp']}°C, humidity idx {a['humidity_idx']}")
            a["heating"] = heating

            # humidity index step
            raw_prev = pa.get("humidity_raw_logged", pa.get("humidity_raw"))
            a["humidity_raw_logged"] = raw_prev
            sig = (a["humidity_raw"] is not None and raw_prev is not None and abs(a["humidity_raw"] - raw_prev) >= 3) or \
                  (a["humidity_raw"] is None and pa.get("humidity_idx") is not None and a["humidity_idx"] != pa.get("humidity_idx"))
            if sig:
                changed = True; a["humidity_raw_logged"] = a["humidity_raw"]
                self.event("HUMIDITY", f"{ams_id}:*", frm=pa.get("humidity_idx"), to=a["humidity_idx"],
                           raw=a["humidity_raw"], msg=f"{ams_label(ams_id)} humidity {raw_prev}% → {a['humidity_raw']}% RH (index {a['humidity_idx']})")

            # ---- trays ----
            for tray_id, tr in a["trays"].items():
                key = slot_key(ams_id, tray_id)
                ptr = pa.get("trays", {}).get(tray_id, {})
                cur_sid = slot_spool.get(key)

                s = self.spools.get(cur_sid, {})
                user_set = bool(s and not s.get("removed_at") and (s.get("manual") or s.get("from_printer")))
                # A user's assignment outranks stale material/read status, including
                # the RFID timeout during a print. Only actual removal or a new
                # known tag ends that binding; keep the raw telemetry unchanged.
                manual_removed = user_set and tr.get("present_known", True) and tr.get("present") is False and ptr.get("present") is True
                new_tag = bool(s.get("rfid") and tr["rfid"] and s["rfid"] != tr["rfid"])
                if user_set and not manual_removed and not new_tag:
                    # Firmware can retain the previous spool's UID after a no-tag scan.
                    # Never attach that cached UID (or its remaining percentage) to a manually set spool.
                    no_tag_read = tr.get("state") == 9 and tr.get("read_done")
                    if s.get("manual") and no_tag_read and (s.get("rfid") or s.get("remain") is not None):
                        s["rfid"] = ""; s.pop("remain", None); changed = True
                    tr = dict(tr, empty=False, type=s["type"], pending=False, manual=True)
                    if tr["rfid"] and not s.get("rfid") and not s.get("manual") and not no_tag_read: s["rfid"] = tr["rfid"]
                if tr.get("pending"):
                    pend = self.state.setdefault("pending", {})
                    since = pend.setdefault(key, t)
                    if t - since < 180:
                        continue          # give the AMS time to read the tag
                    # no tag after 3 min: create a provisional spool from the printer's remembered type (user must confirm)
                    tr = dict(tr, empty=False, type=tr.get("remembered_type") or "", pending=False)
                    if not (cur_sid and self.spools.get(cur_sid, {}).get("provisional")):
                        pend.pop(key, None)
                        sid = self.new_spool(key, tr, ams_id); self.spools[sid]["provisional"] = True
                        self.event("SPOOL_UNVERIFIED", key, spool=sid, msg=f"{ams_label(ams_id)} slot {int(tray_id)+1}: no RFID read — type '{tr['type'] or '?'}' is the slot's old memory, please confirm")
                        slot_spool[key] = sid; cur_sid = sid; changed = True
                elif key in self.state.get("pending", {}):
                    self.state["pending"].pop(key, None)
                if tr["empty"]:
                    if cur_sid and (not ptr.get("empty", True) or manual_removed):
                        changed = True
                        self.spools[cur_sid]["removed_at"] = t
                        self.spools[cur_sid]["last_slot"] = f"{ams_label(ams_id)} slot {int(tray_id)+1}"
                        self.spools[cur_sid]["slot"] = None
                        self.event("SPOOL_OUT", key, spool=cur_sid,
                                   msg=f"{self.spool_name(cur_sid)} removed from {ams_label(ams_id)} slot {tray_id}")
                        slot_spool.pop(key, None)
                    continue

                # occupied
                identity_changed = (ptr.get("empty", True) or ptr.get("rfid") != tr["rfid"] or ptr.get("type") != tr["type"] or (cur_sid and self.spools.get(cur_sid, {}).get("removed_at")))
                if cur_sid and self.spools.get(cur_sid, {}).get("provisional") and tr["rfid"]:
                    self.spools[cur_sid]["removed_at"] = t; self.spools[cur_sid]["slot"] = None; slot_spool.pop(key, None); cur_sid = None; identity_changed = True
                if tr.get("manual"): identity_changed = False
                if (identity_changed or not cur_sid) and not tr.get("manual"):
                    sid = self.find_spool_for_slot(key, tr)
                    if sid and cur_sid and sid != cur_sid:
                        self.spools[cur_sid]["removed_at"] = t; self.spools[cur_sid]["last_slot"] = f"{ams_label(ams_id)} slot {int(tray_id)+1}"; self.spools[cur_sid]["slot"] = None
                        self.event("SPOOL_OUT", key, spool=cur_sid, msg=f"{self.spool_name(cur_sid)} removed")
                    if sid is None:
                        if cur_sid:
                            self.spools[cur_sid]["removed_at"] = t; self.spools[cur_sid]["last_slot"] = f"{ams_label(ams_id)} slot {int(tray_id)+1}"; self.spools[cur_sid]["slot"] = None
                            self.event("SPOOL_OUT", key, spool=cur_sid, msg=f"{self.spool_name(cur_sid)} removed")
                        if tr["rfid"]:
                            sid = self.new_spool(key, tr, ams_id)
                        else:
                            hint = tr["type"]
                            sid = self.new_spool(key, dict(tr, type="", sub=""), ams_id)
                            self.spools[sid].update(provisional=True, hint_type=hint, name=None)
                            # The bounded periodic reader runs after telemetry is
                            # saved; never move filament from a parse callback.
                    elif sid != cur_sid:
                        self.event("SPOOL_IN", key, spool=sid, msg=f"{self.spool_name(sid)} inserted in {ams_label(ams_id)} slot {tray_id}")
                    s = self.spools[sid]; s["slot"] = key; s["removed_at"] = None
                    if not s.get("provisional"): s["type"] = s["type"] or tr["type"]
                    s["rfid"] = s["rfid"] or tr["rfid"]
                    if tr.get("drying_temp"): s["tag_dry_temp"] = tr["drying_temp"]
                    slot_spool[key] = sid; cur_sid = sid; changed = True

                # ---- accrue drying / exposure for the spool sitting here ----
                s = self.spools[cur_sid]
                if s.get("provisional"): continue
                if tr.get("drying_temp"): s["tag_dry_temp"] = tr["drying_temp"]
                if isinstance(tr.get("remain"), (int, float)) and tr["remain"] >= 0 and not (s.get("manual") and tr.get("state") == 9 and tr.get("read_done")): s["remain"] = tr["remain"]
                rule = spool_rule(s)
                credit_heating=heating and pa.get("heating") and not identity_changed and 0<dt_h<=120/3600
                credit_temp=min(a["temp"],pa["temp"]) if a.get("temp") is not None and pa.get("temp") is not None else None
                if dt_h > 0:
                    if credit_heating and credit_temp is not None and credit_temp >= rule["temp"] - 1:
                        s["dry_credit_h"] = min(s["dry_credit_h"] + dt_h, rule["hours"] * 1.5)
                        s["last_dry_end"] = t
                        s["last_dry_temp"] = a["temp"]
                    elif credit_heating and credit_temp is not None:
                        # heater on but below this material's target: partial credit
                        # (e.g. PETG-CF at 55 while printing: firmware clamp → 60 % of the drying rate)
                        frac = credit_rate(rule, credit_temp)
                        s["dry_credit_h"] = min(s["dry_credit_h"] + dt_h * frac, rule["hours"] * 1.5)
                        s["last_dry_temp"] = a["temp"]
                        s.setdefault("underheated_h", 0.0); s["underheated_h"] += dt_h
                    else:
                        if not heating and humid_exposed(a):
                            s["exposure_h"] += dt_h
                            s["dry_credit_h"] = max(0.0, s["dry_credit_h"] - dt_h / rule["rewet_h"])

        # ---- loaded-to-nozzle ----
        scanning = bool(PARSE_FLAGS.get("scan"))
        if scanning:
            if t >= self.state.get("scan_until", 0):
                self.event("AMS_SCAN", None, msg="AMS is reading spool tags — load/unload activity from the scan is not logged")
            self.state["scan_until"] = t + 90                      # power-on tag scan: ignore its load/unload churn
        quiet = t < self.state.get("scan_until", 0)
        if tray_now is not None and tray_now != self.state.get("tray_now"):
            changed = True
            tn = int(tray_now)
            if quiet:
                pass                                              # AMS is scanning tags, not really loading
            elif tn == 255:
                self.event("UNLOAD", None, msg="No filament loaded")
            else:
                ams_id, tray_id = tn // 4, tn % 4
                if ams_id * 4 + tray_id != tn or str(ams_id) not in ams:  # HT units use id>=128
                    for aid in ams:
                        if int(aid) * 4 <= tn < int(aid) * 4 + 4: ams_id, tray_id = int(aid), tn - int(aid) * 4
                key = slot_key(ams_id, tray_id)
                if str(ams_id) in ams and not quiet:
                    sid = slot_spool.get(key)
                    self.event("LOAD", key, spool=sid, msg=f"Loaded {self.spool_name(sid) if sid else key} to extruder")

        self.state.update(ams=ams, tray_now=tray_now, t=t, ambient=self.ambient)
        self._save()
        if changed or t - self.last_snapshot >= SNAPSHOT_MIN_INTERVAL:
            append(SNAPS, dict(t=t, time=iso(t), ams=ams, tray_now=tray_now)); self.last_snapshot = t

    def _backfill_gap(self, prev, ams, gap_h, t):
        """Unobserved temperatures, swaps and interruptions cannot establish drying credit."""
        for aid,a in ams.items():
            for tid in a.get("trays",{}):
                sid=self.state.get("slot_spool",{}).get(slot_key(aid,tid));sp=self.spools.get(sid)
                if sp:sp["unobserved_h"]=sp.get("unobserved_h",0)+gap_h
        self.event("GAP",msg=f"No drying credit added for {gap_h:.1f} hours without telemetry")

    def spool_name(self, sid):
        s = self.spools.get(sid, {})
        return s.get("name") or f"{s.get('type','?')} [{sid}]"


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def credit_rate(rule, temp):
    """Fraction of drying credit earned per hour at a given AMS air temperature."""
    if temp is None: return 0.0
    if temp >= rule["temp"] - 1: return 1.0          # 1 °C of sensor slack only
    if temp < 40: return 0.0
    return round(0.5 ** ((rule["temp"] - temp) / 15.0), 2)   # halves every 15 °C below target: 60→55 = 79 %, 80→65 = 50 %


def assess(s, unit_temp=None, heating=False):
    rule = spool_rule(s)
    need = rule["hours"]; have = s["dry_credit_h"]
    pct = min(100, int(100 * have / need)) if need else 100
    since = (now() - s["last_dry_end"]) / 3600 if s.get("last_dry_end") else None
    if pct >= 100: status = "DRY"
    elif pct >= 60: status = "MARGINAL"
    else: status = "NEEDS DRYING"
    if s.get("last_dry_temp") is not None and s["last_dry_temp"] < rule["temp"] - TEMP_TOLERANCE and status != "DRY":
        status += " (temp too low: %.0f < %d)" % (s["last_dry_temp"], rule["temp"])
    rate = credit_rate(rule, unit_temp) if heating else None
    eta = round(max(0, need - have) / rate, 1) if rate else None
    return dict(status=status, pct=pct, have_h=round(have, 1), need_h=need, target_temp=rule["temp"],
                exposure_h=round(s["exposure_h"], 1), hours_since_dry=None if since is None else round(since, 1),
                remaining_h=round(max(0, need - have), 1), rate=rate, eta_h=eta)


STATUS = {"mqtt": "starting", "since": now(), "error": None}
MQTT = {"client": None, "topic_req": None}
import threading as _th
LOCK = _th.RLock()

# drying presets shown in the dashboard: (temp °C, hours). AMS 2 Pro max 65 °C, AMS HT max 85 °C.
DRY_PRESETS = {"PETG-CF": (65, 8), "PETG": (65, 6), "PLA": (55, 4), "TPU": (60, 8), "ASA/ABS": (65, 4),
               "PA/PA-CF (HT)": (80, 10), "PC (HT)": (80, 8)}


SETTINGS = P("settings.json")
AUTO_DEFAULT = {"auto": {}, "manual_hold_until": {}}
# safe max drying temp per material (so PLA next to PETG-CF is not deformed). Missing => rule temp.
SAFE_MAX = {"PLA": 55, "TPU": 55, "TPU-AMS": 70, "PETG": 65, "PETG-CF": 65, "ASA": 65, "ABS": 65, "PA": 80, "PA-CF": 80, "PA6": 80, "PAHT": 80, "PC-CF": 80, "PC": 80, "PVA": 55}
UNIT_MAX = lambda ams_id: 85 if int(ams_id) >= 128 else 65
# Conservative request cap while printing on the H2 fleet's shared supply.
# Firmware can reduce power/temperature further or refuse the request. Idle
# drying retains the material/unit limits; AMS HT has its own supply.
PRINT_SHARED_MAX = 55
AUTO_START_PCT = 85       # start a cycle when any spool's dryness falls below this
AUTO_MIN_HOURS, AUTO_MAX_HOURS = 2, 12
MANUAL_HOLD_H = 6         # after a manual Stop, auto-dry leaves that unit alone this long
AUTO_HUMIDITY_RH = 20     # ambient AMS humidity can call for drying even at 100% tracked dryness
AUTO_HUMIDITY_DWELL = 10 * 60
AUTO_HUMIDITY_COOLDOWN = 24 * 3600
AUTO_HUMIDITY_STATE = P("auto_humidity.json")


def settings(): return jload(SETTINGS, {"auto": {}, "manual_hold_until": {}})
def save_settings(o): jsave(SETTINGS, o)


# Request state has its own lock/file: MQTT replies must not race unrelated settings writes.
DRY_REQUESTS = P("dry_requests.json")
DRY_LOCK = _th.RLock()
DRY_MAX_ATTEMPTS = 3
FILAMENT_BUSY_UNTIL = 0


def mark_filament_busy(seconds):
    global FILAMENT_BUSY_UNTIL
    FILAMENT_BUSY_UNTIL = max(FILAMENT_BUSY_UNTIL, now()+seconds)
    stg=settings();stg["filament_busy_until"]=FILAMENT_BUSY_UNTIL;save_settings(stg)


def filament_busy(stg):
    return now()<max(FILAMENT_BUSY_UNTIL,stg.get("filament_busy_until",0))



def dry_requests():
    with DRY_LOCK:
        saved = jload(DRY_REQUESTS, None)
        if saved is not None:
            if not isinstance(saved, dict): raise ValueError("Drying request state is invalid; automatic control stopped")
            return saved
        if os.path.exists(DRY_REQUESTS): raise ValueError("Drying request state could not be read; automatic control stopped")
        # Adopt in-flight requests once, preserving their attempt budget across upgrades.
        saved = {}
        for aid, old in (settings().get("dry_pending") or {}).items():
            sf = old.get("sf") or []
            waiting = old.get("err") in DRY_WAIT_CODES or (sf and set(sf).issubset(DRY_WAIT_REASONS))
            saved[aid] = dict(old, status="waiting" if waiting else "pending",
                              wait_context=dry_wait_context(aid, jload(STATE, {})),
                              attempts=max(old.get("tries", 0), old.get("step", 0)),
                              last=old.get("last") or old.get("since") or now(), sequence=None)
        jsave(DRY_REQUESTS, saved)
        return saved


DRY_WAIT_REASONS = {0, 1, 2, 4, 6, 7, 9}
DRY_REASON_TEXT = {
    0: "The printer is busy with another task", 1: "Waiting for drying power",
    2: "The AMS is loading, unloading, calibrating, or reading a tag", 3: "Filament remains in the AMS outlet; waiting for a safe retract",
    4: "The printer is starting a drying cycle", 5: "Drying is unavailable in the printer's current operating mode",
    6: "A drying cycle is already running", 7: "The printer or AMS is updating",
    8: "The AMS needs an external power adapter", 9: "The AMS is temporarily unavailable after stopping; waiting for it to become ready",
    10: "The printer reports filament at the AMS outlet; drying is waiting for clearance"}


def unresolved_print_fault(state):
    """A canceled job is terminal, but its 0300_400C echo is not a fault."""
    try: code = int(state.get("print_error") or 0)
    except (TypeError, ValueError): code = 0
    fatal_hms = any(((int(h.get("code") or 0) >> 16) & 0xFFFF) == 1 for h in (state.get("hms") or []))
    return (code not in (0, 0x0300400C)) or fatal_hms


def drying_readiness(aid, state, stg, resume_saved=False):
    """Returns (ready/wait/blocked, explanation). Never overrides a firmware interlock."""
    if STATUS.get("mqtt") != "connected" or not MQTT.get("client"): return "wait", "Waiting for the printer connection"
    connected = STATUS.get("since", 0)
    if any(now()-state.get(k, 0)>120 or state.get(k, 0)<connected for k in ("t", "print_state_t")):
        return "wait", "Waiting for fresh printer and AMS status"
    if dry_unload_busy(): return "wait", "Waiting for the printer to finish retracting filament"
    a = state.get("ams", {}).get(str(aid))
    if not a: return "wait", "Waiting for the AMS to reconnect"
    if ams_info_bits(a.get("info"), 0, 4) not in (3, 4): return "blocked", "This AMS has not reported support for active drying"
    ps = state.get("print_state")
    # Bambu Studio treats FAILED as a finished job (including cancellation),
    # not an in-progress operation. Keep actual faults and AMS interlocks below.
    # https://github.com/bambulab/BambuStudio/blob/master/src/slic3r/GUI/DeviceManager.cpp
    if ps == "FAILED" and unresolved_print_fault(state):
        return "blocked", "The last print stopped with an unresolved printer fault. Check and clear it on the printer screen before retrying drying"
    if ps == "PAUSE": return "wait", "The print is paused. Resume or end it on the printer before starting drying"
    if ps in ("PREPARE", "SLICING"): return "wait", "Waiting for print preparation to finish"
    if ps not in ("IDLE", "FINISH", "FAILED", "RUNNING"): return "wait", "Waiting for a recognized printer operating state"
    if stg.get("rfid_pause_busy") or stg.get("retry_filament") or filament_busy(stg) or now()<state.get("scan_until", 0):
        return "wait", "Waiting for the filament operation to finish"
    tracked = jload(SPOOLS,{})
    for tid, tray in a.get("trays", {}).items():
        spool = tracked.get(state.get("slot_spool",{}).get(slot_key(aid,tid)),{})
        manually_known = spool.get("manual") and not spool.get("provisional") and spool_rule(spool)["key"] != "?"
        if tray.get("reading") or (tray.get("pending") and not manually_known and not resume_saved): return "wait", "Waiting for the AMS tag scan"
    reasons = a.get("dry_sf")
    if reasons is None: return "wait", "Waiting for AMS drying availability"
    for reason in reasons:
        if reason not in DRY_WAIT_REASONS: return "blocked", DRY_REASON_TEXT.get(reason, "The printer reports an unknown drying restriction; check its screen")
    if reasons: return "wait", DRY_REASON_TEXT.get(reasons[0], "Waiting for the AMS")
    phase = ams_info_bits(a.get("info"), 4, 4)
    if phase in (5, 6): return "blocked", "The AMS reports a drying fault; check its screen before retrying"
    if phase not in (0,): return "wait", "Waiting for the current AMS cycle to finish"
    busy = dry_slot_taken_by(aid, state, stg)
    if busy is not None: return "wait", f"Waiting for {ams_label(busy)} to release the printer's drying power"
    return "ready", ""


def dry_is_held(w):
    return w.get("status") in ("setup", "preparing", "unloading", "pending", "waiting", "blocked", "cancelled", "retry_ready") or (
        w.get("status") == "held" and now() < w.get("until", 0))


def dry_wait_context(aid, state):
    a = state.get("ams", {}).get(aid, {})
    return [state.get("print_state"), sorted(a.get("dry_sf") or []),
            sorted(k for k, v in state.get("ams", {}).items() if v.get("heating")),
            has_psu(settings(), aid)]


def dry_block(aid, w, message):
    w.update(status="blocked", message=message, failed_at=now())
    if "filament" in message.lower() and "outlet" in message.lower():
        w["block_kind"] = "outlet"
    act_step(f"Drying blocked — {message}", done=True, outcome="failed")
    append(EVENTS, dict(t=now(), time=iso(), kind="DRY_CMD", slot=f"{aid}:*",
                        msg=f"{ams_label(aid)}: drying blocked. {message}"))


def cancel_dry_request(aid, manual_stop=False, automatic_stop=False):
    with DRY_LOCK:
        requests = dry_requests(); w = requests.get(str(aid), {})
        if automatic_stop and w.get("status") in ("blocked", "cancelled"): return "ok"
        w.update(status="held" if manual_stop else "stopped" if automatic_stop else "cancelled", until=now()+MANUAL_HOLD_H*3600,
                 observed_active=False, owned=False, last=now())
        if manual_stop or automatic_stop: w["last_stop_at"] = now()
        requests[str(aid)] = w; jsave(DRY_REQUESTS, requests)
    if not manual_stop and not automatic_stop: act_step("Drying request cancelled — automatic retries stopped", done=True, outcome="cancelled")
    return "ok"


def drying_reply(pr):
    # Never turn an unsolicited/late reply into a fresh drying request.
    if pr.get("command") != "ams_filament_drying" or str(pr.get("mode")) != "1" or pr.get("result") not in ("success", "fail"): return
    aid = str(pr.get("ams_id"))
    with DRY_LOCK:
        requests = dry_requests(); w = requests.get(aid)
        if not w or w.get("status") not in ("pending", "waiting"): return
        sequence = str(pr.get("sequence_id", ""))
        if w.get("sequence") and sequence != w["sequence"]: return
        if w.get("reply_received"): return
        w["reply_received"] = True
        if pr.get("result") == "success":
            w.update(accepted=True, owned=True, last=now(), accepted_at=now(), message="Printer accepted drying — waiting for heating")
        else:
            state = jload(STATE, {}); sf = state.get("ams", {}).get(aid, {}).get("dry_sf") or []
            try: err = int(pr.get("err_code") or 0)
            except (ValueError, TypeError): err = 0
            w.update(err=err, sf=sf, accepted=False, err_text=dry_err_text(err))
            if 3 in sf and queue_drying_unload(aid,w,state): pass
            elif 3 in sf or 10 in sf:
                dry_block(aid, w, "The printer reports filament in the AMS outlet. Drying is paused because the printer has not cleared its outlet interlock.")
            elif err in DRY_WAIT_CODES or (sf and set(sf).issubset({0, 1, 2, 4, 6})):
                w.update(status="waiting", wait_context=dry_wait_context(aid, state),
                         message="Waiting for the printer or drying power to become available")
            elif w.get("attempts", 0) >= DRY_MAX_ATTEMPTS:
                dry_block(aid, w, "The printer refused to start drying after three attempts. Check its screen for the cause, then retry. Automatic retries are stopped.")
            else:
                w.update(status="pending", message=f"Printer refused drying — attempt {w.get('attempts', 1)} of {DRY_MAX_ATTEMPTS}")
        jsave(DRY_REQUESTS, requests)


def drying_faults(state):
    faults = []
    for aid, w in dry_requests().items():
        if w.get("status") != "blocked" or (ams_is_drying(state.get("ams", {}).get(aid, {})) and not w.get("safety_stop_sent")): continue
        label = f"AMS {int(aid)+1}" if int(aid) < 128 else f"AMS HT {int(aid)-127}"
        faults.append(dict(code=f"DRYING_{aid}_{w.get('failed_at', 0)}", text=f"{label}: Drying blocked. {w['message']}",
                           blocking=True, process="drying", ams_id=aid))
    return faults


def has_psu(st, ams_id): return int(ams_id) >= 128 or bool(st.get("psu", {}).get(str(ams_id), False))


def drying_priority(aid, state, spools):
    """Least-dry known spool wins shared power; material type does not rank units."""
    percentages = []
    for tid in state.get("ams", {}).get(str(aid), {}).get("trays", {}):
        sp = spools.get(state.get("slot_spool", {}).get(slot_key(aid, tid)), {})
        if not sp or sp.get("provisional") or spool_rule(sp)["key"] == "?": continue
        percentages.append(assess(sp)["pct"])
    return -min(percentages, default=100)


def drying_order(state, stg, spools, aids=None):
    """Rank only the AMS units competing for the printer's one drying supply."""
    aids = list(state.get("ams", {}) if aids is None else aids)
    shared = iter(sorted((aid for aid in aids if not has_psu(stg, aid)),
                         key=lambda aid: drying_priority(aid, state, spools), reverse=True))
    return [aid if has_psu(stg, aid) else next(shared) for aid in aids]


def higher_priority_drying_unit(aid, state, stg, spools, requests):
    if has_psu(stg, aid): return None
    priority = drying_priority(aid, state, spools)
    for other in drying_order(state, stg, spools):
        if other == aid or has_psu(stg, other) or drying_priority(other, state, spools) <= priority: continue
        a = state["ams"][other]; w = requests.get(other, {})
        plan = plan_unit(other, a, spools, state.get("slot_spool", {}))
        if not plan or plan.get("unidentified") or drying_readiness(other, state, stg)[0] != "ready": continue
        if w.get("status") in ("pending", "waiting"):
            if w.get("accepted") or w.get("attempts", 0) >= DRY_MAX_ATTEMPTS: continue
            if w.get("status") == "waiting" and w.get("reply_received") and w.get("err") in DRY_WAIT_CODES and dry_wait_context(other, state) == w.get("wait_context"): continue
            delay = 20 if w.get("reply_received") or w.get("status") == "waiting" else 90
            if now()-w.get("last", 0) < delay: continue
        else:
            if not stg.get("auto", {}).get(other) or dry_is_held(w) or w.get("observed_active"): continue
            if now() < stg.get("manual_hold_until", {}).get(other, 0) or now()-w.get("last", 0) < 900: continue
            if plan["worst_pct"] >= AUTO_START_PCT: continue
        return other
    return None


def plan_unit(ams_id, a, spools, slot_spool):
    """Decide what a unit needs. Returns dict(spools, temp, hours, worst_pct, cannot_fully=[...])."""
    present = []; unidentified = []
    for tray_id in a["trays"]:
        sid = slot_spool.get(slot_key(ams_id, tray_id))
        tray = a["trays"][tray_id]
        if sid and sid in spools and not spools[sid].get("provisional") and spool_rule(spools[sid])["key"] != "?": present.append(spools[sid])
        elif tray.get("present") or not tray.get("empty", True) or sid: unidentified.append(tray_id)
    if not present: return None
    temp = UNIT_MAX(ams_id); cannot = []
    for sp in present:
        r = spool_rule(sp)
        # ceiling = what the material tolerates (table); the RFID value is the drying *target*, not a limit
        cap = 55 if r["key"] == "?" else SAFE_MAX.get(r["key"], r["temp"])
        temp = min(temp, cap)
    for sp in present:
        r = spool_rule(sp)
        if temp < r["temp"] - TEMP_TOLERANCE: cannot.append(f"{sp.get('name') or sp['type']} needs {r['temp']}°C")
    worst = min(assess(sp)["pct"] for sp in present)
    hours = max(assess(sp)["remaining_h"] for sp in present)
    hours = int(min(AUTO_MAX_HOURS, max(AUTO_MIN_HOURS, round(hours + 0.5))))
    return dict(n=len(present), temp=max(45, temp), hours=hours, worst_pct=worst, cannot_fully=cannot, unidentified=unidentified)


def dry_slot_taken_by(ams_id, state, stg):
    # Independent adapters/AMS HT do not consume the printer's single drying supply.
    if int(ams_id) >= 128 or has_psu(stg, ams_id): return None
    requests = dry_requests()
    for other, a in state.get("ams", {}).items():
        if other == str(ams_id) or int(other) >= 128 or has_psu(stg, other): continue
        phase = ams_info_bits(a.get("info"), 4, 4)
        w = requests.get(other, {})
        if ams_is_drying(a) or phase in (1, 3, 4): return other
        if w.get("status") == "pending" and w.get("attempts", 0)>0: return other
    return None

def drying_wait_details(aid, state, stg, request):
    """Explain a queued start using live owner telemetry or the observed stop cooldown."""
    if request.get("status") not in ("preparing", "unloading", "pending", "waiting"):
        return None
    if STATUS.get("mqtt") != "connected" or now() - state.get("t", 0) > 120:
        return None
    owner = dry_slot_taken_by(aid, state, stg)
    if owner is not None:
        unit = state.get("ams", {}).get(str(owner), {})
        left = rfid_dry_time(owner, unit.get("dry_time")) if ams_is_drying(unit) else None
        return dict(kind="shared_power", owner=ams_label(owner),
                    remaining_min=left if isinstance(left, (int, float)) and left > 0 else None,
                    until=now()+left*60 if isinstance(left, (int, float)) and left > 0 else None)
    unit = state.get("ams", {}).get(str(aid), {})
    stopped = request.get("last_stop_at")
    if 9 in (unit.get("dry_sf") or []) and isinstance(stopped, (int, float)) and now() - stopped < 180:
        return dict(kind="restart_cooldown", until=stopped + 60)
    return None

def clear_zombie_dry(state):
    # A target without heating can be checking, cooling, a firmware hold, or stale telemetry.
    # Never issue speculative stops, global stop commands, or recovery motion.
    return False


def humidity_auto_due(aid, unit, enabled):
    """Require sustained humidity and limit humidity-only drying to once per day."""
    records = jload(AUTO_HUMIDITY_STATE, {}) or {}
    record = records.get(str(aid), {})
    raw = unit.get("humidity_raw")
    valid = isinstance(raw, (int, float)) and not isinstance(raw, bool) and 0 <= raw <= 100
    if not enabled or ams_is_drying(unit) or not valid:
        if record.get("high_since"):
            record.pop("high_since", None); records[str(aid)] = record; jsave(AUTO_HUMIDITY_STATE, records)
        return False
    if raw < AUTO_HUMIDITY_RH - 2:
        if record.get("high_since"):
            record.pop("high_since", None); records[str(aid)] = record; jsave(AUTO_HUMIDITY_STATE, records)
        return False
    if raw < AUTO_HUMIDITY_RH: return False
    if not record.get("high_since"):
        record["high_since"] = now(); records[str(aid)] = record; jsave(AUTO_HUMIDITY_STATE, records)
        return False
    return (now() - record["high_since"] >= AUTO_HUMIDITY_DWELL
            and now() - record.get("last_start", 0) >= AUTO_HUMIDITY_COOLDOWN)


def humidity_auto_started(aid):
    records = jload(AUTO_HUMIDITY_STATE, {}) or {}
    record = records.get(str(aid), {})
    record.update(last_start=now(), high_since=None)
    records[str(aid)] = record; jsave(AUTO_HUMIDITY_STATE, records)

def autodry_tick():
    state = jload(STATE, {}); stg = settings(); spools = jload(SPOOLS, {})
    if STATUS.get("mqtt") != "connected" or now()-state.get("t", 0)>120: return
    if state.get("t", 0)<STATUS.get("since", 0) or now()-state.get("print_state_t", 0)>120: return
    with DRY_LOCK:
        requests = dry_requests()
        for aid in drying_order(state, stg, spools):
            if aid in (jload(RFID_HEAT_CYCLES, {}) or {}): continue
            a = state["ams"][aid]
            w = requests.get(aid, {}); active = ams_is_drying(a)
            plan = plan_unit(aid, a, spools, state.get("slot_spool", {}))
            auto_enabled = bool(stg.get("auto", {}).get(aid))
            humidity_due = humidity_auto_due(aid, a, auto_enabled and bool(plan) and not plan.get("unidentified"))
            if active:
                # Respect printer/Studio cycles and any lower firmware-selected temperature.
                target = (a.get("dry_setting") or {}).get("dry_temperature") or 0
                if plan and target>plan["temp"] and not w.get("safety_stop_sent"):
                    set_drying(aid, False)
                    w = dry_requests().get(aid, {})
                    dry_block(aid, w, "Drying was stopped because its temperature exceeds the limit for a spool in this AMS. Check the materials before retrying.")
                    w['safety_stop_sent'] = True; requests[aid] = w; jsave(DRY_REQUESTS, requests)
                    continue
                if w.get('safety_stop_sent'): continue
                if w.get("status") in ("held", "cancelled", "stopped"): continue
                w.update(last_dry_time=a.get("dry_time"), last_observed=now())
                if w.get("status") in ("pending", "waiting", "blocked"):
                    w.update(status="succeeded", observed_active=True)
                    act_step("Drying cycle is running", done=True)
                elif not w.get("observed_active"):
                    w.update(status="external", owned=False, observed_active=True)
                if w.get("owned") and (target>w.get("temp", target) or (a.get("dry_setting") or {}).get("dry_duration", -1)>w.get("hours", 24)):
                    w["owned"] = False
                requests[aid] = w; jsave(DRY_REQUESTS, requests)
                if plan and stg.get('auto',{}).get(aid) and w.get("owned") and w.get("source")=="auto" and plan["worst_pct"]>=100:
                    set_drying(aid, False); requests = dry_requests()
                continue
            if w.get("observed_active"):
                if ams_info_bits(a.get("info"),4,4) in (1,3,4): continue
                # A printer-side interruption is not permission to immediately restart it.
                remaining=w.get("last_dry_time")
                completed=(remaining is not None and remaining>0 and now()-w.get("last_observed",now()) >= max(0,remaining*60-90))
                w.update(status="completed" if completed else "cancelled", owned=False, observed_active=False, last=now(),
                         message="Drying cycle finished" if completed else "The drying cycle was interrupted. Auto-dry is paused until Retry drying.")
                requests[aid] = w; jsave(DRY_REQUESTS, requests)
            if not auto_enabled or not plan or plan.get("unidentified"): continue
            if dry_is_held(w) or now()<stg.get("manual_hold_until",{}).get(aid,0): continue
            if plan["worst_pct"]>=AUTO_START_PCT and not humidity_due: continue
            if now()-w.get("last",0)<900: continue
            result = set_drying(aid, True, plan["temp"], plan["hours"])
            if humidity_due and result.startswith("ok"):
                humidity_auto_started(aid)
                append(EVENTS, dict(t=now(), time=iso(), kind="AUTO_DRY", slot=f"{aid}:*",
                                    msg=f"{ams_label(aid)}: sustained {a['humidity_raw']}% RH triggered drying ({result})"))
            requests = dry_requests()

DEFAULT_PREHEAT = {"PLA": {"bed": 60, "chamber": 0, "nozzle": 220}, "PETG": {"bed": 80, "chamber": 0}, "PETG-CF": {"bed": 70, "chamber": 0}, "PA-CF": {"bed": 100, "chamber": 65}}


def send_gcode(lines):
    c = MQTT["client"]
    if not c: return "printer not connected"
    c.publish(MQTT["topic_req"], json.dumps({"print": {"sequence_id": "0", "command": "gcode_line", "param": lines}}))
    return "ok"


def _pending_after_unload():
    return settings().get("resume_dry_after_unload") or {}


def do_filament(action, target=255):
    with DRY_LOCK:
        if any(w.get("status")=="pending" for w in dry_requests().values()): return "wait for the pending drying request to finish before moving filament"
        """Unload (target 255) or load a slot (target = ams*4+tray). Explicit user command; firmware handles heating and filament interlocks."""
        if dry_unload_busy(): return "the printer is retracting filament for drying; wait for it to finish"
        c = MQTT["client"]
        if not c: return "printer not connected"
        st = jload(STATE, {})
        blocked = toolhead_safe(st)
        if blocked:
            append(EVENTS, dict(t=now(), time=iso(), kind="FILAMENT", msg=f"filament {action} NOT sent — {blocked}"))
            return blocked
        # a sensible nozzle temp: the loaded/target spool's max, else 250
        temp = 250
        try:
            t = int(target)
            if t != 255:
                aid, tid = (t // 4, t % 4) if t < 128 * 4 else (t // 4, t % 4)
                for k, a in st.get("ams", {}).items():
                    if int(k) * 4 <= t < int(k) * 4 + 4:
                        tr = a["trays"].get(str(t - int(k) * 4), {}); temp = int(tr.get("nozzle_max") or 250)
            else:
                tn = st.get("tray_now")
                if tn not in (None, "255", 255):
                    tn = int(tn)
                    for k, a in st.get("ams", {}).items():
                        if int(k) * 4 <= tn < int(k) * 4 + 4:
                            tr = a["trays"].get(str(tn - int(k) * 4), {}); temp = int(tr.get("nozzle_max") or 250)
        except Exception: pass
        env = st.get("env", {}); cur = int(env.get("nozzle") or 0)
        t = int(target)
        # Bambu Studio (H2 series protocol): unload → target 255, slot_id 255, ams_id of the loaded unit; load → ams_id, slot_id, target=tray index
        if t == 255:
            tn = st.get("tray_now"); ams_id = 0
            try:
                tn = int(tn)
                for k in st.get("ams", {}):
                    if int(k) * 4 <= tn < int(k) * 4 + 4: ams_id = int(k)
            except Exception: pass
            payload = {"print": {"sequence_id": "0", "command": "ams_change_filament", "curr_temp": cur, "tar_temp": temp,
                                 "ams_id": ams_id, "target": 255, "slot_id": 255, "extruder_id": 0}}
        else:
            ams_id, slot_id = 0, t % 4
            for k in st.get("ams", {}):
                if int(k) * 4 <= t < int(k) * 4 + 4: ams_id, slot_id = int(k), t - int(k) * 4
            payload = {"print": {"sequence_id": "0", "command": "ams_change_filament", "curr_temp": cur, "tar_temp": temp,
                                 "ams_id": ams_id, "target": t, "slot_id": slot_id, "extruder_id": 0}}
        mark_filament_busy(180)
        c.publish(MQTT["topic_req"], json.dumps(payload))
        append(EVENTS, dict(t=now(), time=iso(), kind="FILAMENT", msg=f"{'UNLOAD' if int(target) == 255 else 'LOAD slot ' + str(int(target) % 4 + 1)} sent (nozzle → {temp}°C)"))
        return "ok"


def _rfid_now(ams_id, slot_id):
    c = MQTT["client"]
    if not c: return "printer not connected"
    mark_filament_busy(8)
    c.publish(MQTT["topic_req"], json.dumps({"print": {"sequence_id": "0", "command": "ams_get_rfid", "ams_id": int(ams_id), "slot_id": int(slot_id)}}))
    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"RFID read requested for {ams_label(ams_id)} slot {int(slot_id)+1}"))
    return "ok"


def do_ams_read_options(on=True):
    """Enable the AMS's own tag scanning (on insert + at power-on). If this is off, every read
    request returns success and does nothing."""
    c = MQTT["client"]
    if not c: return "printer not connected"
    c.publish(MQTT["topic_req"], json.dumps({"print": {"sequence_id": "0", "command": "ams_user_setting",
        "ams_id": -1, "startup_read_option": bool(on), "tray_read_option": bool(on), "calibrate_remain_flag": True}}))
    append(EVENTS, dict(t=now(), time=iso(), kind="RFID", msg=f"AMS tag scanning (on insert + at power-on) set to {'ON' if on else 'OFF'}"))
    return "ok"


READ_MAX_TEMP = 45      # the AMS will not spin a spool for a tag read while it is hot from drying
RFID_HEAT_CYCLES = P("rfid_heat_cycles.json")


def rfid_dry_time(aid, reported, cycles=None):
    """Show the saved countdown while a resumed cycle uses rounded firmware hours."""
    w = (cycles if cycles is not None else jload(RFID_HEAT_CYCLES, {})).get(str(aid), {})
    if w.get("stage") not in ("active", "finishing"): return reported
    left = max(0, math.ceil((w.get("stop_at", now()) - now()) / 60))
    return min(left, reported) if isinstance(reported, (int, float)) and reported > 0 else left


def rfid_resume_tracking(w, a):
    """Start counting only once the heater actually resumes, not during cooling or scanning."""
    cycle_started = (a.get("dry_cycle") or {}).get("started_at")
    started = cycle_started if isinstance(cycle_started, (int, float)) and cycle_started >= w.get("pause_at", 0) else now()
    remaining = float(w.get("remaining_min") or w["hours"] * 60)
    w.update(stage="active", stage_at=now(), resumed_at=started,
             stop_at=started + remaining * 60, printer_cycle_started_at=cycle_started)


def rfid_finish_scans(cycles, aid, w):
    """Restore every dryer paused for this tag read, including the loaded spool's AMS."""
    source=w.pop("source_restore",None)
    if source:
        source_aid=source.pop("aid")
        source.update(stage="resume",stage_at=now(),slots=[],restore_dry=True)
        cycles[source_aid]=source
    if w.get("restore_dry"):w.update(stage="resume",stage_at=now())
    else:cycles.pop(aid,None)


def rfid_heat_cycle_tick():
    """Finish an explicitly requested scan, then restore the interrupted AMS cycle."""
    with DRY_LOCK:
        cycles = jload(RFID_HEAT_CYCLES, {}) or {}
        if not cycles: return
        state = jload(STATE, {})
        if STATUS.get("mqtt") != "connected" or now()-state.get("t", 0)>120 or state.get("t", 0)<STATUS.get("since", 0): return
        changed = False
        for aid, w in list(cycles.items()):
            a = state.get("ams", {}).get(aid, {})
            if not a: continue
            stage = w.get("stage")
            elapsed = now()-w.get("stage_at", now())
            if stage == "stopping":
                if not ams_is_drying(a) and ams_info_bits(a.get("info"),4,4)==0 and (a.get("temp") or 999)<=READ_MAX_TEMP:
                    w.update(stage="scan", stage_at=now()); changed=True
                elif elapsed>120:
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} did not cool to {READ_MAX_TEMP}°C within two minutes; restoring drying"))
                    w.update(stage="resume",stage_at=now());changed=True
            elif stage == "unload":
                # Save the command sequence before publishing so a restart cannot
                # send a second filament-cutter operation while the first is pending.
                if str(state.get("tray_now"))=="255" and state.get("ams_status")==0 and not ams_is_drying(a):
                    w.update(stage="scan",stage_at=now());changed=True;continue
                if w.get("unload_reply")=="fail" and w.get("unload_error")==-9 and not w.get("drying_refusal_retry"):
                    # Firmware explicitly refused because the loaded spool's AMS
                    # is drying. A confirmed refusal is safe to retry after stopping
                    # that source dryer; no cutter movement occurred.
                    w["drying_refusal_retry"]=True
                    for field in ("unload_reply","unload_error","unload_sequence","unload_sent_at"):w.pop(field,None)
                    changed=True;continue
                if w.get("unload_reply")=="fail" or (w.get("unload_sent_at") and now()-w["unload_sent_at"]>300):
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} could not confirm the nozzle unloaded; tag read cancelled"))
                    rfid_finish_scans(cycles,aid,w)
                    changed=True;continue
                if w.get("unload_sent_at"):continue
                if ams_is_drying(a) or (a.get("temp") or 999)>READ_MAX_TEMP or ams_info_bits(a.get("info"),4,4)!=0:continue
                if state.get("ams_status")!=0 or filament_busy(settings()) or now()<state.get("scan_until",0):continue
                blocked=toolhead_safe(state)
                if blocked:
                    if elapsed>300:
                        append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} cannot unload for RFID: {blocked}"))
                        rfid_finish_scans(cycles,aid,w)
                        changed=True
                    continue
                loaded=int(state["tray_now"])
                loaded_aid=next((int(k) for k in state.get("ams",{}) if int(k)*4<=loaded<int(k)*4+4),None)
                if loaded_aid is None:continue
                if loaded_aid!=int(aid):
                    source=state["ams"][str(loaded_aid)]
                    if ams_is_drying(source):
                        if not w.get("source_restore"):
                            source_aid=str(loaded_aid)
                            remaining=rfid_dry_time(source_aid,source.get("dry_time"),cycles)
                            temp=int((source.get("dry_setting") or {}).get("dry_temperature") or 0)
                            if not isinstance(remaining,(int,float)) or remaining<=0 or not 45<=temp<=UNIT_MAX(source_aid):continue
                            original=cycles.pop(source_aid,None)
                            w["source_restore"]=dict(aid=source_aid,temp=temp,
                                hours=max(1,min(24,int(math.ceil(remaining/60)))),remaining_min=float(remaining),rotate=False,pause_at=now())
                            cycles[aid]=w;jsave(RFID_HEAT_CYCLES,cycles)
                            result=set_drying(source_aid,False)
                            if result!="ok":
                                if original:cycles[source_aid]=original
                                w.pop("source_restore",None);changed=True
                            else:
                                append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(source_aid)}: pausing loaded spool's dryer for automatic unload"))
                                changed=True
                        continue
                    if (source.get("temp") or 999)>READ_MAX_TEMP or ams_info_bits(source.get("info"),4,4)!=0:continue
                sequence=str(time.time_ns()%2147483647)
                w.update(unload_sequence=sequence,unload_sent_at=now());cycles[aid]=w;jsave(RFID_HEAT_CYCLES,cycles)
                payload={"print":{"sequence_id":sequence,"command":"ams_change_filament","ams_id":loaded_aid,
                                  "target":255,"slot_id":255,"curr_temp":210,"tar_temp":210}}
                try:
                    result=MQTT["client"].publish(MQTT["topic_req"],json.dumps(payload))
                    if getattr(result,"rc",0)!=0:raise RuntimeError("publish failed")
                    mark_filament_busy(30)
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: retracting loaded filament before tag read"))
                except Exception:
                    # Delivery is ambiguous, so do not automatically retry.
                    w["unload_reply"]="fail";changed=True
            elif stage == "scan":
                if not w.get("slots"):
                    rfid_finish_scans(cycles,aid,w)
                    changed=True;continue
                slot=str(w["slots"][0]);tray=a.get("trays",{}).get(slot,{})
                if not w.get("sent_at"):
                    if filament_busy(settings()):continue
                    if ams_is_drying(a) or ams_info_bits(a.get("info"),4,4)!=0:
                        rfid_finish_scans(cycles,aid,w)
                        changed=True;continue
                    if _rfid_now(aid,slot)!="ok":continue
                    w["sent_at"]=now();w["seen_reading"]=False;w["seen_tray_busy"]=False
                    w["initial_rfid"]=tray.get("rfid")
                    w["scan_until_before"]=state.get("scan_until",0);changed=True
                else:
                    if tray.get("reading") or state.get("scan_until",0)>w.get("scan_until_before",0)+1:
                        w["seen_reading"]=True;changed=True
                    if tray.get("reading") or not tray.get("read_done",True):
                        w["seen_tray_busy"]=True;changed=True
                    elapsed_scan=now()-w["sent_at"]
                    if not w.get("seen_reading") and elapsed_scan>12:
                        append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} slot {int(slot)+1}: printer acknowledged the request but did not start a scan"))
                        rfid_finish_scans(cycles,aid,w);changed=True;continue
                    done=(w.get("seen_reading") and not tray.get("reading") and
                          ((w.get("seen_tray_busy") and tray.get("read_done")) or
                           (tray.get("rfid") and tray.get("rfid")!=w.get("initial_rfid")))) or elapsed_scan>45
                    if done:
                        outcome="tag read" if tray.get("rfid") and w.get("seen_reading") else "scan ended without a readable tag" if w.get("seen_reading") else "scan did not finish"
                        append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} slot {int(slot)+1}: {outcome}"))
                        w["slots"].pop(0)
                        for field in ("sent_at","seen_reading","seen_tray_busy","initial_rfid","scan_until_before"):w.pop(field,None)
                        changed=True
            elif stage == "clear_outlet":
                # Some H2 printers report a separate AMS-outlet interlock even
                # after the nozzle has been unloaded. The native command for the
                # target AMS clears it; observe the printer state, not just its ACK.
                if 3 not in (a.get("dry_sf") or []) and str(state.get("tray_now"))=="255" and state.get("ams_status")==0:
                    w.update(stage="resume",stage_at=now(),outlet_cleared=True);changed=True;continue
                if w.get("outlet_reply")=="fail" or (w.get("outlet_sent_at") and now()-w["outlet_sent_at"]>300):
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} outlet did not clear; saved drying restart is blocked by the printer"))
                    cycles.pop(aid);changed=True;continue
                if w.get("outlet_sent_at"):continue
                if str(state.get("tray_now"))!="255" or state.get("ams_status")!=0:continue
                if ams_is_drying(a) or (a.get("temp") or 999)>READ_MAX_TEMP or filament_busy(settings()):continue
                if toolhead_safe(state):continue
                sequence=str(time.time_ns()%2147483647)
                w.update(outlet_sequence=sequence,outlet_sent_at=now());cycles[aid]=w;jsave(RFID_HEAT_CYCLES,cycles)
                payload={"print":{"sequence_id":sequence,"command":"ams_change_filament","ams_id":int(aid),
                                  "target":255,"slot_id":255,"curr_temp":210,"tar_temp":210}}
                try:
                    result=MQTT["client"].publish(MQTT["topic_req"],json.dumps(payload))
                    if getattr(result,"rc",0)!=0:raise RuntimeError("publish failed")
                    mark_filament_busy(30)
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: clearing the AMS outlet automatically before restoring drying"))
                except Exception:
                    # A lost ACK is ambiguous; do not repeat a filament operation.
                    w["outlet_reply"]="fail";changed=True
            elif stage == "resume":
                if 3 in (a.get("dry_sf") or []) and not w.get("outlet_cleared"):
                    w.update(stage="clear_outlet",stage_at=now());changed=True;continue
                if ams_is_drying(a):
                    rfid_resume_tracking(w, a)
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} drying restored with {math.ceil(float(w.get('remaining_min') or w['hours']*60))} min remaining"))
                    changed=True;continue
                if elapsed>900:
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} drying restart is pending; check the printer if it does not resume"))
                    cycles.pop(aid);changed=True;continue
                if w.get("resume_sent") and now()-w["resume_sent"]<30:continue
                # The printer's own target and remaining timer were captured before stop.
                result=set_drying(aid,True,w["temp"],w["hours"],w.get("rotate",False),resume_saved=True)
                w["resume_sent"]=now();changed=True
                if result.startswith("ok") or result.startswith("already drying"):
                    w.update(stage="confirm",stage_at=now())
            elif stage == "confirm":
                if ams_is_drying(a):
                    rfid_resume_tracking(w, a)
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} drying restored with {math.ceil(float(w.get('remaining_min') or w['hours']*60))} min remaining"))
                    changed=True
                elif elapsed>180:
                    w.update(stage="resume",stage_at=now());changed=True
            elif stage == "active":
                current_cycle = (a.get("dry_cycle") or {}).get("started_at")
                if not ams_is_drying(a) or (w.get("printer_cycle_started_at") and current_cycle and abs(current_cycle-w["printer_cycle_started_at"])>60):
                    # A physical-printer stop or new cycle overrides this saved timer.
                    cycles.pop(aid);changed=True
                elif now() >= w["stop_at"]:
                    result=set_drying(aid,False)
                    if result=="ok":
                        w.update(stage="finishing",stage_at=now());changed=True
                        append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: original drying time completed; stopping heater"))
            elif stage == "finishing":
                if not ams_is_drying(a):
                    cycles.pop(aid);changed=True
                elif elapsed>30:
                    if set_drying(aid,False)=="ok": w["stage_at"]=now();changed=True
        if changed: jsave(RFID_HEAT_CYCLES, cycles)


def do_read_rfid(ams_id, slot_id, pause_drying=False):
    with DRY_LOCK:
        aid=str(int(ams_id));slot=str(int(slot_id));state=jload(STATE,{});a=state.get("ams",{}).get(aid,{})
        if STATUS.get("mqtt")!="connected" or now()-state.get("t",0)>120: return "waiting for fresh printer status"
        if state.get("print_state") not in ("IDLE","FINISH","FAILED") or (state.get("print_state")=="FAILED" and unresolved_print_fault(state)): return "the printer is busy — tag reads wait until idle"
        if slot not in a.get("trays",{}):return "that AMS slot is unavailable"
        cycles=jload(RFID_HEAT_CYCLES,{}) or {}
        if aid in cycles:
            w=cycles[aid]
            if slot not in w.get("slots",[]) and w.get("stage") in ("stopping","unload","scan"):
                w.setdefault("slots",[]).append(slot);cycles[aid]=w;jsave(RFID_HEAT_CYCLES,cycles)
            return "ok"
        if ams_is_drying(a) and pause_drying:
            if not MQTT.get("client"):return "printer not connected"
            setting=a.get("dry_setting") or {}
            temp=int(setting.get("dry_temperature") or 0)
            if not 45<=temp<=UNIT_MAX(aid):return "the current drying target is unavailable; cannot restore it safely"
            remaining=a.get("dry_time")
            if not isinstance(remaining,(int,float)) or not math.isfinite(remaining) or remaining<=0:return "the remaining drying time is unavailable; cannot restore it safely"
            hours=max(1,min(24,int(math.ceil(float(remaining)/60))))
            cycles[aid]=dict(stage="stopping",stage_at=now(),pause_at=now(),slots=[slot],temp=temp,hours=hours,remaining_min=float(remaining),rotate=False,restore_dry=True)
            jsave(RFID_HEAT_CYCLES,cycles)
            result=set_drying(aid,False)
            if result!="ok":cycles.pop(aid,None);jsave(RFID_HEAT_CYCLES,cycles);return result
            append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: pausing drying for RFID read; will resume {temp}°C with {math.ceil(remaining)} min left"))
            return "ok"
        if ams_is_drying(a) or (a.get("temp") or 0)>READ_MAX_TEMP:return "wait until drying finishes and the AMS cools before reading the tag"
        if any(reason in (2, 3, 4, 7, 10) for reason in (a.get("dry_sf") or [])):return "the AMS is busy — wait for it to finish"
        if now()-state.get("print_state_t",0)>120 or state.get("t",0)<STATUS.get("since",0):return "waiting for fresh printer status"
        if a.get("temp") is None or ams_info_bits(a.get("info"),4,4) != 0:return "waiting for the AMS to be idle and cool"
        if filament_busy(settings()):return "waiting for the filament operation to finish"
        if any(w.get("status")=="pending" for w in dry_requests().values()):return "waiting for the drying request to finish"
        cycles[aid]=dict(stage="scan",stage_at=now(),slots=[slot],restore_dry=False)
        jsave(RFID_HEAT_CYCLES,cycles)
        return "ok"

def ffmpeg_bin():
    import shutil
    for c in ("ffmpeg", "/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg"):
        p = shutil.which(c) or (c if os.path.exists(c) else None)
        if p: return p
    return None


def cam_url():
    cfg = jload(CONFIG, {})
    return f"rtsps://bblp:{cfg.get('access_code','')}@{cfg.get('ip','')}:322/streaming/live/1"


CAM = {"proc": None, "clients": set(), "lock": _th.Lock(), "last": 0, "frames": 0, "jpeg": None, "snapshot_touch": 0, "snapshot_attempt": 0}


def _cam_reader(p):
    """One ffmpeg per printer. Its mpjpeg output is split into whole frames and fanned out to every
    viewer; a viewer that cannot keep up (background tab, slow link) loses OLD frames, never gets a
    backlog, so it is always showing the newest picture the moment you look at it."""
    boundary = b"--ffmpeg"
    buf = b""
    try:
        while True:
            chunk = p.stdout.read(32768)
            if not chunk: break
            buf += chunk
            while True:
                nxt = buf.find(boundary, 1)          # a complete part ends where the next one starts
                if nxt < 0: break
                frame, buf = buf[:nxt], buf[nxt:]
                frame = frame.replace(b"\r\n\r\n", b"\r\nX-Camera-Time: " + str(round(time.monotonic()*1000, 3)).encode() + b"\r\n\r\n", 1)
                CAM["frames"] += 1; CAM["last"] = now()
                with CAM["lock"]:
                    begin, end = frame.find(b"\xff\xd8"), frame.rfind(b"\xff\xd9")
                    if begin >= 0 and end > begin: CAM["jpeg"] = frame[begin:end+2]
                    qs = list(CAM["clients"])
                for q in qs:
                    try:
                        if q.full():
                            try: q.get_nowait()      # drop the stale frame
                            except Exception: pass
                        q.put_nowait(frame)
                    except Exception: pass
            if len(buf) > 4_000_000: buf = b""       # safety valve on a malformed stream
    finally:
        try: p.kill()
        except Exception: pass
        with CAM["lock"]:
            if CAM["proc"] is p: CAM["proc"] = None
            for q in list(CAM["clients"]):
                try: q.put_nowait(None)
                except Exception: pass


def cam_start():
    import subprocess, queue
    with CAM["lock"]:
        p = CAM["proc"]
        if p and p.poll() is None: return p, None
        exe = ffmpeg_bin()
        if not exe: return None, "ffmpeg is not installed on this Mac. Install it with:  brew install ffmpeg"
        # Eight simultaneous wall cameras must fit on a phone CPU and network link.
        # Browser snapshots sample these cached frames, so higher encode cadence wastes work.
        args = [exe, "-loglevel", "error", "-rtsp_transport", "tcp", "-tls_verify", "0", "-fflags", "nobuffer",
                "-flags", "low_delay", "-i", cam_url(), "-an", "-vf", "fps=8,scale=720:-2",
                "-q:v", "7", "-threads", "2", "-f", "mpjpeg", "-"]
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        CAM["proc"] = p
        _th.Thread(target=_cam_reader, args=(p,), daemon=True).start()
        return p, None



def cam_snapshot(handler):
    """Finite, cached camera reads for the wall. Never wait for an RTSP frame."""
    with CAM["lock"]:
        CAM["snapshot_touch"] = now()
        jpeg = CAM["jpeg"] if now()-CAM["last"] < 5 else None
        running = bool(CAM["proc"] and CAM["proc"].poll() is None)
        start = not running and now()-CAM["snapshot_attempt"] > 30
        if start: CAM["snapshot_attempt"] = now()
    if start:
        def start_preview():
            proc, _ = cam_start()
            if not proc: return
            while True:
                time.sleep(10)
                with CAM["lock"]:
                    if CAM["proc"] is not proc: return
                    idle = not CAM["clients"] and now()-CAM["snapshot_touch"] > 35
                    stalled = not CAM["clients"] and now()-max(CAM["last"],CAM["snapshot_attempt"]) > 30
                    if not idle and not stalled: continue
                    CAM["proc"] = None
                try: proc.kill()
                except Exception: pass
                return
        _th.Thread(target=start_preview, daemon=True).start()
    handler._send(200 if jpeg else 204, jpeg or b"", "image/jpeg")


def cam_stream(handler):
    """Serve MJPEG to one viewer from the shared transcode."""
    import queue
    p, err = cam_start()
    if err:
        handler.send_response(503); handler.send_header("Content-Type", "text/plain"); handler.end_headers()
        handler.wfile.write(err.encode()); return
    q = queue.Queue(maxsize=2)      # at most ~1 frame of lag; older frames are dropped
    with CAM["lock"]: CAM["clients"].add(q)
    try:
        first = q.get(timeout=20)                     # fail fast if the printer gives nothing
        if first is None: raise RuntimeError("no data")
    except Exception:
        with CAM["lock"]: CAM["clients"].discard(q)
        e = b""
        try:
            p.kill(); e = (p.stderr.read(600) or b"")
        except Exception: pass
        txt = e.decode("utf-8", "replace").strip()
        hint = ""
        low = txt.lower()
        if "401" in low or "unauthor" in low: hint = " — access code rejected"
        elif "503" in low or "refused" in low or "timed out" in low or not txt:
            hint = " — the printer serves one viewer at a time; close the live view in Bambu Studio / Orca and try again"
        with CAM["lock"]:
            if CAM["proc"] is p: CAM["proc"] = None
        print(f"[{iso()}] camera failed: {txt[:300]}", flush=True)
        append(EVENTS, dict(t=now(), time=iso(), kind="CAMERA", msg=f"stream failed{hint}: {txt[:200] or 'no data'}"))
        handler.send_response(503); handler.send_header("Content-Type", "text/plain"); handler.end_headers()
        handler.wfile.write(("camera unavailable" + hint + ("\n\n" + txt if txt else "")).encode()); return
    handler.send_response(200)
    handler.send_header("Content-Type", "multipart/x-mixed-replace; boundary=ffmpeg")
    handler.send_header("Cache-Control", "no-store"); handler.end_headers()
    try:
        handler.wfile.write(first)
        while True:
            chunk = q.get()
            if chunk is None: break
            handler.wfile.write(chunk)
    except Exception: pass
    finally:
        with CAM["lock"]:
            CAM["clients"].discard(q)
            alone = not CAM["clients"]
        if alone:                                     # last viewer left: stop the transcode shortly after
            def _idle_stop(proc):
                while True:
                    time.sleep(30)
                    with CAM["lock"]:
                        if CAM["clients"] or CAM["proc"] is not proc: return
                        if now()-CAM["snapshot_touch"] < 35: continue
                        CAM["proc"] = None
                        break
                try: proc.kill()
                except Exception: pass
            _th.Thread(target=_idle_stop, args=(p,), daemon=True).start()


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


def do_ams_unload(temp=250):
    with DRY_LOCK:
        if any(w.get("status")=="pending" for w in dry_requests().values()): return "wait for the pending drying request to finish before moving filament"
        """Bambu's AMS unload sequence (M620 S255 … T255 … M621 S255), without the stock macro's
        hard-coded X1 purge coordinates. Used when filament is stuck in the AMS outlet."""
        st = jload(STATE, {})
        blocked = toolhead_safe(st)
        if blocked:
            append(EVENTS, dict(t=now(), time=iso(), kind="FILAMENT",
                                msg=f"AMS outlet unload NOT sent — {blocked}"))
            return blocked
        temp = max(200, min(280, int(temp or 250)))
        g = (f"M620 S255\nM104 S{temp}\nM109 S{temp}\nT255\nG4 P1000\nM104 S0\nM621 S255\n")
        mark_filament_busy(180)
        r = send_gcode(g)
        append(EVENTS, dict(t=now(), time=iso(), kind="FILAMENT", msg=f"AMS outlet unload sequence sent (nozzle {temp}°C) → {r}"))
        act_step(f"Heating the nozzle to {temp} °C to release the filament")
        return r


MOVE_WRAP = "M211 S\nM211 X1 Y1 Z1\nM1002 push_ref_mode\nG91\n{body}\nM1002 pop_ref_mode\nM211 R\nG90\n"
UNRESTRICTED_MOVE_WRAP = "M211 S\nM211 X0 Y0 Z0\nM1002 push_ref_mode\nG91\n{body}\nM400\nM1002 pop_ref_mode\nM211 R\nG90\n"


def do_control(body):
    """Manual printer controls, using Bambu Studio's own command forms."""
    st = jload(STATE, {}); busy = st.get("print_state") in ("RUNNING", "PREPARE", "PAUSE")
    act = body.get("action")
    motion = act in ("home", "move", "move_unrestricted", "extrude", "level", "motors_off")
    if motion and busy: return "a print job is active — pause and stop it first"
    c = MQTT["client"]
    if not c: return "printer not connected"

    if act == "home":
        what = {"all": "G28\n", "xy": "G28 X Y\n", "z": "G28 Z\n"}.get(body.get("what", "all"))
        if not what: return "unknown axis"
        r = send_gcode(what)
    elif act == "move":
        ax = str(body.get("axis", "")).upper()
        if ax not in ("X", "Y", "Z"): return "unknown axis"
        d = max(-100.0, min(100.0, float(body.get("dist", 0))))
        f = 900 if ax == "Z" else 6000
        r = send_gcode(MOVE_WRAP.format(body=f"G1 {ax}{d:.2f} F{f}"))
    elif act == "move_unrestricted":
        if not jload(CONFIG, {}).get("allow_unrestricted_jog", False): return "unrestricted jogging is disabled for this printer"
        ax = str(body.get("axis", "")).upper()
        if ax not in ("X", "Y", "Z"): return "unknown axis"
        try: d = float(body.get("dist", 0))
        except (TypeError, ValueError): return "invalid distance"
        if not math.isfinite(d) or d == 0: return "invalid distance"
        f = 900 if ax == "Z" else 6000
        r = send_gcode(UNRESTRICTED_MOVE_WRAP.format(body=f"G1 {ax}{d:.2f} F{f}"))
    elif act == "extrude":
        d = max(-100.0, min(100.0, float(body.get("dist", 0))))
        noz = (st.get("env", {}) or {}).get("nozzle") or 0
        if noz < 170: return f"nozzle is {int(noz)}°C — heat it above 170 first (use a preheat preset)"
        r = send_gcode(f"M83\nG1 E{d:.1f} F900\n")
    elif act == "level":
        r = send_gcode("G29\n")
    elif act == "motors_off":
        r = send_gcode("M18\n")
    elif act == "light":
        node = body.get("node", "chamber_light")
        if node not in ("chamber_light", "chamber_light2", "work_light"): return "unknown light"
        c.publish(MQTT["topic_req"], json.dumps({"system": {"sequence_id": "0", "command": "ledctrl",
            "led_node": node, "led_mode": "on" if body.get("on") else "off",
            "led_on_time": 500, "led_off_time": 500, "loop_times": 0, "interval_time": 0}}))
        r = "ok"
    elif act == "fan":
        p = int(body.get("p", 1)); pct = max(0, min(100, int(body.get("pct", 0))))
        if p not in (1, 2, 3): return "unknown fan"
        with FAN_CONTROL_LOCK:
            r = send_gcode(f"M106 P{p} S{round(pct * 2.55)}\n")
            if r == "ok": jsave(COOLDOWN, {"active": False})
    elif act in ("fans_max", "fans_all"):
        if STATUS.get("mqtt") != "connected" or now()-st.get("t", 0) > 120:
            return "waiting for a fresh printer connection — fans were not changed"
        pct = 100 if act == "fans_max" else body.get("pct")
        if isinstance(pct, bool) or pct not in (0, 100): return "choose all fans off or 100%"
        speed = 255 if pct == 100 else 0
        with FAN_CONTROL_LOCK:
            r = send_gcode("".join(f"M106 P{p} S{speed}\n" for p in (1, 2, 3)))
            # Persist ownership on the controller, independent of browser tabs.
            if r == "ok": jsave(COOLDOWN, {"active": False, "fan_off_at": now()+420 if pct == 100 else None})
    elif act == "speed":
        lvl = int(body.get("level", 2))
        if lvl not in (1, 2, 3, 4): return "unknown speed level"
        c.publish(MQTT["topic_req"], json.dumps({"print": {"sequence_id": "0", "command": "print_speed", "param": str(lvl)}}))
        r = "ok"
    else:
        return "unknown action"
    append(EVENTS, dict(t=now(), time=iso(), kind="CONTROL", msg=f"{act} {json.dumps({k: v for k, v in body.items() if k != 'action'})} → {r}"))
    return r


PROBE = {"busy": False}


def dry_probe(ams_id, temp=55):
    return "Payload probing is disabled. Use Start drying; firmware restrictions are respected."

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


CMDFILE = P("command.json")
CMDRESULT = P("command_result.json")


def command_file_tick():
    """A file-based control channel: drop {"action": ...} into command.json and the worker runs it.
    Whitelisted, read-only-ish actions so a stray file cannot do anything destructive."""
    try:
        if not os.path.exists(CMDFILE): return
        body = jload(CMDFILE, None) or {}
        try: os.remove(CMDFILE)
        except Exception: pass
        act = body.get("action")
        st = jload(STATE, {})
        if act == "probe": r = dry_probe(body.get("ams_id", 0), body.get("temp", 55))
        elif act == "dry": r = set_drying(body.get("ams_id", 0), True, body.get("temp", 55), body.get("hours", 8))
        elif act == "stop_dry": r = set_drying(body.get("ams_id", 0), False)
        elif act == "read_options": r = do_ams_read_options(body.get("on", True))
        elif act == "read_rfid": r = do_read_rfid(body.get("ams_id", 0), body.get("slot_id", 0), pause_drying=True)
        elif act == "resume_dry": r = set_drying(body.get("ams_id", 0), True, body.get("temp", 45), body.get("hours", 8), resume_saved=True)
        elif act == "sync_filaments": r = orca_filament_sync(force=bool(body.get("force")))
        elif act == "status": r = "ok"
        else: r = f"unknown action {act!r}"
        jsave(CMDRESULT, dict(t=now(), time=iso(), request=body, result=r,
                              print_state=st.get("print_state"),
                              ams={k: dict(temp=v.get("temp"), heating=v.get("heating"), sf=v.get("dry_sf"),
                                           setpoint=(v.get("dry_setting") or {}).get("dry_temperature"))
                                   for k, v in st.get("ams", {}).items()}))
        append(EVENTS, dict(t=now(), time=iso(), kind="CONTROL", msg=f"command file: {act} → {r}"))
    except Exception as e:
        print(f"[{iso()}] command file error: {e}", flush=True)


PULLBACK_RETRY = P("pullback_retry.json")
PULLBACK_MAX_RETRIES = 2
PULLBACK_RETRY_DELAY = 60


def pullback_fault(state):
    # 07xx8004: AMS failed to pull back filament. Do not retry other faults.
    try: code = int(state.get("print_error") or 0)
    except (TypeError, ValueError): return None
    return str(code) if code & 0xFF00FFFF == 0x07008004 else None


def pullback_fresh(state):
    return STATUS.get("mqtt") == "connected" and MQTT.get("client") and all(
        now()-state.get(k, 0) <= 120 and state.get(k, 0) >= STATUS.get("since", 0)
        for k in ("t", "print_state_t", "ams_status_t", "print_error_t"))


def pullback_recovery():
    saved=jload(PULLBACK_RETRY,None)
    if saved is None and not os.path.exists(PULLBACK_RETRY): return {}
    if not isinstance(saved,dict) or not isinstance(saved.get("auto_attempts",0),int) or not 0<=saved.get("auto_attempts",0)<=PULLBACK_MAX_RETRIES:
        raise ValueError("Pullback retry journal is invalid; recovery stopped")
    return saved


def pullback_retry_data(state):
    if not pullback_fault(state): return None
    recovery = pullback_recovery()
    last = recovery.get("last", 0)
    return dict(auto_attempts=recovery.get("auto_attempts", 0), limit=PULLBACK_MAX_RETRIES,
                busy=now()-last < PULLBACK_RETRY_DELAY,
                message=recovery.get("message") or "Automatic pullback recovery queued (up to 2 retries)")


def retry_pullback(automatic=False):
    """Retry the firmware's failed AMS step, never clear a fault or resume a job.

    Persist attempts before publish. Ambiguous delivery consumes the attempt and
    stops automatic recovery; reconnect/restart cannot reset the retry budget.
    """
    with DRY_LOCK:
        state=jload(STATE, {}); code=pullback_fault(state)
        if not code: return "The pullback fault is no longer active"
        if not pullback_fresh(state): return "Waiting for fresh printer status"
        if state.get("print_state") not in ("PAUSE", "FAILED", "IDLE", "FINISH"):
            return "Waiting for the printer to stop its current operation"
        if any(((int(h.get("code") or 0)>>16)&0xFFFF)==1 for h in state.get("hms", [])):
            return "Another printer fault needs attention before pullback can retry"
        r=pullback_recovery()
        if now()-r.get("last", 0)<PULLBACK_RETRY_DELAY: return "A pullback retry was just sent; waiting for the printer"
        if r.get("last") and any(state.get(k,0)<=r["last"] for k in ("print_error_t","ams_status_t")):
            return "Waiting for a fresh fault report after the last retry"
        if automatic and (r.get("auto_attempts",0)>=PULLBACK_MAX_RETRIES or r.get("delivery_unknown")):
            return "Automatic pullback retries stopped; use Retry pullback after checking the path"
        if automatic and r.get("cancelled"): return "Drying request was cancelled; automatic pullback retries stopped"
        attempt=r.get("auto_attempts",0)+(1 if automatic else 0)
        r.update(code=code,auto_attempts=attempt,last=now(),clear_since=None,
                 message=(f"Automatic pullback retry {attempt} of 2 sent — waiting for the printer" if automatic else "Pullback retry sent — waiting for the printer"))
        # An interrupted publish is treated as uncertain on restart.
        r["delivery_unknown"]=True;jsave(PULLBACK_RETRY,r)
        sequence=str(time.time_ns()%2147483647)
        try:
            result=MQTT["client"].publish(MQTT["topic_req"],json.dumps({"print":dict(
                sequence_id=sequence,command="ams_control",param="resume")}))
            if getattr(result,"rc",0)!=0: raise RuntimeError("publish failed")
        except Exception:
            r["message"]="Could not confirm retry delivery; automatic retries stopped"
            jsave(PULLBACK_RETRY,r);return r["message"]
        r["delivery_unknown"]=False;jsave(PULLBACK_RETRY,r)
        # Continue the original drying request only if the user has not cancelled it.
        op=jload(DRY_UNLOAD,{});requests=dry_requests();aid=op.get("ams_id");w=requests.get(aid,{})
        if aid is not None and op.get("status") in ("pending","failed") and w.get("status") in ("unloading","preparing","blocked") and w.get("allow_unload"):
            op.update(status="pending",since=now(),reply=None);jsave(DRY_UNLOAD,op)
            w.update(status="unloading",message=r["message"]);requests[aid]=w;jsave(DRY_REQUESTS,requests)
        append(EVENTS,dict(t=now(),time=iso(),kind="ERROR_ACTION",msg=r["message"]))
        return "ok"


def pullback_retry_tick():
    with DRY_LOCK:
        state=jload(STATE,{})
        if not pullback_fresh(state): return
        r=pullback_recovery()
        if not pullback_fault(state):
            # A transient cleared error during movement is not a new retry budget.
            if not r: return
            if state.get("ams_status") != 0:
                r.pop("clear_since",None)
            else:
                if not r.get("clear_since"): r["clear_since"]=now()
                if now()-r["clear_since"]>=60: r={}
            jsave(PULLBACK_RETRY,r);return
        r.pop("clear_since",None)
        signature=[pullback_fault(state),state.get("ams_status")]
        if r.get("observed")!=signature:
            r.update(observed=signature,stable_since=now())
        op=jload(DRY_UNLOAD,{});w=dry_requests().get(op.get("ams_id"),{})
        if op.get("status") in ("pending","failed") and w.get("status") in ("cancelled","held","stopped"):
            r.update(cancelled=True,message="Drying request cancelled — automatic pullback retries stopped")
        if r.get("auto_attempts",0)>=PULLBACK_MAX_RETRIES and now()-r.get("last",0)>=PULLBACK_RETRY_DELAY:
            r["message"]="Both automatic retries used. Check the filament path, then use Retry pullback."
        jsave(PULLBACK_RETRY,r)
        if now()-r.get("stable_since",now())<20: return
        retry_pullback(automatic=True)


def do_resolve(action):
    c = MQTT["client"]
    if not c: return "printer not connected"
    cmds = {"resume": {"print": {"sequence_id": "0", "command": "resume", "param": ""}},
            "stop":   {"print": {"sequence_id": "0", "command": "stop", "param": ""}},
            "retry":  {"print": {"sequence_id": "0", "command": "ams_control", "param": "resume"}},
            "done":   {"print": {"sequence_id": "0", "command": "ams_control", "param": "done"}}}
    if action not in cmds: return "unknown action"
    c.publish(MQTT["topic_req"], json.dumps(cmds[action]))
    append(EVENTS, dict(t=now(), time=iso(), kind="ERROR_ACTION", msg=f"sent '{action}' for a printer error"))
    return "ok"


def do_temp(kind, value):
    """Set bed / chamber / nozzle directly."""
    st = jload(STATE, {})
    lim = {"bed": 120, "chamber": 65, "nozzle": 320}
    if kind not in lim: return "unknown heater"
    v = max(0, min(lim[kind], int(value or 0)))
    printing = st.get("print_state") in ("RUNNING", "PREPARE", "PAUSE")
    if kind == "nozzle" and printing and v == 0: return "refusing to switch the nozzle off mid-print"
    g = {"bed": f"M140 S{v}\n", "chamber": f"M141 S{v}\n", "nozzle": f"M104 S{v}\n"}[kind]
    r = send_gcode(g)
    if str(r).startswith("ok"):
        try:
            stg = settings(); stg.setdefault("asked_env", {})[kind] = v; stg["asked_env"]["t"] = now(); save_settings(stg)
        except Exception: pass
    append(EVENTS, dict(t=now(), time=iso(), kind="PREHEAT", msg=f"{kind} set to {v} °C{' (mid-print)' if printing else ''} → {r}"))
    return r


def do_cooldown_heaters():
    """Turn off printer heaters without changing fan or AMS drying settings."""
    st = jload(STATE, {})
    if st.get("print_state") in ("RUNNING", "PREPARE", "PAUSE"):
        return "printer is printing — not switching off its heaters"
    cfg = jload(CONFIG, {}) or {}
    dual_nozzle = "H2D" in str(cfg.get("name", "")).upper() or len((st.get("env") or {}).get("nozzles") or []) > 1
    nozzle_gcode = "M104 S0 T0\nM104 S0 T1\n" if dual_nozzle else "M104 S0\n"
    r = send_gcode(nozzle_gcode + "M140 S0\nM141 S0\n")
    if r != "ok": return r
    stg = settings()
    stg["asked_env"] = dict(nozzle=0, bed=0, chamber=0, t=now())
    save_settings(stg)
    append(EVENTS, dict(t=now(), time=iso(), kind="PREHEAT", msg="Cooldown: printer nozzle(s), bed and chamber heaters off; fans and AMS dryers unchanged"))
    return r


def do_print_control(action):
    if action not in ("pause", "resume", "stop"): return "unknown action"
    c = MQTT["client"]
    if not c: return "printer not connected"
    c.publish(MQTT["topic_req"], json.dumps({"print": {"sequence_id": "0", "command": action, "param": ""}}))
    append(EVENTS, dict(t=now(), time=iso(), kind="PRINT_CTRL", msg=f"print {action.upper()} sent from dashboard"))
    return "ok"


CAMERA_INIT_ERROR = 0x05008092
CAMERA_RESUME = P("camera_resume.json")
CAMERA_WARNING_HOLD = 45


def camera_warning_status():
    """Keep a short-lived visual record after a camera-only pause clears."""
    record = jload(CAMERA_RESUME, {}) or {}
    at = record.get("resolved_at") or record.get("first_seen") or 0
    if not at or now() - at > CAMERA_WARNING_HOLD:
        return None
    return "resumed" if record.get("resolved_at") else "resuming"


def camera_resume_tick():
    """Resume one H2S print paused only by the toolhead-camera warning."""
    if STANDBY or "H2S" not in str(jload(CONFIG, {}).get("name", "")).upper(): return
    st = jload(STATE, {})
    code = int(st.get("print_error") or 0)
    state = st.get("print_state")
    record = jload(CAMERA_RESUME, {})
    if state == "RUNNING" or (state in ("IDLE", "FINISH") and code != CAMERA_INIT_ERROR):
        if record and not record.get("resolved_at"):
            record["resolved_at"] = now()
            jsave(CAMERA_RESUME, record)
        elif record and now() - record.get("resolved_at", 0) > CAMERA_WARNING_HOLD:
            jsave(CAMERA_RESUME, {})
        return
    if state != "PAUSE" or code != CAMERA_INIT_ERROR: return
    if STATUS.get("mqtt") != "connected" or not MQTT.get("client") or not MQTT.get("topic_req"): return
    connected = STATUS.get("since", 0)
    if any(not connected <= st.get(k, 0) <= now() or now() - st.get(k, 0) > 120
           for k in ("t", "print_state_t", "print_error_t")): return
    for h in st.get("hms") or []:
        try:
            if ((int(h.get("code") or 0) >> 16) & 0xffff) == 1: return
        except (TypeError, ValueError, AttributeError): return
    if not record or record.get("resolved_at"):
        jsave(CAMERA_RESUME, {"first_seen": now(), "attempts": 0})
        return
    attempts = int(record.get("attempts", int(bool(record.get("attempted")))))
    if attempts >= 2: return
    if attempts == 0 and now() - record.get("first_seen", now()) < 4: return
    if attempts and now() - record.get("attempted_at", now()) < 12: return
    # Record the attempt before publishing, so a restart never repeats an ambiguous command.
    attempt = attempts + 1
    jsave(CAMERA_RESUME, {"first_seen": record["first_seen"], "attempts": attempt, "attempted_at": now()})
    try:
        result = do_print_control("resume")
        append(EVENTS, dict(t=now(), time=iso(), kind="AUTO_CAMERA_RESUME",
                            msg=f"H2S camera warning 0500_8092: automatic resume attempt {attempt}/2 ({result})"))
    except Exception as exc:
        append(EVENTS, dict(t=now(), time=iso(), kind="AUTO_CAMERA_RESUME",
                            msg=f"H2S camera warning 0500_8092: automatic resume attempt {attempt}/2 failed ({exc})"))


def do_preheat(bed, chamber, label="", nozzle=None):
    with FAN_CONTROL_LOCK:
        return _do_preheat(bed, chamber, label, nozzle)


def _do_preheat(bed, chamber, label="", nozzle=None):
    st = jload(STATE, {})
    if st.get("print_state") in ("RUNNING", "PREPARE", "PAUSE"): return "printer is printing — not touching temperatures"
    bed = max(0, min(120, int(bed or 0))); chamber = max(0, min(65, int(chamber or 0)))
    nozzle = None if nozzle is None else max(0, min(320, int(nozzle)))
    g = f"M140 S{bed}\n" + (f"M141 S{chamber}\n" if chamber > 0 else "M141 S0\n")
    if nozzle is not None: g += f"M104 S{nozzle}\n"
    cooling = (bed == 0 and chamber == 0)
    env = st.get("env", {}); cur_bed = env.get("bed"); cur_ch = env.get("chamber")
    if cooling and cur_bed is not None and cur_bed <= 40 and (cur_ch is None or cur_ch <= 40):
        r = send_gcode(g + "M106 P1 S0\nM106 P2 S0\nM106 P3 S0\n")
        jsave(COOLDOWN, {"active": False})
        append(EVENTS, dict(t=now(), time=iso(), kind="PREHEAT", msg=f"{label or 'cool down'}: already cool (bed {cur_bed}°C) — heaters and fans off"))
        return "already cool — heaters and fans off"
    # P1 part fan, P2 aux fan, P3 chamber/exhaust fan: full blast on cool-down, off when soaking
    g += "M106 P1 S255\nM106 P2 S255\nM106 P3 S255\n" if cooling else "M106 P1 S0\nM106 P2 S0\nM106 P3 S0\n"
    r = send_gcode(g)
    try:
        _s = settings(); _s["asked_env"] = dict(bed=bed, chamber=chamber, t=now())
        if nozzle is not None: _s["asked_env"]["nozzle"] = nozzle
        save_settings(_s)
    except Exception: pass
    jsave(COOLDOWN, {"active": bool(cooling), "manual": True, "started_at": now()})
    append(EVENTS, dict(t=now(), time=iso(), kind="PREHEAT", msg=f"{label or 'preheat'}: bed {bed}°C, chamber {chamber or 'off'}{f', nozzle {nozzle}°C' if nozzle is not None else ''}, fans {'100%' if cooling else 'off'} → {r}"))
    return r


COOLDOWN = P("cooldown.json")
FAN_CONTROL_LOCK = _th.RLock()


def fan_timer_tick():
    with FAN_CONTROL_LOCK:
        cd = jload(COOLDOWN, {}); deadline = cd.get("fan_off_at")
        if not deadline or now() < deadline: return
        state = jload(STATE, {})
        if STATUS.get("mqtt") != "connected" or now()-state.get("t", 0) > 120: return
        if send_gcode("M106 P1 S0\nM106 P2 S0\nM106 P3 S0\n") == "ok":
            jsave(COOLDOWN, {"active": False, "fan_off_at": None, "stopped_at": now()})
            append(EVENTS, dict(t=now(), time=iso(), kind="CONTROL", msg="7-minute fan timer finished — all fans off requested"))


def cooldown_tick():
    cd = jload(COOLDOWN, {}); st = jload(STATE, {}); env = st.get("env", {})
    # Only an explicit Cool down click owns a cooling cycle. Retire old automatic
    # cycles without sending any heater or fan commands on upgrade.
    if cd.get("active") and not cd.get("manual"):
        jsave(COOLDOWN, {"active": False}); return
    if STATUS.get("mqtt")!="connected" or now()-st.get("t",0)>120: return
    bed = env.get("bed"); ch = env.get("chamber"); idle = st.get("print_state") in ("IDLE", "FINISH")
    fans = st.get("fans", {})
    if cd.get("active"):
        if not idle:
            jsave(COOLDOWN, {"active": False}); return                     # a print took over
        if bed is not None and bed <= 40 and (ch is None or ch <= 40):
            send_gcode("M106 P1 S0\nM106 P2 S0\nM106 P3 S0\n"); jsave(COOLDOWN, {"active": False, "stopped_at": now()})
            act_step(f"Cooled to {int(bed)} °C — fans off", done=True)
            append(EVENTS, dict(t=now(), time=iso(), kind="PREHEAT", msg=f"cool-down finished (bed {bed}°C) — fans off"))
        return
    # Fans outside a dashboard-owned cool-down remain under printer/user control.


def psu_check_tick():
    # A reduced setpoint can be a filament/printing protection, not evidence of a missing PSU.
    return

def missing_drying_slots(aid, unit, spools, bindings):
    missing = []
    for tid, tray in unit.get("trays", {}).items():
        sid = bindings.get(slot_key(aid, tid)); spool = spools.get(sid, {})
        known = spool and not spool.get("provisional") and spool_rule(spool)["key"] != "?"
        if not known and (tray.get("present") or not tray.get("empty", True) or sid):
            missing.append(dict(key=slot_key(aid, tid), slot=int(tid)+1))
    return missing


def request_fleet_drying_unit(aid):
    """Explicit fleet request: retain missing setup, use the existing guarded queue."""
    aid = str(aid)
    with DRY_LOCK:
        state=jload(STATE,{}); spools=jload(SPOOLS,{}); requests=dry_requests()
        unit=state.get("ams",{}).get(aid,{}); previous=requests.get(aid,{})
        missing=missing_drying_slots(aid,unit,spools,state.get("slot_spool",{}))
        fresh=STATUS.get("mqtt")=="connected" and all(now()-state.get(k,0)<=120 and state.get(k,0)>=STATUS.get("since",0) for k in ("t","print_state_t"))
        result=dict(ams_id=int(aid),ams=ams_label(aid),slots=missing)
        if not fresh:
            status,message="setup","Waiting for the printer to reconnect with fresh readings"
        elif ams_is_drying(unit):
            return dict(result,status="drying",message="Already drying — left running")
        elif ams_info_bits(unit.get("info"),0,4) not in (3,4):
            status,message="unavailable","This AMS does not report support for active drying"
        elif missing:
            status,message="setup","Set the filament in slot"+("s " if len(missing)>1 else " ")+", ".join(str(s["slot"]) for s in missing)+". Drying will continue after setup."
        else:
            plan=plan_unit(aid,unit,spools,state.get("slot_spool",{}))
            if not plan:
                status,message="empty","No filament in this AMS"
            elif previous.get("status") in ("preparing","unloading","pending","waiting"):
                return dict(result,status=previous["status"],message=previous.get("message","Drying request queued"))
            else:
                reply=set_drying(aid,True,plan["temp"],24,False,manual=True)
                current=dry_requests(); w=current.get(aid,{})
                if reply.startswith("ok"):
                    w["fleet_requested"]=True;current[aid]=w;jsave(DRY_REQUESTS,current)
                    return dict(result,status=w.get("status","pending"),message=w.get("message","Waiting for printer confirmation"))
                status,message="blocked",reply
        w=dict(previous,since=previous.get("since",now()),last=now(),status=status,message=message,
               fleet_requested=True,setup_slots=missing,owned=False)
        requests[aid]=w;jsave(DRY_REQUESTS,requests)
        return dict(result,status=status,message=message)


def request_fleet_drying():
    state=jload(STATE,{});spools=jload(SPOOLS,{})
    return [request_fleet_drying_unit(aid) for aid in drying_order(state,settings(),spools)]


def fleet_setup_tick():
    """Continue only explicitly requested units; cancellation removes setup state."""
    with DRY_LOCK:
        state=jload(STATE,{});spools=jload(SPOOLS,{});requests=dry_requests()
        for aid in drying_order(state,settings(),spools):
            w=requests.get(aid,{})
            if w.get("status")=="setup" and w.get("fleet_requested"):
                request_fleet_drying_unit(aid)


# Native AMS recovery: mirrors Bambu Studio's AMSDryControl Unload action.
# https://github.com/bambulab/BambuStudio/blob/master/src/slic3r/GUI/AMSDryControl.cpp
# command_ams_change_filament(false, ams_id, "255") uses 210/210 and omits
# extruder_id. Firmware chooses the path; never send a raw cutter/G-code macro.
DRY_UNLOAD = P("dry_unload.json")
DRY_UNLOAD_TIMEOUT = 300


def dry_unload_busy():
    return jload(DRY_UNLOAD, {}).get("status") == "pending"


def dry_unload_reply(pr):
    if pr.get("command") != "ams_change_filament": return
    with DRY_LOCK:
        op = jload(DRY_UNLOAD, {})
        if op.get("status") != "pending" or str(pr.get("sequence_id")) != op.get("sequence"): return
        if pr.get("result") not in ("success", "fail"): return
        op["reply"] = pr["result"]
        op["error"] = pr.get("errno", pr.get("err_code"))
        op["soft_temp"] = pr.get("soft_temp")
        op["reason"] = pr.get("reason")
        jsave(DRY_UNLOAD, op)


def rfid_unload_reply(pr):
    if pr.get("command")!="ams_change_filament" or pr.get("result") not in ("success","fail"):return
    with DRY_LOCK:
        cycles=jload(RFID_HEAT_CYCLES,{}) or {}
        for aid,w in cycles.items():
            if w.get("stage")=="unload" and str(pr.get("sequence_id"))==w.get("unload_sequence"):
                w["unload_reply"]=pr["result"]
                w["unload_error"]=pr.get("errno",pr.get("err_code"))
                jsave(RFID_HEAT_CYCLES,cycles)
                break
            if w.get("stage")=="clear_outlet" and str(pr.get("sequence_id"))==w.get("outlet_sequence"):
                w["outlet_reply"]=pr["result"]
                w["outlet_error"]=pr.get("errno",pr.get("err_code"))
                jsave(RFID_HEAT_CYCLES,cycles)
                break


def dry_unload_thermal_refusal(op):
    """Bambu Studio's errno -2/-4 plus soft_temp describe a thermal interlock.
    Wait for its actual threshold; never bypass it or retry an unexplained error.
    """
    error = fnum(op.get("error")); limit = fnum(op.get("soft_temp"))
    if error not in (-2, -4) or limit is None or not math.isfinite(limit) or not 0 < limit <= 150:
        return None
    return dict(sensor="ams" if error == -4 else "chamber", limit=limit, since=now())


def drying_prepare_tick():
    """One native unload at a time, only for an explicit drying request, once per request."""
    with DRY_LOCK:
        state = jload(STATE, {}); stg = settings(); requests = dry_requests()
        fresh = STATUS.get("mqtt") == "connected" and MQTT.get("client") and all(
            now()-state.get(k, 0) <= 120 and state.get(k, 0) >= STATUS.get("since", 0)
            for k in ("t", "print_state_t", "ams_status_t"))
        op = jload(DRY_UNLOAD, {})
        if op.get("status") == "pending":
            aid = op["ams_id"]; w = requests.get(aid, {}); a = state.get("ams", {}).get(aid, {})
            failed = op.get("reply") == "fail" or now()-op["since"] >= DRY_UNLOAD_TIMEOUT
            clear = fresh and state.get("ams_status_t", 0) > op["since"] and now()-op["since"] >= 5 and state.get("ams_status") == 0 and a.get("dry_sf") is not None and not any(r in a["dry_sf"] for r in (2, 3, 10))
            if failed:
                thermal = dry_unload_thermal_refusal(op) if op.get("reply") == "fail" else None
                if thermal and w.get("status") in ("preparing", "unloading") and w.get("unload_attempts", 1) < 3:
                    op["status"] = "waiting_temperature"; jsave(DRY_UNLOAD, op)
                    w.update(status="preparing", unload_sent=False, unload_thermal_wait=thermal,
                             message=f"Waiting for the {thermal['sensor'].upper() if thermal['sensor']=='ams' else 'chamber'} to cool below {thermal['limit']:g} °C — filament will retract and drying will start automatically")
                    requests[aid] = w; jsave(DRY_REQUESTS, requests)
                    act_step(w["message"])
                    return
                op["status"] = "failed"; jsave(DRY_UNLOAD, op)
                if w.get("status") in ("preparing", "unloading"):
                    dry_block(aid, w, "The printer refused the remote unload." if op.get("reply") == "fail" else "The printer has not confirmed the outlet is clear after 5 minutes. Remote unloading stopped; check the filament path before retrying.")
                    w["unload_error"] = op.get("error"); requests[aid] = w; jsave(DRY_REQUESTS, requests)
                return
            if not clear: return
            op["status"] = "completed"; jsave(DRY_UNLOAD, op)
            if w.get("status") in ("preparing", "unloading"):
                w.update(status="waiting", message="Filament retracted — preparing drying", last=0, unload_done=True, unload_sent=True)
                requests[aid] = w; jsave(DRY_REQUESTS, requests)
            # Return to the drying scheduler before preparing another AMS.
            return
        for aid in drying_order(state, stg, jload(SPOOLS, {}), requests):
            w = requests[aid]
            if w.get("status") != "preparing" or not w.get("allow_unload"): continue
            a = state.get("ams", {}).get(aid, {}); reasons = a.get("dry_sf")
            def wait(message):
                w["message"] = message; requests[aid] = w; jsave(DRY_REQUESTS, requests)
            if not fresh:
                wait("Waiting for fresh printer readings before retracting filament"); continue
            if state.get("print_state") not in ("IDLE", "FINISH", "FAILED"):
                wait("Drying queued — filament will retract after the print or operation finishes"); continue
            if unresolved_print_fault(state):
                dry_block(aid,w,"Clear the printer fault before retrying remote unload and drying");requests[aid]=w;jsave(DRY_REQUESTS,requests);continue
            thermal = w.get("unload_thermal_wait")
            if thermal and reasons is not None and 3 not in reasons:
                w.pop("unload_thermal_wait", None); thermal = None
            if thermal:
                env = state.get("env", {})
                temp = fnum(a.get("temp") if thermal["sensor"] == "ams" else env.get("chamber"))
                target = fnum(env.get("chamber_target")) if thermal["sensor"] == "chamber" else 0
                if now()-thermal["since"] > 3600:
                    dry_block(aid,w,"The printer's unload temperature restriction has not cleared after an hour. Drying is stopped; check the AMS/chamber temperature before retrying.");requests[aid]=w;jsave(DRY_REQUESTS,requests);continue
                if temp is None or temp >= thermal["limit"]-1 or (target is not None and target >= thermal["limit"]):
                    sensor = "AMS" if thermal["sensor"] == "ams" else "chamber"
                    wait(f"Waiting for the {sensor} to cool below {thermal['limit']:g} °C"+(f" (now {temp:g} °C)" if temp is not None else "")+" — unloading and drying will continue automatically");continue
                w.pop("unload_thermal_wait", None)
            if reasons is None:
                wait("Waiting for AMS drying availability");continue
            if any(r not in DRY_WAIT_REASONS and r != 3 for r in reasons):
                reason=next(r for r in reasons if r not in DRY_WAIT_REASONS and r != 3)
                dry_block(aid,w,DRY_REASON_TEXT.get(reason,"The printer reports an unsupported drying restriction"));requests[aid]=w;jsave(DRY_REQUESTS,requests);continue
            if 3 not in reasons:
                w.update(status="waiting",message="Outlet clear — preparing drying",last=0);requests[aid]=w;jsave(DRY_REQUESTS,requests);continue
            if state.get("ams_status") != 0:
                wait("Waiting for the printer's current filament movement to finish before retracting this AMS");continue
            if filament_busy(stg) or now()<state.get("scan_until",0):
                wait("Waiting for the filament load, unload, or tag scan to finish");continue
            firmware_wait = next((r for r in reasons if r in DRY_WAIT_REASONS), None)
            if firmware_wait is not None:
                wait(DRY_REASON_TEXT.get(firmware_wait,"Waiting for the AMS"));continue
            if ams_is_drying(a) or ams_info_bits(a.get("info"),4,4) in (1,3,4):
                wait("Waiting for this AMS's current drying or cooling cycle to finish");continue
            power_owner = dry_slot_taken_by(aid,state,stg)
            if power_owner is not None:
                wait(f"Waiting for AMS {int(power_owner)+1} to release the shared printer drying power");continue
            if any(x.get("status")=="pending" for x in requests.values()):
                wait("Waiting for the current drying request before retracting filament");continue
            if w.get("unload_sent"):
                dry_block(aid,w,"Filament is still reported at the outlet after remote unloading. Check the path before retrying.");requests[aid]=w;jsave(DRY_REQUESTS,requests);continue
            plan=plan_unit(aid,a,jload(SPOOLS,{}),state.get("slot_spool",{}))
            if not plan or plan.get("unidentified"):
                dry_block(aid,w,"Set the material in every occupied slot before retrying drying");requests[aid]=w;jsave(DRY_REQUESTS,requests);continue
            sequence=str(time.time_ns()%2147483647)
            # Persist intent before publishing: restart/reconnect must never duplicate unload.
            op=dict(status="pending",ams_id=aid,sequence=sequence,since=now())
            jsave(DRY_UNLOAD,op)
            w.update(status="unloading",unload_sent=True,unload_attempts=w.get("unload_attempts",0)+1,message="Printer is retracting filament automatically — drying will start when the outlet is clear")
            requests[aid]=w;jsave(DRY_REQUESTS,requests)
            payload=dict(command="ams_change_filament",sequence_id=sequence,ams_id=int(aid),target=255,slot_id=255,curr_temp=210,tar_temp=210)
            try:
                result=MQTT["client"].publish(MQTT["topic_req"],json.dumps({"print":payload}))
                if getattr(result,"rc",0)!=0:raise RuntimeError("publish failed")
            except Exception:
                # Delivery may be ambiguous. Never resend this operation automatically.
                op["reply"]="fail";jsave(DRY_UNLOAD,op)
            append(EVENTS,dict(t=now(),time=iso(),kind="FILAMENT",slot=f"{aid}:*",msg="Native remote unload requested for drying; waiting for confirmed outlet clearance"))
            return


def queue_drying_unload(aid, w, state):
    reasons=state.get("ams",{}).get(str(aid),{}).get("dry_sf") or []
    if not w.get("allow_unload") or w.get("unload_sent") or 3 not in reasons or any(r not in DRY_WAIT_REASONS and r!=3 for r in reasons): return False
    w.update(status="preparing",message="Preparing drying — filament will retract automatically when the printer is idle")
    return True


def dry_pending_tick():
    fleet_setup_tick()
    drying_prepare_tick()
    state=jload(STATE,{});stg=settings();spools=jload(SPOOLS,{})
    if STATUS.get("mqtt")!="connected" or now()-state.get("t",0)>120: return
    with DRY_LOCK:
        requests=dry_requests()
        for aid in drying_order(state,stg,spools,requests):
            w=requests[aid]
            if w.get("status") not in ("pending","waiting","blocked"):continue
            a=state.get("ams",{}).get(aid,{})
            if ams_is_drying(a):
                if w.get("status")!="blocked" and not w.get("safety_stop_sent"):
                    w.update(status="succeeded",observed_active=True);act_step("Drying cycle is running",done=True)
                continue
            if w.get("status")=="blocked":
                # A cleared physical condition is no longer an active error. Preserve the
                # user's request without automatically restarting its heater.
                outlet = w.get("block_kind")=="outlet" or (
                    "filament" in w.get("message", "").lower() and "outlet" in w.get("message", "").lower())
                if outlet and not w.get("safety_stop_sent") and drying_readiness(aid,state,stg)[0]=="ready":
                    w.update(status="retry_ready", owned=False,
                             message="The AMS outlet is clear. Choose Retry drying to start your saved request.")
                    act_step(w["message"],done=True,outcome="cancelled")
                continue
            ready,message=drying_readiness(aid,state,stg,w.get("resume_saved",False))
            if ready=="blocked":
                if not queue_drying_unload(aid,w,state):dry_block(aid,w,message+". Automatic retries are stopped.")
                continue
            if ready=="wait":w.update(status="waiting",message=message);continue
            if w.get("accepted"):
                if now()-w.get("accepted_at",w.get("last",0))<=120:continue
                # An ACK confirms receipt, not heater startup. Retry once only,
                # after fresh telemetry and all readiness checks above allow it.
                if w.get("accepted_start_retries",0)>=1 or w.get("attempts",0)>=DRY_MAX_ATTEMPTS:
                    dry_block(aid,w,"The printer accepted drying but still has not started it. Automatic retries are stopped; check its drying screen.")
                    continue
                w.update(accepted=False,accepted_start_retries=w.get("accepted_start_retries",0)+1,
                         reply_received=False,err=None,status="pending",
                         message="Drying has not started — retrying once")
                jsave(DRY_REQUESTS,requests)
                result=set_drying(aid,True,w["temp"],w["hours"],w.get("rotate",False),retry=True,resume_saved=w.get("resume_saved",False))
                requests[aid]=dry_requests().get(aid,w)
                if not result.startswith(("ok","already drying")):
                    dry_block(aid,requests[aid],result+". Automatic retries are stopped.")
                continue
            if w.get("attempts",0)>=DRY_MAX_ATTEMPTS:
                if w.get("status")=="waiting":w["message"]="Automatic retries paused. Retry manually when the printer is available."
                elif now()-w.get("last",0)>=90:dry_block(aid,w,"Drying did not start after three attempts. Check the printer screen, then retry. Automatic retries are stopped.")
                continue
            if w.get("status")=="waiting" and w.get("reply_received") and w.get("err") in DRY_WAIT_CODES and dry_wait_context(aid,state)==w.get("wait_context"):
                continue
            delay=20 if w.get("reply_received") or w.get("status")=="waiting" else 90
            if now()-w.get("last",0)<delay:continue
            jsave(DRY_REQUESTS,requests)
            set_drying(aid,True,w["temp"],w["hours"],w.get("rotate",False),retry=True,resume_saved=w.get("resume_saved",False))
            requests[aid]=dry_requests().get(aid,w)
        jsave(DRY_REQUESTS,requests)

def cool_read_tick():
    stg=settings();pending=stg.get("cool_read") or {};state=jload(STATE,{})
    if state.get("print_state") not in ("IDLE","FINISH"):return
    changed=False
    for key,w in list(pending.items()):
        aid,slot=key.split(":");a=state.get("ams",{}).get(aid,{})
        if now()-w.get("since",0)>3*3600:pending.pop(key,None);changed=True;continue
        if ams_is_drying(a) or (a.get("temp") or 0)>READ_MAX_TEMP:continue
        result=do_read_rfid(aid,slot)
        if result=="ok":pending.pop(key,None);changed=True
    if changed:stg=settings();stg["cool_read"]=pending;save_settings(stg)

def ams_wedged_tick():
    """A slot that has read tags before but now fails every scan means the AMS itself is wedged —
    a power cycle fixes it. Say so instead of blaming the spool."""
    st = jload(STATE, {}); sp = jload(SPOOLS, {}); stg = settings()
    hist = stg.setdefault("slot_read_ok", {})        # slots that have ever produced a tag
    wedged = []
    for key, sid in (st.get("slot_spool") or {}).items():
        a, tr = key.split(":")
        t = ((st.get("ams", {}).get(a, {}).get("trays", {}) or {}).get(tr, {}) or {})
        if t.get("rfid"): hist[key] = True; continue
        unit = st.get("ams", {}).get(a, {})
        if (unit.get("temp") or 0) > READ_MAX_TEMP or unit.get("heating"): continue   # hot units cannot read; not a fault
        if t.get("state") == 9 and t.get("read_done") and hist.get(key): wedged.append(key)
    msg = None
    if len(wedged) >= 2:
        msg = (f"{len(wedged)} slots that have read tags before are now all failing to read — the AMS is "
               f"most likely wedged. Power-cycle the printer; that clears it. Do not bother reseating spools.")
    elif len(wedged) == 1 and not stg.get("wedge_note"):
        msg = (f"slot {int(wedged[0].split(':')[1])+1} has read tags before but is not reading now. If a known-good "
               f"spool also fails there, the AMS is wedged — power-cycle the printer.")
    if msg and stg.get("wedge_note") != msg:
        stg["wedge_note"] = msg
        append(EVENTS, dict(t=now(), time=iso(), kind="RFID", msg=msg))
    if not wedged and stg.get("wedge_note"):
        stg.pop("wedge_note", None)
    stg["slot_read_ok"] = hist; save_settings(stg)


def rfid_retry_tick():
    """Read a newly occupied, unread slot promptly after the printer's own scan chance."""
    st = jload(STATE, {}); sp = jload(SPOOLS, {})
    stg = settings(); probes = stg.get("rfid_auto_probe") or {}; active = {}
    for key, since in (st.get("pending") or {}).items():
        try: aid, tid = key.split(":")
        except ValueError: continue
        tray = (st.get("ams", {}).get(aid, {}).get("trays") or {}).get(tid, {})
        if not tray.get("present") or tray.get("rfid"): continue
        if tray.get("reading"):
            if key in probes: active[key] = probes[key]
            continue
        old = probes.get(key, {})
        if not old and now()-since>600: continue
        token = f"pending:{since}"
        active[key] = old if old.get("token") == token else dict(token=token, first_seen=since, attempts=0, last=0)
    for key, sid in (st.get("slot_spool") or {}).items():
        if key in active: continue
        if not sp.get(sid, {}).get("provisional"): continue
        aid, tid = key.split(":")
        tray = (st.get("ams", {}).get(aid, {}).get("trays") or {}).get(tid, {})
        if not tray.get("present") or tray.get("empty") or tray.get("rfid"): continue
        if tray.get("reading"):
            if key in probes: active[key] = probes[key]
            continue
        probe = probes.get(key, {})
        if not probe and now()-sp[sid].get("first_seen",0)>600: continue
        if not probe or (probe.get("token") not in (f"provisional:{sid}",) and not str(probe.get("token", "")).startswith("pending:")):
            probe = dict(token=f"provisional:{sid}", first_seen=sp[sid].get("first_seen", now()), attempts=0, last=0)
        active[key] = probe
    changed = active != probes
    # Firmware may read during a print, but an explicit scan must wait until idle.
    if st.get("print_state") not in ("IDLE", "FINISH", "FAILED"):
        if changed: stg["rfid_auto_probe"] = active; save_settings(stg)
        return
    for key, probe in sorted(active.items()):
        if probe["attempts"] >= 1: continue
        # Allow a few seconds for the AMS's native insertion read, then try once.
        if now() - probe["first_seen"] < 5: continue
        aid, tid = key.split(":")
        unit = st.get("ams", {}).get(aid, {})
        if aid in (jload(RFID_HEAT_CYCLES, {}) or {}): continue
        if do_read_rfid(aid, tid, pause_drying=True) == "ok":
            probe["attempts"] += 1; probe["last"] = now(); changed = True
            break  # one physical spool movement at a time
    if changed:
        stg = settings(); stg["rfid_auto_probe"] = active; save_settings(stg)


def drying_start_in_flight(aid, state, requests):
    """Serialize startup per printer, including units with independent power.

    Queued requests with no command sent cannot reserve this slot. A rejection,
    confirmed cycle, cancellation or expired confirmation window releases it.
    """
    for other,w in requests.items():
        if other==aid or w.get("status") not in ("pending","waiting"):continue
        if not w.get("attempts") or w.get("start_deferred") or ams_is_drying(state.get("ams",{}).get(other,{})):continue
        if w.get("reply_received") and not w.get("accepted"):continue
        started=w.get("accepted_at",w.get("last",0)) if w.get("accepted") else w.get("last",0)
        if now()-started<150:return other
    return None


def set_drying(ams_id, on, temp=65, hours=8, rotate=False, manual=False, conflict=None, retry=False, resume_saved=False):
    try:
        aid = str(int(ams_id)); temp = int(temp); hours = int(hours)
    except (TypeError, ValueError, OverflowError): return "invalid drying settings"
    if on and (not 45<=temp<=85 or not 1<=hours<=24): return "use 45–85 °C and 1–24 hours"
    with DRY_LOCK:
        state = jload(STATE, {}); stg = settings(); requests = dry_requests(); previous = requests.get(aid, {})
        c = MQTT.get("client")
        if manual:
            cycles=jload(RFID_HEAT_CYCLES,{}) or {}
            if aid in cycles:
                cycles.pop(aid);jsave(RFID_HEAT_CYCLES,cycles)
        if not on:
            cancel_dry_request(aid, manual_stop=manual, automatic_stop=not manual)
            if not c or STATUS.get("mqtt") != "connected": return "request cancelled; printer is offline, so its heater state cannot be confirmed"
            payload = dict(sequence_id=str(time.time_ns()%2147483647), command="ams_filament_drying", ams_id=int(aid),
                           mode=0,temp=0,duration=0,cooling_temp=0,humidity=0,rotate_tray=False,filament="",close_power_conflict=False)
            c.publish(MQTT["topic_req"], json.dumps({"print":payload}))
            append(EVENTS,dict(t=now(),time=iso(),kind="DRY_CMD",slot=f"{aid}:*",msg=f"{ams_label(aid)}: targeted drying stop"))
            return "ok"
        if manual and previous.get("status") in ("preparing","unloading","pending"): return "ok — a drying request is already pending"
        if retry and previous.get("status") not in ("pending", "waiting"): return "request is no longer pending"
        if not manual and not retry and not resume_saved and dry_is_held(previous): return "drying is pending or paused — use Retry drying"
        if retry and previous.get("attempts", 0)>=DRY_MAX_ATTEMPTS: return "automatic retry limit reached"
        a = state.get("ams", {}).get(aid, {})
        if ams_is_drying(a): return "already drying — the printer controls this cycle"
        spools = jload(SPOOLS,{})
        plan = plan_unit(aid,a,spools,state.get("slot_spool",{})) if a else None
        if a and (not plan or plan.get("unidentified")) and not resume_saved: return "set the material for every occupied slot before drying"
        safe_temp = min(temp, UNIT_MAX(aid), plan["temp"] if plan else temp)
        if state.get("print_state") == "RUNNING" and not has_psu(stg, aid):
            safe_temp = min(safe_temp, PRINT_SHARED_MAX)
        w = dict(previous) if retry else dict(since=now(),attempts=0,source="dashboard" if manual else "auto",owned=False,allow_unload=manual)
        if resume_saved: w["source"] = "rfid_resume"
        w.update(temp=safe_temp,hours=hours,rotate=bool(rotate),last=now(),resume_saved=bool(resume_saved or w.get("resume_saved")))
        readiness, message = drying_readiness(aid,state,stg,w["resume_saved"])
        if readiness == "ready" and not manual:
            preferred = higher_priority_drying_unit(aid,state,stg,spools,requests)
            if preferred is not None:
                readiness, message = "wait", f"Waiting for {ams_label(preferred)} — its filament is less dry"
        if readiness == "ready":
            starting=drying_start_in_flight(aid,state,requests)
            if starting is not None:
                readiness,message="wait",f"Waiting for {ams_label(starting)} to confirm its drying start"
        if readiness != "ready":
            w["start_deferred"]=True
            if readiness == "blocked" and queue_drying_unload(aid,w,state): pass
            elif readiness == "blocked": dry_block(aid,w,message+". Automatic retries are stopped.")
            else: w.update(status="waiting",message=message,wait_context=dry_wait_context(aid,state))
            requests[aid]=w; jsave(DRY_REQUESTS,requests)
            return "ok — "+message
        sequence=str(time.time_ns()%2147483647)
        w.update(status="pending",attempts=w.get("attempts",0)+1,sequence=sequence,reply_received=False,accepted=False,start_deferred=False,
                 message="Starting drying — waiting for the printer",owned=False)
        requests[aid]=w;jsave(DRY_REQUESTS,requests)
        payload=dict(sequence_id=sequence,command="ams_filament_drying",ams_id=int(aid),mode=1,temp=safe_temp,
                     duration=hours,cooling_temp=45,humidity=0,rotate_tray=bool(rotate) and state.get("print_state") in ("IDLE","FINISH"),
                     filament="",close_power_conflict=False)
        if manual: act_start("Starting drying",f"Requesting {safe_temp} °C for {hours} h")
        sent=c.publish(MQTT["topic_req"],json.dumps({"print":payload}))
        if getattr(sent,"rc",0)!=0:
            w.update(status="waiting",start_deferred=True,message="Waiting for the printer connection");jsave(DRY_REQUESTS,requests)
        append(EVENTS,dict(t=now(),time=iso(),kind="DRY_CMD",slot=f"{aid}:*",msg=f"{ams_label(aid)} drying START {safe_temp}°C {hours} h ({w['source']})"))
        return "ok" if safe_temp==temp else f"ok — temperature capped at {safe_temp} °C"

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


SEVERITY = {1: "fatal", 2: "serious", 3: "common", 4: "info"}
ERRDB_FILE = os.path.join(os.path.dirname(BASE), "error_texts.json")
ERRDB_URLS = {
    "print": "https://raw.githubusercontent.com/greghesp/ha-bambulab/37f2c20d55ec018a3a3072e12361d6eccdfe6f9f/custom_components/bambu_lab/pybambu/const_print_errors.py",
    "hms":   "https://raw.githubusercontent.com/greghesp/ha-bambulab/37f2c20d55ec018a3a3072e12361d6eccdfe6f9f/custom_components/bambu_lab/pybambu/const_hms_errors.py",
}
ERRDB = {"loaded": 0, "print": {}, "hms": {}}


def errdb():
    if ERRDB["loaded"] and now() - ERRDB["loaded"] < 3600: return ERRDB
    cached = jload(ERRDB_FILE, None)
    if cached and now() - cached.get("fetched", 0) < 30 * 86400:
        ERRDB.update({k: cached.get(k, {}) for k in ("print", "hms")}, loaded=now()); return ERRDB
    import urllib.request, ssl, re as _re
    ctxs = [None]
    try:
        import certifi; ctxs.append(ssl.create_default_context(cafile=certifi.where()))
    except Exception: pass
    got = {"print": dict((cached or {}).get("print", {})), "hms": dict((cached or {}).get("hms", {}))}
    for kind, url in ERRDB_URLS.items():
        for ctx in ctxs:
            try:
                txt = urllib.request.urlopen(url, timeout=25, context=ctx).read().decode("utf-8", "replace")
                for k, v in _re.findall(r'"([0-9A-Fa-f]{8,16})"\s*:\s*"((?:[^"\\]|\\.)*)"', txt):
                    if v.strip(): got[kind][k.upper()] = v.strip()
                break
            except Exception: continue
    if any(got.values()): jsave(ERRDB_FILE, dict(fetched=now(), **got))
    ERRDB.update(got, loaded=now()); return ERRDB


def _code4(n):
    h = f"{int(n) & 0xFFFFFFFF:08X}"
    return h[:4] + "_" + h[4:]


def dry_err_text(code):
    """Bambu's own wording for a drying-command error code, if we have it."""
    try: c = int(code)
    except Exception: return None
    if not c: return None
    flat = f"{c:08X}"
    return errdb()["print"].get(flat) or errdb()["print"].get(flat.lower())


# The printer is telling us to wait, not that anything is broken. Retrying harder
# cannot help and unloading filament is actively wrong.
DRY_WAIT_CODES = {
    0x03004013,  # printing cannot start while an AMS is drying
    0x05004040,  # no power for drying during printing
    0x05004041,  # only one AMS may dry during printing
    0x05004042,  # this AMS is in use for printing
    0x05004043,  # only one AMS may dry at a time
}


def error_data(st):
    """Show stopped-job faults and explicit AMS motor stalls in any job state.

    AMS health faults describe filament movement independently of print status.
    Preserve the detailed cause even when a generic print error is also present.
    """
    out = {"print_error": None, "hms": []}
    state = st.get("print_state"); stopped = state in ("PAUSE", "FAILED")
    db = errdb()
    try: pe = int(st.get("print_error") or 0)
    except (TypeError, ValueError): pe = 0
    # AMS / AMS HT assist-motor overload, including firmware unit variants.
    motor_print = (pe & 0xFF00FFFF) in (0x07008010, 0x18008010)
    fallback = ("The print is paused because the printer reported a fault. Check the printer screen for the cause and recovery steps."
                if state == "PAUSE" else
                "The print stopped because the printer reported a fault. Check the printer screen for the cause before restarting.")
    if pe and pe != 0x0300400C and (stopped or motor_print):
        code = _code4(pe); flat = code.replace("_", "")
        text = db["print"].get(flat) or ("The AMS motor is overloaded. The filament may be tangled or stuck." if motor_print else fallback)
        out["print_error"] = dict(code=code, text=text, process="filament" if motor_print else "print",
            url=f"https://e.bambulab.com/query.php?{flat}", blocking=True)
    for h in (st.get("hms") or []):
        try:
            attr = int(h.get("attr", 0)); code = int(h.get("code", 0))
            severity = SEVERITY.get((code >> 16) & 0xFFFF)
            # Serious AMS assist/slot motor overload is actionable even at idle.
            # Other common/info diagnostics remain outside the interruption panel.
            motor = (attr >> 24) in (0x07, 0x12, 0x18) and code == 0x00020002
            if not motor and not (stopped and severity == "fatal"): continue
            full = f"{_code4(attr)}_{_code4(code)}"; flat = full.replace("_", "")
            text = db["hms"].get(flat) or ("The AMS motor is overloaded. The filament may be tangled or stuck." if motor else fallback)
            out["hms"].append(dict(code=full, severity=severity, text=text, process="filament" if motor else "print",
                url=f"https://e.bambulab.com/query.php?{flat}", blocking=True))
        except (TypeError, ValueError): continue
    return out


# Stage labels follow Bambu Studio's get_stage_string(stg_cur).
PRINTER_STAGES = {
    0:"Printing", 1:"Leveling the bed", 2:"Preheating the bed", 3:"Calibrating vibration compensation",
    4:"Changing filament", 5:"Print paused", 6:"Paused — filament ran out", 7:"Heating the nozzle",
    8:"Calibrating filament flow", 9:"Scanning the bed", 10:"Inspecting the first layer",
    11:"Checking the build plate", 12:"Calibrating lidar", 13:"Homing the toolhead",
    14:"Cleaning the nozzle", 15:"Checking nozzle temperature", 16:"Print paused",
    17:"Paused — front cover open", 18:"Calibrating lidar", 19:"Calibrating flow ratio",
    20:"Paused — nozzle temperature fault", 21:"Paused — bed temperature fault", 22:"Unloading filament",
    23:"Paused — motion fault", 24:"Loading filament", 25:"Calibrating motor noise",
    26:"Paused — AMS disconnected", 27:"Paused — nozzle cooling fan fault",
    28:"Paused — chamber temperature fault", 29:"Cooling the chamber", 30:"Print paused",
    31:"Calibrating motor noise", 32:"Paused — filament buildup detected", 33:"Paused — cutter fault",
    34:"Paused — first layer needs attention", 35:"Paused — nozzle clog detected",
}


def current_printer_action(state):
    if STATUS.get("mqtt") != "connected" or now()-state.get("t",0)>120 or state.get("t",0)<STATUS.get("since",0):
        return "Waiting for live printer status"
    ps=state.get("print_state")
    stage=state.get("stage_current")
    stage_fresh=now()-state.get("stage_current_t",0)<=120 and state.get("stage_current_t",0)>=STATUS.get("since",0)
    if stage_fresh and stage in PRINTER_STAGES and (stage!=0 or ps=="RUNNING"):
        return PRINTER_STAGES[stage]
    ams_fresh=now()-state.get("ams_status_t",0)<=120 and state.get("ams_status_t",0)>=STATUS.get("since",0)
    busy=ams_fresh and state.get("ams_status",0)!=0
    op=jload(DRY_UNLOAD,{})
    if op.get("status")=="pending":
        if busy:
            heating=any(n.get("target") is not None and n.get("temp") is not None and n["target"]>n["temp"]+5 for n in state.get("env",{}).get("nozzles",[]))
            return "Heating the nozzle for unload" if heating else "Unloading filament before drying"
        return "Waiting for the printer to confirm filament retraction"
    if busy:return "Printer is moving or checking filament"
    if ps=="PAUSE":return "Print paused"
    if ps in ("PREPARE","SLICING"):return "Preparing the print"
    if ps=="RUNNING":return "Printing"+(f" · {state['print_pct']}% complete" if state.get("print_pct") is not None else "")
    if unresolved_print_fault(state):return "Printer stopped — check the alert below"
    active=[("AMS "+str(int(aid)+1) if int(aid)<128 else "AMS HT "+str(int(aid)-127)) for aid,a in state.get("ams",{}).items() if ams_is_drying(a)]
    if active:return "Drying filament · "+", ".join(active)
    env=state.get("env",{})
    warming=[name for key,name in (("bed","bed"),("chamber","chamber"),("nozzle","nozzle")) if env.get(key) is not None and env.get(key+"_target") is not None and env[key+"_target"]>env[key]+3]
    if warming:return "Preheating "+" and ".join(warming)
    return "Idle · ready for the next job"


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


import math


# These are initial engineering priors, not measured ratings or calibration.
# Heat balance: electrical input ~= heat loss + thermal capacity * dT/dt.
_POWER_SAMPLES = {}
_POWER_ESTIMATE_LOCK = _th.RLock()


def _power_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _power_slope(key, stamp, temperature, window):
    if not _power_number(stamp) or not _power_number(temperature): return None
    with _POWER_ESTIMATE_LOCK:
        samples = _POWER_SAMPLES.setdefault(key, [])
        if samples and (stamp < samples[-1][0] or stamp-samples[-1][0] > 45): samples.clear()
        if not samples or stamp-samples[-1][0] >= 4:
            samples.append((stamp, temperature))
        samples[:] = [(t,v) for t,v in samples if stamp-t <= window][-40:]
        if len(samples)<4 or samples[-1][0]-samples[0][0]<20: return None
        origin=samples[0][0]
        xs=[t-origin for t,v in samples]; ys=[v for t,v in samples]
        mx=sum(xs)/len(xs); my=sum(ys)/len(ys)
        denom=sum((x-mx)**2 for x in xs)
        return sum((x-mx)*(y-my) for x,y in zip(xs,ys))/denom if denom else None


def estimated_power(state):
    """Approximate operating average for H2 printers + attached AMS supplies.
    Uses temperature trends; no heater-duty/current sensor is available. Constants
    are uncalibrated priors. Never use this estimate for scheduling or power limits.
    """
    cfg=jload(CONFIG,{}) or {};name=str(cfg.get("name","")).upper()
    if not any(model in name for model in ("H2D","H2S")):
        return dict(available=False,reason="No power profile")
    env=state.get("env") or {};stamp=state.get("t",0);et=env.get("t",0)
    if STATUS.get("mqtt")!="connected" or now()-stamp>120 or now()-et>120:
        return dict(available=False,reason="Waiting for fresh telemetry")
    # Optional meter calibration can adjust the model without changing UI.
    calibration=(settings().get("power_estimate") or {})
    ambient=calibration.get("ambient_c",25)
    if not _power_number(ambient) or not 5<=ambient<=45: ambient=25
    chamber=env.get("chamber")
    surrounding=max(ambient,min(70,chamber)) if _power_number(chamber) else ambient
    parts=[];trends=0;missing=False
    def add(label,value):parts.append(dict(label=label,watts=round(max(0,value),1)))
    def heater(key,label,current,target,capacity,loss,cap,air,fallback,threshold,window=45):
        nonlocal trends,missing
        slope=_power_slope(BASE+":"+key,et if not key.startswith("ams") else stamp,current,window)
        if not _power_number(target) or target<0:
            missing=True;return
        if target==0:
            add(label,0);return
        if not _power_number(current): missing=True;return
        if current>target+threshold:
            add(label,0);return
        steady=loss*max(0,current-air)
        if slope is not None:
            # Least-squares trend over several readings damps thermostat cycling
            # and sensor rounding. Cooling subtracts stored heat from the estimate.
            watts=steady+capacity*slope;trends+=1
        else:
            watts=fallback if target-current>threshold else steady
        add(label,min(cap,max(0,watts)))
    printing=state.get("print_state")=="RUNNING"
    preparing=state.get("print_state") in ("PREPARE","SLICING")
    moving=printing or preparing or bool((state.get("ams_status") or 0)&255)
    lights=sum(1 for value in (state.get("lights") or {}).values() if value in ("on","flashing"))
    fans=state.get("fans") or {}
    fan_w=sum(max(0,min(255,float(fans.get(key) or 0)))/255*maximum for key,maximum in (("part",5),("aux",12),("exhaust",8)))
    add("Electronics, lights, fans & motion",18+min(lights,3)*3+fan_w+(18 if moving else 0))
    heater("bed","Bed",env.get("bed"),env.get("bed_target"),1800,2.5,1850,ambient*.4+surrounding*.6,1400,5)
    heater("chamber","Chamber",chamber,env.get("chamber_target"),5000,5,450,ambient,300,4,90)
    nozzles=env.get("nozzles") or [dict(id=0,temp=env.get("nozzle"),target=env.get("nozzle_target"))]
    for index,nozzle in enumerate(nozzles):
        heater("nozzle"+str(nozzle.get("id",index)),"Nozzle "+str(index+1),nozzle.get("temp"),nozzle.get("target"),15,.16,110,surrounding,90,15)
    stg=settings()
    for aid,unit in state.get("ams",{}).items():
        label="AMS "+str(int(aid)+1) if int(aid)<128 else "AMS HT "+str(int(aid)-127)
        if not ams_is_drying(unit):
            add(label,5 if unit.get("dry_status") in (3,4) else 2);continue
        # Firmware sub-status 0 is heater off, even within an active drying cycle.
        sub=ams_info_bits(unit.get("info"),22,2)
        if sub==0:
            add(label,5);continue
        ht=int(aid)>=128;limited=printing and not has_psu(stg,aid)
        cap=200 if ht else (70 if limited else 90)
        heater("ams"+aid,label,unit.get("temp"),(unit.get("dry_setting") or {}).get("dry_temperature"),
               1000 if ht else 900,2.2 if ht else 1.5,cap,ambient,cap*.85,4,120)
        add(label+" electronics & fan",3)
    if missing:return dict(available=False,reason="Waiting for complete heater readings")
    scale=calibration.get("scale",1)
    if not _power_number(scale) or not .25<=scale<=4:scale=1
    estimate=sum(p["watts"] for p in parts)*scale
    return dict(available=True,estimate_w=round(estimate),estimated=True,calibrated=scale!=1,
                model="thermal-balance-v2",trend_components=trends,parts=parts,
                note="Operating estimate from thermal trends and assumed heat losses; not a meter reading.")


def display_slot_spool(tray, spool):
    """Never present a remembered binding as the identity of a newly inserted spool.

    Explicit manual assignments remain authoritative while firmware catches up.
    This is presentation only; historical spool records are retained.
    """
    if not spool or spool.get("removed_at"):
        return {}
    assigned = spool.get("manual") or spool.get("from_printer")
    if tray.get("pending") and not assigned:
        return {}
    if tray.get("rfid") and spool.get("rfid") and tray["rfid"] != spool["rfid"]:
        return {}
    return spool


def rev_data():
    """Tiny payload polled every few seconds: a revision stamp plus what the tabs show."""
    st = jload(STATE, {}); sp = jload(SPOOLS, {}); ss = st.get("slot_spool", {}); env = st.get("env", {})
    slot_read_ok = (settings() or {}).get("slot_read_ok") or {}
    rfid_cycles = jload(RFID_HEAT_CYCLES, {}) or {}
    nozzle_source = active_nozzle_source(st)
    heating = any(a.get("heating") for a in st.get("ams", {}).values())
    pcts = [assess(sp[s2])["pct"] for s2 in ss.values() if s2 in sp and not sp[s2].get("provisional")]
    parts = [
        str(sorted(ss.items())), str(sorted((k, bool(a.get("heating")), int(rfid_dry_time(k, a.get("dry_time"), rfid_cycles) or 0) // 5,
                                             int(a.get("temp") or 0), a.get("humidity_raw"), a.get("humidity_idx"))
                                            for k, a in st.get("ams", {}).items())),
        current_printer_action(st), str(pullback_retry_data(st)), str(nozzle_source), st.get("print_state"), str(st.get("print_pct")), str(st.get("print_layer")),
        json.dumps(st.get("fans", {}), sort_keys=True), json.dumps(st.get("lights", {}), sort_keys=True),
        str(st.get("spd_lvl")), str(st.get("spd_mag")),
        json.dumps({k: env.get(k) for k in ("bed_target", "chamber_target", "nozzle_target", "nozzles")}, sort_keys=True),
        str(int(env.get("bed") or 0)), str(int(env.get("chamber") or 0)), str(int(env.get("nozzle") or 0)),
        str(sorted(p // 2 for p in pcts)), STATUS.get("mqtt"),
    ]
    try:
        parts.append(str(int(os.path.getmtime(SPOOLS)))); parts.append(str(int(os.path.getmtime(SETTINGS))))
        if os.path.exists(ACTFILE): parts.append(str(os.path.getmtime(ACTFILE)))
        if os.path.exists(COOLDOWN): parts.append(str(os.path.getmtime(COOLDOWN)))
    except Exception: pass
    err = error_data(st); parts.append(str(err))
    camera_warning = camera_warning_status(); parts.append(str(camera_warning))
    dry_faults = drying_faults(st); parts.append(str(dry_requests()))
    rev = hashlib.sha1("|".join(map(str, parts)).encode()).hexdigest()[:12]
    room_units = []
    for aid, unit in st.get("ams", {}).items():
        rows = []
        for tid, tray in sorted(unit.get("trays", {}).items()):
            key = slot_key(aid, tid)
            spool = display_slot_spool(tray, sp.get(ss.get(key), {}))
            known = bool(spool and not spool.get("provisional") and spool.get("type"))
            occupied = bool(spool or tray.get("present") or not tray.get("empty", True))
            setup = occupied and not known
            rfid_pending = bool(setup and slot_read_ok.get(key) and ams_is_drying(unit) and not tray.get("rfid"))
            tag = bool(tray.get("rfid")) and known and not (tray.get("state") == 9 and tray.get("read_done"))
            remaining = tray.get("remain")
            remaining = remaining if tag and isinstance(remaining,(int,float)) and not isinstance(remaining,bool) and 0<=remaining<=100 else None
            rows.append(dict(key=key,slot=int(tid)+1,type=spool.get("type", "") if known else "",empty=not occupied,
                setup_required=setup,rfid_pending=rfid_pending,pending=bool(tray.get("pending")) and not known,reading=bool(tray.get("reading")),
                color=str((spool.get("color") if spool.get("manual") else tray.get("color")) or tray.get("color") or spool.get("color") or "").lstrip("#")[:6] if known else "",
                pct=assess(spool)["pct"] if known else None,rfid=tag,remaining=remaining,
                loaded=nozzle_source.get("key")==key))
        label = ("HT-" if int(aid) >= 128 else "AMS-") + str(int(aid)-128 if int(aid)>=128 else int(aid))
        fault = next((f.get("text") for f in dry_faults if str(f.get("ams_id")) == str(aid)), None)
        room_units.append(dict(id=int(aid),name=label,spools=rows,min_pct=min((r["pct"] for r in rows if r["pct"] is not None),default=None),heating=ams_is_drying(unit),temp=unit.get("temp"),dry_time=rfid_dry_time(aid, unit.get("dry_time"), rfid_cycles),humidity_raw=unit.get("humidity_raw"),humidity_idx=unit.get("humidity_idx"),
            dry_cycle=unit.get("dry_cycle"),dry_cycles=unit.get("dry_cycles",[]),blocked=fault,
            fleet_requested=dry_requests().get(aid,{}).get("fleet_requested",False),request_status=dry_requests().get(aid,{}).get("status"),request_since=dry_requests().get(aid,{}).get("since"),
            request_message=dry_requests().get(aid,{}).get("message"),request_wait=drying_wait_details(aid,st,settings(),dry_requests().get(aid,{})),setup_slots=missing_drying_slots(aid,unit,sp,ss)))
    identities = [{k: row.get(k) for k in ("key", "type", "empty", "setup_required", "pending", "reading", "color", "rfid", "remaining", "loaded")}
                  for unit in room_units for row in unit["spools"]]
    rev = hashlib.sha1((rev + json.dumps(identities, sort_keys=True)).encode()).hexdigest()[:12]
    return dict(current_action=current_printer_action(st), estimated_power=estimated_power(st), nozzle_source=nozzle_source, tray_now=st.get("tray_now"), room_units=room_units, print_name=st.get("print_name"), rev=rev, pullback_retry=pullback_retry_data(st), errors=err, process_faults=dry_faults, has_error=bool(err["print_error"] or err["hms"] or dry_faults), camera_warning=camera_warning, print_state=st.get("print_state"), print_pct=st.get("print_pct"),
                print_left_min=st.get("print_left_min"), heating=heating,
                needs=sum(1 for p in pcts if p < 60), spools=len(pcts),
                mqtt=STATUS.get("mqtt"), error=STATUS.get("error"),
                stale=(now() - st.get("t", 0)) > 900)


def tag_read_note(a, state):
    if state.get("print_state") not in ("IDLE", "FINISH"):
        return "Tag re-check deferred until the printer is idle."
    if ams_is_drying(a):
        return "Tag re-check deferred until drying finishes."
    if state.get("tray_now") not in (None, 255, "255", -1, "-1"):
        return "Tag re-check deferred until filament is unloaded."
    if a.get("temp") is not None and a["temp"] >= 45:
        return "Tag re-check deferred until the AMS cools below 45 °C."
    return "Tag re-check queued; waiting for the AMS to be available."


def report_data():
    spools = jload(SPOOLS, {}); state = jload(STATE, {})
    nozzle_source = active_nozzle_source(state)
    _stg = settings() or {}
    _stg_cool = _stg.get("cool_read") or {}
    _slot_read_ok = _stg.get("slot_read_ok") or {}
    ams = state.get("ams", {}); slot_spool = state.get("slot_spool", {})
    rfid_cycles = jload(RFID_HEAT_CYCLES, {}) or {}
    rows = []
    for ams_id in sorted(ams, key=int):
        a = ams[ams_id]
        for tray_id in sorted(a["trays"]):
            key = slot_key(ams_id, tray_id); sid = slot_spool.get(key)
            tr = a["trays"][tray_id]
            row = dict(key=key, ams=ams_label(ams_id), ams_id=int(ams_id), dry_time=rfid_dry_time(ams_id, a.get("dry_time"), rfid_cycles), slot=int(tray_id) + 1,
                       setpoint=((a.get("dry_setting") or {}).get("dry_temperature") or None),
                       present=bool(tr.get("present")) and tr.get("empty", True), pending=bool(tr.get("pending")), reading=bool(tr.get("reading")), remembered=tr.get("remembered_type"),
                       no_tag=(tr.get("state") == 9 and tr.get("read_done")), rfid=bool(tr.get("rfid")) and not (tr.get("state") == 9 and tr.get("read_done")),
                       tag_retry_queued=bool(_slot_read_ok.get(key) and not tr.get("rfid") and sid and spools.get(sid, {}).get("provisional")),
                       cool_read=bool((_stg_cool or {}).get(f"{ams_id}:{tray_id}")),
                       tag_read_note=tag_read_note(a, state),
                       remain=tr.get("remain") if tr.get("state") != 9 and isinstance(tr.get("remain"), (int, float)) and tr.get("remain") >= 0 else None,
                       color=((spools.get(sid, {}).get("color") if spools.get(sid, {}).get("manual") else tr.get("color")) or tr.get("color") or "")[:6], ams_temp=a["temp"], humidity=a["humidity_idx"],
                       humidity_raw=a.get("humidity_raw"), heating=a.get("heating"), spool=None,
                       loaded=nozzle_source.get("key")==key)
            s = display_slot_spool(tr, spools.get(sid, {}))
            if s:
                row["spool"] = dict(id=sid, name=s.get("name"), type=s["type"], color=s.get("color"), needs_name=not s.get("name"), manual=bool(s.get("manual")), provisional=bool(s.get("provisional")), from_printer=bool(s.get("from_printer")), hint=s.get("hint_type"), **assess(s, a.get("temp"), a.get("heating")))
            rows.append(row)
    st = settings(); units = {}
    for ams_id, a in ams.items():
        p = plan_unit(ams_id, a, spools, slot_spool) or {}
        _f = st.get("dry_fail", {}).get(ams_id)
        if _f and a.get("heating"): _f = None          # it is drying now — stale refusal
        _request = dry_requests().get(ams_id, {})
        _q = _request if _request.get("status") in ("preparing", "unloading", "pending", "waiting") else None
        if _q: _q = dict(_q, wait_detail=drying_wait_details(ams_id,state,st,_q))
        _blocked = _request if _request.get("status") == "blocked" and not a.get("heating") else None
        if _q and a.get("heating"): _q = None
        units[ams_label(ams_id)] = dict(auto=bool(st["auto"].get(ams_id)), psu=has_psu(st, ams_id), psu_warn=st.get("psu_warn", {}).get(ams_id),
                                        dry_fail=(_f if _f and now() - _f.get("t", 0) < 900 else None), dry_pending=_q, dry_blocked=_blocked, dry_cancelled=_request.get("status") in ("cancelled", "retry_ready"), dry_retry_ready=_request.get("status") == "retry_ready", hold=st["manual_hold_until"].get(ams_id, 0) > now(), **p)
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
    known = [dict(id=s["id"], name=s["name"], type=s["type"]) for s in spools.values() if s.get("name")]
    movable = [dict(id=s["id"], name=s.get("name") or s["type"], type=s["type"], where=("removed" if s.get("removed_at") else s.get("slot")),
                    pct=assess(s)["pct"]) for s in spools.values() if s.get("name") or s.get("manual") or s.get("removed_at")]
    try: ev = [json.loads(l) for l in open(EVENTS).read().splitlines()[-40:]][::-1]
    except FileNotFoundError: ev = []
    return dict(unrestricted_jog=bool(jload(CONFIG, {}).get("allow_unrestricted_jog")), current_action=current_printer_action(state), estimated_power=estimated_power(state), nozzle_source=nozzle_source, generated=iso(), last_update=iso(state.get("t")) if state.get("t") else None,
                print_state=state.get("print_state"), print_left_min=state.get("print_left_min"),
                pullback_retry=pullback_retry_data(state), errors=error_data(state), process_faults=drying_faults(state), spd_lvl=state.get("spd_lvl"), spd_mag=state.get("spd_mag"),
                wedge_note=settings().get("wedge_note"), print_pct=state.get("print_pct"), print_layer=state.get("print_layer"), print_layers=state.get("print_layers"), print_name=state.get("print_name"),
                activity=jload(ACTFILE, None), asked_env=(settings().get("asked_env") if state.get("print_state") in ("IDLE","FINISH") else None), env=state.get("env", {}), fans=state.get("fans", {}), fan_off_at=jload(COOLDOWN, {}).get("fan_off_at"), lights=state.get("lights", {}), preheat=(st.get("preheat") if st.get("preheat") is not None else DEFAULT_PREHEAT), auto_cooldown=False,
                stale=(now() - state.get("t", 0)) > 900, mqtt=STATUS, slots=rows, known=known, used=used, palette=[dict(hex=h, name=n) for h, n in PALETTE], movable=movable, candidates=move_candidates(), presets=DRY_PRESETS, units=units, materials=list(MATERIALS),
                unnamed=[dict(id=s["id"], type=s["type"], slot=s["slot"]) for s in unknown], events=ev)


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


# Bambu filament ids used to interpret Orca's saved slot types.
FILAMENT_IDS = {
    "PETG-CF":  ("GFG50", 240, 270), "PETG":    ("GFG99", 230, 260),
    "PLA":      ("GFA00", 190, 230), "PLA-CF":  ("GFA50", 200, 240),
    "TPU-AMS":  ("GFU02", 220, 240), "TPU":     ("GFU99", 200, 240),
    "PAHT":     ("GFN04", 260, 300), "PA-CF":   ("GFN98", 260, 300),
    "PA":       ("GFN99", 260, 300), "PA6":     ("GFN99", 260, 300),
    # Use the existing generic PC printer profile; keep PC last for reverse ID lookup.
    "PC-CF":    ("GFC99", 260, 300),
    "PC":       ("GFC99", 260, 300), "ASA":     ("GFB98", 240, 270),
    "ABS":      ("GFB99", 240, 270), "PVA":     ("GFS99", 190, 230),
}


ORCA_CONF = os.environ.get("AMS_ORCA_CONF", os.path.expanduser("~/Documents/OrcaSlicer/OrcaSlicer.conf"))
ID_TO_TYPE = {}


def orca_slot_type(ams_id, tray_id):
    """What Orca has recorded for this AMS slot (its Device tab assignment), if anything."""
    global ID_TO_TYPE
    if not ID_TO_TYPE:
        ID_TO_TYPE = {v[0]: k for k, v in FILAMENT_IDS.items() if v[0]}
    try:
        cfg = jload(CONFIG, {}); serial = cfg.get("serial")
        conf = jload(ORCA_CONF, {}) or {}
        ids = (conf.get("ams_filament_ids") or {}).get(serial, "")
        parts = ids.split(",")
        i = int(ams_id) * 4 + int(tray_id)
        code = parts[i].strip() if i < len(parts) else ""
        return ID_TO_TYPE.get(code) if code else None
    except Exception:
        return None


PALETTE = [("FFFFFF", "White"), ("000000", "Black"), ("7A7A7A", "Grey"), ("C12E1F", "Red"),
           ("1F5FC1", "Blue"), ("2E8B57", "Green"), ("FFD60A", "Yellow"), ("FF7A00", "Orange"),
           ("8B5A2B", "Brown"), ("E8E0D0", "Natural"), ("9B59B6", "Purple"), ("00B3B3", "Teal")]


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


def do_dried(sid, hours, temp):
    sp = jload(SPOOLS, {})
    if sid not in sp: return "unknown spool id"
    s = sp[sid]; rule = mat_rule(s["type"])
    factor = 1.0 if temp >= rule["temp"] - TEMP_TOLERANCE else 0.5
    s["dry_credit_h"] = min(s["dry_credit_h"] + hours * factor, rule["hours"] * 1.5)
    s["last_dry_end"] = now(); s["last_dry_temp"] = temp; s["exposure_h"] = 0.0
    jsave(SPOOLS, sp)
    append(EVENTS, dict(t=now(), time=iso(), kind="DRY_MANUAL", spool=sid, hours=hours, temp=temp,
                        msg=f"{s.get('name') or sid}: external dryer {hours} h @ {temp}°C")); return "ok"


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


# ---------------------------------------------------------------------------
# MQTT daemon
# ---------------------------------------------------------------------------
def mqtt_loop():
    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        STATUS.update(mqtt="error", error="paho-mqtt not installed — run Install.command again"); return
    cfg = jload(CONFIG, None)
    if not cfg or not all(cfg.get(k) for k in ("ip", "serial", "access_code")):
        STATUS.update(mqtt="error", error="config.json missing ip / serial / access_code"); return
    tr = Tracker()
    topic_rep = f"device/{cfg['serial']}/report"; topic_req = f"device/{cfg['serial']}/request"

    def on_connect(c, u, flags, rc, props=None):
        ok = (rc == 0) if isinstance(rc, int) else (getattr(rc, "value", 1) == 0)
        STATUS.update(mqtt="connected" if ok else "auth failed", since=now(), error=None if ok else f"rc={rc} — check access code")
        print(f"[{iso()}] connected rc={rc}", flush=True)
        c.subscribe(topic_rep)
        c.publish(topic_req, json.dumps({"pushing": {"sequence_id": "0", "command": "pushall"}}))
        if ok and not STANDBY:
            # Let the AMS read tags as soon as a spool is inserted. The bounded
            # dashboard retry below is only for an insertion scan that was missed.
            do_ams_read_options(True)

    def on_message(c, u, msg):
        try: rep = json.loads(msg.payload)
        except Exception: return
        pr = rep.get("print", {}) if isinstance(rep, dict) else {}
        if isinstance(rep, dict) and len(pr) > 40:   # a full report: keep the latest for diagnostics
            try: jsave(P("last_report_raw.json"), rep)
            except Exception: pass
        if "stg_cur" in pr:
            with LOCK:
                tr.state["stage_current"] = int(pr["stg_cur"]); tr.state["stage_current_t"] = now()
        if "ams_status" in pr:
            with LOCK:
                tr.state["ams_status"] = int(pr["ams_status"]); tr.state["ams_status_t"] = now()
        if pr.get("command") == "ams_change_filament":
            dry_unload_reply(pr)
            rfid_unload_reply(pr)
        if pr.get("command") in ("ams_filament_drying", "auto_stop_ams_dry"):
            try: drying_reply(pr)
            except Exception as e: print(f"[{iso()}] drying reply handler: {e}", flush=True)
            append(EVENTS, dict(t=now(), time=iso(), kind="DRY_REPLY", slot=f"{pr.get('ams_id')}:*",
                                msg=f"printer reply to {pr.get('command')}: {pr.get('result')} {pr.get('reason') or ''} err={pr.get('err_code')}"
                                    + (f" — {dry_err_text(pr.get('err_code'))}" if dry_err_text(pr.get('err_code')) else "")))
            print(f"[{iso()}] drying reply {json.dumps(pr)}", flush=True)
        if "spd_lvl" in pr or "spd_mag" in pr:
            with LOCK:
                if "spd_lvl" in pr: tr.state["spd_lvl"] = fnum(pr.get("spd_lvl"))
                if "spd_mag" in pr: tr.state["spd_mag"] = fnum(pr.get("spd_mag"))
        if "lights_report" in pr:
            with LOCK:
                tr.state["lights"] = {x.get("node"): x.get("mode") for x in (pr.get("lights_report") or [])}
        if "hms" in pr or "print_error" in pr:
            with LOCK:
                if "hms" in pr: tr.state["hms"] = pr.get("hms") or []
                if "print_error" in pr:
                    tr.state["print_error"] = pr.get("print_error") or 0; tr.state["print_error_t"] = now()
        if "fan_gear" in pr:
            # Bambu Studio DevFan::ParseV1_0: packed bytes are native 0..255 speeds.
            gear = int(pr["fan_gear"])
            tr.state["fans"] = dict(part=gear & 255, aux=(gear >> 8) & 255, exhaust=(gear >> 16) & 255)
        elif any(k in pr for k in ("cooling_fan_speed", "big_fan1_speed", "big_fan2_speed")):
            f = tr.state.setdefault("fans", {})
            for k, dst in (("cooling_fan_speed", "part"), ("big_fan1_speed", "aux"), ("big_fan2_speed", "exhaust")):
                if k in pr:
                    value = fnum(pr[k])
                    if value is not None: f[dst] = max(0, min(255, math.floor(value / 1.5) * 25.5))
        if any(k in pr for k in ("bed_temper", "chamber_temper", "nozzle_temper")):
            env = tr.state.setdefault("env", {})
            for k, dst in (("bed_temper", "bed"), ("bed_target_temper", "bed_target"), ("chamber_temper", "chamber"),
                           ("chamber_target_temper", "chamber_target"), ("nozzle_temper", "nozzle")):
                if k in pr: env[dst] = fnum(pr[k])
            env["t"] = now()
        dev = pr.get("device") if isinstance(pr, dict) else None
        if isinstance(dev, dict):
            env = tr.state.setdefault("env", {})
            def unpack(v):
                try: v = int(v); return (v & 0xFFFF), (v >> 16) & 0xFFFF
                except Exception: return None, None
            if "extruder" in dev:
                with LOCK: update_nozzle_routing(tr.state, dev["extruder"])
            exs = ((dev.get("extruder") or {}).get("info") or [])
            noz = []
            for e in exs:
                v = e.get("temp")
                if v is None: continue
                try: v = int(v)
                except Exception: continue
                cur, tgt = v & 0xFFFF, (v >> 16) & 0xFFFF  # packed target 0 means heater off
                if cur > 500: continue                       # nonsense packing, skip
                noz.append(dict(id=e.get("id"), temp=cur, target=tgt,
                                active=bool(e.get("stat")) or bool(e.get("filam_bak"))))
            if noz:
                env["nozzles"] = noz
                hot = max(noz, key=lambda n: (n["active"], n["temp"]))
                env["nozzle"] = hot["temp"]
                if hot.get("target") is not None: env["nozzle_target"] = hot["target"]
            ctc = (dev.get("ctc") or {}).get("info", {}).get("temp")
            if ctc is not None:
                cur, tgt = unpack(ctc); env["chamber"], env["chamber_target"] = cur, tgt; env["chamber_on"] = (dev.get("ctc") or {}).get("state")
            if "bed_temp" in dev and env.get("bed") is None:
                cur, tgt = unpack(dev["bed_temp"]); env["bed"], env["bed_target"] = cur, tgt
            env["t"] = now()
        if pr.get("command") == "ams_change_filament":
            ok = pr.get("result") == "success"
            append(EVENTS, dict(t=now(), time=iso(), kind="FILAMENT", msg=f"printer reply to filament change: {pr.get('result')} {pr.get('reason') or ''} err={pr.get('errno', pr.get('err_code'))} soft_temp={pr.get('soft_temp')}"))
            if not ok:
                thermal = dry_unload_thermal_refusal(dict(error=pr.get("errno", pr.get("err_code")), soft_temp=pr.get("soft_temp")))
                if thermal and dry_unload_busy():
                    act_step(f"Waiting for the {thermal['sensor']} to cool below {thermal['limit']:g} °C before automatic unloading")
                else:
                    act_step("The printer refused the filament change", done=True, outcome="failed")
        if pr.get("command") in ("pause", "resume", "stop"):
            append(EVENTS, dict(t=now(), time=iso(), kind="PRINT_CTRL", msg=f"printer reply to {pr.get('command')}: {pr.get('result')} {pr.get('reason') or ''}"))
        if pr.get("command") == "ams_get_rfid":
            append(EVENTS, dict(t=now(), time=iso(), kind="RFID", msg=f"printer reply to ams_get_rfid: {pr.get('result')} {pr.get('reason') or ''} err={pr.get('err_code')}"))
        if pr.get("command") == "ams_user_setting":
            append(EVENTS, dict(t=now(), time=iso(), kind="RFID", msg=f"printer reply to AMS auto-read setting: {pr.get('result')} err={pr.get('err_code')}"))
        if pr.get("command") == "gcode_line":
            append(EVENTS, dict(t=now(), time=iso(), kind="GCODE_REPLY", msg=f"printer reply to gcode: {pr.get('result')} {pr.get('reason') or ''} err={pr.get('err_code')}"))
        if "gcode_state" in pr:
            STATUS["print_state"] = pr.get("gcode_state"); STATUS["print_pct"] = pr.get("mc_percent"); STATUS["print_left_min"] = pr.get("mc_remaining_time")
            with LOCK:
                tr.state["print_state"] = pr.get("gcode_state"); tr.state["print_state_t"] = now(); tr.state["print_left_min"] = pr.get("mc_remaining_time")
                if pr.get("home_flag") is not None:
                    try: tr.state["home_flag"] = int(pr.get("home_flag"))
                    except Exception: pass
                tr.state.setdefault("seen_since", now())
                tr.state["print_pct"] = pr.get("mc_percent"); tr.state["print_layer"] = pr.get("layer_num"); tr.state["print_layers"] = pr.get("total_layer_num")
                if pr.get("subtask_name"): tr.state["print_name"] = pr.get("subtask_name")
        if "gcode_state" in pr:
            with LOCK:
                # The API may have rebound a spool since the last MQTT report.
                # Merge that edit before writing, even if this report has no AMS.
                tr._reload_if_changed()
                jsave(STATE, tr.state)
        ams, tray_now = parse_ams(rep)
        if ams is not None:
            with LOCK:
                try: tr.update(ams, tray_now)
                except Exception as e: print(f"[{iso()}] update error {e}", flush=True)

    def on_disconnect(c, u, *a):
        STATUS.update(mqtt="disconnected", since=now())
        print(f"[{iso()}] disconnected, retrying", flush=True)

    try: client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"amstracker-{uuid.uuid4().hex[:6]}")
    except Exception: client = mqtt.Client(client_id=f"amstracker-{uuid.uuid4().hex[:6]}")
    if STANDBY:
        original_publish = client.publish
        def telemetry_only_publish(topic, payload=None, *args, **kwargs):
            try:
                obj = json.loads(payload)
            except (TypeError, ValueError):
                raise RuntimeError("Standby host blocked a non-telemetry command")
            if not (isinstance(obj, dict) and set(obj) == {"pushing"}
                    and obj["pushing"].get("command") == "pushall"):
                raise RuntimeError("Standby host blocked a printer control command")
            return original_publish(topic, payload, *args, **kwargs)
        client.publish = telemetry_only_publish
    MQTT.update(client=client, topic_req=topic_req)
    client.username_pw_set("bblp", cfg["access_code"])
    client.tls_set(cert_reqs=ssl.CERT_NONE); client.tls_insecure_set(True)
    client.on_connect = on_connect; client.on_message = on_message; client.on_disconnect = on_disconnect
    client.reconnect_delay_set(5, 60)
    # periodic pushall so accrual keeps ticking even if nothing changes
    def loop():
        STATUS.update(mqtt="connecting", error=None)
        client.connect(cfg["ip"], 8883, keepalive=30)
        client.loop_start()
        while True:
            time.sleep(300)
            client.publish(topic_req, json.dumps({"pushing": {"sequence_id": "0", "command": "pushall"}}))
    while True:
        try: loop()
        except SystemExit: raise
        except Exception as e:
            STATUS.update(mqtt="error", error=f"{e} — is the printer on and at {cfg['ip']}?")
            print(f"[{iso()}] error {e}; reconnect in 15s", flush=True); time.sleep(15)


# ---------------------------------------------------------------------------
# Web dashboard
# ---------------------------------------------------------------------------
PORT = 8765

FOCUS_DASHBOARD_SCRIPT = 'on run argv\n    -- Alerts never launch Chrome, create tabs/windows, or navigate a tab.\n    if application "Google Chrome" is not running then return "not-running"\n    set localhostURL to item 1 of argv\n    set loopbackURL to item 2 of argv\n    set ipv6URL to item 3 of argv\n    tell application "Google Chrome"\n        repeat with dashboardWindow in windows\n            set tabNumber to 0\n            repeat with dashboardTab in tabs of dashboardWindow\n                set tabNumber to tabNumber + 1\n                set tabURL to URL of dashboardTab\n                set tabTitle to title of dashboardTab\n                -- The title also recognizes the same dashboard opened via dry.local or a LAN IP.\n                if tabURL starts with localhostURL or tabURL starts with loopbackURL or tabURL starts with ipv6URL or tabTitle is "AMS Dryness" or tabTitle ends with " · AMS Dryness" then\n                    set active tab index of dashboardWindow to tabNumber\n                    set minimized of dashboardWindow to false\n                    set index of dashboardWindow to 1\n                    activate\n                    return "reused"\n                end if\n            end repeat\n        end repeat\n        return "not-found"\n    end tell\nend run\n'


class StoreCatalog:
    """Optional exact-match stock availability from a configured store."""
    # Optional stock catalog comes from the local, ignored config.json.
    # Keep product IDs and preferred shops out of the portable source tree.
    PRODUCTS = {}
    BASE_URL = ""

    def __init__(self):
        self.lock = _th.RLock()
        self.results = {}
        config = jload(CONFIG, {})
        self.BASE_URL = str(config.get("stock_store_url") or "").rstrip("/")
        for item in config.get("stock_catalog", []):
            try:
                key = (str(item["material"]), str(item["color"]).upper())
                self.PRODUCTS[key] = (int(item["product_id"]), str(item["sku"]), str(item["expected_name"]))
            except (KeyError, TypeError, ValueError):
                continue

    def refresh(self):
        import urllib.request
        updates = {}
        for key, (product_id, sku, expected_name) in self.PRODUCTS.items():
            try:
                url = f'{self.BASE_URL}/wp-json/wc/store/v1/products/{product_id}'
                request = urllib.request.Request(url, headers={'User-Agent': 'AMS-Filament-Stock/1.0'})
                with urllib.request.urlopen(request, timeout=8) as response:
                    product = json.load(response)
                title = str(product.get('name') or '').split('(')[0]
                exact = (product.get('id') == product_id and product.get('sku') == sku
                         and expected_name.casefold() in title.casefold()
                        )
                status = ('different_product' if not exact else
                          'available' if product.get('is_in_stock') and product.get('is_purchasable')
                          and product.get('type') == 'simple' and not product.get('has_options') else 'out_of_stock')
                updates[key] = dict(status=status, product_url=product.get('permalink'), checked_at=time.time(),
                    cart_url=f'{self.BASE_URL}/cart/?add-to-cart={product_id}' if status == 'available' else None)
            except Exception as exc:
                updates[key] = dict(status='unknown', product_url=None, checked_at=time.time(), cart_url=None)
                print(f'[{iso()}] Stock check failed for {product_id}: {exc}', flush=True)
        with self.lock:
            self.results.update(updates)

    def run(self):
        while True:
            if self.BASE_URL and self.PRODUCTS: self.refresh()
            time.sleep(900)

    def for_item(self, item):
        key = (item.get('sub'), item.get('color'))
        if key not in self.PRODUCTS:
            return dict(status='not_listed', product_url=None, cart_url=None)
        with self.lock:
            return dict(self.results.get(key, dict(status='checking', product_url=None, cart_url=None)))


class FilamentInventory:
    """Fleet-wide unopened stock. A newly seen RFID tag opens one stock spool once."""
    def __init__(self, base):
        import threading
        self.base = base
        self.catalog = None
        self.path = os.path.join(base, 'filament_stock.json')
        self.lock = threading.RLock()
        saved = jload(self.path, {})
        self.items = saved.get('items', {}) if isinstance(saved.get('items'), dict) else {}
        self.tags = saved.get('tags', {}) if isinstance(saved.get('tags'), dict) else {}
        changed = False
        if not saved.get('history_seeded'):
            changed = self._seed_history() or True
        for item in self.items.values():
            if 'tracking_since' not in item:
                item['tracking_since'] = time.time(); changed = True
        if changed: self._save()

    def _save(self):
        jsave(self.path, dict(items=self.items, tags=self.tags, history_seeded=True))

    def _seed_history(self):
        """Old RFID spools must never be mistaken for new stock when reinserted."""
        changed = False
        root = os.path.join(self.base, 'printers')
        if not os.path.isdir(root): return False
        for printer in os.listdir(root):
            saved = jload(os.path.join(root, printer, 'spools.json'), {})
            if not isinstance(saved, dict): continue
            for spool in saved.values():
                if not isinstance(spool, dict): continue
                tag = str(spool.get('rfid') or '').upper()
                if not tag or not tag.strip('0') or tag in self.tags: continue
                self.tags[tag] = dict(id=tag, first_seen=0, item_id=None,
                    type=spool.get('type') or '', sub=spool.get('sub') or spool.get('name') or '',
                    color=str(spool.get('color') or '')[:6].upper(), code=spool.get('idx') or '')
                changed = True
        return changed

    def _products(self):
        colors = {'000000':'Black', 'C12E1F':'Red', 'FF6A13':'Orange',
                  '5898DD':'Blue', 'ADB1B2':'Light grey'}
        # ASA Black is in stock even though none of its RFID tags has been read yet.
        asa_key = hashlib.sha256(b'ASA|ASA|000000').hexdigest()[:16]
        products = {asa_key: dict(key=asa_key, material='ASA', sub='ASA', color='000000',
            code='', brand='Bambu Lab', name='Bambu ASA Black')}
        for tag in self.tags.values():
            material, sub = str(tag.get('type') or ''), str(tag.get('sub') or '')
            color = str(tag.get('color') or '')[:6].upper()
            if not material or not sub or len(color) != 6: continue
            key = hashlib.sha256(f'{material}|{sub}|{color}'.encode()).hexdigest()[:16]
            product = products.setdefault(key, dict(key=key, material=material, sub=sub,
                color=color, code='', brand='Bambu Lab',
                name=f'Bambu {sub} {colors.get(color, "#"+color)}'))
            if tag.get('code'): product['code'] = tag['code']
        # Inventory counts each material/subtype once, regardless of color.
        # Preserve an existing row's representative key and purchase variant.
        pooled = {}
        for product in products.values():
            group = (product['material'], product['sub'])
            existing = next((item for item in self.items.values()
                             if (item.get('material'), item.get('sub')) == group), None)
            if group not in pooled or (existing and product['key'] == existing.get('product_key')):
                pooled[group] = dict(product, name=f"Bambu {product['sub']}")
        return sorted(pooled.values(), key=lambda x: x['name'].casefold())

    def _matching_item(self, read):
        matches = []
        for item_id, item in self.items.items():
            # Stock is pooled by filament type. A newly added printer can have
            # the same material in several colors without needing one stock row
            # for each color. RFID identity still prevents double deductions.
            if item.get('material') == read['type'] and item.get('sub') == read['sub']:
                matches.append(item_id)
        return matches[0] if len(matches) == 1 else None

    def scan(self, workers):
        stamp = time.time()
        seen = {}
        runouts = set()
        for printer, worker in list(workers.items()):
            state = jload(os.path.join(worker['dir'], 'state.json'), {})
            if stamp - state.get('t', 0) > 300: continue
            if state.get('print_state') in ('PAUSE', 'FAILED'):
                try: filament_runout = int(state.get('print_error') or 0) == 0x07008011
                except (TypeError, ValueError): filament_runout = False
                if filament_runout:
                    source = active_nozzle_source(state)
                    if source.get('key'):
                        aid, tid = source['key'].split(':')
                        runout_tag = str(state.get('ams', {}).get(aid, {}).get('trays', {}).get(tid, {}).get('rfid') or '').upper()
                        if runout_tag and runout_tag.strip('0'): runouts.add(runout_tag)
            for aid, unit in state.get('ams', {}).items():
                for tid, tray in unit.get('trays', {}).items():
                    tag = str(tray.get('rfid') or '').upper()
                    if not tag or not tag.strip('0') or tray.get('empty') or tray.get('pending') or (tray.get('state') == 9 and tray.get('read_done')): continue
                    remain = fnum(tray.get('remain'))
                    if remain is None or not 0 <= remain <= 100: remain = None
                    read = dict(printer=printer, ams=ams_label(aid), slot=int(tid)+1,
                                type=tray.get('type') or '', sub=tray.get('sub') or '',
                                color=str(tray.get('color') or '')[:6].upper(), code=tray.get('idx') or '',
                                remaining_pct=remain, seen_at=state.get('t', stamp))
                    if tag not in seen or read['seen_at'] > seen[tag]['seen_at']: seen[tag] = read
        with self.lock:
            changed = False
            for tag, read in seen.items():
                record = self.tags.get(tag)
                if record is None:
                    record = dict(id=tag, first_seen=stamp, initial_remaining_pct=read['remaining_pct'], min_remaining_pct=read['remaining_pct'], item_id=None)
                    self.tags[tag] = record; changed = True
                old_min = record.get('min_remaining_pct')
                new_pct = read['remaining_pct']
                if new_pct is not None and (old_min is None or new_pct < old_min):
                    record['min_remaining_pct'] = new_pct; changed = True
                for key, value in read.items():
                    if key == 'seen_at' and value - record.get(key, 0) < 30: continue
                    if record.get(key) != value: record[key] = value; changed = True
                item_id = self._matching_item(read)
                if item_id and record.get('item_id') != item_id:
                    record['item_id'] = item_id; changed = True
                if item_id and not record.get('counted_item_id') and record.get('first_seen', 0) > self.items[item_id].get('tracking_since', stamp):
                    item = self.items[item_id]
                    if item['unopened'] > 0:
                        item['unopened'] -= 1
                        item['opened_since_tracking'] = item.get('opened_since_tracking', 0) + 1
                    record['counted_item_id'] = item_id
                    record['counted_at'] = stamp
                    changed = True
            for tag in runouts:
                if tag in self.tags and not self.tags[tag].get('runout_at'):
                    self.tags[tag]['runout_at'] = stamp; changed = True
            if changed: self._save()

    def snapshot(self):
        with self.lock:
            items = [dict(x) for x in self.items.values()]
            for item in items:
                item['low'] = item.get('threshold') is not None and item.get('unopened', 0) <= item['threshold']
                if self.catalog:
                    item['spider'] = self.catalog.for_item(item)
                # The purchase listing may still be color-specific, but one
                # inventory row now counts every color of this filament type.
                item['purchase_variant'] = item.get('name', '').removeprefix(f"Bambu {item.get('sub', '')} ")
                item['name'] = f"Bambu {item.get('sub', item.get('material', 'Filament'))}"
            return dict(items=items, products=self._products(), low=[x['id'] for x in items if x['low']])

    def action(self, body, workers=None):
        action = body.get('action')
        if action == 'save' and not body.get('id') and workers:
            self.scan(workers)  # Take a baseline before starting a new stock count.
        with self.lock:
            if action == 'save':
                product = next((x for x in self._products() if x['key'] == body.get('product_key')), None)
                if not product: raise ValueError('Choose a Bambu filament from the list.')
                item_id = str(body.get('id') or '')
                if item_id and item_id not in self.items: raise ValueError('Unknown stock item.')
                if any(x.get('material') == product['material'] and x.get('sub') == product['sub'] and key != item_id for key, x in self.items.items()):
                    raise ValueError('That filament type already has a stock row. Edit its amount instead; all colors share one count.')
                count = int(body.get('unopened', 0))
                if not 0 <= count <= 100000: raise ValueError('Invalid spool count.')
                threshold_raw = body.get('threshold')
                threshold = None if threshold_raw in (None, '') else int(threshold_raw)
                if threshold is not None and not 0 <= threshold <= 100000: raise ValueError('Invalid alert threshold.')
                item_id = item_id or uuid.uuid4().hex[:12]
                existing = self.items.get(item_id, {})
                self.items[item_id] = dict(id=item_id, product_key=product['key'], **{k:product[k] for k in ('name','material','sub','brand','color','code')},
                    unopened=count, threshold=threshold, tracking_since=existing.get('tracking_since', time.time()),
                    opened_since_tracking=existing.get('opened_since_tracking', 0))
            elif action == 'remove':
                item_id = str(body.get('id') or '')
                if item_id not in self.items: raise ValueError('Unknown stock item.')
                del self.items[item_id]
                for tag in self.tags.values():
                    if tag.get('item_id') == item_id: tag['item_id'] = None
            elif action == 'assign':
                raise ValueError('Spools are matched automatically from their Bambu RFID product and color.')
            else: raise ValueError('Unknown stock action.')
            self._save()
            return self.snapshot()


class AttentionCenter:
    """Hub-owned incident tracking: one Mac alert regardless of open browser tabs."""
    def __init__(self, base, port, inventory=None):
        self.path = os.path.join(base, 'attention.json')
        saved = jload(self.path, {})
        self.lock = _th.RLock()
        self.port = port
        self.settings = dict(sound=True, bring_chrome=True)
        self.settings.update({k: v for k, v in saved.get('settings', {}).items() if k in self.settings and isinstance(v, bool)})
        self.legacy_diagnostics = saved.get('legacy_diagnostics', [])
        if saved.get('policy_version') == 2:
            self.active = saved.get('active', {})
            self.history = [i for i in saved.get('history', []) if i.get('code') != 'BACKUP_FAILED'][-40:]
            self.active = {k: i for k, i in self.active.items() if i.get('code') != 'BACKUP_FAILED'}
        else:
            self.legacy_diagnostics += list(saved.get('active', {}).values()) + saved.get('history', [])
            self.active, self.history = {}, []
        self.policy_dirty = saved.get('policy_version') != 2
        self.last_sound = 0
        self.last_focus = 0
        self.last_native_error = None
        self.connection_since = {}
        self.native = sys.platform == 'darwin'
        self.inventory = inventory

    def _save(self):
        jsave(self.path, dict(policy_version=2, settings=self.settings, active=self.active, history=self.history[-40:], legacy_diagnostics=self.legacy_diagnostics))

    def snapshot(self):
        with self.lock:
            return dict(incidents=list(self.active.values()), history=self.history[-20:][::-1],
                        settings=dict(self.settings), native=self.native, native_error=self.last_native_error,
                        reminder_seconds=30)

    def update(self, reports, stamp=None):
        stamp = time.time() if stamp is None else stamp
        fresh = []
        with self.lock:
            wanted = {}
            for printer, report in reports.items():
                connected = bool(report and report.get('mqtt') == 'connected' and not report.get('stale'))
                if not connected:
                    # Loss of telemetry cannot prove a process stopped. Retain only
                    # previously confirmed faults; never invent a connection alarm.
                    for key, item in self.active.items():
                        if item['printer'] == printer and item.get('blocking'): wanted[key] = item
                    continue
                errors = report.get('errors') or {}
                faults = ([errors['print_error']] if errors.get('print_error') else []) + (errors.get('hms') or [])
                faults = [f for f in faults if f.get('blocking') and (f.get('process') == 'filament' or report.get('print_state') in ('PAUSE', 'FAILED'))]
                faults += [f for f in report.get('process_faults', []) if f.get('blocking') and f.get('process') == 'drying']
                for fault in faults:
                    code = str(fault.get('code') or 'UNKNOWN')
                    url = str(fault.get('url') or '')
                    if not url.startswith('https://e.bambulab.com/'): url = ''
                    wanted[printer+'|'+code] = dict(printer=printer, code=code,
                        severity='error', message=str(fault.get('text') or 'The printer stopped because of a fault. Check its screen for recovery steps.'), url=url, blocking=True)
            if self.inventory:
                for item in self.inventory.snapshot()['items']:
                    if item['low']:
                        wanted['stock|'+item['id']] = dict(printer='Stock', code='LOW_STOCK_'+item['id'],
                            severity='warning', message=f"Order {item['name']}: {item['unopened']} spool(s) left, alert threshold {item['threshold']}.", url='', blocking=False)
                        spider_status = item.get('spider', {}).get('status')
                        if spider_status in ('out_of_stock', 'not_listed', 'different_product'):
                            reason = ('out of stock' if spider_status == 'out_of_stock' else
                                      'the listing has changed to a different product' if spider_status == 'different_product'
                                      else 'no exact listing was found')
                            wanted['spider|'+item['id']] = dict(printer='Stock', code='SPIDER_UNAVAILABLE_'+item['id'],
                                severity='warning', message=f"{store_name() or 'Store'}: {item['name']} is unavailable ({reason}). No substitute was added to the cart.",
                                url='', blocking=False)
            changed = self.policy_dirty
            self.policy_dirty = False
            for key in list(self.active):
                if key not in wanted:
                    item = self.active.pop(key)
                    self.history.append(dict(item, resolved_at=stamp))
                    self.history = self.history[-40:]
                    changed = True
            for key, data in wanted.items():
                if key not in self.active:
                    self.active[key] = dict(data, id=uuid.uuid4().hex, first_seen=stamp, acknowledged=False, snoozed_until=0)
                    fresh.append(self.active[key]['id']); changed = True
                else:
                    item = self.active[key]
                    for field in ('message', 'severity', 'url'):
                        if item.get(field) != data.get(field): item[field] = data.get(field); changed = True
            if changed: self._save()
            pending = [x for x in self.active.values() if not x['acknowledged'] and x.get('snoozed_until', 0) <= stamp]
            sound = bool(self.native and self.settings['sound'] and pending and stamp-self.last_sound >= 30)
            focus = bool(self.native and self.settings['bring_chrome'] and fresh)
            if sound: self.last_sound = stamp
            if focus: self.last_focus = stamp
        return dict(sound=sound, focus=focus)

    def action(self, body, local):
        if body.get('action') == 'test':
            if not local: raise ValueError('Test Mac alerts from this Mac.')
            self.emit(dict(sound=True, focus=True, preview=True))
            return self.snapshot()
        with self.lock:
            action = body.get('action')
            if action == 'settings':
                if not local: raise ValueError('Change Mac alert settings from this Mac.')
                for k in ('sound', 'bring_chrome'):
                    if k in body:
                        if not isinstance(body[k], bool): raise ValueError('Invalid setting')
                        self.settings[k] = body[k]
            elif action in ('acknowledge', 'snooze'):
                ids = body.get('ids')
                if not isinstance(ids, list) or not all(isinstance(i, str) for i in ids): raise ValueError('Invalid incident list')
                for item in list(self.active.values()) + (self.history if action == 'acknowledge' else []):
                    if item['id'] in ids:
                        if action == 'acknowledge': item['acknowledged'] = True
                        else: item['snoozed_until'] = time.time()+300
            else: raise ValueError('Unknown alert action')
            self._save()
        return self.snapshot()

    def emit(self, effects):
        if not self.native: return
        import subprocess
        failures = []
        commands = []
        if effects.get('sound'): commands.append(['/usr/bin/afplay', '/System/Library/Sounds/Glass.aiff'])
        if effects.get('focus'):
            base = f"http://localhost:{self.port}/"
            commands.append(['/usr/bin/osascript', '-e', FOCUS_DASHBOARD_SCRIPT,
                base, f"http://127.0.0.1:{self.port}/", f"http://[::1]:{self.port}/"])
        for command in commands:
            try: subprocess.run(command, timeout=8, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            except Exception:
                failures.append('Chrome focus failed. Allow this background service to control Chrome in macOS Privacy & Security → Automation.' if command[0].endswith('osascript') else 'The Mac notification sound could not be played.')
        if commands:
            self.last_native_error = 'Mac attention alert failed: '+'; '.join(failures) if failures else None
            if failures: print(f'[{iso()}] {self.last_native_error}', flush=True)

    def run(self, workers):
        import urllib.request
        while True:
            if self.inventory:
                try: self.inventory.scan(workers)
                except Exception as e: print(f'[{iso()}] inventory scan: {e}', flush=True)
            reports = {}
            for printer, worker in list(workers.items()):
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{worker['port']}/api/rev", timeout=2) as response:
                        reports[printer] = json.loads(response.read())
                except Exception: reports[printer] = None
            try: self.emit(self.update(reports))
            except Exception as e: print(f'[{iso()}] attention monitor: {e}', flush=True)
            time.sleep(3)


HTML = r"""<!doctype html><html><head><meta charset="utf-8"><title>AMS Dryness</title>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<style>
:root{--bg:#111;--card:#1c1c1e;--fg:#eee;--mute:#999;--ok:#30d158;--warn:#ffd60a;--bad:#ff453a;--blue:#0a84ff}
body{margin:0;background:var(--bg);color:var(--fg);font:15px -apple-system,Helvetica,Arial,sans-serif}
header{display:flex;justify-content:space-between;align-items:center;padding:14px 20px;border-bottom:1px solid #333}
h1{font-size:18px;margin:0}
.hdr{display:flex;gap:14px;align-items:center}
.conn{display:flex;align-items:center;gap:7px;font-size:13px;font-weight:600;padding:5px 12px;border-radius:999px;background:#2c2c2e;color:#bbb}
.conn i{width:9px;height:9px;border-radius:50%;background:#777}
.conn.ok{background:#173a22;color:#7ee2a0}.conn.ok i{background:var(--ok);box-shadow:0 0 8px var(--ok)}
.conn.bad{background:#3a1d1d;color:#ffb3ad}.conn.bad i{background:var(--bad)}
.upd{font-size:13px;color:var(--mute)}
#refresh{font-size:13px;padding:7px 14px}#refresh.busy{opacity:.6}
#dryall{font-size:13px;padding:7px 14px;background:#3a2a10;color:#ffb340;font-weight:600}#dryall:hover{background:#5a3a00}#dryall:disabled{opacity:.5}
#stopall{font-size:13px;padding:7px 14px;background:#3a1d1d;color:#ffb3ad;font-weight:600}#stopall:hover{background:#5a2626}#stopall:disabled{opacity:.5}
.menu{position:relative}.menu>button{font-size:16px;padding:5px 10px}
.menu .dd{display:none;position:absolute;right:0;top:36px;background:var(--card);border:1px solid #444;border-radius:10px;min-width:190px;z-index:5;overflow:hidden}
.menu.open .dd{display:block}.dd a{display:block;padding:10px 14px;font-size:13px;cursor:pointer;color:var(--fg)}.dd a:hover{background:#2c2c2e}
main{padding:16px 20px;max-width:1100px;margin:auto}
.ams{margin-bottom:22px}.ams h2{font-size:15px;color:var(--mute);margin:0 0 8px;font-weight:500}
.ams h2 span{margin-left:12px}.heat{color:#ff9f0a}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:10px}
.card{background:var(--card);border-radius:12px;padding:12px 14px;border-left:5px solid #444;position:relative}
.card.ok{border-color:var(--ok)}.card.warn{border-color:var(--warn)}.card.bad{border-color:var(--bad)}.card.empty{opacity:.45}
.slot{font-size:12px;color:var(--mute)}.name{font-weight:600;margin:2px 0 6px;font-size:15px}
.name.unnamed{color:var(--warn)}
.bar{height:6px;background:#333;border-radius:3px;overflow:hidden;margin:6px 0}.bar i{display:block;height:100%;background:var(--ok)}
.warn .bar i{background:var(--warn)}.bad .bar i{background:var(--bad)}
.meta{font-size:12px;color:var(--mute);line-height:1.5}.verdict{font-weight:600;font-size:13px}
.ok .verdict{color:var(--ok)}.warn .verdict{color:var(--warn)}.bad .verdict{color:var(--bad)}
.loaded{position:absolute;top:10px;right:12px;font-size:11px;background:var(--blue);color:#fff;border-radius:6px;padding:2px 6px}
.acts{margin-top:8px;display:flex;gap:6px;flex-wrap:wrap}
button{background:#2c2c2e;color:var(--fg);border:0;border-radius:7px;padding:5px 9px;font-size:12px;cursor:pointer}
button:hover{background:#3a3a3c}button.primary{background:var(--blue)}
#events{font:12px ui-monospace,Menlo,monospace;color:var(--mute);background:var(--card);border-radius:12px;padding:10px 14px;max-height:260px;overflow:auto;white-space:pre}
.banner{background:#3a1d1d;color:#ffb3ad;padding:10px 20px;font-size:14px}
dialog{background:var(--card);color:var(--fg);border:0;border-radius:14px;padding:18px;min-width:300px}
dialog input,dialog select{width:100%;box-sizing:border-box;margin:6px 0 12px;padding:8px;border-radius:8px;border:1px solid #444;background:#111;color:var(--fg);font-size:14px}
dialog label{font-size:12px;color:var(--mute)}
::backdrop{background:rgba(0,0,0,.6)}
.unit{display:flex;align-items:center;gap:14px;flex-wrap:wrap;background:#161618;border:1px solid #2a2a2c;border-radius:12px;padding:10px 14px;margin-bottom:10px}
.unit .title{font-size:16px;font-weight:600;color:var(--fg)}
.pill{font-size:12px;font-weight:600;padding:4px 10px;border-radius:999px;letter-spacing:.3px}
.pill.on{background:#5a3a00;color:#ffb340}.pill.off{background:#2c2c2e;color:#aaa}.pill.empty{background:#2c2c2e;color:#777}
.pill.cool{background:#1c3a5a;color:#7cc4ff}
.env{font-size:13px;color:var(--mute)}
.sw{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--mute);cursor:pointer;user-select:none;margin-left:auto}
.sw .track{width:40px;height:22px;border-radius:11px;background:#444;position:relative;transition:.2s}
.sw .track::after{content:"";position:absolute;top:2px;left:2px;width:18px;height:18px;border-radius:50%;background:#fff;transition:.2s}
.sw.on .track{background:var(--ok)}.sw.on .track::after{left:20px}.sw b{color:var(--fg)}
button.stop{background:#5a1d1d;color:#ffb3ad}button.stop:hover{background:#7a2626}
.dry-waiting:disabled{opacity:1;cursor:wait}.dry-wait-spinner{display:inline-block;width:13px;height:13px;margin-right:6px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;vertical-align:-2px;animation:dry-wait-spin .8s linear infinite}@keyframes dry-wait-spin{to{transform:rotate(360deg)}}
.legend{font-size:12px;color:var(--mute);margin:-6px 0 14px}
#cam :is(img,canvas.camera-surface){width:100%;max-width:1100px;border-radius:12px;border:1px solid #2a2a2c;display:block}
#cam{margin-bottom:14px}
#act{margin-bottom:14px}
#err{margin-bottom:16px}
.bigerr{background:linear-gradient(180deg,#4a1414,#2c0f0f);border:2px solid #b03030;border-radius:14px;padding:20px 24px;box-shadow:0 0 30px rgba(176,48,48,.25)}
.bigerr .lbl{font-size:12px;letter-spacing:1.5px;text-transform:uppercase;color:#ff9a90;font-weight:700}
.bigerr .msg{font-size:22px;line-height:1.35;font-weight:700;color:#fff;margin:8px 0 4px}
.bigerr .sub{font-size:13px;color:#ffc9c4;margin-bottom:14px}
.bigerr .acts{display:flex;gap:10px;flex-wrap:wrap}
.bigerr .acts button{padding:11px 18px;font-size:14px;font-weight:600;border-radius:10px}
.bigerr a{color:#ffd9d4}
.bigwarn{background:linear-gradient(180deg,#43330f,#2a2008);border-color:#9a7420;box-shadow:0 0 24px rgba(154,116,32,.2)}
.bigwarn .lbl{color:#ffcf7a}.bigwarn .sub{color:#f0dcae}
.actbox{background:#12202a;border:1px solid #1d4a63;border-radius:12px;padding:12px 16px}
.actbox.done{background:#14261a;border-color:#2b5c38}
.actbox .title{font-size:15px;font-weight:700;color:#7cc4ff;margin-bottom:8px}
.actbox.done .title{color:var(--ok)}
.stepli{display:flex;align-items:center;gap:10px;font-size:14px;padding:3px 0;color:var(--mute)}
.stepli.cur{color:var(--fg);font-weight:600}
.stepli .dot{width:16px;text-align:center;flex:none}
.stepli.cur .dot{color:#7cc4ff;animation:pulse 1.2s ease-in-out infinite}
.stepli .when{margin-left:auto;font-size:12px;opacity:.6;font-variant-numeric:tabular-nums}
#ctl{margin-bottom:16px}
.ctlbox{background:#161618;border:1px solid #2a2a2c;border-radius:12px;padding:12px 16px}
.ctlhead{display:flex;align-items:center;gap:12px;cursor:pointer;user-select:none}
.ctlhead .title{font-size:16px;font-weight:600}
.ctlgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:18px;margin-top:14px}
.panel{background:#1c1c1e;border:1px solid #2a2a2c;border-radius:12px;padding:12px 14px}
.panel h4{margin:0 0 10px;font-size:12px;color:var(--mute);font-weight:600;text-transform:uppercase;letter-spacing:.5px;display:flex;justify-content:space-between;align-items:center}
.panel h4 b{color:var(--fg);font-weight:600;text-transform:none;letter-spacing:0}

/* jog pad */
.pad{display:grid;grid-template-columns:repeat(3,52px);grid-template-rows:repeat(3,44px);gap:6px;justify-content:center}
.pad button{border-radius:10px;font-size:15px;font-weight:600;display:flex;align-items:center;justify-content:center;background:#2c2c2e}
.pad button:hover:not(:disabled){background:#0a84ff}
.pad .mid{background:#232325;font-size:12px;color:var(--mute);font-weight:500}
.zcol{display:grid;grid-template-rows:repeat(2,44px);gap:6px;width:52px}
.zcol button{border-radius:10px;font-size:15px;font-weight:600;background:#2c2c2e}
.zcol button:hover:not(:disabled){background:#0a84ff}
.axisrow{display:flex;gap:14px;align-items:center;justify-content:center}
.steps{display:flex;gap:6px;justify-content:center;margin-top:10px}
.steps button{border-radius:999px;padding:5px 12px;font-size:12px;background:#2c2c2e}
.steps button.sel{background:var(--blue);color:#fff;font-weight:600}
.bigrow{display:flex;gap:8px;flex-wrap:wrap}
.bigrow button{flex:1;min-width:84px;padding:9px 10px;border-radius:10px;font-size:13px}

/* light chips */
.chips{display:flex;gap:8px;flex-wrap:wrap}
.chip{display:flex;align-items:center;gap:7px;padding:7px 12px;border-radius:999px;background:#2c2c2e;cursor:pointer;font-size:13px;border:1px solid transparent}
.chip.on{background:#3a3410;border-color:#6a5a10;color:#ffe08a}
.chip .bulb{width:9px;height:9px;border-radius:50%;background:#555}
.chip.on .bulb{background:#ffd60a;box-shadow:0 0 8px #ffd60a}

/* sliders */
.fan{display:flex;align-items:center;gap:10px;margin-bottom:9px}
.fan label{width:60px;font-size:13px;color:var(--mute)}
.fan input[type=range]{flex:1;accent-color:var(--blue)}
.fan .val{width:44px;text-align:right;font-size:13px;font-variant-numeric:tabular-nums}

/* speed segmented */
.seg{display:flex;background:#2c2c2e;border-radius:10px;overflow:hidden}
.seg button{flex:1;border-radius:0;padding:9px 6px;font-size:12px;background:transparent}
.seg button.sel{background:var(--blue);color:#fff;font-weight:600}
.seg button:hover:not(.sel){background:#3a3a3c}
.envbox{display:flex;align-items:center;gap:14px;flex-wrap:wrap;background:#161618;border:1px solid #2a2a2c;border-radius:12px;padding:10px 14px;margin:0 0 16px}
.envbox .title{font-size:16px;font-weight:600}
.temp{font-size:13px;color:var(--mute)}.temp b{color:var(--fg);font-size:15px}
.tinp{width:58px;margin-left:4px;padding:3px 6px;border-radius:7px;border:1px solid #3a3a3c;background:#111;color:var(--fg);font-size:13px}
.tinp:focus{outline:1px solid var(--blue)}.temp .tgt{color:#ff9f0a}
.presets{display:flex;gap:6px;flex-wrap:wrap;margin-left:auto}
button.preset{background:#3a2a10;color:#ffb340;font-weight:600;padding:7px 12px;font-size:13px}button.preset:hover{background:#5a3a00}
button.ghost{background:transparent;border:1px dashed #555;color:var(--mute)}
.fchips{display:flex;flex-wrap:wrap;gap:8px}
.fchip{display:flex;align-items:center;gap:8px;padding:10px 14px;border-radius:10px;background:#2c2c2e;cursor:pointer;font-size:14px;font-weight:600;border:1px solid #3a3a3c}
.fchip:hover{background:var(--blue);border-color:var(--blue)}
.sw2{display:inline-block;width:16px;height:16px;border-radius:50%;border:2px solid #555;box-shadow:inset 0 0 0 1px #000;flex:none}
.rem{display:flex;align-items:center;gap:8px;margin:2px 0 6px;font-size:12px;color:var(--mute)}
.rembar{flex:1;height:10px;background:#2a2a2c;border-radius:5px;overflow:hidden;border:1px solid #3a3a3c}
.rembar i{display:block;height:100%;border-radius:4px}
nav#tabs{display:flex;gap:8px;padding:14px 20px 0;border-bottom:1px solid #333;overflow-x:auto}
nav#tabs:empty{display:none}
.tab{padding:14px 26px;font-size:18px;font-weight:600;color:var(--mute);background:#1a1a1c;border:1px solid #2a2a2c;border-bottom:0;border-radius:12px 12px 0 0;cursor:pointer;white-space:nowrap;display:flex;align-items:center;gap:10px}
.tab:hover{color:var(--fg)}.tab.active{color:var(--fg);background:var(--card);border-color:#444}
.tab .dot{width:10px;height:10px;border-radius:50%;background:#555;flex:none}
.tab .dot.busy{background:var(--blue);box-shadow:0 0 8px var(--blue);animation:pulse 1.6s ease-in-out infinite}
.tab .dot.paused{background:#ff9f0a}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}.tab .dot.ok{background:var(--ok)}.tab .dot.bad{background:var(--bad)}.tab .dot.heat{background:#ff9f0a}
.tab small{font-size:12px;font-weight:400;color:var(--mute)}
.tab.over{outline:2px dashed var(--blue);outline-offset:-2px}.tab[draggable]{cursor:grab}

.printer-controls{--pc-bg:#121416;--pc-surface:#1a1d21;--pc-field:#252930;--pc-line:#343941;--pc-text:#eef1f5;--pc-mute:#a1aab8;--pc-blue:#62a5ff;--pc-green:#65d5a4;--pc-amber:#eab567;--pc-red:#f18f98;font:400 14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:var(--pc-text);padding:0;}
.printer-controls *{box-sizing:border-box;}
.printer-controls [hidden]{display:none!important;}
.printer-controls .pc-shell{background:var(--pc-bg);border:1px solid var(--pc-line);border-radius:16px;overflow:hidden;}
.printer-controls button,.printer-controls input,.printer-controls select{font:inherit;}
.printer-controls button{color:inherit;cursor:pointer;}
.printer-controls .pc-header{padding:20px 22px 14px;display:flex;align-items:center;justify-content:space-between;gap:12px;}
.printer-controls .pc-heading{font-size:19px;font-weight:600;letter-spacing:-.4px;}
.printer-controls .pc-muted{color:var(--pc-mute);font-size:12px;}
.printer-controls .pc-live{display:inline-flex;align-items:center;gap:5px;}
.printer-controls .pc-dot{width:6px;height:6px;border-radius:50%;background:var(--pc-green);}
.printer-controls .pc-quiet{background:none;border:0;padding:7px 0;display:flex;align-items:center;gap:7px;color:var(--pc-mute);font-size:13px;}
.printer-controls button:focus-visible{outline:2px solid var(--pc-blue);outline-offset:3px;}
.printer-controls svg{width:17px;height:17px;flex:none;}
.printer-controls .pc-job{margin:0 22px;padding:13px 0 17px;display:grid;grid-template-columns:minmax(0,1fr) auto;gap:20px;border-bottom:1px solid var(--pc-line);}
.printer-controls .pc-jobtitle{display:flex;align-items:center;gap:9px;font-weight:500;}
.printer-controls .pc-jobtitle span:first-child{color:var(--pc-green);font-size:12px;}
.printer-controls .pc-file{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.printer-controls .pc-progressline{display:flex;align-items:center;gap:12px;margin-top:6px;font-size:12px;color:var(--pc-mute);}
.printer-controls .pc-progress{height:4px;max-width:180px;flex:1;background:var(--pc-field);border-radius:10px;overflow:hidden;}
.printer-controls .pc-progress i{width:46%;height:100%;display:block;background:var(--pc-green);}
.printer-controls .pc-actions{display:flex;gap:8px;align-items:center;}
.printer-controls .pc-btn{background:var(--pc-field);border:1px solid transparent;border-radius:8px;padding:8px 12px;display:inline-flex;align-items:center;justify-content:center;gap:7px;font-size:13px;font-weight:500;}
.printer-controls .pc-btn.primary{background:var(--pc-blue);color:#102743;}
.printer-controls .pc-btn.stop{background:color-mix(in srgb,var(--pc-red) 9%,transparent);color:var(--pc-red);}
.printer-controls .pc-btn:disabled{opacity:.38;cursor:default;}
.printer-controls .pc-temperatures{display:grid;grid-template-columns:repeat(var(--pc-temp-cols,4),minmax(0,1fr));padding:16px 10px 12px;}
.printer-controls .pc-temp{background:none;border:0;text-align:left;padding:8px 12px;border-radius:10px;min-width:0;}
.printer-controls .pc-temp:hover{background:var(--pc-field);}
.printer-controls .pc-temphead{display:flex;align-items:center;gap:6px;color:var(--pc-mute);font-size:12px;}
.printer-controls .pc-temphead svg{width:13px;height:13px;opacity:.7;}
.printer-controls .pc-tempvalue{font-size:26px;font-weight:500;letter-spacing:-.6px;font-variant-numeric:tabular-nums;margin:2px 0;}
.printer-controls .pc-tempvalue small{font-size:14px;color:var(--pc-mute);font-weight:400;margin-left:3px;}
.printer-controls .pc-tempfoot{color:var(--pc-mute);font-size:11px;}
.printer-controls .pc-controls{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.05fr);gap:25px;margin:0 22px;padding:18px 0 20px;border-top:1px solid var(--pc-line);}
.printer-controls .pc-sectiontitle{font-size:12px;color:var(--pc-mute);margin-bottom:10px;}
.printer-controls .pc-speed{display:flex;gap:3px;background:var(--pc-field);padding:3px;border-radius:9px;}
.printer-controls .pc-speed button{border:0;background:none;flex:1;min-width:0;padding:8px 2px;font-size:12px;border-radius:6px;color:var(--pc-mute);}
.printer-controls .pc-speed button[aria-pressed=true]{background:var(--pc-surface);color:var(--pc-text);box-shadow:0 1px 5px #00000014;}
.printer-controls .pc-speednote{display:flex;justify-content:space-between;margin-top:9px;font-size:11px;color:var(--pc-mute);}
.printer-controls .pc-lights{display:flex;justify-content:space-between;gap:10px;}
.printer-controls .pc-light{border:0;background:none;display:flex;flex-direction:column;align-items:center;gap:6px;min-width:0;padding:0;font-size:11px;color:var(--pc-mute);}
.printer-controls .pc-lighticon{height:35px;width:35px;display:flex;align-items:center;justify-content:center;border-radius:50%;background:var(--pc-field);color:var(--pc-mute);}
.printer-controls .pc-light[aria-pressed=true] .pc-lighticon{background:color-mix(in srgb,var(--pc-amber) 14%,transparent);color:var(--pc-amber);}
.printer-controls .pc-lightstate{font-size:10px;line-height:1;color:var(--pc-mute);}
.printer-controls .pc-footer{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:13px 22px;background:var(--pc-surface);border-top:1px solid var(--pc-line);}
.printer-controls .pc-cooling{background:none;border:0;padding:0;display:flex;align-items:center;gap:9px;text-align:left;font-size:12px;}
.printer-controls .pc-cooling .pc-muted{font-size:11px;}
.printer-controls .pc-switchrow{display:flex;align-items:center;gap:8px;font-size:11px;color:var(--pc-mute);}
.printer-controls .pc-switch{width:31px;height:18px;border:0;border-radius:20px;background:var(--pc-field);padding:2px;display:flex;align-items:center;}
.printer-controls .pc-switch i{width:14px;height:14px;border-radius:50%;background:var(--pc-mute);display:block;}
.printer-controls .pc-switch[aria-checked=true]{background:var(--pc-green);justify-content:flex-end;}
.printer-controls .pc-switch[aria-checked=true] i{background:#14241e;}
.printer-controls .pc-editor{padding:20px 22px;background:var(--pc-surface);border-top:1px solid var(--pc-line);}
.printer-controls .pc-edithead{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;}
.printer-controls .pc-edithead h3{font-size:15px;font-weight:600;margin:0;}
.printer-controls .pc-close{background:none;border:0;padding:3px;color:var(--pc-mute);display:flex;align-items:center;gap:6px;font-size:12px;}
.printer-controls .pc-inline{display:flex;align-items:center;gap:9px;}
.printer-controls .pc-alert{padding:10px 12px;color:var(--pc-amber);background:color-mix(in srgb,var(--pc-amber) 9%,transparent);border-radius:8px;font-size:12px;margin-bottom:16px;display:flex;gap:9px;align-items:flex-start;}
.printer-controls .pc-alert svg{margin-top:2px;}
.printer-controls .pc-fieldlabel{display:block;color:var(--pc-mute);font-size:12px;margin-bottom:6px;}
.printer-controls .pc-input{background:var(--pc-bg);color:var(--pc-text);border:1px solid var(--pc-line);border-radius:7px;padding:8px 10px;width:110px;}
.printer-controls .pc-editbody{display:flex;justify-content:space-between;gap:20px;align-items:flex-end;}
.printer-controls .pc-range{width:100%;accent-color:var(--pc-blue);}
.printer-controls .pc-fanrow{display:grid;grid-template-columns:85px minmax(0,1fr) 72px;align-items:center;gap:15px;margin:16px 0;}
.printer-controls .pc-fanrow .pc-input{width:72px;padding:6px 8px;}
.printer-controls .pc-lock{display:flex;align-items:center;gap:13px;padding:12px 0 18px;}
.printer-controls .pc-lockbox{width:40px;height:40px;background:var(--pc-field);border-radius:10px;display:flex;align-items:center;justify-content:center;color:var(--pc-mute);flex:none;}
.printer-controls .pc-machinenav{display:flex;gap:17px;border-bottom:1px solid var(--pc-line);margin-bottom:20px;}
.printer-controls .pc-machinenav button{background:none;border:0;padding:0 0 10px;color:var(--pc-mute);font-size:12px;}
.printer-controls .pc-machinenav button[aria-selected=true]{border-bottom:2px solid var(--pc-blue);color:var(--pc-text);}
.printer-controls .pc-machinebody{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:25px;}
.printer-controls .pc-jog{display:flex;justify-content:center;gap:24px;align-items:center;margin-top:18px;}
.printer-controls .pc-pad{display:grid;grid-template-columns:repeat(3,44px);grid-template-rows:repeat(3,40px);gap:3px;}
.printer-controls .pc-pad button,.printer-controls .pc-z button{border:0;border-radius:8px;background:var(--pc-field);font-size:17px;display:flex;justify-content:center;align-items:center;}
.printer-controls .pc-pad span{display:flex;align-items:center;justify-content:center;color:var(--pc-mute);font-size:11px;}
.printer-controls .pc-z{display:grid;grid-template-rows:auto 40px 40px;gap:3px;width:44px;text-align:center;color:var(--pc-mute);font-size:11px;}
.printer-controls .pc-step{display:flex;justify-content:center;gap:5px;margin-top:12px;}
.printer-controls .pc-step button{border:1px solid var(--pc-line);background:none;color:var(--pc-mute);padding:4px 10px;border-radius:6px;font-size:11px;}
.printer-controls .pc-step button[aria-pressed=true]{color:var(--pc-blue);border-color:var(--pc-blue);background:color-mix(in srgb,var(--pc-blue) 8%,transparent);}
.printer-controls .pc-service{display:flex;gap:14px;flex-wrap:wrap;align-items:flex-start;}
.printer-controls .pc-serviceitem{flex:1;min-width:180px;}
.printer-controls .pc-serviceitem p{font-size:12px;color:var(--pc-mute);margin:8px 0 13px;}
.printer-controls .pc-commandfeedback{font-size:12px;color:var(--pc-green);min-height:18px;margin-top:12px;}
.printer-controls .pc-confirm{display:flex;gap:8px;align-items:center;flex-wrap:wrap;}
@media(max-width:600px){.printer-controls .pc-header,.printer-controls .pc-job,.printer-controls .pc-editor{padding-left:16px;padding-right:16px;}.printer-controls .pc-job{margin:0;padding-top:10px;padding-bottom:15px;}.printer-controls .pc-jobtitle{flex-wrap:wrap;gap:4px;}.printer-controls .pc-job{gap:8px;}.printer-controls .pc-btn{padding:7px 9px;}.printer-controls .pc-temperatures{grid-template-columns:repeat(2,minmax(0,1fr));padding:12px 4px;}.printer-controls .pc-controls{grid-template-columns:1fr;gap:20px;margin:0 16px;}.printer-controls .pc-lights{justify-content:flex-start;gap:35px;}.printer-controls .pc-footer{flex-wrap:wrap;padding:13px 16px;gap:15px;}.printer-controls .pc-machinebody{grid-template-columns:1fr;}.printer-controls .pc-header{align-items:flex-start;}.printer-controls .pc-editbody{flex-wrap:wrap;align-items:flex-start;}.printer-controls .pc-fanrow{grid-template-columns:70px minmax(0,1fr) 60px;gap:8px;}.printer-controls .pc-fanrow .pc-input{width:60px;}.printer-controls .pc-machinenav{gap:13px;}.printer-controls .pc-header .pc-quiet{font-size:11px;}}

.pc-command-error{display:flex;align-items:center;gap:10px;padding:10px 14px;border:1px solid var(--bad);border-radius:10px;background:var(--card);color:var(--bad);font-size:12px}
.pc-command-error .alert-close{margin-left:auto}
.printer-controls .pc-temp:disabled{cursor:default;opacity:.65}
.printer-controls .pc-light:disabled,.printer-controls .pc-speed button:disabled,.printer-controls .pc-switch:disabled{cursor:default;opacity:.5}
.printer-controls .pc-loading{padding:20px;color:var(--pc-mute)}
.printer-controls .pc-tempfoot.capped{color:var(--pc-amber)}
.printer-controls .pc-light .pc-lightstate{font-size:11px}
.printer-controls .pc-fan-unit{display:flex;align-items:center;gap:5px}
.printer-controls .pc-blocked-note{color:var(--pc-amber);font-size:12px;margin:10px 0}
@media(pointer:coarse){.printer-controls button{min-height:44px}.printer-controls .pc-switch{width:44px;height:26px;min-height:26px}.printer-controls .pc-switch i{width:22px;height:22px}.printer-controls .pc-pad{grid-template-columns:repeat(3,44px);grid-template-rows:repeat(3,44px)}.printer-controls .pc-input{font-size:16px}}

@media(max-width:600px){header{flex-direction:column;align-items:flex-start;gap:12px}.hdr{width:100%;flex-wrap:wrap;gap:8px}main{padding:16px 12px}.printer-controls .pc-fanrow{grid-template-columns:60px minmax(0,1fr) 82px}.printer-controls .pc-fan-unit .pc-input{width:60px}}

/* Shared dashboard design: same surfaces, type and feedback as printer controls. */
:root{--bg:#121416;--card:#1a1d21;--fg:#eef1f5;--mute:#a1aab8;--ok:#65d5a4;--warn:#eab567;--bad:#f18f98;--blue:#62a5ff;--line:#343941;--field:#252930}
*{box-sizing:border-box}body{font:400 14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}button,input,select{font:inherit}button{padding:8px 12px;border-radius:8px;font-size:13px;font-weight:500;background:var(--field);line-height:1.4}button:hover:not(:disabled){background:#323842}button:disabled{cursor:default;opacity:.45}button.primary{background:var(--blue);color:#102743}button.primary:hover{background:#80b7ff}button.stop{background:#f18f9817;color:var(--bad)}button.stop:hover{background:#f18f9826}button:focus-visible,a:focus-visible,[tabindex]:focus-visible{outline:2px solid var(--blue);outline-offset:3px}input:focus,select:focus{outline:2px solid var(--blue);outline-offset:1px}
header{max-width:1144px;margin:auto;padding:28px 22px 18px;align-items:flex-start;gap:24px;border:0;flex-wrap:wrap}.brand{min-width:175px}h1{font-size:25px;letter-spacing:-.7px;font-weight:600}.brand p{margin:3px 0 0;color:var(--mute);font-size:12px}.hdr{gap:8px;flex-wrap:wrap;justify-content:flex-end;flex:1}.conn{padding:5px 0;background:none!important;font-size:11px;font-weight:400;color:var(--mute)!important}.conn i{width:6px;height:6px;box-shadow:none!important}.conn.ok i{background:var(--ok)}#net{margin-right:12px}#upd{order:2;flex-basis:100%;text-align:right;font-size:11px}.hdr>button,.menu>button{padding:8px 12px;font-size:12px}#dryall{background:#eab56717;color:var(--warn);font-weight:500}#stopall{background:#f18f9817;color:var(--bad);font-weight:500}#refresh{background:var(--field);color:var(--fg);font-size:12px}.menu>button{display:flex;align-items:center;gap:6px}.menu svg{width:15px;height:15px}.menu .dd{top:42px;min-width:210px;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:5px;box-shadow:0 10px 30px #0006}.dd a{border-radius:6px;padding:10px;font-size:12px}.dd a:hover{background:var(--field)}
nav#tabs{max-width:1100px;margin:0 auto;padding:0 0 16px;border:0;gap:8px}.tab{padding:12px 15px;border:1px solid var(--line);border-radius:10px;background:var(--card);font-size:13px;font-weight:500;gap:8px;min-width:160px;display:grid;grid-template-columns:6px 1fr;flex:1}.tab small{grid-column:2;font-size:11px;line-height:1.2}.tab .dot{width:6px;height:6px;box-shadow:none!important}.tab.active{background:#62a5ff0c;border-color:#62a5ff80;color:var(--fg)}.tab.add-printer{min-width:120px;flex:0;display:flex;justify-content:center;background:none;font-size:12px!important}.tab[draggable]{cursor:grab}.tab:active{cursor:grabbing}
main{padding:4px 22px 32px;max-width:1144px}.dashboard-summary{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:18px}.summary-item{border:1px solid var(--line);border-radius:11px;padding:14px 16px;background:var(--card)}.summary-item span{display:block;color:var(--mute);font-size:11px}.summary-item strong{display:block;margin-top:3px;font-size:24px;font-weight:500;letter-spacing:-.6px}.summary-item strong small{font-size:12px;font-weight:400;letter-spacing:0;color:var(--mute);margin-left:5px}.summary-item.needs strong{color:var(--warn)}.summary-item.ready strong{color:var(--ok)}.section-heading{display:flex;align-items:baseline;justify-content:space-between;gap:12px;margin:26px 0 12px}.section-heading h2{font-size:17px;font-weight:600;letter-spacing:-.3px;margin:0}.section-heading p{font-size:11px;color:var(--mute);margin:0}.legend{font-size:11px;line-height:1.6;margin:0 0 14px;color:var(--mute)}
#cam{background:var(--card);border:1px solid var(--line);border-radius:16px;overflow:hidden;margin:18px 0}#cam .envbox{background:none;border:0;border-bottom:1px solid var(--line);border-radius:0;padding:14px 20px;margin:0;gap:12px}#cam .title{font-size:14px}#cam .temp{font-size:11px}#cam :is(img,canvas.camera-surface){max-height:420px;object-fit:contain;background:#0b0d0f;width:100%;border:0;border-radius:0}#cam .presets{align-items:center;gap:14px}#cam .sw{font-size:11px}#cam .sw b{display:none}.envbox{background:var(--card);border-color:var(--line);padding:18px;border-radius:12px}.envbox .title{font-size:14px}.envbox .temp{font-size:12px}
.ams{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:20px;margin:0 0 18px}.unit{background:none;border:0;border-radius:0;padding:0 0 16px;margin:0;border-bottom:1px solid var(--line);gap:12px;display:grid;grid-template-columns:minmax(0,1fr) auto}.unit-title{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.unit .title{font-size:16px;font-weight:600}.unit-readings{display:flex;align-items:center;gap:12px;margin-top:7px}.unit-actions{display:flex;align-items:center;justify-content:flex-end;gap:14px;flex-wrap:wrap}.pill{font-size:10px;font-weight:500;letter-spacing:0;padding:4px 8px;white-space:normal}.pill.on{background:#eab56717;color:var(--warn)}.pill.off,.pill.empty{background:var(--field);color:var(--mute)}.pill.cool{background:#62a5ff17;color:var(--blue)}.env{font-size:11px;color:var(--mute)}.sw{font-size:11px;margin-left:0;gap:6px}.sw .track{width:31px;height:18px;border-radius:20px;background:var(--field);flex:none}.sw .track::after{width:14px;height:14px;background:var(--mute)}.sw.on .track{background:var(--ok)}.sw.on .track::after{left:15px;background:#14241e}.sw b{font-weight:500;font-size:10px}.unit-actions .sw b{display:none}.ams>.legend{margin:12px 0 0;padding:9px 12px;border-radius:8px;background:#121416;font-size:11px}.grid{grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-top:16px}.card{background:var(--bg);border:1px solid var(--line);border-radius:11px;border-top:2px solid var(--line);padding:16px;display:flex;flex-direction:column;min-width:0}.card.ok{border-color:var(--line);border-top-color:var(--ok)}.card.warn{border-color:var(--line);border-top-color:var(--warn)}.card.bad{border-color:var(--line);border-top-color:var(--bad)}.card.empty{opacity:1;background:transparent;border-style:dashed}.slot{font-size:10px;margin-bottom:6px;padding-right:0}.name{font-size:15px;font-weight:600;line-height:1.35;margin:0 0 12px;overflow-wrap:anywhere}.loaded{position:static;align-self:flex-start;order:-1;font-size:9px;padding:3px 7px;border-radius:5px;background:#62a5ff1a;color:var(--blue);margin-bottom:7px}.rem{font-size:10px;margin:0 0 10px;gap:8px}.rembar{height:4px;border:0;background:var(--field)}.rembar i{border-radius:4px}.verdict{font-size:11px;font-weight:500;line-height:1.5}.bar{height:3px;background:var(--field);margin:8px 0 10px}.meta{font-size:11px;line-height:1.65}.card .acts{margin-top:auto;padding-top:15px;gap:6px}.card .acts>button{font-size:11px;padding:7px 9px}.card details{width:100%;margin-top:3px}.card summary{font-size:11px;color:var(--mute);cursor:pointer;padding:7px 0;list-style:none;display:flex;align-items:center;justify-content:space-between}.card summary::after{content:'+';font-size:16px}.card details[open] summary::after{content:'−'}.card details .acts{margin-top:2px;padding-top:6px;border-top:1px solid var(--line);display:flex;flex-wrap:wrap;gap:6px}.card details .acts button{font-size:11px;padding:7px 9px}
#events{font:400 12px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;border:1px solid var(--line);border-radius:12px;background:var(--card);padding:0;max-height:330px;white-space:normal}.event-row{display:grid;grid-template-columns:125px 110px minmax(0,1fr);align-items:start;gap:16px;padding:12px 16px;border-bottom:1px solid var(--line)}.event-row:last-child{border:0}.event-row time{font-size:10px;color:var(--mute);font-variant-numeric:tabular-nums}.event-kind{font-size:10px;color:var(--blue);text-transform:lowercase}.event-message{font-size:11px;color:var(--fg);overflow-wrap:anywhere}.events-empty{padding:20px;color:var(--mute)}
.bigerr,.bigwarn{background:var(--card);border:1px solid var(--bad);border-radius:12px;padding:20px;box-shadow:none}.bigwarn{border-color:var(--warn)}.bigerr .lbl{font-size:10px;letter-spacing:.6px;color:var(--bad)}.bigwarn .lbl{color:var(--warn)}.bigerr .msg{font-size:20px;font-weight:600;margin:6px 0}.bigerr .sub,.bigwarn .sub{font-size:12px;color:var(--mute)}.bigerr .acts button{font-size:12px;padding:9px 12px}.actbox,.actbox.done{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:18px}.actbox .title{font-size:14px;font-weight:500;color:var(--blue)}.stepli{font-size:12px;padding:5px 0}.banner{max-width:1100px;margin:0 auto 12px;padding:12px 16px;border:1px solid #f18f9860;background:#f18f9810;border-radius:10px;color:var(--bad);font-size:12px}
dialog{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:24px;min-width:0;width:min(480px,calc(100vw - 28px));max-height:calc(100dvh - 40px);overflow-y:auto;box-shadow:0 24px 80px #0008}dialog h3{font-size:18px;font-weight:600;letter-spacing:-.3px}dialog label{font-size:12px;color:var(--mute)}dialog input,dialog select{background:var(--bg);border-color:var(--line);border-radius:8px;padding:10px 12px;font-size:14px;margin:7px 0 16px;min-width:0}dialog input[type=checkbox]{accent-color:var(--blue);width:auto}dialog button{font-size:12px}dialog .dialog-actions{margin-top:20px;padding-top:16px;border-top:1px solid var(--line)}dialog::backdrop{background:#080a0dc9;backdrop-filter:blur(5px)}.fchips{gap:8px}.fchip{background:var(--field);border:1px solid var(--line);font-size:12px;font-weight:500;padding:10px 12px;border-radius:8px}.fchip:hover{background:#62a5ff18;color:var(--blue);border-color:var(--blue)}
.printer-controls .pc-lights{justify-content:flex-start;flex-wrap:wrap;gap:8px}.printer-controls .pc-light{display:grid;grid-template-columns:20px 1fr;column-gap:7px;row-gap:0;background:var(--pc-field);border:1px solid transparent;padding:9px 10px;min-width:100px;text-align:left;align-items:center;border-radius:8px}.printer-controls .pc-lighticon{grid-row:span 2;width:20px;height:24px;background:none!important;border-radius:0}.printer-controls .pc-lighticon svg{width:20px;height:20px}.printer-controls .pc-light>span:nth-child(2){font-size:11px;color:var(--pc-text)}.printer-controls .pc-lightstate{font-size:10px!important;line-height:1.4}.printer-controls .pc-light[aria-pressed=true]{background:#eab5670c;border-color:#eab56730}.printer-controls .pc-light[aria-pressed=true] .pc-lighticon{color:var(--pc-amber)}
@media(max-width:1000px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.unit{grid-template-columns:1fr}.unit-actions{justify-content:flex-start}.hdr{justify-content:flex-start}#upd{text-align:left}nav#tabs{margin:0 22px}.tab{min-width:155px}.printer-controls .pc-light{min-width:90px;padding:8px}}
@media(max-width:600px){header{padding:20px 16px 14px;gap:14px}.hdr{gap:8px}#net{margin-right:4px}.hdr>button,.menu>button{padding:8px 10px}.hdr .conn{flex:none}#upd{font-size:10px}nav#tabs{margin:0 16px;padding-bottom:14px}.tab{padding:10px 12px;min-width:145px}.tab small{font-size:10px}main{padding:0 12px 24px}.dashboard-summary{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.summary-item{padding:12px}.summary-item strong{font-size:22px}.section-heading{flex-wrap:wrap;margin-top:22px}.section-heading p{font-size:10px}.ams{padding:14px;border-radius:12px}.grid{gap:8px}.card{padding:12px}.name{font-size:14px}.card .acts{flex-wrap:wrap}.unit-actions{gap:10px}.unit-readings{flex-wrap:wrap;gap:8px}#cam .envbox{padding:12px;gap:8px}#cam .presets{width:100%;margin-left:0;justify-content:space-between}#cam :is(img,canvas.camera-surface){max-height:280px}.event-row{grid-template-columns:1fr auto;gap:4px 10px;padding:12px}.event-row .event-message{grid-column:1/-1}.printer-controls .pc-lights{gap:8px}.printer-controls .pc-light{min-width:88px}.banner{margin:0 12px 12px}dialog{padding:20px}.bigerr .msg{font-size:18px}}
@media(max-width:380px){.grid{grid-template-columns:1fr}.card .acts>button{flex:1}.unit-actions>button{width:100%}.printer-controls .pc-light{min-width:0;flex:1;padding:8px 7px}.printer-controls .pc-light>span:nth-child(2){font-size:10px}}
@media(pointer:coarse){body button,.fchip,.card summary,.dd a{min-height:44px}.sw{min-height:44px}dialog input,dialog select{font-size:16px}}
:root{color-scheme:dark}nav#tabs{scrollbar-width:thin;scrollbar-color:var(--line) transparent}dialog .fchip:focus-visible{outline:2px solid var(--blue);outline-offset:2px}.card .meta{margin-bottom:4px}
@media(max-width:600px){.printer-controls .pc-job{grid-template-columns:1fr;gap:12px}.printer-controls .pc-job .pc-actions{justify-content:flex-end}.printer-controls .pc-jobtitle{flex-wrap:nowrap}.printer-controls .pc-progressline{flex-wrap:wrap;gap:8px}}

.printer-controls .pc-temperatures{position:relative}.printer-controls .pc-thermalcell{position:relative;min-width:0}.printer-controls .pc-thermalcell>.pc-temp{width:100%;height:100%}.printer-controls .pc-temp-popover{position:absolute;top:calc(100% + 6px);left:0;width:min(380px,calc(100vw - 48px));z-index:6;box-shadow:0 16px 48px #0009;border:1px solid var(--pc-line);border-radius:12px;overflow:hidden}.printer-controls .pc-temp-popover .pc-editor{border:0;padding:18px}.printer-controls .pc-temp-popover .pc-edithead{margin-bottom:14px}.printer-controls .pc-temp-popover .pc-editbody{flex-wrap:wrap;gap:16px}.printer-controls .pc-temp-popover .pc-inline{flex-wrap:wrap}.printer-controls .pc-thermalcell:nth-child(n+3) .pc-temp-popover{left:auto;right:0}.printer-controls .pc-shell:has(.pc-temp-popover){overflow:visible}.printer-controls .pc-shell:has(.pc-temp-popover) .pc-footer{border-radius:0 0 16px 16px}@media(max-width:600px){.printer-controls .pc-thermalcell:nth-child(odd) .pc-temp-popover{left:0;right:auto}.printer-controls .pc-thermalcell:nth-child(even) .pc-temp-popover{left:auto;right:0}}
.card>.slot{display:flex;align-items:center;gap:6px;min-height:21px}.slot-label{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.slot .loaded{flex:none;position:static;align-self:auto;order:0;margin:0 0 0 auto;font-size:9px;line-height:15px;padding:2px 6px}
.attention-button{display:inline-flex;align-items:center;gap:6px}.attention-button svg{width:16px;height:16px}.attention-button.unread{color:var(--bad);background:#f18f9820;box-shadow:0 0 0 1px #f18f9860}.attention-button.unread::after{content:'';width:7px;height:7px;background:var(--bad);border-radius:50%}
.attention-top{display:flex;justify-content:space-between;align-items:center;gap:12px}.attention-mark{width:62px;height:62px;border-radius:16px;background:#f18f981c;color:var(--bad);display:grid;place-items:center;font-size:42px;font-weight:600}.attention-nav{display:flex;gap:6px;flex-wrap:wrap}.attention-nav button{font-size:12px}.attention-heading{font-size:clamp(28px,4vw,46px);letter-spacing:-1.5px;line-height:1.1;margin:22px 0 12px;font-weight:650}#attention-subtitle{font-size:14px;line-height:1.5;color:var(--mute);margin:0 0 24px}.attention-item{background:#111418;border:1px solid #f18f9840;border-left:4px solid var(--bad);border-radius:10px;padding:20px;margin:12px 0}.attention-item.warning{border-left-color:var(--warn)}.attention-item.resolved{border-color:var(--line);border-left-color:var(--ok)}.attention-printer{font-size:13px;font-weight:600;color:var(--bad)}.attention-item.warning .attention-printer{color:var(--warn)}.attention-item.resolved .attention-printer{color:var(--ok)}.attention-item p{font-size:clamp(19px,2.5vw,27px);line-height:1.4;margin:10px 0 18px;font-weight:500;overflow-wrap:anywhere}.attention-detail{display:flex;gap:14px;align-items:center;flex-wrap:wrap;color:var(--mute);font-size:12px}.attention-detail a{color:var(--blue)}.attention-detail button{margin-left:auto}.attention-detail code{font-size:11px}.attention-actions{display:flex;gap:10px;justify-content:flex-end;flex-wrap:wrap;margin-top:24px;border-top:1px solid var(--line);padding-top:20px}.attention-actions button{font-size:14px;padding:12px 16px}.attention-setting{display:flex;align-items:center;justify-content:space-between;gap:20px;padding:18px 0;border-bottom:1px solid var(--line)}.attention-setting strong{font-size:14px}.attention-setting p{font-size:12px;line-height:1.6;color:var(--mute);margin:5px 0 0;max-width:560px}.attention-setting button{flex:none}.attention-setting [aria-checked=true]{background:#65d5a41c;color:var(--ok)}.attention-empty{padding:24px;color:var(--mute)}.attention-error,#attention-message{color:var(--bad);font-size:12px;margin-top:12px}#attention-message:empty{display:none}
@media(max-width:600px){.attention-top{align-items:flex-start}.attention-mark{width:44px;height:44px;font-size:30px;flex:none}.attention-nav{justify-content:flex-end;gap:4px}.attention-nav button{font-size:10px;padding:8px}.attention-heading{letter-spacing:-.7px}.attention-item{padding:14px}.attention-actions button{width:100%;font-size:13px}.attention-detail button{margin-left:0}.attention-setting{gap:12px}.attention-detail code{overflow-wrap:anywhere;max-width:100%}}

.fault-details{font-size:12px;color:var(--mute);margin:10px 0}.fault-details summary{cursor:pointer;padding:6px 0}.fault-details code{display:inline-block;margin-top:5px;font-size:11px;overflow-wrap:anywhere}
.actbox.failed{border-color:var(--bad)}.actbox.failed .title,.dry-blocked strong{color:var(--bad)}.dry-blocked{border-left:3px solid var(--bad);padding:14px 16px}.dry-blocked p{margin:8px 0}.actbox.cancelled .title{color:var(--mute)}

#attention-panel{padding:16px 18px;border:1px solid var(--bad);border-radius:12px;background:var(--card);scroll-margin-top:20px}#attention-panel[hidden]{display:none}#attention-panel .attention-top{align-items:center;flex-wrap:wrap}#attention-panel .attention-heading{font-size:14px;line-height:1.4;letter-spacing:0;margin:0;color:var(--bad)}#attention-panel .attention-nav button{font-size:11px;padding:5px 9px;background:transparent;color:var(--mute)}#attention-panel .attention-nav button:hover{background:var(--btn);color:var(--fg)}#attention-panel #attention-subtitle{font-size:12px;margin:8px 0;color:var(--mute)}#attention-panel .attention-item{background:transparent;border:0;border-top:1px solid var(--line);border-radius:0;margin:10px 0 0;padding:12px 0 0}#attention-panel .attention-printer{font-size:12px}#attention-panel .attention-item p{font-size:14px;line-height:1.5;font-weight:500;margin:6px 0 8px}#attention-panel .attention-detail{gap:12px}#attention-panel .fault-details{margin:0}#attention-panel .fault-details summary{padding:3px 0}#attention-panel .attention-detail button{font-size:11px;padding:5px 9px}#attention-panel .attention-actions{justify-content:flex-start;margin-top:12px;padding-top:0;border:0}#attention-panel .attention-actions:empty{display:none}#attention-panel .attention-actions button{font-size:12px;padding:7px 10px;width:auto}#attention-panel .attention-empty{padding:8px 0;font-size:12px}#attention-panel .attention-setting{padding:12px 0}#attention-panel .attention-setting p{max-width:680px}
nav#tabs{padding-bottom:8px}main{padding-top:0}#cam{margin-top:0}#attention-panel{margin-bottom:18px}
#attention-panel{max-width:calc(100vw - 32px)}#attention-panel .attention-top{justify-content:flex-start;flex-wrap:wrap}#attention-panel .attention-close-visible{flex:none;min-height:44px;padding:8px 12px}#attention-panel .attention-nav{margin-left:auto}#attention-panel .attention-actions{justify-content:flex-start}
/* Print room: camera-first overview and task-focused printer desk. */
:root{--bg:#111719;--card:#1a2225;--field:#232d31;--line:#344045;--fg:#f0f4f2;--mute:#a0b0b2;--ok:#aee6cc;--blue:#aee6cc;--warn:#ffcc86;--bad:#ff9b9b}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}body *{box-sizing:border-box}body [hidden]{display:none!important}
header{max-width:1440px;padding:22px 28px;margin:auto;align-items:center;gap:20px;flex-wrap:wrap;border-bottom:1px solid var(--line)}header h1{font-size:21px;letter-spacing:-.6px}header .subtitle{display:none}.hdr{gap:9px;flex-wrap:wrap}#upd{display:none}#conn{display:none}.hdr #dryall,.hdr #stopall{display:none}#refresh{background:var(--field)}button.primary,.pc-btn.primary{color:#10231b!important;background:var(--ok)!important}button:focus-visible,a:focus-visible{outline:2px solid var(--ok);outline-offset:4px}.room-nav{display:flex;gap:6px}.room-nav button{padding:9px 15px;color:var(--mute);background:transparent;font-size:13px}.room-nav button[aria-pressed=true]{color:var(--fg);background:var(--field)}main#room-main{max-width:1440px;padding:28px;margin:auto}.room-heading{display:flex;justify-content:space-between;align-items:end;gap:18px;margin-bottom:22px}.room-heading h2{font-size:27px;font-weight:600;letter-spacing:-.8px;margin:0 0 7px}.room-heading p{font-size:13px;color:var(--mute);margin:0}.room-heading-actions{display:flex;gap:8px;align-items:center}.room-heading button{padding:10px 14px}.room-wall{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}.room-printer{border:1px solid var(--line);border-radius:14px;background:var(--card);overflow:hidden;min-width:0}.room-feed{position:relative;display:block;width:100%;aspect-ratio:1.9;padding:0;background:#080d0e;text-align:left;overflow:hidden;border-radius:0}.room-feed:hover{background:#080d0e}.room-feed :is(img,canvas.camera-surface){position:absolute;inset:0;width:100%;height:100%;object-fit:cover}.room-feed:after{content:'';position:absolute;inset:0;background:linear-gradient(180deg,#050a0c66,transparent 40%,#050a0ccc);pointer-events:none}.room-cam-empty{position:absolute;inset:0;display:grid;place-content:center;text-align:center;gap:6px;color:var(--mute);font-size:13px;padding:50px 20px}.room-feed img:not([src]){visibility:hidden}.room-feed-top{position:absolute;z-index:1;top:14px;left:16px;right:16px;display:flex;justify-content:space-between;gap:14px;font-size:12px}.room-camera-note{color:#d6dfdc;font-size:11px}.room-job{position:absolute;z-index:1;bottom:17px;left:16px;right:16px;display:flex;justify-content:space-between;align-items:end;gap:20px}.room-job strong{display:block;font-size:17px;font-weight:550;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.room-job>div:first-child{min-width:0}.room-job small{font-size:12px;color:#d0d8d5;display:block;margin-top:5px}.room-left{font-size:21px;font-variant-numeric:tabular-nums;white-space:nowrap;text-align:right}.room-progress{height:3px;background:var(--field)}.room-progress i{display:block;height:100%;background:var(--ok)}.room-cardbody{padding:15px 16px}.room-statusline{display:flex;align-items:center;justify-content:space-between;gap:10px}.room-status{display:flex;align-items:center;gap:7px;font-size:12px;color:var(--ok)}.room-status.offline{color:var(--mute)}.room-status.issue{color:var(--warn)}.room-status:before{content:'';width:6px;height:6px;border-radius:50%;background:currentColor;flex:none}.room-unit{border-top:1px solid var(--line);padding-top:10px;margin-top:12px;display:flex;align-items:center;gap:12px;justify-content:space-between}.room-unit-label{font-size:12px;color:var(--mute)}.room-spools{display:flex;gap:5px;flex-wrap:wrap;justify-content:flex-end}.room-spool{width:21px;height:21px;border-radius:50%;background:var(--spool,#596069);border:4px solid #485457;box-shadow:inset 0 0 0 2px #151a1d}.room-link{background:transparent;color:var(--ok);padding:3px 0;font-size:12px;white-space:nowrap}.room-issue{display:flex;align-items:center;justify-content:space-between;gap:12px;background:#392d22;color:var(--warn);padding:11px 16px;font-size:12px}.room-issue span{overflow-wrap:anywhere}.room-issue button{color:var(--warn);flex:none}.room-empty{padding:36px 20px;color:var(--mute);border:1px dashed var(--line);border-radius:12px}.room-foot{color:var(--mute);font-size:12px;display:flex;justify-content:space-between;gap:18px;margin-top:20px}.room-detailbar{display:flex;gap:12px;align-items:center;justify-content:space-between;margin-bottom:16px}.room-detailbar button{padding:9px 13px}.room-detailbar span{font-size:12px;color:var(--mute)}#printer-detail{max-width:1100px;margin:auto}nav#tabs{margin:0 0 8px;padding:0;max-width:none;gap:8px}.tab{padding:11px 13px;min-width:130px;border-radius:10px}.tab.active{border-color:var(--ok);background:#21332d}#cam{margin:0 0 18px}#cam .envbox{background:var(--card);border-color:var(--line);padding:14px 18px}#cam :is(img,canvas.camera-surface){max-height:420px;border-color:var(--line)}#banner:empty{display:none}#overview{display:flex;flex-wrap:wrap;gap:20px;padding:0 3px 14px;margin:0}.summary-item{background:none;border:0;padding:0;display:flex;align-items:center;gap:8px}.summary-item strong{font-size:17px;display:flex;gap:5px;margin:0}.summary-item>span{font-size:11px}.summary-item small{font-size:11px}.printer-controls{--pc-bg:var(--bg);--pc-panel:var(--card);--pc-field:var(--field);--pc-line:var(--line);--pc-text:var(--fg);--pc-muted:var(--mute);--pc-blue:var(--ok);--pc-green:var(--ok);--pc-amber:var(--warn)}.printer-controls .pc-shell{background:var(--card)}.printer-controls .pc-temperatures{background:var(--bg);margin:0 20px;border:1px solid var(--line);border-radius:10px}.printer-controls .pc-controls{gap:18px}.printer-controls .pc-controls>div{background:var(--bg);border:1px solid var(--line);padding:16px;border-radius:10px}.printer-controls .pc-header{padding:20px}.printer-controls .pc-speed button[aria-pressed=true]{background:var(--ok);color:#10231b}.printer-controls .pc-editor{background:var(--bg)}.printer-controls .pc-editor .pc-btn.primary{color:#10231b}.printer-controls .pc-header .pc-quiet{background:var(--field);border-radius:8px;padding:10px}.ams{background:var(--card)}.card{padding:15px}.card .name .sw2{width:28px;height:28px;border:5px solid #485457;box-shadow:inset 0 0 0 2px #151a1d}.card .verdict{font-size:12px}.card .acts>button{padding:9px 10px}.section-heading h2{font-size:18px}.room-dry-list{display:grid;gap:12px}.room-dry-unit{display:grid;grid-template-columns:minmax(180px,1fr) minmax(160px,1.4fr) auto;gap:24px;align-items:center;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:20px}.room-dry-unit h3{font-size:15px;margin:0 0 6px}.room-dry-unit p{font-size:12px;color:var(--mute);margin:0}.room-dry-unit .bar{margin:8px 0}.room-dry-unit .room-dry-actions{display:flex;gap:8px}.room-dry-unit button{padding:9px 12px}.room-dry-explainer{color:var(--mute);font-size:13px;line-height:1.5;margin:0 0 22px;max-width:740px}.room-dry-unit.blocked{border-color:#80613d}.room-dry-unit.blocked p.reason{color:var(--warn)}.room-unit-title{display:flex;justify-content:space-between;font-size:12px;gap:12px}.room-dry-footer{display:flex;gap:10px;justify-content:flex-end;margin-top:20px}.room-dry-footer button{padding:10px 14px}.room-dry-footer .stop{background:#392729}.room-settings-name{font-size:11px;color:var(--mute);padding:10px 14px;border-bottom:1px solid var(--line)}.room-backup-note{font-size:12px;color:var(--mute)}
.room-dry-unit .room-dry-result{margin-top:12px;padding-top:10px;border-top:1px solid var(--line);line-height:1.45}.room-dry-result strong{font-weight:600;color:var(--fg)}.room-dry-result[data-outcome=completed] strong{color:var(--ok)}.room-dry-result[data-outcome=stopped] strong{color:var(--warn)}.room-dry-result[data-reason-code="8"] strong{color:var(--bad)}.room-dry-result small{display:block;color:var(--mute);font-size:11px;margin-top:3px}.room-dry-history{margin-top:7px;font-size:11px;color:var(--mute)}.room-dry-history summary{cursor:pointer}.room-dry-history ol{padding-left:20px;margin:6px 0 0}.room-dry-history li{margin:5px 0}
.room-dry-unit .room-dry-humidity{display:inline-flex;gap:5px;align-items:center;margin-top:7px;font-size:12px;font-variant-numeric:tabular-nums;color:var(--mute)}.room-dry-humidity[data-status=dry]{color:var(--ok)}.room-dry-humidity[data-status=humid]{color:var(--warn)}
@media(min-width:1400px){.room-feed{aspect-ratio:2.05}}
@media(max-width:1000px){header{padding:18px 22px}.hdr{margin-left:auto}main#room-main{padding:22px}.room-heading h2{font-size:25px}.room-dry-unit{grid-template-columns:1fr 1fr;gap:14px}.room-dry-actions{grid-column:1/-1;justify-content:flex-end}}
@media(max-width:650px){header{padding:16px;gap:12px}.room-nav{order:3;width:100%}.hdr{gap:6px}.hdr #net{display:none}.hdr .menu>button{font-size:0}.hdr .menu svg{width:17px;height:17px}.hdr #attention-button{max-width:135px}main#room-main{padding:16px}.room-wall{grid-template-columns:1fr}.room-heading{align-items:start;flex-wrap:wrap}.room-heading h2{font-size:24px}.room-heading-actions{width:100%;justify-content:space-between}.room-feed{aspect-ratio:1.75}.room-foot{flex-wrap:wrap}.room-dry-unit{grid-template-columns:1fr}.room-dry-actions{grid-column:auto}.room-job strong{font-size:15px}.room-left{font-size:18px}#overview{gap:12px}.room-detailbar span{display:none}.printer-controls .pc-temperatures{margin:0 12px}.printer-controls .pc-controls>div{padding:12px}.room-dry-footer{justify-content:flex-start;flex-wrap:wrap}}
@media(prefers-reduced-motion:no-preference){.room-printer{transition:border-color .15s}.room-printer:hover{border-color:#708780}}
.room-desk-shortcuts{display:grid;grid-template-columns:1fr 1fr;gap:14px;padding:0 20px 20px}.pc-desk-task{background:var(--bg);border:1px solid var(--line);border-radius:10px;padding:16px;text-align:left}.pc-desk-task strong{display:block;font-size:13px;font-weight:500;margin-bottom:7px}.pc-desk-task span{font-size:11px;color:var(--mute);line-height:1.5}.pc-desk-task:hover{border-color:var(--ok);background:var(--bg)}@media(max-width:600px){.room-desk-shortcuts{grid-template-columns:1fr;padding:0 12px 14px}}


.room-printer-name{font-size:18px;font-weight:600;letter-spacing:-.3px;margin:0;padding:16px 18px;border-bottom:1px solid var(--line)}.room-feed-top>span:first-child{visibility:hidden}.room-unit{display:block}.room-spools{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:12px}.room-filament-remaining{min-width:0;background:var(--bg);padding:10px;border-radius:8px}.room-filament-label{display:flex;gap:6px;align-items:center;font-size:10px;color:var(--mute)}.room-filament-label b{display:block;color:var(--fg);font-weight:500;font-size:11px;overflow-wrap:anywhere}.room-filament-remaining>strong{display:block;margin-top:8px;font-size:13px;font-weight:500}.room-remaining-bar{height:3px;background:var(--field);margin-top:7px}.room-remaining-bar i{height:100%;display:block;background:var(--ok)}.room-followup{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:20px;margin-bottom:20px}.room-followup h3{font-size:16px;margin:0 0 12px}.room-followup-row{display:flex;justify-content:space-between;gap:16px;padding:14px 0;border-top:1px solid var(--line)}.room-followup-row strong{font-size:13px}.room-followup-row p{font-size:12px;color:var(--mute);margin:6px 0 0;line-height:1.5}.room-followup-actions{display:flex;flex-wrap:wrap;gap:6px;align-items:center;justify-content:flex-end}.room-followup-actions button{padding:8px 10px}@media(max-width:800px){.room-spools{grid-template-columns:repeat(2,minmax(0,1fr))}.room-followup-row{flex-direction:column}.room-followup-actions{justify-content:flex-start}}
.printer-controls .pc-preheat-shortcuts{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:4px 24px 20px}.pc-preheat-shortcuts>span{font-size:12px;color:var(--mute);margin-right:4px}.pc-preheat-shortcuts .pc-btn{display:flex;gap:4px;flex-direction:column;align-items:flex-start}.pc-preheat-shortcuts small{font-size:11px;font-weight:400;opacity:.8}
.printer-current-action{font-size:13px;line-height:1.5;padding:10px 18px;border-top:1px solid var(--line);color:var(--fg);background:var(--card)}

.room-live-summary{display:grid;gap:7px;margin-top:13px;font-size:12px;line-height:1.5}.room-live-summary>div{display:flex;gap:9px;align-items:baseline}.room-live-label{color:var(--mute);flex:0 0 68px}.room-live-value{min-width:0;overflow-wrap:anywhere}.room-live-value.active{color:var(--ok)}.room-filament-remaining.is-loaded{box-shadow:inset 0 0 0 1px var(--ok)}.room-loaded-label{display:block;color:var(--ok);font-size:10px;margin-top:6px}

.room-dry-actions button.is-not-drying{background:var(--bad);color:var(--bg)}
.room-filament-remaining.needs-setup{box-shadow:inset 0 0 0 1px var(--warn);background:#30291d}
.room-filament-remaining.needs-setup .room-filament-label b,.room-setup-note{color:var(--warn)}
.rfid-reading-indicator{display:inline-flex;align-items:center;justify-content:center;flex:none;width:17px;height:17px;margin-left:4px;vertical-align:middle;color:var(--ok)}
.rfid-reading-indicator::before{content:'';display:block;width:13px;height:13px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:rfid-reading-spin .8s linear infinite}
@keyframes rfid-reading-spin{to{transform:rotate(360deg)}}
@media(prefers-reduced-motion:reduce){.rfid-reading-indicator::before{animation:none;border-right-color:currentColor}}
.room-setup-note{font-size:12px;margin:10px 0 0}
.room-filament-remaining .room-slot-setup{display:block;text-align:left;white-space:normal;font-size:11px;line-height:1.4;margin-top:7px;padding:4px 0;background:transparent;color:var(--warn);text-decoration:underline;text-underline-offset:3px}
#slots [data-slot-key]:focus{outline:2px solid var(--warn);outline-offset:3px}
.room-filament-remaining.is-empty{background:#3b2024;box-shadow:inset 0 0 0 1px var(--bad)}
.room-filament-remaining.is-empty .room-filament-label,.room-filament-remaining.is-empty .room-filament-label b,.room-filament-remaining.is-empty>strong{color:var(--bad)}
.room-filament-remaining.is-empty .room-spool{border-color:var(--bad);background:transparent}
.card.empty{background:#3b2024;border-color:var(--bad);border-style:solid}
.card.empty .slot,.card.empty .name{color:var(--bad)}

.room-filament-route{position:relative}.room-filament-route>.room-spools{position:relative}.room-filament-path{position:absolute;inset:0;width:100%;height:100%;pointer-events:none;overflow:visible}.room-filament-path .route-edge{fill:none;stroke:#a0b0b2;stroke-opacity:.6;stroke-width:3.5;stroke-linejoin:round;stroke-linecap:round}.room-filament-path .route-color{fill:none;stroke:var(--filament-path);stroke-width:1.5;stroke-linejoin:round;stroke-linecap:round}.room-filament-route.is-stale .route-color{stroke-dasharray:4 4;opacity:.6}.room-nozzle-destination{display:flex;justify-content:center;align-items:center;gap:7px;padding-top:24px;color:var(--mute);font-size:10px;line-height:1.4}.room-nozzle-destination svg{width:20px;height:25px;flex:none;color:var(--fg)}.room-nozzle-destination span{max-width:65%;overflow-wrap:anywhere}.room-nozzle-destination b{color:var(--fg);font-weight:500}.room-filament-route .room-loaded-label{display:none}
.room-filament-path .route-flow{fill:none;stroke:#fff;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round;stroke-dasharray:3 15;opacity:.6;animation:filament-flow 1.2s linear infinite;pointer-events:none}
@keyframes filament-flow{to{stroke-dashoffset:-18}}
.room-shared-route.is-stale .route-flow{display:none}
@media(prefers-reduced-motion:reduce){.room-filament-path .route-flow{display:none}}

.room-shared-route{isolation:isolate}.room-shared-route>.room-filament-path{z-index:1}.room-shared-route .room-unit{position:relative}.room-shared-route .room-nozzle-junction{position:relative;height:82px;color:var(--fg)}.room-nozzle-junction>[data-nozzle-anchor]{position:absolute;width:24px;height:30px;left:50%;top:50%;transform:translate(-50%,-50%);z-index:2;background:transparent}.room-nozzle-junction>span{position:absolute;left:calc(50% + 24px);right:0;top:50%;transform:translateY(-50%);font-size:10px;line-height:1.5;color:var(--mute)}.room-nozzle-junction b{display:block;font-weight:500;color:var(--fg);overflow-wrap:anywhere}.room-shared-route .room-unit-below{display:flex;flex-direction:column;align-items:stretch;border-top:0;margin-top:0;padding-top:0;gap:10px}.room-shared-route .room-unit-below>.room-unit-label{order:2}.room-shared-route .room-unit-below>.room-spools{width:100%;margin-top:0}.room-filament-path .route-idle{fill:none;stroke:var(--mute);stroke-opacity:.2;stroke-width:1;stroke-linejoin:round;stroke-linecap:round}

.room-power{display:flex;justify-content:flex-end;margin:-8px 0 14px;color:var(--mute);font-size:15px;font-variant-numeric:tabular-nums}.room-power>span{padding:4px 0}

.room-wall-layout{display:grid;grid-template-columns:36px minmax(0,1fr);gap:14px;align-items:start}.room-wall-viewport{min-width:0}.room-zoom{position:sticky;top:24px;display:flex;flex-direction:column;align-items:center;gap:8px;padding:8px 0;border:1px solid var(--line);border-radius:18px;background:var(--card)}.room-zoom button{display:grid;place-items:center;width:30px;height:30px;padding:0;background:transparent;color:var(--mute);font-size:20px;border-radius:50%}.room-zoom button:hover{background:var(--field);color:var(--fg)}.room-zoom input[type=range]{writing-mode:vertical-lr;direction:rtl;width:24px;height:150px;margin:0;padding:0;accent-color:var(--ok);cursor:pointer}.room-zoom .room-zoom-value{font-size:9px;font-variant-numeric:tabular-nums;height:24px;width:34px;border-radius:8px}.room-wall-layout .room-wall{transform-origin:top left;align-items:stretch}@media(max-width:650px){.room-wall-layout{grid-template-columns:32px minmax(0,1fr);gap:8px}.room-zoom{top:12px}.room-zoom input[type=range]{height:110px}.room-zoom .room-zoom-value{width:30px}}

.room-wall .room-filament-remaining{min-height:96px}

/* Use desktop width for the room instead of the former 1440px canvas. */
@media(min-width:1100px){body>header,main#room-main{max-width:none;width:100%;padding-left:clamp(20px,1.5vw,48px);padding-right:clamp(20px,1.5vw,48px)}.room-heading{margin-bottom:16px}.room-wall-layout{gap:16px}.room-wall{gap:18px}.room-heading h2{font-size:25px}}
@media(min-width:1900px){#printer-detail{max-width:1700px}#printer-detail #slots{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;align-items:start}#printer-detail #slots>.ams{margin-bottom:0}}

.room-nozzle-junction.from-below>[data-nozzle-anchor]{transform:translate(-50%,-50%)}

/* The detail page uses the same live slot-to-nozzle map as the camera wall. */
.printer-filament-map{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px 20px;margin-bottom:18px}
.printer-filament-map h2{font-size:14px;font-weight:600;margin:0 0 12px}
.printer-filament-map .room-shared-route{max-width:760px;margin:0 auto}
.printer-filament-map .room-unit{margin:0;padding:0;border:0}
.printer-filament-map .room-spools{grid-template-columns:repeat(4,minmax(0,1fr));margin-top:8px}
.printer-filament-map .room-nozzle-junction{height:64px}
.printer-filament-map .room-filament-remaining{min-height:62px}
.printer-workspace{margin-bottom:18px}
.printer-workspace>*{min-width:0}
.printer-filament-map .room-unit-label{display:flex;flex-wrap:wrap;justify-content:space-between;gap:4px 12px}
.detail-map-reading{font-variant-numeric:tabular-nums}
.printer-controls .pc-compact-tools{justify-content:flex-start;flex-wrap:wrap;gap:8px 20px;padding-top:9px;padding-bottom:9px}
@media(min-width:1100px){
 .printer-workspace{display:grid;grid-template-columns:minmax(360px,.9fr) minmax(0,1.3fr);gap:14px;align-items:start}
 .printer-filament-column:has(>.printer-filament-map[hidden]):has(>#overview:empty){display:none}
 .printer-workspace:has(>.printer-filament-column>.printer-filament-map[hidden]){grid-template-columns:1fr}
 .printer-filament-map{padding:14px;margin:0 0 10px;border-radius:12px}
 .printer-filament-map h2{margin-bottom:10px}
 .printer-filament-map .room-shared-route{max-width:none}
 .printer-filament-map .room-nozzle-junction{height:52px}
 .printer-filament-map .room-filament-remaining{padding:8px;min-height:56px}
 .printer-filament-map .room-spools{gap:6px}
 .printer-filament-column #overview{gap:8px 14px;padding:0 3px}
 .printer-filament-column .summary-item{gap:5px}
 .printer-filament-column .summary-item strong{font-size:16px}
 .printer-workspace .pc-header{padding:12px 14px;gap:10px}
 .printer-workspace .pc-heading{font-size:17px}
 .printer-workspace .pc-header .pc-quiet{padding:7px 9px;font-size:12px}
 .printer-workspace .pc-job{margin:0 14px;padding:7px 0 11px;gap:12px}
 .printer-workspace .pc-jobtitle{font-size:12px}
 .printer-workspace .pc-progressline{gap:8px;font-size:11px}
 .printer-workspace .pc-temperatures{margin:0 14px;padding:8px 2px}
 .printer-workspace .pc-temp{padding:6px 8px}
 .printer-workspace .pc-temphead{font-size:11px}
 .printer-workspace .pc-tempvalue{font-size:24px;margin:0}
 .printer-workspace .pc-tempfoot{font-size:10px;line-height:1.4}
 .printer-workspace .pc-preheat-shortcuts{padding:10px 14px;gap:6px}
 .printer-workspace .pc-preheat-shortcuts .pc-btn{padding:7px 9px;font-size:11px;gap:2px}
 .printer-workspace .pc-preheat-shortcuts small{font-size:10px}
 .printer-workspace .pc-controls{margin:0 14px;padding:10px 0;gap:10px;grid-template-columns:1fr 1fr}
 .printer-workspace .pc-controls>div{padding:10px;min-width:0}
 .printer-workspace .pc-sectiontitle{margin-bottom:7px;font-size:11px}
 .printer-workspace .pc-speed{display:grid;grid-template-columns:repeat(4,minmax(0,1fr))}
 .printer-workspace .pc-speed button{min-width:0;padding:7px 3px;font-size:10px}
 .printer-workspace .pc-speednote{font-size:10px;margin-top:6px;gap:8px}
 .printer-workspace .pc-lights{gap:5px;flex-wrap:wrap}
 .printer-workspace .pc-light{min-width:0;padding:6px 7px;column-gap:5px;font-size:10px}
 .printer-workspace .pc-lighticon svg{width:15px;height:15px}
 .printer-workspace .pc-lightstate{font-size:9px}
 .printer-workspace .pc-compact-tools{padding:8px 14px;gap:6px 14px}
 .printer-workspace .pc-compact-tools .pc-quiet{font-size:11px;padding:4px 0}
 #printer-detail .section-heading{margin-top:14px;margin-bottom:8px}
 #printer-detail #slots{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;align-items:start}
 #printer-detail #slots>.ams{margin-bottom:0;padding:14px;min-width:0}
 #printer-detail #slots>.ams:only-child{grid-column:1/-1}
 #printer-detail .unit{gap:10px;padding-bottom:10px}
 #printer-detail .unit-actions{gap:6px;flex-wrap:wrap}
 #printer-detail .unit-actions>button{padding:8px;font-size:11px}
 #printer-detail .grid{gap:8px}
 #printer-detail .card{padding:10px;min-width:0}
 #printer-detail .card .name{font-size:15px}
 #printer-detail .card .acts{flex-wrap:wrap;gap:5px}
 #printer-detail .card .acts>button{padding:7px;font-size:10px}
}
@media(min-width:1100px) and (max-width:1499px){#printer-detail #slots .grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:700px){.printer-filament-map{padding:12px 10px;margin-bottom:12px}.printer-filament-map h2{font-size:13px;margin-bottom:8px}.printer-filament-map .room-filament-remaining{min-height:0;padding:6px 5px}.printer-filament-map .room-nozzle-junction{height:48px}.printer-filament-map .room-spools{gap:4px}.printer-filament-map .room-filament-label{flex-wrap:wrap}}

body.room-drying-view main#room-main{max-width:none;width:100%;margin-left:auto;margin-right:auto}body.room-drying-view .room-dry-list{grid-template-columns:1fr}

/* Compact phone layout: four AMS rows fit below the navigation on a large iPhone. */
.room-dry-help summary{cursor:pointer;color:var(--mute);font-size:12px;padding:8px 0}
.room-dry-help{margin-bottom:12px}
.room-dry-help .room-dry-explainer{margin:4px 0 12px}
.room-dry-short-label{display:none}
@media(max-width:650px){
 body.room-drying-view>header{display:grid;grid-template-columns:1fr auto;padding:10px 14px;gap:8px;align-items:center}
 body.room-drying-view header h1{font-size:18px}
 body.room-drying-view .hdr{width:auto;margin:0;gap:5px;flex-wrap:nowrap}
 body.room-drying-view .hdr>button,body.room-drying-view .hdr .menu>button{min-height:44px;padding:8px;font-size:12px}
 body.room-drying-view .hdr #attention-button{max-width:106px}
 body.room-drying-view .room-nav{grid-column:1/-1;order:initial;display:grid;grid-template-columns:1fr 1fr;gap:4px;background:var(--card);border-radius:10px;padding:3px}
 body.room-drying-view .room-nav button{min-height:40px;padding:8px 12px}
 body.room-drying-view main#room-main{padding:14px 12px calc(94px + env(safe-area-inset-bottom,0px))}
 body.room-drying-view .room-heading{margin-bottom:2px;gap:0}
 body.room-drying-view .room-heading h2{font-size:24px;margin-bottom:5px}
 body.room-drying-view .room-heading p{font-size:12px;line-height:1.4}
 body.room-drying-view .room-heading-actions{display:none}
 body.room-drying-view .room-dry-help{margin-bottom:8px}
 body.room-drying-view .room-dry-help summary{min-height:36px;display:flex;align-items:center;gap:6px}
 body.room-drying-view .room-dry-help summary:before{content:'›';font-size:20px}
 body.room-drying-view .room-dry-help[open] summary:before{transform:rotate(90deg)}
 body.room-drying-view .room-dry-help summary::-webkit-details-marker{display:none}
 body.room-drying-view .room-dry-list{gap:8px}
 body.room-drying-view .room-dry-unit{grid-template-columns:minmax(0,1fr) auto;gap:8px 12px;padding:12px;border-radius:12px;align-items:start}
 body.room-drying-view .room-dry-identity{min-width:0}
 body.room-drying-view .room-dry-unit h3{font-size:14px;line-height:20px;margin:0 0 3px;overflow-wrap:anywhere}
 body.room-drying-view .room-dry-unit .room-dry-materials{font-size:11px;line-height:16px;color:var(--mute);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
 body.room-drying-view .room-dry-metrics{grid-column:1/-1;grid-row:2;min-width:0}
 body.room-drying-view .room-unit-title{font-size:12px;line-height:17px;align-items:center;gap:8px}
 body.room-drying-view .room-unit-title>span:first-child{font-weight:600;white-space:nowrap}
 body.room-drying-view .room-dry-state{text-align:right;color:var(--mute)}
 body.room-drying-view [data-dry-state="drying"] .room-dry-state{color:var(--ok)}
 body.room-drying-view [data-dry-state="waiting"] .room-dry-state,body.room-drying-view [data-dry-state="blocked"] .room-dry-state{color:var(--warn)}
 body.room-drying-view .room-dry-unit .bar{height:4px;margin:7px 0 0}
 body.room-drying-view .room-dry-unit .reason{font-size:12px;line-height:1.45;margin-top:8px;overflow-wrap:anywhere}
 body.room-drying-view .room-dry-unit .reason[data-routine="true"]{display:none}
 body.room-drying-view .room-dry-actions{grid-column:2;grid-row:1;align-self:center}
 body.room-drying-view .room-dry-actions button{min-height:44px;padding:8px 12px;font-size:12px;background:var(--field);color:var(--fg)!important;border:1px solid var(--line)}
 body.room-drying-view .room-dry-long-label{display:none}
 body.room-drying-view .room-dry-short-label{display:inline}
 body.room-drying-view .room-followup{padding:12px;margin-bottom:10px;border-color:var(--line)}
 body.room-drying-view .room-followup h3{font-size:13px;line-height:1.4;margin:0}
 body.room-drying-view .room-followup-row{padding:10px 0 0;margin-top:10px;gap:6px}
 body.room-drying-view .room-followup-row[data-followup-status="drying"],body.room-drying-view .room-followup-row[data-followup-status="succeeded"]{display:none}
 body.room-drying-view .room-followup-actions{gap:6px}
 body.room-drying-view .room-followup-actions button{min-height:44px;font-size:12px}
 body.room-drying-view .room-dry-footer{position:fixed;z-index:20;bottom:0;left:0;right:0;margin:0;display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;padding:10px 12px calc(10px + env(safe-area-inset-bottom,0px));background:var(--bg);border-top:1px solid var(--line);box-shadow:0 -8px 24px #0002}
 body.room-drying-view .room-dry-footer button{min-height:46px;padding:10px 16px;font-size:14px;white-space:normal}
 body.room-drying-view .room-dry-footer .stop{background:transparent;border:1px solid #754449;color:var(--bad)}
 body.room-drying-view .room-foot{display:none}
 dialog{box-sizing:border-box;width:calc(100% - 24px);max-width:440px;max-height:calc(100dvh - 32px);overflow-y:auto;padding:18px;border-radius:16px}
 dialog input:not([type=checkbox]):not([type=radio]),dialog select{min-height:46px;font-size:16px;box-sizing:border-box}
 dialog .dialog-actions{position:sticky;bottom:-18px;background:var(--card);padding:12px 0;margin-top:8px}
 dialog .dialog-actions button{min-height:46px;flex:1}
}


/* Phone layout: native size, readable slots, and no desktop zoom rail. */
@media(max-width:700px){
 html{-webkit-text-size-adjust:100%;text-size-adjust:100%}
 body>header,body.room-drying-view>header{display:grid;grid-template-columns:1fr auto;padding:8px 12px;gap:6px;align-items:center}
 header h1,body.room-drying-view header h1{font-size:18px}
 body>header .brand{grid-row:1;grid-column:1}
 body>header .hdr{grid-row:1;grid-column:2}
 body>header .room-nav{grid-row:2}
 .hdr,body.room-drying-view .hdr{width:auto;margin:0;gap:4px;flex-wrap:nowrap}
 .hdr #net,.hdr #refresh{display:none}
 .hdr>button,.hdr .menu>button{min-height:44px;padding:8px;font-size:12px}
 .hdr .menu>button{font-size:0;min-width:44px}
 .room-nav,body.room-drying-view .room-nav{grid-column:1/-1;order:initial;width:100%;display:grid;grid-template-columns:1fr 1fr;gap:4px;background:var(--card);border-radius:10px;padding:3px;box-sizing:border-box}
 .room-nav button{min-height:40px;padding:8px 12px}
 main#room-main{padding:12px 10px 24px;width:100%;box-sizing:border-box}
 .room-heading{gap:6px;margin-bottom:8px;align-items:center}
 .room-heading h2{font-size:19px;letter-spacing:-.4px;margin-bottom:4px}
 .room-heading p{font-size:12px}
 .room-heading-actions{width:auto;margin-left:auto}
 .room-heading-actions button{min-height:40px;padding:6px 8px;font-size:11px}
 #room-plan{display:none}
 .room-power{font-size:12px;margin:0 0 8px}
 .room-wall-layout{display:block}
 .room-zoom{display:none}
 .room-wall-layout .room-wall{zoom:1!important;width:100%!important;grid-template-columns:minmax(0,1fr)!important;gap:10px}
 .room-printer{border-radius:12px}
 .room-printer-name{font-size:15px;padding:8px 12px}
 .room-feed{height:88px;aspect-ratio:auto}
 .room-feed :is(img,canvas.camera-surface){object-fit:contain}
 .room-feed-top{top:6px;left:10px;right:10px}
 .room-camera-note{font-size:9px}
 .room-job{left:10px;right:10px;bottom:8px;gap:8px}
 .room-job strong{font-size:13px}.room-job small{font-size:10px;margin-top:3px}.room-left{font-size:16px}
 .room-cardbody{padding:6px 10px 10px}
 .room-statusline{min-height:32px}.room-status{font-size:11px}.room-statusline button{min-height:32px;font-size:11px}
 .room-live-summary{display:none} /* Each AMS below already displays its drying state. */
 .room-unit{margin-top:4px;padding-top:5px}.room-unit-label{font-size:10px;line-height:1.4}
 .room-spools{grid-template-columns:repeat(4,minmax(0,1fr));gap:4px;margin-top:5px}
 .room-wall .room-filament-remaining{min-height:0;padding:5px;border-radius:6px}
 .room-filament-label{gap:3px;font-size:9px}.room-filament-label b{font-size:10px;line-height:1.2}
 .room-spool{width:12px;height:12px;border-width:2px;flex:none}
 .room-filament-remaining>strong{font-size:10px;margin-top:4px}.room-remaining-bar{height:2px;margin-top:4px}
 .room-shared-route .room-nozzle-junction{height:34px}.room-nozzle-junction>[data-nozzle-anchor]{width:16px;height:22px}
 .room-nozzle-junction>span{font-size:9px;line-height:1.2;left:calc(50% + 15px)}
 .room-shared-route .room-unit-below{gap:4px}.room-foot{font-size:11px;margin-top:12px}.room-foot>span{display:none}
 .room-detailbar{gap:8px;margin-bottom:10px}.room-detailbar span{display:none}.room-detailbar button{min-height:44px;font-size:12px;padding:8px}
 nav#tabs{flex-wrap:nowrap;overflow-x:auto;margin:0 0 10px;padding-bottom:5px;scroll-snap-type:x proximity}
 .tab{min-width:125px;flex:0 0 auto;scroll-snap-align:start;padding:8px 10px;font-size:12px}
 #cam{margin-bottom:12px}#cam .envbox{padding:8px 10px}#cam .presets{width:auto;margin-left:auto}#cam :is(img,canvas.camera-surface){width:100%;max-height:240px;object-fit:contain}
 .printer-current-action{padding:8px 10px;font-size:12px}
 #overview{gap:8px 14px;padding-bottom:10px}.summary-item>span,.summary-item small{font-size:10px}.summary-item strong{font-size:15px}
 .section-heading{margin:16px 0 8px}.section-heading h2{font-size:17px}.section-heading p{display:none}
 .ams{padding:10px;margin-bottom:10px}.unit{grid-template-columns:1fr;gap:8px;padding-bottom:8px}.unit-actions{justify-content:flex-start;gap:8px}
 .unit-actions>button{margin-left:auto;min-height:44px}.unit .title{font-size:15px}.unit-readings{margin-top:3px}
 .grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px}.card{padding:10px}.card .name .sw2{width:18px;height:18px;border-width:3px}
 .card .name{font-size:14px}.card .acts{gap:5px;padding-top:8px}.card .acts>button{min-height:44px;font-size:11px;padding:6px 8px;flex:1}
 .printer-controls .pc-header{padding:12px}.printer-controls .pc-temperatures{grid-template-columns:repeat(2,minmax(0,1fr));margin:0 10px}
 .printer-controls .pc-controls{grid-template-columns:1fr;margin:0 10px;gap:10px}.printer-controls .pc-preheat-shortcuts{padding:8px 10px 12px;gap:6px}
 dialog{margin:auto 0 0;width:100%;max-width:none;max-height:90dvh;box-sizing:border-box;border-radius:18px 18px 0 0;padding:18px 14px calc(16px + env(safe-area-inset-bottom,0px));overscroll-behavior:contain}
 dialog::backdrop{background:#0008}.fchips{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
 .fchip{box-sizing:border-box;min-height:48px;display:flex;align-items:center;padding:10px;font-size:14px;touch-action:manipulation}
 dialog input:not([type=checkbox]):not([type=radio]),dialog select{min-height:48px;font-size:16px;max-width:100%;box-sizing:border-box}
 dialog button{min-height:44px}.spool-save-message{font-size:13px;line-height:1.5;padding:10px 0;color:var(--warn)}
}

/* Reserve the camera area and reveal only once a stream has a decoded frame. */
.room-feed :is(img,canvas.camera-surface).camera-fading,#cam :is(img,canvas.camera-surface).camera-fading{opacity:0;transition:opacity 2000ms cubic-bezier(.22,.61,.36,1)}
.room-feed.camera-ready :is(img,canvas.camera-surface).camera-fading,#cam .camera-ready :is(img,canvas.camera-surface).camera-fading{opacity:1}
.room-cam-empty,.camera-placeholder{background:radial-gradient(ellipse at 45% 40%,#20302f,#0b1012 75%);transition:opacity 450ms ease;pointer-events:none}
.camera-ready .room-cam-empty,.camera-ready .camera-placeholder{opacity:0}
.camera-stage{position:relative;height:clamp(180px,56.25vw,420px);background:#0b0d0f;overflow:hidden}
#cam .camera-stage :is(img,canvas.camera-surface){position:absolute;inset:0;width:100%;height:100%;max-width:none;max-height:none;object-fit:contain;object-position:center;background:transparent}
.camera-placeholder{position:absolute;inset:0;display:grid;place-items:center;color:var(--mute);font-size:12px}
@media(max-width:700px){.camera-stage{height:clamp(180px,56.25vw,240px)}}
/* Wide printer desk: full camera frame, filament route and controls share one row. */
.printer-top>*{min-width:0}
.printer-history{margin-top:12px;border-top:1px solid var(--line)}
.printer-history>summary{display:flex;align-items:center;gap:12px;cursor:pointer;padding:12px 0;font-size:13px;font-weight:600}
.printer-history>summary:before{content:'›';font-size:18px;color:var(--mute)}
.printer-history[open]>summary:before{transform:rotate(90deg)}
.printer-history>summary span{font-size:11px;font-weight:400;color:var(--mute)}
@media(min-width:1100px){
 #printer-detail{max-width:none;width:100%}
 #printer-detail .room-detailbar{margin-bottom:8px}
 #printer-detail #tabs{margin-bottom:10px}
 #printer-detail .unit-readings{flex-wrap:wrap;gap:5px 10px}
 #printer-detail .card .name{margin-bottom:6px}
 #printer-detail .card .meta{line-height:1.45}
 #printer-detail .card .acts{padding-top:8px}
 #printer-detail .grid{margin-top:10px}
 #printer-detail .ams>.legend{margin-top:8px;padding:6px 8px}
 #printer-detail .legend{margin-bottom:8px}
}
@media(min-width:1500px){
 body:has(#printer-detail:not([hidden]))>header{padding-top:12px;padding-bottom:12px}
 body:has(#printer-detail:not([hidden])) main#room-main{padding-top:12px;padding-bottom:12px}
 .printer-top{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,2.05fr);gap:14px;align-items:start;margin-bottom:12px}
 .printer-top #cam{margin:0}
 .printer-top #cam .envbox{padding:9px 12px;gap:8px}
 .printer-top #cam .presets{gap:8px}
 .printer-top #cam .camera-stage{height:clamp(220px,30vh,340px)}
 .printer-top .printer-current-action{padding:8px 12px;font-size:12px}
 .printer-top .printer-workspace{grid-template-columns:minmax(0,.85fr) minmax(0,1.15fr);margin:0}
 .printer-top .printer-filament-map{padding:12px}
 .printer-top .printer-filament-column #overview{display:grid;grid-template-columns:1fr 1fr;gap:6px 12px}
 #printer-detail .section-heading{margin-top:10px;margin-bottom:6px}
 #printer-detail #slots .grid{grid-template-columns:repeat(4,minmax(0,1fr))}
 #printer-detail #slots>.ams{padding:12px}
 #printer-detail #slots .card{padding:9px}
 #printer-detail #slots .card .bar{margin:6px 0}
 #printer-detail #slots .card .name .sw2{width:20px;height:20px;border-width:3px}
 #printer-detail #slots .card summary{padding:5px 0}
}
@media(min-width:1100px) and (max-width:1699px){
 #printer-detail .unit{grid-template-columns:1fr;gap:8px}
 #printer-detail .unit-actions{justify-content:flex-start}
}

@media(prefers-reduced-motion:reduce){.room-feed :is(img,canvas.camera-surface).camera-fading,#cam :is(img,canvas.camera-surface).camera-fading,.room-cam-empty,.camera-placeholder{transition:none}}
/* Short interaction feedback; live telemetry refreshes never trigger entrances. */
@keyframes ui-panel-in{from{opacity:0;transform:translateY(8px) scale(.985)}to{opacity:1;transform:translateY(0) scale(1)}}
@keyframes ui-backdrop-in{from{opacity:0}to{opacity:1}}
@media(prefers-reduced-motion:no-preference){
 button,.tab,.fchip,.menu .dd a{transition:background-color 160ms ease,color 160ms ease,border-color 160ms ease,box-shadow 160ms ease,scale 100ms ease}
 button:not(:disabled):not(.room-feed):active,.fchip:not([aria-disabled=true]):active{scale:.97}
 button.room-feed:active{filter:brightness(.94)}
 .menu.open .dd{animation:ui-panel-in 180ms cubic-bezier(.2,.8,.2,1);transform-origin:top right}
 dialog[open]{animation:ui-panel-in 220ms cubic-bezier(.2,.8,.2,1)}
 dialog[open]::backdrop{animation:ui-backdrop-in 180ms ease-out}
 summary::after{transition:transform 160ms ease,color 160ms ease}
 .pc-switch i{transition:transform 180ms ease,background-color 180ms ease}
}
@media(hover:hover) and (prefers-reduced-motion:no-preference){
 button:not(:disabled):not(.room-feed):hover,.fchip:not([aria-disabled=true]):hover{filter:brightness(1.08)}
 .room-printer{transition:border-color 180ms ease,box-shadow 180ms ease}
 .room-printer:hover{box-shadow:0 6px 20px #0002}
}
@media(prefers-reduced-motion:reduce){
 button,.tab,.fchip,.sw .track,.sw .track::after,.pc-switch i{transition:none!important}
 dialog[open],dialog[open]::backdrop,.menu.open .dd{animation:none!important}
}
#printer-detail #act:empty{display:none}
.desk-operation{padding:10px 12px;border:1px solid var(--line);border-radius:8px;color:var(--ok);font-size:12px}
.desk-operation summary{cursor:pointer}
.desk-operation time{float:right;margin-left:8px;font-size:10px;color:var(--mute)}
.desk-operation p{font-size:11px;color:var(--mute);margin:8px 0 0}
/* Klipper-style task panels; one interactive representation of every AMS. */
.printer-dashboard{display:grid;grid-template-columns:minmax(0,1fr);gap:14px;align-items:start}
.printer-dashboard>*{min-width:0}
.printer-monitor-column,.printer-control-column>.printer-controls{display:flex;flex-direction:column;gap:14px}
.desk-panel,.printer-materials{background:var(--card);border:1px solid var(--line);border-radius:10px;min-width:0}
.desk-panel-heading{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:11px 14px;border-bottom:1px solid var(--line);min-height:43px}
.desk-panel-heading h2{margin:0;font-size:14px;font-weight:600;letter-spacing:0}
.desk-panel-heading>span{font-size:11px;color:var(--mute);text-align:right}
.desk-panel-heading .pc-quiet{padding:0;font-size:11px}
#printer-detail #cam{border-radius:10px;margin:0}
#printer-detail #cam .envbox{padding:10px 12px;gap:8px;min-height:43px}
#printer-detail #cam .camera-stage{height:clamp(220px,34vh,440px);min-height:160px}
#printer-detail #cam .presets{gap:8px}
#printer-detail .printer-current-action{padding:8px 12px;font-size:12px}
#printer-job .pc-job{margin:0;padding:14px;display:flex;flex-wrap:wrap;gap:12px;border:0}
#printer-job .pc-job>div:first-child{width:100%;min-width:0}
#printer-job .pc-jobtitle{font-size:14px}
#printer-job .pc-progressline{flex-wrap:wrap;font-size:12px;gap:8px}
#printer-job .pc-progress{max-width:none;min-width:60px}
#printer-job .pc-actions{margin-left:auto}
.desk-idle{padding:14px;color:var(--mute);font-size:13px}
#printer-detail #overview{display:grid;grid-template-columns:1fr 1fr;gap:9px;padding:0;margin:0}
#printer-detail #overview .summary-item{padding:9px 12px;border:1px solid var(--line);border-radius:8px;gap:6px;justify-content:space-between;background:var(--card)}
#printer-detail #overview .summary-item strong{font-size:16px}
.printer-dashboard .printer-history{margin-top:0}
.printer-dashboard .printer-history>summary{font-size:13px}
.printer-dashboard .printer-history>summary span{display:none}
.printer-dashboard .event-row{font-size:11px;grid-template-columns:auto 1fr;gap:4px 10px;padding:9px}
.printer-dashboard .event-message{grid-column:1/-1}
.desk-thermal-chart{padding:10px 14px 4px}
.desk-thermal-chart svg{display:block;width:100%;height:auto;max-height:110px}
.desk-thermal-chart>div{display:flex;justify-content:space-between;font-size:10px;color:var(--mute);margin:4px 0 6px}
.desk-thermal-head,.printer-dashboard .desk-thermal-row{display:grid;grid-template-columns:minmax(0,1fr) 76px 90px;align-items:center;gap:8px}
.desk-thermal-head{padding:7px 14px;font-size:10px;color:var(--mute);background:var(--bg)}
.desk-thermal-head>span:not(:first-child){text-align:right}
.printer-dashboard .desk-thermals{padding:0 14px 7px}
.printer-dashboard .desk-thermal-row{border-bottom:1px solid var(--line);padding:6px 0;position:relative}
.printer-dashboard .desk-thermal-row:last-child{border-bottom:0}
.desk-sensor{display:flex;align-items:center;gap:7px;font-size:12px;flex-wrap:wrap}
.desk-sensor>i{height:7px;width:7px;border-radius:50%;flex:none}
.desk-sensor small{font-size:9px;color:var(--mute)}
.desk-thermal-row>strong{text-align:right;font-size:15px;font-weight:550;font-variant-numeric:tabular-nums}
.desk-thermal-row>strong>small{font-size:10px;font-weight:400;color:var(--mute)}
.desk-target{position:relative;text-align:right;font-size:12px}
.desk-target>span{color:var(--mute);padding:5px 10px;display:inline-block}
.desk-target>small{display:block;font-size:10px;color:var(--warn)}
.printer-dashboard .desk-target .pc-btn{padding:5px 7px;font-size:12px;min-width:65px;justify-content:space-between}
.printer-dashboard .desk-target .pc-btn svg{width:12px;height:12px}
.printer-dashboard .desk-target form{display:flex;justify-content:flex-end;align-items:center;gap:3px}
.printer-dashboard .desk-target input{width:55px;min-width:0;padding:5px 3px;text-align:right;font:inherit;font-variant-numeric:tabular-nums;color:var(--fg);background:var(--field);border:1px solid var(--line);border-radius:6px}
.printer-dashboard .desk-target input:focus{outline:2px solid var(--blue);outline-offset:1px}
.printer-dashboard .desk-target .pc-btn{min-width:0;padding:5px 6px}
.desk-fan-list{margin:0;padding:4px 14px 8px}
.desk-fan-row{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid var(--line)}
.desk-fan-row:last-child{border-bottom:0}
.desk-fan-row dt{font-size:12px;color:var(--mute)}
.desk-fan-row dd{margin:0;font-size:15px;font-weight:550;font-variant-numeric:tabular-nums;white-space:nowrap}
.desk-fan-row dd[data-off="true"]{color:var(--mute)}
.printer-dashboard .desk-target .pc-temp-popover{left:auto;right:0;top:calc(100% + 8px);text-align:left;width:min(340px,calc(100vw - 48px))}
.printer-dashboard .pc-preheat-shortcuts{padding:12px;gap:7px;display:grid;grid-template-columns:repeat(2,minmax(0,1fr))}
.printer-dashboard .pc-preheat-shortcuts>span{display:none}
.printer-dashboard .pc-preheat-shortcuts .pc-btn{padding:9px 10px;font-size:12px;min-width:0}
.printer-dashboard .pc-preheat-shortcuts .pc-btn small{font-size:10px;text-align:left;line-height:1.4}
.pc-preheat-preset.is-active{border-color:#e8aa76;background:#182b31;box-shadow:inset 0 0 0 1px #e8aa7630,0 0 18px #ed855b16}
.pc-preheat-preset.is-active,.desk-panel.heaters-on{position:relative;overflow:hidden;isolation:isolate}
.pc-preheat-preset.is-active::before,.pc-preheat-preset.is-active::after,.desk-panel.heaters-on::before{content:'';position:absolute;inset:-55% -35%;pointer-events:none;z-index:0;filter:blur(18px) saturate(1.3);will-change:transform}
.pc-preheat-preset.is-active::before,.desk-panel.heaters-on::before{background:radial-gradient(ellipse 38% 38% at 20% 68%,#53aab36b 0%,#2772863d 58%,transparent 100%),radial-gradient(ellipse 34% 44% at 52% 80%,#f2b15f7a 0%,#d76b4a54 55%,transparent 100%),radial-gradient(ellipse 36% 42% at 80% 42%,#ec795c70 0%,#a94a6560 60%,transparent 100%);animation:pc-thermal-drift 18s ease-in-out infinite}
.pc-preheat-preset.is-active::after{background:radial-gradient(ellipse 26% 37% at 30% 40%,#e2646880,transparent 100%),radial-gradient(ellipse 28% 35% at 70% 70%,#f5cb7770,transparent 100%);mix-blend-mode:screen;opacity:.58;animation:pc-thermal-crossflow 24s ease-in-out infinite}
.desk-panel.heaters-on::before{opacity:.3;filter:blur(32px) saturate(1.15);animation-duration:26s}
.pc-preheat-preset.is-active>*,.desk-panel.heaters-on>*{position:relative;z-index:1}
.printer-materials .ams.is-heating{position:relative;overflow:hidden;isolation:isolate}
.printer-materials .ams.is-heating::before{content:'';position:absolute;inset:-55% -35%;pointer-events:none;z-index:0;filter:blur(32px) saturate(1.15);opacity:.3;will-change:transform;background:radial-gradient(ellipse 38% 38% at 20% 68%,#53aab36b 0%,#2772863d 58%,transparent 100%),radial-gradient(ellipse 34% 44% at 52% 80%,#f2b15f7a 0%,#d76b4a54 55%,transparent 100%),radial-gradient(ellipse 36% 42% at 80% 42%,#ec795c70 0%,#a94a6560 60%,transparent 100%);animation:pc-thermal-drift 26s ease-in-out infinite}
.printer-materials .ams.is-heating>*{position:relative;z-index:1}
@keyframes pc-thermal-drift{0%,100%{transform:translate(-9%,7%) rotate(-8deg) scale(1)}25%{transform:translate(5%,-7%) rotate(6deg) scale(1.15)}50%{transform:translate(10%,5%) rotate(-4deg) scale(1.05)}75%{transform:translate(-5%,-5%) rotate(4deg) scale(1.1)}}
@keyframes pc-thermal-crossflow{0%,100%{transform:translate(14%,-7%) rotate(9deg) scale(.95)}25%{transform:translate(-7%,8%) rotate(-7deg) scale(1.12)}50%{transform:translate(-12%,-3%) rotate(5deg) scale(1)}75%{transform:translate(5%,10%) rotate(-3deg) scale(1.08)}}
.pc-preheat-title{display:flex;align-items:center;gap:7px}
.pc-preheat-preset .pc-heat-state{color:var(--mute)}
.pc-preheat-preset.is-active .pc-heat-state{color:#ffe0ad}
.desk-thermal-row.is-warming .desk-sensor>i{box-shadow:0 0 0 3px #eab56730}
@media(prefers-reduced-motion:reduce){.pc-preheat-preset.is-active::before,.pc-preheat-preset.is-active::after,.desk-panel.heaters-on::before,.printer-materials .ams.is-heating::before{animation:none;transform:none}}
.printer-dashboard .pc-controls{display:block;border:0;margin:0;padding:12px}
.printer-dashboard .pc-controls>div{background:transparent;border:0;border-radius:0;padding:0}
.printer-dashboard .pc-controls>div+div{margin-top:13px}
.printer-dashboard .pc-sectiontitle{font-size:11px;margin-bottom:7px}
.printer-dashboard .pc-speed button{font-size:11px;padding:7px 2px}
.printer-dashboard .pc-speednote{font-size:10px;margin-top:5px}
.printer-dashboard .pc-lights{display:flex;flex-wrap:wrap;gap:7px}
.printer-dashboard .pc-light{min-width:0;flex:1;padding:7px;font-size:11px}
.printer-dashboard .pc-lightstate{font-size:9px}
.printer-dashboard .pc-compact-tools{display:flex;justify-content:flex-start;flex-wrap:wrap;gap:6px 13px;padding:8px 12px;border-radius:0 0 10px 10px;background:transparent}
.printer-dashboard .pc-compact-tools .pc-quiet{font-size:11px;padding:4px 0;gap:4px}
.printer-dashboard .pc-editor{padding:14px}
.printer-dashboard .pc-machinebody,.printer-dashboard .pc-service{grid-template-columns:1fr;gap:14px}
#printer-detail .printer-materials #slots{display:block;padding:12px;position:relative}
#printer-detail .printer-materials #slots>.ams{display:flex;flex-direction:column;padding:0;margin:0;border:0;border-radius:0;background:transparent;gap:0;position:relative}
#printer-detail .printer-materials .unit{display:flex;flex-wrap:wrap;justify-content:space-between;gap:9px;padding:0 0 10px;border:0}
#printer-detail .printer-materials .unit .title{font-size:13px}
#printer-detail .printer-materials .unit-readings{display:flex;flex-wrap:wrap;gap:5px 8px;margin-top:5px}
#printer-detail .printer-materials .unit-actions{justify-content:flex-start;gap:8px;flex-wrap:wrap}
#printer-detail .printer-materials .unit-actions>button{font-size:11px;padding:7px 9px}
#printer-detail .printer-materials .unit-actions .sw{font-size:10px}
#printer-detail .printer-materials .ams>.legend{font-size:10px;padding:6px 8px;margin:0 0 8px;line-height:1.4}
#printer-detail .printer-materials .grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));margin:0;gap:6px}
#printer-detail .printer-materials .card{padding:9px 7px;border-radius:7px;min-height:0}
#printer-detail .printer-materials .card.is-loaded{border-color:var(--ok)}
#printer-detail .printer-materials .card .slot{font-size:9px;min-height:18px;margin-bottom:5px;gap:3px}
#printer-detail .printer-materials .card .loaded{font-size:8px;padding:1px 3px}
#printer-detail .printer-materials .card .name{font-size:12px;margin-bottom:7px;gap:5px!important;line-height:1.3}
#printer-detail .printer-materials .card .name .sw2{width:16px;height:16px;border-width:3px}
#printer-detail .printer-materials .card .rem{font-size:10px;line-height:1.4;margin-bottom:7px;flex-wrap:wrap;gap:4px}
#printer-detail .printer-materials .card .rembar{height:3px;min-width:30px}
#printer-detail .printer-materials .card .verdict{font-size:10px;line-height:1.4}
#printer-detail .printer-materials .card .bar{margin:5px 0;height:3px}
#printer-detail .printer-materials .card .meta{font-size:10px;line-height:1.4}
#printer-detail .printer-materials .card .acts{display:flex;flex-wrap:wrap;padding-top:8px;gap:5px}
#printer-detail .printer-materials .card .acts>button{padding:6px;font-size:10px;flex:1;line-height:1.3}
#printer-detail .printer-materials .card summary{font-size:10px;padding:5px 0}
#printer-detail .printer-materials .room-nozzle-junction{height:56px}
#printer-detail .printer-materials .ams-below>.grid{order:0}
#printer-detail .printer-materials .ams-below>.unit{order:1;padding:10px 0 0}
#printer-detail .printer-materials .ams-below>.legend{order:2;margin:8px 0 0}
@media(min-width:900px){
 #printer-detail .printer-dashboard{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}
 .printer-materials{grid-column:1/-1}
 #printer-detail .printer-materials .card{padding:12px}
}
@media(min-width:1250px){
 #printer-detail .printer-dashboard{grid-template-columns:minmax(0,.95fr) minmax(0,.85fr) minmax(0,1.35fr);gap:14px}
 .printer-materials{grid-column:auto}
 #printer-detail .printer-materials .card{padding:9px 7px}
 #printer-detail{max-width:none;width:100%}
 body:has(#printer-detail:not([hidden]))>header{padding-top:12px;padding-bottom:12px}
 body:has(#printer-detail:not([hidden])) main#room-main{padding-top:12px;padding-bottom:16px}
 #printer-detail .room-detailbar{margin-bottom:8px}
 #printer-detail #tabs{margin-bottom:12px}
}
@media(max-width:700px){
 #printer-detail #cam .camera-stage{height:clamp(180px,56.25vw,260px)}
 .printer-dashboard{gap:12px}
 .printer-monitor-column,.printer-control-column>.printer-controls{gap:12px}
 #printer-detail .printer-materials #slots{padding:10px}
 #printer-detail .printer-materials .card .name{flex-wrap:wrap}
 #printer-detail .printer-materials .card .acts>button{min-height:40px}
 #printer-detail .printer-materials .card .loaded{display:none}
 .desk-panel-heading{padding:10px 12px}
 .printer-dashboard .desk-target .pc-btn{min-height:38px}
 #printer-detail .printer-materials .unit-actions>button{min-height:40px}
}

/* Adjustable printer-detail widgets. The saved size is a column span, like a widget grid. */
.detail-layout-actions{display:flex;gap:8px;align-items:center;margin-left:auto}
#printer-detail .printer-dashboard{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));grid-auto-rows:8px;gap:14px;align-items:start}
#printer-detail .printer-dashboard>.dashboard-widget{grid-column:span 4;grid-row-end:span 1;min-width:0;position:relative;align-self:start}
#printer-detail .printer-dashboard>.dashboard-widget[data-size="medium"]{grid-column:span 8}
#printer-detail .printer-dashboard>.dashboard-widget[data-size="large"]{grid-column:span 12}
#printer-detail .printer-dashboard .printer-materials{grid-column:auto}
#printer-detail .printer-dashboard .widget-control>.desk-panel{margin:0}
#printer-detail .printer-dashboard .dashboard-widget[data-widget="filament"][data-size="small"] .grid{grid-template-columns:repeat(2,minmax(0,1fr))}
#printer-detail .printer-dashboard .dashboard-widget[data-widget="camera"][data-size="large"] .camera-stage{height:clamp(280px,50vh,620px)}
#printer-detail.layout-editing .dashboard-widget{outline:1px dashed var(--line);outline-offset:5px;border-radius:8px}
#printer-detail .dashboard-widget.layout-dragging{z-index:20;opacity:.83;filter:drop-shadow(0 10px 20px #0008)}
#printer-detail .dashboard-widget.layout-drop-target{outline:2px solid var(--ok);outline-offset:5px}
@media(max-width:1249px){#printer-detail .printer-dashboard>.dashboard-widget{grid-column:span 6}#printer-detail .printer-dashboard>.dashboard-widget[data-size="medium"],#printer-detail .printer-dashboard>.dashboard-widget[data-size="large"]{grid-column:span 12}}
@media(max-width:899px){#printer-detail .printer-dashboard>.dashboard-widget,#printer-detail .printer-dashboard>.dashboard-widget[data-size="medium"],#printer-detail .printer-dashboard>.dashboard-widget[data-size="large"]{grid-column:span 12}#printer-detail .printer-dashboard .dashboard-widget[data-widget="filament"][data-size="small"] .grid{grid-template-columns:repeat(4,minmax(0,1fr))}}
@media(max-width:650px){#printer-detail .room-detailbar{flex-wrap:wrap}.detail-layout-actions{width:100%;justify-content:space-between}.detail-layout-actions button{min-height:38px}#printer-detail .printer-dashboard{gap:12px}.layout-controls{gap:5px}.layout-controls strong{font-size:11px}}

/* Edge resizing stays on the persistent widget shell, outside live content. */
.widget-content{min-width:0;position:relative}
.layout-edge{display:none;position:absolute;z-index:8;background:transparent!important;border:0!important;border-radius:5px;padding:0;opacity:0;touch-action:none;user-select:none;min-height:0;min-width:0;transition:opacity .15s}
#printer-detail.layout-editing .layout-edge{display:block}
.layout-edge:after{content:'';position:absolute;background:var(--ok);border-radius:4px;opacity:.7}
.layout-edge-n,.layout-edge-s{height:10px;left:12px;right:12px;width:calc(100% - 24px);cursor:ns-resize}
.layout-edge-n{top:-5px}.layout-edge-s{bottom:-5px}
.layout-edge-n:after,.layout-edge-s:after{width:34px;height:3px;top:4px;left:calc(50% - 17px)}
.layout-edge-e,.layout-edge-w{width:10px;top:12px;bottom:12px;height:calc(100% - 24px);cursor:ew-resize}
.layout-edge-e{right:-5px}.layout-edge-w{left:-5px}
.layout-edge-e:after,.layout-edge-w:after{height:34px;width:3px;left:4px;top:calc(50% - 17px)}
.layout-edge-se{width:16px;height:16px;right:-5px;bottom:-5px;cursor:nwse-resize}
.layout-edge-se:after{width:7px;height:7px;right:4px;bottom:4px;background:none;border-right:2px solid var(--ok);border-bottom:2px solid var(--ok)}
#printer-detail.layout-editing .dashboard-widget:hover>.layout-edge,#printer-detail.layout-editing .layout-edge:focus-visible,#printer-detail.layout-editing .layout-edge{opacity:1}
.layout-resizing{outline:1px solid var(--ok);outline-offset:2px;border-radius:10px;z-index:15}
.layout-dimensions{position:absolute;right:12px;bottom:12px;z-index:9;background:var(--field);border:1px solid var(--line);padding:5px 8px;border-radius:6px;font-size:11px;color:var(--fg);pointer-events:none}
body.layout-resize-active{user-select:none}
@media(min-width:900px){
 #printer-detail .printer-dashboard{grid-template-columns:repeat(48,minmax(0,1fr));grid-auto-flow:row dense}
 #printer-detail .printer-dashboard>.dashboard-widget{grid-column:auto / span var(--widget-span,16)}
 #printer-detail .printer-dashboard>.dashboard-widget[data-size="medium"]{grid-column:auto / span var(--widget-span,32)}
 #printer-detail .printer-dashboard>.dashboard-widget[data-size="large"]{grid-column:auto / span var(--widget-span,48)}
 .dashboard-widget[data-height]>.widget-content{height:var(--widget-height);overflow:auto;scrollbar-width:thin;border-radius:10px}
 #printer-detail .dashboard-widget[data-widget="camera"][data-height]>.widget-content{overflow:hidden}
 #printer-detail .dashboard-widget[data-widget="camera"][data-height] #cam{height:100%;display:flex;flex-direction:column;box-sizing:border-box}
 #printer-detail .dashboard-widget[data-widget="camera"][data-height] #cam .camera-stage{flex:1 1 auto;height:auto;min-height:80px}
 #printer-detail .dashboard-widget[data-widget="camera"][data-height] #cam>.envbox,
 #printer-detail .dashboard-widget[data-widget="camera"][data-height] #cam>.printer-current-action{flex:none}
}
@media(min-width:900px) and (max-width:1249px){
 #printer-detail .printer-dashboard>.dashboard-widget{grid-column:auto / span var(--widget-span,24)}
 #printer-detail .printer-dashboard>.dashboard-widget[data-size="medium"],#printer-detail .printer-dashboard>.dashboard-widget[data-size="large"]{grid-column:auto / span var(--widget-span,48)}
}
@media(max-width:899px){.layout-edge{display:none}.layout-dimensions{display:none}}

.printer-dashboard .desk-speed-only{padding:8px}.printer-dashboard .desk-speed-only .pc-speed button{min-height:38px}

#cam #printer-job{flex:none;border-top:1px solid var(--line)}
#cam #printer-job .pc-job{padding:10px 12px;gap:8px;align-items:center}
#cam #printer-job .pc-job>div:first-child{width:auto;flex:1 1 260px}
#cam #printer-job .pc-actions{flex:none;margin-left:auto}
#cam #printer-job .pc-jobtitle{font-size:13px}
#cam #printer-job .pc-progressline{font-size:11px}
#cam #printer-job[hidden]{display:none}
#printer-detail .dashboard-widget[data-widget="camera"][data-height]>.widget-content{overflow:auto}

#printer-detail .printer-dashboard{position:relative}
.layout-placement{position:absolute;box-sizing:border-box;pointer-events:none;z-index:19;border:2px dashed var(--ok);background:#aee6cc12;border-radius:10px;color:var(--ok);padding:10px;font-size:12px}
.layout-placement.invalid{border-color:var(--bad);color:var(--bad);background:#fa879112}
@media(min-width:900px){
 #printer-detail .printer-dashboard>.dashboard-widget[data-column]{grid-column-start:var(--widget-column)!important;grid-row-start:var(--widget-row)!important}
 #printer-detail.layout-editing .printer-dashboard{padding-bottom:240px;min-height:65vh}
}

.printer-dashboard .desk-activity{display:flex;flex-direction:column;height:320px;box-sizing:border-box;overflow:hidden;margin:0}
.printer-dashboard .desk-activity>.desk-panel-heading{flex:none}
.printer-dashboard .desk-activity #events{flex:1;min-height:0;max-height:none;margin:0;border:0;border-radius:0;padding:0;overflow:auto;scrollbar-width:thin}
.printer-dashboard .desk-activity .event-row{padding:10px 14px}
@media(min-width:900px){
 #printer-detail .dashboard-widget[data-widget="activity"][data-height]>.widget-content{overflow:hidden}
 #printer-detail .dashboard-widget[data-widget="activity"][data-height] .desk-activity{height:100%}
}

#printer-detail.layout-editing .dashboard-widget>.widget-content{cursor:grab;touch-action:none;user-select:none}
#printer-detail.layout-editing .dashboard-widget>.widget-content *{cursor:inherit}
#printer-detail.layout-editing .dashboard-widget.layout-dragging>.widget-content{cursor:grabbing}
#printer-detail.layout-editing .dashboard-widget:focus-visible{outline:2px solid var(--ok);outline-offset:3px}

.layout-guides{position:absolute;left:0;top:0;pointer-events:none;z-index:30;overflow:visible}
.layout-guides line{stroke:var(--ok);stroke-width:1;stroke-dasharray:4 3;vector-effect:non-scaling-stroke;opacity:.85}
.layout-guides line.spacing{stroke-dasharray:none}
.layout-guides text{fill:var(--ok);font:10px ui-monospace,monospace;paint-order:stroke;stroke:var(--bg);stroke-width:4px;stroke-linejoin:round}

/* Dismissal hides presentation only; printer faults remain in Alerts. */
.alert-close{display:inline-grid;place-items:center;width:44px;min-width:44px;height:44px;padding:0!important;font-size:24px!important;line-height:1;background:transparent;color:var(--mute);flex:none;touch-action:manipulation}
#attention-panel .attention-nav .alert-close{font-size:24px;min-height:44px}
.room-issue .alert-close{margin-left:auto}
.room-printer.has-printer-error{position:relative;isolation:isolate;border-color:#ff5965}
.room-printer.has-printer-error::after{content:'';position:absolute;inset:0;z-index:5;border-radius:inherit;pointer-events:none;box-shadow:inset 0 0 0 2px #ff5965,0 0 0 2px #ff596580;background:#e8213810;animation:printer-error-flash 1.8s ease-in-out infinite}
@keyframes printer-error-flash{0%,100%{opacity:.32}50%{opacity:1;box-shadow:inset 0 0 0 3px #ff5965,0 0 0 4px #ff596588,0 0 30px #ff344b88;background:#e8213828}}
@media(prefers-reduced-motion:reduce){.room-printer.has-printer-error::after{animation:none;opacity:1;box-shadow:inset 0 0 0 3px #ff5965,0 0 0 3px #ff596566}}
.actbox .title{display:flex;align-items:center;gap:10px}.actbox .title .alert-close{margin-left:auto}
@media(max-width:700px){
 #room-home .room-heading{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:6px;margin-bottom:6px}
 #room-home .room-heading h2{font-size:18px}.room-heading-actions{margin:0;width:auto}
 #room-home .room-power{margin:0 0 10px;min-height:18px}
 #room-wall .room-feed{display:grid;grid-template-columns:62% minmax(0,1fr);height:clamp(120px,38vw,180px);aspect-ratio:auto;background:var(--bg)}
 #room-wall .room-feed :is(img,canvas.camera-surface){inset:0 auto 0 0;width:62%;height:100%;object-fit:contain;object-position:center}
 #room-wall .room-feed:after{display:none}
 #room-wall .room-cam-empty{inset:0 38% 0 0;padding:26px 10px;font-size:12px;line-height:1.4}
 #room-wall .room-feed-top{left:6px;right:calc(38% + 6px);top:5px;justify-content:flex-end;gap:0}
 #room-wall .room-feed-top>span:first-child{display:none}
 #room-wall .room-camera-note{font-size:9px;padding:3px 5px;border-radius:4px;background:#080d0eb3;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
 #room-wall .room-job{position:static;grid-column:2;display:flex;flex-direction:column;justify-content:center;align-items:stretch;padding:10px;gap:10px;min-width:0;height:100%;border-left:1px solid var(--line)}
 #room-wall .room-job strong{font-size:13px;white-space:normal;overflow-wrap:anywhere;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
 #room-wall .room-job small{font-size:11px;line-height:1.4;margin-top:4px;white-space:normal}
 #room-wall .room-left{font-size:17px;text-align:left;white-space:normal}
 #room-wall .room-feed:not(.is-printing) .room-job>div:first-child{display:none}
 #room-wall .room-statusline{gap:8px;min-height:40px}.room-statusline .room-link{min-height:40px}
 #room-wall .room-filament-label{font-size:10px;gap:4px;align-items:flex-start}
 #room-wall .room-filament-label b{font-size:11px;line-height:1.3}
 #room-wall .room-filament-remaining>strong{font-size:11px}
 #room-wall .room-filament-remaining{padding:7px 5px}
 #room-wall .room-unit-label{font-size:11px}
 #room-wall .room-spools.has-single{grid-template-columns:minmax(0,1fr)}
 #room-wall .room-spools.has-single .room-filament-remaining{display:grid;grid-template-columns:1fr auto;align-items:center;column-gap:10px}
 #room-wall .room-spools.has-single .room-filament-remaining>strong{margin:0}
 #room-wall .room-spools.has-single .room-remaining-bar{grid-column:1/-1}
 #room-wall .room-slot-setup{min-height:44px;font-size:11px;line-height:1.3}
 #room-wall .room-issue{display:grid;grid-template-columns:minmax(0,1fr) 44px;gap:0 8px;padding:8px 10px;align-items:start;font-size:12px;line-height:1.45}
 #room-wall .room-issue>span{grid-column:1;grid-row:1}
 #room-wall .room-issue>.alert-close{grid-column:2;grid-row:1}
 #room-wall .room-issue>.room-link{grid-column:1;grid-row:2;justify-self:start;min-height:40px;font-size:12px}
 #attention-panel{padding:10px 12px;margin-bottom:12px}
 #attention-panel .attention-top{align-items:center;gap:4px}
 #attention-panel .attention-nav{margin-left:auto;gap:0}
 #attention-panel .attention-nav button{min-height:44px;padding:6px}
 #attention-panel .attention-detail{align-items:flex-start;gap:6px}
 #attention-panel .attention-detail button{min-height:44px;margin-left:0}
 #attention-panel .attention-detail>span{flex:1 1 100%}
 #attention-panel .attention-detail a{display:inline-flex;align-items:center;min-height:44px}
 #printer-detail .printer-dashboard>.dashboard-widget{min-width:0;grid-column:1/-1!important;grid-row:auto!important}
 #printer-detail .printer-dashboard .dashboard-widget[data-widget="filament"] .grid{grid-template-columns:repeat(2,minmax(0,1fr))}
 #layout-edit,#layout-reset{display:none!important}
 #printer-detail .detail-layout-actions{width:auto;margin-left:auto}
}

/* A phone is a flowing page, independent of the saved desktop grid. */
@media(max-width:899px){
 #printer-detail .printer-dashboard{display:flex;flex-direction:column;align-items:stretch;gap:12px;padding-bottom:0;min-height:0}
 #printer-detail .printer-dashboard>.dashboard-widget{flex:none;width:100%;min-width:0;height:auto;min-height:0;max-height:none;grid-area:auto!important;align-self:stretch;margin:0;transform:none}
 #printer-detail .printer-dashboard>.dashboard-widget>.widget-content{height:auto!important;min-height:0;max-height:none;overflow:visible!important;touch-action:auto;user-select:auto;cursor:auto}
 #printer-detail.layout-editing .dashboard-widget{outline:none}
 #printer-detail.layout-editing .dashboard-widget>.widget-content *{cursor:auto}
 #printer-detail .dashboard-widget[data-widget="camera"]{order:0}
 #printer-detail .dashboard-widget[data-widget="temperatures"]{order:1}
 #printer-detail .dashboard-widget[data-widget="fans"]{order:2}
 #printer-detail .dashboard-widget[data-widget="quick"]{order:3}
 #printer-detail .dashboard-widget[data-widget="machine"]{order:4}
 #printer-detail .dashboard-widget[data-widget="filament"]{order:5}
 #printer-detail .dashboard-widget[data-widget="activity"]{order:6}
 #printer-detail .printer-dashboard .dashboard-widget[data-widget="camera"] #cam .camera-stage{height:clamp(180px,56.25vw,420px);min-height:0}
 #printer-detail .desk-activity{height:280px;max-height:45vh}
 #printer-detail .layout-edge,#printer-detail .layout-dimensions,#printer-detail .layout-guides,#printer-detail .layout-placement,#layout-edit,#layout-reset{display:none!important}
}

.slot-colour-name{font-size:11px;color:var(--mute);font-weight:400;line-height:1.3}
.slot-colour-chip{display:inline-flex;align-items:center;gap:5px;flex:none}
.card .name{flex-wrap:wrap}
.room-filament-label>span:last-child{min-width:0;overflow-wrap:anywhere}
.room-feed img.camera-buffer-anchor,#cam img.camera-buffer-anchor{display:none!important}
.room-printer-name{display:flex;align-items:center;justify-content:space-between;gap:8px}
.camera-expand{display:inline-flex;align-items:center;justify-content:center;gap:6px;min-height:36px;padding:6px 10px;font-size:12px;flex:none}
.camera-expand svg{width:16px;height:16px}
.room-printer-actions{display:flex;align-items:center;gap:7px;flex:none}
.room-movement-launch{min-height:36px;padding:6px 10px;border:1px solid var(--ok);color:var(--ok);background:transparent;font-size:12px;white-space:nowrap}
/* Eight cards share the width, while each card ends after its own AMS units. */
#room-wall.is-eight-wide{align-items:start}
#room-wall.is-eight-wide .room-printer{align-self:start}
#room-wall.is-eight-wide .room-printer-name{min-width:0;padding:10px 12px;font-size:15px}
#room-wall.is-eight-wide .room-printer-name>span:first-child{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#room-wall.is-eight-wide .room-printer-actions{gap:4px}
#room-wall.is-eight-wide .room-printer-actions button{padding:5px 7px;font-size:10px;min-height:30px}
#room-wall.is-eight-wide .room-cardbody{padding:9px 10px 11px}
#room-wall.is-eight-wide .room-statusline{min-height:30px;gap:5px}
#room-wall.is-eight-wide .room-statusline :is(.room-status,.room-link){font-size:10px}
#room-wall.is-eight-wide .room-unit{margin-top:7px;padding-top:7px}
#room-wall.is-eight-wide .room-unit-label{font-size:10px;line-height:1.3}
#room-wall.is-eight-wide .room-spools{gap:4px;margin-top:6px}
#room-wall.is-eight-wide .room-filament-remaining{display:grid;grid-template-rows:38px 24px 14px;row-gap:3px;height:94px;min-height:94px;padding:6px 5px;box-sizing:border-box;overflow:hidden}
#room-wall.is-eight-wide .room-filament-label{display:grid;grid-template-columns:12px minmax(0,1fr);align-items:start;gap:3px;font-size:9px;line-height:1.2}
#room-wall.is-eight-wide .room-slot-copy{display:grid;grid-template-rows:14px 24px;min-width:0;overflow:hidden}
#room-wall.is-eight-wide .room-slot-top{display:block;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#room-wall.is-eight-wide .room-filament-label b{display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:2;overflow:hidden;font-size:10px;line-height:1.2}
#room-wall.is-eight-wide .room-spool{width:12px;height:12px;border-width:2px;box-sizing:border-box}
#room-wall.is-eight-wide .room-filament-remaining>strong{font-size:10px;line-height:1.2;margin:0;align-self:end;max-height:24px;overflow:hidden}
#room-wall.is-eight-wide .room-remaining-bar{height:3px;margin:0;align-self:center}
#room-wall.is-eight-wide .room-slot-setup{min-height:0;font-size:9px;line-height:1.2;margin:0;padding:0;align-self:center;overflow:hidden;white-space:nowrap;text-overflow:ellipsis}
#room-wall.is-eight-wide .room-shared-route .room-nozzle-junction{height:47px}
#room-wall.is-eight-wide .room-shared-route .room-unit-below{gap:5px}
#camera-fullscreen{position:fixed;inset:0;width:100vw;height:100dvh;max-width:none;max-height:none;margin:0;padding:0;border:0;border-radius:0;background:#080d0e;color:var(--fg);overflow:hidden}
#camera-fullscreen[open]{display:flex;flex-direction:column}
#camera-fullscreen::backdrop{background:#080d0e}
#camera-fullscreen .fullscreen-toolbar{display:flex;align-items:center;gap:12px;padding:max(10px,env(safe-area-inset-top)) max(14px,env(safe-area-inset-right)) 10px max(14px,env(safe-area-inset-left));flex:none;background:var(--card)}
#camera-fullscreen .fullscreen-toolbar strong{flex:1;min-width:0}
#camera-fullscreen button{min-height:44px;padding:8px 14px}
#camera-fullscreen .fullscreen-picture{position:relative;flex:1;min-height:0;display:grid;place-items:center;padding-bottom:env(safe-area-inset-bottom)}
#camera-fullscreen canvas{display:block;width:100%;height:100%;min-height:0;object-fit:contain}
#camera-fullscreen .fullscreen-wait{position:absolute;inset:0;display:grid;place-items:center;color:var(--mute)}
#camera-fullscreen .fullscreen-wait[hidden]{display:none}
#camera-fullscreen .fullscreen-status{position:absolute;bottom:max(12px,env(safe-area-inset-bottom));left:14px;background:#080d0ebb;padding:5px 9px;border-radius:6px;color:#d6dfdc;font-size:12px;pointer-events:none}
@media(max-width:700px){.camera-expand{min-height:44px;min-width:44px}.room-printer-name .camera-expand{padding:4px 8px}.room-printer-name .camera-expand span{display:none}}
@media(max-width:700px){.room-movement-launch{min-height:44px;padding:7px 9px}}
.dd .storage-clear-action{display:block;width:100%;text-align:left;border:0;border-radius:6px;background:transparent;color:var(--bad);font-size:12px;font-weight:400;padding:10px;white-space:normal}
.dd .storage-clear-action:hover{background:var(--field)}
.dd .storage-clear-action:disabled{opacity:.5;cursor:wait}
.dd .storage-clear-status{display:block;padding:0 10px 8px;font-size:11px;color:var(--mute);white-space:pre-line;overflow-wrap:anywhere}
.dd .storage-clear-status:empty{display:none}
.terminal-launch{display:inline-flex;align-items:center;justify-content:center;min-height:34px;padding:8px 12px;border:1px solid var(--line);border-radius:8px;background:var(--field);color:var(--fg);font-size:12px;text-decoration:none;white-space:nowrap}
.terminal-launch:hover{background:#364448}
.stock-panel{margin:0 0 28px;padding:20px;border:1px solid var(--line);border-radius:12px;background:var(--panel,#1b2528)}
.stock-panel h3{margin:0 0 6px;font-size:19px}.stock-panel p{color:var(--mute);font-size:13px;line-height:1.5}
.stock-toolbar{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
.stock-form{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin:15px 0}
.stock-form .stock-product-choice{grid-column:span 2;min-width:0}@media(max-width:600px){.stock-form .stock-product-choice{grid-column:1/-1}}
.stock-form[hidden],.stock-alert[hidden]{display:none}
.stock-form label{font-size:12px;color:var(--mute)}.stock-form input,.stock-form select{width:100%;display:block;margin-top:5px;min-height:38px;padding:8px;color:var(--fg);background:var(--field);border:1px solid var(--line);border-radius:7px}
.stock-form .stock-form-actions{display:flex;align-items:end;gap:8px}.stock-form-actions button{min-height:38px}
.stock-scroll{overflow-x:auto}.stock-table{width:100%;border-collapse:collapse;font-size:13px}.stock-table th,.stock-table td{text-align:left;padding:10px;border-bottom:1px solid var(--line);white-space:nowrap}.stock-table th{color:var(--mute);font-size:11px;font-weight:600}.stock-table button{font-size:12px}.stock-table select{max-width:200px;min-height:34px;background:var(--field);color:var(--fg);border:1px solid var(--line);border-radius:6px}
.stock-cart{display:inline-block;padding:8px 10px;margin-right:6px;border-radius:7px;background:var(--ok);color:#10231b!important;font-size:12px;font-weight:600;text-decoration:none}.stock-table a:not(.stock-cart){color:var(--ok);font-size:12px}
.stock-low{color:var(--bad)}.stock-ok{color:var(--good,#a8e4d1)}.stock-alert{padding:11px 14px;margin:14px 0;border-radius:8px;background:#4a351e;color:#ffcf8c}.stock-note{font-size:12px}.stock-empty{padding:18px;color:var(--mute)}
#room-drying-content .stock-panel{padding:14px 16px}
#room-drying-content .stock-toolbar p{margin:3px 0 8px}
#room-drying-content .stock-table th,#room-drying-content .stock-table td{padding:7px 9px}
.management-toolbar{display:flex;justify-content:flex-end;gap:8px;margin:0 0 12px}.management-toolbar button{min-height:36px}
#management-grid{display:grid;grid-template-columns:repeat(48,minmax(0,1fr));grid-auto-rows:8px;grid-auto-flow:row dense;gap:14px;align-items:start;position:relative;width:100%;min-width:0}
#management-grid>[data-management-card]{position:relative;grid-column:auto/span var(--management-span,24);grid-row-end:span var(--management-rows,1);min-width:0;margin:0;align-self:start}
#management-grid>[data-management-card][data-management-column]{grid-column-start:var(--management-column)!important;grid-row-start:var(--management-row)!important}
#room-drying-content.management-editing #management-grid{padding-bottom:240px;min-height:65vh}
.management-drying{padding:14px;border:1px solid var(--line);border-radius:12px;background:var(--card);min-width:0;overflow-anchor:none}
.management-drying>h3{margin:0 0 10px}
#room-drying-list{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,400px),1fr));gap:10px;align-items:start;width:100%;min-width:0}
#room-drying-list>.room-empty{width:100%}
#room-drying-list>.room-dry-unit{background:var(--bg);padding:12px;gap:8px 12px;align-content:start;grid-template-columns:minmax(0,1fr);min-width:0;margin:0}
#room-drying-content [data-management-card][data-management-height]{height:var(--management-height);overflow:auto}
#room-drying-list>.room-dry-unit h3{margin-bottom:3px;line-height:1.25}
#room-drying-list>.room-dry-unit .room-dry-materials{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#room-drying-list>.room-dry-unit .room-unit-title{flex-wrap:wrap;row-gap:2px;line-height:1.3}
#room-drying-list>.room-dry-unit .room-dry-humidity{margin-top:4px}
#room-drying-list>.room-dry-unit .bar{margin:6px 0}
#room-drying-list>.room-dry-unit .room-dry-result{margin-top:7px;padding-top:7px}
#room-drying-list>.room-dry-unit .room-dry-actions button{min-height:36px;padding:7px 10px}
#room-drying-list>.room-dry-unit .reason[data-routine="true"]{display:none}
.management-edge{display:none;position:absolute;z-index:8;padding:0;border:0;border-radius:5px;background:transparent!important;touch-action:none;user-select:none;min-width:0;min-height:0}
#room-drying-content .management-edge{padding:0;border:0;background:transparent;min-width:0;min-height:0}
.management-edge:after{content:'';position:absolute;background:var(--ok);border-radius:4px;opacity:.85}
.management-edge-n,.management-edge-s{height:12px;left:14px;right:14px;cursor:ns-resize}
.management-edge-n{top:0}.management-edge-s{bottom:0}
.management-edge-n:after,.management-edge-s:after{width:38px;height:3px;left:calc(50% - 19px);top:4px}
.management-edge-e,.management-edge-w{width:12px;top:14px;bottom:14px;cursor:ew-resize}
.management-edge-e{right:0}.management-edge-w{left:0}
.management-edge-e:after,.management-edge-w:after{width:3px;height:38px;top:calc(50% - 19px);left:4px}
.management-edge-se{width:18px;height:18px;right:0;bottom:0;cursor:nwse-resize}
.management-edge-se:after{width:8px;height:8px;right:4px;bottom:4px;background:none;border-right:2px solid var(--ok);border-bottom:2px solid var(--ok)}
#room-drying-content.management-editing .management-edge{display:block}
#room-drying-content.management-editing [data-management-card]{outline:1px dashed var(--ok);outline-offset:3px;cursor:grab;user-select:none}
#room-drying-content.management-editing [data-management-card]:active{cursor:grabbing}
#room-drying-content.management-editing .management-dimensions{position:absolute;right:14px;bottom:14px;z-index:9;background:var(--field);padding:4px 7px;border-radius:6px;font-size:11px;color:var(--fg);pointer-events:none}
#room-drying-content.management-editing .management-drop-target{outline:2px solid var(--ok);outline-offset:3px}
#room-drying-content.management-editing .management-dragging{opacity:.8;pointer-events:none;position:relative;z-index:5;box-shadow:0 12px 28px #0008}
.management-placement{position:absolute;box-sizing:border-box;pointer-events:none;z-index:19;border:2px dashed var(--ok);background:#aee6cc12;border-radius:10px;color:var(--ok);padding:10px;font-size:12px}
.management-placement.invalid{border-color:var(--bad);color:var(--bad);background:#fa879112}
@media(min-width:1100px){body.room-drying-view main#room-main{padding-left:clamp(20px,1.5vw,48px);padding-right:clamp(20px,1.5vw,48px)}}
@media(max-width:900px){#management-grid{grid-template-columns:repeat(12,minmax(0,1fr))}#management-grid>[data-management-card]{grid-column:span 12!important;grid-row-start:auto!important}.management-edge{display:none!important}}

.room-alert-wrap{position:relative}.room-alert-dropdown{position:absolute;right:0;top:calc(100% + 8px);z-index:50;width:min(390px,calc(100vw - 24px));max-height:min(65vh,620px);overflow:auto;padding:8px;background:var(--card);border:1px solid var(--line);border-radius:12px;box-shadow:0 20px 45px #0009}.room-alert-dropdown[hidden]{display:none}.fleet-alert{display:flex;align-items:flex-start;gap:6px;padding:7px;border-bottom:1px solid var(--line)}.fleet-alert:last-child{border-bottom:0}.fleet-alert-open{flex:1;min-width:0;text-align:left;background:transparent;border:0;padding:6px;white-space:normal}.fleet-alert-open strong,.fleet-alert-open span{display:block}.fleet-alert-open strong{color:var(--warn);font-size:12px;margin-bottom:4px}.fleet-alert-open span{color:var(--fg);font-size:12px;line-height:1.4;overflow-wrap:anywhere}.fleet-alert-dismiss{flex:none;font-size:11px;padding:6px 8px}#room-stop-all{background:#f18f9817;color:var(--bad);font-weight:500}@media(max-width:600px){.room-alert-dropdown{right:-10px}.room-heading-actions{flex-wrap:wrap;justify-content:flex-end}}
</style></head><body>
<header><div class="brand"><h1 style="display:flex;align-items:center;gap:10px"><svg class="madking" viewBox="0 0 110 200" width="38" height="68" role="img" aria-label="printing_farmlord logo" xmlns="http://www.w3.org/2000/svg"><image href="data:image/png;base64,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" x="0" y="0" width="110" height="200"/></svg><span>printing_farmlord</span></h1></div><nav class="room-nav" aria-label="Main navigation"><button id="room-watch" aria-pressed="true" onclick="roomShow('watch')">Watch</button><button id="room-drying" aria-pressed="false" onclick="roomShow('drying')">Filament management</button></nav>
<div class="hdr">
  <span class="conn" id="net" title="Which network this Mac is on"><i></i><span>network…</span></span>
  <span class="conn" id="conn"><i></i><span>connecting…</span></span>
  <span class="upd" id="upd"></span>
  <button id="dryall" onclick="dryAll()" title="Start drying on every AMS of every printer, each at the highest temperature safe for the spools inside">Dry all printers</button>
  <button id="stopall" onclick="stopAll()" title="Stop drying on every AMS of every printer">Stop all drying</button>
  <a id="print-terminal" class="terminal-launch" href="amsprint://connect" role="button" title="Open the Mac Terminal connection used by Orca for office printers">Open Orca terminal ↗</a>
  <button class="primary" id="refresh" onclick="manualRefresh()">↻ Refresh</button>
  <div class="room-alert-wrap"><button id="attention-button" class="attention-button" type="button" onclick="openFleetAlerts()" aria-expanded="false" aria-controls="room-alert-dropdown">Alerts</button><div id="room-alert-dropdown" class="room-alert-dropdown" role="region" aria-label="Printer and stock alerts" hidden></div></div>
  <div class="menu"><button title="Printer settings" aria-label="Printer settings" onclick="this.parentNode.classList.toggle('open')"><svg viewBox="0 0 18 18" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><path d="M3 5h12M3 13h12M6 3v4M12 11v4"/></svg>Settings</button>
    <div class="dd"><div class="room-settings-name">Selected printer: <span id="room-settings-printer"></span></div><a onclick="this.closest('.menu').classList.remove('open');addPrinterDlg()">Add a printer</a><a onclick="this.closest('.menu').classList.remove('open');roomShow('detail');openAttention('settings')">Sounds &amp; notifications</a><a onclick="this.closest('.menu').classList.remove('open');editPrinterDlg()">Edit printer details</a><a onclick="this.closest('.menu').classList.remove('open');removePrinter()">Remove this printer</a><button type="button" id="room-clear-storage" class="storage-clear-action" onclick="clearAllStorage()">Clear all printer USB files…</button><span id="room-clear-storage-status" class="storage-clear-status" role="status" aria-live="polite"></span></div></div>
</div></header>
<div id="banner" hidden></div>
<main id="room-main"><section id="room-home"><div class="room-heading"><div><h2 id="room-heading-title">Your room, in one glance</h2><p id="room-heading-subtitle">Connecting to your printers…</p></div><div class="room-heading-actions"><button id="room-camera-toggle" onclick="roomToggleCameras()">Pause cameras</button><button id="room-plan" class="primary" onclick="roomShow('drying')">Plan drying</button><button id="room-stop-all" type="button" onclick="stopAll()" hidden>■ Stop all drying AMSs</button></div></div><section id="room-power" class="room-power" aria-label="Approximate current power load" hidden></section><div id="room-wall-layout" class="room-wall-layout"><aside class="room-zoom" aria-label="Printer grid zoom"><button type="button" onclick="roomSetZoom(ROOM_ZOOM+5)" aria-label="Enlarge printer cards" title="Enlarge cards">+</button><input id="room-zoom-slider" type="range" min="25" max="125" step="5" value="100" aria-label="Printer card zoom" aria-orientation="vertical" oninput="roomSetZoom(this.value)"><button type="button" onclick="roomSetZoom(ROOM_ZOOM-5)" aria-label="Shrink printer cards" title="Shrink cards">−</button><button type="button" id="room-zoom-value" class="room-zoom-value" onclick="roomSetZoom(100)" aria-label="Reset printer zoom to 100 percent" title="Reset to 100%">100%</button></aside><div id="room-wall-viewport" class="room-wall-viewport"><div id="room-wall" class="room-wall"></div></div></div><div id="room-drying-content" hidden><section class="stock-panel" aria-label="Filament stock"><div class="stock-toolbar"><div><h3>Filament stock</h3><p>One total per filament. Enter the spools you have available; spools already in an AMS are not counted. A newly inserted matching Bambu RFID spool reduces this number once.</p></div><button type="button" onclick="stockNew()">+ Add filament</button></div><div id="stock-alert" class="stock-alert" role="status" hidden></div><form id="stock-form" class="stock-form" hidden onsubmit="stockSave(event)"><input type="hidden" name="id"><label class="stock-product-choice">Filament<select name="product_key" required aria-label="Bambu filament"></select></label><label>Spools left<input name="unopened" type="number" min="0" max="100000" value="0" required></label><label>Order when at or below<input name="threshold" type="number" min="0" max="100000" placeholder="Optional"></label><div class="stock-form-actions"><button class="primary" type="submit">Save</button><button type="button" onclick="stockClose()">Cancel</button></div></form><p id="stock-message" role="status"></p><div class="stock-scroll"><table class="stock-table"><thead><tr><th>Filament</th><th>Spools left</th><th>Order threshold</th><th>Status</th><th></th></tr></thead><tbody id="stock-rows"></tbody></table></div></section><h3>Drying</h3><details class="room-dry-help"><summary>How drying works</summary><p class="room-dry-explainer">Filament retracts automatically when needed and the printer is idle. Least-dry spools come first. AMS units sharing printer power take turns; the printer always decides what power and temperature are available.</p></details><section id="room-drying-followup" class="room-followup" hidden aria-label="Drying request follow-up"><h3 id="room-followup-title" role="status" aria-live="polite"></h3><div id="room-followup-rows"></div></section><div id="room-drying-list" class="room-dry-list"></div><div class="room-dry-footer"><button class="primary" data-start-fleet onclick="dryAll()">Start all eligible AMS units</button><button class="stop" onclick="stopAll()">Stop all drying</button></div></div><div class="room-foot"><span>Open any printer for its live camera, filament and controls.</span><button class="room-link" onclick="addPrinterDlg()">+ Add printer</button></div></section><section id="printer-detail" hidden><div class="room-detailbar"><button onclick="roomShow('watch')">← Back to the room</button><span>Live controls · changes apply to this printer</span><div class="detail-layout-actions"><button type="button" id="layout-reset" hidden onclick="layoutReset()">Reset layout</button><button type="button" id="layout-edit" title="Drag panels directly to move them; drag edges to resize. Printer controls are inactive while editing." aria-pressed="false" onclick="layoutToggle()">Edit layout</button><button onclick="roomShow('drying')">All drying →</button></div></div><nav id="tabs" aria-label="Printers"></nav><section id="attention-panel" aria-labelledby="attention-title" hidden><div class="attention-top"><button type="button" class="attention-close-visible" onclick="ATTENTION_DEMO?closeAttention():dismissPrinterAlerts(P)" aria-label="Close printer alert">× Close</button><h2 class="attention-heading" id="attention-title">Printer alerts</h2><div class="attention-nav"><button onclick="ATTENTION_DEMO=false;openAttention('active')">Active</button><button onclick="ATTENTION_DEMO=false;openAttention('history')">History</button><button onclick="ATTENTION_DEMO=false;openAttention('settings')">Settings</button></div></div><p id="attention-subtitle"></p><div id="attention-content" aria-live="polite"></div><p id="attention-message" role="status"></p><div class="attention-actions" id="attention-actions"></div></section><div id="err"></div><div id="act"></div><div class="printer-dashboard" id="printer-dashboard">
<section class="dashboard-widget" data-widget="camera" data-size="medium"><div id="cam"></div></section>
<section class="dashboard-widget" data-widget="temperatures" data-size="small"><div class="widget-control printer-controls" id="ctl-temperatures"></div></section>
<section class="dashboard-widget" data-widget="fans" data-size="small"><section class="desk-panel desk-fans" aria-labelledby="fans-heading"><div class="desk-panel-heading"><h2 id="fans-heading">Fans status</h2><span id="fans-reading-status">Waiting for readings</span></div><dl class="desk-fan-list" id="fans-status"></dl></section></section>
<section class="dashboard-widget" data-widget="filament" data-size="medium"><div class="printer-materials"><div class="desk-panel-heading"><h2>Filament &amp; drying</h2><span id="room-reading-status">Waiting for readings</span></div><div id="slots"></div></div></section>
<section class="dashboard-widget" data-widget="quick" data-size="small"><div class="widget-control printer-controls" id="ctl-quick"></div></section>
<section class="dashboard-widget" data-widget="machine" data-size="small"><div class="widget-control printer-controls" id="ctl-machine"></div></section>
<section class="dashboard-widget" data-widget="activity" data-size="small"><section class="desk-panel desk-activity" aria-labelledby="activity-heading"><div class="desk-panel-heading"><h2 id="activity-heading">Recent activity</h2></div><div id="events" role="region" aria-label="Printer and filament history" tabindex="0"></div></section></section>
</div><div id="ctl" hidden></div></section></main>

<dialog id="dlg"><form method="dialog" id="form"></form></dialog>

<script>
let D=null,PRINTERS=[],P=null,DLG=false;const SPOOL_OPTIONS=new Set();const MANUAL_HOLD=6,AUTO_PCT=85;
const $=s=>document.querySelector(s);
const DETAIL_LAYOUT_KEY='ams_detail_widgets_v1';
const DETAIL_WIDGETS=[['camera','Camera','medium'],['temperatures','Temperatures','small'],['fans','Fans status','small'],['filament','Filament & drying','medium'],['quick','Quick actions','small'],['machine','Print speed','small'],['activity','Recent activity','small']];
let DETAIL_DRAG=null,DETAIL_EDGE=null,DETAIL_SUPPRESS_CLICK=0;
let DETAIL_LAYOUT_FRAME=0,DETAIL_RESIZE_OBSERVER=null;
function layoutMeasure(){
 const grid=$('#printer-dashboard');if(!grid||!grid.offsetWidth)return;
 // Mobile panels use natural document height. Desktop row measurements must
 // never reserve tiny tracks or rewrite saved positions on a narrow viewport.
 if(window.innerWidth<900){
  grid.querySelectorAll(':scope > .dashboard-widget').forEach(widget=>widget.style.removeProperty('grid-row-end'));
  return;
 }
 layoutFreezePositions();
 const style=getComputedStyle(grid),row=parseFloat(style.gridAutoRows)||8,gap=parseFloat(style.rowGap)||14;
 grid.querySelectorAll(':scope > .dashboard-widget').forEach(widget=>{
  const rows=Math.max(1,Math.ceil((widget.getBoundingClientRect().height+gap)/(row+gap)));
  const value=`span ${rows}`;if(widget.style.gridRowEnd!==value)widget.style.gridRowEnd=value;
 });
 layoutResolvePositions();
}
function layoutQueueMeasure(){
 if(DETAIL_LAYOUT_FRAME)return;
 DETAIL_LAYOUT_FRAME=requestAnimationFrame(()=>{DETAIL_LAYOUT_FRAME=0;layoutMeasure();});
}
function layoutSave(settle=true){
 const grid=$('#printer-dashboard');if(!grid)return;
 if(settle&&grid.offsetWidth&&window.innerWidth>=900){layoutMeasure();layoutResolvePositions(true);}
 const widgets=[...grid.querySelectorAll(':scope > .dashboard-widget')];
 try{localStorage.setItem(DETAIL_LAYOUT_KEY,JSON.stringify({placementVersion:2,directDrag:true,machineCompact:true,order:widgets.map(w=>w.dataset.widget),sizes:Object.fromEntries(widgets.map(w=>[w.dataset.widget,w.dataset.size])),positions:Object.fromEntries(widgets.map(w=>[w.dataset.widget,{column:Number(w.dataset.column)||null,row:Number(w.dataset.row)||null}])),dimensions:Object.fromEntries(widgets.map(w=>[w.dataset.widget,{span:Number(w.dataset.span)||null,height:Number(w.dataset.height)||null}]))}));}catch(e){}
}
function layoutApply(saved){
 const grid=$('#printer-dashboard');if(!grid)return;
 const byId=new Map([...grid.querySelectorAll(':scope > .dashboard-widget')].map(w=>[w.dataset.widget,w]));
 const order=Array.isArray(saved?.order)?saved.order:[];
 [...new Set([...order,...DETAIL_WIDGETS.map(x=>x[0])])].forEach(id=>{const w=byId.get(id);if(w)grid.appendChild(w);});
 DETAIL_WIDGETS.forEach(([id,,defaultSize])=>{
  const widget=byId.get(id);if(!widget)return;
  const size=saved?.sizes?.[id];widget.dataset.size=['small','medium','large'].includes(size)?size:defaultSize;
   const dimensions=saved?.dimensions?.[id];
  layoutSetDimensions(widget,id==='machine'&&!saved?.machineCompact?{...dimensions,height:null}:dimensions);
  layoutPosition(widget,saved?.placementVersion===2?saved?.positions?.[id]:null);
 });
 // Remove the rows previously reserved by the detached editing bars, once.
 if(saved?.placementVersion===2&&!saved?.directDrag){
  const placed=[...byId.values()].filter(w=>w.dataset.row).map(widget=>({widget,cell:layoutCell(widget),shift:0})).sort((a,b)=>a.cell.row-b.cell.row);
  placed.forEach((entry,index)=>{
   const above=placed.slice(0,index).filter(other=>other.cell.row<entry.cell.row&&other.cell.column<entry.cell.column+entry.cell.width&&other.cell.column+other.cell.width>entry.cell.column);
   entry.shift=above.length?Math.max(...above.map(other=>other.shift+2)):0;
   layoutPosition(entry.widget,{column:entry.cell.column,row:Math.max(1,entry.cell.row-entry.shift)});
  });
 }
 layoutQueueMeasure();
}
function layoutReset(){layoutApply(null);try{localStorage.removeItem(DETAIL_LAYOUT_KEY);}catch(e){}}
function layoutToggle(){
 const detail=$('#printer-detail'),editing=detail.classList.toggle('layout-editing');
 const button=$('#layout-edit');button.textContent=editing?'Done editing':'Edit layout';button.setAttribute('aria-pressed',String(editing));
 $('#layout-reset').hidden=!editing;
 $('#printer-dashboard').querySelectorAll(':scope > .dashboard-widget').forEach(widget=>{widget.tabIndex=editing?0:-1;});
 $('#printer-dashboard').querySelectorAll('.layout-edge').forEach(grip=>{grip.tabIndex=editing?0:-1;});
 if(DETAIL_DRAG)layoutClearDrag();
 if(DETAIL_EDGE)layoutEdgeEnd(null,true);
 layoutQueueMeasure();
}
function layoutSize(select){
 const widget=select.closest('.dashboard-widget');if(!widget||!['small','medium','large'].includes(select.value))return;
 widget.dataset.size=select.value;delete widget.dataset.span;widget.style.removeProperty('--widget-span');layoutSave();layoutQueueMeasure();
}
function layoutPosition(widget,position){
 const column=position?.column,row=position?.row;
 if(Number.isFinite(column)&&column>=1&&column<=48&&Number.isFinite(row)&&row>=1&&row<=1000){
  widget.dataset.column=Math.round(column);widget.dataset.row=Math.round(row);
  widget.style.setProperty('--widget-column',Math.round(column));widget.style.setProperty('--widget-row',Math.round(row));
 }else{delete widget.dataset.column;delete widget.dataset.row;widget.style.removeProperty('--widget-column');widget.style.removeProperty('--widget-row');}
}
function layoutMinWidth(widget){return widget.dataset.widget==='fans'?190:300;}
function layoutMinSpan(widget){return widget.dataset.widget==='fans'?4:8;}
function layoutSpan(widget){
 const custom=Number(widget.dataset.span);
 if(Number.isInteger(custom)&&custom>=layoutMinSpan(widget)&&custom<=48)return custom;
 return widget.dataset.size==='large'?48:widget.dataset.size==='medium'?(window.innerWidth<1250?48:32):(window.innerWidth<1250?24:16);
}
function layoutCell(widget,preferred=false){
 return {column:Number(preferred?widget.dataset.column:widget.style.getPropertyValue('--widget-column'))||Number(widget.dataset.column)||1,row:Number(preferred?widget.dataset.row:widget.style.getPropertyValue('--widget-row'))||Number(widget.dataset.row)||1,
  width:layoutSpan(widget),
  height:Number(widget.style.gridRowEnd.replace('span ',''))||1};
}
function layoutOverlap(a,b){return a.column<b.column+b.width&&a.column+a.width>b.column&&a.row<b.row+b.height&&a.row+a.height>b.row;}
function layoutFreezePositions(){
 // Once arranged, content scrolls inside panel bounds. Telemetry must not move the layout.
 if(window.innerWidth<900||!D||PC_DATA_PRINTER!==P)return;
 const grid=$('#printer-dashboard');if(!grid?.offsetWidth)return;
 const widgets=[...grid.querySelectorAll(':scope > .dashboard-widget[data-column]')];
 const style=getComputedStyle(grid),gap=parseFloat(style.rowGap)||14,step=(parseFloat(style.gridAutoRows)||8)+gap;
 const frames=widgets.map(widget=>({widget,cell:layoutCell(widget,true),height:widget.querySelector('.widget-content').getBoundingClientRect().height}));
 let changed=false;
 frames.forEach(({widget,cell,height})=>{
  if(widget.dataset.height||height<=0)return;
  const below=frames.filter(other=>other.cell.row>cell.row&&other.cell.column<cell.column+cell.width&&other.cell.column+other.cell.width>cell.column);
  const room=below.length?(Math.min(...below.map(other=>other.cell.row))-cell.row)*step-gap:1800;
  layoutSetDimensions(widget,{span:Number(widget.dataset.span)||null,height:Math.max(40,Math.min(1800,height,room))});changed=true;
 });
 if(changed)layoutSave(false);
}
function layoutPin(){
 const grid=$('#printer-dashboard'),bounds=grid.getBoundingClientRect(),style=getComputedStyle(grid),gap=parseFloat(style.columnGap)||14,unit=(grid.clientWidth+gap)/48,step=(parseFloat(style.gridAutoRows)||8)+(parseFloat(style.rowGap)||14);
 const positions=[...grid.querySelectorAll(':scope > .dashboard-widget')].map(widget=>{const rect=widget.getBoundingClientRect();return [widget,{column:Math.max(1,Math.round((rect.left-bounds.left)/unit)+1),row:Math.max(1,Math.round((rect.top-bounds.top)/step)+1)}];});
 positions.forEach(([widget,position])=>layoutPosition(widget,position));
 layoutFreezePositions();
}
function layoutResolvePositions(commit=false){
 if(window.innerWidth<900)return;
 const widgets=[...$('#printer-dashboard').querySelectorAll(':scope > .dashboard-widget[data-column]')];
 widgets.sort((a,b)=>Number(a.dataset.row)-Number(b.dataset.row)||Number(a.dataset.column)-Number(b.dataset.column));
 const occupied=[];
 for(const widget of widgets){
  const cell=layoutCell(widget,true);cell.column=Math.max(1,Math.min(49-cell.width,cell.column));
  let obstacle;while((obstacle=occupied.find(other=>layoutOverlap(cell,other))))cell.row=obstacle.row+obstacle.height;
  if(commit)layoutPosition(widget,cell);
  else{widget.style.setProperty('--widget-column',cell.column);widget.style.setProperty('--widget-row',cell.row);}
  occupied.push(cell);
 }
}
function layoutPeerRects(widget){
 const grid=$('#printer-dashboard'),origin=grid.getBoundingClientRect();
 return [...grid.querySelectorAll(':scope > .dashboard-widget')].filter(w=>w!==widget).map(w=>{
  const r=w.getBoundingClientRect();return {left:r.left-origin.left,top:r.top-origin.top,width:r.width,height:r.height,right:r.right-origin.left,bottom:r.bottom-origin.top};
 });
}
function layoutSnapCoordinate(raw,offsets,targets,quantize,min,max){
 let best=raw,distance=13;
 for(const offset of offsets)for(const target of targets){
  const value=quantize(target-offset),delta=Math.abs(value-raw);
  if(value>=min&&value<=max&&delta<distance&&Math.abs(value+offset-target)<1.5){best=value;distance=delta;}
 }
 return best;
}
function layoutClearGuides(){$('#printer-dashboard')?.querySelector('.layout-guides')?.remove();}
function layoutShowGuides(widget,rect){
 layoutClearGuides();const grid=$('#printer-dashboard'),peers=layoutPeerRects(widget),ns='http://www.w3.org/2000/svg';
 const svg=document.createElementNS(ns,'svg');svg.classList.add('layout-guides');svg.setAttribute('aria-hidden','true');
 const width=Math.max(grid.clientWidth,rect.right+20),height=Math.max(grid.scrollHeight,rect.bottom+20);
 svg.setAttribute('width',width);svg.setAttribute('height',height);
 const segments=new Set();let count=0;
 function line(x1,y1,x2,y2,spacing=false){
  const key=[x1,y1,x2,y2].map(Math.round).join(':');if(segments.has(key)||count>=8)return;
  segments.add(key);count++;const el=document.createElementNS(ns,'line');
  Object.entries({x1,y1,x2,y2}).forEach(([k,v])=>el.setAttribute(k,v.toFixed(1)));if(spacing)el.classList.add('spacing');svg.appendChild(el);
  if(spacing){
   const horizontal=y1===y2;
   for(const [x,y] of [[x1,y1],[x2,y2]]){const tick=document.createElementNS(ns,'line');Object.entries({x1:x-(horizontal?0:3),y1:y-(horizontal?3:0),x2:x+(horizontal?0:3),y2:y+(horizontal?3:0)}).forEach(([k,v])=>tick.setAttribute(k,v));svg.appendChild(tick);}
   const label=document.createElementNS(ns,'text');label.setAttribute('x',(x1+x2)/2+(horizontal?0:7));label.setAttribute('y',(y1+y2)/2+(horizontal?-6:4));label.setAttribute('text-anchor',horizontal?'middle':'start');label.textContent=Math.round(Math.hypot(x2-x1,y2-y1))+' px';svg.appendChild(label);
  }
 }
 for(const peer of peers){
  for(const [a,b] of [[rect.left,peer.left],[rect.right,peer.right],[(rect.left+rect.right)/2,(peer.left+peer.right)/2]])if(Math.abs(a-b)<1.5)line(a,Math.min(rect.top,peer.top)-6,a,Math.max(rect.bottom,peer.bottom)+6);
  for(const [a,b] of [[rect.top,peer.top],[rect.bottom,peer.bottom],[(rect.top+rect.bottom)/2,(peer.top+peer.bottom)/2]])if(Math.abs(a-b)<1.5)line(Math.min(rect.left,peer.left)-6,a,Math.max(rect.right,peer.right)+6,a);
 }
 const gap=parseFloat(getComputedStyle(grid).columnGap)||14;
 for(const peer of peers){
  const top=Math.max(rect.top,peer.top),bottom=Math.min(rect.bottom,peer.bottom),left=Math.max(rect.left,peer.left),right=Math.min(rect.right,peer.right);
  if(bottom>top){const y=top+Math.min(20,(bottom-top)/2);if(Math.abs(rect.left-peer.right-gap)<1.5)line(peer.right,y,rect.left,y,true);if(Math.abs(peer.left-rect.right-gap)<1.5)line(rect.right,y,peer.left,y,true);}
  if(right>left){const x=left+Math.min(24,(right-left)/2);if(Math.abs(rect.top-peer.bottom-gap)<1.5)line(x,peer.bottom,x,rect.top,true);if(Math.abs(peer.top-rect.bottom-gap)<1.5)line(x,rect.bottom,x,peer.top,true);}
 }
 if(count)grid.appendChild(svg);
}
function layoutDropPreview(event){
 const drag=DETAIL_DRAG,grid=$('#printer-dashboard'),bounds=grid.getBoundingClientRect(),style=getComputedStyle(grid);
 const gap=parseFloat(style.columnGap)||14,unit=(grid.clientWidth+gap)/48,rowSize=parseFloat(style.gridAutoRows)||8,rowGap=parseFloat(style.rowGap)||14,step=rowSize+rowGap;
 const cell=layoutCell(drag.widget),peers=layoutPeerRects(drag.widget),width=cell.width*unit-gap,height=drag.widget.getBoundingClientRect().height;
 let left=event.clientX-drag.grabX-bounds.left,top=event.clientY-drag.grabY-bounds.top;
 if(!event.altKey){
  left=layoutSnapCoordinate(left,[0,width/2,width],peers.flatMap(r=>[r.left,(r.left+r.right)/2,r.right]),v=>Math.round(v/unit)*unit,0,(48-cell.width)*unit);
  left=layoutSnapCoordinate(left,[0,width],peers.flatMap(r=>[r.right+gap,r.left-gap]),v=>Math.round(v/unit)*unit,0,(48-cell.width)*unit);
  top=layoutSnapCoordinate(top,[0,height/2,height],peers.flatMap(r=>[r.top,(r.top+r.bottom)/2,r.bottom]),v=>Math.round(v/step)*step,0,999*step);
  top=layoutSnapCoordinate(top,[0,height],peers.flatMap(r=>[r.bottom+rowGap,r.top-rowGap]),v=>Math.round(v/step)*step,0,999*step);
 }
 cell.column=Math.max(1,Math.min(49-cell.width,Math.round(left/unit)+1));
 cell.row=Math.max(1,Math.min(1000,Math.round(top/step)+1));
 const inside=event.clientX>=bounds.left&&event.clientX<=bounds.right&&event.clientY>=bounds.top;
 const blocked=[...grid.querySelectorAll(':scope > .dashboard-widget')].some(widget=>widget!==drag.widget&&layoutOverlap(cell,layoutCell(widget)));
 drag.destination=inside&&!blocked?cell:null;
 let marker=grid.querySelector('.layout-placement');if(!marker){marker=document.createElement('div');marker.className='layout-placement';grid.appendChild(marker);}
 Object.assign(marker.style,{left:((cell.column-1)*unit)+'px',top:((cell.row-1)*step)+'px',width:(cell.width*unit-gap)+'px',height:(cell.height*step-rowGap)+'px'});
 marker.classList.toggle('invalid',!drag.destination);marker.textContent=drag.destination?'Move here':'Not enough room';
 if(drag.destination){
  const left=(cell.column-1)*unit,top=(cell.row-1)*step;
  layoutShowGuides(drag.widget,{left,top,width,height,right:left+width,bottom:top+height});
  // Show the panel at the proposed position, not a second unsnapped location.
  const actual=drag.widget.getBoundingClientRect(),transform=new DOMMatrixReadOnly(getComputedStyle(drag.widget).transform);
  drag.widget.style.transform=`translate3d(${left-(actual.left-bounds.left-transform.m41)}px,${top-(actual.top-bounds.top-transform.m42)}px,0)`;
 }else layoutClearGuides();
}
function layoutClearDrag(){
 if(!DETAIL_DRAG)return;
 const {handle,widget,pointerId,moved}=DETAIL_DRAG;
 DETAIL_DRAG=null;if(moved)DETAIL_SUPPRESS_CLICK=Date.now()+400;
 widget.classList.remove('layout-dragging');widget.style.transform='';
 layoutClearGuides();
 $('#printer-dashboard').querySelector('.layout-placement')?.remove();
 $('#printer-dashboard').querySelectorAll('.layout-drop-target').forEach(w=>w.classList.remove('layout-drop-target'));
 try{if(handle.hasPointerCapture(pointerId))handle.releasePointerCapture(pointerId);}catch(e){}
 DETAIL_DRAG=null;
}
function layoutPointerDown(event){
 if(window.innerWidth<900||!$('#printer-detail').classList.contains('layout-editing')||DETAIL_EDGE||DETAIL_DRAG||event.target.closest('.layout-edge')||!event.isPrimary||event.button!==0)return;
 const handle=event.currentTarget,widget=handle.closest('.dashboard-widget'),bounds=widget.getBoundingClientRect();
 DETAIL_DRAG={handle,widget,pointerId:event.pointerId,startX:event.clientX,startY:event.clientY,moved:false,target:null,free:window.innerWidth>=900,grabX:event.clientX-bounds.left,grabY:event.clientY-bounds.top};
 handle.setPointerCapture(event.pointerId);event.preventDefault();event.stopPropagation();
}
function layoutPointerMove(event){
 const drag=DETAIL_DRAG;if(!drag||drag.pointerId!==event.pointerId)return;
 const dx=event.clientX-drag.startX,dy=event.clientY-drag.startY;
 if(!drag.moved&&Math.hypot(dx,dy)<6)return;
 if(!drag.moved&&drag.free){layoutMeasure();layoutPin();}
 drag.moved=true;drag.widget.classList.add('layout-dragging');drag.widget.style.transform=`translate3d(${dx}px,${dy}px,0)`;
 if(drag.free){layoutDropPreview(event);event.preventDefault();return;}
 const grid=$('#printer-dashboard');
 const target=document.elementsFromPoint(event.clientX,event.clientY).map(el=>el.closest?.('.dashboard-widget')).find(w=>w&&w!==drag.widget&&w.parentElement===grid)||null;
 if(drag.target!==target){if(drag.target)drag.target.classList.remove('layout-drop-target');drag.target=target;if(target)target.classList.add('layout-drop-target');}
 event.preventDefault();
}
function layoutPointerUp(event){
 const drag=DETAIL_DRAG;if(!drag||drag.pointerId!==event.pointerId)return;
 if(drag.free){if(drag.moved&&drag.destination){layoutPosition(drag.widget,drag.destination);layoutSave();}layoutClearDrag();layoutQueueMeasure();return;}
 if(drag.moved&&drag.target){
  const target=drag.target,rect=target.getBoundingClientRect(),nearMiddle=Math.abs(event.clientY-(rect.top+rect.height/2))<rect.height*.3;
  const after=nearMiddle?event.clientX>rect.left+rect.width/2:event.clientY>rect.top+rect.height/2;
  $('#printer-dashboard').insertBefore(drag.widget,after?target.nextSibling:target);
  layoutSave();
 }
 layoutClearDrag();
}
function layoutMove(widget,direction){
 const grid=$('#printer-dashboard');
 if(window.innerWidth>=900){layoutMeasure();layoutPin();const cell=layoutCell(widget);cell.row=Math.max(1,cell.row+direction);const blocked=[...grid.querySelectorAll(':scope > .dashboard-widget')].some(w=>w!==widget&&layoutOverlap(cell,layoutCell(w)));if(!blocked){layoutPosition(widget,cell);layoutSave();}return;}
 if(direction<0&&widget.previousElementSibling)grid.insertBefore(widget,widget.previousElementSibling);
 else if(direction>0&&widget.nextElementSibling)grid.insertBefore(widget,widget.nextElementSibling.nextSibling);
 else return;
 layoutSave();
}
function layoutHandleKey(event){
 if(window.innerWidth<900)return;
 if(!$('#printer-detail').classList.contains('layout-editing')||event.target!==event.currentTarget)return;
 const direction={ArrowLeft:[-1,0],ArrowRight:[1,0],ArrowUp:[0,-1],ArrowDown:[0,1]}[event.key];if(!direction)return;
 const widget=event.currentTarget;
 if(window.innerWidth>=900){
  layoutMeasure();layoutPin();const cell=layoutCell(widget);
  cell.column=Math.max(1,Math.min(49-cell.width,cell.column+direction[0]));cell.row=Math.max(1,cell.row+direction[1]);
  const blocked=[...$('#printer-dashboard').querySelectorAll(':scope > .dashboard-widget')].some(w=>w!==widget&&layoutOverlap(cell,layoutCell(w)));
  if(!blocked){layoutPosition(widget,cell);layoutSave();}
 }else layoutMove(widget,direction[0]||direction[1]);
 event.preventDefault();widget.focus({preventScroll:true});
}
function layoutSetDimensions(widget,dimensions){
 const span=dimensions?.span,height=dimensions?.height;
 if(Number.isFinite(span)&&span>=layoutMinSpan(widget)&&span<=48){widget.dataset.span=Math.round(span);widget.style.setProperty('--widget-span',Math.round(span));}
 else{delete widget.dataset.span;widget.style.removeProperty('--widget-span');}
 if(Number.isFinite(height)&&height>=40&&height<=1800){widget.dataset.height=Math.round(height);widget.style.setProperty('--widget-height',Math.round(height)+'px');}
 else{delete widget.dataset.height;widget.style.removeProperty('--widget-height');}
}
function layoutEdgeDown(event){
 if(window.innerWidth<900||!$('#printer-detail').classList.contains('layout-editing')||DETAIL_DRAG||DETAIL_EDGE||!event.isPrimary||event.button!==0)return;
 const handle=event.currentTarget,widget=handle.closest('.dashboard-widget'),grid=$('#printer-dashboard');
 const bounds=widget.getBoundingClientRect(),gap=parseFloat(getComputedStyle(grid).columnGap)||14;
 const unit=(grid.clientWidth+gap)/48;
 DETAIL_EDGE={handle,widget,pointerId:event.pointerId,edge:handle.dataset.edge,x:event.clientX,y:event.clientY,
  width:bounds.width,height:widget.querySelector('.widget-content').getBoundingClientRect().height,unit,gap,
  before:{span:Number(widget.dataset.span)||null,height:Number(widget.dataset.height)||null},moved:false};
 handle.setPointerCapture(event.pointerId);event.preventDefault();
}
function layoutEdgeMove(event){
 const drag=DETAIL_EDGE;if(!drag||drag.pointerId!==event.pointerId)return;
 const dx=event.clientX-drag.x,dy=event.clientY-drag.y;
 if(!drag.moved&&Math.hypot(dx,dy)<3)return;
 drag.moved=true;drag.widget.classList.add('layout-resizing');document.body.classList.add('layout-resize-active');
 const dimensions={...drag.before};
 const minSpan=Math.max(layoutMinSpan(drag.widget),Math.ceil((layoutMinWidth(drag.widget)+drag.gap)/drag.unit));
 if(/[ew]/.test(drag.edge)){
  const width=drag.width+dx*(drag.edge.includes('w')?-1:1);
  dimensions.span=Math.min(48,Math.max(minSpan,Math.round((width+drag.gap)/drag.unit)));
 }
 if(/[ns]/.test(drag.edge))dimensions.height=Math.max(drag.widget.dataset.widget==='camera'?240:100,Math.min(1800,Math.round(drag.height+dy*(drag.edge.includes('n')?-1:1))));
 const grid=$('#printer-dashboard'),origin=grid.getBoundingClientRect(),actual=drag.widget.getBoundingClientRect(),peers=layoutPeerRects(drag.widget),left=actual.left-origin.left,top=actual.top-origin.top;
 if(!event.altKey){
  if(dimensions.span&&/[ew]/.test(drag.edge)){
   const end=left+dimensions.span*drag.unit-drag.gap;
   const snapped=layoutSnapCoordinate(end,[0],peers.flatMap(r=>[r.right,r.left-drag.gap]),v=>left+Math.round((v-left+drag.gap)/drag.unit)*drag.unit-drag.gap,left+layoutMinWidth(drag.widget),grid.clientWidth);
   dimensions.span=Math.min(48,Math.max(minSpan,Math.round((snapped-left+drag.gap)/drag.unit)));
  }
  if(dimensions.height&&/[ns]/.test(drag.edge)){
   const end=top+dimensions.height,minHeight=drag.widget.dataset.widget==='camera'?240:100;
   dimensions.height=Math.round(layoutSnapCoordinate(end,[0],peers.flatMap(r=>[r.bottom,r.top-drag.gap]),v=>v,top+minHeight,top+1800)-top);
  }
 }
 layoutSetDimensions(drag.widget,dimensions);layoutMeasure();
 const rect=drag.widget.getBoundingClientRect();layoutShowGuides(drag.widget,{left:rect.left-origin.left,top:rect.top-origin.top,right:rect.right-origin.left,bottom:rect.bottom-origin.top,width:rect.width,height:rect.height});
 let badge=drag.widget.querySelector('.layout-dimensions');
 if(!badge){badge=document.createElement('output');badge.className='layout-dimensions';drag.widget.appendChild(badge);}
 badge.textContent=`${Math.round(drag.widget.getBoundingClientRect().width)} × ${Math.round(drag.widget.querySelector('.widget-content').getBoundingClientRect().height)}`;
 event.preventDefault();
}
function layoutEdgeEnd(event,cancel=false){
 const drag=DETAIL_EDGE;if(!drag||(event?.pointerId!=null&&event.pointerId!==drag.pointerId))return;
 DETAIL_EDGE=null;layoutClearGuides();
 if(cancel)layoutSetDimensions(drag.widget,drag.before);
 else if(drag.moved){layoutMeasure();layoutSave();}
 drag.widget.classList.remove('layout-resizing');drag.widget.querySelector('.layout-dimensions')?.remove();
 document.body.classList.remove('layout-resize-active');
 try{if(drag.handle.hasPointerCapture(drag.pointerId))drag.handle.releasePointerCapture(drag.pointerId);}catch(e){}
 layoutQueueMeasure();
}
function layoutEdgeKey(event){
 if(window.innerWidth<900||!$('#printer-detail').classList.contains('layout-editing'))return;
 const handle=event.currentTarget,widget=handle.closest('.dashboard-widget'),edge=handle.dataset.edge;
 if(event.key==='Home'){layoutSetDimensions(widget,null);layoutSave();layoutQueueMeasure();event.preventDefault();return;}
 const horizontal=/[ew]/.test(edge),vertical=/[ns]/.test(edge);
 const direction=event.key==='ArrowUp'||event.key==='ArrowLeft'?-1:event.key==='ArrowDown'||event.key==='ArrowRight'?1:0;
 if(!direction)return;
 const dimensions={span:Number(widget.dataset.span)||null,height:Number(widget.dataset.height)||null};
 if(horizontal&&['ArrowLeft','ArrowRight'].includes(event.key)){
  const grid=$('#printer-dashboard'),gap=parseFloat(getComputedStyle(grid).columnGap)||14,unit=(grid.clientWidth+gap)/48;
  dimensions.span=Math.min(48,Math.max(layoutMinSpan(widget),Math.ceil((layoutMinWidth(widget)+gap)/unit),Math.round((widget.getBoundingClientRect().width+gap)/unit)+direction*(edge.includes('w')?-1:1)));
 }else if(vertical&&['ArrowUp','ArrowDown'].includes(event.key)){
  dimensions.height=Math.min(1800,Math.max(widget.dataset.widget==='camera'?240:100,widget.querySelector('.widget-content').getBoundingClientRect().height+direction*(edge.includes('n')?-1:1)*(event.shiftKey?50:10)));
 }else return;
 layoutSetDimensions(widget,dimensions);layoutSave();layoutQueueMeasure();event.preventDefault();
}
document.addEventListener('keydown',event=>{if(event.key==='Escape'&&DETAIL_DRAG){layoutClearDrag();event.preventDefault();}if(event.key==='Escape'&&DETAIL_EDGE){layoutEdgeEnd(null,true);event.preventDefault();}});
function initDashboardLayout(){
 DETAIL_WIDGETS.forEach(([id,title])=>{
  const widget=$(`#printer-dashboard>[data-widget="${id}"]`);if(!widget)return;
  const content=document.createElement('div');content.className='widget-content';
  while(widget.firstChild)content.appendChild(widget.firstChild);
  widget.appendChild(content);
  [['n','top'],['s','bottom'],['w','left'],['e','right'],['se','bottom-right']].forEach(([edge,label])=>{
   const grip=document.createElement('button');grip.type='button';grip.className=`layout-edge layout-edge-${edge}`;grip.dataset.edge=edge;
   grip.setAttribute('aria-label',`Resize ${title} from ${label} edge`);grip.title='Drag to resize · Alt ignores alignment snapping · double-click to fit content · arrow keys to adjust';
   grip.addEventListener('pointerdown',layoutEdgeDown);grip.addEventListener('pointermove',layoutEdgeMove);grip.addEventListener('pointerup',layoutEdgeEnd);
   grip.addEventListener('pointercancel',event=>layoutEdgeEnd(event,true));grip.addEventListener('lostpointercapture',event=>layoutEdgeEnd(event,true));
   grip.tabIndex=-1;grip.addEventListener('keydown',layoutEdgeKey);grip.addEventListener('dblclick',()=>{if(!$('#printer-detail').classList.contains('layout-editing'))return;layoutSetDimensions(widget,null);layoutSave();layoutQueueMeasure();});widget.appendChild(grip);
  });
  widget.tabIndex=-1;widget.setAttribute('aria-label',`${title} panel`);
  widget.addEventListener('pointerdown',layoutPointerDown,true);
  widget.addEventListener('pointermove',layoutPointerMove);
  widget.addEventListener('pointerup',layoutPointerUp);
  widget.addEventListener('pointercancel',layoutClearDrag);
  widget.addEventListener('lostpointercapture',()=>{if(DETAIL_DRAG?.widget===widget)layoutClearDrag();});
  widget.addEventListener('keydown',layoutHandleKey);
  widget.addEventListener('dragstart',event=>{if($('#printer-detail').classList.contains('layout-editing'))event.preventDefault();});
  widget.addEventListener('click',event=>{
   if(event.target.closest('.layout-edge'))return;
   if(window.innerWidth>=900&&($('#printer-detail').classList.contains('layout-editing')||Date.now()<DETAIL_SUPPRESS_CLICK)){event.preventDefault();event.stopImmediatePropagation();}
  },true);
 });
 let saved=null;try{saved=JSON.parse(localStorage.getItem(DETAIL_LAYOUT_KEY)||'null');}catch(e){}
 layoutApply(saved);layoutSave(false);
 if(typeof ResizeObserver!=='undefined'){
  DETAIL_RESIZE_OBSERVER=new ResizeObserver(layoutQueueMeasure);
  $('#printer-dashboard').querySelectorAll(':scope > .dashboard-widget').forEach(widget=>DETAIL_RESIZE_OBSERVER.observe(widget));
 }
 window.addEventListener('resize',()=>{if(DETAIL_DRAG)layoutClearDrag();if(DETAIL_EDGE)layoutEdgeEnd(null,true);layoutQueueMeasure();});
}
const UI_MOTION=window.matchMedia('(prefers-reduced-motion: reduce)');
const UI_ENTRANCES=new WeakMap();
function uiReveal(element,distance=8){
 if(!element||element.hidden||UI_MOTION.matches||!element.animate)return;
 UI_ENTRANCES.get(element)?.cancel();
 const motion=element.animate([{opacity:.25,transform:`translateY(${distance}px)`},{opacity:1,transform:'translateY(0)'}],{duration:240,easing:'cubic-bezier(.2,.8,.2,1)'});
 UI_ENTRANCES.set(element,motion);
}
document.addEventListener('click',event=>{
 const summary=event.target.closest?.('summary');
 if(summary)requestAnimationFrame(()=>{
  const detail=summary.parentElement;
  if(detail?.tagName==='DETAILS'&&detail.open)Array.from(detail.children).filter(el=>el!==summary).forEach(el=>uiReveal(el,4));
 });
 const button=event.target.closest?.('button,.fchip');
 if(!button||button.disabled||button.getAttribute('aria-disabled')==='true'||UI_MOTION.matches||!button.animate)return;
 // Visual acknowledgement only: commands still run immediately through existing handlers.
 button.animate([{opacity:.7},{opacity:1}],{duration:180,easing:'ease-out'});
},true);
try{P=localStorage.getItem('ams_printer')}catch(e){}
// Room overview is read-only. Existing printer actions still use the selected
// printer's fresh state and existing controller guards.
let ROOM_VIEW='watch',ROOM_CAMERAS=true,ROOM_SNAP_BUSY=false,ROOM_DATA=new Map(),ROOM_STREAMS=new Map(),ROOM_ABORTS=new Set();
try{ROOM_CAMERAS=localStorage.getItem('ams_wall_cameras')!=='0';}catch(e){}
const roomText=v=>pcEscape(String(v??''));
function roomInfo(name){return ROOM_DATA.get(name)||{};}
function roomOnline(v){return !v.refresh_delayed&&v.mqtt==='connected'&&!v.stale&&!v.error;}
function roomUnits(v){return Array.isArray(v.room_units)?v.room_units:[];}
function roomShow(view){
 if(DLG)return;
 const changed=ROOM_VIEW!==view;
 ROOM_VIEW=view;dropCam();roomStopSnapshots();
 document.body.classList.toggle('room-drying-view',view==='drying');
 $('#room-home').hidden=view==='detail';$('#printer-detail').hidden=view!=='detail';
 $('#banner').hidden=view!=='detail';$('#room-watch').setAttribute('aria-pressed',String(view==='watch'));$('#room-drying').setAttribute('aria-pressed',String(view==='drying'));
 if(view==='detail'){renderCam();if(D)render();else load();}else{renderRoom();roomSnapshots();}
 attentionBadge();
 if(changed)uiReveal($(view==='detail'?'#printer-detail':'#room-home'));
}
async function roomOpen(name,task){
 if(DLG)return;
 selectPrinter(name,true);
 if(task==='alerts'){await load();if(P===name)openAttention('active',true);}
}
function roomToggleCameras(){ROOM_CAMERAS=!ROOM_CAMERAS;try{localStorage.setItem('ams_wall_cameras',ROOM_CAMERAS?'1':'0');}catch(e){}roomStopSnapshots();if(!ROOM_CAMERAS)cameraStopAll();renderRoom();if(ROOM_CAMERAS)roomSnapshots();}

let ROOM_ZOOM=100,ROOM_ZOOM_OBSERVER=null,ROOM_ZOOM_LAYOUT='';
try{const saved=Number(localStorage.getItem('ams_room_zoom'));if(Number.isFinite(saved)&&saved>=25&&saved<=125)ROOM_ZOOM=Math.round(saved/5)*5;}catch(e){}
try{if(window.innerWidth>=1100&&localStorage.getItem('ams_wide_layout')!=='1'){ROOM_ZOOM=100;localStorage.setItem('ams_room_zoom','100');localStorage.setItem('ams_wide_layout','1');}}catch(e){}
function roomSetZoom(value){
 const number=Number(value);if(!Number.isFinite(number))return;
 ROOM_ZOOM=Math.max(25,Math.min(125,Math.round(number/5)*5));
 try{localStorage.setItem('ams_room_zoom',String(ROOM_ZOOM));}catch(e){}
 roomApplyZoom();
}
function roomApplyZoom(){
 const viewport=$('#room-wall-viewport'),wall=$('#room-wall');if(!viewport||!wall)return;
 $('#room-zoom-slider').value=String(ROOM_ZOOM);$('#room-zoom-slider').setAttribute('aria-valuetext',ROOM_ZOOM+' percent');$('#room-zoom-value').textContent=ROOM_ZOOM+'%';
 if(!ROOM_ZOOM_OBSERVER&&typeof ResizeObserver!=='undefined'){ROOM_ZOOM_OBSERVER=new ResizeObserver(roomApplyZoom);ROOM_ZOOM_OBSERVER.observe(viewport);}
 const width=viewport.clientWidth;if(!width||ROOM_VIEW!=='watch')return;
 if(window.innerWidth<=700){wall.classList.remove('is-eight-wide');wall.style.zoom='1';wall.style.width='100%';wall.style.gridTemplateColumns='minmax(0,1fr)';ROOM_ZOOM_LAYOUT='';roomScheduleFilamentPaths();return;}
 const key=width+':'+ROOM_ZOOM+':'+PRINTERS.length;if(key===ROOM_ZOOM_LAYOUT)return;ROOM_ZOOM_LAYOUT=key;
 // Balance rows at a readable natural card width; ultrawide screens get
 // additional columns instead of two oversized cards. Zoom scales this baseline.
 const capacity=Math.max(1,Math.floor((width+18)/(380+18))),count=Math.max(1,PRINTERS.length);
 const baseColumns=Math.ceil(count/Math.ceil(count/capacity));
 const scale=ROOM_ZOOM/100,baseWidth=(width-(baseColumns-1)*18)/baseColumns;
 const columns=Math.max(1,Math.min(Math.max(1,PRINTERS.length),Math.floor((width/scale+18)/(baseWidth+18)+.001)));
 wall.classList.toggle('is-eight-wide',columns>=8);
 wall.style.zoom=String(scale);wall.style.width=(100/scale)+'%';wall.style.gridTemplateColumns=`repeat(${columns},${Math.min(baseWidth,width/scale)}px)`;
 roomScheduleFilamentPaths();
}
window.addEventListener('resize',roomApplyZoom);
function roomStatus(v,printer){return v.refresh_delayed?'Updating connection…':!Object.keys(v).length?'Connecting…':!roomOnline(v)?'Printer unavailable':attentionPending(printer).length?'Needs your attention':v.print_state==='PAUSE'?'Print paused':v.print_state==='PREPARE'?'Preparing the print':v.print_state==='RUNNING'?'Printing':v.heating?'Drying filament':'Ready for the next print';}
function roomUnitQueued(u){return ['setup','preparing','unloading','pending','waiting'].includes(u.request_status);}
function roomRenderPower(){
 const section=$('#room-power');if(!section)return;section.hidden=ROOM_VIEW!=='watch';if(section.hidden)return;
 const estimates=PRINTERS.map(p=>roomInfo(p.name));
 const complete=estimates.length&&estimates.every(v=>roomOnline(v)&&v.estimated_power?.available&&Number.isFinite(v.estimated_power.estimate_w));
 const total=complete?'≈ '+(Math.round(estimates.reduce((sum,v)=>sum+v.estimated_power.estimate_w,0)/10)*10).toLocaleString()+' W':'— W';
 section.innerHTML=`<span title="${complete?'Estimated total for all printers and AMS units; not a meter reading.':'Waiting for fresh readings from every printer.'}" aria-label="${complete?'Estimated total power':'Total power unavailable'}">${total}</span>`;
}
function roomLiveSummary(v){
 const units=roomUnits(v),online=roomOnline(v);
 const drying=units.filter(u=>u.heating).map(u=>unitName(u.name)),waiting=units.filter(u=>!u.heating&&roomUnitQueued(u)&&/power/i.test(u.request_message||'')).map(u=>unitName(u.name));
 const missing=units.flatMap(u=>u.spools||[]).filter(s=>s.setup_required).length;
 const dryText=(drying.length?drying.join(' + '):'None')+(waiting.length?' · '+waiting.join(' + ')+' waiting for power':'');
 return `<div class="room-live-summary" aria-label="${online?'Current':'Last reported'} drying"><div><span class="room-live-label">Drying</span><span class="room-live-value ${drying.length?'active':''}">${online?'':'Last reported: '}${roomText(dryText)}</span></div></div>${missing?`<p class="room-setup-note">${missing} slot${missing===1?' needs':'s need'} filament setup${online?'':' · last reported'}</p>`:''}`;
}
// Names describe the reported swatch, not a guessed filament brand.
function slotColourName(value){
 const hex=String(value||'').replace(/^#/,'').toUpperCase();
 if(!/^(?:[0-9A-F]{6}|[0-9A-F]{8})$/.test(hex))return 'Colour unknown';
 const colour=hex.slice(0,6),named={'FFFFFF':'White','000000':'Black','7A7A7A':'Grey','C12E1F':'Red','1F5FC1':'Blue','2E8B57':'Green','FFD60A':'Yellow','FF7A00':'Orange','8B5A2B':'Brown','E8E0D0':'Natural','9B59B6':'Purple','00B3B3':'Teal'};
 if(named[colour])return named[colour];
 const [r,g,b]=[0,2,4].map(i=>parseInt(colour.slice(i,i+2),16)/255),max=Math.max(r,g,b),min=Math.min(r,g,b),delta=max-min,sat=max?delta/max:0;
 if(max<.16)return 'Black';
 if(delta<.10){if(min>=.92)return 'White';return max>=.65?'Light grey':max<.36?'Dark grey':'Grey';}
 if(sat<.20&&min>.72)return r>=b?'Cream':'Light grey';
 let hue=delta===0?0:max===r?((g-b)/delta)%6:max===g?(b-r)/delta+2:(r-g)/delta+4;hue=(hue*60+360)%360;
 if(hue<15||hue>=345)return max>.75&&min>.35?'Pink':'Red';
 if(hue<45)return max<.65?'Brown':sat<.45?'Beige':'Orange';
 if(hue<70)return max<.55?'Olive':'Yellow';
 if(hue<165)return 'Green';
 if(hue<195)return 'Teal';
 if(hue<260)return 'Blue';
 if(hue<290)return 'Purple';
 return max>.75&&min>.35?'Pink':'Magenta';
}
function roomWatchSlot(slot,printer,v){
 const setup=slot.setup_required,label=slot.empty?'Empty':slot.rfid_pending?'RFID pending':setup?'Set filament':slot.type||'Not set';
 const staleFull=slot.loaded&&v.print_state==='RUNNING'&&Number(v.print_pct)>=5&&slot.remaining===100;
 return `<div class="room-filament-remaining ${slot.loaded?'is-loaded':''} ${slot.empty?'is-empty':''} ${setup?'needs-setup':''}"><div class="room-filament-label"><span class="room-spool" style="--spool:#${/^[0-9a-f]{6}$/i.test(slot.color)?slot.color:'596069'}"></span><span class="room-slot-copy"><span class="room-slot-top">Slot ${slot.slot}${!slot.empty&&!setup?` · <span class="slot-colour-name" title="Reported colour #${roomText(slot.color||'')}">${slotColourName(slot.color)}</span>`:''}</span><b>${roomText(label)}</b></span>${slot.reading?`<span class="rfid-reading-indicator" role="status" aria-label="Reading RFID in slot ${slot.slot}" title="Reading RFID in slot ${slot.slot}"></span>`:''}</div>${setup?`<strong>${slot.reading?'Reading RFID…':slot.rfid_pending?'Retry after drying':'Needs setup'}</strong><button type="button" class="room-slot-setup" data-room-slot="${roomText(slot.key)}" data-slot-printer="${roomText(printer)}" aria-label="Open filament status for ${roomText(printer)} slot ${slot.slot}">${slot.rfid_pending?'RFID status →':'Set / read RFID →'}</button>`:`<strong title="${staleFull?'Printer still reports 100% during this print; actual remaining amount is unknown.':'Remaining filament is reported by the printer and may be an estimate.'}">${staleFull?'Estimate stale':slot.remaining!=null?Math.round(slot.remaining)+'% reported':slot.empty?'—':slot.rfid?'Not reported':'No RFID'}</strong>${slot.remaining!=null&&!staleFull?`<div class="room-remaining-bar"><i style="width:${slot.remaining}%"></i></div>`:''}`}</div>`;
}
function filamentIsMoving(v){
 const action=String(v?.current_action||'');
 return /^Printing(?: ·|$)|^(?:Changing|Loading|Unloading) filament(?:$| before drying)/.test(action);
}
async function roomOpenSlot(printer,key){
 if(DLG)return;
 selectPrinter(printer,true);const ok=await load();if(!ok||P!==printer||ROOM_VIEW!=='detail')return;
 requestAnimationFrame(()=>requestAnimationFrame(()=>{
  if(P!==printer||ROOM_VIEW!=='detail')return;
  const card=[...document.querySelectorAll('#slots [data-slot-key]')].find(el=>el.dataset.slotKey===key);
  if(card){card.scrollIntoView({behavior:'smooth',block:'center',inline:'nearest'});card.focus({preventScroll:true});}
 }));
}
document.addEventListener('click',e=>{const b=e.target.closest('[data-room-slot]');if(b)roomOpenSlot(b.dataset.slotPrinter,b.dataset.roomSlot);});
function roomFilamentRoute(units,online,v,printer){
 if(!units.length)return `<div class="room-unit-label" style="margin-top:12px">${online?'No AMS reported':'Waiting for printer data'}</div>`;
 const loaded=units.flatMap(u=>u.spools||[]).find(s=>s.loaded),color=/^[0-9a-f]{6}$/i.test(loaded?.color||'')?loaded.color:'aee6cc';
 const sourceLabel=loaded?loaded.type||'Material not set':v.nozzle_source?.status==='empty'?'No filament loaded':v.nozzle_source?.status==='external'?'External spool':'Source not reported';
 const fromBelow=units.slice(1).some(u=>(u.spools||[]).some(slot=>slot.loaded));
 const nozzle=`<div class="room-nozzle-junction ${fromBelow?'from-below':''}"><svg data-nozzle-anchor viewBox="0 0 20 25" fill="none" aria-hidden="true"><g data-nozzle-shape transform="${loaded&&!fromBelow?'rotate(180 10 12.5)':'rotate(0 10 12.5)'}"><path d="M7 1h6v6H7z" stroke="currentColor" stroke-width="1.3"/><path d="M4 7h12v9H4z" fill="var(--card)" stroke="currentColor" stroke-width="1.3"/><path d="M5 16h10l-4 7H9z" fill="currentColor"/><path d="M7 10h6M7 13h6" stroke="currentColor" opacity=".5"/></g></svg><span>${online?'Active nozzle':'Last reported nozzle'}<b>${roomText(sourceLabel)}</b></span></div>`;
 return `<div class="room-filament-route room-shared-route ${online?'':'is-stale'} ${online&&filamentIsMoving(v)?'is-flowing':''}" style="--filament-path:#${color}"><svg class="room-filament-path" aria-hidden="true"></svg>${units.map((u,index)=>`<div class="room-unit ${index?'room-unit-below':''}"><div class="room-unit-label">${roomText(unitName(u.name))} · ${online?'':'last reported: '}${roomText(u.heating?'drying'+(u.temp!=null?' at '+Math.round(u.temp)+'°C':''):u.min_pct!=null?Math.round(u.min_pct)+'% dry · least-dry spool':'no filament set')}${u.detail_status?`<span class="detail-map-reading">${roomText(u.detail_status)}</span>`:''}</div><div class="room-spools ${u.spools.length===1?'has-single':''}">${u.spools.map(slot=>roomWatchSlot(slot,printer,v)).join('')}</div></div>${index===0?nozzle:''}`).join('')}</div>`;
}
let ROOM_ROUTE_FRAME=0,ROOM_ROUTE_OBSERVER=null;
function roomDrawFilamentPaths(){
 ROOM_ROUTE_FRAME=0;
 document.querySelectorAll('#room-wall .room-shared-route,#slots.room-shared-route').forEach(route=>{
  const nozzle=route.querySelector('[data-nozzle-anchor]'),svg=route.querySelector('.room-filament-path');if(!nozzle||!svg)return;
  const box=route.getBoundingClientRect(),target=nozzle.getBoundingClientRect();if(!box.width||!box.height)return;
  const loadedSlot=route.querySelector('.room-filament-remaining.is-loaded');
  // Point the tip at the highlighted source, using actual layout after zoom/reflow.
  const entryFromBelow=loadedSlot?loadedSlot.getBoundingClientRect().top>=target.bottom:false;
  nozzle.parentElement.classList.toggle('from-below',entryFromBelow);
  nozzle.querySelector('[data-nozzle-shape]')?.setAttribute('transform',loadedSlot&&!entryFromBelow?'rotate(180 10 12.5)':'rotate(0 10 12.5)');
  nozzle.setAttribute('aria-label',loadedSlot?(entryFromBelow?'Nozzle points toward the loaded slot in the AMS below':'Nozzle points toward the loaded slot in the AMS above'):'No confirmed filament source');
  const tx=target.left+target.width/2-box.left;
  let tubes=[],active=[];
  route.querySelectorAll('.room-filament-remaining').forEach(source=>{
   const slot=source.getBoundingClientRect(),gridEl=source.closest('.room-spools'),grid=gridEl.getBoundingClientRect(),above=slot.top<target.top,sign=above?1:-1;
   const x=slot.left+slot.width/2-box.left,y=(above?slot.bottom:slot.top)-box.top,ty=(source.classList.contains('is-loaded')?target.top+target.height*(above?2:23)/25:(above?target.top:target.bottom))-box.top,bus=(above?grid.bottom:grid.top)-box.top+sign*Math.min(12,Math.abs((above?target.top:target.bottom)-(above?grid.bottom:grid.top))/2);
   let points=[[x,y]];
   if(above?slot.bottom<grid.bottom-4:slot.top>grid.top+4){
    // Route wrapped rows through the central gutter, never over another slot.
    const gutter=grid.left+grid.width/2-box.left,gap=(parseFloat(getComputedStyle(gridEl).rowGap)||8)/2;
    points.push([x,y+sign*gap],[gutter,y+sign*gap],[gutter,bus]);
   }else points.push([x,bus]);
   points.push([tx,bus],[tx,ty]);
   const d=points.map((p,i)=>(i?'L':'M')+p.map(n=>n.toFixed(1)).join(' ')).join(' ');
   tubes.push(d);
   if(source.classList.contains('is-loaded'))active.push(`<path class="route-edge" d="${d}"/><path class="route-color" d="${d}"/>${route.classList.contains('is-flowing')?`<path class="route-flow" d="${d}" style="animation-delay:-${Math.round(performance.now()%1200)}ms"/>`:''}<circle cx="${x}" cy="${y}" r="2.5" fill="var(--filament-path)" stroke="var(--mute)" stroke-width="1"/>`);
  });
  svg.setAttribute('viewBox',`0 0 ${box.width} ${box.height}`);svg.innerHTML=`<path class="route-idle" d="${tubes.join(' ')}"/>`+active.join('');
 });
}
function roomScheduleFilamentPaths(){
 if(!ROOM_ROUTE_FRAME)ROOM_ROUTE_FRAME=requestAnimationFrame(roomDrawFilamentPaths);
 if(!ROOM_ROUTE_OBSERVER&&typeof ResizeObserver!=='undefined'){ROOM_ROUTE_OBSERVER=new ResizeObserver(()=>{if(!ROOM_ROUTE_FRAME)ROOM_ROUTE_FRAME=requestAnimationFrame(roomDrawFilamentPaths);});['#room-wall','#slots'].forEach(selector=>{const el=$(selector);if(el)ROOM_ROUTE_OBSERVER.observe(el);});}
}
window.addEventListener('resize',roomScheduleFilamentPaths);
function renderPrinterFilamentMap(){
 const panel=$('#slots');if(!panel)return;
 if(!D||PC_DATA_PRINTER!==P){panel.innerHTML='';return;}
 const loaded=(D.slots||[]).find(r=>r.loaded),color=(loaded?.spool?.color||loaded?.color||'').replace(/^#/,'').slice(0,6);
 panel.style.setProperty('--filament-path',/^[0-9a-f]{6}$/i.test(color)?'#'+color:'var(--ok)');
 panel.classList.toggle('is-stale',!pcConnected()||LOAD_FAILED.has(P));
 roomScheduleFilamentPaths();
}
function printerNozzle(){
 const loaded=(D.slots||[]).find(r=>r.loaded),source=D.nozzle_source||{},label=loaded?loaded.spool?.type||'Material not set':source.status==='empty'?'No filament loaded':source.status==='external'?'External spool':'Source not reported';
 return `<div class="room-nozzle-junction"><svg data-nozzle-anchor viewBox="0 0 20 25" fill="none" aria-hidden="true"><g data-nozzle-shape><path d="M7 1h6v6H7z" stroke="currentColor" stroke-width="1.3"/><path d="M4 7h12v9H4z" fill="var(--card)" stroke="currentColor" stroke-width="1.3"/><path d="M5 16h10l-4 7H9z" fill="currentColor"/><path d="M7 10h6M7 13h6" stroke="currentColor" opacity=".5"/></g></svg><span>${pcConnected()?'Active nozzle':'Last reported nozzle'}<b>${pcEscape(label)}</b></span></div>`;
}
function printerSlotCard(r){
 // The existing actions and drying information are also the visual route's slots.
 return card(r).replace('<div class="card',`<div data-slot-key="${pcEscape(r.key)}" tabindex="-1" class="room-filament-remaining `+(r.loaded?'is-loaded ':'')+'card');
}
function roomNeedsErrorFlash(v){
 if(!v.has_error)return false;
 const errors=v.errors||{},faults=[...(errors.print_error?[errors.print_error]:[]),...(errors.hms||[]),...(v.process_faults||[])].filter(f=>f.blocking);
 // The camera warning is handled by automatic resume. Escalate it only if
 // recovery does not finish; keep other printer and AMS faults visible.
 return !v.camera_warning||faults.some(f=>String(f.code||'').replace(/[_-]/g,'').toUpperCase()!=='05008092');
}
function roomCardBody(x,v){
 const online=roomOnline(v),busy=['RUNNING','PREPARE','PAUSE'].includes(v.print_state),pct=Number.isFinite(v.print_pct)?Math.max(0,Math.min(100,v.print_pct)):0;
 const units=roomUnits(v),incidents=attentionPending(x.name),issue=incidents[0]?.message||'';
 const cameraNote=v.camera_warning==='resumed'?'Camera warning · print resumed':v.camera_warning==='resuming'?'Camera warning · resuming print':'';
 return {job:`<div><strong>${roomText(!online?'Waiting for this printer':busy?v.print_name||'Current print':'Ready for the next print')}</strong><small>${roomText((online?'':'Last report · ')+(busy?(v.print_state==='PREPARE'?'Preparing':v.print_state==='PAUSE'?'Paused':'Printing')+' · '+pct+'%':'Printer idle'))}</small></div><div class="room-left">${roomText(online?(busy&&v.print_left_min!=null?hm(v.print_left_min):'Idle'):'—')}<small>${online&&busy?'remaining':online?'ready when you are':'check connection'}</small></div>`,pct,
 body:`<div class="room-statusline"><span class="room-status ${!online?'offline':issue?'issue':''}">${issue?`<button class="room-link" data-room-alert="${roomText(x.name)}" title="${roomText(issue)}">Needs attention →</button>`:roomText(cameraNote||roomStatus(v,x.name))}</span><button class="room-link" data-room-open="${roomText(x.name)}">Open printer ↗</button></div>${roomLiveSummary(v)}${roomFilamentRoute(units,online,v,x.name)}`,issue};
}
function renderRoom(){
 if(!$('#room-home')||ROOM_VIEW==='detail')return;
 attentionBadge();
 const online=PRINTERS.filter(x=>roomOnline(roomInfo(x.name))),delayed=PRINTERS.filter(x=>roomInfo(x.name).refresh_delayed).length,offline=PRINTERS.length-online.length-delayed,printing=online.filter(x=>['RUNNING','PREPARE'].includes(roomInfo(x.name).print_state)).length,heating=online.filter(x=>roomInfo(x.name).heating).length,attention=online.filter(x=>attentionPending(x.name).length).length;
 $('#room-heading-title').textContent=ROOM_VIEW==='drying'?'Filament management':window.innerWidth<=700?'Your printers':'Your room, in one glance';
 $('#room-heading-subtitle').textContent=ROOM_VIEW==='drying'?'Stock, tagged spools and drying · live printer status':`${printing} printing · ${heating} drying${offline?' · '+offline+' offline':''}${delayed?' · reconnecting':''}${attention?' · '+attention+' need'+(attention===1?'s':'')+' help':''}`;
 $('#room-camera-toggle').hidden=ROOM_VIEW!=='watch';$('#room-camera-toggle').textContent=ROOM_CAMERAS?'Pause cameras':'Show cameras';$('#room-plan').hidden=false;$('#room-plan').textContent=ROOM_VIEW==='drying'?(ROOM_DRY_BUSY?'Checking AMS units…':'Start all eligible AMS units'):'Plan drying';$('#room-plan').disabled=ROOM_DRY_BUSY;$('#room-plan').onclick=()=>ROOM_VIEW==='drying'?dryAll():roomShow('drying');$('#room-stop-all').hidden=ROOM_VIEW!=='drying';
 $('#room-wall-layout').hidden=ROOM_VIEW!=='watch';$('#room-wall').hidden=ROOM_VIEW!=='watch';$('#room-drying-content').hidden=ROOM_VIEW!=='drying';
 roomRenderPower();
 if(ROOM_VIEW==='drying'){roomRenderDrying();return;}
 const wall=$('#room-wall');
 // Keep image elements mounted across telemetry updates: no stream resets.
 const names=PRINTERS.map(x=>x.name);for(const el of [...wall.children])if(!names.includes(el.dataset.printer))el.remove();
 for(const x of PRINTERS){let el=[...wall.children].find(e=>e.dataset.printer===x.name);if(!el){el=document.createElement('article');el.className='room-printer';el.dataset.printer=x.name;el.innerHTML=`<h3 class="room-printer-name"><span>${roomText(x.name)}</span><span class="room-printer-actions"><button type="button" class="room-movement-launch" data-movement-window="${roomText(x.name)}" aria-label="Open movement controls and live camera for ${roomText(x.name)} in a new window">Move + camera ↗</button><button type="button" class="camera-expand" data-camera-fullscreen="${roomText(x.name)}" aria-label="Fullscreen camera for ${roomText(x.name)}" title="Fullscreen camera">${cameraExpandIcon()}<span>Fullscreen</span></button></span></h3><button type="button" class="room-feed" data-room-open="${roomText(x.name)}" aria-label="Open ${roomText(x.name)}"><span class="room-cam-empty">Connecting camera…</span><img class="camera-fading" alt="${roomText(x.name)} camera" decoding="async"><span class="room-feed-top"><span>${roomText(x.name)}</span><span class="room-camera-note">Connecting…</span></span><span class="room-job"></span></button><div class="room-progress"><i></i></div><div class="room-issue" hidden><span></span><button class="room-link" data-room-alert="${roomText(x.name)}">Help me fix it →</button><button type="button" class="alert-close" data-dismiss-alert="${roomText(x.name)}" aria-label="Dismiss alert for ${roomText(x.name)}" title="Dismiss alert">×</button></div><div class="room-cardbody"></div>`;wall.append(el);}
 const v=roomInfo(x.name),c=roomCardBody(x,v);el.classList.toggle('has-printer-error',roomOnline(v)&&roomNeedsErrorFlash(v)&&!printerAlertsDismissed(x.name));el.querySelector('.room-feed').classList.toggle('is-printing',['RUNNING','PREPARE','PAUSE'].includes(v.print_state));el.querySelector('.room-job').innerHTML=c.job;el.querySelector('.room-progress i').style.width=c.pct+'%';el.querySelector('.room-cardbody').innerHTML=c.body;el.querySelector('.room-issue').hidden=!c.issue||printerAlertsDismissed(x.name);el.querySelector('.room-issue span').textContent=c.issue;
 if(!ROOM_CAMERAS){el.querySelector('.room-camera-note').textContent='Camera paused';el.querySelector('.room-cam-empty').textContent='Camera paused';}else if(!roomOnline(v)&&!CAMERA_CACHE.get(`/api/p/${encodeURIComponent(x.name)}/api/cam.mjpg`)?.shown){el.querySelector('.room-camera-note').textContent='Waiting for printer';el.querySelector('.room-cam-empty').textContent='Camera returns when the printer connects';}
 wall.append(el);
 }
 roomApplyZoom();roomScheduleFilamentPaths();
 if(!PRINTERS.length)wall.innerHTML='<div class="room-empty">No printers yet. Add your first printer to see its camera and filament here.</div>';
}
function roomCycleLabel(c){return c.outcome==='completed'?'Finished completely':c.outcome==='stopped'?(c.reason_code===8?'Stopped early — power fault':'Stopped early'):'Outcome uncertain';}
function roomCycleTime(t){return t?new Date(t*1000).toLocaleString(undefined,{weekday:'short',month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}):'End time unknown';}
function roomCycleResult(u){
 const cycles=Array.isArray(u.dry_cycles)?u.dry_cycles:[],last=cycles[cycles.length-1];
 const starting=!u.blocked&&['setup','preparing','unloading','pending','waiting'].includes(u.request_status)&&Number(u.request_since)>Number(last?.ended_at||0);
 if((u.heating&&!u.blocked)||starting){
  const history=cycles.length?`<details class="room-dry-history"><summary>Previous cycles (${cycles.length})</summary><ol>${cycles.slice().reverse().map(c=>`<li>${roomText(roomCycleTime(c.ended_at))} · ${roomText(roomCycleLabel(c))}${c.outcome==='stopped'&&c.last_remaining_min!=null?' · '+roomText(hm(c.last_remaining_min))+' left':''}${c.reason?' · '+roomText(c.reason):''}</li>`).join('')}</ol></details>`:'';
  return `<div class="room-dry-result" data-outcome="active"><strong>${u.heating?'Drying cycle in progress':'New drying request in progress'}</strong>${last?`<small>Previous cycle ${last.ended_at?'ended '+roomText(roomCycleTime(last.ended_at)):'end time unknown'}</small>`:''}${history}</div>`;
 }
 if(!last)return '<div class="room-dry-result"><small>No completed or stopped cycle recorded yet. Tracking begins with the next observed finish.</small></div>';
 const detail=last.outcome==='stopped'&&last.last_remaining_min!=null?' · '+hm(last.last_remaining_min)+' left on the printer timer':'';
 const history=cycles.length>1?`<details class="room-dry-history"><summary>Previous cycles (${cycles.length-1})</summary><ol>${cycles.slice(0,-1).reverse().map(c=>`<li>${roomText(roomCycleTime(c.ended_at))} · ${roomText(roomCycleLabel(c))}${c.outcome==='stopped'&&c.last_remaining_min!=null?' · '+roomText(hm(c.last_remaining_min))+' left':''}${c.reason?' · '+roomText(c.reason):''}</li>`).join('')}</ol></details>`:'';
 return `<div class="room-dry-result" data-outcome="${roomText(last.outcome)}" data-reason-code="${roomText(last.reason_code??'')}"><strong>Last cycle: ${roomText(roomCycleLabel(last))}</strong><small>${last.ended_at?'Ended ':''}${roomText(roomCycleTime(last.ended_at))}${roomText(detail)} · ${roomText(last.reason||'')}</small>${history}</div>`;
}
function roomHumidity(u,offline){
 const raw=Number(u.humidity_raw),index=Number(u.humidity_idx),last=offline?'Last report · ':'';
 if(u.humidity_raw!=null&&Number.isFinite(raw)&&raw>=0&&raw<=100)return `<p class="room-dry-humidity" data-status="${raw>=20?'humid':'dry'}">${last}Humidity ${Math.round(raw)}% RH · ${raw>35?'Humid':raw>=20?'At auto-dry threshold':'Dry'}</p>`;
 if(u.humidity_idx!=null&&Number.isFinite(index)&&index>=1&&index<=5)return `<p class="room-dry-humidity" data-status="${index<=2?'humid':index>=4?'dry':'unknown'}" title="AMS humidity index: 5 is driest">${last}Humidity ${Math.round(index)}/5 · ${index<=2?'Humid':index>=4?'Dry':'Moderate'}</p>`;
 return `<p class="room-dry-humidity" data-status="unknown">${last}Humidity unavailable</p>`;
}
function roomRenderDrying(){
 roomRenderFollowup();
 if($('#room-drying-content').classList.contains('management-editing')&&$('#room-drying-list').querySelector('.room-dry-unit'))return;
 const entries=PRINTERS.flatMap(p=>roomUnits(roomInfo(p.name)).map(u=>({p,u}))).sort((a,b)=>(a.u.min_pct??101)-(b.u.min_pct??101));
 const list=$('#room-drying-list'),scroller=list.closest('[data-management-card="drying"]'),scrollTop=scroller?.scrollTop||0;
 const markup=entries.map(({p,u})=>{
  const fault=u.blocked,offline=!roomOnline(roomInfo(p.name)),queued=roomUnitQueued(u);
  const wait=u.request_wait;
  const waitingPower=queued&&(wait?.kind==='shared_power'||/power/i.test(u.request_message||''));
  const status=u.heating?'Drying'+(u.dry_time>0?' · '+hm(u.dry_time)+' left':'')+(u.temp!=null?' · '+Math.round(u.temp)+'°C':''):fault?'Drying blocked':waitingPower?'Waiting for shared power':queued?({setup:'Set filament to continue',preparing:'Preparing to dry',unloading:'Retracting filament',pending:'Waiting for confirmation',waiting:'Waiting to dry'}[u.request_status]):'Heater off';
  const statusHtml=wait?.kind==='shared_power'&&queued?`Waiting for ${roomText(unitName(wait.owner))}${wait.until?' · '+dryWaitCountdown(wait.until):''}`:wait?.kind==='restart_cooldown'&&queued?`Restart pause · ${dryWaitCountdown(wait.until)}`:roomText(status);
  const delayed=!!roomInfo(p.name).refresh_delayed;
  const reason=delayed?'Refreshing the connection — tap to try again.':offline?'Printer unavailable':fault||(u.heating?'Printer controls the temperature and power':queued?(wait?.kind==='shared_power'?'No external PSU; starts when the other AMS releases power. Its reported time may change.':wait?.kind==='restart_cooldown'?'Estimated restart pause; starts when the printer allows it.':u.request_message||'Drying request queued.'):'Open this AMS to choose a drying plan.');
  return `<article class="room-dry-unit ${fault?'blocked':''}" data-dry-unit-key="${roomText(p.name)}:${u.id}" data-dry-state="${offline?'offline':fault?'blocked':u.heating?'drying':queued?'waiting':'idle'}"><div class="room-dry-identity"><h3>${roomText(p.name)} · ${roomText(unitName(u.name))}</h3><p class="room-dry-materials">${roomText(u.spools.map(s=>s.type||'Unknown').join(' · ')||'No filament set')}</p>${roomHumidity(u,offline||delayed)}</div><div class="room-dry-metrics"><div class="room-unit-title"><span>${u.min_pct!=null?Math.round(u.min_pct)+'% dry':'No tracked spools'}</span><span class="room-dry-state">${delayed?'Updating · ':offline?'Offline · ':''}${statusHtml}</span></div><div class="bar"><i style="width:${Math.max(0,Math.min(100,u.min_pct||0))}%"></i></div><p class="reason" data-routine="${!offline&&!fault&&!queued}">${roomText(reason)}</p>${roomCycleResult(u)}</div><div class="room-dry-actions"><button class="${u.heating?'primary':'is-not-drying'}" data-room-unit="${roomText(p.name)}" data-ams="${roomText(u.name)}" data-id="${u.id}"><span class="room-dry-long-label">${fault?'Retry drying':u.heating?'View drying':queued?'View request':'Plan drying'}</span><span class="room-dry-short-label">${fault?'Retry':u.heating?'Manage':queued?'Request':'Plan'} →</span></button></div></article>`;
 }).join('')||'<div class="room-empty">Waiting for AMS readings. Each unit will appear as its printer connects.</div>';
 const incoming=document.createElement('div');incoming.innerHTML=markup;
 const existing=new Map([...list.children].map(card=>[card.dataset.dryUnitKey||'empty',card]));
 const wanted=new Set();
 for(const next of [...incoming.children]){
  const key=next.dataset.dryUnitKey||'empty';wanted.add(key);
  const card=existing.get(key);
  if(!card){list.append(next);continue;}
  if(card.className!==next.className)card.className=next.className;
  if(card.dataset.dryState!==next.dataset.dryState)card.dataset.dryState=next.dataset.dryState;
  if(card.innerHTML!==next.innerHTML){
   const open=[...card.querySelectorAll('details')].map(detail=>detail.open);
   card.innerHTML=next.innerHTML;
   [...card.querySelectorAll('details')].forEach((detail,index)=>{detail.open=!!open[index];});
  }
 }
 for(const [key,card] of existing)if(!wanted.has(key))card.remove();
 if(scroller&&scroller.scrollTop!==scrollTop)scroller.scrollTop=scrollTop;
 managementPrepare();
}
const MANAGEMENT_LAYOUT_KEY='filament_management_layout_v5';
let MANAGEMENT_LAYOUT={order:[],spans:{},heights:{},positions:{}},MANAGEMENT_DRAG=null,MANAGEMENT_RESIZE=null,MANAGEMENT_FRAME=0,MANAGEMENT_OBSERVER=null;
try{const saved=JSON.parse(localStorage.getItem(MANAGEMENT_LAYOUT_KEY)||'null');if(saved&&typeof saved==='object')MANAGEMENT_LAYOUT={order:Array.isArray(saved.order)?saved.order:[],spans:saved.spans||{},heights:saved.heights||{},positions:saved.positions||{}};}catch(e){}
function managementGrid(){return $('#management-grid');}
function managementCards(){return [...managementGrid().children].filter(el=>el.dataset.managementCard);}
function managementDefaultSpan(card){return card.dataset.managementCard==='stock'?20:28;}
function managementSpan(card){return Number(card.dataset.managementSpan)||managementDefaultSpan(card);}
function managementPosition(card,position){
 const column=Number(position?.column),row=Number(position?.row);
 if(Number.isInteger(column)&&column>=1&&column<=48&&Number.isInteger(row)&&row>=1&&row<=1000){card.dataset.managementColumn=String(column);card.dataset.managementRow=String(row);card.style.setProperty('--management-column',column);card.style.setProperty('--management-row',row);}
 else{delete card.dataset.managementColumn;delete card.dataset.managementRow;card.style.removeProperty('--management-column');card.style.removeProperty('--management-row');}
}
function managementDimensions(card,span,height){
 const grid=managementGrid(),gap=parseFloat(getComputedStyle(grid).columnGap)||14,unit=(grid.clientWidth+gap)/48;
 const minimum=grid.clientWidth?Math.min(48,Math.max(4,Math.ceil((Math.min(240,grid.clientWidth)+gap)/unit))):4;
 span=Number(span);span=Number.isFinite(span)?Math.min(48,Math.max(minimum,Math.round(span))):managementDefaultSpan(card);
 card.dataset.managementSpan=String(span);card.style.setProperty('--management-span',span);
 card.dataset.managementSize=span<=16?'small':'large';
 height=Number(height);
 if(Number.isFinite(height)&&height>=100){const step=22,rounded=Math.max(118,Math.round((height+14)/step)*step-14);card.dataset.managementHeight=String(rounded);card.style.setProperty('--management-height',rounded+'px');}
 else{card.removeAttribute('data-management-height');card.style.removeProperty('--management-height');}
}
function managementEdges(card){
 if(card.querySelector(':scope > .management-edge'))return;
 const title=card.dataset.managementCard==='stock'?'Filament stock':'Drying';
 for(const [edge,label] of [['n','top'],['s','bottom'],['w','left'],['e','right'],['se','bottom-right']]){
  const handle=document.createElement('button');handle.type='button';handle.className=`management-edge management-edge-${edge}`;handle.dataset.edge=edge;
  handle.setAttribute('aria-label',`Resize ${title} from ${label} edge`);handle.title='Drag to resize · double-click to fit content · arrow keys to adjust';handle.tabIndex=-1;card.append(handle);
 }
}
function managementCell(card){return {column:Number(card.dataset.managementColumn)||1,row:Number(card.dataset.managementRow)||1,width:managementSpan(card),height:Number(card.style.gridRowEnd.replace('span ',''))||1};}
function managementResolve(commit=false){
 const occupied=[];for(const card of managementCards().filter(c=>c.dataset.managementColumn).sort((a,b)=>Number(a.dataset.managementRow)-Number(b.dataset.managementRow)||Number(a.dataset.managementColumn)-Number(b.dataset.managementColumn))){
  const cell=managementCell(card);cell.column=Math.max(1,Math.min(49-cell.width,cell.column));
  let obstacle;while((obstacle=occupied.find(other=>layoutOverlap(cell,other))))cell.row=obstacle.row+obstacle.height;
  if(commit)managementPosition(card,cell);else{card.style.setProperty('--management-column',cell.column);card.style.setProperty('--management-row',cell.row);}
  occupied.push(cell);
 }
}
function managementMeasure(){
 const grid=managementGrid();if(!grid?.offsetWidth)return;
 const style=getComputedStyle(grid),row=parseFloat(style.gridAutoRows)||8,gap=parseFloat(style.rowGap)||14;
 for(const card of managementCards()){const rows=Math.max(1,Math.ceil((card.getBoundingClientRect().height+gap)/(row+gap)));const value=`span ${rows}`;if(card.style.gridRowEnd!==value)card.style.gridRowEnd=value;}
 managementResolve();
}
function managementQueueMeasure(){if(MANAGEMENT_FRAME)return;MANAGEMENT_FRAME=requestAnimationFrame(()=>{MANAGEMENT_FRAME=0;managementMeasure();});}
function managementSave(){
 managementMeasure();managementResolve(true);
 const cards=managementCards();MANAGEMENT_LAYOUT={order:cards.map(c=>c.dataset.managementCard),spans:Object.fromEntries(cards.map(c=>[c.dataset.managementCard,managementSpan(c)])),heights:Object.fromEntries(cards.filter(c=>c.dataset.managementHeight).map(c=>[c.dataset.managementCard,Number(c.dataset.managementHeight)])),positions:Object.fromEntries(cards.filter(c=>c.dataset.managementColumn).map(c=>[c.dataset.managementCard,{column:Number(c.dataset.managementColumn),row:Number(c.dataset.managementRow)}]))};
 try{localStorage.setItem(MANAGEMENT_LAYOUT_KEY,JSON.stringify(MANAGEMENT_LAYOUT));}catch(e){}
}
function managementPrepare(){
 const grid=managementGrid();if(!grid)return;
 MANAGEMENT_OBSERVER?.disconnect();
 const cards=managementCards(),ordered=[...MANAGEMENT_LAYOUT.order.map(key=>cards.find(c=>c.dataset.managementCard===key)).filter(Boolean),...cards.filter(c=>!MANAGEMENT_LAYOUT.order.includes(c.dataset.managementCard))];
 ordered.forEach((card,index)=>{if(grid.children[index]!==card)grid.insertBefore(card,grid.children[index]||null);});
 for(const card of managementCards()){
  const key=card.dataset.managementCard;managementEdges(card);
  managementDimensions(card,MANAGEMENT_LAYOUT.spans[key]??managementDefaultSpan(card),MANAGEMENT_LAYOUT.heights[key]);managementPosition(card,MANAGEMENT_LAYOUT.positions[key]);
  card.querySelectorAll(':scope > .management-edge').forEach(edge=>edge.tabIndex=$('#room-drying-content').classList.contains('management-editing')?0:-1);
  MANAGEMENT_OBSERVER?.observe(card);
 }
 managementQueueMeasure();
}
function managementPin(){
 managementMeasure();const grid=managementGrid(),bounds=grid.getBoundingClientRect(),style=getComputedStyle(grid),unit=(grid.clientWidth+(parseFloat(style.columnGap)||14))/48,step=(parseFloat(style.gridAutoRows)||8)+(parseFloat(style.rowGap)||14);
 const positions=managementCards().map(card=>{const r=card.getBoundingClientRect();return [card,{column:Math.max(1,Math.round((r.left-bounds.left)/unit)+1),row:Math.max(1,Math.round((r.top-bounds.top)/step)+1)}];});
 positions.forEach(([card,position])=>managementPosition(card,position));
}
function managementPeerRects(card){const origin=managementGrid().getBoundingClientRect();return managementCards().filter(c=>c!==card).map(c=>{const r=c.getBoundingClientRect();return {left:r.left-origin.left,right:r.right-origin.left,top:r.top-origin.top,bottom:r.bottom-origin.top,width:r.width,height:r.height};});}
function managementClearPreview(){managementGrid()?.querySelector('.management-placement')?.remove();}
function managementDropPreview(event){
 const drag=MANAGEMENT_DRAG,grid=managementGrid(),bounds=grid.getBoundingClientRect(),style=getComputedStyle(grid),gap=parseFloat(style.columnGap)||14,rowGap=parseFloat(style.rowGap)||14,unit=(grid.clientWidth+gap)/48,step=(parseFloat(style.gridAutoRows)||8)+rowGap;
 const cell=managementCell(drag.card),peers=managementPeerRects(drag.card),width=cell.width*unit-gap,height=drag.card.getBoundingClientRect().height;
 let left=event.clientX-drag.grabX-bounds.left,top=event.clientY-drag.grabY-bounds.top;
 if(!event.altKey){left=layoutSnapCoordinate(left,[0,width/2,width],peers.flatMap(r=>[r.left,(r.left+r.right)/2,r.right]),v=>Math.round(v/unit)*unit,0,(48-cell.width)*unit);left=layoutSnapCoordinate(left,[0,width],peers.flatMap(r=>[r.right+gap,r.left-gap]),v=>Math.round(v/unit)*unit,0,(48-cell.width)*unit);top=layoutSnapCoordinate(top,[0,height/2,height],peers.flatMap(r=>[r.top,(r.top+r.bottom)/2,r.bottom]),v=>Math.round(v/step)*step,0,999*step);top=layoutSnapCoordinate(top,[0,height],peers.flatMap(r=>[r.bottom+rowGap,r.top-rowGap]),v=>Math.round(v/step)*step,0,999*step);}
 cell.column=Math.max(1,Math.min(49-cell.width,Math.round(left/unit)+1));cell.row=Math.max(1,Math.min(1000,Math.round(top/step)+1));
 const blocked=managementCards().some(c=>c!==drag.card&&layoutOverlap(cell,managementCell(c)));
 drag.destination=event.clientX>=bounds.left&&event.clientX<=bounds.right&&event.clientY>=bounds.top&&!blocked?cell:null;
 let marker=grid.querySelector('.management-placement');if(!marker){marker=document.createElement('div');marker.className='management-placement';grid.append(marker);}
 Object.assign(marker.style,{left:((cell.column-1)*unit)+'px',top:((cell.row-1)*step)+'px',width:(cell.width*unit-gap)+'px',height:(cell.height*step-rowGap)+'px'});marker.classList.toggle('invalid',!drag.destination);marker.textContent=drag.destination?'Move here':'Not enough room';
 if(drag.destination){const r=drag.card.getBoundingClientRect(),transform=new DOMMatrixReadOnly(getComputedStyle(drag.card).transform),x=(cell.column-1)*unit,y=(cell.row-1)*step;drag.card.style.transform=`translate3d(${x-(r.left-bounds.left-transform.m41)}px,${y-(r.top-bounds.top-transform.m42)}px,0)`;}
}
function managementToggle(){
 const content=$('#room-drying-content'),editing=content.classList.toggle('management-editing');const button=$('#management-edit');button.textContent=editing?'Done editing':'Edit layout';button.setAttribute('aria-pressed',String(editing));$('#management-reset').hidden=!editing;
 managementCards().forEach(card=>card.tabIndex=editing?0:-1);content.querySelectorAll('.management-edge').forEach(edge=>edge.tabIndex=editing?0:-1);
 if(!editing){managementClearDrag();managementEndResize(null,true);roomRenderDrying();}managementQueueMeasure();
}
function managementReset(){try{localStorage.removeItem(MANAGEMENT_LAYOUT_KEY);}catch(e){}MANAGEMENT_LAYOUT={order:[],spans:{},heights:{},positions:{}};const grid=managementGrid(),stock=grid.querySelector('[data-management-card="stock"]');if(stock)grid.prepend(stock);managementPrepare();}
function managementClearDrag(){const drag=MANAGEMENT_DRAG;if(!drag)return;MANAGEMENT_DRAG=null;drag.card.classList.remove('management-dragging');drag.card.style.transform='';managementClearPreview();try{if(drag.card.hasPointerCapture(drag.id))drag.card.releasePointerCapture(drag.id);}catch(e){}}
function managementEndResize(event,cancel=false){const drag=MANAGEMENT_RESIZE;if(!drag||(event?.pointerId!=null&&event.pointerId!==drag.id))return;MANAGEMENT_RESIZE=null;if(cancel)managementDimensions(drag.card,drag.beforeSpan,drag.beforeHeight);else if(drag.moved)managementSave();drag.card.querySelector(':scope > .management-dimensions')?.remove();managementQueueMeasure();}
function managementInit(){
 const content=$('#room-drying-content'),stock=content?.querySelector(':scope > .stock-panel');if(!content||!stock)return;
 const toolbar=document.createElement('div');toolbar.className='management-toolbar';toolbar.innerHTML='<button type="button" id="management-reset" hidden>Reset layout</button><button type="button" id="management-edit" aria-pressed="false">Edit layout</button>';
 const grid=document.createElement('div');grid.id='management-grid';const drying=document.createElement('section');drying.className='management-drying';drying.dataset.managementCard='drying';stock.dataset.managementCard='stock';
 [...content.children].filter(el=>el!==stock).forEach(el=>drying.append(el));grid.append(stock,drying);content.prepend(toolbar,grid);
 $('#management-edit').addEventListener('click',managementToggle);$('#management-reset').addEventListener('click',managementReset);
 MANAGEMENT_OBSERVER=new ResizeObserver(managementQueueMeasure);
 content.addEventListener('click',event=>{if(content.classList.contains('management-editing')&&event.target.closest('[data-management-card]')&&!event.target.closest('.management-edge')){event.preventDefault();event.stopImmediatePropagation();}},true);
 content.addEventListener('pointerdown',event=>{if(window.innerWidth<900||!content.classList.contains('management-editing')||!event.isPrimary||event.button!==0||MANAGEMENT_RESIZE||event.target.closest('.management-edge'))return;const card=event.target.closest('[data-management-card]');if(!card||card.parentElement!==grid)return;
  const r=card.getBoundingClientRect();MANAGEMENT_DRAG={card,id:event.pointerId,x:event.clientX,y:event.clientY,grabX:event.clientX-r.left,grabY:event.clientY-r.top,moved:false,destination:null};card.setPointerCapture(event.pointerId);event.preventDefault();});
 content.addEventListener('pointermove',event=>{const drag=MANAGEMENT_DRAG;if(!drag||drag.id!==event.pointerId)return;const dx=event.clientX-drag.x,dy=event.clientY-drag.y;if(!drag.moved&&Math.hypot(dx,dy)<6)return;if(!drag.moved)managementPin();drag.moved=true;drag.card.classList.add('management-dragging');drag.card.style.transform=`translate3d(${dx}px,${dy}px,0)`;managementDropPreview(event);event.preventDefault();});
 const finishDrag=event=>{const drag=MANAGEMENT_DRAG;if(!drag||drag.id!==event.pointerId)return;if(event.type==='pointerup'&&drag.moved&&drag.destination){managementPosition(drag.card,drag.destination);managementSave();}managementClearDrag();managementQueueMeasure();};content.addEventListener('pointerup',finishDrag);content.addEventListener('pointercancel',finishDrag);
 content.addEventListener('pointerdown',event=>{const handle=event.target.closest('.management-edge');if(window.innerWidth<900||!handle||!content.classList.contains('management-editing')||!event.isPrimary||event.button!==0||MANAGEMENT_DRAG)return;const card=handle.closest('[data-management-card]'),style=getComputedStyle(grid),gap=parseFloat(style.columnGap)||14,rowGap=parseFloat(style.rowGap)||14,bounds=card.getBoundingClientRect();
  MANAGEMENT_RESIZE={handle,card,id:event.pointerId,edge:handle.dataset.edge,x:event.clientX,y:event.clientY,width:bounds.width,height:bounds.height,unit:(grid.clientWidth+gap)/48,step:(parseFloat(style.gridAutoRows)||8)+rowGap,gap,rowGap,beforeSpan:managementSpan(card),beforeHeight:card.dataset.managementHeight||null,moved:false};handle.setPointerCapture(event.pointerId);event.preventDefault();});
 content.addEventListener('pointermove',event=>{const drag=MANAGEMENT_RESIZE;if(!drag||drag.id!==event.pointerId)return;const dx=event.clientX-drag.x,dy=event.clientY-drag.y;if(!drag.moved&&Math.hypot(dx,dy)<3)return;drag.moved=true;let span=drag.beforeSpan,height=drag.beforeHeight;
  if(/[ew]/.test(drag.edge)){const proposed=drag.width+dx*(drag.edge.includes('w')?-1:1);span=Math.round((proposed+drag.gap)/drag.unit);const origin=grid.getBoundingClientRect(),left=drag.card.getBoundingClientRect().left-origin.left,peers=managementPeerRects(drag.card),right=left+span*drag.unit-drag.gap;if(!event.altKey){const snap=layoutSnapCoordinate(right,[0],peers.flatMap(r=>[r.right,r.left-drag.gap]),v=>left+Math.round((v-left+drag.gap)/drag.unit)*drag.unit-drag.gap,left+240,grid.clientWidth);span=Math.round((snap-left+drag.gap)/drag.unit);}}
  if(/[ns]/.test(drag.edge)){const raw=drag.height+dy*(drag.edge.includes('n')?-1:1);height=Math.max(118,Math.round((raw+drag.rowGap)/drag.step)*drag.step-drag.rowGap);}
  managementDimensions(drag.card,span,height);managementQueueMeasure();let badge=drag.card.querySelector(':scope > .management-dimensions');if(!badge){badge=document.createElement('output');badge.className='management-dimensions';drag.card.append(badge);}badge.textContent=`${Math.round(drag.card.getBoundingClientRect().width)} × ${Math.round(drag.card.getBoundingClientRect().height)}`;event.preventDefault();});
 content.addEventListener('pointerup',event=>managementEndResize(event));content.addEventListener('pointercancel',event=>managementEndResize(event,true));
 content.addEventListener('dblclick',event=>{const handle=event.target.closest('.management-edge');if(!handle||!content.classList.contains('management-editing'))return;const card=handle.closest('[data-management-card]');managementDimensions(card,/[ew]/.test(handle.dataset.edge)?managementDefaultSpan(card):managementSpan(card),/[ns]/.test(handle.dataset.edge)?null:card.dataset.managementHeight);managementSave();event.preventDefault();});
 content.addEventListener('keydown',event=>{const handle=event.target.closest('.management-edge');if(!handle||!content.classList.contains('management-editing'))return;const card=handle.closest('[data-management-card]'),edge=handle.dataset.edge;if(event.key==='Home'){managementDimensions(card,managementDefaultSpan(card),null);managementSave();event.preventDefault();return;}const horizontal=/[ew]/.test(edge)&&['ArrowLeft','ArrowRight'].includes(event.key),vertical=/[ns]/.test(edge)&&['ArrowUp','ArrowDown'].includes(event.key);if(!horizontal&&!vertical)return;const direction=['ArrowRight','ArrowDown'].includes(event.key)?1:-1;if(horizontal)managementDimensions(card,managementSpan(card)+direction*(edge.includes('w')?-1:1),card.dataset.managementHeight);else managementDimensions(card,managementSpan(card),card.getBoundingClientRect().height+direction*22*(edge.includes('n')?-1:1));managementSave();event.preventDefault();});
 window.addEventListener('resize',managementQueueMeasure);managementPrepare();
}
let STOCK={items:[],products:[],low:[]},STOCK_BUSY=false;
async function stockPoll(){
 if(STOCK_BUSY)return;STOCK_BUSY=true;
 try{STOCK=await dashboardJSON('/api/filament_stock',8000);stockRender();}
 catch(e){if(ROOM_VIEW==='drying')$('#stock-message').textContent='Stock is reconnecting…';}
 finally{STOCK_BUSY=false;}
}
function stockRender(){
 const items=STOCK.items||[];
 const low=items.filter(x=>x.low),unavailable=low.filter(x=>['out_of_stock','not_listed','different_product'].includes(x.spider?.status));
 $('#stock-alert').hidden=!low.length;
 $('#stock-alert').textContent=low.length?'Order soon: '+low.map(x=>x.name+' ('+x.unopened+' left)').join(' · ')+(unavailable.length?' · Unavailable at configured store: '+unavailable.map(x=>x.name).join(', '):''):'';
 $('#stock-rows').innerHTML=items.map(x=>{
  const spider=x.spider||{},availability={available:'Available at configured store',out_of_stock:'Out of stock at configured store',not_listed:'No exact listing at configured store',different_product:'Listing changed; no exact match',unknown:'Could not check configured store',checking:'Checking configured store…'}[spider.status]||'Checking configured store…';
  const cart=x.low&&spider.status==='available'&&spider.cart_url?`<a class="stock-cart" href="${roomText(spider.cart_url)}" target="_blank" rel="noopener noreferrer" title="Order the ${roomText(x.purchase_variant||'selected')} variant; stock counts all colors">Cart ↗</a>`:x.low?`<button type="button" disabled title="${roomText(availability)}">Cart unavailable</button>`:'';
  const listing=x.low&&spider.product_url&&spider.status!=='available'?` <a href="${roomText(spider.product_url)}" target="_blank" rel="noopener noreferrer">View listing</a>`:'';
  return `<tr><td><strong>${roomText(x.name)}</strong><small style="display:block;color:var(--mute);margin-top:3px">All colors pooled${x.purchase_variant?' · cart: '+roomText(x.purchase_variant):''}</small></td><td><strong>${x.unopened} left</strong></td><td>${x.threshold==null?'—':x.threshold}</td><td class="${x.low?'stock-low':'stock-ok'}">${x.low?'Order now · '+availability:x.threshold==null?'No alert set':'OK'}</td><td>${cart}${listing} <button type="button" data-stock-edit="${roomText(x.id)}">Edit</button> <button type="button" data-stock-remove="${roomText(x.id)}">Remove</button></td></tr>`;
 }).join('')||'<tr><td colspan="5" class="stock-empty">No stock items yet. Choose a filament above and enter how many spools you have.</td></tr>';
}
function stockProductOptions(selected=''){
 const select=$('#stock-form').elements.product_key;
 select.innerHTML='<option value="">Choose a filament…</option>'+(STOCK.products||[]).map(x=>`<option value="${roomText(x.key)}">${roomText(x.name)}</option>`).join('');
 select.value=selected;
}
function stockNew(){const f=$('#stock-form');f.reset();f.elements.id.value='';stockProductOptions();f.hidden=false;f.elements.product_key.focus();}
function stockClose(){$('#stock-form').hidden=true;$('#stock-message').textContent='';}
function stockEdit(id){const x=STOCK.items.find(i=>i.id===id);if(!x)return;const f=$('#stock-form');f.hidden=false;stockProductOptions(x.product_key);for(const key of ['id','unopened','threshold'])f.elements[key].value=x[key]??'';f.scrollIntoView({block:'nearest',behavior:'smooth'});}
async function stockAction(payload){const r=await fetch('/api/filament_stock',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});const data=await r.json();if(!r.ok)throw new Error(data.error||'Could not update stock');STOCK=data;stockRender();return data;}
async function stockSave(event){event.preventDefault();const f=event.currentTarget,fields=Object.fromEntries(new FormData(f));try{await stockAction({action:'save',...fields});stockClose();}catch(e){$('#stock-message').textContent=e.message;}}
document.addEventListener('click',async e=>{const edit=e.target.closest('[data-stock-edit]');if(edit)stockEdit(edit.dataset.stockEdit);const remove=e.target.closest('[data-stock-remove]');if(remove){const item=STOCK.items.find(x=>x.id===remove.dataset.stockRemove);if(!item||!confirm('Remove '+item.name+' from stock?'))return;try{await stockAction({action:'remove',id:item.id});$('#stock-message').textContent='Stock item removed.';}catch(err){$('#stock-message').textContent=err.message;}}});
async function roomOpenUnit(name,ams,id){
 selectPrinter(name,true);const ok=await load();if(!ok||P!==name)return;
 const unit=[...document.querySelectorAll('#slots .ams')].find(el=>el.dataset.ams===ams);unit?.scrollIntoView({behavior:'smooth',block:'start'});
 const rows=D.slots.filter(r=>r.ams===ams);
 if((D.units[ams]||{}).dry_pending||roomUnits(roomInfo(name)).some(u=>String(u.id)===String(id)&&roomUnitQueued(u)))return;
 if(rows.length&&!rows.some(r=>r.heating)&&rows.some(r=>r.spool))dryDlg(id,ams);
}
// Keep one persistent canvas per printer and always paint the newest complete JPEG.
// A long playback queue makes every camera lag and rebuilds that lag after tab switches.
const CAMERA_BUFFERS=new WeakMap(),CAMERA_CACHE=new Map(),CAMERA_BUFFER_MS=600;
let CAMERA_FULLSCREEN=null;
function cameraExpandIcon(){return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><path d="M8 3H3v5m13-5h5v5M3 16v5h5m13-5v5h-5"/></svg>';}
function cameraFullscreenPaint(s){
 const f=CAMERA_FULLSCREEN;if(!f||f.key!==s.key)return;
 f.status.textContent=s.message||'Connecting camera…';
 if(!s.shown)return;
 if(f.canvas.width!==s.canvas.width||f.canvas.height!==s.canvas.height){f.canvas.width=s.canvas.width;f.canvas.height=s.canvas.height;}
 f.context.drawImage(s.canvas,0,0);f.wait.hidden=true;
}
function closeCameraFullscreen(){
 const f=CAMERA_FULLSCREEN;if(!f)return;CAMERA_FULLSCREEN=null;
 if(document.fullscreenElement===f.dialog)document.exitFullscreen().catch(()=>{});
 f.dialog.close();f.dialog.remove();document.body.style.overflow=f.overflow;f.trigger?.focus?.();
}
function openCameraFullscreen(name){
 if(!name)return;closeCameraFullscreen();
 const key=`/api/p/${encodeURIComponent(name)}/api/cam.mjpg`,dialog=document.createElement('dialog');
 dialog.id='camera-fullscreen';dialog.setAttribute('aria-label',`${name} fullscreen camera`);
 dialog.innerHTML='<div class="fullscreen-toolbar"><strong></strong><button type="button" autofocus>× Close</button></div><div class="fullscreen-picture"><canvas role="img"></canvas><span class="fullscreen-wait">Connecting camera…</span><span class="fullscreen-status" role="status"></span></div>';
 dialog.querySelector('strong').textContent=name;const canvas=dialog.querySelector('canvas');canvas.setAttribute('aria-label',`${name} camera`);
 CAMERA_FULLSCREEN={key,dialog,canvas,context:canvas.getContext('2d',{alpha:false}),wait:dialog.querySelector('.fullscreen-wait'),status:dialog.querySelector('.fullscreen-status'),overflow:document.body.style.overflow,trigger:document.activeElement};
 dialog.querySelector('button').onclick=closeCameraFullscreen;
 dialog.addEventListener('cancel',e=>{e.preventDefault();closeCameraFullscreen();});
 document.body.append(dialog);document.body.style.overflow='hidden';dialog.showModal();
 // iPhone browsers without element fullscreen use the same full-viewport viewer.
 if(dialog.requestFullscreen)dialog.requestFullscreen().then(()=>{if(CAMERA_FULLSCREEN?.dialog!==dialog&&document.fullscreenElement===dialog)document.exitFullscreen().catch(()=>{});}).catch(()=>{});
 const session=CAMERA_CACHE.get(key);
 if(session&&!session.closed)cameraFullscreenPaint(session);
 else CAMERA_FULLSCREEN.wait.textContent='Start this camera to view its stream.';
}
document.addEventListener('fullscreenchange',()=>{const f=CAMERA_FULLSCREEN;if(!f)return;if(document.fullscreenElement===f.dialog)f.native=true;else if(f.native)closeCameraFullscreen();});
document.addEventListener('click',e=>{const b=e.target.closest('[data-camera-fullscreen]');if(b){e.preventDefault();openCameraFullscreen(b.dataset.cameraFullscreen);}});
document.addEventListener('click',e=>{const b=e.target.closest('[data-movement-window]');if(b){e.preventDefault();openMovementWindow(b.dataset.movementWindow);}});

function cameraBufferNote(img){return img?.closest('.room-feed,.camera-stage')?.querySelector('.room-camera-note')||(img?.id==='camimg'?$('#camnote'):null);}
// Navigation releases only the display. The printer owns its stream and queue.
function cameraBufferStop(img){
 const s=CAMERA_BUFFERS.get(img);if(!s)return;
 CAMERA_BUFFERS.delete(img);s.pause?.();s.canvas.remove();img.classList.remove('camera-buffer-anchor');img.removeAttribute('src');
 if(s.img===img){s.img=null;s.onFailure=null;}
}
function cameraBufferDestroy(s){
 if(!s||s.closed)return;
 if(s.img)cameraBufferStop(s.img);
 CAMERA_CACHE.delete(s.key);s.closed=true;s.controller.abort();s.pause?.();clearTimeout(s.retryTimer);s.resolveRetry?.();clearInterval(s.watchdog);
 s.frames.length=0;s.bytes=0;for(const url of s.urls)URL.revokeObjectURL(url);s.urls.clear();s.canvas?.remove();
}
function cameraStopAll(){for(const s of [...CAMERA_CACHE.values()])cameraBufferDestroy(s);}
function cameraStopPrinter(name){for(const s of [...CAMERA_CACHE.values()])if(s.key===`/api/p/${encodeURIComponent(name)}/api/cam.mjpg`)cameraBufferDestroy(s);}
function cameraBufferAttach(s,img,onFailure){
 if(s.img===img){s.onFailure=onFailure;return;}
 if(s.img)cameraBufferStop(s.img);
 cameraBufferStop(img);cameraFadeReset(img);img.onload=null;img.onerror=null;
 s.img=img;s.onFailure=onFailure;CAMERA_BUFFERS.set(img,s);img.after(s.canvas);img.classList.add('camera-buffer-anchor');
 s.canvas.setAttribute('aria-label',img.alt||'Buffered printer camera');
 const shell=img.closest('.room-feed,.camera-stage'),placeholder=shell?.querySelector('.room-cam-empty,.camera-placeholder');
 if(s.shown){s.canvas.style.transition='none';shell?.classList.add('camera-ready');if(placeholder)placeholder.hidden=true;}
 const note=cameraBufferNote(img);if(note)note.textContent=s.message||'Building a smooth stream…';
}
function cameraBufferStart(img,url,onFailure){
 const key=new URL(url,location.href).pathname,cached=CAMERA_CACHE.get(key);
 if(cached&&!cached.closed){cameraBufferAttach(cached,img,onFailure);cached.foreground?.();return;}
 if(cached)cameraBufferDestroy(cached);
 const canvas=document.createElement('canvas');canvas.className='camera-surface camera-fading';canvas.setAttribute('role','img');
 const context=canvas.getContext('2d',{alpha:false});if(!context){onFailure?.();return;}
 const snapshotURL=url.replace('/api/cam.mjpg','/api/cam.jpg').split('?')[0];
 const s={key,canvas,controller:new AbortController(),urls:new Set(),frames:[],bytes:0,closed:false,shown:false,backgrounded:false,inflight:false,timer:0,lastFrame:0,message:'Connecting camera…',failures:0};
 CAMERA_CACHE.set(key,s);cameraBufferAttach(s,img,onFailure);
 const active=()=>!s.closed&&CAMERA_CACHE.get(key)===s;
 const label=text=>{if(s.message===text)return;s.message=text;const note=cameraBufferNote(s.img);if(note)note.textContent=text;cameraFullscreenPaint(s);};
 const delay=()=>s.img?.closest('.room-feed')?180:100;
 const schedule=ms=>{clearTimeout(s.timer);s.timer=0;if(active()&&!s.backgrounded&&!document.hidden&&s.img)s.timer=setTimeout(poll,ms);};
 s.pause=()=>{s.backgrounded=true;clearTimeout(s.timer);s.timer=0;s.controller.abort();};
 s.resume=()=>{if(active()&&!document.hidden&&s.img&&!s.inflight&&!s.timer){s.backgrounded=false;s.timer=setTimeout(poll,0);}};
 s.foreground=()=>{s.backgrounded=false;s.resume();};
 async function poll(){
  s.timer=0;if(!active()||s.backgrounded||document.hidden||!s.img||s.inflight)return;
  s.inflight=true;const controller=new AbortController();s.controller=controller;
  const timeout=setTimeout(()=>controller.abort(),5000);let next=delay(),objectURL='';
  try{
   // A finite request releases its browser connection after each cached frame.
   // Eight permanent MJPEG requests exhausted Chrome's per-origin connection pool.
   const response=await fetch(snapshotURL,{cache:'no-store',signal:controller.signal});
   if(response.status===204){next=400;return;}
   if(!response.ok)throw new Error('Camera snapshot unavailable');
   const blob=await response.blob();if(blob.size<100)throw new Error('Empty camera snapshot');
   objectURL=URL.createObjectURL(blob);s.urls.add(objectURL);
   const decoded=new Image();decoded.src=objectURL;await decoded.decode();
   if(!active()||s.backgrounded||document.hidden||!s.img)return;
   if(canvas.width!==decoded.naturalWidth||canvas.height!==decoded.naturalHeight){canvas.width=decoded.naturalWidth;canvas.height=decoded.naturalHeight;}
   context.drawImage(decoded,0,0,canvas.width,canvas.height);
   s.lastFrame=performance.now();s.failures=0;
   if(!s.shown){s.shown=true;const shell=s.img.closest('.room-feed,.camera-stage');shell?.classList.add('camera-ready');const placeholder=shell?.querySelector('.room-cam-empty,.camera-placeholder');if(placeholder)placeholder.hidden=true;}
   label('Live');
  }catch(e){
   if(!controller.signal.aborted){s.failures++;next=Math.min(1500,300*s.failures);if(!s.shown||performance.now()-s.lastFrame>8000)label('Camera reconnecting…');}
  }finally{
   clearTimeout(timeout);if(objectURL){URL.revokeObjectURL(objectURL);s.urls.delete(objectURL);}
   s.inflight=false;schedule(next);
  }
 }
 s.timer=setTimeout(poll,Math.random()*240);
}

const CAMERA_REVEALS=new WeakMap();
function cameraFadeReset(img){
 CAMERA_REVEALS.get(img)?.();CAMERA_REVEALS.delete(img);
 const shell=img.closest('.room-feed,.camera-stage');shell?.classList.remove('camera-ready');
 const placeholder=shell?.querySelector('.room-cam-empty,.camera-placeholder');if(placeholder)placeholder.hidden=false;
}
function cameraFadeBegin(img){
 cameraFadeReset(img);img.classList.add('camera-fading');
 const shell=img.closest('.room-feed,.camera-stage'),url=img.getAttribute('src'),started=Date.now();let timer=0,finished=false;
 const cancel=()=>{clearTimeout(timer);img.removeEventListener('load',reveal);img.removeEventListener('error',failed);finished=true;};
 const reveal=()=>{
  if(finished||!img.isConnected||img.getAttribute('src')!==url)return;
  if(img.naturalWidth>0&&img.naturalHeight>0){
   shell?.classList.add('camera-ready');
   const note=shell?.querySelector('.room-camera-note')||(img.id==='camimg'?$('#camnote'):null);
   if(note)note.textContent=img.id==='camimg'?'Live camera':'Live stream';
   cancel();CAMERA_REVEALS.delete(img);
  }
 };
 const failed=()=>{cameraFadeReset(img);};
 const check=()=>{
  if(finished)return;
  if(!img.isConnected||document.hidden||img.getAttribute('src')!==url){cancel();CAMERA_REVEALS.delete(img);return;}
  reveal();
  // MJPEG may decode its first frame without completing the image load event.
  if(!finished&&Date.now()-started<25000)timer=setTimeout(check,150);
 };
 CAMERA_REVEALS.set(img,cancel);img.addEventListener('load',reveal);img.addEventListener('error',failed);
 timer=setTimeout(check,150);
}
function roomStopStream(name){const stream=ROOM_STREAMS.get(name);if(!stream)return;cameraBufferStop(stream.img);cameraFadeReset(stream.img);stream.img.onload=null;stream.img.onerror=null;stream.img.removeAttribute('src');ROOM_STREAMS.delete(name);}
function roomStopSnapshots(){for(const c of ROOM_ABORTS)c.abort();ROOM_ABORTS.clear();for(const name of [...ROOM_STREAMS.keys()])roomStopStream(name);}
function roomDisposeImages(){roomStopSnapshots();dropCam();cameraStopAll();}
function roomConnectStream(name,el,stream){
 const img=stream.img,url=`/api/p/${encodeURIComponent(name)}/api/cam.mjpg?t=${Date.now()}`;stream.url=url;stream.started=Date.now();stream.failed=false;
 const valid=()=>ROOM_STREAMS.get(name)===stream&&stream.url===url;
 cameraBufferStart(img,url,()=>{if(valid())roomStreamFailed(el,stream);});
}
function roomStreamFailed(el,stream){
 if(stream.failed)return;stream.failed=true;stream.retries++;stream.retryAt=Date.now()+20000;cameraBufferStop(stream.img);cameraFadeReset(stream.img);stream.img.onload=null;stream.img.onerror=null;stream.img.removeAttribute('src');
 el.querySelector('.room-cam-empty').hidden=false;el.querySelector('.room-cam-empty').textContent=stream.retries>=5?'Camera unavailable · pause and resume cameras to retry':'Camera reconnecting…';el.querySelector('.room-camera-note').textContent=stream.retries>=5?'Camera unavailable':'Reconnecting…';
}
async function roomSnapshots(){
 if(ROOM_VIEW!=='watch'||!ROOM_CAMERAS||DLG||document.hidden||document.readyState!=='complete')return;
 // MQTT telemetry can briefly disconnect while the RTSP camera still works.
 // Do not blank or restart a camera because a status poll was delayed.
 let visible=PRINTERS.map(p=>p.name);
 // Warm all printer streams once; switching views only reattaches their canvas.
 const names=new Set(visible);
 for(const name of [...ROOM_STREAMS.keys()])if(!names.has(name)||!ROOM_STREAMS.get(name).img.isConnected)roomStopStream(name);
 const queue=[];
 for(const name of names){const el=[...$('#room-wall').children].find(x=>x.dataset.printer===name);if(!el)continue;
  let stream=ROOM_STREAMS.get(name);
  if(!stream){stream={img:el.querySelector('img'),retries:0,checked:Date.now(),failed:false};ROOM_STREAMS.set(name,stream);roomConnectStream(name,el,stream);}
  else if(stream.failed&&stream.retries<5&&Date.now()>=stream.retryAt)roomConnectStream(name,el,stream);
  if(!stream.failed&&Date.now()-stream.checked>=10000)queue.push({name,el,stream});
 }
 if(ROOM_SNAP_BUSY||!queue.length)return;ROOM_SNAP_BUSY=true;
 const worker=async()=>{while(queue.length&&ROOM_VIEW==='watch'&&ROOM_CAMERAS&&!document.hidden){const {name,el,stream}=queue.shift();stream.checked=Date.now();const ctrl=new AbortController();ROOM_ABORTS.add(ctrl);const timer=setTimeout(()=>ctrl.abort(),4000);
  try{const r=await fetch(`/api/p/${encodeURIComponent(name)}/api/cam_status`,{cache:'no-store',signal:ctrl.signal});if(!r.ok)continue;const status=await r.json();if(ROOM_STREAMS.get(name)!==stream||stream.failed)continue;
   if(CAMERA_BUFFERS.has(stream.img)){if(CAMERA_BUFFERS.get(stream.img).shown)stream.retries=0;continue;}
   if(status.running&&status.age<10&&stream.img.naturalWidth>0){stream.retries=0;if(!stream.img.closest('.room-feed').classList.contains('camera-ready'))cameraFadeBegin(stream.img);el.querySelector('.room-camera-note').textContent='Live stream';}
   else if(Date.now()-stream.started>45000)roomStreamFailed(el,stream);
  }catch(e){}finally{clearTimeout(timer);ROOM_ABORTS.delete(ctrl);}
 }};
 try{await Promise.all([worker(),worker()]);}finally{ROOM_SNAP_BUSY=false;}
}
document.addEventListener('click',e=>{const b=e.target.closest('[data-room-open],[data-room-alert],[data-room-unit]');if(!b)return;if(b.dataset.roomUnit)roomOpenUnit(b.dataset.roomUnit,b.dataset.ams,Number(b.dataset.id));else roomOpen(b.dataset.roomOpen||b.dataset.roomAlert,b.dataset.roomAlert?'alerts':null);});
document.addEventListener('visibilitychange',()=>{if(document.hidden){for(const s of CAMERA_CACHE.values())s.pause?.();}else{for(const s of CAMERA_CACHE.values())s.foreground?.();tick();load();roomSnapshots();startCamWhenReady();}});
window.addEventListener('online',()=>{tick();load();});
window.addEventListener('pageshow',e=>{if(e.persisted){tick();load();roomSnapshots();startCamWhenReady();}});
window.addEventListener('pagehide',roomDisposeImages);window.addEventListener('beforeunload',roomDisposeImages);window.addEventListener('load',roomSnapshots);setInterval(roomSnapshots,2000);

const RFID_REQUESTED=new Map();
async function readRfid(ams_id,slot_id){
 const printer=P,key=printer+':'+ams_id+':'+slot_id;
 try{
  const r=await api('/api/read_rfid',{ams_id,slot_id});
  if(r.result!=='ok'){alert(r.result);return;}
  RFID_REQUESTED.set(key,Date.now()+12000);
  await load();nudge();
  setTimeout(()=>{if(RFID_REQUESTED.has(key)){RFID_REQUESTED.delete(key);if(P===printer)load();}},12000);
 }catch(e){alert('Could not request an RFID read: '+e.message);}
}
async function nudge(){setTimeout(tick,400);setTimeout(tick,1500);}
function checkLogin(response){if(response.status===401){location.replace('/login');throw new Error('Please sign in.');}}
async function api(path,body){const url=P?`/api/p/${encodeURIComponent(P)}${path}`:path;const r=await fetch(url,body?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:{});checkLogin(r);if(!r.ok)throw new Error(`HTTP ${r.status} from ${url}`);const t=await r.text();try{return JSON.parse(t)}catch(e){throw new Error(`bad JSON from ${url}: ${t.slice(0,80)}`)}}
// Bound read requests; a stalled connection must not queue endless polling work.
const UI_VERSION='__AMS_UI_VERSION__';let UI_UPDATE_PENDING=false;
function applyDashboardUpdate(){
 if(!UI_UPDATE_PENDING||DLG||editing())return;
 UI_UPDATE_PENDING=false;dropCam();location.reload();
}
async function dashboardJSON(url,timeout=5000){
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),timeout);
 try{const r=await fetch(url,{cache:'no-store',signal:controller.signal});checkLogin(r);const version=r.headers?.get('X-AMS-UI-Version');if(version&&version!==UI_VERSION){UI_UPDATE_PENDING=true;applyDashboardUpdate();}if(!r.ok)throw new Error(`HTTP ${r.status}`);return await r.json();}finally{clearTimeout(timer);}
}
let NETWORK_PENDING=null,NETWORK_CHECKED=0;
function loadNetwork(){
 if(NETWORK_PENDING)return NETWORK_PENDING;
 if(Date.now()-NETWORK_CHECKED<30000)return Promise.resolve();
 NETWORK_PENDING=(async()=>{try{const n=await dashboardJSON('/api/network');const el=$('#net');const here=Object.values(n.printers_here||{}).some(Boolean);
 el.className='conn '+(here?'ok':'bad');el.querySelector('span').textContent=`${n.label}${n.ip?' · '+n.ip:''}${here?'':' — printers not on this network'}`;NETWORK_CHECKED=Date.now();}catch(e){}finally{NETWORK_PENDING=null;}})();return NETWORK_PENDING;
}
async function loadPrinters(){
  try{PRINTERS=await dashboardJSON('/api/printers');}catch(e){console.warn('Printer list refresh delayed',e);return;}
  if(!PRINTERS.length){$('#tabs').innerHTML='';return;}          // single-printer worker mode
  if(!P||!PRINTERS.find(x=>x.name===P))P=PRINTERS[0].name;
  renderTabs();renderRoom();
}
function renderTabs(){
  $('#room-settings-printer').textContent=P||'None';renderRoom();
  $('#tabs').innerHTML=PRINTERS.map(x=>{
    const st={...(x.summary||{})};if(attentionPending(x.name).length){st.error=true;st.text='Needs attention';}const dot=st.error?'bad':st.busy?'busy':st.paused?'paused':st.heating?'heat':st.ok?'ok':'';
    return`<div class="tab ${x.name===P?'active':''}" draggable="true" data-name="${pcEscape(x.name)}" role="button" tabindex="0" aria-pressed="${x.name===P}" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();selectPrinter(this.dataset.name)}" onclick="selectPrinter(this.dataset.name)" ondragstart="dragTab(event)" ondragover="event.preventDefault();this.classList.add('over')" ondragleave="this.classList.remove('over')" ondrop="dropTab(event,this)"><span class="dot ${dot}"></span>${pcEscape(x.name)}<small>${pcEscape(st.text||'')}</small></div>`}).join('')
    +`<div class="tab add-printer" style="font-weight:400;font-size:15px" role="button" tabindex="0" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();addPrinterDlg()}" onclick="addPrinterDlg()">+ Add printer</div>`;
}
function editPrinterDlg(){const x=PRINTERS.find(x=>x.name===P)||{};const c=x.config||{};
  openDialog(`<h3 style="margin:0 0 10px">Edit ${pcEscape(P)}</h3>
  <label>Name</label><input name="name" value="${pcEscape(P)}">
  <label>IP address</label><input name="ip" value="${pcEscape(c.ip||'')}">
  <label>Serial</label><input name="serial" value="${pcEscape(c.serial||'')}">
  <label>Access code</label><input name="access_code" value="" autocomplete="off" placeholder="${c.access_code_set?'Saved; leave blank to keep it':''}">`,
  async fd=>{const b=Object.fromEntries(fd);b.old_name=P;const r=await (await fetch('/api/edit_printer',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(b)})).json();
    if(r.result!=='ok')alert(r.result);else{P=b.name;try{localStorage.setItem('ams_printer',P)}catch(e){};await loadPrinters();load();}});}
function addPrinterDlg(){openDialog(`<h3 style="margin:0 0 10px">Add a printer</h3>
  <label>Name (e.g. H2D, X1C)</label><input name="name" autofocus>
  <label>IP address (printer screen → Settings → LAN Only)</label><input name="ip" placeholder="192.0.2.10">
  <label>Serial (Settings → Device and Serial Number)</label><input name="serial">
  <label>Access code (same LAN Only page; Developer Mode must be ON)</label><input name="access_code">`,
  async fd=>{const r=await (await fetch('/api/add_printer',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(Object.fromEntries(fd))})).json();
    if(r.result!=='ok')alert(r.result);else{P=fd.get('name');try{localStorage.setItem('ams_printer',P)}catch(e){};await loadPrinters();}});}
async function removePrinter(){if(!confirm(`Remove ${P} from the dashboard? Its history stays on disk.`))return;await fetch('/api/remove_printer',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:P})});P=null;await loadPrinters();load();}
let DRAG=null;
function dragTab(e){DRAG=e.currentTarget.dataset.name;e.dataTransfer.effectAllowed='move';}
async function dropTab(e,el){e.preventDefault();el.classList.remove('over');const to=el.dataset.name;if(!DRAG||DRAG===to)return;
  const order=PRINTERS.map(x=>x.name);const from=order.indexOf(DRAG),ti=order.indexOf(to);order.splice(from,1);order.splice(ti,0,DRAG);
  PRINTERS.sort((a,b)=>order.indexOf(a.name)-order.indexOf(b.name));renderTabs();
  await fetch('/api/reorder',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({order})});DRAG=null;}
function selectPrinter(n){if(DLG)return;if(ROOM_VIEW==='detail'&&P===n&&!arguments[1]){roomShow('watch');return;}ROOM_VIEW='detail';document.body.classList.remove('room-drying-view');roomStopSnapshots();$('#room-home').hidden=true;$('#printer-detail').hidden=false;$('#banner').hidden=false;$('#room-watch').setAttribute('aria-pressed','false');$('#room-drying').setAttribute('aria-pressed','false');const img=document.getElementById('camimg');if(img)img.src='';P=n;D=null;PC_DATA_PRINTER=null;try{localStorage.setItem('ams_printer',n)}catch(e){};renderTabs();renderCam();renderCtl();renderFansStatus();renderPrinterFilamentMap();$('#slots').innerHTML='';$('#events').innerHTML='';closeAttention();load();layoutQueueMeasure();uiReveal($('#printer-detail'));}
function openMovementWindow(name=P){if(!name)return;window.open('/movement?printer='+encodeURIComponent(name),'_blank','width=1100,height=760,resizable=yes,scrollbars=yes');}
function cls(s){if(!s)return'empty';return s.pct>=100?'ok':s.pct>=60?'warn':'bad'}
function ago(ts){const d=(Date.now()-new Date(ts.replace(' ','T')))/1000;if(d<60)return'just now';if(d<3600)return Math.round(d/60)+' min ago';if(d<86400)return(d/3600).toFixed(1)+' h ago';return ts;}
let ROOM_DRY_BUSY=false,ROOM_DRY_FOLLOWUP=false,ROOM_DRY_RESULTS=[];
function roomRenderFollowup(){
 const target=$('#room-drying-followup');if(!target)return;
 if(!ROOM_DRY_FOLLOWUP){const pending=PRINTERS.flatMap(p=>roomUnits(roomInfo(p.name)).filter(u=>u.fleet_requested&&['setup','preparing','unloading','pending','waiting'].includes(u.request_status)).map(u=>({printer:p.name,ams_id:u.id,ams:u.name,status:u.request_status,message:u.request_message,slots:u.setup_slots})));if(pending.length){ROOM_DRY_RESULTS=pending;ROOM_DRY_FOLLOWUP=true;}}
 target.hidden=!ROOM_DRY_FOLLOWUP;if(target.hidden)return;
 let rows=ROOM_DRY_RESULTS.map(item=>{const u=roomUnits(roomInfo(item.printer)).find(x=>x.id===item.ams_id);if(!u)return item;
 if(!roomOnline(roomInfo(item.printer)))return {...item,status:'waiting',message:'Waiting for fresh printer readings; heating is not confirmed.',slots:[]};
 if(u.heating)return {...item,status:'drying',message:'Drying confirmed by the printer',slots:[]};
 if(!u.fleet_requested)return item.status==='drying'?{...item,status:'stopped',message:'This AMS is no longer heating.'}:item;
 return {...item,status:roomOnline(roomInfo(item.printer))&&u.heating?'drying':u.request_status||item.status,message:u.request_message||item.message,slots:u.setup_slots||item.slots};});
 const active=rows.filter(x=>x.status==='drying'||x.status==='succeeded').length,queued=rows.filter(x=>['preparing','unloading','pending','waiting'].includes(x.status)).length,setup=rows.filter(x=>x.status==='setup').length;
 $('#room-followup-title').textContent=ROOM_DRY_BUSY?'Checking every AMS…':`${active} drying · ${queued} starting or queued${setup?' · '+setup+' waiting for setup or connection':''}`;
 // Full detail stays inline and updates with telemetry; no alert()/confirm().
 const html=rows.map(r=>`<div class="room-followup-row" data-followup-status="${roomText(r.status)}"><div><strong>${roomText(r.printer)}${r.ams?' · '+roomText(unitName(r.ams)):''}</strong><p>${roomText(['drying','succeeded'].includes(r.status)?'Drying confirmed by the printer':r.message||'Waiting for confirmation')}</p></div><div class="room-followup-actions">${r.status==='setup'?(r.slots||[]).map(slot=>`<button data-setup-printer="${roomText(r.printer)}" data-setup-key="${roomText(slot.key)}" data-setup-slot="${slot.slot}" data-setup-ams="${roomText(r.ams)}">Set slot ${slot.slot} filament →</button>`).join(''):''}${['setup','preparing','unloading','pending','waiting'].includes(r.status)&&r.ams_id!=null?`<button class="room-link" data-cancel-fleet="${roomText(r.printer)}" data-ams-id="${r.ams_id}">Cancel request</button>`:''}${['blocked','unavailable'].includes(r.status)?`<button class="room-link" data-room-open="${roomText(r.printer)}">View printer →</button>`:''}</div></div>`).join('');
 if($('#room-followup-rows').innerHTML!==html)$('#room-followup-rows').innerHTML=html;
 document.querySelectorAll('[data-start-fleet]').forEach(b=>{b.disabled=ROOM_DRY_BUSY;b.textContent=ROOM_DRY_BUSY?'Checking AMS units…':'Start all eligible AMS units';});
}
async function dryAll(){
 if(ROOM_DRY_BUSY)return;ROOM_DRY_BUSY=true;ROOM_DRY_FOLLOWUP=true;ROOM_DRY_RESULTS=[];roomShow('drying');roomRenderFollowup();
 try{const response=await fetch('/api/dry_all_printers',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});if(!response.ok)throw new Error('The drying service did not respond');const data=await response.json();
 ROOM_DRY_RESULTS=Array.isArray(data.units)?data.units:[{printer:'Print room',status:'blocked',message:'The request could not be confirmed. Check printer status before retrying.'}];
 }catch(e){ROOM_DRY_RESULTS=[{printer:'Print room',status:'blocked',message:'Could not confirm the request. Check the connection; no automatic resend was made.'}];}
 finally{ROOM_DRY_BUSY=false;await tick();roomRenderFollowup();}
}
let STORAGE_CLEAR_BUSY=false;
async function clearAllStorage(){
 if(STORAGE_CLEAR_BUSY)return;
 const button=$('#room-clear-storage'),status=$('#room-clear-storage-status');
 STORAGE_CLEAR_BUSY=true;button.disabled=true;status.textContent='Checking all printer USB drives…';
 try{
  const check=await fetch('/api/storage_clear_preview',{cache:'no-store'});checkLogin(check);
  const preview=await check.json();if(!check.ok)throw new Error(preview.error||'Could not check printer storage');
  const total=preview.printers.reduce((n,p)=>n+p.files,0);
  const lines=preview.printers.map(p=>`${p.name}: ${p.files} files · ${(p.bytes/1e9).toFixed(2)} GB`).join('\n');
  const firmware=preview.printers.flatMap(p=>p.firmware.map(f=>`${p.name}: ${f}`));
  if(!total){status.textContent='All printer USB drives are already empty.';return;}
  const message=`Clear ALL files from all ${preview.printers.length} printer USB drives?\n\n${lines}\n\nThis permanently deletes print files and recordings.${firmware.length?'\n\nStaged firmware will also be deleted:\n'+firmware.join('\n'):''}\n\nPrinters must remain idle until this finishes. This empties files; it does not rebuild the filesystem.`;
  if(!confirm(message)){status.textContent='Cancelled. No files were changed.';return;}
  if(prompt('Type CLEAR ALL to confirm permanent deletion from every printer USB drive:')!=='CLEAR ALL'){status.textContent='Cancelled. No files were changed.';return;}
  status.textContent='Clearing files… Keep printers idle.';
  const response=await fetch('/api/storage_clear',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:preview.token,confirmation:'CLEAR ALL'})});
  checkLogin(response);const started=await response.json();if(!response.ok)throw new Error(started.error||'Could not start');
  while(true){
   await new Promise(resolve=>setTimeout(resolve,2000));
   const job=await dashboardJSON('/api/storage_clear_status',15000);
   status.textContent=job.message||'Clearing files…';
   if(job.done){
    const detail=(job.results||[]).map(p=>`${p.name}: ${p.remaining===0?'cleared':p.remaining<0?'remaining files could not be checked':p.remaining+' files remain'}${p.error?' — '+p.error:''}`).join('\n');
    alert((job.error?'Clear stopped: '+job.error:'Storage clearing finished.')+(detail?'\n\n'+detail:''));
    break;
   }
  }
 }catch(e){status.textContent='Could not finish: '+e.message+'. Check drive contents before retrying.';}
 finally{STORAGE_CLEAR_BUSY=false;button.disabled=false;}
}
async function roomSetupFilament(printer,key,slot,ams){
 selectPrinter(printer,true);const ok=await load();if(!ok||P!==printer)return;
 setSpoolDlg(key,Number(slot),ams);
 const dialog=$('#dlg'),original=dialog.onclose;
 dialog.onclose=async()=>{if(original)await original();roomShow('drying');tick();};
}
async function roomCancelFleet(printer,id){
 try{const response=await fetch(`/api/p/${encodeURIComponent(printer)}/api/cancel_pending`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ams_id:Number(id)})});if(!response.ok)throw new Error('Request failed');const data=await response.json();
 ROOM_DRY_RESULTS=ROOM_DRY_RESULTS.map(r=>r.printer===printer&&r.ams_id===Number(id)?{...r,status:'cancelled',message:data.result==='ok'?'Request cancelled':data.result,slots:[]}:r);await tick();roomRenderFollowup();
 }catch(e){$('#room-followup-title').textContent='Could not confirm cancellation — check this printer before leaving it.';}
}
document.addEventListener('click',e=>{const b=e.target.closest('[data-setup-printer],[data-cancel-fleet]');if(!b)return;if(b.dataset.setupPrinter)roomSetupFilament(b.dataset.setupPrinter,b.dataset.setupKey,b.dataset.setupSlot,b.dataset.setupAms);else roomCancelFleet(b.dataset.cancelFleet,b.dataset.amsId);});

async function stopAll(){
  if(!confirm('Stop drying on every AMS of every printer?\n\nAuto-dry stays on and will restart cycles when spools need it — it is paused 6 h per unit after a manual stop.'))return;
  const b=$('#stopall');b.textContent='■ Stopping…';b.disabled=true;
  try{const r=await (await fetch('/api/stop_all_printers',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'})).json();
    alert((r.detail||[]).join('\n')||'nothing was drying');}catch(e){alert('failed: '+e.message);}
  b.textContent='■ Stop all drying';b.disabled=false;load();refreshTabs();}
async function manualRefresh(){const b=$('#refresh');if(b.disabled)return;b.disabled=true;b.classList.add('busy');b.textContent='↻ Refreshing…';try{const ok=await load();b.textContent=ok?'✓ Updated':'↻ Retry refresh';refreshTabs();}finally{b.disabled=false;b.classList.remove('busy');setTimeout(()=>{if(!b.disabled)b.textContent='↻ Refresh';},1500);}}
// keep streaming in the background; browsers may still stall a hidden <img>, so re-arm on return
document.addEventListener('visibilitychange',()=>{if(!document.hidden&&camWanted()){CAM_TRIES=0;camHealth();}});
// An MJPEG <img> is a request that never ends. On refresh the browser keeps that
// socket until the server notices, and with only ~6 connections per origin the new
// page's API calls queue behind it. Drop it explicitly before leaving.
function dropCam(){try{const i=document.getElementById('camimg');if(i){cameraBufferStop(i);cameraFadeReset(i);i.removeAttribute('src');}}catch(e){}}
window.addEventListener('pagehide',dropCam);
window.addEventListener('beforeunload',dropCam);
document.addEventListener('click',e=>{if(!e.target.closest('.menu'))document.querySelectorAll('.menu.open').forEach(m=>m.classList.remove('open'))});
function unitName(a){const m=/^(AMS|HT)-(\d+)$/.exec(a);if(!m)return a;return m[1]==='HT'?`AMS HT ${+m[2]+1}`:`AMS ${+m[2]+1}`;}
function hm(mins){const m=Math.max(0,Math.round(mins||0));const h=Math.floor(m/60),r=m%60;
  return h?(r?`${h}h ${r}m`:`${h}h`):`${r}m`;}
function dryWaitElapsed(since){const seconds=Math.max(0,Math.floor(Date.now()/1000-Number(since||Date.now()/1000)));return `${Math.floor(seconds/60)}:${String(seconds%60).padStart(2,'0')}`;}
function dryWaitRemaining(until){const seconds=Math.max(0,Math.ceil(Number(until||0)-Date.now()/1000));if(!seconds)return 'waiting for printer';const h=Math.floor(seconds/3600),m=Math.floor(seconds%3600/60),s=seconds%60;return h?`${h}h ${m}m left`:`${m}:${String(s).padStart(2,'0')} left`;}
function dryWaitCountdown(until){return `<span data-dry-until="${Number(until)||0}">${dryWaitRemaining(until)}</span>`;}
function updateDryWaitTimers(){document.querySelectorAll('[data-dry-elapsed]').forEach(el=>{el.textContent=dryWaitElapsed(el.dataset.dryElapsed);});document.querySelectorAll('[data-dry-until]').forEach(el=>{el.textContent=dryWaitRemaining(el.dataset.dryUntil);});}
setInterval(updateDryWaitTimers,1000);
function fmt(h){return h==null?'—':h<48?h.toFixed(1)+' h':(h/24).toFixed(1)+' d'}
function editing(){const a=document.activeElement;return a&&(a.tagName==='INPUT'||a.tagName==='SELECT');}
function renderFansStatus(){
 const rows=$('#fans-status'),status=$('#fans-reading-status');if(!rows||!status)return;
 if(!D||PC_DATA_PRINTER!==P){status.textContent='Waiting for readings';rows.innerHTML='';return;}
 status.textContent=pcConnected()?'Live readings':'Last report';
 const fans=D.fans||{};
 rows.innerHTML=[['part','Part cooling'],['aux','Auxiliary'],['exhaust','Chamber / exhaust']].map(([key,label])=>{
  const value=fans[key],pct=value==null||!Number.isFinite(Number(value))?null:pcPercent(Number(value));
  return `<div class="desk-fan-row"><dt>${label}</dt><dd data-off="${pct===0}">${pct==null?'No reading':pct===0?'0% · Off':pct+'%'}</dd></div>`;
 }).join('');
}
function render(){
  if(editing())return;
  const ok=D.mqtt.mqtt==='connected'&&!D.stale, bad=!!D.mqtt.error||D.stale;
  const c=$('#conn');c.className='conn '+(ok?'ok':bad?'bad':'');
  c.querySelector('span').textContent=ok?'Printer connected':bad?(D.stale?'No data from printer':'Not connected'):D.mqtt.mqtt;
  $('#room-reading-status').textContent=ok?'Live readings from this printer':'Offline · last saved readings';
  const ps=D.print_state==='RUNNING'?` · printing${D.print_left_min?' ('+hm(D.print_left_min)+' left)':''}`:D.print_state==='PAUSE'?' · print paused':'';
  $('#upd').textContent=D.last_update?`Updated ${ago(D.last_update)}${ps} · live`:'';
  document.querySelector('.menu').style.display=PRINTERS.length?'':'none';
  $('#banner').innerHTML=!ok?'<div class="banner">Printer offline · showing saved readings. Live controls resume when it reconnects.</div>':'';
  const byAms={};D.slots.forEach(r=>(byAms[r.ams]=byAms[r.ams]||[]).push(r));
  if(!document.getElementById('camimg')||$('#cam').dataset.p!==P||!camWanted()){$('#cam').dataset.p=P;renderCam();}   // never rebuild a working <img>
  renderCtl();renderFansStatus();renderErr();renderAct();renderPrinterFilamentMap();
  if(!D.slots.length){$('#slots').innerHTML=`<div class="envbox"><span class="title">No AMS connected</span><span class="temp">This printer reports no AMS units. Filament tracking starts when one is attached.</span></div>`;}
  else $('#slots').innerHTML='<svg class="room-filament-path" aria-hidden="true"></svg>'+Object.entries(byAms).sort((a,b)=>a[1][0].ams_id-b[1][0].ams_id).map(([ams,rows],index)=>{
    rows.sort((a,b)=>a.slot-b.slot);
    const a=rows[0];
    const u=D.units[ams]||{};
    const empty=rows.every(r=>!r.spool);
    const cooling=!a.heating&&a.ams_temp!=null&&a.ams_temp>=40;
    const printing=['RUNNING','PREPARE'].includes(D.print_state);
    const sp=a.setpoint&&a.setpoint>0?a.setpoint:null;
    const q=u.dry_pending;
    const wait=q?.wait_detail;
    const waitLabel=wait?.kind==='shared_power'?`Waiting for ${pcEscape(unitName(wait.owner))}${wait.until?' · '+dryWaitCountdown(wait.until):''}`
      :wait?.kind==='restart_cooldown'?`Restart pause · ${dryWaitCountdown(wait.until)}`
      :`Preparing heating · <span data-dry-elapsed="${Number(q?.since)||0}">${dryWaitElapsed(q?.since)}</span> elapsed`;
    const pill=a.heating?`<span class="pill on">● HEATING ${a.ams_temp} °C${sp?` → set ${sp} °C`:''}${a.dry_time>0?' · '+hm(a.dry_time)+' left':''}</span>`
              :q?''
              :empty?`<span class="pill empty">EMPTY</span>`:cooling?`<span class="pill cool">COOLING DOWN ${a.ams_temp} °C</span>`:`<span class="pill off">HEATER OFF</span>`;
    const env=`<span class="env">${a.ams_temp??'—'} °C · ${a.humidity_raw!=null?a.humidity_raw+' % RH':(a.humidity??'—')+'/5'}</span>`;
    const sw=`<label class="sw ${u.auto?'on':''}" role="switch" tabindex="0" aria-label="Auto-dry" aria-checked="${!!u.auto}" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();this.click()}" onclick="api('/api/auto',{ams_id:${a.ams_id},on:${!u.auto}}).then(load)"><span class="track"></span>Auto-dry <b>${u.auto?'ON':'OFF'}</b></label>`;
    const psu=`<label class="sw ${u.psu?'on':''}" role="switch" tabindex="0" aria-label="External power adapter" aria-checked="${!!u.psu}" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();this.click()}" style="margin-left:0" title="External power adapter on this AMS: independent drying power. Without it, one AMS at a time can use H2 printer power; mid-print requests are capped at 55 °C. The printer may reduce the temperature further or refuse drying." onclick="api('/api/psu',{ams_id:${a.ams_id},on:${!u.psu}}).then(load)"><span class="track"></span>Ext. PSU <b>${u.psu?'YES':'NO'}</b></label>`;
    const btn=a.heating?`<button class="stop" onclick="dry(${a.ams_id},false)">■ Stop heating</button>`
             :q?`<button class="primary dry-waiting" disabled title="${wait?.kind==='restart_cooldown'?'Estimated restart pause; the printer decides when heating can resume':wait?.kind==='shared_power'?'The other AMS’s reported drying time; it may stop earlier':'Time since request; the printer does not report a start ETA'}"><span class="dry-wait-spinner" aria-hidden="true"></span>${waitLabel}</button>`
             :empty?`<button disabled style="opacity:.4">Start drying</button>`:`<button class="primary" onclick="dryDlg(${a.ams_id},'${ams}')">${u.dry_blocked||u.dry_cancelled||(u.dry_pending?.attempts>=3)?'Retry drying':'▶ Start drying now'}</button>`;
    const df=u.dry_blocked?''
      :q?`<div class="legend">${wait?.kind==='shared_power'?`No external PSU: ${pcEscape(unitName(wait.owner))} currently has the shared drying power. ${wait.until?'Its remaining time comes from the printer and may change.':'It has not reported time remaining.'}`:wait?.kind==='restart_cooldown'?'The printer is clearing its stop state. This countdown is an estimate; heating will start as soon as it allows.':pcEscape(q.message||'Waiting for drying to start')} ${q.attempts>=3?'· Retry limit reached':'· '+(q.attempts||0)+' of 3 attempts'}
      <button style="margin-left:6px" onclick="api('/api/cancel_pending',{ams_id:${a.ams_id}}).then(nudge)">Cancel request</button></div>`
      :u.dry_retry_ready?'<div class="legend">Ready to retry — the AMS outlet is clear. Choose Retry drying to start.</div>':u.dry_cancelled?'<div class="legend">The drying cycle was stopped or cancelled. Auto-dry is paused until you choose Retry drying.</div>':'';
    const cr=rows.some(x=>x.cool_read)?`<div class="legend">${pcEscape(a.tag_read_note)}</div>`:'';
    const psuw='';
    const note=cr+df+psuw+(!u.dry_blocked&&!u.dry_cancelled&&u.auto&&u.cannot_fully&&u.cannot_fully.length?`<div class="legend" style="color:var(--warn)">Auto-dry limited to ${u.temp} °C in this unit: ${u.cannot_fully.join('; ')}</div>`
              :(u.hold?`<div class="legend">Auto-dry paused for ${MANUAL_HOLD} h because you stopped it manually</div>`:(false?``:(u.auto&&!empty&&!u.dry_blocked&&!u.dry_cancelled&&!q?`<div class="legend">Auto-dry will start a cycle when any spool here drops below ${AUTO_PCT}% dry${u.psu?'':' — shared printer power: one AMS at a time, up to 55 °C during printing'}</div>`:''))));
    return `<section class="ams ${index?'ams-below':''} ${a.heating?'is-heating':''}" data-ams="${pcEscape(ams)}"><div class="unit"><div><div class="unit-title"><span class="title">${unitName(ams)}</span></div><div class="unit-readings">${!ok?'<span class="pill off">Last report</span>':''}${pill}${env}</div></div><div class="unit-actions">${sw}${psu}${btn}</div></div>${note}
    <div class="grid room-spools">${rows.map(printerSlotCard).join('')}</div></section>${index===0?printerNozzle():''}`}).join('');
  $('#slots').classList.add('room-filament-route','room-shared-route');
  $('#slots').classList.toggle('is-flowing',ok&&filamentIsMoving(D));renderPrinterFilamentMap();
  $('#events').innerHTML=D.events.length?D.events.map(e=>`<div class="event-row"><time>${pcEscape(e.time)}</time><span class="event-kind">${pcEscape(e.kind.replaceAll('_',' '))}</span><span class="event-message">${pcEscape(e.msg||'')}</span></div>`).join(''):'<div class="events-empty">No recent activity</div>';
}
async function amsUnload(){if(!confirm('Run the AMS unload sequence?\n\nThe nozzle heats to 250 °C and the AMS releases the filament in its outlet. Takes about a minute.'))return;
  const r=await api('/api/ams_unload',{});if(r.result&&r.result!=='ok')alert(r.result);nudge();}
async function filament(target,msg){if(!confirm(msg))return;const r=await api('/api/filament',{action:'change',target});if(r.result!=='ok')alert(r.result);setTimeout(load,3000);}
async function printCtl(action){
  if(action==='stop'&&!confirm(`Stop the print on ${P}? This cannot be undone.`))return;
  if(action==='pause'&&!confirm(`Pause the print on ${P}?`))return;
  return pcRequest('/api/print',{action},action==='resume'?'Resume print':action==='pause'?'Pause print':'Stop print',d=>action==='pause'?d.print_state==='PAUSE':action==='resume'?d.print_state==='RUNNING':!['RUNNING','PREPARE','PAUSE'].includes(d.print_state),'print');
}
async function preheat(label,bed,chamber,nozzle=null){
 if(pcBusy()){pcFeedback('Preheat and cool-down are locked while a print is active.','error',P,'quick');return false;}
  if(pcPresetIsOn(label,{bed,chamber,nozzle}))return pcCooldown();
  return pcRequest('/api/preheat',{label,bed,chamber,nozzle},label,d=>Number((d.env||{}).bed_target)===Number(bed)&&Number((d.env||{}).chamber_target)===Number(chamber)&&(nozzle==null||Number((d.env||{}).nozzle_target)===Number(nozzle)));
}
function presetDlg(){
  const pr=D.preheat||{}; const rows=Object.entries(pr).map(([k,v])=>`<div style="display:flex;gap:8px;align-items:center;margin:4px 0"><span style="flex:1">${pcEscape(k)} — bed ${v.bed}, chamber ${v.chamber||'off'}${v.nozzle!=null?', nozzle '+v.nozzle:''}</span><button onclick="delPreset('${pcEscape(k)}')">delete</button></div>`).join('');
  openDialog(`<h3 style="margin:0 0 10px">Preheat presets</h3>${rows||'<p class="meta">none yet</p>'}<hr style="border:0;border-top:1px solid #333;margin:12px 0">
  <label>New preset name</label><input name="name" placeholder="ASA">
  <div style="display:flex;gap:10px"><div style="flex:1"><label>Bed °C</label><input name="bed" type="number" value="90" min="0" max="120"></div><div style="flex:1"><label>Chamber °C (0 = off, max 65)</label><input name="chamber" type="number" value="0" min="0" max="65"></div><div style="flex:1"><label>Nozzle °C (optional)</label><input name="nozzle" type="number" min="0" max="320"></div></div>`,
  async fd=>{if(!fd.get('name'))return;const p=Object.assign({},D.preheat||{}),nozzle=fd.get('nozzle');p[fd.get('name')]={bed:+fd.get('bed'),chamber:+fd.get('chamber')};if(nozzle!=='')p[fd.get('name')].nozzle=+nozzle;await api('/api/preheat_presets',{presets:p});});
}
async function delPreset(k){const p=Object.assign({},D.preheat||{});delete p[k];await api('/api/preheat_presets',{presets:p});$('#dlg').close();load();}
let CAM_ON={},CAM_AUTO=true;
try{CAM_AUTO=localStorage.getItem('ams_cam_auto')!=='0'}catch(e){}
function camWanted(){return ROOM_VIEW==='detail'&&pcConnected()&&(P in CAM_ON?CAM_ON[P]:CAM_AUTO);}
function renderControlError(){
 const container=$('#err'),old=container.querySelector('.pc-command-error');
 if(old)old.remove();
 const feedback=pcState().feedback;
 if(feedback?.status==='error')container.insertAdjacentHTML('beforeend',`<div class="pc-command-error" role="alert"><span>${pcEscape(feedback.text)}</span><button type="button" class="alert-close" onclick="dismissControlFeedback()" aria-label="Dismiss command error">×</button></div>`);
}
function renderErr(){$('#err').innerHTML='';renderControlError();renderAttention();}


async function resolve(a,msg){if(!confirm(msg))return;const r=await api('/api/resolve',{action:a});if(r.result!=='ok')alert(r.result);nudge();}
function renderAct(){
  const a=D&&D.activity;
  if(a&&ACT_DISMISSED.get(P)===JSON.stringify([a.op,a.steps,a.outcome])){$('#act').innerHTML='';return;}
  if(!a||!a.steps||!a.steps.length){$('#act').innerHTML='';return;}
  if(a.outcome==='failed'&&String(a.op).startsWith('Drying blocked')&&attentionItems().some(i=>i.printer===P&&String(i.code).startsWith('DRYING_'))){$('#act').innerHTML='';return;}
  const last=a.steps.length-1, failed=a.outcome==='failed', cancelled=a.outcome==='cancelled';
  const duplicateLast=a.done&&a.steps[last].text===a.op;
  const visibleSteps=a.steps.map((step,index)=>({step,index})).filter(({index})=>!duplicateLast||index!==last);
  const hhmm=t=>new Date(t*1000).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'});
  if(a.done&&!failed){$('#act').innerHTML=`<details class="desk-operation"><summary>${cancelled?'':'✓ '}${pcEscape(a.op)}<time>${hhmm(a.steps[last].t)}</time></summary>${visibleSteps.map(({step})=>`<p>${pcEscape(step.text)}</p>`).join('')}</details>`;return;}

  $('#act').innerHTML=`<div class="actbox ${failed?'failed':cancelled?'cancelled':a.done?'done':''}">
    <div class="title">${failed?'× ':cancelled?'':a.done?'✓ ':''}${pcEscape(a.op)}${duplicateLast?`<span class="when" style="font-size:12px;font-weight:400">${hhmm(a.steps[last].t)}</span>`:''}${failed?'<button type="button" class="alert-close" onclick="dismissActivity()" aria-label="Dismiss operation error">×</button>':''}</div>
    ${visibleSteps.map(({step:s,index:i})=>`<div class="stepli ${!a.done&&i===last?'cur':''}">
      <span class="dot">${i===last?(failed?'×':cancelled?'−':a.done?'✓':'●'):'·'}</span><span>${pcEscape(s.text)}</span><span class="when">${hhmm(s.t)}</span></div>`).join('')}
  </div>`;
}
// Each printer owns its open editor, feedback, and outstanding commands.
const PC_STATES=new Map();let PC_DATA_PRINTER=null;
let JOG_STEP=1;try{JOG_STEP=parseFloat(localStorage.getItem('ams_step'))||1}catch(e){}
function pcState(printer=P){const key=printer||'';if(!PC_STATES.has(key))PC_STATES.set(key,{panel:null,tab:'Movement',inflight:new Set(),feedback:null,waiting:null,tempTargets:{}});return PC_STATES.get(key);}
function pcEscape(value){return String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
// A string argument for an inline handler: JSON-quoted, then HTML-escaped for the attribute.
function jsArg(value){return pcEscape(JSON.stringify(String(value??'')));}
function pcIcon(name){const paths={edit:'<path d="m12 3 3 3-8 8-4 1 1-4 8-8Z"/>',arrow:'<path d="m7 4 5 5-5 5"/>',controls:'<path d="M3 5h12M3 13h12M6 3v4M12 11v4"/>',light:'<path d="M6.5 11.5C6.5 10 4.5 9 4.5 6.5a4.5 4.5 0 0 1 9 0c0 2.5-2 3.5-2 5Z M6.5 14h5 M8 16h2"/>',fan:'<circle cx="9" cy="9" r="1.5"/><path d="M8 7C3 3 9 0 10 4l-.2 3M11 9c6-1 4 6 1 4l-2-2M8 11c-1 6-6 1-3-1l2-1"/>',lock:'<rect x="4" y="8" width="10" height="8" rx="2"/><path d="M6 8V5a3 3 0 0 1 6 0v3"/>',pause:'<path d="M6 4v10M12 4v10"/>',play:'<path d="m6 3 8 6-8 6Z"/>',close:'<path d="m4 4 10 10M14 4 4 14"/>',home:'<path d="m2 8 7-6 7 6M4 7v9h10V7M7 16v-5h4v5"/>'};return `<svg viewBox="0 0 18 18" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[name]||''}</svg>`;}
function pcConnected(){return !!D&&PC_DATA_PRINTER===P&&!D.stale&&D.mqtt&&D.mqtt.mqtt==='connected';}
function pcBusy(){return !!D&&['RUNNING','PREPARE','PAUSE'].includes(D.print_state);}
function pcMotionReason(){if(!pcConnected())return 'Printer connection is unavailable. Controls will return when live data is available.';if(pcBusy())return 'A print job is active. Movement, extrusion and maintenance unlock when the printer is idle; pausing keeps the lock active.';if(D.print_state==='FAILED'&&(D.errors?.print_error||D.errors?.hms?.some(e=>e.blocking)))return 'Clear the failed print on the printer screen before using machine controls.';return null;}
function pcPercent(v){return v==null?null:Math.max(0,Math.min(100,Math.round(v/2.55)));}
function pcFormatTemp(v){return v==null?'—':Math.round(v);}
function pcFeedback(text,status='sent',printer=P,target='machine'){pcState(printer).feedback={text,status,target};if(printer===P)renderCtl();}
function pcObserve(){const s=pcState();if(!s.waiting)return;if(s.waiting.matches(D)){s.feedback=s.waiting.label==='All printer heaters off'?null:{text:s.waiting.label+' · confirmed by printer',status:'confirmed',target:s.waiting.target};s.waiting=null;}else if(Date.now()>s.waiting.until){s.feedback={text:s.waiting.label+' sent; the requested value has not appeared in the printer report. Check the live value before retrying.',status:'sent',target:s.waiting.target};s.waiting=null;}}
async function pcRequest(path,body,label,matches=null,channel='control'){
 const printer=P,s=pcState(printer),target=path==='/api/preheat'||['fans_all','fans_max'].includes(body?.action)?'quick':['/api/temp','/api/cooldown_heaters'].includes(path)?'temperatures':path==='/api/print'?'camera':'machine';if(s.inflight.has(channel))return false;
 if(!pcConnected()){pcFeedback('Printer connection is unavailable. Command was not sent.','error',printer,target);return false;}
 if(path==='/api/preheat'||path==='/api/cooldown_heaters')s.tempTargets={};
 const quietSuccess=path==='/api/cooldown_heaters';
 s.inflight.add(channel);s.waiting=null;s.feedback=quietSuccess?null:{text:label+' · sending…',status:'sending',target};renderCtl();
 let sent=false;
 try{
  // Build the route before awaiting, so switching printer tabs cannot retarget a command.
  const url=printer?`/api/p/${encodeURIComponent(printer)}${path}`:path;
  const response=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  if(!response.ok)throw new Error(`HTTP ${response.status}`);
  const reply=await response.json();
  if(typeof reply.result!=='string')throw new Error('No command acknowledgement. Check the printer before retrying.');
  if(!/^ok(?:\b|$)/.test(reply.result)&&!(path==='/api/preheat'&&reply.result==='already cool — heaters and fans off'))throw new Error(reply.result);
  sent=true;s.feedback=quietSuccess?null:{text:label+' · sent to printer',status:'sent',target};
  if(matches){s.waiting={matches,label,target,until:Date.now()+15000};if(s.feedback)s.feedback.text=label+' · sent, waiting for printer report';}
 }catch(error){s.feedback={text:label+' · '+error.message,status:'error',target};}
 finally{
  // Never retry physical commands. Keep quick repeated clicks from sending a second one.
  await new Promise(resolve=>setTimeout(resolve,600));s.inflight.delete(channel);
  if(printer===P){renderCtl();if(sent)nudge();}
 }
 return sent;
}
function pcDesk(tab){const s=pcState();s.panel=s.panel==='machine'&&s.tab===tab?null:'machine';s.tab=tab;renderCtl();uiReveal($('#pc-editor'),6);}
function pcOpen(panel){const s=pcState();s.panel=s.panel===panel?null:panel;renderCtl();const editor=document.getElementById('pc-editor');if(editor){editor.focus({preventScroll:true});uiReveal(editor,6);}}
function pcClose(){pcState().panel=null;renderCtl();}
function pcTab(tab){const s=pcState();if(s.tab===tab){s.panel=null;}else{s.tab=tab;}renderCtl();uiReveal($('#pc-machine-pane'),4);}
function setStep(value){JOG_STEP=value;try{localStorage.setItem('ams_step',value)}catch(e){}renderCtl();}
function ctl(action,extra){if(['move','home','extrude','level','motors_off'].includes(action)&&pcMotionReason()){pcFeedback(pcMotionReason(),'error');return Promise.resolve(false);}return pcRequest('/api/control',Object.assign({action},extra||{}),({home:'Homing',level:'Bed leveling',motors_off:'Motors off',extrude:'Extrusion',move:'Movement'})[action]||action);}
function jogA(axis,sign){return ctl('move',{axis,dist:sign*JOG_STEP});}
function pcHome(){const el=document.getElementById('pc-home');if(el)return ctl('home',{what:el.value});}
function pcExtrude(sign){const input=document.getElementById('pc-distance');if(!input||!input.reportValidity())return;return ctl('extrude',{dist:sign*Number(input.value)});}
function pcMaintenance(action){if(!confirm(action==='level'?'Run bed leveling? This takes a few minutes.':'Turn motors off and release the axes?'))return;return ctl(action);}
function pcSpeed(level){return pcRequest('/api/control',{action:'speed',level},'Print speed',d=>Number(d.spd_lvl)===level);}
function pcAllFansOn(){const s=pcState();if(s.fanToggle&&Date.now()<s.fanToggle.until)return s.fanToggle.on;return ['part','aux','exhaust'].every(name=>pcPercent((D?.fans||{})[name])===100);}
async function pcToggleAllFans(){
 const s=pcState();if(s.inflight.has('control'))return;
 const on=!pcAllFansOn(),pct=on?100:0;
 s.fanToggle={on,until:Date.now()+15000};
 const sent=await pcRequest('/api/control',{action:'fans_all',pct},on?'All fans 100% · auto-off in 7 min':'All fans off',d=>['part','aux','exhaust'].every(name=>pcPercent((d.fans||{})[name])===pct));
 if(!sent){s.fanToggle=null;if(pcState()===s)renderCtl();}
}
async function pcCooldown(){
 if(pcBusy())return;
 const matches=d=>{const e=d.env||{};return Number(e.bed_target)===0&&Number(e.chamber_target)===0&&(e.nozzles?.length?e.nozzles.every(n=>Number(n.target)===0):Number(e.nozzle_target)===0);};
 return pcRequest('/api/cooldown_heaters',{},'All printer heaters off',matches);
}
async function pcApplyTemp(kind){
 const input=document.getElementById('pc-target-'+kind);if(!input||!input.reportValidity())return;
 const value=Number(input.value);if(pcBusy()&&!confirm(`Change the ${kind} target to ${value} °C during the active print?`))return;
 const state=pcState(),request={value,sent:false,at:Date.now()};state.tempTargets[kind]=request;input.blur();
 const sent=await pcRequest('/api/temp',{kind,value},kind+' target',d=>Number((d.env||{})[kind+'_target'])===value);
 if(state.tempTargets[kind]===request){if(sent)request.sent=true;else delete state.tempTargets[kind];if(P&&state===pcState())renderCtl();}
 return sent;
}
function pcQuickPreheat(i){const entry=Object.entries(D.preheat||{})[i];if(entry)return preheat(entry[0],entry[1].bed,entry[1].chamber,entry[1].nozzle);}
function pcPresetIsOn(name,v){
 if(!pcConnected())return false;
 const waiting=pcState().waiting;
 if(waiting?.label==='All printer heaters off')return false;
 if(waiting?.label===name&&waiting.target==='quick'&&waiting.until>Date.now())return true;
 const e=D?.env||{},asked=D?.asked_env||{};
 const matches=(kind,wanted)=>{
  const actual=e[kind+'_target'];
  if(actual==null)return false;
  if(Number(actual)===Number(wanted))return true;
  return kind==='chamber'&&Number(wanted)>0&&Number(actual)>0&&Number(actual)<Number(wanted)&&Number(asked.chamber)===Number(wanted)&&Number(asked.bed)===Number(v.bed);
 };
 return [v.bed,v.chamber,v.nozzle].some(x=>Number(x)>0)&&matches('bed',v.bed)&&matches('chamber',v.chamber||0)&&(v.nozzle==null||matches('nozzle',v.nozzle));
}
function pcPresetHeatState(name,v){
 if(!pcPresetIsOn(name,v))return 'off';
 if(pcState().waiting?.label===name)return 'starting';
 const e=D?.env||{},kinds=['bed','chamber',...(v.nozzle==null?[]:['nozzle'])].filter(k=>Number(e[k+'_target'])>0);
 if(kinds.some(k=>e[k]==null))return 'target-set';
 return kinds.some(k=>Number(e[k])<Number(e[k+'_target'])-2)?'warming':'ready';
}
function pcFanTimerLabel(){const deadline=Number(D?.fan_off_at);return deadline?`Auto-off in ${Math.max(0,Math.ceil((deadline*1000-Date.now())/60000))} min`:'Auto-off after 7 min';}
function pcPreheatButtons(){return `<div class="pc-preheat-shortcuts"><span>Preheat</span>${Object.entries(D.preheat||{}).map(([name,v],i)=>{const heat=pcPresetHeatState(name,v),on=heat!=='off';return `<button class="pc-btn pc-preheat-preset ${on?'is-active':''}" data-heat="${heat}" aria-pressed="${on}" ${!pcConnected()||pcBusy()||pcState().inflight.size||pcState().waiting?.label==='All printer heaters off'?'disabled':''} onclick="pcQuickPreheat(${i})"><span class="pc-preheat-title"><strong>${on?'Turn off '+pcEscape(name):pcEscape(name)}</strong></span><small>Bed ${v.bed}°C${v.nozzle!=null?' · nozzle '+v.nozzle+'°C':''}${v.chamber?' · chamber '+v.chamber+'°C':''}</small><small class="pc-heat-state">${{off:'Not active',starting:'Starting heat…',warming:'Heating up…',ready:'Holding temperature','target-set':'Target set'}[heat]}</small></button>`;}).join('')}<button class="pc-btn ${pcAllFansOn()?'primary':''}" ${!pcConnected()||pcState().inflight.has('control')?'disabled':''} aria-pressed="${pcAllFansOn()}" onclick="pcToggleAllFans()"><strong>${pcAllFansOn()?'Turn all fans off':'All fans 100%'}</strong><small>${pcFanTimerLabel()}</small></button></div>`;}
function pcPreset(){const input=document.getElementById('pc-preset');if(!input)return;const entry=Object.entries(D.preheat||{})[Number(input.value)];if(!entry)return;return preheat(entry[0],entry[1].bed,entry[1].chamber,entry[1].nozzle);}
function pcPresetSelectionChange(){const input=document.getElementById('pc-preset'),button=document.getElementById('pc-preset-run');if(!input||!button)return;const entry=Object.entries(D.preheat||{})[Number(input.value)];if(entry)button.textContent=pcPresetIsOn(entry[0],entry[1])?'Turn off heaters':'Start preheat';}
function pcCool(){return preheat('Cool down',0,0);}
function pcLock(reason){return `<div class="pc-lock"><span class="pc-lockbox">${pcIcon('lock')}</span><div>Controls unavailable<div class="pc-muted">${pcEscape(reason)}</div></div></div>`;}
function pcEditor(){
 const s=pcState(),panel=s.panel;if(!panel)return '';
 const e=D.env||{},locked=!pcConnected()||s.inflight.has('control'),dis=locked?'disabled':'';let title='',content='';
 if(panel.startsWith('temp:')){
  const kind=panel.slice(5),labels={nozzle:'Active nozzle',bed:'Bed',chamber:'Chamber'},max={nozzle:320,bed:120,chamber:65}[kind];
  title=labels[kind]+' temperature';content=`${pcBusy()?'<div class="pc-alert">Changing this target affects the current print.</div>':''}<form onsubmit="event.preventDefault();pcApplyTemp('${kind}')"><div class="pc-editbody"><div><label class="pc-fieldlabel" for="pc-target">Target temperature · °C</label><div class="pc-inline"><input class="pc-input" id="pc-target" type="number" required min="0" max="${max}" step="1" value="${e[kind+'_target']??''}" placeholder="Set target" ${dis}><span class="pc-muted">0 = heater off${kind==='nozzle'&&pcBusy()?' (not allowed during a print)':''}</span></div></div><button type="submit" class="pc-btn primary" ${dis}>Apply target</button></div></form>`;
 }else if(panel==='prepare'){
  title='Preheat and cool-down';const reason=!pcConnected()?'Connect the printer to use preheat or cool-down.':pcBusy()?'Preheat presets and cool-down are locked during an active print.':null;
  if(reason)content=pcLock(reason);
  else {const entries=Object.entries(D.preheat||{});content=`<div class="pc-service"><div class="pc-serviceitem"><label class="pc-fieldlabel" for="pc-preset">Preheat preset</label><select class="pc-input" id="pc-preset" style="width:100%" onchange="pcPresetSelectionChange()" ${dis}>${entries.map(([name,v],i)=>`<option value="${i}">${pcEscape(name)} · bed ${v.bed}°C${v.nozzle!=null?' · nozzle '+v.nozzle+'°C':''} · chamber ${v.chamber? v.chamber+'°C':'off'}</option>`).join('')}</select><p>Heats the selected targets. Select it again to turn off the printer heaters.</p><div class="pc-actions"><button class="pc-btn primary" id="pc-preset-run" ${locked||!entries.length?'disabled':''} onclick="pcPreset()">${entries.length&&pcPresetIsOn(entries[0][0],entries[0][1])?'Turn off heaters':'Start preheat'}</button><button class="pc-quiet" onclick="presetDlg()">Manage presets</button></div></div><div class="pc-serviceitem"><span class="pc-fieldlabel">Cool-down</span><p>Bed and chamber heaters off. Fans run until the bed is below 40°C.</p><button class="pc-btn" ${dis} onclick="pcCool()">Cool down now</button></div></div>`;}
 }else if(panel==='machine'){
  title='Machine controls';content=`<div class="pc-machinenav" role="tablist" aria-label="Machine control task">${['Movement','Extruder','Maintenance'].map(tab=>`<button type="button" role="tab" id="pc-tab-${tab}" aria-controls="pc-machine-pane" aria-selected="${s.tab===tab}" onclick="pcTab('${tab}')">${tab}</button>`).join('')}</div><div role="tabpanel" id="pc-machine-pane" aria-labelledby="pc-tab-${s.tab}">`;
  const reason=pcMotionReason();
  if(reason)content+=pcLock(reason);
  else if(s.tab==='Movement'){
   const jog=(axis,sign,label)=>`<button type="button" ${dis} aria-label="${axis==='Z'?(sign>0?'Lower bed':'Raise bed'):'Move '+axis+' '+(sign>0?'positive':'negative')} ${JOG_STEP} mm" onclick="jogA('${axis}',${sign})">${label}</button>`;
   content+=`<div class="pc-machinebody"><div><div class="pc-fieldlabel">Jog distance · ${JOG_STEP} mm</div><div class="pc-jog"><div class="pc-pad"><span></span>${jog('Y',1,'↑')}<span></span>${jog('X',-1,'←')}<span>X Y</span>${jog('X',1,'→')}<span></span>${jog('Y',-1,'↓')}<span></span></div><div class="pc-z"><span>Bed</span>${jog('Z',-1,'↑')}${jog('Z',1,'↓')}</div></div><div class="pc-step">${[.1,1,10].map(v=>`<button type="button" aria-pressed="${JOG_STEP===v}" onclick="setStep(${v})">${v} mm</button>`).join('')}</div></div><div><span class="pc-fieldlabel">Homing</span><p class="pc-muted">Return the selected axes to their reference position.</p><label class="pc-fieldlabel" for="pc-home">Axes to home</label><select class="pc-input" id="pc-home" style="width:100%;margin-bottom:10px" ${dis}><option value="all">All axes</option><option value="xy">X and Y</option><option value="z">Z only</option></select><button class="pc-btn" ${dis} onclick="pcHome()">${pcIcon('home')}Home axes</button></div></div>`;
  }else if(s.tab==='Extruder'){
   const hot=e.nozzle!=null&&e.nozzle>=170;content+=`<div class="pc-editbody"><div><span class="pc-fieldlabel">Active nozzle · ${pcFormatTemp(e.nozzle)}°C</span><div class="${hot?'pc-muted':'pc-blocked-note'}">${hot?'Ready to extrude':'Heat the active nozzle above 170°C to extrude.'}</div><label class="pc-fieldlabel" for="pc-distance">Distance · mm</label><input class="pc-input" id="pc-distance" type="number" required min="1" max="100" step="1" value="10" ${dis}></div><div class="pc-actions"><button class="pc-btn" ${locked||!hot?'disabled':''} onclick="pcExtrude(-1)">Retract</button><button class="pc-btn" ${locked||!hot?'disabled':''} onclick="pcExtrude(1)">Extrude</button></div></div>`;
  }else content+=`<div class="pc-service"><div class="pc-serviceitem"><span class="pc-fieldlabel">Bed leveling</span><p>Measures the bed surface. Takes a few minutes.</p><button class="pc-btn" ${dis} onclick="pcMaintenance('level')">Run bed leveling</button></div><div class="pc-serviceitem"><span class="pc-fieldlabel">Motors</span><p>Release the axes so you can move them manually.</p><button class="pc-btn" ${dis} onclick="pcMaintenance('motors_off')">Turn motors off</button></div></div>`;
  content+='</div>';
 }
 return `<section class="pc-editor" id="pc-editor" tabindex="-1" aria-label="${pcEscape(title)}"><div class="pc-edithead"><h3>${pcEscape(title)}</h3><button class="pc-close" onclick="pcClose()">Done ${pcIcon('close')}</button></div>${content}</section>`;
}
const DESK_SENSOR_COLORS=['#eeb67d','#bf9ee8','#7bc8dc','#aee6cc'];
const DESK_THERMALS=new Map();
function deskThermalChart(readings){
 const now=Date.now();let history=DESK_THERMALS.get(P)||[];
 history=history.filter(point=>now-point.t<=300000);
 if(pcConnected()&&!LOAD_FAILED.has(P)&&(!history.length||now-history[history.length-1].t>=5000)){
  history.push({t:now,values:Object.fromEntries(readings.map(r=>[r.label,r.value]))});
 }
 DESK_THERMALS.set(P,history);
 const max=Math.max(50,Math.ceil(Math.max(0,...history.flatMap(p=>Object.values(p.values).filter(Number.isFinite)))/50)*50),width=480,height=100;
 const lines=readings.map((r,i)=>{let path='',last=null;history.forEach(p=>{const v=p.values[r.label];if(!Number.isFinite(v)){last=null;return;}const x=32+(p.t-(now-300000))/300000*(width-40),y=height-10-(v/max)*(height-20);path+=(last&&p.t-last<20000?'L':'M')+x.toFixed(1)+' '+y.toFixed(1)+' ';last=p.t;});return `<path d="${path}" stroke="${DESK_SENSOR_COLORS[i%4]}" fill="none" stroke-width="1.7"/>`;}).join('');
 return `<div class="desk-thermal-chart"><svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Temperatures recorded while this page is open, last five minutes">${[0,.5,1].map(f=>`<line x1="32" x2="480" y1="${90-80*f}" y2="${90-80*f}" stroke="var(--line)" stroke-dasharray="3 4"/><text x="0" y="${94-80*f}" fill="var(--mute)" font-size="10">${Math.round(max*f)}°</text>`).join('')}${lines}</svg><div><span>${history.length<2?'Collecting live temperature history…':'Last 5 min · recorded while open'}</span><span>Now</span></div></div>`;
}
function pcUpdateControlButton(current,fresh){
 for(const name of current.getAttributeNames())if(!fresh.hasAttribute(name))current.removeAttribute(name);
 for(const name of fresh.getAttributeNames())if(current.getAttribute(name)!==fresh.getAttribute(name))current.setAttribute(name,fresh.getAttribute(name));
 if(current.innerHTML!==fresh.innerHTML)current.innerHTML=fresh.innerHTML;
}
function pcUpdateQuickPanel(current,fresh){
 const oldShortcuts=current.querySelector(':scope > .pc-preheat-shortcuts'),newShortcuts=fresh.querySelector(':scope > .pc-preheat-shortcuts');
 if(!oldShortcuts||!newShortcuts){current.innerHTML=fresh.innerHTML;return;}
 const oldButtons=[...oldShortcuts.querySelectorAll(':scope > button')],newButtons=[...newShortcuts.querySelectorAll(':scope > button')];
 if(oldButtons.length===newButtons.length)newButtons.forEach((button,i)=>pcUpdateControlButton(oldButtons[i],button));
 else oldShortcuts.replaceChildren(...newShortcuts.childNodes);
 const oldHeading=current.querySelector(':scope > .desk-panel-heading'),newHeading=fresh.querySelector(':scope > .desk-panel-heading');
 if(oldHeading&&newHeading)oldHeading.replaceWith(newHeading);
 for(const child of [...current.children])if(child!==oldShortcuts&&child!==newHeading)child.remove();
 for(const child of [...fresh.children])if(child!==newShortcuts&&child!==newHeading)current.appendChild(child);
}
function mountControlWidgets(){
 const source=$('#ctl'),panels=[...source.querySelectorAll(':scope > .printer-controls > .desk-panel')];
 const targets=[['ctl-temperatures','Temperatures'],['ctl-quick','Quick actions'],['ctl-machine','Print speed']];
 targets.forEach(([id,title],index)=>{
  const target=document.getElementById(id);if(!target)return;
  if(panels[index]){
   const current=target.querySelector(':scope > .desk-panel'),fresh=panels[index];
   if(current&&index===0){if(current.className!==fresh.className)current.className=fresh.className;current.innerHTML=fresh.innerHTML;}
   else if(current&&index===1){if(current.className!==fresh.className)current.className=fresh.className;pcUpdateQuickPanel(current,fresh);}
   else target.replaceChildren(fresh);
  }
  else target.innerHTML=`<div class="desk-panel"><div class="desk-panel-heading"><h2>${title}</h2></div><div class="desk-idle">Loading printer controls…</div></div>`;
 });
 source.replaceChildren();
}
function renderCtl(){
 const activeTarget=document.activeElement?.id?.startsWith('pc-target-')?document.activeElement:null;
 const activeDraft=activeTarget?{id:activeTarget.id,value:activeTarget.value}:null;
 renderPrinterAction();
 if(!D||PC_DATA_PRINTER!==P){renderCameraJob();$('#ctl').innerHTML='<div class="printer-controls"><div class="pc-shell pc-loading">Loading printer controls…</div></div>';mountControlWidgets();return;}
 pcObserve();const s=pcState(),e=D.env||{},f=D.fans||{},connected=pcConnected(),pending=s.inflight.has('control'),dis=(!connected||pending)?'disabled':'';
 const readings=[];const nozzles=e.nozzles||[];const active=nozzles.length?nozzles.reduce((a,b)=>((b.active&&!a.active)||(!!b.active===!!a.active&&b.temp>a.temp))?b:a):null;
 if(nozzles.length>1)nozzles.forEach(n=>readings.push({label:'Nozzle '+(Number.isFinite(Number(n.id))?Number(n.id)+1:'—'),value:n.temp,target:n.target,kind:n===active?'nozzle':null,note:n===active?'temperature control':'read only'}));
 else readings.push({label:'Nozzle',value:e.nozzle,target:e.nozzle_target,kind:'nozzle'});
 readings.push({label:'Bed',value:e.bed,target:e.bed_target,kind:'bed'},{label:'Chamber',value:e.chamber,target:e.chamber_target,kind:'chamber'});
 const thermal=readings.map((r,i)=>{
  let draft=r.kind?s.tempTargets[r.kind]:null;
  if(draft?.sent&&r.target!=null&&(Number(r.target)===draft.value||(Number(r.target)===0&&Date.now()-draft.at>15000))){delete s.tempTargets[r.kind];draft=null;}
  const asked=r.kind?(D.asked_env||{})[r.kind]:null;
  const requested=draft?.value??(r.target>0&&Number(asked)>0?Number(asked):null);
  const fieldTarget=requested!=null&&Number.isFinite(requested)?requested:r.target??0;
  const different=r.target!=null&&Math.round(fieldTarget)!==Math.round(r.target);
 const target=r.target==null?'—':r.target>0?Math.round(r.target)+'°':'Off';
 const warming=Number(r.target)>0&&r.value!=null&&Number(r.value)<Number(r.target)-2;
 return `<div class="pc-thermalcell desk-thermal-row ${warming?'is-warming':''}"><span class="desk-sensor"><i style="background:${DESK_SENSOR_COLORS[i%DESK_SENSOR_COLORS.length]}"></i>${r.label}${r.kind==='nozzle'?'<small>active</small>':''}</span><strong>${pcFormatTemp(r.value)}<small> °C</small></strong><div class="desk-target">${r.kind?`<form onsubmit="event.preventDefault();pcApplyTemp('${r.kind}')"><input id="pc-target-${r.kind}" aria-label="${r.label} target in degrees Celsius" type="number" min="0" max="${{nozzle:320,bed:120,chamber:65}[r.kind]}" step="1" required value="${fieldTarget}" ${dis}><button class="pc-btn" type="submit" aria-label="Apply ${r.label} target" ${dis}>✓</button></form>`:`<span title="Target is read only">${target}</span>`}${different?`<small>Printer reports ${target}</small>`:''}</div></div>`;
 }).join('');
 renderCameraJob();
 $('#ctl').innerHTML=`<div class="printer-controls"><section class="desk-panel ${connected&&readings.some(r=>Number(r.target)>0)?'heaters-on':''}"><div class="desk-panel-heading"><h2>Temperatures</h2><button class="pc-btn" type="button" title="Turn off nozzle, bed and chamber heaters; leave fans and AMS dryers running" aria-label="Cooldown: turn off printer heaters" ${!connected||pending||pcBusy()?'disabled':''} onclick="pcCooldown()">Cooldown</button></div>${deskThermalChart(readings)}<div class="desk-thermal-head"><span>Sensor</span><span>Current</span><span>Target</span></div><div class="desk-thermals">${thermal}</div></section><section class="desk-panel"><div class="desk-panel-heading"><h2>Quick actions</h2><button class="pc-quiet" onclick="pcOpen('prepare')">Edit presets${pcIcon('arrow')}</button></div>${pcPreheatButtons()}${s.panel==='prepare'?pcEditor():''}</section><section class="desk-panel desk-speed-only" aria-label="Print speed"><div class="pc-speed" role="group" aria-label="Print speed">${[[1,'Silent'],[2,'Standard'],[3,'Sport'],[4,'Ludicrous']].map(([v,n])=>`<button ${dis} aria-pressed="${Number(D.spd_lvl)===v}" onclick="pcSpeed(${v})">${n}</button>`).join('')}</div></section></div>`;
 mountControlWidgets();
 renderControlError();
 if(activeDraft){const input=document.getElementById(activeDraft.id);if(input&&!input.disabled){input.value=activeDraft.value;input.focus({preventScroll:true})}}
}

function renderPrinterAction(){const line=document.getElementById('printer-current-action');if(line)line.textContent=(D&&PC_DATA_PRINTER===P?D.current_action:null)||roomInfo(P)?.current_action||'Waiting for live printer status';}
function startCamWhenReady(){
 const img=document.getElementById('camimg');if(!img||DLG||document.hidden||!camWanted()||!img.dataset.src||CAMERA_BUFFERS.has(img))return;
 cameraBufferStart(img,img.dataset.src,()=>{const n=$('#camnote');if(n)n.textContent='Camera reconnecting…';});
}
window.addEventListener('load',()=>setTimeout(startCamWhenReady,0),{once:true});
function renderCameraJob(){
 const panel=$('#printer-job');if(!panel)return;
 if(!D||PC_DATA_PRINTER!==P){panel.hidden=true;panel.replaceChildren();return;}
 const s=pcState(),connected=pcConnected();
 const jobActive=['RUNNING','PREPARE','PAUSE'].includes(D.print_state),paused=D.print_state==='PAUSE',jobDis=(!connected||s.inflight.has('print'))?'disabled':'';
 const job=jobActive?`<div class="pc-job"><div><div class="pc-jobtitle"><span>${!connected?'Last report · ':''}${paused?'Paused':D.print_state==='PREPARE'?'Preparing':'Printing'}</span><span class="pc-file" title="${pcEscape(D.print_name)}">${pcEscape(D.print_name||'Current print')}</span></div><div class="pc-progressline"><span>${D.print_pct!=null?pcEscape(D.print_pct)+'%':'—'}</span><div class="pc-progress" role="progressbar" aria-label="Print progress" aria-valuemin="0" aria-valuemax="100" ${D.print_pct!=null?'aria-valuenow="'+Number(D.print_pct)+'"':''}><i style="width:${Math.max(0,Math.min(100,Number(D.print_pct)||0))}%"></i></div><span>${D.print_left_min!=null?hm(D.print_left_min)+' left':''}${D.print_layer!=null?' · layer '+D.print_layer+'/'+(D.print_layers??'—'):''}</span></div></div><div class="pc-actions"><button class="pc-btn" ${jobDis} onclick="printCtl('${paused?'resume':'pause'}')">${pcIcon(paused?'play':'pause')}${paused?'Resume':'Pause'}</button><button class="pc-btn stop" ${jobDis} onclick="printCtl('stop')">Stop print</button></div></div>`:'';
 panel.hidden=!jobActive;panel.innerHTML=job;
 layoutQueueMeasure();
}
function renderCam(){
  dropCam();CAM_TRIES=0;$('#cam').dataset.p=P;
  const on=camWanted();
  $('#cam').innerHTML=`<div class="envbox"><span class="title">${pcEscape(P||'Printer')} · Camera</span>
    <span class="temp" id="camnote">${on?'Connecting to live camera…':pcConnected()?'Camera off':'Waiting for printer connection'}</span>
    <span class="presets"><button type="button" class="camera-expand" onclick="openMovementWindow()" title="Open movement controls and direct live camera in a new window">Move + live camera ↗</button>${on?`<button type="button" class="camera-expand" onclick="openCameraFullscreen(P)" title="Fullscreen camera">${cameraExpandIcon()} Fullscreen</button>`:''}<button class="${on?'':'primary'}" ${!pcConnected()?'disabled':''} onclick="toggleCam()">${on?'■ Stop camera':'▶ Show camera'}</button>
    <label class="sw ${CAM_AUTO?'on':''}" role="switch" tabindex="0" aria-label="Automatic camera" aria-checked="${!!CAM_AUTO}" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();this.click()}" style="margin-left:4px" title="Show the camera automatically on whichever printer tab is open" onclick="autoCam()"><span class="track"></span>Auto <b>${CAM_AUTO?'ON':'OFF'}</b></label></span></div>
    ${on?`<div class="camera-stage"><span class="camera-placeholder">Connecting camera…</span><img id="camimg" class="camera-fading" alt="Live printer camera" data-src="/api/p/${encodeURIComponent(P)}/api/cam.mjpg?t=${Date.now()}"></div>`:''}<div class="printer-current-action" id="printer-current-action" role="status" aria-live="polite"></div><div id="printer-job" class="printer-controls" hidden></div>`;
  renderPrinterAction();renderCameraJob();
  if(document.readyState==='complete')setTimeout(startCamWhenReady,0);
}
function autoCam(){CAM_AUTO=!CAM_AUTO;try{localStorage.setItem('ams_cam_auto',CAM_AUTO?'1':'0')}catch(e){};CAM_ON={};if(!camWanted())cameraStopPrinter(P);renderCam();}
async function toggleCam(){
  if(!camWanted()){
    try{const s=await (await fetch(`/api/p/${encodeURIComponent(P)}/api/cam_status`)).json();
      if(!s.ffmpeg){alert('ffmpeg is not installed on this Mac.\n\nInstall it once with:\n  brew install ffmpeg\n\nThen the camera works for every printer.');return;}}catch(e){}
  }
  CAM_ON[P]=!camWanted();if(!CAM_ON[P])cameraStopPrinter(P);renderCam();
}
let CAM_TRIES=0,CAM_HEALTH_BUSY=false;
async function camHealth(){
  if(CAM_HEALTH_BUSY||!camWanted()||document.hidden||document.readyState!=='complete')return;
  const printer=P;
  const img=document.getElementById('camimg'); if(!img)return;
  if(CAMERA_BUFFERS.has(img))return;
  if(!img.getAttribute('src')){startCamWhenReady();return;}
  CAM_HEALTH_BUSY=true;try{
    const s=await dashboardJSON(`/api/p/${encodeURIComponent(printer)}/api/cam_status`,4000);
    if(P!==printer||document.getElementById('camimg')!==img)return;
    const n=$('#camnote');
    if(s.running&&s.age<10){CAM_TRIES=0;if(img.naturalWidth>0&&!img.closest('.camera-stage').classList.contains('camera-ready'))cameraFadeBegin(img);if(n)n.textContent=`live · ${s.viewers} viewer${s.viewers==1?'':'s'}`;return;}
    if(CAM_TRIES>=5){if(n)n.textContent='stream stalled — press ▶ to try again';return;}
    CAM_TRIES++; if(n)n.textContent=`reconnecting (${CAM_TRIES}/5)…`;
    cameraBufferStop(img);cameraFadeReset(img);img.removeAttribute('src');startCamWhenReady();
  }catch(e){}finally{CAM_HEALTH_BUSY=false;}
}
setInterval(camHealth,15000);
function card(r){
  const s=r.spool;
  const reading=r.reading?`<span class="rfid-reading-indicator" role="status" aria-label="Reading RFID in slot ${r.slot}" title="Reading RFID in slot ${r.slot}"></span>`:'';
  const requested=!r.reading&&RFID_REQUESTED.get(P+':'+r.key)>Date.now();
  if(r.cool_read&&!s)return`<div class="card" style="border-color:#ff9f0a"><div class="slot">Slot ${r.slot}${reading}</div>
    <div class="name" style="color:#ffb340">Waiting to read the tag</div>
    <div class="meta">${pcEscape(r.tag_read_note)} You can set the filament manually at any time.</div>
    <div class="acts"><button class="primary" onclick="setSpoolDlg('${r.key}',${r.slot},'${r.ams}')">Set filament</button>
    <button onclick="api('/api/cancel_read',{slot:'${r.key}'}).then(nudge)">Stop waiting</button></div></div>`;
  if(!s&&r.pending)return`<div class="card" style="border-color:var(--blue)"><div class="slot">Slot ${r.slot}${reading}</div><div class="name" style="color:#7cc4ff">${r.reading?'Reading RFID…':'Waiting for RFID read…'}</div>
    <div class="meta">Spool inserted, RFID not read yet${r.remembered?` (slot previously held ${r.remembered} — not assumed)`:''}. The AMS reads tags when it can; mid-print this may wait.</div>
    <div class="acts"><button onclick="readRfid(${r.ams_id},${r.slot-1})">Read RFID now</button>
    <button onclick="setSpoolDlg('${r.key}',${r.slot},'${r.ams}')">Set filament manually</button></div></div>`;
  if(!s)return r.present?`<div class="card warn" style="opacity:1"><div class="slot">Slot ${r.slot}${reading}</div><div class="name unnamed">Spool detected — type unknown</div>
    <div class="meta">The printer sees a spool but no RFID. Tell me what it is so it can be tracked and dried.</div>
    <div class="acts"><button class="primary" onclick="setSpoolDlg('${r.key}',${r.slot},'${r.ams}')">Set filament</button></div></div>`
   :`<div class="card empty"><div class="slot">Slot ${r.slot}${reading}</div><div class="name">Empty</div>
    <div class="meta">The printer detects no filament here. Push it in until the AMS grips it — the tag is read as it pulls.</div>
    <div class="acts"><button onclick="readRfid(${r.ams_id},${r.slot-1})">Read RFID</button>
    <button onclick="setSpoolDlg('${r.key}',${r.slot},'${r.ams}')">Set filament</button></div></div>`;
  if(s.provisional&&r.tag_retry_queued){
    return`<div class="card" style="border-color:var(--blue)"><div class="slot">Slot ${r.slot}${reading}</div>
    <div class="name" style="color:#7cc4ff">RFID not identified yet</div>
    <div class="meta">${r.heating?'This AMS is drying. The tag will be retried automatically after drying ends and it cools.':'The tag will be retried automatically when the AMS is ready.'} The printer has not reported this spool’s material, so its identity and drying target are still unknown.</div>
    <div class="acts"><button onclick="readRfid(${r.ams_id},${r.slot-1})">Read RFID now</button><button onclick="setSpoolDlg('${r.key}',${r.slot},'${r.ams}')">Set manually</button></div></div>`;
  }
  if(s.provisional){
    return`<div class="card" style="border-color:var(--blue)"><div class="slot">Slot ${r.slot}${reading}</div>
    <div class="name" style="color:#7cc4ff">No filament set</div>
    <div class="meta">${r.no_tag?'The printer scanned this slot and found no RFID tag (third-party spool).':'No RFID tag read yet.'} Pick the material for drying. Your printer and Orca profiles stay as set.</div>
    <div class="acts"><button class="primary" onclick="setSpoolDlg('${r.key}',${r.slot},'${r.ams}')">Set filament</button>
    ${r.no_tag?'':`<button onclick="readRfid(${r.ams_id},${r.slot-1})">Read RFID</button>`}</div></div>`;
  }
  const nm=s.name||`Unknown ${s.type||'spool'}`;
  const col=((s.manual?s.color:r.color)||r.color||s.color||'').replace(/^#/,'').slice(0,6);
  return`<div class="card ${cls(s)}">
  <div class="slot"><span class="slot-label" title="${pcEscape('Slot '+r.slot+' · '+(s.type||'?')+(s.manual||s.from_printer?' · set by hand':''))}">Slot ${r.slot} · ${s.type||'?'}${s.manual||s.from_printer?' · set by hand':''}${r.cool_read?' · <span style="color:#ffb340">tag re-check queued</span>':''}</span>${reading}${r.loaded?'<span class="loaded">Loaded</span>':''}</div>
  <div class="name ${s.name?'':'unnamed'}" style="display:flex;align-items:center;gap:8px">${/^[0-9a-f]{6}$/i.test(col)?`<span class="slot-colour-chip" title="${s.manual?'Selected':'Reported'} colour #${col}"><span class="sw2" style="background:#${col}" aria-hidden="true"></span><span class="slot-colour-name">${slotColourName(col)}</span></span>`:'<span class="slot-colour-name">Colour unknown</span>'}<span>${nm}</span></div>
  ${requested?'<div class="meta">RFID request sent · waiting for the printer to start the scan</div>':''}
  ${r.rfid&&Number.isFinite(r.remain)&&r.remain>=0&&r.remain<=100?`<div class="rem"><div class="rembar"><i style="width:${r.remain}%;background:${col?'#'+col:'#888'}"></i></div><span>${r.remain}% left</span></div>`:'<div class="rem">'+(r.rfid?'Remaining not reported':'No RFID · remaining unknown')+'</div>'}
  <div class="verdict">${s.pct>=100?"Ready to print":pcConnected()&&r.heating?(s.rate>0?"Drying":"Warming up")+" · "+Math.round(s.pct)+"% dry":"Needs drying · "+Math.round(s.pct)+"% dry"}</div>
  <div class="bar"><i style="width:${s.pct}%"></i></div>
  <div class="meta">${s.pct>=100?'Drying target met':pcConnected()&&r.heating&&s.eta_h!=null?fmt(s.eta_h)+' estimated remaining':'Needs '+fmt(s.remaining_h)+' at ≥'+s.target_temp+'°C'}</div>
  <div class="acts"><button onclick="setSpoolDlg('${r.key}',${r.slot},'${r.ams}')">Change filament</button><button onclick="readRfid(${r.ams_id},${r.slot-1})">Read RFID</button>${r.loaded?`<button class="primary" onclick="filament(255,'Unload ${nm.replace(/'/g,'')} from the nozzle?')">Unload</button>`:`<button onclick="filament(${r.ams_id*4+(r.slot-1)},'Load ${nm.replace(/'/g,'')} into the nozzle?')">Load</button>`}</div><details ${SPOOL_OPTIONS.has(P+':'+r.key)?'open':''} ontoggle="if(this.open)SPOOL_OPTIONS.add('${pcEscape(P+':'+r.key)}');else SPOOL_OPTIONS.delete('${pcEscape(P+':'+r.key)}')"><summary>Spool options</summary>  <div class="meta">${s.have_h} / ${s.need_h} h at ≥${s.target_temp} °C${s.remaining_h>0?(s.eta_h!=null?` · <b style="color:var(--fg)">${fmt(s.eta_h)} to go</b>${s.rate<1?` at ${r.ams_temp} °C (${Math.round(s.rate*100)}% rate)`:''}`:` · needs ${fmt(s.remaining_h)} at ≥${s.target_temp} °C`):''}<br>
  exposure ${fmt(s.exposure_h)} · ${s.hours_since_dry==null?'never dried here':'dried '+fmt(s.hours_since_dry)+' ago'}</div><div class="acts">${s.name?'':`<button class="primary" onclick="nameDlg('${s.id}')">Name this spool</button>`}
  ${s.provisional?`<button class="primary" onclick="readRfid(${r.ams_id},${r.slot-1})">Read RFID now</button>`:''}
  <button onclick="driedDlg('${s.id}','${nm.replace(/'/g,'')}')">Dried elsewhere</button>
  <button onclick="moveDlg('${s.id}','${nm.replace(/'/g,'')}','${r.key}')">Move to…</button>
  ${s.manual?`<button onclick="if(confirm('Remove ${nm.replace(/'/g,'')} from this slot?'))api('/api/remove_spool',{id:'${s.id}'}).then(load)">Remove</button>`:''}</div></details></div>`;
}
function openDialog(html,onok){const f=$('#form');f.innerHTML=html+`<div class="dialog-actions" style="display:flex;gap:8px;justify-content:flex-end"><button value="cancel">Cancel</button><button value="ok" class="primary">Save</button></div>`;
  const d=$('#dlg');DLG=true;roomStopSnapshots();dropCam();
  d.onclose=async()=>{DLG=false;if(d.returnValue==='ok'){await onok(new FormData(f));}load();tick();startCamWhenReady();roomSnapshots();};d.showModal();}
function nameDlg(id,cur){
  const known=D.known.map(k=>`<option value="${pcEscape(k.id)}">${pcEscape(k.name)} (${pcEscape(k.type)})</option>`).join('');
  openDialog(`<h3 style="margin:0 0 10px">${cur?'Rename':'What spool is this?'}</h3>
  <label>Name (brand, material, colour)</label><input name="name" value="${pcEscape(cur||'')}" placeholder="Sunlu PETG-CF black #2" autofocus>
  ${cur?'':`<label>Material</label><select name="material"><option value="">— keep —</option>${D.materials.map(m=>`<option>${pcEscape(m)}</option>`).join('')}</select>`}
  ${known&&!cur?`<label>…or it's a spool I already know</label><select name="same"><option value="">— no —</option>${known}</select>`:''}`,
  async fd=>{if(fd.get('same'))await api('/api/same',{new_id:id,old_id:fd.get('same')});else if(fd.get('name'))await api('/api/name',{id,name:fd.get('name'),material:fd.get('material')||undefined});});
}
function setSpoolDlg(key,slot,ams){
  const d=$('#dlg'); if(!D)return;const cur=(D.slots.find(x=>x.key===key)||{}).spool;
  d.dataset.printer=P;roomStopSnapshots();dropCam();
  const used=(D.used||[]);
  const chip=t=>`<div class="fchip" role="button" tabindex="0" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();this.click()}" onclick="pickType(${jsArg(key)},${slot},${jsArg(ams)},${jsArg(t.type)})">${pcEscape(t.type)}</div>`;
  const rest=(D.materials||[]).filter(m=>!used.find(u=>u.type===m))
    .map(m=>`<div class="fchip" role="button" tabindex="0" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();this.click()}" onclick="pickType(${jsArg(key)},${slot},${jsArg(ams)},${jsArg(m)})">${pcEscape(m)}</div>`).join('');
  d.querySelector('#form').innerHTML=`<h3 style="margin:0 0 4px">${unitName(ams)} slot ${slot}</h3>
    <div class="meta" style="margin-bottom:12px">Step 1 of 2 — filament type${cur?` (now ${cur.type||'?'})`:''}</div>
    <div class="fchips">${used.map(chip).join('')}</div>
    ${rest?`<div class="meta" style="margin:12px 0 6px">other materials</div><div class="fchips">${rest}</div>`:''}
    <div style="display:flex;justify-content:flex-end;margin-top:14px"><button value="cancel">Cancel</button></div>`;
  DLG=true; d.onclose=()=>{DLG=false;load();tick();startCamWhenReady();roomSnapshots();}; if(!d.open)d.showModal();
}
function pickType(key,slot,ams,type){
  const d=$('#dlg');if(d.dataset.saving==='1')return;
  const seen=((D.used||[]).find(u=>u.type===type)||{}).colors||[];
  const pal=(D.palette||[]);
  const seenChips=seen.map(h=>`<div class="fchip" role="button" tabindex="0" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();this.click()}" onclick="pickFil('${key}','${type}','${h}')"><span class="sw2" style="background:#${h}"></span>${(pal.find(p=>p.hex.toUpperCase()===h.toUpperCase())||{}).name||('#'+h)}</div>`).join('');
  const palChips=pal.filter(p=>!seen.find(h=>h.toUpperCase()===p.hex.toUpperCase()))
    .map(p=>`<div class="fchip" role="button" tabindex="0" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();this.click()}" onclick="pickFil('${key}','${type}','${p.hex}')"><span class="sw2" style="background:#${p.hex}"></span>${p.name}</div>`).join('');
  d.querySelector('#form').innerHTML=`<h3 style="margin:0 0 4px">${unitName(ams)} slot ${slot} — ${type}</h3>
    <div class="meta" style="margin-bottom:12px">Step 2 of 2 — colour</div>
    ${seenChips?`<div class="meta" style="margin-bottom:6px">colours you use</div><div class="fchips">${seenChips}</div><div class="meta" style="margin:12px 0 6px">other colours</div>`:''}
    <div class="fchips">${palChips}</div>
    <div style="display:flex;gap:8px;justify-content:space-between;margin-top:16px">
      <button onclick="event.preventDefault();setSpoolDlg('${key}',${slot},'${ams}')">← back</button>
      <span><button onclick="event.preventDefault();pickFil('${key}','${type}','')">skip colour</button>
      <button value="cancel">Cancel</button></span></div>`;
}
async function pickFil(key,type,color){
 const d=$('#dlg'),printer=d.dataset.printer;if(d.dataset.saving==='1')return;
 d.dataset.saving='1';let message=d.querySelector('.spool-save-message');if(!message){message=document.createElement('div');message.className='spool-save-message';message.setAttribute('role','status');$('#form').append(message);}
 message.textContent='Saving filament…';const controls=[...d.querySelectorAll('button,.fchip')];controls.forEach(el=>{el.setAttribute('aria-disabled','true');if(el.tagName==='BUTTON')el.disabled=true;});
 try{
  const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),15000);
  let result;try{const response=await fetch(`/api/p/${encodeURIComponent(printer)}/api/set_spool`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({slot:key,type,name:type,color:color||''}),signal:controller.signal});if(!response.ok)throw new Error('Save not confirmed');result=await response.json();}finally{clearTimeout(timer);}
  if(result.result!=='ok')throw new Error(result.result||'Save not confirmed');
  d.close();if(P===printer){await load();}nudge();
 }catch(e){message.textContent='Could not confirm the save. Your selection is still here. Check the connection, then tap the colour again to retry.';}
 finally{delete d.dataset.saving;controls.forEach(el=>{el.removeAttribute('aria-disabled');if(el.tagName==='BUTTON')el.disabled=false;});}
}
function setSpoolDlgFor(r){setSpoolDlg(r.key,r.slot,r.ams);}
function moveDlg(id,nm,cur){
  const opts=D.slots.filter(r=>r.key!==cur).map(r=>`<option value="${r.key}">${unitName(r.ams)} slot ${r.slot}${r.spool?' — currently '+(r.spool.name||r.spool.type):' (empty)'}</option>`).join('');
  openDialog(`<h3 style="margin:0 0 10px">Move ${nm}</h3><p class="meta">Drying history moves with it. Do this when you physically move the spool; then set its type on the printer as usual.</p><label>To</label><select name="slot">${opts}</select>`,
  async fd=>api('/api/move_spool',{id,slot:fd.get('slot')}));}
function driedDlg(id,nm){openDialog(`<h3 style="margin:0 0 10px">${nm} — dried in an external dryer</h3>
  <label>Hours</label><input name="hours" type="number" step="0.5" value="8"><label>Temperature °C</label><input name="temp" type="number" value="65">`,
  async fd=>api('/api/dried',{id,hours:+fd.get('hours'),temp:+fd.get('temp')}));}
async function dry(id,on,temp,hours,rotate){const r=await api('/api/dry',{ams_id:id,on,temp,hours,rotate});if(r.result&&!r.result.startsWith('ok'))alert(r.result);await load();setTimeout(load,1500);}
function dryDlg(id,ams){
  const isHT=id>=128; const u=D.units[ams]||{}; const max=u.temp||(isHT?85:65); const initial=Math.min(max,u.temp||max);
  openDialog(`<h3 style="margin:0 0 10px">Dry ${unitName(ams)}</h3><p class="meta">Temperature is set from the filament already in this AMS.</p>
  <div style="display:flex;gap:10px"><div style="flex:1"><label>Temp °C (max ${max} — safe for every spool in this unit)</label><input name="temp" type="number" value="${initial}" min="45" max="${max}"></div>
  <div style="flex:1"><label>Hours</label><input name="hours" type="number" value="24" min="1" max="24"></div></div>
  ${isHT?'':'<label><input type="checkbox" name="rotate" style="width:auto;margin-right:6px">Rotate spools while drying</label><br><br>'}`,
  async fd=>dry(id,true,Math.min(max,+fd.get('temp')),+fd.get('hours'),!!fd.get('rotate')));
}
let REV={};
function summFromRev(v){
  const busy=['RUNNING','PREPARE'].includes(v.print_state), paused=v.print_state==='PAUSE';
  let text;
  if(v.refresh_delayed) text='Updating connection…';
  else if(v.error||v.stale||v.mqtt!=='connected') text='offline';
  else if(busy) text=(v.print_pct!=null?v.print_pct+'%':'printing')+(v.print_left_min?' · '+hm(v.print_left_min):'');
  else if(paused) text='paused';
  else text=(v.heating?'drying':v.needs?v.needs+' need drying':v.spools?'idle · all dry':'idle');
  return{error:!!v.error||v.stale||v.mqtt!=='connected',busy,paused,heating:v.heating,ok:v.spools&&!v.needs,text};
}
let TICK_PENDING=null;const LOAD_PENDING=new Map(),LOAD_FAILED=new Set();
function tick(){
 applyDashboardUpdate();
 if(document.hidden)return Promise.resolve();
 if(TICK_PENDING)return TICK_PENDING;
 TICK_PENDING=(async()=>{
  if(!PRINTERS.length){await loadPrinters();return;}
  const queue=[...PRINTERS];
  const poll=async()=>{while(queue.length){const x=queue.shift();
   try{
    const v=await dashboardJSON(`/api/p/${encodeURIComponent(x.name)}/api/rev`,10000);
    ROOM_DATA.set(x.name,v);x.summary=summFromRev(v);
    if(REV[x.name]!==v.rev||x.name===P&&(!D||LOAD_FAILED.has(P))){REV[x.name]=v.rev;if(x.name===P)load();}
   }catch(e){const previous=roomInfo(x.name);ROOM_DATA.set(x.name,{...previous,refresh_delayed:true});x.summary={...(x.summary||{}),error:false,text:'Updating connection…'};}
  }};
  await Promise.all([poll(),poll()]);
  if(!DLG&&!editing())renderTabs();
 })().finally(()=>{TICK_PENDING=null;});return TICK_PENDING;
}
async function load(){
 if(DLG||document.hidden)return false;
 if(!PRINTERS.length)await loadPrinters();
 const requestedPrinter=P;
 if(LOAD_PENDING.has(requestedPrinter))return LOAD_PENDING.get(requestedPrinter);
 loadNetwork();
 const pending=(async()=>{try{
  const path=requestedPrinter?`/api/p/${encodeURIComponent(requestedPrinter)}/api/state`:'/api/state';
  const state=await dashboardJSON(path,12000);
  if(requestedPrinter!==P||DLG)return false;
  D=state;PC_DATA_PRINTER=requestedPrinter;LOAD_FAILED.delete(requestedPrinter);render();
  const x=PRINTERS.find(x=>x.name===P);if(x){x.summary=summ(D);renderTabs();}
  return true;
 }catch(e){LOAD_FAILED.add(requestedPrinter);if(requestedPrinter===P){const c=$('#conn');c.className='conn bad';c.querySelector('span').textContent='Refresh delayed — retrying';renderPrinterFilamentMap();console.warn('Dashboard refresh failed',e);}return false;
 }finally{LOAD_PENDING.delete(requestedPrinter);}})();
 LOAD_PENDING.set(requestedPrinter,pending);return pending;
}
function summ(d){const heating=d.slots.some(r=>r.heating);const sp=d.slots.filter(r=>r.spool);const bad=sp.filter(r=>r.spool.pct<60).length;
  const busy=['RUNNING','PREPARE'].includes(d.print_state), paused=d.print_state==='PAUSE';
  let text;
  if(d.mqtt.error||d.stale) text='offline';
  else if(busy) text=(d.print_pct!=null?d.print_pct+'%':'printing')+(d.print_left_min?' · '+hm(d.print_left_min):'');
  else if(paused) text='paused';
  else text=(heating?'drying':bad?bad+' need drying':sp.length?'idle · all dry':'idle');
  return{error:!!d.mqtt.error||d.stale,busy,paused,heating,ok:sp.length&&!bad,text};}
function refreshTabs(){return tick();}
(async()=>{await loadPrinters();loadNetwork();await load();tick();
  setInterval(tick,3000);            // live: cheap revision poll, full reload only on change
  setInterval(load,600000);          // safety net
})();
let ATTENTION={incidents:[],history:[],settings:{},native:false},ATTENTION_BUSY=false,ATTENTION_SHOWN=new Set(),ATTENTION_NOTIFIED=new Set(),ATTENTION_DEMO=false,ATTENTION_VIEW='active',ATTENTION_EXPANDED=false;
let ALERT_DISMISSED={};const ACT_DISMISSED=new Map(),ALERT_ACK_BUSY=new Set();
try{const saved=JSON.parse(sessionStorage.getItem('ams_dismissed_alerts')||'{}');if(saved&&typeof saved==='object'&&!Array.isArray(saved))ALERT_DISMISSED=saved;}catch(e){}
function alertFingerprint(printer){
 const v=roomInfo(printer),errors=v.errors||{},faults=[...(errors.print_error?[errors.print_error]:[]),...(errors.hms||[]),...(v.process_faults||[])];
 const keys=ATTENTION.incidents.filter(i=>i.printer===printer).map(i=>String(i.code));
 if(printer===P&&PC_DATA_PRINTER===printer&&D){const e=D.errors||{};faults.push(...(e.print_error?[e.print_error]:[]),...(e.hms||[]),...(D.process_faults||[]));}
 keys.push(...faults.filter(f=>f.blocking).map(f=>String(f.code)));
 return [...new Set(keys)].sort().join('|');
}
function saveDismissedAlerts(){try{sessionStorage.setItem('ams_dismissed_alerts',JSON.stringify(ALERT_DISMISSED));}catch(e){}}
function printerAlertsDismissed(printer){
 const key=alertFingerprint(printer),saved=ALERT_DISMISSED[printer];if(!key||!saved)return false;
 if(typeof saved==='string')return saved===key;
 return saved.fingerprint===key&&ATTENTION.incidents.filter(i=>i.printer===printer).every(i=>saved.ids.includes(i.id));
}
async function acknowledgeDismissedAlerts(printer){
 if(ALERT_ACK_BUSY.has(printer)||!printerAlertsDismissed(printer))return;
 const incidents=ATTENTION.incidents.filter(i=>i.printer===printer);
 // Upgrade earlier panel-only dismissals and retain incident identities so a
 // recurrence of the same error is still treated as a new notification.
 if(typeof ALERT_DISMISSED[printer]==='string'){ALERT_DISMISSED[printer]={fingerprint:alertFingerprint(printer),ids:incidents.map(i=>i.id)};saveDismissedAlerts();}
 const ids=incidents.filter(i=>!i.acknowledged).map(i=>i.id);if(!ids.length)return;
 ALERT_ACK_BUSY.add(printer);
 try{await attentionSend({action:'acknowledge',ids});renderAttention();}
 catch(e){console.warn('Alert acknowledgement will retry',e);}
 finally{ALERT_ACK_BUSY.delete(printer);}
}
function dismissPrinterAlerts(printer){
 if(printer===P){ATTENTION_EXPANDED=false;$('#attention-panel').hidden=true;}
 ALERT_DISMISSED[printer]={fingerprint:alertFingerprint(printer),ids:ATTENTION.incidents.filter(i=>i.printer===printer).map(i=>i.id)};saveDismissedAlerts();
 if(printer===P){ATTENTION_DEMO=false;ATTENTION_EXPANDED=false;ATTENTION_VIEW='active';renderAttention();attentionBadge();}
 attentionBadge();renderTabs();renderRoom();acknowledgeDismissedAlerts(printer);
}
document.addEventListener('click',e=>{const b=e.target.closest('[data-dismiss-alert]');if(b)dismissPrinterAlerts(b.dataset.dismissAlert);});
function dismissActivity(){const a=D?.activity;if(a)ACT_DISMISSED.set(P,JSON.stringify([a.op,a.steps,a.outcome]));renderAct();}
function dismissControlFeedback(){pcState().feedback=null;renderCtl();}
const attentionLocal=['localhost','127.0.0.1','[::1]'].includes(location.hostname);
// Keep fleet-wide notifications, but scope every visible list and action to its printer.
const attentionForPrinter=items=>items.filter(i=>i.printer===P);
const attentionPending=(printer=null)=>ATTENTION.incidents.filter(i=>(printer===null||i.printer===printer)&&!i.acknowledged&&!printerAlertsDismissed(i.printer)&&(!i.snoozed_until||i.snoozed_until*1000<=Date.now()));
function attentionEscape(s){return pcEscape(s);}
function attentionIcon(){return '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><path d="M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4"/></svg>';}
function attentionBadge(){const n=attentionPending(null).length,b=$('#attention-button');if(!b)return;b.className=n?'attention-button unread':'attention-button';b.innerHTML=attentionIcon()+`<span>${n?n+' new alert'+(n===1?'':'s'):'Alerts'}</span>`;b.setAttribute('aria-label',n?n+' unacknowledged alerts':'Open alerts');renderFleetAlerts();}
function attentionNotify(items){for(const item of items){if(ATTENTION_NOTIFIED.has(item.id))continue;ATTENTION_NOTIFIED.add(item.id);try{if('Notification' in window&&Notification.permission==='granted'){const title=item.printer==='Stock'?(String(item.code||'').startsWith('SPIDER_UNAVAILABLE_')?'Filament unavailable at configured store':'Filament stock is low'):item.printer+' needs attention';const n=new Notification(title,{body:item.message,tag:'ams-'+item.id,requireInteraction:true,silent:!!ATTENTION.native});n.onclick=()=>{window.focus();if(item.printer==='Stock')roomShow('drying');else attentionViewPrinter(item.printer);n.close();};}}catch(e){console.warn('Notification unavailable',e);}}}
function renderFleetAlerts(){
 const panel=$('#room-alert-dropdown');if(!panel||panel.hidden)return;
 const items=attentionPending(null).slice().sort((a,b)=>(a.severity==='warning')-(b.severity==='warning')||a.printer.localeCompare(b.printer));
 panel.innerHTML=items.length?items.map(i=>`<div class="fleet-alert"><button type="button" class="fleet-alert-open" data-open-alert-id="${attentionEscape(i.id)}"><strong>${attentionEscape(i.printer==='Stock'?'Filament stock':i.printer)}</strong><span>${attentionEscape(i.message)}</span></button><button type="button" class="fleet-alert-dismiss" data-dismiss-alert-id="${attentionEscape(i.id)}" aria-label="Dismiss alert for ${attentionEscape(i.printer)}">Dismiss</button></div>`).join(''):'<p class="attention-empty">No new alerts.</p>';
}
function openFleetAlerts(){const panel=$('#room-alert-dropdown'),button=$('#attention-button');panel.hidden=!panel.hidden;button.setAttribute('aria-expanded',String(!panel.hidden));renderFleetAlerts();}
function closeFleetAlerts(){const panel=$('#room-alert-dropdown');if(panel)panel.hidden=true;$('#attention-button')?.setAttribute('aria-expanded','false');}
function openFleetAlert(id){const item=ATTENTION.incidents.find(i=>i.id===id);if(!item)return;closeFleetAlerts();if(item.printer==='Stock'){roomShow('drying');requestAnimationFrame(()=>$('#stock-alert')?.scrollIntoView({behavior:'smooth',block:'center'}));return;}selectPrinter(item.printer,true);openAttention('active',true);}
async function dismissFleetAlert(id){try{const item=ATTENTION.incidents.find(i=>i.id===id);await attentionSend({action:'acknowledge',ids:[id]});if(item?.printer&&item.printer!=='Stock'&&!attentionPending(item.printer).length)dismissPrinterAlerts(item.printer);renderFleetAlerts();renderAttention();}catch(e){const panel=$('#room-alert-dropdown');if(panel)panel.insertAdjacentHTML('beforeend',`<p class="attention-error">${attentionEscape(e.message)}</p>`);}}
document.addEventListener('click',e=>{const open=e.target.closest('[data-open-alert-id]'),dismiss=e.target.closest('[data-dismiss-alert-id]');if(open){openFleetAlert(open.dataset.openAlertId);return;}if(dismiss){dismissFleetAlert(dismiss.dataset.dismissAlertId);return;}if(!e.target.closest('.room-alert-wrap'))closeFleetAlerts();});
function attentionItems(){
 const items=attentionForPrinter(ATTENTION.incidents),data=PC_DATA_PRINTER===P&&D?D:roomInfo(P),e=data.errors||{};
 const faults=[...(e.print_error?[e.print_error]:[]),...(e.hms||[]),...(data.process_faults||[])].filter(f=>f.blocking);
 for(const f of faults){if(!items.some(i=>i.printer===P&&i.code===f.code))items.push({printer:P,code:f.code,message:f.text,url:f.url,severity:'error',acknowledged:true});}
 return items;
}
async function pollAttention(){
 if(ATTENTION_BUSY)return;ATTENTION_BUSY=true;
 try{
  const data=await dashboardJSON('/api/attention');if(!Array.isArray(data.incidents))return;
  const changed=JSON.stringify(ATTENTION)!==JSON.stringify(data);ATTENTION=data;
  for(const printer of Object.keys(ALERT_DISMISSED)){if(roomOnline(roomInfo(printer))&&!data.incidents.some(i=>i.printer===printer)&&!alertFingerprint(printer)){delete ALERT_DISMISSED[printer];saveDismissedAlerts();}}
  for(const printer of Object.keys(ALERT_DISMISSED))acknowledgeDismissedAlerts(printer);
  attentionBadge();
  const fresh=attentionPending().filter(i=>!ATTENTION_SHOWN.has(i.id));attentionNotify(fresh);
  fresh.forEach(i=>ATTENTION_SHOWN.add(i.id));
  if(changed&&!ATTENTION_DEMO){renderAttention();renderAct();renderTabs();}
  if(location.hash==='#alert-preview'){if(attentionPending().length)openAttention();else previewAttention();history.replaceState(null,'',location.pathname+location.search);}
  if(location.hash==='#alerts'){openAttention();history.replaceState(null,'',location.pathname+location.search);}
 }catch(e){console.warn('Alert status unavailable',e);}finally{ATTENTION_BUSY=false;}
}
function openAttention(view='active',force=false){if(ROOM_VIEW!=='detail'){const name=ATTENTION.incidents[0]?.printer||PRINTERS.find(x=>roomInfo(x.name).has_error)?.name||P||PRINTERS[0]?.name;if(name)roomOpen(name,'alerts');return;}if(view==='active'&&!ATTENTION_DEMO&&!attentionItems().length)view='history';if(!force&&ATTENTION_EXPANDED&&ATTENTION_VIEW===view){dismissPrinterAlerts(P);return;}ATTENTION_VIEW=view;ATTENTION_EXPANDED=true;renderAttention();$('#attention-panel').scrollIntoView({behavior:'smooth',block:'center'});}
function closeAttention(){ATTENTION_DEMO=false;ATTENTION_EXPANDED=false;ATTENTION_VIEW='active';renderAttention();attentionBadge();}
function previewAttention(){ATTENTION_DEMO=true;ATTENTION_VIEW='active';openAttention();}
function renderAttention(){
 const all=ATTENTION_DEMO?[{id:'demo',printer:'Example printer',message:'Filament could not feed into the nozzle. Check the spool and filament path.',code:'PREVIEW',severity:'error',acknowledged:false}]:attentionItems();
 const pending=ATTENTION_DEMO?all:attentionPending(P);const urgent=pending.length>0;
 const d=$('#attention-panel');
 if(!all.length&&ATTENTION_VIEW==='active'&&!ATTENTION_DEMO){ATTENTION_EXPANDED=false;d.hidden=true;$('#attention-content').innerHTML='';$('#attention-actions').innerHTML='';return;}
 d.hidden=!ATTENTION_EXPANDED&&!ATTENTION_DEMO&&(!urgent||printerAlertsDismissed(P));
 if(d.hidden)return;
 const title=ATTENTION_VIEW==='settings'?'Alert settings':ATTENTION_VIEW==='history'?'Recent alerts':urgent?(ATTENTION_DEMO?'Alert preview':'Needs attention'):all.length?'Active errors':'Alerts';
 $('#attention-title').textContent=title;
 $('#attention-subtitle').textContent=ATTENTION_VIEW==='settings'?'Choose how the dashboard gets your attention.':ATTENTION_VIEW==='history'?`Resolved alerts for ${P||'this printer'}.`:ATTENTION_DEMO?'This is a preview. No printer command is sent.':urgent?`${pending.length} new alert${pending.length===1?'':'s'}`:'';
 $('#attention-subtitle').hidden=!$('#attention-subtitle').textContent;
 let content='';
 if(ATTENTION_VIEW==='settings'){
  const permission='Notification' in window?Notification.permission:'unsupported';
  content=`<div class="attention-setting"><div><strong>Sound on this Mac</strong><p>Chime when a fault stops printing or a drying request, then every 30 seconds until acknowledged or snoozed.</p></div><button role="switch" aria-label="Sound on this Mac" aria-checked="${!!ATTENTION.settings.sound}" ${!attentionLocal||!ATTENTION.native?'disabled':''} onclick="attentionSetting('sound',${!ATTENTION.settings.sound})">${ATTENTION.settings.sound?'On':'Off'}</button></div><div class="attention-setting"><div><strong>Bring Chrome forward</strong><p>Focus an existing AMS Dryness tab when a fault stops printing or drying. Never opens tabs or windows. If the dashboard is closed, sound alerts still work. macOS may ask to allow Chrome control.</p></div><button role="switch" aria-label="Bring Chrome forward" aria-checked="${!!ATTENTION.settings.bring_chrome}" ${!attentionLocal||!ATTENTION.native?'disabled':''} onclick="attentionSetting('bring_chrome',${!ATTENTION.settings.bring_chrome})">${ATTENTION.settings.bring_chrome?'On':'Off'}</button></div><div class="attention-setting"><div><strong>Browser notifications</strong><p>${permission==='granted'?'Enabled. Click a notification to return to the error.':permission==='denied'?'Blocked in this browser. Allow notifications in the site settings.':!isSecureContext?'Browser notifications require localhost or HTTPS. Mac sound and Chrome alerts still work.':'Optional system banners for this browser.'}</p></div><button ${permission!=='default'||!isSecureContext?'disabled':''} onclick="enableAttentionNotifications()">${permission==='granted'?'Enabled':'Enable'}</button></div><div class="attention-setting"><div><strong>Preview</strong><p>See an example inline alert without changing any printer.</p></div><button onclick="previewAttention()">Preview alert</button></div>${attentionLocal&&ATTENTION.native?'<button onclick="testMacAttention()">Test sound &amp; Chrome</button>':''}${!attentionLocal?'<p class="meta">Open localhost:8765 on the Mac to change its sound and Chrome settings.</p>':''}${ATTENTION.native_error?'<p class="attention-error">'+attentionEscape(ATTENTION.native_error)+'</p>':''}`;
 }else if(ATTENTION_VIEW==='history'){
  const history=attentionForPrinter(ATTENTION.history);
  content=history.length?history.map(i=>`<article class="attention-item resolved"><div class="attention-printer">${attentionEscape(i.printer)} · resolved</div><p>${attentionEscape(i.message)}</p><small>${attentionEscape(new Date(i.resolved_at*1000).toLocaleString())}</small></article>`).join(''):'<p class="attention-empty">No resolved alerts yet.</p>';
 }else{
  const visible=ATTENTION_EXPANDED||ATTENTION_DEMO?all:all.filter(i=>pending.some(n=>n.id===i.id));
  content=visible.length?visible.map(i=>`<article class="attention-item ${i.severity==='warning'?'warning':''}"><div class="attention-printer">${attentionEscape(i.printer)}${i.acknowledged?' · acknowledged':i.snoozed_until*1000>Date.now()?' · snoozed for 5 minutes':''}</div><p>${attentionEscape(i.message)}</p><div class="attention-detail">${!ATTENTION_DEMO&&i.printer===P&&/^07[0-9a-f]{2}8004$/i.test(String(i.code).replace(/[_-]/g,''))?`<button class="primary" ${PULLBACK_SENDING.has(P)||D?.pullback_retry?.busy?'disabled':''} onclick="retryPullback()">${PULLBACK_SENDING.has(P)||D?.pullback_retry?.busy?'Retrying pullback…':'Retry pullback'}</button><span>${attentionEscape(D?.pullback_retry?.message||'Automatic recovery: up to 2 retries')}</span>`:''}${i.url&&i.url.startsWith('https://e.bambulab.com/')?`<a href="${attentionEscape(i.url)}" target="_blank" rel="noopener noreferrer">Troubleshooting guide ↗</a>`:''}${!ATTENTION_DEMO&&i.printer!==P?`<button data-printer="${attentionEscape(i.printer)}" onclick="attentionViewPrinter(this.dataset.printer)">View printer</button>`:''}${!ATTENTION_DEMO&&i.printer===P&&D?.print_state==='PAUSE'&&!String(i.code).startsWith('DRYING_')?`<button onclick="resolve('resume','Resume the print after fixing the fault?')">Resume print</button>`:''}</div></article>`).join(''):'';
 }
 $('#attention-content').innerHTML=content;
 $('#attention-actions').innerHTML=!ATTENTION_DEMO&&ATTENTION_VIEW==='history'&&attentionForPrinter(ATTENTION.history).some(i=>!i.acknowledged)?'<button class="primary" onclick="attentionReadHistory()">Mark history as read</button>':'';
 $('#attention-message').textContent='';
}
const PULLBACK_SENDING=new Set();
async function retryPullback(){
 const printer=P;if(PULLBACK_SENDING.has(printer))return;
 PULLBACK_SENDING.add(printer);renderAttention();
 let message='';
 try{
  const response=await fetch(`/api/p/${encodeURIComponent(printer)}/api/retry_pullback`,{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
  if(!response.ok)throw new Error('Retry not confirmed');
  const result=await response.json();message=result.result==='ok'?'Retry sent — waiting for the printer':result.result;
 }catch(e){message='Could not confirm the retry. Refresh printer status before trying again.';}
 finally{PULLBACK_SENDING.delete(printer);if(P===printer){await load();renderAttention();$('#attention-message').textContent=message;}nudge();}
}
async function attentionSend(body){const r=await fetch('/api/attention',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const data=await r.json();if(!r.ok||!Array.isArray(data.incidents))throw new Error(data.error||'Could not update alerts');ATTENTION=data;attentionBadge();return data;}
async function attentionAction(action){const ids=attentionPending(P).map(i=>i.id);if(!ids.length)return;try{await attentionSend({action,ids});if(action==='snooze')ids.forEach(id=>ATTENTION_SHOWN.delete(id));closeAttention();}catch(e){$('#attention-message').textContent=e.message;}}
async function attentionReadHistory(){try{await attentionSend({action:'acknowledge',ids:attentionForPrinter(ATTENTION.history).filter(i=>!i.acknowledged).map(i=>i.id)});renderAttention();}catch(e){$('#attention-message').textContent=e.message;}}
async function testMacAttention(){try{$('#attention-message').textContent='Playing the Mac chime and focusing the dashboard in Chrome…';await attentionSend({action:'test'});$('#attention-message').textContent=ATTENTION.native_error||'Test sent to this Mac.';}catch(e){$('#attention-message').textContent=e.message;}}
async function attentionSetting(key,value){try{await attentionSend({action:'settings',[key]:value});renderAttention();}catch(e){$('#attention-message').textContent=e.message;}}
async function enableAttentionNotifications(){try{await Notification.requestPermission();renderAttention();}catch(e){$('#attention-message').textContent='Notification permission is unavailable in this browser.';}}
function attentionViewPrinter(name){if(DLG){if(!confirm('Close your open edit and view this printer?'))return;$('#dlg').returnValue='cancel';$('#dlg').close();}closeAttention();selectPrinter(name,true);document.querySelector('main').scrollIntoView({behavior:'smooth'});}

managementInit();
initDashboardLayout();
window.addEventListener('hashchange',()=>{if(location.hash==='#alert-preview'){if(attentionPending().length)openAttention();else previewAttention();history.replaceState(null,'',location.pathname+location.search);}if(location.hash==='#alerts'){openAttention();history.replaceState(null,'',location.pathname+location.search);}});
setInterval(()=>{const n=attentionPending().length;document.title=n?((Math.floor(Date.now()/1800)%2?'⚠ ':'')+n+' alert'+(n===1?'':'s')+' · Filament management'):'Filament management';},1800);
setInterval(pollAttention,3000);pollAttention();
setInterval(stockPoll,15000);stockPoll();

</script></body></html>"""

if STANDBY:
    HTML = HTML.replace('<body>', '<body><div style="padding:10px;text-align:center;background:#394335;color:#fff">Android preview · Controls are disabled until migration is complete</div>', 1)

MOVEMENT_HTML = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Movement · printing_farmlord</title>
<style>
:root{color-scheme:dark;--bg:#111719;--panel:#1c2528;--line:#354347;--fg:#edf3f2;--mute:#adbec0;--accent:#ace4d0;--bad:#ffaaa6}
*{box-sizing:border-box}body{margin:0;min-height:100vh;background:var(--bg);color:var(--fg);font:14px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
header{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:14px 20px;border-bottom:1px solid var(--line)}h1{font-size:19px;margin:0}header select{max-width:56vw}
main{display:grid;grid-template-columns:minmax(0,1.65fr) minmax(290px,.8fr);gap:18px;max-width:1500px;margin:auto;padding:18px}
section{background:var(--panel);border:1px solid var(--line);border-radius:14px;overflow:hidden}section h2{font-size:15px;font-weight:600;margin:0 0 14px}
.camera{min-width:0}.camera-head{padding:14px 16px;display:flex;align-items:center;justify-content:space-between;gap:10px}.camera-head h2{margin:0}.picture{position:relative;background:#080d0e;aspect-ratio:16/9;display:grid;place-items:center;overflow:hidden}.picture img{width:100%;height:100%;object-fit:contain;display:block}.picture span{position:absolute;inset:0;display:grid;place-items:center;color:var(--mute);pointer-events:none}.picture.ready span{display:none}.camera-note{margin:0;padding:12px 16px;color:var(--mute);font-size:12px}
.controls{padding:18px}.controls h2{margin-bottom:7px}.status{min-height:30px;color:var(--mute);font-size:12px;line-height:1.5}.status.error{color:var(--bad)}.move-pad{display:grid;grid-template-columns:repeat(3,62px);grid-template-rows:repeat(3,56px);gap:7px;justify-content:center;margin:20px 0}.move-pad .axis{display:grid;place-items:center;color:var(--mute);font-weight:600}.z-row{display:flex;justify-content:center;align-items:center;gap:9px;margin-bottom:22px}.z-row span{min-width:36px;text-align:center;color:var(--mute)}
button,select{font:inherit;color:var(--fg);border:1px solid var(--line);border-radius:9px;background:#293539;min-height:44px;padding:9px 13px}button{cursor:pointer}button:hover:not(:disabled){background:#364448}button:disabled{opacity:.42;cursor:not-allowed}.move-pad button{font-size:22px;padding:0}.z-row button{min-width:62px;font-size:20px}.steps{display:flex;justify-content:center;gap:7px;flex-wrap:wrap;margin-bottom:20px}.steps button[aria-pressed=true]{border-color:var(--accent);color:var(--accent)}.home{border-top:1px solid var(--line);padding-top:16px;display:flex;gap:9px;align-items:center;flex-wrap:wrap}.home select{flex:1;min-width:125px}.feedback{min-height:20px;margin-top:13px;font-size:12px;color:var(--accent)}.feedback.error{color:var(--bad)}
@media(max-width:820px){main{grid-template-columns:1fr}.picture{aspect-ratio:16/10}}
@media(max-width:500px){header{flex-wrap:wrap}header select{max-width:100%;width:100%}main{padding:10px;gap:10px}.controls{padding:14px}}
</style></head><body><header><h1>Movement</h1><select id="printer" aria-label="Printer"></select></header>
<main><section class="camera"><div class="camera-head"><h2>Live camera</h2><span id="camera-status" class="status" role="status">Connecting…</span></div><div id="picture" class="picture"><img id="feed" alt="Live printer camera"><span>Connecting camera…</span></div><p class="camera-note">Direct feed · no added playback buffer. Network and printer encoding can still add delay.</p></section>
<section class="controls"><h2>Move axes</h2><div id="machine-status" class="status" role="status">Checking printer…</div>
<div class="move-pad" aria-label="X and Y movement"><i></i><button type="button" data-axis="Y" data-sign="1" aria-label="Move Y positive">↑</button><i></i><button type="button" data-axis="X" data-sign="-1" aria-label="Move X negative">←</button><span class="axis">X Y</span><button type="button" data-axis="X" data-sign="1" aria-label="Move X positive">→</button><i></i><button type="button" data-axis="Y" data-sign="-1" aria-label="Move Y negative">↓</button><i></i></div>
<div class="z-row"><button type="button" data-axis="Z" data-sign="-1" aria-label="Raise bed">↑</button><span>Bed</span><button type="button" data-axis="Z" data-sign="1" aria-label="Lower bed">↓</button></div>
<div class="steps" aria-label="Jog distance"><button type="button" data-step="0.1">0.1 mm</button><button type="button" data-step="1">1 mm</button><button type="button" data-step="10">10 mm</button></div>
<div class="home"><select id="home-axes" aria-label="Axes to home"><option value="all">All axes</option><option value="xy">X and Y</option><option value="z">Z only</option></select><button type="button" id="home">Home axes</button></div>
<div id="feedback" class="feedback" role="status" aria-live="polite"></div></section></main>
<script>
const $=id=>document.getElementById(id),query=new URLSearchParams(location.search),MOVE_UI_VERSION='__AMS_UI_VERSION__';
let printer=query.get('printer')||'',state=null,step=1,sending=false,camRetry=0,camCheck=0;
try{const saved=Number(localStorage.getItem('ams_step'));if([.1,1,10].includes(saved))step=saved}catch(e){}
function endpoint(path){return `/api/p/${encodeURIComponent(printer)}${path}`}
function feedback(message,error=false){$('feedback').textContent=message;$('feedback').classList.toggle('error',error)}
function connected(){return !!state&&!state.stale&&state.mqtt?.mqtt==='connected'}
function blocked(){if(!connected())return 'Printer connection is unavailable.';if(['RUNNING','PREPARE','PAUSE'].includes(state.print_state))return 'Movement is locked while a print is active.';if(state.print_state==='FAILED'&&(state.errors?.print_error||state.errors?.hms?.some(e=>e.blocking)))return 'Clear the failed print on the printer screen first.';return ''}
function render(){const reason=blocked(),status=$('machine-status');status.textContent=reason||(state&&state.unrestricted_jog?'Idle · unrestricted X/Y/Z movement':'Idle · movement available');status.classList.toggle('error',!!reason);for(const b of document.querySelectorAll('[data-axis],#home'))b.disabled=!!reason||sending;for(const b of document.querySelectorAll('[data-step]'))b.setAttribute('aria-pressed',String(Number(b.dataset.step)===step))}
async function poll(){if(!printer)return;const selected=printer,controller=new AbortController(),timer=setTimeout(()=>controller.abort(),8000);try{const r=await fetch(endpoint('/api/state'),{cache:'no-store',signal:controller.signal});const version=r.headers.get('X-AMS-UI-Version');if(version&&version!==MOVE_UI_VERSION){if(!sending)location.reload();return}if(!r.ok)throw Error('Printer status unavailable');const data=await r.json();if(printer!==selected)return;state=data;render()}catch(e){if(printer!==selected)return;state=null;render()}finally{clearTimeout(timer)}}
async function command(body,label){const reason=blocked();if(reason){feedback(reason,true);return}if(sending)return;sending=true;render();feedback(label+' · sending…');const selected=printer;
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),20000);
 try{const r=await fetch(endpoint('/api/control'),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),signal:controller.signal});const data=await r.json();if(printer!==selected)return;if(!r.ok||!/^ok(?:\b|$)/.test(data.result||''))throw Error(data.result||data.error||'Command was not accepted');feedback(label+' · sent to printer')}
 catch(e){if(printer===selected)feedback(label+' · '+(e.name==='AbortError'?'No reply from printer control; check position before retrying':e.message),true)}finally{clearTimeout(timer);sending=false;render();poll()}}
function stopCamera(){clearTimeout(camRetry);clearInterval(camCheck);camRetry=0;camCheck=0;const img=$('feed');img.removeAttribute('src');$('picture').classList.remove('ready')}
function startCamera(){stopCamera();if(!printer||document.hidden)return;const selected=printer,img=$('feed');$('camera-status').textContent='Connecting…';img.onload=()=>{if(printer!==selected)return;$('picture').classList.add('ready');$('camera-status').textContent='Live'};img.onerror=()=>{if(printer!==selected)return;$('picture').classList.remove('ready');$('camera-status').textContent='Reconnecting…';camRetry=setTimeout(startCamera,1800)};img.src=endpoint('/api/cam.mjpg')+'?t='+Date.now();
 camCheck=setInterval(async()=>{if(document.hidden||printer!==selected)return;try{const r=await fetch(endpoint('/api/cam_status'),{cache:'no-store'});const s=await r.json();if(printer!==selected)return;if(s.running&&s.age<5){$('camera-status').textContent='Live';if(img.naturalWidth>0)$('picture').classList.add('ready')}else if(s.running&&s.age>=10){$('camera-status').textContent='Stream stalled · reconnecting';startCamera()}else if(!s.ffmpeg){$('camera-status').textContent='Camera service unavailable'}}catch(e){}},5000)}
function selectPrinter(name){printer=name;state=null;feedback('');render();history.replaceState(null,'','/movement?printer='+encodeURIComponent(name));document.title=name+' · Movement';startCamera();poll()}
async function init(){try{const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),8000);let r;try{r=await fetch('/api/printers',{cache:'no-store',signal:controller.signal})}finally{clearTimeout(timer)}if(!r.ok)throw Error('Cannot load printers');const list=await r.json();const names=list.map(x=>x.name);if(!names.includes(printer))printer=names[0]||'';const select=$('printer');select.replaceChildren();for(const name of names){const option=document.createElement('option');option.value=name;option.textContent=name;select.append(option)}select.value=printer;select.onchange=()=>selectPrinter(select.value);if(printer)selectPrinter(printer);else $('machine-status').textContent='No printers configured'}catch(e){$('machine-status').textContent='Reconnecting to printer controls…';if(printer&&!$('printer').options.length){const option=document.createElement('option');option.value=printer;option.textContent=printer;$('printer').append(option);startCamera();poll()}setTimeout(init,3000)}}
document.querySelectorAll('[data-axis]').forEach(b=>b.onclick=()=>command({action:state&&state.unrestricted_jog?'move_unrestricted':'move',axis:b.dataset.axis,dist:Number(b.dataset.sign)*step},b.dataset.axis==='Z'?(Number(b.dataset.sign)>0?'Lower bed ':'Raise bed ')+step+' mm':`Move ${b.dataset.axis} ${Number(b.dataset.sign)*step} mm`));
document.querySelectorAll('[data-step]').forEach(b=>b.onclick=()=>{step=Number(b.dataset.step);try{localStorage.setItem('ams_step',String(step))}catch(e){}render()});
$('home').onclick=()=>{const axes=$('home-axes').value;if(confirm('Home '+(axes==='all'?'all axes':axes==='xy'?'X and Y':'Z')+'? Make sure the toolhead and bed can move freely.'))command({action:'home',what:axes},'Homing')};
// Keep the MJPEG request alive while switching tabs or macOS windows. The
// camera-health check reconnects only when the stream has actually stalled.
document.addEventListener('visibilitychange',()=>{if(!document.hidden){if(!camCheck&&printer)startCamera();poll()}});window.addEventListener('pagehide',stopCamera);setInterval(()=>{if(!document.hidden)poll()},2000);render();init();
</script></body></html>"""

# Both dashboards reload when either page changes; normal reloads never reuse old HTML.
UI_VERSION = hashlib.sha256((HTML + MOVEMENT_HTML).encode()).hexdigest()[:16]
HTML = HTML.replace("__AMS_UI_VERSION__", UI_VERSION)
MOVEMENT_HTML = MOVEMENT_HTML.replace("__AMS_UI_VERSION__", UI_VERSION)


def serve(open_browser=False, port=None):
    import threading, http.server, webbrowser
    port = port or PORT
    stg = settings()
    if not stg.get("preheat_presets_oct4"):
        stg["preheat"] = dict(stg.get("preheat") or {}, **DEFAULT_PREHEAT)
        stg["preheat_presets_oct4"] = True; save_settings(stg)
    if not stg.get("preheat_pla_oct7"):
        stg.setdefault("preheat", {}).setdefault("PLA", DEFAULT_PREHEAT["PLA"].copy())
        stg["preheat_pla_oct7"] = True; save_settings(stg)
    stg = settings(); stg["auto_cooldown"] = False; stg["rfid_pause_busy"] = False; stg.pop("retry_filament", None); save_settings(stg)   # runtime flags don't survive restarts
    # Keep bounded RFID probe history across restarts so an untagged spool is not
    # repeatedly spun whenever the host or dashboard restarts.
    threading.Thread(target=mqtt_loop, daemon=True).start()
    threading.Thread(target=lambda: (time.sleep(3), errdb()), daemon=True).start()
    def auto_thread():
        time.sleep(30)
        while True:
            try:
                command_file_tick(); act_clear_tick()
                pullback_retry_tick(); autodry_tick(); cool_read_tick(); ams_wedged_tick(); cooldown_tick(); psu_check_tick(); dry_pending_tick()
            except Exception as e: print(f"[{iso()}] autodry error {e}", flush=True)
            time.sleep(20)
    if not STANDBY:
        threading.Thread(target=auto_thread, daemon=True).start()
        def rfid_thread():
            time.sleep(2)
            while True:
                try: rfid_heat_cycle_tick(); rfid_retry_tick()
                except Exception as e: print(f"[{iso()}] RFID error {e}", flush=True)
                time.sleep(2)
        threading.Thread(target=rfid_thread, daemon=True).start()
        def camera_resume_thread():
            while True:
                try: camera_resume_tick()
                except Exception as e: print(f"[{iso()}] camera resume error {e}", flush=True)
                time.sleep(2)
        threading.Thread(target=camera_resume_thread, daemon=True).start()
    def fan_timer_thread():
        while True:
            try: fan_timer_tick()
            except Exception as e: print(f"[{iso()}] fan timer error {e}", flush=True)
            time.sleep(1)
    threading.Thread(target=fan_timer_thread, daemon=True).start()

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a): pass
        def _send(self, code, body, ctype="application/json"):
            b = body.encode() if isinstance(body, str) else body
            self.send_response(code); self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(b))); self.send_header("Cache-Control", "no-store"); self.send_header("X-AMS-UI-Version", UI_VERSION); self.end_headers(); self.wfile.write(b)
        def do_GET(self):
            if not host_ok(self.headers.get("Host")): return self._send(421, json.dumps({"error": "Unknown host."}))
            if self.path.startswith("/api/cam.jpg"): cam_snapshot(self)
            elif self.path.startswith("/api/cam.mjpg"): cam_stream(self)
            elif self.path.startswith("/api/cam_status"): self._send(200, json.dumps(
                {"ffmpeg": bool(ffmpeg_bin()), "frames": CAM["frames"], "age": round(now() - (CAM["last"] or 0), 1),
                 "viewers": len(CAM["clients"]), "running": bool(CAM["proc"] and CAM["proc"].poll() is None)}))
            elif self.path.startswith("/api/rev"): self._send(200, json.dumps(rev_data()))
            elif self.path.startswith("/api/state"): self._send(200, json.dumps(report_data()))
            elif self.path == "/": self._send(200, HTML, "text/html; charset=utf-8")
            else: self._send(404, "{}")
        def do_POST(self):
            if STANDBY:
                return self._send(503, json.dumps({"error": "Android host is in standby; use the current dashboard for controls."}))
            if not host_ok(self.headers.get("Host")) or not json_post_ok(self.headers):
                return self._send(403, json.dumps({"error": "Use the dashboard to make changes."}))
            raw = read_body(self.headers, self.rfile)
            if raw is None: return self._send(413, json.dumps({"error": "Request too large."}))
            try: body = json.loads(raw or b"{}")
            except ValueError: return self._send(400, json.dumps({"error": "Invalid JSON."}))
            if not isinstance(body, dict): return self._send(400, json.dumps({"error": "Invalid request."}))
            with LOCK: self._post(body)
        def _post(self, body):
            if self.path == "/api/name": r = do_name(body["id"], body["name"], body.get("material"))
            elif self.path == "/api/same": r = do_same(body["new_id"], body["old_id"])
            elif self.path == "/api/dried": r = do_dried(body["id"], float(body["hours"]), float(body["temp"]))
            elif self.path == "/api/set_spool": r = do_set_spool(body["slot"], body["type"], body.get("name", ""), float(body.get("dried_h") or 0), body.get("dried_temp"), body.get("color", ""))
            elif self.path == "/api/remove_spool": r = do_remove_spool(body["id"])
            elif self.path == "/api/print": r = do_print_control(body.get("action"))
            elif self.path == "/api/control": r = do_control(body)
            elif self.path == "/api/retry_pullback": r = retry_pullback()
            elif self.path == "/api/resolve": r = do_resolve(body.get("action"))
            elif self.path == "/api/read_rfid": r = do_read_rfid(body["ams_id"], body["slot_id"], pause_drying=True)
            elif self.path == "/api/read_options": r = do_ams_read_options(body.get("on", True))
            elif self.path == "/api/cancel_read":
                stg = settings(); (stg.get("cool_read") or {}).pop(body.get("slot"), None); save_settings(stg); r = "ok"
            elif self.path == "/api/claim": r = do_claim(body["id"], body["from_printer"], body["from_id"])
            elif self.path == "/api/filament": r = do_filament(body.get("action"), body.get("target", 255))
            elif self.path == "/api/ams_unload": r = do_ams_unload(body.get("temp", 250))
            elif self.path == "/api/temp": r = do_temp(body.get("kind"), body.get("value"))
            elif self.path == "/api/cooldown_heaters": r = do_cooldown_heaters()
            elif self.path == "/api/preheat": r = do_preheat(body.get("bed", 0), body.get("chamber", 0), body.get("label", ""), body.get("nozzle"))
            elif self.path == "/api/auto_cooldown":
                r = "automatic cool-down has been removed; use manual fan controls" if body.get("on") else "ok"
            elif self.path == "/api/preheat_presets":
                st = settings(); st["preheat"] = body.get("presets") or {}; save_settings(st); r = "ok"
            elif self.path == "/api/move_spool": r = do_move_spool(body["id"], body["slot"])
            elif self.path == "/api/dry_all":
                units = request_fleet_drying()
                return self._send(200,json.dumps({"result":"ok","units":units}))
            elif self.path == "/api/stop_all":
                st_ = jload(STATE, {}); done = []
                for aid, a_ in st_.get("ams", {}).items():
                    if not a_.get("heating") and dry_requests().get(aid, {}).get("status") not in ("pending", "waiting"):
                        cancel_dry_request(aid, manual_stop=True); continue
                    res = set_drying(aid, False, manual=True)
                    done.append(f"{ams_label(aid)}: stopped" if res.startswith("ok") else f"{ams_label(aid)}: {res}")
                r = "; ".join(done) or "nothing was drying"
            elif self.path == "/api/cancel_pending":
                r = set_drying(body.get("ams_id"), False, manual=True)
            elif self.path == "/api/probe": r = dry_probe(body.get("ams_id", 0), body.get("temp", 55))
            elif self.path == "/api/dry": r = set_drying(body["ams_id"], bool(body["on"]), body.get("temp", 65), body.get("hours", 8), body.get("rotate", False), manual=True)
            elif self.path == "/api/psu":
                st = settings(); st.setdefault("psu", {})[str(body["ams_id"])] = bool(body["on"]); st.get("psu_warn", {}).pop(str(body["ams_id"]), None); st["psu_obs"] = {}; save_settings(st)
                append(EVENTS, dict(t=now(), time=iso(), kind="AUTO_DRY", slot=f"{body['ams_id']}:*", msg=f"{ams_label(body['ams_id'])}: external PSU {'YES' if body['on'] else 'NO'}")); r = "ok"
            elif self.path == "/api/auto":
                st = settings(); st["auto"][str(body["ams_id"])] = bool(body["on"]); save_settings(st)
                append(EVENTS, dict(t=now(), time=iso(), kind="AUTO_DRY", slot=f"{body['ams_id']}:*", msg=f"{ams_label(body['ams_id'])}: auto-dry {'ON' if body['on'] else 'OFF'}")); r = "ok"
            else: r = "unknown"
            self._send(200, json.dumps({"result": r}))

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), H)
    signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))
    print(f"[{iso()}] worker {os.path.basename(BASE)} on http://localhost:{port}", flush=True)
    if open_browser: webbrowser.open(f"http://localhost:{port}")
    srv.serve_forever()


ORCA_USER_GLOB = os.path.expanduser("~/Documents/OrcaSlicer/user/*/filament/base")
SYNC_LOG = os.path.join(BASE, "filament_sync.log")
SYNC_SKIP = {"name", "filament_settings_id", "compatible_printers", "compatible_printers_condition",
             "filament_max_volumetric_speed", "nozzle_temperature", "nozzle_temperature_initial_layer"}


def _orca_running():
    import subprocess
    try: return bool(subprocess.run(["pgrep", "-x", "OrcaSlicer"], capture_output=True).stdout.strip())
    except Exception: return False


def orca_filament_sync(force=False):
    """Orca owns profile identities, cloud revisions, and per-printer calibration."""
    return "nothing to do"


SETUP_LOGO = '<svg class="madking" viewBox="0 0 110 200" width="38" height="68" role="img" aria-label="printing_farmlord logo" xmlns="http://www.w3.org/2000/svg"><image href="data:image/png;base64,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" x="0" y="0" width="110" height="200"/></svg>'
SETUP_HTML = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="color-scheme" content="dark"><title>Set up · printing_farmlord</title>
<style>*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;padding:24px 16px;background:#0e1618;color:#edf3f2;font:16px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{width:100%;max-width:560px;padding:28px;background:#1b2528;border:1px solid #344448;border-radius:16px}
h1{display:flex;align-items:center;gap:10px;font-size:25px;margin:0 0 6px}h2{font-size:18px;margin:26px 0 6px}
p,.hint{color:#adbec0;font-size:14px;line-height:1.5;margin:0 0 14px}.step{color:#8fd3b5;font-size:13px;text-transform:uppercase;letter-spacing:.08em;margin-top:18px}
label{display:block;font-size:14px;margin:12px 0 6px}input{display:block;width:100%;font:inherit;padding:11px 12px;color:#edf3f2;background:#0e1618;border:1px solid #53666b;border-radius:10px}
input:focus{outline:2px solid #8fd3b5;outline-offset:1px}.row{display:grid;grid-template-columns:1fr 1fr;gap:0 12px}@media(max-width:480px){.row{grid-template-columns:1fr}}
button{font:inherit;font-weight:600;padding:11px 16px;border-radius:10px;border:1px solid #53666b;background:#24343a;color:#edf3f2;cursor:pointer}button.primary{background:#bfe8d4;color:#0e1618;border-color:#bfe8d4}
button:disabled{opacity:.5;cursor:default}.actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:16px}
ul{list-style:none;padding:0;margin:10px 0}li{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:10px 12px;border:1px solid #344448;border-radius:10px;margin-bottom:8px;background:#162024}
li small{color:#adbec0}li button{padding:6px 10px;font-weight:400}.ok{color:#8fd3b5}.bad{color:#ff9b8f}.msg{min-height:22px;font-size:14px;margin-top:10px}
.check{display:flex;gap:10px;align-items:flex-start;margin:12px 0}.check input{width:auto;margin-top:3px}</style></head>
<body><main>
<h1>__LOGO__<span>printing_farmlord</span></h1>
<p>Let's connect your printers. This takes about two minutes, and everything you enter stays on this computer.</p>

<div class="step">Step 1 of 2</div><h2>Add your printers</h2>
<p class="hint">On each printer's screen: <b>Settings → Network</b>, turn on <b>LAN Only</b> and <b>Developer Mode</b>. The IP address, serial number and access code are shown there.</p>
<form id="pf" autocomplete="off">
 <label for="name">Printer name</label><input id="name" required maxlength="40" placeholder="e.g. H2D-1">
 <div class="row"><div><label for="ip">IP address</label><input id="ip" required inputmode="decimal" placeholder="e.g. 192.0.2.10"></div>
 <div><label for="serial">Serial number</label><input id="serial" required maxlength="32"></div></div>
 <label for="code">Access code</label><input id="code" required maxlength="32" autocomplete="off">
 <div class="actions"><button type="button" id="test">Test connection</button><button type="submit">Add printer</button></div>
 <div class="msg" id="pmsg" role="status"></div>
</form>
<ul id="list"></ul>

<div class="step">Step 2 of 2</div><h2>Who can open the dashboard?</h2>
<div class="check"><input type="checkbox" id="lan"><label for="lan" style="margin:0">Let other devices on my network open it (phone, laptop, Tailscale). Requires a password.</label></div>
<div id="pw" hidden><label for="pw1">Dashboard password (at least 8 characters)</label><input id="pw1" type="password" autocomplete="new-password">
<label for="pw2">Repeat password</label><input id="pw2" type="password" autocomplete="new-password"></div>
<div class="actions"><button class="primary" id="save" disabled>Finish and open the dashboard</button></div>
<div class="msg" id="smsg" role="status"></div>
<p class="hint" style="margin-top:22px">Stuck? <a id="report" href="__REPORT__" target="_blank" rel="noopener" style="color:#8fd3b5">Report a problem</a>. It opens a GitHub issue for you to check and send; IP addresses, serial numbers and access codes are never included.</p>
</main>
<script>
const $=id=>document.getElementById(id),printers=[];
function post(url,body){return fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}).then(r=>r.json())}
function current(){return{name:$('name').value.trim(),ip:$('ip').value.trim(),serial:$('serial').value.trim(),access_code:$('code').value.trim()}}
function say(id,text,cls,report){const m=$(id);m.textContent=text;m.className='msg '+(cls||'');if(report){const a=document.createElement('a');a.href=report;a.target='_blank';a.rel='noopener';a.style.color='#8fd3b5';a.textContent=' Report this problem';m.append(a)}}
function render(){const ul=$('list');ul.replaceChildren();printers.forEach((p,i)=>{const li=document.createElement('li'),t=document.createElement('div'),b=document.createElement('strong'),s=document.createElement('small'),x=document.createElement('button');b.textContent=p.name;s.textContent=' '+p.ip;t.append(b,s);x.type='button';x.textContent='Remove';x.onclick=()=>{printers.splice(i,1);render()};li.append(t,x);ul.append(li)});$('save').disabled=!printers.length}
$('test').onclick=async()=>{const p=current();if(!p.ip)return say('pmsg','Enter the IP address first.','bad');say('pmsg','Checking '+p.ip+'…');const r=await post('/api/setup/test',{ip:p.ip});say('pmsg',r.message,r.reachable?'ok':'bad')};
$('pf').onsubmit=async e=>{e.preventDefault();const p=current();const r=await post('/api/setup/check',{printer:p,existing:printers.map(x=>x.name)});if(!r.ok)return say('pmsg',r.message,'bad');printers.push(p);render();$('pf').reset();say('pmsg','Added '+p.name+'. Add another, or go to step 2.','ok');$('name').focus()};
$('lan').onchange=()=>{$('pw').hidden=!$('lan').checked};
$('save').onclick=async()=>{const lan=$('lan').checked,pw=$('pw1').value;if(lan&&pw.length<8)return say('smsg','Choose a password of at least 8 characters.','bad');if(lan&&pw!==$('pw2').value)return say('smsg','The two passwords are different.','bad');
 $('save').disabled=true;say('smsg','Saving…');const r=await post('/api/setup/save',{printers,lan_access:lan,password:lan?pw:''});if(!r.ok){$('save').disabled=false;return say('smsg',r.message,'bad',r.report)}
 say('smsg','Saved. Starting your dashboard…','ok');const wait=async()=>{try{const s=await fetch('/api/setup/state',{cache:'no-store'});if(s.status===200&&(await s.json()).setup)throw 0;location.href='/'}catch(e){setTimeout(wait,1500)}};setTimeout(wait,2500)};
</script></body></html>"""


def setup_wizard():
    """First run: no printers configured yet. Serve a localhost-only setup page, write config.json, then restart into the dashboard."""
    import html, http.server, socket, threading, webbrowser
    page = SETUP_HTML.replace("__LOGO__", SETUP_LOGO).replace("__REPORT__", html.escape(report_url("Setup problem"), quote=True)).encode()
    bind = "0.0.0.0" if os.environ.get("AMS_SETUP_LAN") == "1" else "127.0.0.1"

    def check_printer(p, existing):
        p = {k: str((p or {}).get(k, "")).strip() for k in ("name", "ip", "serial", "access_code")}
        if not all(p.values()): return None, "Fill in all four fields."
        if not printer_name_ok(BASE, p["name"]) or len(p["name"]) > 40: return None, "Use a short name without slashes, and don't start it with a dot."
        if p["name"] in existing: return None, "You already added a printer with that name."
        try: ipaddress.ip_address(p["ip"])
        except ValueError: return None, "That IP address doesn't look right. It's four numbers with dots, e.g. 192.0.2.10."
        if len(p["serial"]) > 32 or not p["serial"].isalnum(): return None, "The serial number should be letters and digits only."
        if len(p["access_code"]) > 32: return None, "That access code is too long."
        return p, ""

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a): pass
        def _send(self, code, body, ctype="application/json"):
            b = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(code); self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(b)))
            self.send_header("Cache-Control", "no-store"); self.end_headers(); self.wfile.write(b)
        def do_GET(self):
            if not host_ok(self.headers.get("Host")): return self._send(421, {"error": "Unknown host."})
            p = self.path.split("?", 1)[0]
            if p == "/setup": return self._send(200, page, "text/html; charset=utf-8")
            if p == "/api/setup/state": return self._send(200, {"setup": True})
            self.send_response(303); self.send_header("Location", "/setup"); self.send_header("Content-Length", "0"); self.end_headers()
        def do_POST(self):
            try: return self._post()
            except Exception:
                import traceback
                err = traceback.format_exc(); print(err, flush=True)
                return self._send(500, {"ok": False, "message": "Something went wrong on this computer.", "report": report_url("Setup error", err)})
        def _post(self):
            if not host_ok(self.headers.get("Host")) or not json_post_ok(self.headers): return self._send(403, {"ok": False, "message": "Use the setup page."})
            raw = read_body(self.headers, self.rfile, 64 * 1024)
            try: body = json.loads(raw or b"{}")
            except (TypeError, ValueError): body = None
            if not isinstance(body, dict): return self._send(400, {"ok": False, "message": "Invalid request."})
            if self.path == "/api/setup/test":
                ip = str(body.get("ip", "")).strip()
                try: ipaddress.ip_address(ip)
                except ValueError: return self._send(200, {"reachable": False, "message": "That IP address doesn't look right."})
                try:
                    socket.create_connection((ip, 8883), timeout=3).close()
                    return self._send(200, {"reachable": True, "message": "Found a printer at " + ip + "."})
                except OSError:
                    return self._send(200, {"reachable": False, "message": "No answer from " + ip + ". Check the IP, that LAN Only mode is on, and that this computer is on the printer's network."})
            if self.path == "/api/setup/check":
                p, err = check_printer(body.get("printer"), [str(x) for x in body.get("existing") or []])
                return self._send(200, {"ok": bool(p), "message": err})
            if self.path == "/api/setup/save":
                printers, names = [], []
                for raw_p in (body.get("printers") or [])[:50]:
                    p, err = check_printer(raw_p, names)
                    if not p: return self._send(200, {"ok": False, "message": err})
                    printers.append(p); names.append(p["name"])
                if not printers: return self._send(200, {"ok": False, "message": "Add at least one printer."})
                lan = bool(body.get("lan_access")); password = str(body.get("password") or "")
                if lan and len(password) < 8: return self._send(200, {"ok": False, "message": "Choose a password of at least 8 characters."})
                cfg = jload(CONFIG, {})
                if cfg.get("printers"): return self._send(200, {"ok": False, "message": "This dashboard is already set up."})
                cfg.update(printers=printers, lan_access=lan)
                cfg.setdefault("lan_name", "printroom")
                if lan: cfg["auth"] = {"password": password}
                os.makedirs(os.path.dirname(CONFIG) or ".", exist_ok=True)
                jsave(CONFIG, cfg)
                print(f"[{iso()}] setup saved {len(printers)} printer(s); starting the dashboard", flush=True)
                self._send(200, {"ok": True})
                # The listening socket is not inherited across exec, so the dashboard can bind the same port.
                threading.Timer(1.0, lambda: os.execv(sys.executable, [sys.executable] + sys.argv)).start()
                return
            self._send(404, {"ok": False, "message": "Not found."})

    srv = http.server.ThreadingHTTPServer((bind, PORT), H)
    url = f"http://localhost:{PORT}/setup"
    print(f"[{iso()}] first run: open {url} to set up your printers", flush=True)
    try: webbrowser.open(url)
    except Exception: pass
    srv.serve_forever()


def hub(open_browser=False):
    """Multi-printer mode: one worker process per printer (config.json → "printers"), tabbed dashboard on PORT."""
    import threading, http.server, subprocess, webbrowser, urllib.request
    cfg = jload(CONFIG, {})
    EXTRA_HOSTS.update(str(h).strip().lower() for h in cfg.get("allowed_hosts", []) if str(h).strip())
    printers = cfg.get("printers") or ([dict(name=cfg.get("name", "Printer"), ip=cfg["ip"], serial=cfg["serial"], access_code=cfg["access_code"])] if cfg.get("ip") else [])
    if not printers: return setup_wizard()
    workers = {}
    def add_worker(p):
        d = os.path.join(BASE, "printers", p["name"]); os.makedirs(d, exist_ok=True)
        jsave(os.path.join(d, "config.json"), dict(ip=p["ip"], serial=p["serial"], access_code=p["access_code"], name=p["name"], allow_unrestricted_jog=bool(p.get("allow_unrestricted_jog"))))
        if not os.path.exists(os.path.join(d, "settings.json")): jsave(os.path.join(d, "settings.json"), dict(auto={}, manual_hold_until={}, auto_started={}))
        used = {w["port"] for w in workers.values()}; port = PORT + 1
        while port in used: port += 1
        workers[p["name"]] = dict(port=port, dir=d, proc=None)
    for p in printers: add_worker(p)

    me = os.path.abspath(__file__); my_mtime = os.path.getmtime(me)
    def supervise():
        nonlocal my_mtime
        while True:
            # self-update: when ams_tracker.py changes on disk, restart everything (launchd relaunches the hub)
            try:
                if os.path.getmtime(me) != my_mtime:
                    time.sleep(3)  # let the write finish
                    try: compile(open(me).read(), me, "exec")
                    except SyntaxError as e:
                        print(f"[{iso()}] new code has a syntax error, not restarting: {e}", flush=True)
                        my_mtime = os.path.getmtime(me)   # noqa — wait for the next edit
                        continue
                    print(f"[{iso()}] code updated — restarting", flush=True)
                    for w in workers.values():
                        if w["proc"]: w["proc"].terminate()
                    os._exit(0)
            except FileNotFoundError: pass
            for name, w in workers.items():
                if w["proc"] is None or w["proc"].poll() is not None:
                    env = dict(os.environ, AMS_TRACKER_DIR=w["dir"])
                    log = open(os.path.join(w["dir"], "worker.log"), "a")
                    w["proc"] = subprocess.Popen([sys.executable, os.path.abspath(__file__), "serve", "--port", str(w["port"])], env=env, stdout=log, stderr=log)
                    print(f"[{iso()}] started worker {name} on port {w['port']}", flush=True)
            time.sleep(5)
    threading.Thread(target=supervise, daemon=True).start()

    def filament_sync_loop():
        while True:
            try:
                r = orca_filament_sync()
                if r not in ("nothing to do",) and not r.startswith("Orca is running"):
                    print(f"[{iso()}] filament sync: {r}", flush=True)
            except Exception as e:
                print(f"[{iso()}] filament sync error: {e}", flush=True)
            time.sleep(900)
    threading.Thread(target=filament_sync_loop, daemon=True).start()

    spider_catalog = StoreCatalog()
    stock_inventory = FilamentInventory(BASE)
    stock_inventory.catalog = spider_catalog
    threading.Thread(target=spider_catalog.run, daemon=True).start()
    attention = AttentionCenter(BASE, PORT, stock_inventory)
    threading.Thread(target=attention.run, args=(workers,), daemon=True).start()

    AUTH = (jload(CONFIG, {}) or {}).get("auth") or {}
    import hmac, secrets, html
    from http.cookies import SimpleCookie, CookieError
    session_path = os.path.join(BASE, "web-session.key")
    try:
        with open(session_path, "rb") as f: session_key = f.read()
    except FileNotFoundError:
        session_key = secrets.token_bytes(32)
        fd = os.open(session_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as f: f.write(session_key)
    SESSION_AGE = 30 * 24 * 60 * 60
    def sign_session(payload):
        # Password changes invalidate existing cookies; the password never goes in a cookie.
        message = (payload + "\0" + str(AUTH.get("password") or "")).encode()
        return hmac.new(session_key, message, hashlib.sha256).hexdigest()

    # H2 printers expose their USB drive over FTPS. This clears files to free space;
    # it deliberately does not claim to recreate the drive's filesystem.
    storage_lock = threading.Lock()
    storage_job_lock = threading.Lock()
    storage_preview = {}
    storage_job = {"done": True, "message": "No storage clear is running.", "results": []}

    def storage_idle(name, worker):
        with urllib.request.urlopen(f"http://127.0.0.1:{worker['port']}/api/state", timeout=5) as response:
            state = json.load(response)
        if state.get("stale") or (state.get("mqtt") or {}).get("mqtt") != "connected":
            raise ValueError(f"{name} is not connected with fresh readings")
        if state.get("print_state") not in ("IDLE", "FINISH", "FAILED"):
            raise ValueError(f"{name} is busy ({state.get('print_state') or 'unknown state'})")

    def storage_curl(printer, path="/", commands=()):
        import urllib.parse
        url = f"ftps://{printer['ip']}:990{urllib.parse.quote(path, safe='/')}"
        config = "user = " + json.dumps("bblp:" + printer["access_code"]) + "\n"
        config += "".join("quote = " + json.dumps(command) + "\n" for command in commands)
        args = ["curl", "-k", "-sS", "--max-time", "90", "-K", "-"]
        if not commands: args += ["--request", "LIST -a"]
        else: args.append("--list-only")
        result = subprocess.run(args + [url], input=config, text=True, capture_output=True, timeout=100)
        if result.returncode:
            raise ValueError(f"USB file service on {printer['name']} failed (code {result.returncode})")
        return result.stdout

    def storage_scan(printer):
        queue = ["/"]; files = []; directories = 0
        while queue:
            if directories > 500 or len(files) > 20000:
                raise ValueError(f"Too many entries on {printer['name']} USB drive; stopped safely")
            path = queue.pop(); directories += 1
            for line in storage_curl(printer, path).splitlines():
                parts = line.split(maxsplit=8)
                if len(parts) != 9: raise ValueError(f"Could not read all files on {printer['name']}")
                name = parts[8]
                if name in (".", ".."): continue
                if not name or "/" in name or any(ord(c) < 32 for c in name):
                    raise ValueError(f"Unsupported filename on {printer['name']}; stopped safely")
                full = path.rstrip("/") + "/" + name
                if parts[0].startswith("d"):
                    queue.append(full + "/")
                elif parts[0].startswith("-"):
                    files.append((full, int(parts[4])))
                else:
                    raise ValueError(f"Unsupported file type on {printer['name']}; stopped safely")
        return sorted(files)

    def storage_snapshot():
        summary = []; inventory = {}
        for name, worker in list(workers.items()):
            storage_idle(name, worker)
            printer = jload(os.path.join(worker["dir"], "config.json"), {})
            if not all(printer.get(k) for k in ("name", "ip", "access_code")):
                raise ValueError(f"Missing connection details for {name}")
            files = storage_scan(printer)
            inventory[name] = files
            summary.append({"name": name, "files": len(files), "bytes": sum(size for _, size in files),
                            "firmware": [path.rsplit("/", 1)[-1] for path, _ in files
                                         if "firmware" in path.lower() or "offline-ota" in path.lower()]})
        digest = hashlib.sha256(json.dumps(inventory, sort_keys=True).encode()).hexdigest()
        return summary, inventory, digest

    def storage_job_update(**changes):
        with storage_job_lock: storage_job.update(changes)

    def storage_clear_run(expected_digest):
        try:
            summary, inventory, digest = storage_snapshot()
            if digest != expected_digest:
                raise ValueError("Drive contents changed since confirmation. Preview again; no files were deleted.")
            results = []
            for name, worker in list(workers.items()):
                printer = jload(os.path.join(worker["dir"], "config.json"), {})
                files = inventory[name]
                storage_job_update(message=f"Clearing {name}: 0 of {len(files)} files…", results=results[:])
                error = ""
                try:
                    for offset in range(0, len(files), 20):
                        storage_idle(name, worker)
                        commands = ["*DELE " + path for path, _ in files[offset:offset + 20]]
                        storage_curl(printer, "/", commands)
                        storage_job_update(message=f"Clearing {name}: {min(offset + 20, len(files))} of {len(files)} files…")
                except Exception as exc:
                    error = str(exc)
                try: remaining = len(storage_scan(printer))
                except Exception as exc:
                    remaining = -1
                    error = (error + "; " if error else "") + "Could not verify remaining files: " + str(exc)
                results.append({"name": name, "remaining": remaining, "error": error})
                storage_job_update(results=results[:], message=f"Checked {name}; {remaining if remaining >= 0 else 'unknown'} files remain.")
                if error: break
            storage_job_update(done=True, results=results,
                               error="Some files may remain. Review each printer before retrying." if any(x["error"] or x["remaining"] != 0 for x in results) or len(results) != len(workers) else "",
                               message="USB file clearing finished." if len(results) == len(workers) and all(x["remaining"] == 0 for x in results) else "USB file clearing stopped; see results.")
        except Exception as exc:
            storage_job_update(done=True, error=str(exc), message="No files were deleted." if "no files were deleted" in str(exc).lower() else "USB file clearing stopped; see results.")
        finally:
            storage_lock.release()


    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a): pass

        def _login_page(self, error=""):
            username = html.escape(str(AUTH.get("user") or "Print room"), quote=True)
            error_html = '<p class="error" role="alert">' + html.escape(error) + '</p>' if error else ''
            page = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="color-scheme" content="dark"><title>Sign in · printing_farmlord</title>
<style>*{box-sizing:border-box}body{margin:0;min-height:100vh;min-height:100dvh;display:grid;place-items:center;padding:24px;background:#0e1618;color:#edf3f2;font:16px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{width:100%;max-width:380px;padding:28px;background:#1b2528;border:1px solid #344448;border-radius:16px}h1{font-size:25px;margin:0 0 8px}p{color:#adbec0;font-size:14px;line-height:1.5;margin:0 0 24px}label{display:block;font-size:14px;margin-bottom:8px}input{display:block;width:100%;font:inherit;padding:13px 14px;color:#edf3f2;background:#0e1618;border:1px solid #53666b;border-radius:9px}input:focus{outline:2px solid #ace4d0;outline-offset:2px}button{width:100%;min-height:48px;margin-top:18px;border:0;border-radius:9px;background:#ace4d0;color:#102721;font:600 16px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;cursor:pointer}button:focus-visible{outline:2px solid #edf3f2;outline-offset:3px}.error{color:#ffaaa6;margin:16px 0 0}</style></head>
<body><main><h1 style="display:flex;align-items:center;gap:10px"><svg class="madking" viewBox="0 0 110 200" width="38" height="68" role="img" aria-label="printing_farmlord logo" xmlns="http://www.w3.org/2000/svg"><image href="data:image/png;base64,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" x="0" y="0" width="110" height="200"/></svg><span>printing_farmlord</span></h1><p>Sign in to your printers.</p>
<form method="post" action="/login" autocomplete="on">
<input type="hidden" name="username" autocomplete="username" value="__USERNAME__">
<label for="password">Password</label><input id="password" name="password" type="password" autocomplete="current-password" required autofocus>
__ERROR__<button type="submit">Sign in</button></form></main></body></html>"""
            self._send(200, page.replace("__USERNAME__", username).replace("__ERROR__", error_html), "text/html; charset=utf-8")

        def _authed(self):
            """Private controller calls use loopback; browsers use a signed persistent session."""
            if not AUTH.get("password"): return True
            if self.client_address[0] in ("127.0.0.1", "::1"): return True
            try:
                cookies = SimpleCookie(self.headers.get("Cookie", ""))
                token = cookies["ams_session"].value
                expiry, nonce, signature = token.split(".")
                payload = expiry + "." + nonce
                if int(expiry) > time.time() and hmac.compare_digest(signature, sign_session(payload)):
                    return True
            except (KeyError, ValueError, TypeError, CookieError): pass
            if self.path.startswith("/api/") or self.command != "GET":
                self._send(401, json.dumps({"error": "Please sign in.", "login": "/login"}))
            else:
                self.send_response(303)
                self.send_header("Location", "/login")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", "0"); self.end_headers()
            return False

        def _send(self, code, body, ctype="application/json"):
            b = body.encode() if isinstance(body, str) else body
            self.send_response(code); self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(b))); self.send_header("Cache-Control", "no-store"); self.send_header("X-AMS-UI-Version", UI_VERSION); self.end_headers(); self.wfile.write(b)
            t0 = getattr(self, "_t0", None)
            if t0 and time.time() - t0 > 2:
                print(f"[{iso()}] SLOW {time.time()-t0:.1f}s {self.command} {self.path}", flush=True)
        def _proxy(self, data=None):
            import urllib.parse
            parts = self.path.split("/", 4)      # '', 'api', 'p', name, rest
            name = urllib.parse.unquote(parts[3]); rest = "/" + (parts[4] if len(parts) > 4 else "")
            w = workers.get(name)
            if not w: return self._send(404, json.dumps({"error": "unknown printer"}))
            try:
                req = urllib.request.Request(f"http://127.0.0.1:{w['port']}{rest}", data=data, headers={"Content-Type": "application/json"}, method="POST" if data else "GET")
                with urllib.request.urlopen(req, timeout=10) as r: self._send(200, r.read())
            except Exception as e: self._send(503, json.dumps({"error": "Printer service is reconnecting; retry shortly."}))
        def do_GET(self):
            self._t0 = time.time()
            if not host_ok(self.headers.get("Host")): return self._send(421, json.dumps({"error": "Unknown host."}))
            if self.path.split("?", 1)[0] == "/login": return self._login_page()
            if not self._authed(): return
            if self.path.startswith("/api/p/") and "/api/cam.jpg" in self.path:
                import urllib.request, urllib.parse
                parts = self.path.split("/", 4); name = urllib.parse.unquote(parts[3]); w = workers.get(name)
                if not w: return self._send(404, b"")
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{w['port']}/api/cam.jpg", timeout=2) as r:
                        self._send(r.status, r.read(), "image/jpeg")
                except Exception: self._send(503, b"", "image/jpeg")
                return
            if "/api/cam.mjpg" in self.path:
                import urllib.request, urllib.parse
                parts = self.path.split("/", 4); name = urllib.parse.unquote(parts[3]); w = workers.get(name)
                if not w: return self._send(404, "{}")
                try:
                    up = urllib.request.urlopen(f"http://127.0.0.1:{w['port']}/api/cam.mjpg", timeout=20)
                    self.send_response(200)
                    self.send_header("Content-Type", up.headers.get("Content-Type", "multipart/x-mixed-replace; boundary=ffmpeg"))
                    self.send_header("Cache-Control", "no-store"); self.end_headers()
                    try:
                        while True:
                            b = up.read1(16384)
                            if not b: break
                            self.wfile.write(b)
                    finally:
                        up.close()
                except Exception: pass
                return
            if self.path == "/": self._send(200, HTML.replace("configured store", store_name() or "configured store"), "text/html; charset=utf-8")
            elif self.path.split("?", 1)[0] == "/movement": self._send(200, MOVEMENT_HTML, "text/html; charset=utf-8")
            elif self.path == "/api/attention": self._send(200, json.dumps(attention.snapshot()))
            elif self.path == "/api/filament_stock": self._send(200, json.dumps(stock_inventory.snapshot()))
            elif self.path == "/api/storage_clear_preview":
                if not storage_lock.acquire(blocking=False):
                    return self._send(409, json.dumps({"error": "A storage check or clear is already running."}))
                try:
                    summary, inventory, digest = storage_snapshot()
                    token = secrets.token_urlsafe(32)
                    storage_preview.clear()
                    storage_preview.update(token=token, digest=digest, expires=time.time() + 300)
                    self._send(200, json.dumps({"printers": summary, "token": token}))
                except Exception as exc:
                    self._send(409, json.dumps({"error": str(exc)}))
                finally: storage_lock.release()
            elif self.path == "/api/storage_clear_status":
                with storage_job_lock: current = dict(storage_job)
                self._send(200, json.dumps(current))
            elif self.path == "/api/network":
                import socket
                ip = None
                try:
                    sck = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); sck.connect(("192.0.2.1", 1)); ip = sck.getsockname()[0]; sck.close()
                except Exception: pass
                nets = jload(CONFIG, {}).get("networks", {})
                label = next((v for k, v in nets.items() if ip and ip.startswith(k)), "Unknown network")
                reach = {n: any(p["ip"].rsplit(".", 1)[0] == (ip or "").rsplit(".", 1)[0] or p["ip"].split(".")[:2] == (ip or "").split(".")[:2] for _ in [0]) for n, w in workers.items() for p in [jload(os.path.join(w["dir"], "config.json"), {})]}
                self._send(200, json.dumps({"ip": ip, "label": label, "printers_here": reach}))
            elif self.path == "/api/printers": self._send(200, json.dumps([dict(name=n, port=w["port"], config=public_printer_config(jload(os.path.join(w["dir"], "config.json"), {}))) for n, w in workers.items()]))
            elif self.path.startswith("/api/p/"): self._proxy()
            else: self._send(404, "{}")
        def do_POST(self):
            if STANDBY:
                return self._send(503, json.dumps({"error": "Android host is in standby; use the current dashboard for controls."}))
            self._t0 = time.time()
            if not host_ok(self.headers.get("Host")): return self._send(421, json.dumps({"error": "Unknown host."}))
            if self.path == "/login":
                import urllib.parse
                origin = self.headers.get("Origin")
                if origin and urllib.parse.urlsplit(origin).netloc != self.headers.get("Host"):
                    return self._send(403, "Sign in from the dashboard address.", "text/plain")
                try: n = int(self.headers.get("Content-Length", "0"))
                except ValueError: return self._login_page("Please enter your password again.")
                if n < 0 or n > 4096: return self._send(413, "Request too large", "text/plain")
                form = urllib.parse.parse_qs(self.rfile.read(n).decode("utf-8", errors="replace"))
                ip = self.client_address[0]; fails = LOGIN_FAILS.get(ip)
                if fails and fails[1] > time.time(): return self._login_page("Too many attempts. Wait a minute and try again.")
                key = form.get("password", [""])[0]
                user = form.get("username", [""])[0]
                ok = hmac.compare_digest(key.encode(), str(AUTH.get("password") or "").encode())
                if AUTH.get("user"):
                    ok = ok and hmac.compare_digest(user.encode(), str(AUTH["user"]).encode())
                if not ok:
                    fails = LOGIN_FAILS.setdefault(ip, [0, 0.0]); fails[0] += 1
                    if fails[0] >= 5: fails[0] = 0; fails[1] = time.time() + 60
                    return self._login_page("Incorrect password. Please try again.")
                LOGIN_FAILS.pop(ip, None)
                payload = str(int(time.time()) + SESSION_AGE) + "." + secrets.token_hex(16)
                token = payload + "." + sign_session(payload)
                self.send_response(303)
                self.send_header("Set-Cookie", f"ams_session={token}; Path=/; Max-Age={SESSION_AGE}; HttpOnly; SameSite=Lax")
                self.send_header("Set-Cookie", "ams_key=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax")
                self.send_header("Location", "/")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", "0"); self.end_headers()
                return
            if not self._authed(): return
            if not json_post_ok(self.headers): return self._send(403, json.dumps({"error": "Use the dashboard to make changes."}))
            body = read_body(self.headers, self.rfile)
            if body is None: return self._send(413, json.dumps({"error": "Request too large."}))
            n = len(body)
            if self.path == "/api/filament_stock":
                import urllib.parse
                origin = self.headers.get('Origin')
                if (origin and urllib.parse.urlsplit(origin).netloc != self.headers.get('Host')) or not self.headers.get('Content-Type', '').startswith('application/json'):
                    return self._send(403, json.dumps({'error': 'Use the dashboard to change filament stock.'}))
                if n > 4096: return self._send(413, json.dumps({'error': 'Request too large.'}))
                try:
                    request = json.loads(body)
                    if not isinstance(request, dict): raise ValueError('Invalid stock request.')
                    return self._send(200, json.dumps(stock_inventory.action(request, workers)))
                except (ValueError, TypeError) as exc: return self._send(400, json.dumps({'error': str(exc)}))
            if self.path == "/api/storage_clear":
                import urllib.parse
                origin = self.headers.get("Origin")
                if (origin and urllib.parse.urlsplit(origin).netloc != self.headers.get("Host")) or not self.headers.get("Content-Type", "").startswith("application/json"):
                    return self._send(403, json.dumps({"error": "Use the dashboard to clear printer storage."}))
                if n > 2048: return self._send(413, json.dumps({"error": "Request too large."}))
                try: request = json.loads(body)
                except (ValueError, TypeError): return self._send(400, json.dumps({"error": "Invalid request."}))
                if not isinstance(request, dict) or request.get("confirmation") != "CLEAR ALL":
                    return self._send(400, json.dumps({"error": "Confirmation is required."}))
                if not storage_lock.acquire(blocking=False):
                    return self._send(409, json.dumps({"error": "A storage check or clear is already running."}))
                token = str(request.get("token") or "")
                if not storage_preview.get("token") or not hmac.compare_digest(token, storage_preview["token"]) or time.time() > storage_preview.get("expires", 0):
                    storage_lock.release()
                    return self._send(409, json.dumps({"error": "Preview expired. Check the drives again."}))
                digest = storage_preview["digest"]
                storage_preview.clear()
                with storage_job_lock:
                    storage_job.clear()
                    storage_job.update(done=False, message="Rechecking all printers before clearing…", results=[], error="")
                try: threading.Thread(target=storage_clear_run, args=(digest,), daemon=True).start()
                except Exception:
                    storage_lock.release()
                    return self._send(500, json.dumps({"error": "Could not start storage clear."}))
                return self._send(202, json.dumps({"result": "started"}))
            if self.path == "/api/attention":
                import urllib.parse
                origin = self.headers.get('Origin')
                if (origin and urllib.parse.urlsplit(origin).netloc != self.headers.get('Host')) or not self.headers.get('Content-Type', '').startswith('application/json'):
                    return self._send(403, json.dumps({'error': 'Use the dashboard to change alerts.'}))
                try:
                    data = json.loads(body)
                    if not isinstance(data, dict): raise ValueError('Invalid alert request')
                    result = attention.action(data, self.client_address[0] in ('127.0.0.1', '::1'))
                    return self._send(200, json.dumps(result))
                except (ValueError, TypeError) as e: return self._send(400, json.dumps({'error': str(e)}))
            elif self.path.startswith("/api/p/"): self._proxy(body)
            elif self.path in ("/api/dry_all_printers", "/api/stop_all_printers"):
                import urllib.request
                target = "/api/dry_all" if self.path.endswith("dry_all_printers") else "/api/stop_all"
                out = []; unit_results = []
                for name, w in workers.items():
                    try:
                        req = urllib.request.Request(f"http://127.0.0.1:{w['port']}{target}", data=b"{}",
                                                     headers={"Content-Type": "application/json"}, method="POST")
                        with urllib.request.urlopen(req, timeout=25) as rr:
                            response=json.loads(rr.read());out.append(f"{name} — " + response.get("result", "?"))
                            unit_results.extend(dict(u,printer=name) for u in response.get("units",[]))
                    except Exception as e:
                        out.append(f"{name} — unreachable ({e})")
                        unit_results.append(dict(printer=name,status="blocked",message="Printer service unavailable. Retry when it reconnects."))
                self._send(200, json.dumps({"result": "ok", "detail": out, "units": unit_results}))
            elif self.path == "/api/add_printer":
                p = json.loads(body); p = {k: str(p.get(k, "")).strip() for k in ("name", "ip", "serial", "access_code")}
                if not all(p.values()): return self._send(200, json.dumps({"result": "all four fields are required"}))
                if not printer_name_ok(BASE, p["name"]): return self._send(200, json.dumps({"result": "use a simple name without slashes or a leading dot"}))
                if p["name"] in workers: return self._send(200, json.dumps({"result": "a printer with that name already exists"}))
                c = jload(CONFIG, {}); c.setdefault("printers", []).append(p); jsave(CONFIG, c); add_worker(p)
                self._send(200, json.dumps({"result": "ok"}))
            elif self.path == "/api/edit_printer":
                import shutil
                p = json.loads(body); old = p.get("old_name"); w = workers.get(old)
                if not w: return self._send(200, json.dumps({"result": "unknown printer"}))
                new = {k: str(p.get(k, "")).strip() for k in ("name", "ip", "serial", "access_code")}
                if not new["access_code"]: new["access_code"] = jload(os.path.join(w["dir"], "config.json"), {}).get("access_code", "")
                if not all(new.values()): return self._send(200, json.dumps({"result": "all four fields are required"}))
                if not printer_name_ok(BASE, new["name"]): return self._send(200, json.dumps({"result": "use a simple name without slashes or a leading dot"}))
                if new["name"] != old and new["name"] in workers: return self._send(200, json.dumps({"result": "that name is already used"}))
                if w["proc"]: w["proc"].terminate(); w["proc"].wait(timeout=10)
                d = w["dir"]
                if new["name"] != old:
                    nd = os.path.join(BASE, "printers", new["name"]); shutil.move(d, nd); d = nd
                    workers[new["name"]] = workers.pop(old)
                w = workers[new["name"]]; w.update(dir=d, proc=None)
                jog = any(x.get("allow_unrestricted_jog") for x in jload(CONFIG, {}).get("printers", []) if x.get("name") == old)
                jsave(os.path.join(d, "config.json"), dict(new, allow_unrestricted_jog=jog))
                c = jload(CONFIG, {}); c["printers"] = [dict(x, **new) if x["name"] == old else x for x in c.get("printers", [])]; jsave(CONFIG, c)
                self._send(200, json.dumps({"result": "ok"}))
            elif self.path == "/api/reorder":
                order = json.loads(body).get("order", [])
                c = jload(CONFIG, {}); byname = {p["name"]: p for p in c.get("printers", [])}
                c["printers"] = [byname[n] for n in order if n in byname] + [p for p in c.get("printers", []) if p["name"] not in order]
                jsave(CONFIG, c)
                ordered = {n: workers[n] for n in order if n in workers}; ordered.update({n: w for n, w in workers.items() if n not in ordered})
                workers.clear(); workers.update(ordered)
                self._send(200, json.dumps({"result": "ok"}))
            elif self.path == "/api/remove_printer":
                name = json.loads(body).get("name"); w = workers.pop(name, None)
                if w and w["proc"]: w["proc"].terminate()
                c = jload(CONFIG, {}); c["printers"] = [x for x in c.get("printers", []) if x["name"] != name]; jsave(CONFIG, c)
                self._send(200, json.dumps({"result": "ok" if w else "unknown printer"}))
            else: self._send(404, "{}")

    bind = "0.0.0.0" if cfg.get("lan_access") else "127.0.0.1"
    srv = http.server.ThreadingHTTPServer((bind, PORT), H)
    if cfg.get("lan_access"):
        import socket
        host = socket.gethostname()
        if host.endswith(".local"): host = host[:-6]
        ip = ""
        try:
            sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); sk.connect(("192.0.2.1", 1)); ip = sk.getsockname()[0]; sk.close()
        except Exception: pass
        print(f"[{iso()}] LAN access on: http://{host}.local:{PORT}" + (f"  or  http://{ip}:{PORT}" if ip else ""), flush=True)
        # publish a short Bonjour name so the office can use http://dry.local:8765
        alias = (jload(CONFIG, {}) or {}).get("lan_name", "dry")
        if ip:
            import subprocess
            def bonjour():
                while True:
                    try:
                        p = subprocess.Popen(["dns-sd", "-P", alias, "_http._tcp", "local", str(PORT),
                                              f"{alias}.local", ip], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        print(f"[{iso()}] short URL published: http://{alias}.local:{PORT}", flush=True)
                        p.wait()
                    except Exception as e:
                        print(f"[{iso()}] bonjour publish failed: {e}", flush=True); return
                    time.sleep(10)
            threading.Thread(target=bonjour, daemon=True).start()
    def stop(*a):
        for w in workers.values():
            if w["proc"]: w["proc"].terminate()
        sys.exit(0)
    signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
    print(f"[{iso()}] dashboard on http://localhost:{PORT} — printers: {', '.join(workers)}", flush=True)
    if open_browser: webbrowser.open(f"http://localhost:{PORT}")
    srv.serve_forever()


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
        try: lines = open(EVENTS).read().splitlines()[-a.n:]
        except FileNotFoundError: lines = []
        for l in lines:
            e = json.loads(l); print(f"{e['time']}  {e['kind']:12s} {e.get('slot') or '':7s} {e.get('msg','')}")
    else: ap.print_help()


if __name__ == "__main__":
    main()

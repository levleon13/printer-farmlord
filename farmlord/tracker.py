"""Tracker: turns each AMS report into spool records, events and drying credit."""

from farmlord.core import *  # noqa: F401,F403
from farmlord.materials import *  # noqa: F401,F403
from farmlord.ams import *  # noqa: F401,F403
from farmlord.errors import *  # noqa: F401,F403

SNAP_KEEP_DAYS = 3     # only recent drying cycles are looked up; older snapshots live in the monthly archives

LAST_SNAP_ROTATE = 0

def rotate_snapshots():
    """Keep the last SNAP_KEEP_DAYS of raw AMS snapshots in snapshots.jsonl; older lines move into
    monthly gzip archives next to it (snapshots-YYYY-MM.jsonl.gz). Nothing is discarded."""
    import gzip
    global LAST_SNAP_ROTATE
    LAST_SNAP_ROTATE = now()
    try:
        if not os.path.exists(SNAPS) or os.path.getsize(SNAPS) < 2_000_000: return
        cutoff = now() - SNAP_KEEP_DAYS * 86400
        keep_path = SNAPS + ".keep.tmp"; archives = {}; moved = 0
        with open(SNAPS) as source, os.fdopen(os.open(keep_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as keep:
            for line in source:
                try: t = float(line[6:line.index(",")]) if line.startswith('{"t": ') else json.loads(line).get("t", 0)
                except (ValueError, TypeError): t = now()
                if t >= cutoff: keep.write(line); continue
                month = dt.datetime.fromtimestamp(t).strftime("%Y-%m")
                if month not in archives:
                    archives[month] = gzip.open(os.path.join(BASE, f"snapshots-{month}.jsonl.gz"), "at")
                archives[month].write(line); moved += 1
        for f in archives.values(): f.close()
        for month in archives: os.chmod(os.path.join(BASE, f"snapshots-{month}.jsonl.gz"), 0o600)
        os.replace(keep_path, SNAPS)
        if moved: print(f"[{iso()}] snapshots: archived {moved} lines older than {SNAP_KEEP_DAYS} days", flush=True)
    except Exception as exc:
        print(f"[{iso()}] snapshot rotation skipped: {exc}", flush=True)

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
            if t - LAST_SNAP_ROTATE > 86400: rotate_snapshots()   # same thread as the writer: no lines lost

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


__all__ = ['SNAP_KEEP_DAYS', 'LAST_SNAP_ROTATE', 'rotate_snapshots', 'recorded_dry_stop_reason', 'Tracker']

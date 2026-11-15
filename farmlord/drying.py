"""AMS drying: requests, readiness, auto-drying, fleet drying and the unload needed before drying."""

from farmlord.core import *  # noqa: F401,F403
from farmlord.materials import *  # noqa: F401,F403
from farmlord.ams import *  # noqa: F401,F403
from farmlord.errors import *  # noqa: F401,F403
from farmlord.tracker import *  # noqa: F401,F403
from farmlord.actions import *  # noqa: F401,F403

AUTO_HUMIDITY_STATE = P("auto_humidity.json")

# Request state has its own lock/file: MQTT replies must not race unrelated settings writes.
DRY_REQUESTS = P("dry_requests.json")

DRY_LOCK = _th.RLock()

DRY_MAX_ATTEMPTS = 3

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

def dry_error_state_hold(w, state):
    """True while a request refused with ERROR STATE should keep waiting: same alerts, and under 5 minutes."""
    return (w.get("status") == "waiting" and w.get("err") == DRY_ERROR_STATE and
            printer_error_signature(state) == w.get("error_sig") and now() - w.get("last", 0) < 300)

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
            alerts = printer_alerts_text(state)
            if err == DRY_ERROR_STATE:
                # The printer refuses every command while an alert is active. Don't use up a try; ask again
                # when its alerts or print error change, or every 5 minutes (dry_error_state_hold).
                w["attempts"] = max(0, w.get("attempts", 1) - 1)
                w.update(status="waiting", error_sig=printer_error_signature(state), last=now(),
                         message="The printer refused: it is in an error state. " +
                                 (f"Active alert: {alerts}. " if alerts else "Check the printer screen for the alert. ") +
                                 "Clear it on the printer (or restart it); drying starts again by itself.")
            elif 3 in sf and queue_drying_unload(aid,w,state): pass
            elif 3 in sf or 10 in sf:
                dry_block(aid, w, "The printer reports filament in the AMS outlet. Drying is paused because the printer has not cleared its outlet interlock.")
            elif err in DRY_WAIT_CODES or (sf and set(sf).issubset({0, 1, 2, 4, 6})):
                w.update(status="waiting", wait_context=dry_wait_context(aid, state),
                         message="Waiting for the printer or drying power to become available")
            elif w.get("attempts", 0) >= DRY_MAX_ATTEMPTS:
                dry_block(aid, w, f"The printer refused to start drying after three attempts ({dry_refusal_reason(err, alerts)}). Automatic retries are stopped.")
                w.pop("block_kind", None)   # quoted alert text may mention the outlet; this is not an outlet block
            else:
                w.update(status="pending", message=f"Printer refused drying ({dry_refusal_reason(err, alerts)}) — attempt {w.get('attempts', 1)} of {DRY_MAX_ATTEMPTS}")
        jsave(DRY_REQUESTS, requests)

def drying_faults(state):
    faults = []
    for aid, w in dry_requests().items():
        if w.get("status") != "blocked" or (ams_is_drying(state.get("ams", {}).get(aid, {})) and not w.get("safety_stop_sent")): continue
        label = ams_name(aid)
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
            if dry_error_state_hold(w, state): continue
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

def _pending_after_unload():
    return settings().get("resume_dry_after_unload") or {}

def rfid_dry_time(aid, reported, cycles=None):
    """Show the saved countdown while a resumed cycle uses rounded firmware hours."""
    w = (cycles if cycles is not None else jload(RFID_HEAT_CYCLES, {})).get(str(aid), {})
    if w.get("stage") not in ("active", "finishing"): return reported
    left = max(0, math.ceil((w.get("stop_at", now()) - now()) / 60))
    return min(left, reported) if isinstance(reported, (int, float)) and reported > 0 else left

PROBE = {"busy": False}

def dry_probe(ams_id, temp=55):
    return "Payload probing is disabled. Use Start drying; firmware restrictions are respected."

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
                wait(f"Waiting for {ams_name(power_owner)} to release the shared printer drying power");continue
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
            if dry_error_state_hold(w,state):continue
            delay=20 if w.get("reply_received") or w.get("status")=="waiting" else 90
            if now()-w.get("last",0)<delay:continue
            jsave(DRY_REQUESTS,requests)
            set_drying(aid,True,w["temp"],w["hours"],w.get("rotate",False),retry=True,resume_saved=w.get("resume_saved",False))
            requests[aid]=dry_requests().get(aid,w)
        jsave(DRY_REQUESTS,requests)

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
        if a and (not plan or plan.get("unidentified")) and not resume_saved:
            slots = ", ".join(str(int(t) + 1) for t in (plan or {}).get("unidentified", []) if str(t).isdigit())
            message = f"Set the material in slot {slots} — drying starts once every occupied slot has one" if slots else "Set the material in every occupied slot — drying starts once they all have one"
            if retry:   # keep the request, but say what it is really waiting for
                previous.update(status="waiting", message=message); requests[aid] = previous; jsave(DRY_REQUESTS, requests)
            return message
        safe_temp = min(temp, UNIT_MAX(aid)) if manual else min(temp, UNIT_MAX(aid), plan["temp"] if plan else temp)
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


__all__ = ['AUTO_HUMIDITY_STATE', 'DRY_REQUESTS', 'DRY_LOCK', 'DRY_MAX_ATTEMPTS', 'dry_requests', 'drying_readiness',
           'dry_is_held', 'dry_wait_context', 'dry_error_state_hold', 'dry_block', 'cancel_dry_request',
           'drying_reply', 'drying_faults', 'has_psu', 'drying_priority', 'drying_order',
           'higher_priority_drying_unit', 'plan_unit', 'dry_slot_taken_by', 'drying_wait_details', 'clear_zombie_dry',
           'humidity_auto_due', 'humidity_auto_started', 'autodry_tick', '_pending_after_unload', 'rfid_dry_time',
           'PROBE', 'dry_probe', 'psu_check_tick', 'missing_drying_slots', 'request_fleet_drying_unit',
           'request_fleet_drying', 'fleet_setup_tick', 'DRY_UNLOAD', 'DRY_UNLOAD_TIMEOUT', 'dry_unload_busy',
           'dry_unload_reply', 'dry_unload_thermal_refusal', 'drying_prepare_tick', 'queue_drying_unload',
           'dry_pending_tick', 'drying_start_in_flight', 'set_drying', 'do_dried']

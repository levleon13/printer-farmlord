"""RFID tag reads: pausing drying, unloading the nozzle, scanning slots and restoring drying."""

from farmlord.core import *  # noqa: F401,F403
from farmlord.materials import *  # noqa: F401,F403
from farmlord.ams import *  # noqa: F401,F403
from farmlord.errors import *  # noqa: F401,F403
from farmlord.tracker import *  # noqa: F401,F403
from farmlord.actions import *  # noqa: F401,F403
from farmlord.drying import *  # noqa: F401,F403

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

READ_COOL_WAIT = 900    # s an AMS may take to cool after drying stops (an AMS HT at 85 °C needs minutes)

READ_GAP = 6            # s between one slot's scan ending and the next request; back-to-back requests get ignored

READ_TRIES = 3          # requests per slot before skipping it

RFID_HEAT_CYCLES = P("rfid_heat_cycles.json")

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

def rfid_restore_refused(aid, w):
    """set_drying reports a definite refusal as "ok — <reason>" and marks the request blocked,
    either at once or after its own retries; only a block raised since this restore counts."""
    req = dry_requests().get(str(aid)) or {}
    return req.get("status") == "blocked" and (req.get("failed_at") or 0) >= (w.get("resume_sent") or now())

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
            feed = ams_feeding_nozzle(state, aid) if stage == "scan" else None
            if feed and w.get("allow_unload") and not w.get("sent_at") and not w.get("auto_unloaded") and not rfid_auto_unload_blocker(state, feed):
                w.update(stage="unload",stage_at=now(),auto_unloaded=True);changed=True
                append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: slot {feed['slot']+1} is loaded in {feed['nozzle']}; unloading it before the tag read"))
                continue
            if feed:
                append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: tag read cancelled — "+feeding_block_text(aid,feed)))
                for field in ("sent_at","seen_reading","seen_tray_busy","initial_rfid","scan_until_before"):w.pop(field,None)
                rfid_finish_scans(cycles,aid,w)
                changed=True;continue
            if stage == "stopping":
                if not ams_is_drying(a) and ams_info_bits(a.get("info"),4,4)==0 and (a.get("temp") or 999)<=READ_MAX_TEMP:
                    w.update(stage="scan", stage_at=now()); changed=True
                elif elapsed>READ_COOL_WAIT:
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} did not cool to {READ_MAX_TEMP}°C within {READ_COOL_WAIT//60} minutes; restoring drying"))
                    w.update(stage="resume",stage_at=now());changed=True
            elif stage == "unload":
                # Save the command sequence before publishing so a restart cannot
                # send a second filament-cutter operation while the first is pending.
                if not ams_feeding_nozzle(state, aid) and state.get("ams_status")==0 and not ams_is_drying(a):
                    if set(a.get("dry_sf") or []) & {3, 10}:
                        if not w.get("outlet_wait"):w["outlet_wait"]=now();changed=True
                        if now()-w["outlet_wait"]<90:continue
                    w.pop("outlet_wait",None)
                    if w.get("unload_sent_at"):
                        append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: nozzle unloaded; reading the tag"))
                    w.update(stage="scan",stage_at=now(),last_done=now());changed=True;continue
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
                if elapsed>600:
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: the AMS never became ready to unload the nozzle; tag read cancelled"))
                    rfid_finish_scans(cycles,aid,w);changed=True;continue
                if ams_is_drying(a) or (a.get("temp") or 999)>READ_MAX_TEMP or ams_info_bits(a.get("info"),4,4)!=0:continue
                if state.get("ams_status")!=0 or filament_busy(settings()) or now()<state.get("scan_until",0):continue
                blocked=toolhead_safe(state)
                if blocked:
                    if elapsed>300:
                        append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} cannot unload for RFID: {blocked}"))
                        rfid_finish_scans(cycles,aid,w)
                        changed=True
                    continue
                feed=ams_feeding_nozzle(state, aid)
                why=rfid_auto_unload_blocker(state, feed) if feed else None
                if why:
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: tag read cancelled — "+feeding_block_text(aid,feed)+f" ({why})"))
                    rfid_finish_scans(cycles,aid,w);changed=True;continue
                if dry_unload_busy():continue
                if not w.get("allow_unload"):
                    rfid_finish_scans(cycles,aid,w);changed=True;continue
                loaded_aid=int(aid)
                sequence=str(time.time_ns()%2147483647)
                w.update(unload_sequence=sequence,unload_sent_at=now());cycles[aid]=w;jsave(RFID_HEAT_CYCLES,cycles)
                payload={"print":{"sequence_id":sequence,"command":"ams_change_filament","ams_id":loaded_aid,
                                  "target":255,"slot_id":255,"curr_temp":210,"tar_temp":210}}
                try:
                    result=MQTT["client"].publish(MQTT["topic_req"],json.dumps(payload))
                    if getattr(result,"rc",0)!=0:raise RuntimeError("publish failed")
                    mark_filament_busy(30)
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: unloading the nozzle before the tag read"))
                except Exception:
                    # Delivery is ambiguous, so do not automatically retry.
                    w["unload_reply"]="fail";changed=True
            elif stage == "scan":
                if not w.get("slots"):
                    rfid_finish_scans(cycles,aid,w)
                    changed=True;continue
                slot=str(w["slots"][0]);tray=a.get("trays",{}).get(slot,{})
                if not w.get("sent_at"):
                    if filament_busy(settings()) or now()-w.get("last_done",0)<READ_GAP:continue
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
                        tries=w.setdefault("tries",{});tries[slot]=tries.get(slot,1)+1
                        if tries[slot]<=READ_TRIES:
                            append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} slot {int(slot)+1}: printer did not start the scan; asking again ({tries[slot]}/{READ_TRIES})"))
                        else:
                            append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} slot {int(slot)+1}: printer would not scan after {READ_TRIES} requests; skipped, continuing with the next slot"))
                            w["slots"].pop(0);tries.pop(slot,None)
                        for field in ("sent_at","seen_reading","seen_tray_busy","initial_rfid","scan_until_before"):w.pop(field,None)
                        w["last_done"]=now();changed=True;continue
                    done=(w.get("seen_reading") and not tray.get("reading") and
                          ((w.get("seen_tray_busy") and tray.get("read_done")) or
                           (tray.get("rfid") and tray.get("rfid")!=w.get("initial_rfid")))) or elapsed_scan>45
                    if done:
                        outcome="tag read" if tray.get("rfid") and w.get("seen_reading") else "scan ended without a readable tag" if w.get("seen_reading") else "scan did not finish"
                        append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} slot {int(slot)+1}: {outcome}"))
                        w["slots"].pop(0);(w.get("tries") or {}).pop(slot,None)
                        for field in ("sent_at","seen_reading","seen_tray_busy","initial_rfid","scan_until_before"):w.pop(field,None)
                        w["last_done"]=now();changed=True
            elif stage == "clear_outlet":
                # Some H2 printers report a separate AMS-outlet interlock even
                # after the nozzle has been unloaded. The native command for the
                # target AMS clears it; observe the printer state, not just its ACK.
                if ams_is_drying(a) or (not set(a.get("dry_sf") or []) & {3, 10} and str(state.get("tray_now"))=="255" and state.get("ams_status")==0):
                    w.update(stage="resume",stage_at=now(),outlet_cleared=True);changed=True;continue
                if w.get("outlet_reply")=="fail" or (w.get("outlet_sent_at") and now()-w["outlet_sent_at"]>300):
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} outlet did not clear; saved drying restart is blocked by the printer"))
                    cycles.pop(aid);changed=True;continue
                if not w.get("outlet_sent_at") and ams_feeding_nozzle(state, aid):
                    w.update(stage="resume",stage_at=now());changed=True;continue
                if not w.get("outlet_sent_at") and elapsed>900:
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: the AMS outlet could not be cleared safely, so the paused drying was not restarted — start it again from the dashboard"))
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
                if ams_is_drying(a):
                    rfid_resume_tracking(w, a)
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} drying restored with {math.ceil(float(w.get('remaining_min') or w['hours']*60))} min remaining"))
                    changed=True;continue
                feed = ams_feeding_nozzle(state, aid)
                outlet = set(a.get("dry_sf") or []) & {3, 10}
                if feed and outlet:
                    # The printer will not dry while this AMS's filament sits in its outlet. Wait without
                    # resending (each refused attempt raises an alert) and resume once it is unloaded.
                    if not w.get("hold_since"):
                        w["hold_since"]=now();changed=True
                        append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: paused drying stays off while slot {feed['slot']+1} is loaded in {feed['nozzle']} — the printer won't dry with filament in the AMS outlet. It restarts by itself after you unload."))
                    elif now()-w["hold_since"]>24*3600:
                        append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: filament stayed loaded for 24 h, so the paused drying was dropped — start it again from the dashboard if needed"))
                        cycles.pop(aid);changed=True
                    continue
                if w.get("hold_since"):
                    for field in ("hold_since","outlet_cleared","outlet_sent_at","outlet_reply","outlet_error","outlet_sequence"):w.pop(field,None)
                    w["stage_at"]=now();elapsed=0;changed=True
                if outlet and not w.get("outlet_cleared"):
                    w.update(stage="clear_outlet",stage_at=now());changed=True;continue
                if elapsed>900:
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)} drying restart is pending; check the printer if it does not resume"))
                    cycles.pop(aid);changed=True;continue
                if w.get("resume_sent") and now()-w["resume_sent"]<30:continue
                # The printer's own target and remaining timer were captured before stop.
                if w.get("starts_sent",0)>=3:
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: the drying restart was sent 3 times but the AMS never started heating; start it again from the dashboard"))
                    cancel_dry_request(aid,automatic_stop=True);cycles.pop(aid);changed=True;continue
                w["resume_sent"]=now();changed=True
                result=set_drying(aid,True,w["temp"],w["hours"],w.get("rotate",False),resume_saved=True)
                if (dry_requests().get(str(aid)) or {}).get("status")=="pending":
                    # A START was actually published (waits publish nothing). Each resume builds a fresh
                    # request, so cap the sends here or an accepted-but-never-heating AMS loops forever.
                    w["starts_sent"]=w.get("starts_sent",0)+1
                if rfid_restore_refused(aid,w):
                    # set_drying answers "ok — <reason>" for a definite refusal and has already raised
                    # one alert; retrying would only repeat it.
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: the printer refused to restart the drying paused for the tag read; start it again from the dashboard once the reason above is cleared"))
                    cycles.pop(aid);continue
                if result.startswith("ok") or result.startswith("already drying"):
                    w.update(stage="confirm",stage_at=now())
            elif stage == "confirm":
                if not ams_is_drying(a) and rfid_restore_refused(aid,w):
                    # The printer refused through its replies (dry_pending_tick gave up and raised the alert).
                    append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: the printer refused to restart the drying paused for the tag read; start it again from the dashboard once the reason above is cleared"))
                    cycles.pop(aid);changed=True;continue
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

def rfid_auto_unload_blocker(state, feed):
    """The native unload (ams_change_filament target 255) lets the firmware pick the nozzle,
    so only automate it when the filament to remove is in the active nozzle."""
    if not feed.get("multi"): return None
    try: active = (int((state.get("nozzle_routing") or {})["state"]) >> 4) & 15
    except (KeyError, TypeError, ValueError): return "the printer has not said which nozzle is active"
    if str(active) != feed.get("nid"): return f"it is in {feed['nozzle']}, which is not the active nozzle"
    return None

def rfid_unload_notice(aid, feed):
    return (f"Unloading {ams_name(aid)} slot {feed['slot']+1} from {feed['nozzle']} first; the tag read starts by itself "
            f"when it's out. The filament stays unloaded afterwards.")

def feeding_block_text(aid, feed):
    return (f"{ams_name(aid)} can't read tags while its slot {feed['slot']+1} filament is loaded in "
            f"{feed['nozzle']}. Unload it (Unload on slot {feed['slot']+1}), then press Read RFID again.")

OUTLET_CLEARS = {}   # ams id -> [send times]; guards against repeating a filament operation

def clear_ams_outlet(aid, state, a):
    """Retract filament left in an AMS outlet with the same native command the drying restore uses.
    Returns "sent", or the reason it is not safe right now (nothing is sent then)."""
    label = ams_name(aid)
    if str(state.get("tray_now")) != "255": return f"{label}: filament is in the nozzle; unload it first"
    if state.get("ams_status") != 0: return f"{label}: the AMS is moving filament; waiting for it to stop"
    if ams_is_drying(a) or (a.get("temp") or 999) > READ_MAX_TEMP: return f"{label}: waiting for drying to finish and the AMS to cool"
    if filament_busy(settings()): return "waiting for the current filament operation to finish"
    unsafe = toolhead_safe(state)
    if unsafe: return f"waiting until it is safe: {unsafe}"
    if not MQTT.get("client"): return "printer not connected"
    sent = [t for t in OUTLET_CLEARS.get(aid, []) if now() - t < 1800]
    if sent and now() - sent[-1] < 120: return "sent"            # one already in flight; give it time
    if len(sent) >= 3: return f"{label}: the outlet did not clear after 3 tries; pull the filament back by hand or check the printer screen"
    sequence = str(time.time_ns() % 2147483647)
    payload = {"print": {"sequence_id": sequence, "command": "ams_change_filament", "ams_id": int(aid),
                         "target": 255, "slot_id": 255, "curr_temp": 210, "tar_temp": 210}}
    try:
        result = MQTT["client"].publish(MQTT["topic_req"], json.dumps(payload))
        if getattr(result, "rc", 0) != 0: raise RuntimeError("publish failed")
    except Exception:
        return "could not reach the printer to clear the outlet; try again in a moment"
    OUTLET_CLEARS[aid] = sent + [now()]
    mark_filament_busy(30)
    append(EVENTS, dict(t=now(), time=iso(), kind="RFID", msg=f"{label}: filament left in the outlet; clearing it automatically before the tag read"))
    return "sent"

def do_read_rfid(ams_id, slot_id, pause_drying=False, user=False):
    """user=True only for an explicit Read RFID press: only then may a loaded nozzle be unloaded
    (that works the filament cutter). Background retries never unload."""
    with DRY_LOCK:
        aid=str(int(ams_id));slot=str(int(slot_id));state=jload(STATE,{});a=state.get("ams",{}).get(aid,{})
        if STATUS.get("mqtt")!="connected" or now()-state.get("t",0)>120: return "waiting for fresh printer status"
        if state.get("print_state") not in ("IDLE","FINISH","FAILED") or (state.get("print_state")=="FAILED" and unresolved_print_fault(state)): return "the printer is busy — tag reads wait until idle"
        if slot not in a.get("trays",{}):return "that AMS slot is unavailable"
        feed=ams_feeding_nozzle(state,aid)
        if feed:
            why=rfid_auto_unload_blocker(state,feed) if user else "only a Read RFID press unloads automatically"
            if why:return feeding_block_text(aid,feed)+(f" (It can't be unloaded automatically: {why}.)" if user else "")
        cycles=jload(RFID_HEAT_CYCLES,{}) or {}
        held=cycles.get(aid) or {}
        if feed and held.get("stage")=="resume" and held.get("hold_since") and not ams_is_drying(a):
            # A drying restore is waiting for exactly this filament to leave; unload it, read, then restore.
            if filament_busy(settings()) or dry_unload_busy():return "waiting for the current filament operation to finish"
            if any(w.get("status")=="pending" for w in dry_requests().values()):return "waiting for the drying request to finish"
            blocked=toolhead_safe(state)
            if blocked:return f"{ams_name(aid)} slot {feed['slot']+1} has to be unloaded before the tag read, but it isn't safe right now: {blocked}"
            for field in ("hold_since","outlet_cleared","outlet_sent_at","outlet_reply","outlet_error","outlet_sequence",
                          "unload_sequence","unload_sent_at","unload_reply","unload_error","drying_refusal_retry",
                          "resume_sent","starts_sent","tries","sent_at","last_done"):held.pop(field,None)
            held.update(stage="unload",stage_at=now(),slots=[slot],auto_unloaded=True,allow_unload=True)
            cycles[aid]=held;jsave(RFID_HEAT_CYCLES,cycles)
            return rfid_unload_notice(aid,feed)+" The paused drying restarts after the read."
        restored=cycles.get(aid) if cycles.get(aid,{}).get("stage")=="active" and pause_drying else None
        if aid in cycles and not restored:
            w=cycles[aid]
            if slot not in w.get("slots",[]) and w.get("stage") in ("stopping","unload","scan"):
                w.setdefault("slots",[]).append(slot)
                if user:w["allow_unload"]=True
                cycles[aid]=w;jsave(RFID_HEAT_CYCLES,cycles)
            if w.get("stage") not in ("stopping","unload","scan"):
                return f"{ams_name(aid)} is still restoring the drying it paused for the last tag read; try again once that finishes."
            return "ok"
        if ams_is_drying(a) and pause_drying:
            if not MQTT.get("client"):return "printer not connected"
            setting=a.get("dry_setting") or {}
            temp=int(setting.get("dry_temperature") or 0)
            if not 45<=temp<=UNIT_MAX(aid):return "the current drying target is unavailable; cannot restore it safely"
            # A cycle restored after an earlier read runs on rounded-up firmware hours; keep its exact countdown.
            remaining=rfid_dry_time(aid,a.get("dry_time"),cycles)
            if not isinstance(remaining,(int,float)) or not math.isfinite(remaining) or remaining<=0:return "the remaining drying time is unavailable; cannot restore it safely"
            hours=max(1,min(24,int(math.ceil(float(remaining)/60))))
            cycles[aid]=dict(stage="stopping",stage_at=now(),pause_at=now(),slots=[slot],temp=temp,hours=hours,remaining_min=float(remaining),rotate=False,restore_dry=True,allow_unload=bool(user))
            jsave(RFID_HEAT_CYCLES,cycles)
            result=set_drying(aid,False)
            if result!="ok":
                if restored:cycles[aid]=restored
                else:cycles.pop(aid,None)
                jsave(RFID_HEAT_CYCLES,cycles);return result
            append(EVENTS,dict(t=now(),time=iso(),kind="RFID",msg=f"{ams_label(aid)}: pausing drying for RFID read; will resume {temp}°C with {math.ceil(remaining)} min left"))
            return "ok"
        if feed and not ams_is_drying(a):
            if filament_busy(settings()) or dry_unload_busy():return "waiting for the current filament operation to finish"
            if any(w.get("status")=="pending" for w in dry_requests().values()):return "waiting for the drying request to finish"
            blocked=toolhead_safe(state)
            if blocked:return f"{ams_name(aid)} slot {feed['slot']+1} has to be unloaded before the tag read, but it isn't safe right now: {blocked}"
            cycles[aid]=dict(stage="unload",stage_at=now(),slots=[slot],restore_dry=False,auto_unloaded=True,allow_unload=True)
            jsave(RFID_HEAT_CYCLES,cycles)
            return rfid_unload_notice(aid,feed)
        if ams_is_drying(a) or (a.get("temp") or 0)>READ_MAX_TEMP:return "wait until drying finishes and the AMS cools before reading the tag"
        busy=[r for r in (a.get("dry_sf") or []) if r in (2, 3, 4, 7, 10)]
        if busy and set(busy) <= {3, 10}:
            # Filament left in the AMS outlet blocks the read. Clear it when safe and keep the read queued:
            # cool_read_tick retries it, and it goes through once the printer reports the outlet clear.
            stg=settings();stg.setdefault("cool_read",{})[f"{aid}:{slot}"]=dict(since=now(),temp=0,quiet=True);save_settings(stg)
            sent=clear_ams_outlet(aid,state,a)
            if sent=="sent":return f"Clearing filament from the {ams_name(aid)} outlet first; the tag read starts by itself when it's clear."
            return sent+". The tag read is queued and starts by itself."
        if busy:
            stg=settings();stg.setdefault("cool_read",{})[f"{aid}:{slot}"]=dict(since=now(),temp=0,quiet=True);save_settings(stg)
            return f"{ams_name(aid)}: "+DRY_REASON_TEXT.get(busy[0],"the AMS is busy").rstrip(".")+". The tag read is queued and starts by itself when it finishes."
        if now()-state.get("print_state_t",0)>120 or state.get("t",0)<STATUS.get("since",0):return "waiting for fresh printer status"
        if a.get("temp") is None or ams_info_bits(a.get("info"),4,4) != 0:return "waiting for the AMS to be idle and cool"
        if filament_busy(settings()):return "waiting for the filament operation to finish"
        if any(w.get("status")=="pending" for w in dry_requests().values()):return "waiting for the drying request to finish"
        cycles[aid]=dict(stage="scan",stage_at=now(),slots=[slot],restore_dry=False,allow_unload=bool(user))
        jsave(RFID_HEAT_CYCLES,cycles)
        return "ok"

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


__all__ = ['_rfid_now', 'do_ams_read_options', 'READ_MAX_TEMP', 'READ_COOL_WAIT', 'READ_GAP', 'READ_TRIES',
           'RFID_HEAT_CYCLES', 'rfid_resume_tracking', 'rfid_finish_scans', 'rfid_restore_refused',
           'rfid_heat_cycle_tick', 'rfid_auto_unload_blocker', 'rfid_unload_notice', 'feeding_block_text',
           'OUTLET_CLEARS', 'clear_ams_outlet', 'do_read_rfid', 'rfid_unload_reply', 'cool_read_tick',
           'ams_wedged_tick', 'rfid_retry_tick', 'tag_read_note']

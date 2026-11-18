"""JSON views served to the dashboard: the fast poll (rev_data) and the full state (report_data)."""

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
    active=[ams_name(aid) for aid,a in state.get("ams",{}).items() if ams_is_drying(a)]
    if active:return "Drying filament · "+", ".join(active)
    env=state.get("env",{})
    warming=[name for key,name in (("bed","bed"),("chamber","chamber"),("nozzle","nozzle")) if env.get(key) is not None and env.get(key+"_target") is not None and env[key+"_target"]>env[key]+3]
    if warming:return "Preheating "+" and ".join(warming)
    return "Idle · ready for the next job"

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
        label=ams_name(aid)
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
        parts.append(str(int(max(os.path.getmtime(SPOOLS), SPOOLS_SAVED["t"])))); parts.append(str(int(os.path.getmtime(SETTINGS))))
        if os.path.exists(ACTFILE): parts.append(str(os.path.getmtime(ACTFILE)))
        if os.path.exists(COOLDOWN): parts.append(str(os.path.getmtime(COOLDOWN)))
    except Exception: pass
    err = error_data(st); parts.append(str(err))
    camera_warning = camera_warning_status(); parts.append(str(camera_warning))
    preview = preview_info(st)
    if preview: parts.append(str(preview))
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
    return dict(current_action=current_printer_action(st), estimated_power=estimated_power(st), nozzle_source=nozzle_source, tray_now=st.get("tray_now"), room_units=room_units, print_name=st.get("print_name"), preview=preview, rev=rev, pullback_retry=pullback_retry_data(st), errors=err, process_faults=dry_faults, has_error=bool(err["print_error"] or err["hms"] or dry_faults), camera_warning=camera_warning, print_state=st.get("print_state"), print_pct=st.get("print_pct"),
                print_left_min=st.get("print_left_min"), heating=heating,
                needs=sum(1 for p in pcts if p < 60), spools=len(pcts),
                mqtt=STATUS.get("mqtt"), error=STATUS.get("error"),
                stale=(now() - st.get("t", 0)) > 900)

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
    try: ev = [json.loads(l) for l in tail_lines(EVENTS, 40)][::-1]
    except FileNotFoundError: ev = []
    return dict(unrestricted_jog=bool(jload(CONFIG, {}).get("allow_unrestricted_jog")), current_action=current_printer_action(state), estimated_power=estimated_power(state), nozzle_source=nozzle_source, generated=iso(), last_update=iso(state.get("t")) if state.get("t") else None,
                print_state=state.get("print_state"), print_left_min=state.get("print_left_min"),
                pullback_retry=pullback_retry_data(state), errors=error_data(state), process_faults=drying_faults(state), spd_lvl=state.get("spd_lvl"), spd_mag=state.get("spd_mag"),
                wedge_note=settings().get("wedge_note"), print_pct=state.get("print_pct"), print_layer=state.get("print_layer"), print_layers=state.get("print_layers"), print_name=state.get("print_name"),
                activity=jload(ACTFILE, None), asked_env=(settings().get("asked_env") if state.get("print_state") in ("IDLE","FINISH") else None), env=state.get("env", {}), fans=state.get("fans", {}), fan_off_at=jload(COOLDOWN, {}).get("fan_off_at"), lights=state.get("lights", {}), preheat=(st.get("preheat") if st.get("preheat") is not None else DEFAULT_PREHEAT), auto_cooldown=False,
                stale=(now() - state.get("t", 0)) > 900, mqtt=STATUS, slots=rows, known=known, used=used, palette=[dict(hex=h, name=n) for h, n in PALETTE], movable=movable, candidates=move_candidates(), presets=DRY_PRESETS, units=units, materials=list(MATERIALS),
                unnamed=[dict(id=s["id"], type=s["type"], slot=s["slot"]) for s in unknown], events=ev)


__all__ = ['current_printer_action', 'math', '_POWER_SAMPLES', '_POWER_ESTIMATE_LOCK', '_power_number',
           '_power_slope', 'estimated_power', 'display_slot_spool', 'rev_data', 'report_data']

"""Manual printer controls: filament load/unload, motion, temperatures, fans, pull-back recovery."""

from farmlord.core import *  # noqa: F401,F403
from farmlord.materials import *  # noqa: F401,F403
from farmlord.ams import *  # noqa: F401,F403
from farmlord.errors import *  # noqa: F401,F403
from farmlord.tracker import *  # noqa: F401,F403
from farmlord.actions import *  # noqa: F401,F403
from farmlord.drying import *  # noqa: F401,F403
from farmlord.rfid import *  # noqa: F401,F403

DEFAULT_PREHEAT = {"PLA": {"bed": 60, "chamber": 0, "nozzle": 220}, "PETG": {"bed": 80, "chamber": 0}, "PETG-CF": {"bed": 70, "chamber": 0}, "PA-CF": {"bed": 100, "chamber": 65}}

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
    lim = {"bed": 120, "chamber": 65, "nozzle": 350}   # H2D/H2S nozzle rating: 350 °C
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

def do_preheat(bed, chamber, label="", nozzle=None):
    with FAN_CONTROL_LOCK:
        return _do_preheat(bed, chamber, label, nozzle)

def _do_preheat(bed, chamber, label="", nozzle=None):
    st = jload(STATE, {})
    if st.get("print_state") in ("RUNNING", "PREPARE", "PAUSE"): return "printer is printing — not touching temperatures"
    bed = max(0, min(120, int(bed or 0))); chamber = max(0, min(65, int(chamber or 0)))
    nozzle = None if nozzle is None else max(0, min(350, int(nozzle)))
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


__all__ = ['DEFAULT_PREHEAT', 'do_filament', 'do_ams_unload', 'MOVE_WRAP', 'UNRESTRICTED_MOVE_WRAP', 'do_control',
           'CMDFILE', 'CMDRESULT', 'command_file_tick', 'PULLBACK_RETRY', 'PULLBACK_MAX_RETRIES',
           'PULLBACK_RETRY_DELAY', 'pullback_fault', 'pullback_fresh', 'pullback_recovery', 'pullback_retry_data',
           'retry_pullback', 'pullback_retry_tick', 'do_resolve', 'do_temp', 'do_cooldown_heaters',
           'do_print_control', 'do_preheat', '_do_preheat', 'COOLDOWN', 'FAN_CONTROL_LOCK', 'fan_timer_tick',
           'cooldown_tick']

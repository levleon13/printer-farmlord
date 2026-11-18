"""The printer connection: MQTT over TLS, parsing each report into state.json and events."""

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
            try: jsave_changed(P("last_report_raw.json"), rep)
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
        if "job_id" in pr or "gcode_start_time" in pr:
            with LOCK:
                if "job_id" in pr: tr.state["job_id"] = str(pr.get("job_id") or "")
                if "gcode_start_time" in pr: tr.state["job_started"] = str(pr.get("gcode_start_time") or "")
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
        if "nozzle_diameter" in pr:
            try: tr.state.setdefault("env", {})["nozzle_diameter"] = round(float(pr["nozzle_diameter"]), 2)
            except (TypeError, ValueError): pass
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
                noz.append(dict(id=e.get("id"), temp=cur, target=tgt, hnow=e.get("hnow"),
                                active=bool(e.get("stat")) or bool(e.get("filam_bak"))))
            if noz:
                env["nozzles"] = noz
                hot = max(noz, key=lambda n: (n["active"], n["temp"]))
                env["nozzle"] = hot["temp"]
                if hot.get("target") is not None: env["nozzle_target"] = hot["target"]
            ninfo = (dev.get("nozzle") or {}).get("info") if isinstance(dev.get("nozzle"), dict) else None
            if isinstance(ninfo, list):
                sizes = {}
                for z in ninfo:
                    if not isinstance(z, dict): continue
                    try: sizes[str(int(z.get("id")))] = dict(d=round(float(z.get("diameter")), 2), type=str(z.get("type") or "")[:8])
                    except (TypeError, ValueError): continue
                if sizes: env["nozzle_info"] = sizes
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
                if pr.get("plate_idx") is not None: tr.state["print_plate"] = pr.get("plate_idx")
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


__all__ = ['mqtt_loop']

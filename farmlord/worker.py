"""Per-printer worker process: its HTTP API and background loops."""

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
from farmlord.spools import *  # noqa: F401,F403
from farmlord.views import *  # noqa: F401,F403
from farmlord.mqtt_client import *  # noqa: F401,F403
from farmlord.pages import *  # noqa: F401,F403

TEMP_HISTORY_FILE = P("temp_history.json")

TEMP_HISTORY = []          # [t_ms, nozzle, [[id, temp], ...], bed, chamber], oldest first, 30 minutes

def temp_history_loop():
    """Sample this printer's temperatures every 5 s, whether or not anyone has the page open."""
    global TEMP_HISTORY
    TEMP_HISTORY = [p for p in (jload(TEMP_HISTORY_FILE, []) or []) if isinstance(p, list) and len(p) == 5]
    n = 0
    while True:
        try:
            st = jload(STATE, {}); env = st.get("env") or {}
            if now() - st.get("t", 0) < 60 and env:
                nozzles = [[int(z.get("id", i)), z.get("temp")] for i, z in enumerate(env.get("nozzles") or []) if isinstance(z, dict)]
                TEMP_HISTORY.append([int(now() * 1000), env.get("nozzle"), nozzles, env.get("bed"), env.get("chamber")])
                cutoff = int((now() - 1800) * 1000)
                while TEMP_HISTORY and TEMP_HISTORY[0][0] < cutoff: TEMP_HISTORY.pop(0)
                n += 1
                if n % 12 == 0: jsave(TEMP_HISTORY_FILE, TEMP_HISTORY)
        except Exception as exc:
            print(f"[{iso()}] temperature history: {exc}", flush=True)
        time.sleep(5)

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
    rotate_snapshots()   # before the MQTT thread starts writing
    threading.Thread(target=mqtt_loop, daemon=True).start()
    threading.Thread(target=temp_history_loop, daemon=True).start()
    threading.Thread(target=lambda: (time.sleep(3), errdb_refresh_now()), daemon=True).start()
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
                try: plate_resume_tick()
                except Exception as e: print(f"[{iso()}] plate resume error {e}", flush=True)
                try: nozzle_resume_tick()
                except Exception as e: print(f"[{iso()}] nozzle ignore error {e}", flush=True)
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
            elif self.path.startswith("/api/temp_history"): self._send(200, json.dumps(TEMP_HISTORY[-360:]))
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
            elif self.path == "/api/read_rfid": r = do_read_rfid(body["ams_id"], body["slot_id"], pause_drying=True, user=True)
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


__all__ = ['TEMP_HISTORY_FILE', 'TEMP_HISTORY', 'temp_history_loop', 'serve', 'ORCA_USER_GLOB', 'SYNC_LOG',
           'SYNC_SKIP', '_orca_running', 'orca_filament_sync']

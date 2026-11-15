"""Printer camera: snapshot and MJPEG stream over the printer's local camera port."""

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


__all__ = ['ffmpeg_bin', 'cam_url', 'CAM', '_cam_reader', 'cam_start', 'cam_snapshot', 'cam_stream']

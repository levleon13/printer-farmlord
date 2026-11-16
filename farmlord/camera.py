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

CAM = {"proc": None, "clients": set(), "lock": _th.Lock(), "last": 0, "frames": 0, "jpeg": None, "snapshot_touch": 0, "snapshot_attempt": 0,
       # Full-rate video for the printer page: the printer's own H.264, remuxed (not re-encoded) to fragmented MP4.
       "vclients": set(), "init": None, "codec": None, "gop": [], "vframes": 0, "vlast": 0, "no_video": False, "started": 0,
       "err": ""}


def _err_reader(p):
    """Drain ffmpeg's stderr (a full pipe would stall it) and keep the last 2 KB for diagnostics."""
    try:
        for line in iter(p.stderr.readline, b""):
            CAM["err"] = (CAM["err"] + line.decode("utf-8", "replace"))[-2048:]
    except Exception:
        pass
VIDEO_QUEUE = 120          # fragments (~12 s) a viewer may fall behind before it is dropped and rejoins live
GOP_CAP = 16 * 1024 * 1024  # bytes kept so a new viewer can start at the latest keyframe


def _boxes(buf, start=0):
    """Complete top-level MP4 boxes in buf: (type, begin, end). Stops at the first incomplete one."""
    pos = start
    while pos + 8 <= len(buf):
        size = int.from_bytes(buf[pos:pos + 4], "big"); kind = bytes(buf[pos + 4:pos + 8])
        if size == 1:
            if pos + 16 > len(buf): return
            size = int.from_bytes(buf[pos + 8:pos + 16], "big")
        if size < 8 or pos + size > len(buf): return
        yield kind, pos, pos + size
        pos += size


def _child(buf, begin, end, kind, header=8):
    for k, b, e in _boxes(buf[begin + header:end]):
        if k == kind: return begin + header + b, begin + header + e
    return None


def fragment_is_key(moof):
    """True when the fragment's first sample is a sync sample (a keyframe), from tfhd/trun flags."""
    traf = _child(moof, 0, len(moof), b"traf")
    if not traf: return False
    default_flags = None
    tfhd = _child(moof, traf[0], traf[1], b"tfhd")
    if tfhd:
        b = tfhd[0]; flags = int.from_bytes(moof[b + 9:b + 12], "big"); off = b + 16
        for bit, size in ((0x1, 8), (0x2, 4), (0x8, 4), (0x10, 4)):
            if flags & bit: off += size
        if flags & 0x20: default_flags = int.from_bytes(moof[off:off + 4], "big")
    trun = _child(moof, traf[0], traf[1], b"trun")
    if not trun: return False
    b = trun[0]; flags = int.from_bytes(moof[b + 9:b + 12], "big"); off = b + 16
    if flags & 0x1: off += 4
    if flags & 0x4:
        first = int.from_bytes(moof[off:off + 4], "big")
    elif flags & 0x400:
        off += (4 if flags & 0x100 else 0) + (4 if flags & 0x200 else 0)
        first = int.from_bytes(moof[off:off + 4], "big")
    else:
        first = default_flags
    return first is not None and not (first >> 16) & 1          # sample_is_non_sync_sample == 0


def codec_string(init):
    """The MSE codec string (e.g. avc1.640029) from the init segment's avcC or hvcC box."""
    i = init.find(b"avcC")
    if i > 0: return "avc1." + init[i + 5:i + 8].hex()
    i = init.find(b"hvcC")
    if i > 0:
        c = init[i + 4:]; space = "ABC"[(c[1] >> 6) - 1] if c[1] >> 6 else ""
        profile, tier, level = c[1] & 31, "H" if c[1] & 32 else "L", c[12]
        compat = int.from_bytes(c[2:6], "big"); compat = int(f"{compat:032b}"[::-1], 2)
        cons = c[6:12].rstrip(b"\0")
        return "hvc1." + ".".join([f"{space}{profile}", f"{compat:x}", f"{tier}{level}"] + [f"{x:x}" for x in cons])
    return None


def _video_reader(fd, p):
    """Split the remuxed stream into the init segment and moof+mdat fragments; fan them out.
    A viewer that falls too far behind is closed (it reconnects at the next keyframe) — dropping
    single fragments would corrupt its picture."""
    buf, moof, used = bytearray(), None, 0
    try:
        while True:
            chunk = os.read(fd, 65536)
            if not chunk: break
            buf += chunk
            used = 0
            for kind, b, e in _boxes(buf):
                used = e
                if kind in (b"ftyp", b"moov"):
                    with CAM["lock"]:
                        CAM["init"] = (CAM["init"] or b"") + bytes(buf[b:e]) if kind == b"moov" else bytes(buf[b:e])
                        if kind == b"moov": CAM["codec"] = codec_string(CAM["init"])
                elif kind == b"moof":
                    moof = bytes(buf[b:e])
                elif kind == b"mdat" and moof is not None:
                    frag = moof + bytes(buf[b:e]); key = fragment_is_key(moof); moof = None
                    with CAM["lock"]:
                        gop = CAM["gop"]
                        if key or sum(map(len, gop)) > GOP_CAP: CAM["gop"] = gop = []
                        if key or gop: gop.append(frag)
                        CAM["vframes"] += 1; CAM["vlast"] = now()
                        qs = list(CAM["vclients"])
                    for q in qs:
                        try: q.put_nowait(frag)
                        except Exception:
                            with CAM["lock"]: CAM["vclients"].discard(q)
                            try:
                                while True: q.get_nowait()
                            except Exception: pass
                            q.put_nowait(None)
            if used: del buf[:used]
            if len(buf) > 8_000_000: buf.clear(); moof = None    # safety valve on a malformed stream
    finally:
        try: os.close(fd)
        except OSError: pass
        with CAM["lock"]:
            if CAM["proc"] is p or CAM["proc"] is None:
                CAM["init"] = None; CAM["codec"] = None; CAM["gop"] = []
            for q in list(CAM["vclients"]):
                try: q.put_nowait(None)
                except Exception: pass

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
        try: p.wait(5)                               # reap it: no zombie ffmpeg processes
        except Exception: pass
        with CAM["lock"]:
            # Video output is an addition: if ffmpeg with it died before giving any picture at all,
            # run picture-only from now on so the wall camera can never be broken by it.
            # Only ffmpeg's own MP4-muxer errors count ("[mp4 @ 0x…] …", "Could not write header"), never
            # a camera/network error that merely mentions a file name.
            err = CAM["err"]
            if (CAM["frames"] == 0 and not CAM["no_video"] and CAM["vframes"] == 0
                    and __import__("re").search(r"\[(?:mp4|mov) @ |Could not write header|Error writing trailer", err)):
                CAM["no_video"] = True
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
        test_input = os.environ.get("AMS_CAM_TEST_INPUT")       # local tests only: a file instead of the printer
        source = (__import__("shlex").split(test_input) if test_input else
                  ["-rtsp_transport", "tcp", "-tls_verify", "0", "-fflags", "nobuffer", "-flags", "low_delay", "-i", cam_url()])
        args = [exe, "-loglevel", "error", *source, "-map", "0:v:0", "-an", "-vf", "fps=8,scale=720:-2",
                "-q:v", "7", "-threads", "2", "-f", "mpjpeg", "-"]
        video_fd = None
        if not CAM["no_video"]:
            # Second output: the printer's H.264 as-is (30 fps), cut into ~100 ms MP4 fragments for the
            # printer page. Copying costs almost no CPU; the camera serves only one connection, so both
            # outputs come from this one ffmpeg.
            video_fd, write_fd = os.pipe()
            args += ["-map", "0:v:0", "-an", "-c:v", "copy", "-f", "mp4", "-movflags",
                     "frag_keyframe+empty_moov+default_base_moof", "-frag_duration", "100000",
                     "-flush_packets", "1", f"pipe:{write_fd}"]
        try:
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 pass_fds=(write_fd,) if video_fd is not None else ())
        except OSError as e:
            if video_fd is not None: os.close(video_fd); os.close(write_fd)
            return None, f"could not start ffmpeg: {e}"
        CAM["proc"] = p; CAM["frames"] = 0; CAM["vframes"] = 0; CAM["started"] = now(); CAM["err"] = ""
        _th.Thread(target=_err_reader, args=(p,), daemon=True).start()
        CAM["init"] = None; CAM["codec"] = None; CAM["gop"] = []
        _th.Thread(target=_cam_reader, args=(p,), daemon=True).start()
        if video_fd is not None:
            os.close(write_fd)
            _th.Thread(target=_video_reader, args=(video_fd, p), daemon=True).start()
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
                    viewers = CAM["clients"] or CAM["vclients"]
                    idle = not viewers and now()-CAM["snapshot_touch"] > 35
                    stalled = not viewers and now()-max(CAM["last"],CAM["snapshot_attempt"]) > 30
                    if not idle and not stalled: continue
                    CAM["proc"] = None
                try: proc.kill(); proc.wait(5)
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
        try: p.kill(); p.wait(5)
        except Exception: pass
        time.sleep(0.2)
        txt = CAM["err"][-600:].strip()
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
                        if CAM["clients"] or CAM["vclients"] or CAM["proc"] is not proc: return
                        if now()-CAM["snapshot_touch"] < 35: continue
                        CAM["proc"] = None
                        break
                try: proc.kill(); proc.wait(5)
                except Exception: pass
            _th.Thread(target=_idle_stop, args=(p,), daemon=True).start()



def cam_video(handler):
    """Serve the printer camera at its full frame rate as a fragmented MP4 stream (for MSE in the
    browser): the init segment, then fragments from the latest keyframe on, then live."""
    import queue
    if CAM["no_video"]:
        handler.send_response(503); handler.send_header("Content-Type", "text/plain"); handler.end_headers()
        handler.wfile.write(b"full-rate video unavailable on this printer"); return
    p, err = cam_start()
    if err:
        handler.send_response(503); handler.send_header("Content-Type", "text/plain"); handler.end_headers()
        handler.wfile.write(err.encode()); return
    deadline = now() + 20
    while now() < deadline:
        with CAM["lock"]:
            ready = CAM["init"] and CAM["codec"] and CAM["gop"]
        if ready or (CAM["proc"] is not p): break
        time.sleep(0.05)
    q = queue.Queue(maxsize=VIDEO_QUEUE)
    with CAM["lock"]:
        init, codec, gop = CAM["init"], CAM["codec"], list(CAM["gop"])
        if init and codec and gop: CAM["vclients"].add(q)
    if not (init and codec and gop):
        handler.send_response(503); handler.send_header("Content-Type", "text/plain"); handler.end_headers()
        handler.wfile.write(b"camera video not ready")
        _stop_when_idle(p); return
    handler.send_response(200)
    handler.send_header("Content-Type", "video/mp4"); handler.send_header("X-Codec", codec)
    handler.send_header("Cache-Control", "no-store"); handler.end_headers()
    try:
        handler.wfile.write(init)
        for frag in gop: handler.wfile.write(frag)
        handler.wfile.flush()
        while True:
            frag = q.get(timeout=15)
            if frag is None: break
            handler.wfile.write(frag); handler.wfile.flush()
    except Exception: pass
    finally:
        with CAM["lock"]:
            CAM["vclients"].discard(q)
        _stop_when_idle(p)


def _stop_when_idle(proc):
    """Stop the camera ffmpeg once nobody watches (no stream viewers and no recent wall snapshots), so
    the printer's single camera connection is never held open for nothing."""
    def wait():
        while True:
            time.sleep(30)
            with CAM["lock"]:
                if CAM["clients"] or CAM["vclients"] or CAM["proc"] is not proc: return
                if now()-CAM["snapshot_touch"] < 35: continue
                CAM["proc"] = None
                break
        try: proc.kill(); proc.wait(5)
        except Exception: pass
    with CAM["lock"]:
        if CAM["clients"] or CAM["vclients"]: return
    _th.Thread(target=wait, daemon=True).start()

__all__ = ['_err_reader', 'VIDEO_QUEUE', 'GOP_CAP', '_boxes', '_child', 'fragment_is_key', 'codec_string', '_video_reader', 'cam_video', '_stop_when_idle', 'ffmpeg_bin', 'cam_url', 'CAM', '_cam_reader', 'cam_start', 'cam_snapshot', 'cam_stream']

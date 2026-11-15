"""Automatic recovery for known-harmless printer stops (camera, plate offset, nozzle mismatch)."""

from farmlord.core import *  # noqa: F401,F403
from farmlord.materials import *  # noqa: F401,F403
from farmlord.ams import *  # noqa: F401,F403
from farmlord.errors import *  # noqa: F401,F403
from farmlord.tracker import *  # noqa: F401,F403
from farmlord.actions import *  # noqa: F401,F403
from farmlord.drying import *  # noqa: F401,F403
from farmlord.rfid import *  # noqa: F401,F403
from farmlord.controls import *  # noqa: F401,F403

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
            if hms_needs_person(h.get("attr"), h.get("code")): return
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

PLATE_OFFSET_ERRORS = {0x0C008009}                 # "Build plate localization marker was not found"

PLATE_OFFSET_HMS = {(0x0C000300, 0x0002000C)}      # "...check if the build plate is aligned correctly"

PLATE_RESUME = P("plate_resume.json")

def plate_resume_tick():
    """Resume a print paused only by the build-plate offset/alignment check (any printer).
    Missing, wrong or dirty plates are left alone. Every pause's error code is logged once."""
    if STANDBY: return
    st = jload(STATE, {})
    try: code = int(st.get("print_error") or 0)
    except (TypeError, ValueError): code = 0
    record = jload(PLATE_RESUME, {}) or {}
    if st.get("print_state") != "PAUSE":
        if record: jsave(PLATE_RESUME, {})
        return
    if STATUS.get("mqtt") != "connected" or not MQTT.get("client") or not MQTT.get("topic_req"): return
    connected = STATUS.get("since", 0)
    if any(not connected <= st.get(k, 0) <= now() or now() - st.get(k, 0) > 120
           for k in ("t", "print_state_t", "print_error_t")): return
    hms = []
    for h in st.get("hms") or []:
        try: hms.append((int(h.get("attr") or 0), int(h.get("code") or 0)))
        except (TypeError, ValueError): return
    seen = f"{_code4(code)} " + " ".join(f"{_code4(a)}_{_code4(c)}" for a, c in hms)
    if record.get("logged") != seen:
        append(EVENTS, dict(t=now(), time=iso(), kind="PAUSE_ERROR", msg=f"Print paused · error {seen.strip() or 'none'}"))
        record = dict(record, logged=seen); jsave(PLATE_RESUME, record)
    plate = code in PLATE_OFFSET_ERRORS or any(x in PLATE_OFFSET_HMS for x in hms)
    others = (code not in PLATE_OFFSET_ERRORS | {0}) or any(x not in PLATE_OFFSET_HMS and hms_needs_person(*x) for x in hms)
    if not plate or others: return
    if not record.get("first_seen"):
        jsave(PLATE_RESUME, dict(record, first_seen=now(), attempts=0)); return
    attempts = int(record.get("attempts", 0))
    if attempts >= 2: return
    if attempts == 0 and now() - record["first_seen"] < 4: return
    if attempts and now() - record.get("attempted_at", now()) < 12: return
    # Record the attempt before publishing, so a restart never repeats an ambiguous command.
    jsave(PLATE_RESUME, dict(record, attempts=attempts + 1, attempted_at=now()))
    try: result = do_print_control("resume")
    except Exception as exc: result = f"failed: {exc}"
    append(EVENTS, dict(t=now(), time=iso(), kind="AUTO_PLATE_RESUME",
                        msg=f"Build plate offset warning: automatic resume attempt {attempts + 1}/2 ({result})"))

NOZZLE_MISMATCH_ERRORS = {0x05008053, 0x05FE8053, 0x05FF8053}   # nozzle (both / left / right) not matched with slicing file

AUTO_IGNORE_ERRORS = {**{c: "nozzle vs slicing file" for c in NOZZLE_MISMATCH_ERRORS},
                      0x05008086: "toolhead camera lens dirty"}   # pauses Bambu answers with "Ignore this and Resume"

NOZZLE_RESUME = P("nozzle_resume.json")

def nozzle_resume_tick():
    """Answer the nozzle-mismatch and dirty-camera pauses with "Ignore this and Resume" (Bambu Studio's command_hms_ignore).
    The printer sometimes misreads the installed nozzle. At most two tries per error code and six per print job; nothing else is touched."""
    if STANDBY: return
    st = jload(STATE, {})
    try: code = int(st.get("print_error") or 0)
    except (TypeError, ValueError): code = 0
    record = jload(NOZZLE_RESUME, {}) or {}
    job = f"{st.get('job_id') or ''}|{st.get('job_started') or ''}|{st.get('print_name') or ''}"
    state = st.get("print_state")
    if record and (record.get("job") != job or state in ("IDLE", "FINISH", "FAILED")):
        record = {}; jsave(NOZZLE_RESUME, {})            # a new or finished job starts with a clean count
    if state != "PAUSE" or code not in AUTO_IGNORE_ERRORS:
        if record.get("code"): jsave(NOZZLE_RESUME, dict(job=job, tried=record.get("tried") or {}))
        return
    if STATUS.get("mqtt") != "connected" or not MQTT.get("client") or not MQTT.get("topic_req"): return
    connected = STATUS.get("since", 0)
    if any(not connected <= st.get(k, 0) <= now() or now() - st.get(k, 0) > 120
           for k in ("t", "print_state_t", "print_error_t")): return
    for h in st.get("hms") or []:
        try:
            if hms_needs_person(h.get("attr"), h.get("code")): return   # a fatal alert needs a person
        except (TypeError, ValueError, AttributeError): return
    tried = record.get("tried") or {}
    if tried.get(str(code), 0) >= 2 or sum(tried.values()) >= 6: return   # per job: 2 per code, 6 in all
    if record.get("code") != code:
        jsave(NOZZLE_RESUME, dict(job=job, code=code, first_seen=now(), attempts=0, tried=tried)); return
    attempts = int(record.get("attempts", 0))
    if attempts == 0 and now() - record.get("first_seen", now()) < 4: return
    if attempts and now() - record.get("attempted_at", now()) < 15: return
    # Record the attempt before publishing, so a restart never repeats an ambiguous command.
    tried = dict(tried); tried[str(code)] = tried.get(str(code), 0) + 1
    jsave(NOZZLE_RESUME, dict(record, attempts=attempts + 1, attempted_at=now(), tried=tried))
    try:
        MQTT["client"].publish(MQTT["topic_req"], json.dumps({"print": {
            "sequence_id": "0", "command": "ignore", "err": str(code), "param": "reserve", "job_id": str(st.get("job_id") or "")}}))
        result = "sent"
    except Exception as exc: result = f"failed: {exc}"
    side = {0x05FE8053: "left nozzle vs slicing file", 0x05FF8053: "right nozzle vs slicing file"}.get(code, AUTO_IGNORE_ERRORS[code])
    append(EVENTS, dict(t=now(), time=iso(), kind="AUTO_NOZZLE_IGNORE",
                        msg=f"Paused on {_code4(code)} ({side}): ignore and continue, attempt {tried[str(code)]}/2 ({result})"))


__all__ = ['CAMERA_INIT_ERROR', 'CAMERA_RESUME', 'CAMERA_WARNING_HOLD', 'camera_warning_status', 'camera_resume_tick',
           'PLATE_OFFSET_ERRORS', 'PLATE_OFFSET_HMS', 'PLATE_RESUME', 'plate_resume_tick', 'NOZZLE_MISMATCH_ERRORS',
           'AUTO_IGNORE_ERRORS', 'NOZZLE_RESUME', 'nozzle_resume_tick']

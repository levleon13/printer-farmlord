"""Printer and drying error codes: the HMS/print-error database and human-readable reasons."""

import threading

from farmlord.core import *  # noqa: F401,F403
from farmlord.materials import *  # noqa: F401,F403
from farmlord.ams import *  # noqa: F401,F403

def dry_stop_reason_text(code):
    if code == 8:
        return "External drying power was lost mid-cycle. The AMS reported its power adapter unavailable when heating stopped (code 8)."
    return f"Printer reported at the stop: {DRY_REASON_TEXT[code]} (code {code})."

def dry_stop_firmware_reason(before, after):
    """Describe a new AMS restriction at heater-off."""
    new = set((after or {}).get("dry_sf") or []) - set((before or {}).get("dry_sf") or [])
    for code in (8, 1, 5, 3, 10, 0, 2, 7, 4):
        if code in new:
            return code, dry_stop_reason_text(code)
    return None, None

DRY_WAIT_REASONS = {0, 1, 2, 4, 6, 7, 9}

DRY_REASON_TEXT = {
    0: "The printer is busy with another task", 1: "Waiting for drying power",
    2: "The AMS is loading, unloading, calibrating, or reading a tag", 3: "Filament remains in the AMS outlet; waiting for a safe retract",
    4: "The printer is starting a drying cycle", 5: "Drying is unavailable in the printer's current operating mode",
    6: "A drying cycle is already running", 7: "The printer or AMS is updating",
    8: "The AMS needs an external power adapter", 9: "The AMS is temporarily unavailable after stopping; waiting for it to become ready",
    10: "The printer reports filament at the AMS outlet; drying is waiting for clearance"}

def unresolved_print_fault(state):
    """A canceled job is terminal, but its 0300_400C echo is not a fault."""
    try: code = int(state.get("print_error") or 0)
    except (TypeError, ValueError): code = 0
    fatal_hms = any(((int(h.get("code") or 0) >> 16) & 0xFFFF) == 1 for h in (state.get("hms") or []))
    return (code not in (0, 0x0300400C)) or fatal_hms

def printer_error_signature(state):
    """What must change before a printer in an error state is worth asking again."""
    return [state.get("print_error") or 0,
            sorted(f"{h.get('attr')}:{h.get('code')}" for h in state.get("hms") or [] if isinstance(h, dict))]

def printer_alerts_text(state):
    """The printer's active HMS alerts, in Bambu's words where we know them."""
    out = []
    for h in state.get("hms") or []:
        if not isinstance(h, dict): continue
        try: full = f"{_code4(h.get('attr', 0))}_{_code4(h.get('code', 0))}"
        except (TypeError, ValueError): continue
        text = errdb()["hms"].get(full.replace("_", ""))
        out.append(f"HMS {full}" + (f" ({text.rstrip('.')})" if text else ""))
    return "; ".join(out)

def dry_refusal_reason(err, alerts=""):
    text = dry_err_text(err) if err else None
    why = (f"code {_code4(err)}" + (f": {text.rstrip('.')}" if text else "")) if err else "no reason given"
    return why + (f"; printer alert: {alerts}" if alerts else "")

def hms_needs_person(attr, code):
    """A fatal (level 1) alert blocks automatic resume, except an AMS / AMS HT tag-read failure
    (07xx_xxxx_0001_0086 / 18xx_xxxx_0001_0086): it only means a spool's RFID wasn't read."""
    attr, code = int(attr or 0), int(code or 0)
    if ((code >> 16) & 0xffff) != 1: return False
    return not (code == 0x00010086 and (attr >> 24) in (0x07, 0x18))

SEVERITY = {1: "fatal", 2: "serious", 3: "common", 4: "info"}

ERRDB_FILE = os.path.join(os.path.dirname(BASE), "error_texts.json")

ERRDB_URLS = {
    "print": "https://raw.githubusercontent.com/greghesp/ha-bambulab/37f2c20d55ec018a3a3072e12361d6eccdfe6f9f/custom_components/bambu_lab/pybambu/const_print_errors.py",
    "hms":   "https://raw.githubusercontent.com/greghesp/ha-bambulab/37f2c20d55ec018a3a3072e12361d6eccdfe6f9f/custom_components/bambu_lab/pybambu/const_hms_errors.py",
}

ERRDB = {"loaded": 0, "print": {}, "hms": {}}

_ERRDB_REFRESH = threading.Lock()


def errdb():
    """The error-code texts. Never waits on the network: a stale or missing cache is refreshed in
    the background and the texts known so far are returned (dashboard requests call this)."""
    if ERRDB["loaded"] and now() - ERRDB["loaded"] < 3600: return ERRDB
    cached = jload(ERRDB_FILE, None)
    if cached and now() - cached.get("fetched", 0) < 30 * 86400:
        ERRDB.update({k: cached.get(k, {}) for k in ("print", "hms")}, loaded=now()); return ERRDB
    if cached and not ERRDB["loaded"]:
        ERRDB.update({k: cached.get(k, {}) for k in ("print", "hms")})
    if _ERRDB_REFRESH.acquire(blocking=False):
        ERRDB["loaded"] = now()          # at most one refresh an hour, as before
        try:
            threading.Thread(target=_errdb_fetch, args=(cached,), daemon=True).start()
        except RuntimeError:              # no thread available: try again on a later call
            ERRDB["loaded"] = 0; _ERRDB_REFRESH.release()
    return ERRDB


def errdb_refresh_now():
    """Blocking refresh for the worker's start-up thread."""
    with _ERRDB_REFRESH:
        cached = jload(ERRDB_FILE, None)
        if cached and now() - cached.get("fetched", 0) < 30 * 86400:
            ERRDB.update({k: cached.get(k, {}) for k in ("print", "hms")}, loaded=now()); return ERRDB
        return _errdb_fetch(cached, locked=True)


def _errdb_fetch(cached, locked=False):
    try:
        return _errdb_download(cached)
    finally:
        if not locked: _ERRDB_REFRESH.release()


def _errdb_download(cached):
    import urllib.request, ssl, re as _re
    ctxs = [None]
    try:
        import certifi; ctxs.append(ssl.create_default_context(cafile=certifi.where()))
    except Exception: pass
    got = {"print": dict((cached or {}).get("print", {})), "hms": dict((cached or {}).get("hms", {}))}
    for kind, url in ERRDB_URLS.items():
        for ctx in ctxs:
            try:
                txt = urllib.request.urlopen(url, timeout=25, context=ctx).read().decode("utf-8", "replace")
                for k, v in _re.findall(r'"([0-9A-Fa-f]{8,16})"\s*:\s*"((?:[^"\\]|\\.)*)"', txt):
                    if v.strip(): got[kind][k.upper()] = v.strip()
                break
            except Exception: continue
    if any(got.values()): jsave(ERRDB_FILE, dict(fetched=now(), **got))
    ERRDB.update(got, loaded=now()); return ERRDB

def _code4(n):
    h = f"{int(n) & 0xFFFFFFFF:08X}"
    return h[:4] + "_" + h[4:]

def dry_err_text(code):
    """Bambu's own wording for a drying-command error code, if we have it."""
    try: c = int(code)
    except Exception: return None
    if not c: return None
    flat = f"{c:08X}"
    return errdb()["print"].get(flat) or errdb()["print"].get(flat.lower())

# The printer is telling us to wait, not that anything is broken. Retrying harder
# cannot help and unloading filament is actively wrong.
DRY_ERROR_STATE = 0x05024027  # "ERROR STATE": an active printer alert blocks every new command

DRY_WAIT_CODES = {
    0x03004013,  # printing cannot start while an AMS is drying
    0x05004040,  # no power for drying during printing
    0x05004041,  # only one AMS may dry during printing
    0x05004042,  # this AMS is in use for printing
    0x05004043,  # only one AMS may dry at a time
}

def error_data(st):
    """Show stopped-job faults and explicit AMS motor stalls in any job state.

    AMS health faults describe filament movement independently of print status.
    Preserve the detailed cause even when a generic print error is also present.
    """
    out = {"print_error": None, "hms": []}
    state = st.get("print_state"); stopped = state in ("PAUSE", "FAILED")
    db = errdb()
    try: pe = int(st.get("print_error") or 0)
    except (TypeError, ValueError): pe = 0
    # AMS / AMS HT assist-motor overload, including firmware unit variants.
    motor_print = (pe & 0xFF00FFFF) in (0x07008010, 0x18008010)
    fallback = ("The print is paused because the printer reported a fault. Check the printer screen for the cause and recovery steps."
                if state == "PAUSE" else
                "The print stopped because the printer reported a fault. Check the printer screen for the cause before restarting.")
    if pe and pe != 0x0300400C and (stopped or motor_print):
        code = _code4(pe); flat = code.replace("_", "")
        text = db["print"].get(flat) or ("The AMS motor is overloaded. The filament may be tangled or stuck." if motor_print else fallback)
        out["print_error"] = dict(code=code, text=text, process="filament" if motor_print else "print",
            url=f"https://e.bambulab.com/query.php?{flat}", blocking=True)
    for h in (st.get("hms") or []):
        try:
            attr = int(h.get("attr", 0)); code = int(h.get("code", 0))
            severity = SEVERITY.get((code >> 16) & 0xFFFF)
            # Serious AMS assist/slot motor overload is actionable even at idle.
            # Other common/info diagnostics remain outside the interruption panel.
            motor = (attr >> 24) in (0x07, 0x12, 0x18) and code == 0x00020002
            if not motor and not (stopped and severity == "fatal"): continue
            full = f"{_code4(attr)}_{_code4(code)}"; flat = full.replace("_", "")
            text = db["hms"].get(flat) or ("The AMS motor is overloaded. The filament may be tangled or stuck." if motor else fallback)
            out["hms"].append(dict(code=full, severity=severity, text=text, process="filament" if motor else "print",
                url=f"https://e.bambulab.com/query.php?{flat}", blocking=True))
        except (TypeError, ValueError): continue
    return out

# Stage labels follow Bambu Studio's get_stage_string(stg_cur).
PRINTER_STAGES = {
    0:"Printing", 1:"Leveling the bed", 2:"Preheating the bed", 3:"Calibrating vibration compensation",
    4:"Changing filament", 5:"Print paused", 6:"Paused — filament ran out", 7:"Heating the nozzle",
    8:"Calibrating filament flow", 9:"Scanning the bed", 10:"Inspecting the first layer",
    11:"Checking the build plate", 12:"Calibrating lidar", 13:"Homing the toolhead",
    14:"Cleaning the nozzle", 15:"Checking nozzle temperature", 16:"Print paused",
    17:"Paused — front cover open", 18:"Calibrating lidar", 19:"Calibrating flow ratio",
    20:"Paused — nozzle temperature fault", 21:"Paused — bed temperature fault", 22:"Unloading filament",
    23:"Paused — motion fault", 24:"Loading filament", 25:"Calibrating motor noise",
    26:"Paused — AMS disconnected", 27:"Paused — nozzle cooling fan fault",
    28:"Paused — chamber temperature fault", 29:"Cooling the chamber", 30:"Print paused",
    31:"Calibrating motor noise", 32:"Paused — filament buildup detected", 33:"Paused — cutter fault",
    34:"Paused — first layer needs attention", 35:"Paused — nozzle clog detected",
}


__all__ = ['_ERRDB_REFRESH', 'errdb_refresh_now', '_errdb_fetch', '_errdb_download', 'dry_stop_reason_text', 'dry_stop_firmware_reason', 'DRY_WAIT_REASONS', 'DRY_REASON_TEXT',
           'unresolved_print_fault', 'printer_error_signature', 'printer_alerts_text', 'dry_refusal_reason',
           'hms_needs_person', 'SEVERITY', 'ERRDB_FILE', 'ERRDB_URLS', 'ERRDB', 'errdb', '_code4', 'dry_err_text',
           'DRY_ERROR_STATE', 'DRY_WAIT_CODES', 'error_data', 'PRINTER_STAGES']

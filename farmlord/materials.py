"""Filament materials and drying rules: what each material needs and how dry a spool is."""

from farmlord.core import *  # noqa: F401,F403

# ---------------------------------------------------------------------------
# Drying targets. hours = full dry from "damp"; temp = minimum useful temp.
# rewet_h = hours at humidity index >=3 that undo ~1 hour of drying credit.
# Tune freely. Keys are matched as substrings of the AMS tray_type (upper).
# ---------------------------------------------------------------------------
MATERIALS = {
    "PA-CF": dict(temp=80, hours=10, rewet_h=2),
    "PA6":   dict(temp=80, hours=10, rewet_h=2),
    "PAHT":  dict(temp=80, hours=12, rewet_h=2),   # Bambu PAHT-CF: 80 °C, 12 h
    "PA":    dict(temp=80, hours=10, rewet_h=2),
    "PC-CF": dict(temp=80, hours=8,  rewet_h=4),   # Inherit the existing generic PC drying profile.
    "PC":    dict(temp=80, hours=8,  rewet_h=4),
    "PETG-CF": dict(temp=65, hours=8, rewet_h=6),
    "PETG":  dict(temp=65, hours=6,  rewet_h=8),
    "TPU-AMS": dict(temp=70, hours=8, rewet_h=4), # Bambu TPU for AMS (68D): Bambu spec 70 °C
    "TPU":   dict(temp=50, hours=8,  rewet_h=4),   # soft TPU: 45–55 °C, cap 55 — softens and jams above
    "ASA":   dict(temp=65, hours=4,  rewet_h=12),
    "ABS":   dict(temp=65, hours=4,  rewet_h=12),
    "PLA":   dict(temp=55, hours=4,  rewet_h=24),
    "PVA":   dict(temp=55, hours=8,  rewet_h=1),
}

DEFAULT_MAT = dict(temp=65, hours=6, rewet_h=8)

TEMP_TOLERANCE = 5        # degC below target still counts (AMS sensor is air temp)

HEATER_DELTA = 8          # AMS temp this far above ambient => heater considered ON

HUMID_BAD_RH = 35         # % RH above this counts as exposure (AMS 2 Pro / HT report raw RH)

HUMID_BAD_IDX = 2         # fallback for old AMS: index 5 = driest, 1 = wettest; <= this counts as exposure

SNAPSHOT_MIN_INTERVAL = 60  # s; also written on every detected change

def mat_rule(tray_type):
    t = (tray_type or "").upper()
    for k in sorted(MATERIALS, key=len, reverse=True):
        if k in t: return dict(MATERIALS[k], key=k)
    return dict(DEFAULT_MAT, key="?")

def spool_rule(s):
    """Material rule for a spool; a Bambu RFID drying temperature overrides the table's target.
    A name that is a more specific member of the same family wins (type PA-CF, name PAHT-CF -> PAHT)."""
    r = mat_rule(s.get("type"))
    n = mat_rule(s.get("name"))
    if n["key"] not in ("?", r["key"]) and r["key"] != "?" and n["key"][:2] == r["key"][:2] and n["key"] not in r["key"]: r = n
    if s.get("tag_dry_temp"): r = dict(r, temp=int(s["tag_dry_temp"]), tag=True)
    return r

def humid_exposed(a):
    """True when the AMS air is humid enough to re-wet filament."""
    if a.get("humidity_raw") is not None: return a["humidity_raw"] > HUMID_BAD_RH
    hi = a.get("humidity_idx")
    return hi is not None and hi <= HUMID_BAD_IDX

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def credit_rate(rule, temp):
    """Fraction of drying credit earned per hour at a given AMS air temperature."""
    if temp is None: return 0.0
    if temp >= rule["temp"] - 1: return 1.0          # 1 °C of sensor slack only
    if temp < 40: return 0.0
    return round(0.5 ** ((rule["temp"] - temp) / 15.0), 2)   # halves every 15 °C below target: 60→55 = 79 %, 80→65 = 50 %

def assess(s, unit_temp=None, heating=False):
    rule = spool_rule(s)
    need = rule["hours"]; have = s["dry_credit_h"]
    pct = min(100, int(100 * have / need)) if need else 100
    since = (now() - s["last_dry_end"]) / 3600 if s.get("last_dry_end") else None
    if pct >= 100: status = "DRY"
    elif pct >= 60: status = "MARGINAL"
    else: status = "NEEDS DRYING"
    if s.get("last_dry_temp") is not None and s["last_dry_temp"] < rule["temp"] - TEMP_TOLERANCE and status != "DRY":
        status += " (temp too low: %.0f < %d)" % (s["last_dry_temp"], rule["temp"])
    rate = credit_rate(rule, unit_temp) if heating else None
    eta = round(max(0, need - have) / rate, 1) if rate else None
    return dict(status=status, pct=pct, have_h=round(have, 1), need_h=need, target_temp=rule["temp"],
                exposure_h=round(s["exposure_h"], 1), hours_since_dry=None if since is None else round(since, 1),
                remaining_h=round(max(0, need - have), 1), rate=rate, eta_h=eta)

# drying presets shown in the dashboard: (temp °C, hours). AMS 2 Pro max 65 °C, AMS HT max 85 °C.
DRY_PRESETS = {"PETG-CF": (65, 8), "PETG": (65, 6), "PLA": (55, 4), "TPU": (60, 8), "ASA/ABS": (65, 4),
               "PA/PA-CF (HT)": (80, 10), "PC (HT)": (80, 8)}

AUTO_DEFAULT = {"auto": {}, "manual_hold_until": {}}

# safe max drying temp per material (so PLA next to PETG-CF is not deformed). Missing => rule temp.
SAFE_MAX = {"PLA": 55, "TPU": 55, "TPU-AMS": 70, "PETG": 65, "PETG-CF": 65, "ASA": 65, "ABS": 65, "PA": 80, "PA-CF": 80, "PA6": 80, "PAHT": 85, "PC-CF": 80, "PC": 80, "PVA": 55}

UNIT_MAX = lambda ams_id: 85 if int(ams_id) >= 128 else 65

# Conservative request cap while printing on the H2 fleet's shared supply.
# Firmware can reduce power/temperature further or refuse the request. Idle
# drying retains the material/unit limits; AMS HT has its own supply.
PRINT_SHARED_MAX = 55

AUTO_START_PCT = 85       # start a cycle when any spool's dryness falls below this

AUTO_MIN_HOURS, AUTO_MAX_HOURS = 2, 12

MANUAL_HOLD_H = 6         # after a manual Stop, auto-dry leaves that unit alone this long

AUTO_HUMIDITY_RH = 20     # ambient AMS humidity can call for drying even at 100% tracked dryness

AUTO_HUMIDITY_DWELL = 10 * 60

AUTO_HUMIDITY_COOLDOWN = 24 * 3600

# Bambu filament ids used to interpret Orca's saved slot types.
FILAMENT_IDS = {
    "PETG-CF":  ("GFG50", 240, 270), "PETG":    ("GFG99", 230, 260),
    "PLA":      ("GFA00", 190, 230), "PLA-CF":  ("GFA50", 200, 240),
    "TPU-AMS":  ("GFU02", 220, 240), "TPU":     ("GFU99", 200, 240),
    "PAHT":     ("GFN04", 260, 300), "PA-CF":   ("GFN98", 260, 300),
    "PA":       ("GFN99", 260, 300), "PA6":     ("GFN99", 260, 300),
    # Use the existing generic PC printer profile; keep PC last for reverse ID lookup.
    "PC-CF":    ("GFC99", 260, 300),
    "PC":       ("GFC99", 260, 300), "ASA":     ("GFB98", 240, 270),
    "ABS":      ("GFB99", 240, 270), "PVA":     ("GFS99", 190, 230),
}

ORCA_CONF = os.environ.get("AMS_ORCA_CONF", os.path.expanduser("~/Documents/OrcaSlicer/OrcaSlicer.conf"))

ID_TO_TYPE = {}

def orca_slot_type(ams_id, tray_id):
    """What Orca has recorded for this AMS slot (its Device tab assignment), if anything."""
    global ID_TO_TYPE
    if not ID_TO_TYPE:
        ID_TO_TYPE = {v[0]: k for k, v in FILAMENT_IDS.items() if v[0]}
    try:
        cfg = jload(CONFIG, {}); serial = cfg.get("serial")
        conf = jload(ORCA_CONF, {}) or {}
        ids = (conf.get("ams_filament_ids") or {}).get(serial, "")
        parts = ids.split(",")
        i = int(ams_id) * 4 + int(tray_id)
        code = parts[i].strip() if i < len(parts) else ""
        return ID_TO_TYPE.get(code) if code else None
    except Exception:
        return None

PALETTE = [("FFFFFF", "White"), ("000000", "Black"), ("7A7A7A", "Grey"), ("C12E1F", "Red"),
           ("1F5FC1", "Blue"), ("2E8B57", "Green"), ("FFD60A", "Yellow"), ("FF7A00", "Orange"),
           ("8B5A2B", "Brown"), ("E8E0D0", "Natural"), ("9B59B6", "Purple"), ("00B3B3", "Teal")]


__all__ = ['MATERIALS', 'DEFAULT_MAT', 'TEMP_TOLERANCE', 'HEATER_DELTA', 'HUMID_BAD_RH', 'HUMID_BAD_IDX',
           'SNAPSHOT_MIN_INTERVAL', 'mat_rule', 'spool_rule', 'humid_exposed', 'credit_rate', 'assess', 'DRY_PRESETS',
           'AUTO_DEFAULT', 'SAFE_MAX', 'UNIT_MAX', 'PRINT_SHARED_MAX', 'AUTO_START_PCT', 'AUTO_MIN_HOURS',
           'AUTO_MAX_HOURS', 'MANUAL_HOLD_H', 'AUTO_HUMIDITY_RH', 'AUTO_HUMIDITY_DWELL', 'AUTO_HUMIDITY_COOLDOWN',
           'FILAMENT_IDS', 'ORCA_CONF', 'ID_TO_TYPE', 'orca_slot_type', 'PALETTE']

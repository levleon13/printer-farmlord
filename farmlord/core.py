"""Shared plumbing: paths, JSON files, the event log, settings, HTTP helpers, MQTT/status globals."""

import json, os, ssl, sys, time, uuid, signal, hashlib, argparse, datetime as dt, math

import ipaddress, urllib.parse, gzip

STANDBY = os.environ.get("AMS_STANDBY", "1") != "0"

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # the repo / install folder
APP = os.path.join(ROOT, "ams_tracker.py")                            # the entry script workers are started with
BASE = os.environ.get("AMS_TRACKER_DIR") or ROOT


def code_files():
    """The entry script, every module and the web pages: changing any of them restarts the hub."""
    pkg = os.path.join(ROOT, "farmlord")
    found = [APP]
    for folder, _, names in os.walk(pkg):
        if "__pycache__" in folder: continue
        found += [os.path.join(folder, n) for n in sorted(names) if n.endswith((".py", ".html", ".b64"))]
    return found


def code_mtime():
    return max((os.path.getmtime(f) for f in code_files() if os.path.exists(f)), default=0)

P = lambda n: os.path.join(BASE, n)

CONFIG, SNAPS, EVENTS, SPOOLS, STATE = map(P, ["config.json", "snapshots.jsonl", "events.jsonl", "spools.json", "state.json"])

def now(): return time.time()

def iso(t=None): return dt.datetime.fromtimestamp(t or now()).strftime("%Y-%m-%d %H:%M:%S")

def jload(p, d):
    try:
        with open(p) as f: return json.load(f)
    except Exception: return d

def jsave(p, o):
    tmp = p + "." + uuid.uuid4().hex + ".tmp"
    # 0600: config files hold printer access codes and the dashboard password.
    with os.fdopen(os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as f: json.dump(o, f, indent=1)
    os.replace(tmp, p)

MAX_BODY = 256 * 1024

EXTRA_HOSTS = set()   # config.json "allowed_hosts": extra DNS names that may address the dashboard

LOGIN_FAILS = {}      # client ip -> [failures, locked_until]

def host_ok(host):
    """Refuse DNS rebinding: only IP literals, local/tailnet names and configured hosts may address this server."""
    h = (host or "").strip().lower()
    if h.startswith("["): h = h[1:].split("]", 1)[0]
    elif h.count(":") == 1: h = h.split(":", 1)[0]
    if not h: return False
    try: ipaddress.ip_address(h); return True
    except ValueError: pass
    return "." not in h or h.endswith((".local", ".localhost", ".ts.net", ".home.arpa", ".lan", ".internal")) or h in EXTRA_HOSTS

def json_post_ok(headers):
    """Other sites cannot send application/json here without a CORS preflight, which this server never grants."""
    if headers.get("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json": return False
    origin = headers.get("Origin")
    return not origin or urllib.parse.urlsplit(origin).netloc == headers.get("Host")

def read_body(headers, rfile, limit=MAX_BODY):
    try: n = int(headers.get("Content-Length") or 0)
    except ValueError: return None
    if n < 0 or n > limit: return None
    return rfile.read(n)

def printer_name_ok(base, name):
    """Printer names become directory names under BASE/printers; keep them there."""
    if not name or len(name) > 64 or name.startswith(".") or any(c in name for c in "/\\") or any(ord(c) < 32 for c in name): return False
    root = os.path.realpath(os.path.join(base, "printers"))
    return os.path.dirname(os.path.realpath(os.path.join(root, name))) == root

REPORT_URL = "https://github.com/levleon13/printer-farmlord/issues/new"

def scrub(text):
    """Blank out IPs, serial-like codes and home-folder names before anything leaves this machine."""
    import re
    text = re.sub(r"\b\d{1,3}(?:\.\d{1,3}){3}\b", "<ip>", str(text))
    text = re.sub(r"\b(?=[0-9A-Z]*\d)(?=[0-9A-Z]*[A-Z])[0-9A-Z]{12,16}\b", "<serial>", text)
    return re.sub(r"/(?:Users|home)/[^/\s]+", "~", text)[-1500:]

def report_url(title, details=""):
    """A prefilled GitHub issue; the user reviews it and clicks Submit, nothing is sent automatically."""
    import platform
    system = f"{platform.system()} {platform.release()} ({platform.machine()}) · Python {platform.python_version()}"
    return REPORT_URL + "?" + urllib.parse.urlencode(dict(template="problem.yml", title=title, details=scrub(details), system=system))

_GZIP_CACHE = {}

def gzip_body(b):
    """gzip a response body; big bodies (the pages) are cached by content hash."""
    if len(b) < 32768: return gzip.compress(b, 5)
    key = hashlib.sha1(b).digest()
    z = _GZIP_CACHE.get(key)
    if z is None:
        if len(_GZIP_CACHE) > 8: _GZIP_CACHE.clear()
        z = _GZIP_CACHE[key] = gzip.compress(b, 6)
    return z

def store_name():
    n = str(jload(CONFIG, {}).get("stock_store_name") or "")
    return "".join(c for c in n if c.isalnum() or c in " .&-")[:40]

def public_printer_config(c):
    """Never send LAN access codes to the browser."""
    c = dict(c); c["access_code_set"] = bool(c.pop("access_code", "")); return c

def append(p, o):
    with open(p, "a") as f: f.write(json.dumps(o) + "\n")

def fnum(x, d=None):
    try: return float(x)
    except Exception: return d

STATUS = {"mqtt": "starting", "since": now(), "error": None}

MQTT = {"client": None, "topic_req": None}

import threading as _th

LOCK = _th.RLock()

SETTINGS = P("settings.json")

def settings(): return jload(SETTINGS, {"auto": {}, "manual_hold_until": {}})

def save_settings(o): jsave(SETTINGS, o)


__all__ = ['code_files', 'code_mtime', 'json', 'os', 'ssl', 'sys', 'time', 'uuid', 'signal', 'hashlib', 'argparse', 'dt', 'math', 'ipaddress',
           'urllib', 'gzip', 'STANDBY', 'BASE', 'P', 'CONFIG', 'SNAPS', 'EVENTS', 'SPOOLS', 'STATE', 'now', 'iso',
           'jload', 'jsave', 'MAX_BODY', 'EXTRA_HOSTS', 'LOGIN_FAILS', 'host_ok', 'json_post_ok', 'read_body',
           'printer_name_ok', 'REPORT_URL', 'scrub', 'report_url', '_GZIP_CACHE', 'gzip_body', 'store_name',
           'public_printer_config', 'append', 'fnum', 'STATUS', 'MQTT', '_th', 'LOCK', 'SETTINGS', 'settings',
           'save_settings', 'ROOT', 'APP']

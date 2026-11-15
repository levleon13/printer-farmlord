"""Loads every module in dependency order, then links them into one shared namespace.

The modules were split out of a single file whose functions call each other freely. Each module
star-imports the ones before it; this link step then gives every module the names of the ones after
it too, so a function behaves exactly as it did in the single file. Names that are reassigned at
runtime (`global X`) are only ever read inside their own module, so no module holds a stale copy.
"""
import importlib

MODULES = ["core", "materials", "ams", "errors", "tracker", "actions", "drying", "rfid", "controls", "alerts",
           "camera", "spools", "views", "mqtt_client", "pages", "worker", "hub", "cli"]

loaded = [importlib.import_module(f"farmlord.{m}") for m in MODULES]
shared = {}
for mod in loaded:
    for name in mod.__all__:
        shared.setdefault(name, getattr(mod, name))
for mod in loaded:
    for name, value in shared.items():
        if name not in mod.__dict__:
            mod.__dict__[name] = value

from farmlord.cli import main  # noqa: E402

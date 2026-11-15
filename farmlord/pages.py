"""The dashboard pages (HTML/CSS/JS live in farmlord/web/)."""

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



WEB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
SETUP_LOGO = open(os.path.join(WEB, "logo.svg"), encoding="utf-8", newline="").read()


def web_page(name):
    """A page from farmlord/web/, with the shared logo filled in (byte-identical to the old inline strings)."""
    with open(os.path.join(WEB, name), encoding="utf-8", newline="") as f:
        return f.read().replace("__FARMLORD_LOGO__", SETUP_LOGO)


HTML = web_page("dashboard.html")

if STANDBY:
    HTML = HTML.replace('<body>', '<body><div style="padding:10px;text-align:center;background:#394335;color:#fff">Android preview · Controls are disabled until migration is complete</div>', 1)

MOVEMENT_HTML = web_page("movement.html")

# Both dashboards reload when either page changes; normal reloads never reuse old HTML.
UI_VERSION = hashlib.sha256((HTML + MOVEMENT_HTML).encode()).hexdigest()[:16]

HTML = HTML.replace("__AMS_UI_VERSION__", UI_VERSION)

MOVEMENT_HTML = MOVEMENT_HTML.replace("__AMS_UI_VERSION__", UI_VERSION)


SETUP_HTML = web_page("setup.html")

_PAGE_CACHE = {}


def dashboard_page(store):
    """The dashboard as bytes for this store name, built once instead of on every page load."""
    b = _PAGE_CACHE.get(store)
    if b is None:
        if len(_PAGE_CACHE) > 4: _PAGE_CACHE.clear()
        b = _PAGE_CACHE[store] = HTML.replace("configured store", store or "configured store").encode()
    return b
LOGIN_HTML = web_page("login.html")


__all__ = ['_PAGE_CACHE', 'dashboard_page', 'WEB', 'web_page', 'LOGIN_HTML', 'HTML', 'MOVEMENT_HTML', 'UI_VERSION', 'SETUP_LOGO', 'SETUP_HTML']

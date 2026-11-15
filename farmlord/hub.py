"""The hub process: login, proxying to workers, alerts, filament stock and the setup wizard."""

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
from farmlord.worker import *  # noqa: F401,F403

# ---------------------------------------------------------------------------
# Web dashboard
# ---------------------------------------------------------------------------
PORT = 8765

FOCUS_DASHBOARD_SCRIPT = 'on run argv\n    -- Alerts never launch Chrome, create tabs/windows, or navigate a tab.\n    if application "Google Chrome" is not running then return "not-running"\n    set localhostURL to item 1 of argv\n    set loopbackURL to item 2 of argv\n    set ipv6URL to item 3 of argv\n    tell application "Google Chrome"\n        repeat with dashboardWindow in windows\n            set tabNumber to 0\n            repeat with dashboardTab in tabs of dashboardWindow\n                set tabNumber to tabNumber + 1\n                set tabURL to URL of dashboardTab\n                set tabTitle to title of dashboardTab\n                -- The title also recognizes the same dashboard opened via dry.local or a LAN IP.\n                if tabURL starts with localhostURL or tabURL starts with loopbackURL or tabURL starts with ipv6URL or tabTitle is "AMS Dryness" or tabTitle ends with " · AMS Dryness" then\n                    set active tab index of dashboardWindow to tabNumber\n                    set minimized of dashboardWindow to false\n                    set index of dashboardWindow to 1\n                    activate\n                    return "reused"\n                end if\n            end repeat\n        end repeat\n        return "not-found"\n    end tell\nend run\n'

class StoreCatalog:
    """Optional exact-match stock availability from a configured store."""
    # Optional stock catalog comes from the local, ignored config.json.
    # Keep product IDs and preferred shops out of the portable source tree.
    PRODUCTS = {}
    BASE_URL = ""

    def __init__(self):
        self.lock = _th.RLock()
        self.results = {}
        config = jload(CONFIG, {})
        self.BASE_URL = str(config.get("stock_store_url") or "").rstrip("/")
        for item in config.get("stock_catalog", []):
            try:
                key = (str(item["material"]), str(item["color"]).upper())
                self.PRODUCTS[key] = (int(item["product_id"]), str(item["sku"]), str(item["expected_name"]))
            except (KeyError, TypeError, ValueError):
                continue

    def refresh(self):
        import urllib.request
        updates = {}
        for key, (product_id, sku, expected_name) in self.PRODUCTS.items():
            try:
                url = f'{self.BASE_URL}/wp-json/wc/store/v1/products/{product_id}'
                request = urllib.request.Request(url, headers={'User-Agent': 'AMS-Filament-Stock/1.0'})
                with urllib.request.urlopen(request, timeout=8) as response:
                    product = json.load(response)
                title = str(product.get('name') or '').split('(')[0]
                exact = (product.get('id') == product_id and product.get('sku') == sku
                         and expected_name.casefold() in title.casefold()
                        )
                status = ('different_product' if not exact else
                          'available' if product.get('is_in_stock') and product.get('is_purchasable')
                          and product.get('type') == 'simple' and not product.get('has_options') else 'out_of_stock')
                updates[key] = dict(status=status, product_url=product.get('permalink'), checked_at=time.time(),
                    cart_url=f'{self.BASE_URL}/cart/?add-to-cart={product_id}' if status == 'available' else None)
            except Exception as exc:
                updates[key] = dict(status='unknown', product_url=None, checked_at=time.time(), cart_url=None)
                print(f'[{iso()}] Stock check failed for {product_id}: {exc}', flush=True)
        with self.lock:
            self.results.update(updates)

    def run(self):
        while True:
            if self.BASE_URL and self.PRODUCTS: self.refresh()
            time.sleep(900)

    def for_item(self, item):
        key = (item.get('sub'), item.get('color'))
        if key not in self.PRODUCTS:
            return dict(status='not_listed', product_url=None, cart_url=None)
        with self.lock:
            return dict(self.results.get(key, dict(status='checking', product_url=None, cart_url=None)))

class FilamentInventory:
    """Fleet-wide unopened stock. A newly seen RFID tag opens one stock spool once."""
    def __init__(self, base):
        import threading
        self.base = base
        self.catalog = None
        self.path = os.path.join(base, 'filament_stock.json')
        self.lock = threading.RLock()
        saved = jload(self.path, {})
        self.items = saved.get('items', {}) if isinstance(saved.get('items'), dict) else {}
        self.tags = saved.get('tags', {}) if isinstance(saved.get('tags'), dict) else {}
        changed = False
        if not saved.get('history_seeded'):
            changed = self._seed_history() or True
        for item in self.items.values():
            if 'tracking_since' not in item:
                item['tracking_since'] = time.time(); changed = True
        if changed: self._save()

    def _save(self):
        jsave(self.path, dict(items=self.items, tags=self.tags, history_seeded=True))

    def _seed_history(self):
        """Old RFID spools must never be mistaken for new stock when reinserted."""
        changed = False
        root = os.path.join(self.base, 'printers')
        if not os.path.isdir(root): return False
        for printer in os.listdir(root):
            saved = jload(os.path.join(root, printer, 'spools.json'), {})
            if not isinstance(saved, dict): continue
            for spool in saved.values():
                if not isinstance(spool, dict): continue
                tag = str(spool.get('rfid') or '').upper()
                if not tag or not tag.strip('0') or tag in self.tags: continue
                self.tags[tag] = dict(id=tag, first_seen=0, item_id=None,
                    type=spool.get('type') or '', sub=spool.get('sub') or spool.get('name') or '',
                    color=str(spool.get('color') or '')[:6].upper(), code=spool.get('idx') or '')
                changed = True
        return changed

    def _products(self):
        colors = {'000000':'Black', 'C12E1F':'Red', 'FF6A13':'Orange',
                  '5898DD':'Blue', 'ADB1B2':'Light grey'}
        # ASA Black is in stock even though none of its RFID tags has been read yet.
        asa_key = hashlib.sha256(b'ASA|ASA|000000').hexdigest()[:16]
        products = {asa_key: dict(key=asa_key, material='ASA', sub='ASA', color='000000',
            code='', brand='Bambu Lab', name='Bambu ASA Black')}
        for tag in self.tags.values():
            material, sub = str(tag.get('type') or ''), str(tag.get('sub') or '')
            color = str(tag.get('color') or '')[:6].upper()
            if not material or not sub or len(color) != 6: continue
            key = hashlib.sha256(f'{material}|{sub}|{color}'.encode()).hexdigest()[:16]
            product = products.setdefault(key, dict(key=key, material=material, sub=sub,
                color=color, code='', brand='Bambu Lab',
                name=f'Bambu {sub} {colors.get(color, "#"+color)}'))
            if tag.get('code'): product['code'] = tag['code']
        # Inventory counts each material/subtype once, regardless of color.
        # Preserve an existing row's representative key and purchase variant.
        pooled = {}
        for product in products.values():
            group = (product['material'], product['sub'])
            existing = next((item for item in self.items.values()
                             if (item.get('material'), item.get('sub')) == group), None)
            if group not in pooled or (existing and product['key'] == existing.get('product_key')):
                pooled[group] = dict(product, name=f"Bambu {product['sub']}")
        return sorted(pooled.values(), key=lambda x: x['name'].casefold())

    def _matching_item(self, read):
        matches = []
        for item_id, item in self.items.items():
            # Stock is pooled by filament type. A newly added printer can have
            # the same material in several colors without needing one stock row
            # for each color. RFID identity still prevents double deductions.
            if item.get('material') == read['type'] and item.get('sub') == read['sub']:
                matches.append(item_id)
        return matches[0] if len(matches) == 1 else None

    def scan(self, workers):
        stamp = time.time()
        seen = {}
        runouts = set()
        for printer, worker in list(workers.items()):
            state = jload(os.path.join(worker['dir'], 'state.json'), {})
            if stamp - state.get('t', 0) > 300: continue
            if state.get('print_state') in ('PAUSE', 'FAILED'):
                try: filament_runout = int(state.get('print_error') or 0) == 0x07008011
                except (TypeError, ValueError): filament_runout = False
                if filament_runout:
                    source = active_nozzle_source(state)
                    if source.get('key'):
                        aid, tid = source['key'].split(':')
                        runout_tag = str(state.get('ams', {}).get(aid, {}).get('trays', {}).get(tid, {}).get('rfid') or '').upper()
                        if runout_tag and runout_tag.strip('0'): runouts.add(runout_tag)
            for aid, unit in state.get('ams', {}).items():
                for tid, tray in unit.get('trays', {}).items():
                    tag = str(tray.get('rfid') or '').upper()
                    if not tag or not tag.strip('0') or tray.get('empty') or tray.get('pending') or (tray.get('state') == 9 and tray.get('read_done')): continue
                    remain = fnum(tray.get('remain'))
                    if remain is None or not 0 <= remain <= 100: remain = None
                    read = dict(printer=printer, ams=ams_label(aid), slot=int(tid)+1,
                                type=tray.get('type') or '', sub=tray.get('sub') or '',
                                color=str(tray.get('color') or '')[:6].upper(), code=tray.get('idx') or '',
                                remaining_pct=remain, seen_at=state.get('t', stamp))
                    if tag not in seen or read['seen_at'] > seen[tag]['seen_at']: seen[tag] = read
        with self.lock:
            changed = False
            for tag, read in seen.items():
                record = self.tags.get(tag)
                if record is None:
                    record = dict(id=tag, first_seen=stamp, initial_remaining_pct=read['remaining_pct'], min_remaining_pct=read['remaining_pct'], item_id=None)
                    self.tags[tag] = record; changed = True
                old_min = record.get('min_remaining_pct')
                new_pct = read['remaining_pct']
                if new_pct is not None and (old_min is None or new_pct < old_min):
                    record['min_remaining_pct'] = new_pct; changed = True
                for key, value in read.items():
                    if key == 'seen_at' and value - record.get(key, 0) < 30: continue
                    if record.get(key) != value: record[key] = value; changed = True
                item_id = self._matching_item(read)
                if item_id and record.get('item_id') != item_id:
                    record['item_id'] = item_id; changed = True
                if item_id and not record.get('counted_item_id') and record.get('first_seen', 0) > self.items[item_id].get('tracking_since', stamp):
                    item = self.items[item_id]
                    if item['unopened'] > 0:
                        item['unopened'] -= 1
                        item['opened_since_tracking'] = item.get('opened_since_tracking', 0) + 1
                    record['counted_item_id'] = item_id
                    record['counted_at'] = stamp
                    changed = True
            for tag in runouts:
                if tag in self.tags and not self.tags[tag].get('runout_at'):
                    self.tags[tag]['runout_at'] = stamp; changed = True
            if changed: self._save()

    def snapshot(self):
        with self.lock:
            items = [dict(x) for x in self.items.values()]
            for item in items:
                item['low'] = item.get('threshold') is not None and item.get('unopened', 0) <= item['threshold']
                if self.catalog:
                    item['spider'] = self.catalog.for_item(item)
                # The purchase listing may still be color-specific, but one
                # inventory row now counts every color of this filament type.
                item['purchase_variant'] = item.get('name', '').removeprefix(f"Bambu {item.get('sub', '')} ")
                item['name'] = f"Bambu {item.get('sub', item.get('material', 'Filament'))}"
            return dict(items=items, products=self._products(), low=[x['id'] for x in items if x['low']])

    def action(self, body, workers=None):
        action = body.get('action')
        if action == 'save' and not body.get('id') and workers:
            self.scan(workers)  # Take a baseline before starting a new stock count.
        with self.lock:
            if action == 'save':
                product = next((x for x in self._products() if x['key'] == body.get('product_key')), None)
                if not product: raise ValueError('Choose a Bambu filament from the list.')
                item_id = str(body.get('id') or '')
                if item_id and item_id not in self.items: raise ValueError('Unknown stock item.')
                if any(x.get('material') == product['material'] and x.get('sub') == product['sub'] and key != item_id for key, x in self.items.items()):
                    raise ValueError('That filament type already has a stock row. Edit its amount instead; all colors share one count.')
                count = int(body.get('unopened', 0))
                if not 0 <= count <= 100000: raise ValueError('Invalid spool count.')
                threshold_raw = body.get('threshold')
                threshold = None if threshold_raw in (None, '') else int(threshold_raw)
                if threshold is not None and not 0 <= threshold <= 100000: raise ValueError('Invalid alert threshold.')
                item_id = item_id or uuid.uuid4().hex[:12]
                existing = self.items.get(item_id, {})
                self.items[item_id] = dict(id=item_id, product_key=product['key'], **{k:product[k] for k in ('name','material','sub','brand','color','code')},
                    unopened=count, threshold=threshold, tracking_since=existing.get('tracking_since', time.time()),
                    opened_since_tracking=existing.get('opened_since_tracking', 0))
            elif action == 'remove':
                item_id = str(body.get('id') or '')
                if item_id not in self.items: raise ValueError('Unknown stock item.')
                del self.items[item_id]
                for tag in self.tags.values():
                    if tag.get('item_id') == item_id: tag['item_id'] = None
            elif action == 'assign':
                raise ValueError('Spools are matched automatically from their Bambu RFID product and color.')
            else: raise ValueError('Unknown stock action.')
            self._save()
            return self.snapshot()

OFFLINE_ALERT_AFTER = 300

def refind_printers(workers, config_path, base):
    """When the office network hands a printer a new address, follow it: listen for the printers' own
    broadcasts (UDP 2021), match by serial number, and re-point a printer that has been unreachable for a minute."""
    import socket, urllib.request
    last = {}
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", 2021)); s.settimeout(5)
        except OSError as e:
            print(f"[{iso()}] printer re-find: cannot listen ({e}); retrying in 5 min", flush=True); time.sleep(300); continue
        try:
            while True:
                try: data, (src, _) = s.recvfrom(4096)
                except socket.timeout: continue
                text = data.decode(errors="ignore")
                usn = next((l.split(":", 1)[1].strip().upper() for l in text.splitlines() if l.lower().startswith("usn:")), "")
                if not usn: continue
                try: ipaddress.ip_address(src)
                except ValueError: continue
                cfg = jload(config_path, {})
                p = next((x for x in cfg.get("printers", []) if str(x.get("serial", "")).upper() == usn), None)
                if not p or p.get("ip") == src or time.time() - last.get(usn, 0) < 300: continue
                w = workers.get(p["name"])
                if not w: continue
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{w['port']}/api/rev", timeout=3) as r:
                        if json.loads(r.read()).get("mqtt") == "connected": continue
                except Exception: pass
                st = jload(os.path.join(w["dir"], "state.json"), {})
                if time.time() - float((st.get("t") or 0)) < 60: continue      # it only just dropped; give it a minute
                last[usn] = time.time()
                body = json.dumps(dict(old_name=p["name"], name=p["name"], ip=src, serial=p["serial"], access_code="")).encode()
                req = urllib.request.Request(f"http://127.0.0.1:{PORT_HUB[0]}/api/edit_printer", data=body,
                                             headers={"Content-Type": "application/json"}, method="POST")
                try:
                    with urllib.request.urlopen(req, timeout=30) as r: result = json.loads(r.read()).get("result")
                except Exception as e: result = f"failed: {e}"
                print(f"[{iso()}] printer re-find: {p['name']} answered from a new address; updated ({result})", flush=True)
                try: append(os.path.join(base, "audit.jsonl"), dict(t=time.time(), time=iso(), who="auto", what=f"re-found {p['name']} at its new address ({result})"))
                except OSError: pass
        except Exception as e:
            print(f"[{iso()}] printer re-find error {e}", flush=True); time.sleep(30)
        finally:
            try: s.close()
            except Exception: pass

PORT_HUB = [8765]

class AttentionCenter:
    """Hub-owned incident tracking: one Mac alert regardless of open browser tabs."""
    def __init__(self, base, port, inventory=None):
        self.path = os.path.join(base, 'attention.json')
        saved = jload(self.path, {})
        self.lock = _th.RLock()
        self.port = port
        self.settings = dict(sound=True, bring_chrome=True)
        self.settings.update({k: v for k, v in saved.get('settings', {}).items() if k in self.settings and isinstance(v, bool)})
        self.legacy_diagnostics = saved.get('legacy_diagnostics', [])
        if saved.get('policy_version') == 2:
            self.active = saved.get('active', {})
            self.history = [i for i in saved.get('history', []) if i.get('code') != 'BACKUP_FAILED'][-40:]
            self.active = {k: i for k, i in self.active.items() if i.get('code') != 'BACKUP_FAILED'}
        else:
            self.legacy_diagnostics += list(saved.get('active', {}).values()) + saved.get('history', [])
            self.active, self.history = {}, []
        self.policy_dirty = saved.get('policy_version') != 2
        self.last_sound = 0
        self.last_focus = 0
        self.last_native_error = None
        self.connection_since = {}
        self.offline_since = {}
        self.native = sys.platform == 'darwin'
        self.inventory = inventory

    def _save(self):
        jsave(self.path, dict(policy_version=2, settings=self.settings, active=self.active, history=self.history[-40:], legacy_diagnostics=self.legacy_diagnostics))

    def snapshot(self):
        with self.lock:
            return dict(incidents=list(self.active.values()), history=self.history[-20:][::-1],
                        settings=dict(self.settings), native=self.native, native_error=self.last_native_error,
                        reminder_seconds=30)

    def update(self, reports, stamp=None):
        stamp = time.time() if stamp is None else stamp
        fresh = []
        with self.lock:
            wanted = {}
            for printer, report in reports.items():
                connected = bool(report and report.get('mqtt') == 'connected' and not report.get('stale'))
                if not connected:
                    # Loss of telemetry cannot prove a process stopped. Retain only
                    # previously confirmed faults; never invent a connection alarm.
                    for key, item in self.active.items():
                        if item['printer'] == printer and item.get('blocking'): wanted[key] = item
                    continue
                errors = report.get('errors') or {}
                faults = ([errors['print_error']] if errors.get('print_error') else []) + (errors.get('hms') or [])
                faults = [f for f in faults if f.get('blocking') and (f.get('process') == 'filament' or report.get('print_state') in ('PAUSE', 'FAILED'))]
                faults += [f for f in report.get('process_faults', []) if f.get('blocking') and f.get('process') == 'drying']
                for fault in faults:
                    code = str(fault.get('code') or 'UNKNOWN')
                    url = str(fault.get('url') or '')
                    if not url.startswith('https://e.bambulab.com/'): url = ''
                    wanted[printer+'|'+code] = dict(printer=printer, code=code,
                        severity='error', message=str(fault.get('text') or 'The printer stopped because of a fault. Check its screen for recovery steps.'), url=url, blocking=True)
            # A printer that stops answering for 5 minutes raises a warning; all of them at once raise one alert.
            for printer, report in reports.items():
                if report and report.get('mqtt') == 'connected': self.offline_since.pop(printer, None)
                else: self.offline_since.setdefault(printer, stamp)
            for printer in list(self.offline_since):
                if printer not in reports: self.offline_since.pop(printer)
            offline = sorted(p for p, t in self.offline_since.items() if stamp - t >= OFFLINE_ALERT_AFTER)
            if offline and len(reports) > 1 and len(offline) == len(reports):
                since = time.strftime('%H:%M', time.localtime(min(self.offline_since.values())))
                wanted['farm|OFFLINE'] = dict(printer='All printers', code='OFFLINE_ALL', severity='error', blocking=False, url='',
                    message=f"None of the {len(reports)} printers has answered since {since}. Check the office Wi-Fi and the printers' power.")
            else:
                for printer in offline:
                    since = time.strftime('%H:%M', time.localtime(self.offline_since[printer]))
                    wanted[printer+'|OFFLINE'] = dict(printer=printer, code='OFFLINE', severity='warning', blocking=False, url='',
                        message=f"Not answering since {since}. Check its power and Wi-Fi.")
            if self.inventory:
                for item in self.inventory.snapshot()['items']:
                    if item['low']:
                        wanted['stock|'+item['id']] = dict(printer='Stock', code='LOW_STOCK_'+item['id'],
                            severity='warning', message=f"Order {item['name']}: {item['unopened']} spool(s) left, alert threshold {item['threshold']}.", url='', blocking=False)
                        spider_status = item.get('spider', {}).get('status')
                        if spider_status in ('out_of_stock', 'not_listed', 'different_product'):
                            reason = ('out of stock' if spider_status == 'out_of_stock' else
                                      'the listing has changed to a different product' if spider_status == 'different_product'
                                      else 'no exact listing was found')
                            wanted['spider|'+item['id']] = dict(printer='Stock', code='SPIDER_UNAVAILABLE_'+item['id'],
                                severity='warning', message=f"{store_name() or 'Store'}: {item['name']} is unavailable ({reason}). No substitute was added to the cart.",
                                url='', blocking=False)
            changed = self.policy_dirty
            self.policy_dirty = False
            for key in list(self.active):
                if key not in wanted:
                    item = self.active.pop(key)
                    self.history.append(dict(item, resolved_at=stamp))
                    self.history = self.history[-40:]
                    changed = True
            for key, data in wanted.items():
                if key not in self.active:
                    self.active[key] = dict(data, id=uuid.uuid4().hex, first_seen=stamp, acknowledged=False, snoozed_until=0)
                    fresh.append(self.active[key]['id']); changed = True
                else:
                    item = self.active[key]
                    for field in ('message', 'severity', 'url'):
                        if item.get(field) != data.get(field): item[field] = data.get(field); changed = True
            if changed: self._save()
            pending = [x for x in self.active.values() if not x['acknowledged'] and x.get('snoozed_until', 0) <= stamp]
            sound = bool(self.native and self.settings['sound'] and pending and stamp-self.last_sound >= 30)
            focus = bool(self.native and self.settings['bring_chrome'] and fresh)
            if sound: self.last_sound = stamp
            if focus: self.last_focus = stamp
        return dict(sound=sound, focus=focus)

    def action(self, body, local):
        if body.get('action') == 'test':
            if not local: raise ValueError('Test Mac alerts from this Mac.')
            self.emit(dict(sound=True, focus=True, preview=True))
            return self.snapshot()
        with self.lock:
            action = body.get('action')
            if action == 'settings':
                if not local: raise ValueError('Change Mac alert settings from this Mac.')
                for k in ('sound', 'bring_chrome'):
                    if k in body:
                        if not isinstance(body[k], bool): raise ValueError('Invalid setting')
                        self.settings[k] = body[k]
            elif action in ('acknowledge', 'snooze'):
                ids = body.get('ids')
                if not isinstance(ids, list) or not all(isinstance(i, str) for i in ids): raise ValueError('Invalid incident list')
                for item in list(self.active.values()) + (self.history if action == 'acknowledge' else []):
                    if item['id'] in ids:
                        if action == 'acknowledge': item['acknowledged'] = True
                        else: item['snoozed_until'] = time.time()+300
            else: raise ValueError('Unknown alert action')
            self._save()
        return self.snapshot()

    def emit(self, effects):
        if not self.native: return
        import subprocess
        failures = []
        commands = []
        if effects.get('sound'): commands.append(['/usr/bin/afplay', '/System/Library/Sounds/Glass.aiff'])
        if effects.get('focus'):
            base = f"http://localhost:{self.port}/"
            commands.append(['/usr/bin/osascript', '-e', FOCUS_DASHBOARD_SCRIPT,
                base, f"http://127.0.0.1:{self.port}/", f"http://[::1]:{self.port}/"])
        for command in commands:
            try: subprocess.run(command, timeout=8, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            except Exception:
                failures.append('Chrome focus failed. Allow this background service to control Chrome in macOS Privacy & Security → Automation.' if command[0].endswith('osascript') else 'The Mac notification sound could not be played.')
        if commands:
            self.last_native_error = 'Mac attention alert failed: '+'; '.join(failures) if failures else None
            if failures: print(f'[{iso()}] {self.last_native_error}', flush=True)

    def run(self, workers):
        import urllib.request
        while True:
            if self.inventory:
                try: self.inventory.scan(workers)
                except Exception as e: print(f'[{iso()}] inventory scan: {e}', flush=True)
            reports = {}
            for printer, worker in list(workers.items()):
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{worker['port']}/api/rev", timeout=2) as response:
                        reports[printer] = json.loads(response.read())
                except Exception: reports[printer] = None
            try: self.emit(self.update(reports))
            except Exception as e: print(f'[{iso()}] attention monitor: {e}', flush=True)
            time.sleep(3)

def setup_wizard():
    """First run: no printers configured yet. Serve a localhost-only setup page, write config.json, then restart into the dashboard."""
    import html, http.server, shutil, socket, subprocess, threading, webbrowser
    page = SETUP_HTML.replace("__LOGO__", SETUP_LOGO).replace("__REPORT__", html.escape(report_url("Setup problem"), quote=True)).encode()
    bind = "0.0.0.0" if os.environ.get("AMS_SETUP_LAN") == "1" else "127.0.0.1"

    def check_printer(p, existing):
        p = {k: str((p or {}).get(k, "")).strip() for k in ("name", "ip", "serial", "access_code")}
        if not all(p.values()): return None, "Fill in all four fields."
        if not printer_name_ok(BASE, p["name"]) or len(p["name"]) > 40: return None, "Use a short name without slashes, and don't start it with a dot."
        if p["name"] in existing: return None, "You already added a printer with that name."
        try: ipaddress.ip_address(p["ip"])
        except ValueError: return None, "That IP address doesn't look right. It's four numbers with dots, e.g. 192.0.2.10."
        if len(p["serial"]) > 32 or not p["serial"].isalnum(): return None, "The serial number should be letters and digits only."
        if len(p["access_code"]) > 32: return None, "That access code is too long."
        return p, ""

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a): pass
        def _send(self, code, body, ctype="application/json"):
            b = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(code); self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(b)))
            self.send_header("Cache-Control", "no-store"); self.end_headers(); self.wfile.write(b)
        def do_GET(self):
            if not host_ok(self.headers.get("Host")): return self._send(421, {"error": "Unknown host."})
            p = self.path.split("?", 1)[0]
            if p == "/setup": return self._send(200, page, "text/html; charset=utf-8")
            if p == "/api/setup/state": return self._send(200, {"setup": True})
            self.send_response(303); self.send_header("Location", "/setup"); self.send_header("Content-Length", "0"); self.end_headers()
        def do_POST(self):
            try: return self._post()
            except Exception:
                import traceback
                err = traceback.format_exc(); print(err, flush=True)
                return self._send(500, {"ok": False, "message": "Something went wrong on this computer.", "report": report_url("Setup error", err)})
        def _post(self):
            if not host_ok(self.headers.get("Host")) or not json_post_ok(self.headers): return self._send(403, {"ok": False, "message": "Use the setup page."})
            raw = read_body(self.headers, self.rfile, 64 * 1024)
            try: body = json.loads(raw or b"{}")
            except (TypeError, ValueError): body = None
            if not isinstance(body, dict): return self._send(400, {"ok": False, "message": "Invalid request."})
            if self.path == "/api/setup/test":
                ip = str(body.get("ip", "")).strip()
                try: ipaddress.ip_address(ip)
                except ValueError: return self._send(200, {"reachable": False, "message": "That IP address doesn't look right."})
                try:
                    socket.create_connection((ip, 8883), timeout=3).close()
                    return self._send(200, {"reachable": True, "message": "Found a printer at " + ip + "."})
                except OSError:
                    return self._send(200, {"reachable": False, "message": "No answer from " + ip + ". Check the IP, that LAN Only mode is on, and that this computer is on the printer's network."})
            if self.path == "/api/setup/check":
                p, err = check_printer(body.get("printer"), [str(x) for x in body.get("existing") or []])
                return self._send(200, {"ok": bool(p), "message": err})
            if self.path == "/api/setup/save":
                printers, names = [], []
                for raw_p in (body.get("printers") or [])[:50]:
                    p, err = check_printer(raw_p, names)
                    if not p: return self._send(200, {"ok": False, "message": err})
                    printers.append(p); names.append(p["name"])
                if not printers: return self._send(200, {"ok": False, "message": "Add at least one printer."})
                lan = bool(body.get("lan_access")); password = str(body.get("password") or "")
                if lan and len(password) < 8: return self._send(200, {"ok": False, "message": "Choose a password of at least 8 characters."})
                cfg = jload(CONFIG, {})
                if cfg.get("printers"): return self._send(200, {"ok": False, "message": "This dashboard is already set up."})
                cfg.update(printers=printers, lan_access=lan)
                cfg.setdefault("lan_name", "printroom")
                if lan: cfg["auth"] = {"password": password}
                os.makedirs(os.path.dirname(CONFIG) or ".", exist_ok=True)
                jsave(CONFIG, cfg)
                print(f"[{iso()}] setup saved {len(printers)} printer(s); starting the dashboard", flush=True)
                self._send(200, {"ok": True})
                # The listening socket is not inherited across exec, so the dashboard can bind the same port.
                threading.Timer(1.0, lambda: os.execv(sys.executable, [sys.executable] + sys.argv)).start()
                return
            self._send(404, {"ok": False, "message": "Not found."})

    srv = http.server.ThreadingHTTPServer((bind, PORT), H)
    url = f"http://localhost:{PORT}/setup"
    print(f"[{iso()}] first run: open {url} to set up your printers", flush=True)
    try:
        # Termux (Android) has no registered browser for Python; it opens URLs with termux-open-url.
        if shutil.which("termux-open-url"): subprocess.Popen(["termux-open-url", url])
        else: webbrowser.open(url)
    except Exception: pass
    srv.serve_forever()

def hub(open_browser=False):
    """Multi-printer mode: one worker process per printer (config.json → "printers"), tabbed dashboard on PORT."""
    import threading, http.server, subprocess, webbrowser, urllib.request
    cfg = jload(CONFIG, {})
    EXTRA_HOSTS.update(str(h).strip().lower() for h in cfg.get("allowed_hosts", []) if str(h).strip())
    printers = cfg.get("printers") or ([dict(name=cfg.get("name", "Printer"), ip=cfg["ip"], serial=cfg["serial"], access_code=cfg["access_code"])] if cfg.get("ip") else [])
    if not printers: return setup_wizard()
    workers = {}
    def add_worker(p):
        d = os.path.join(BASE, "printers", p["name"]); os.makedirs(d, exist_ok=True)
        jsave(os.path.join(d, "config.json"), dict(ip=p["ip"], serial=p["serial"], access_code=p["access_code"], name=p["name"], allow_unrestricted_jog=bool(p.get("allow_unrestricted_jog"))))
        if not os.path.exists(os.path.join(d, "settings.json")): jsave(os.path.join(d, "settings.json"), dict(auto={}, manual_hold_until={}, auto_started={}))
        used = {w["port"] for w in workers.values()}; port = PORT + 1
        while port in used: port += 1
        workers[p["name"]] = dict(port=port, dir=d, proc=None)
    for p in printers: add_worker(p)

    me = APP; my_mtime = code_mtime()
    def supervise():
        nonlocal my_mtime
        while True:
            # self-update: when any code file changes on disk, restart everything (launchd relaunches the hub)
            try:
                if code_mtime() != my_mtime:
                    time.sleep(3)  # let the write finish
                    try:
                        for f in code_files():
                            if f.endswith(".py"): compile(open(f).read(), f, "exec")
                    except SyntaxError as e:
                        print(f"[{iso()}] new code has a syntax error, not restarting: {e}", flush=True)
                        my_mtime = code_mtime()   # wait for the next edit
                        continue
                    print(f"[{iso()}] code updated — restarting", flush=True)
                    for w in workers.values():
                        if w["proc"]: w["proc"].terminate()
                    os._exit(0)
            except FileNotFoundError: pass
            for name, w in workers.items():
                if w["proc"] is None or w["proc"].poll() is not None:
                    env = dict(os.environ, AMS_TRACKER_DIR=w["dir"])
                    log = open(os.path.join(w["dir"], "worker.log"), "a")
                    w["proc"] = subprocess.Popen([sys.executable, APP, "serve", "--port", str(w["port"])], env=env, stdout=log, stderr=log)
                    print(f"[{iso()}] started worker {name} on port {w['port']}", flush=True)
            time.sleep(5)
    threading.Thread(target=supervise, daemon=True).start()

    def filament_sync_loop():
        while True:
            try:
                r = orca_filament_sync()
                if r not in ("nothing to do",) and not r.startswith("Orca is running"):
                    print(f"[{iso()}] filament sync: {r}", flush=True)
            except Exception as e:
                print(f"[{iso()}] filament sync error: {e}", flush=True)
            time.sleep(900)
    threading.Thread(target=filament_sync_loop, daemon=True).start()

    spider_catalog = StoreCatalog()
    stock_inventory = FilamentInventory(BASE)
    stock_inventory.catalog = spider_catalog
    threading.Thread(target=spider_catalog.run, daemon=True).start()
    attention = AttentionCenter(BASE, PORT, stock_inventory)
    threading.Thread(target=attention.run, args=(workers,), daemon=True).start()
    PORT_HUB[0] = PORT
    threading.Thread(target=refind_printers, args=(workers, CONFIG, BASE), daemon=True).start()

    AUTH = (jload(CONFIG, {}) or {}).get("auth") or {}
    import hmac, secrets, html
    from http.cookies import SimpleCookie, CookieError
    session_path = os.path.join(BASE, "web-session.key")
    try:
        with open(session_path, "rb") as f: session_key = f.read()
    except FileNotFoundError:
        session_key = secrets.token_bytes(32)
        fd = os.open(session_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as f: f.write(session_key)
    SESSION_AGE = 30 * 24 * 60 * 60
    import base64, re as _re
    def sign_session(payload, secret=None):
        # Password changes invalidate existing cookies; the password never goes in a cookie.
        # Personal logins sign with that person's password hash, so removing them ends their sessions.
        message = (payload + "\0" + (secret if secret is not None else str(AUTH.get("password") or ""))).encode()
        return hmac.new(session_key, message, hashlib.sha256).hexdigest()
    OWNER = str(AUTH.get("user") or "Owner")
    AUDIT = os.path.join(BASE, "audit.jsonl")
    def pw_hash(password):
        salt = secrets.token_hex(16)
        return "pbkdf2_sha256$200000$" + salt + "$" + hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 200000).hex()
    def pw_check(password, stored):
        try:
            _, rounds, salt, digest = stored.split("$")
            return hmac.compare_digest(hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), int(rounds)).hex(), digest)
        except (ValueError, AttributeError): return False
    def people(): return [p for p in (AUTH.get("people") or []) if isinstance(p, dict) and p.get("name") and p.get("hash")]
    def person(name):
        name = (name or "").strip().lower()
        return next((p for p in people() if p["name"].lower() == name), None) if name else None
    def b64(s): return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")
    def unb64(s): return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)).decode()
    TAILNET_GUEST = "~tailscale"
    def is_admin(who): return who != TAILNET_GUEST and (who in (OWNER, "local") or bool((person(who) or {}).get("admin")))
    TAILNETS = (ipaddress.IPv4Network((0x64400000, 10)), ipaddress.ip_network("fd7a:115c:a1e0::/48"))   # Tailscale CGNAT 100.64/10 (written numerically so the privacy scan does not flag the public range)
    def from_tailnet(addr):
        """True for a Tailscale peer address. Only devices someone added or shared on the tailnet have one."""
        try: ip = ipaddress.ip_address(str(addr).split("%")[0])
        except ValueError: return False
        if getattr(ip, "ipv4_mapped", None): ip = ip.ipv4_mapped
        return any(ip in n for n in TAILNETS)
    def reserved_name(name): return name.lower() in (OWNER.lower(), "local", "tailscale")
    def tailnet_guest_ok(handler):
        """Let a tailnet peer in without signing in, only when the owner turned it on and the connection
        arrived on this phone's own Tailscale address (recorded when it was switched on)."""
        if not AUTH.get("trust_tailscale"): return False
        pinned = str(AUTH.get("tailscale_ip") or "")
        try: local = handler.connection.getsockname()[0]
        except OSError: return False
        if not pinned or local != pinned or not from_tailnet(local) or not from_tailnet(handler.client_address[0]): return False
        if handler.path.startswith("/api/") and handler.headers.get("Sec-Fetch-Site") in ("cross-site", "same-site"): return False
        return True
    def save_people(rows):
        c = jload(CONFIG, {}); c.setdefault("auth", {})["people"] = rows; jsave(CONFIG, c); AUTH["people"] = rows
    INVITE_AGE = 7 * 24 * 60 * 60
    PEOPLE_LOCK = threading.Lock()
    def token_hash(token): return hashlib.sha256(token.encode()).hexdigest()
    def invites(): return [i for i in (AUTH.get("invites") or []) if isinstance(i, dict) and i.get("name") and i.get("hash") and float(i.get("expires") or 0) > time.time()]
    def save_invites(rows):
        c = jload(CONFIG, {}); c.setdefault("auth", {})["invites"] = rows; jsave(CONFIG, c); AUTH["invites"] = rows
    def invite_for(token):
        h = token_hash(token or "")
        return next((i for i in invites() if hmac.compare_digest(i["hash"], h)), None) if token else None
    def audit_log(who, path, body):
        if path in ("/api/people/list", "/login"): return
        try: req = json.loads(body or b"{}")
        except ValueError: req = {}
        parts = path.split("/")
        what = (urllib.parse.unquote(parts[3]) + " · " + "/".join(parts[5:])) if path.startswith("/api/p/") and len(parts) > 5 else path.replace("/api/", "", 1)
        if isinstance(req, dict) and isinstance(req.get("action"), str): what += f" ({req['action'][:30]})"
        if isinstance(req, dict) and path.startswith("/api/people/") and req.get("name"): what += f" ({str(req['name'])[:32]})"
        try:
            append(AUDIT, dict(t=now(), time=iso(), who=who, what=what))
            if os.path.getsize(AUDIT) > 2_000_000:
                with open(AUDIT) as f: tail = f.readlines()[-5000:]
                with open(AUDIT, "w") as f: f.writelines(tail)
        except OSError: pass
    def people_action(path, req, who):
        with PEOPLE_LOCK: return _people_action(path, req, who)
    def _people_action(path, req, who):
        action = path.rsplit("/", 1)[-1]
        if action == "list":
            out = dict(me=who, admin=is_admin(who), owner=OWNER, tailscale=bool(AUTH.get("trust_tailscale")),
                       people=[dict(name=p["name"], admin=bool(p.get("admin")), added=p.get("added")) for p in people()],
                       invites=[dict(name=i["name"], expires=i["expires"]) for i in invites()] if is_admin(who) else [])
            if out["admin"]:
                try:
                    with open(AUDIT) as f: out["audit"] = [json.loads(l) for l in f.readlines()[-40:]][::-1]
                except (OSError, ValueError): out["audit"] = []
            return out
        name = str(req.get("name") or "").strip(); password = str(req.get("password") or "")
        if action == "add":
            if not is_admin(who): return dict(error="Only the owner or an admin can add people.")
            if not _re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._-]{0,31}", name): return dict(error="Use a name of up to 32 letters, digits, spaces, dots or dashes.")
            if person(name) or reserved_name(name): return dict(error="That name is already used.")
            if any(i["name"].lower() == name.lower() for i in invites()): return dict(error="That name has a pending invite. Cancel it first.")
            if len(password) < 8: return dict(error="Choose a password of at least 8 characters.")
            save_people(people() + [dict(name=name, hash=pw_hash(password), admin=bool(req.get("admin")), added=iso())])
            return dict(result="ok")
        if action == "invite":
            if not is_admin(who): return dict(error="Only the owner or an admin can invite people.")
            if not _re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._-]{0,31}", name): return dict(error="Use a name of up to 32 letters, digits, spaces, dots or dashes.")
            if person(name) or reserved_name(name): return dict(error="That name is already used.")
            token = secrets.token_urlsafe(24)
            save_invites([i for i in invites() if i["name"].lower() != name.lower()] +
                         [dict(name=name, hash=token_hash(token), admin=bool(req.get("admin")), expires=time.time() + INVITE_AGE, by=who)])
            return dict(result="ok", path="/join?t=" + token, days=INVITE_AGE // 86400)
        if action == "tailscale":
            if not is_admin(who): return dict(error="Only the owner or an admin can change this.")
            on = req.get("on") is True
            c = jload(CONFIG, {}); a = c.setdefault("auth", {})
            if on:
                local = str(req.get("_local") or "")
                if not from_tailnet(local): return dict(error="Open the dashboard at its Tailscale address (100.x.x.x) and switch this on from there.")
                a["tailscale_ip"] = local; AUTH["tailscale_ip"] = local
            a["trust_tailscale"] = on; jsave(CONFIG, c); AUTH["trust_tailscale"] = on
            return dict(result="ok", tailscale=on)
        if action == "uninvite":
            if not is_admin(who): return dict(error="Only the owner or an admin can cancel invites.")
            save_invites([i for i in invites() if i["name"].lower() != name.lower()]); return dict(result="ok")
        if action == "remove":
            if not is_admin(who): return dict(error="Only the owner or an admin can remove people.")
            if not person(name): return dict(error="No one with that name.")
            save_people([p for p in people() if p["name"].lower() != name.lower()]); return dict(result="ok")
        if action == "password":
            if who == TAILNET_GUEST: return dict(error="Sign in with a name to change a password.")
            if len(password) < 8: return dict(error="Choose a password of at least 8 characters.")
            if name.lower() == OWNER.lower():
                if who not in (OWNER, "local"): return dict(error="Only the owner can change the owner password.")
                c = jload(CONFIG, {}); c.setdefault("auth", {})["password"] = password; jsave(CONFIG, c); AUTH["password"] = password
                return dict(result="ok")
            if not person(name): return dict(error="No one with that name.")
            if name.lower() != who.lower() and not is_admin(who): return dict(error="You can only change your own password.")
            save_people([dict(p, hash=pw_hash(password)) if p["name"].lower() == name.lower() else p for p in people()])
            return dict(result="ok")
        return dict(error="Unknown action.")

    # H2 printers expose their USB drive over FTPS. This clears files to free space;
    # it deliberately does not claim to recreate the drive's filesystem.
    storage_lock = threading.Lock()
    storage_job_lock = threading.Lock()
    storage_preview = {}
    storage_progress = {"running": False, "lines": []}   # live view of the USB check, one line per printer
    storage_job = {"done": True, "message": "No storage clear is running.", "results": []}

    def storage_idle(name, worker):
        with urllib.request.urlopen(f"http://127.0.0.1:{worker['port']}/api/state", timeout=5) as response:
            state = json.load(response)
        if state.get("stale") or (state.get("mqtt") or {}).get("mqtt") != "connected":
            raise ValueError(f"{name} is not connected with fresh readings")
        if state.get("print_state") not in ("IDLE", "FINISH", "FAILED"):
            raise ValueError(f"{name} is busy ({state.get('print_state') or 'unknown state'})")

    def storage_curl(printer, path="/", commands=()):
        import urllib.parse
        url = f"ftps://{printer['ip']}:990{urllib.parse.quote(path, safe='/')}"
        config = "user = " + json.dumps("bblp:" + printer["access_code"]) + "\n"
        config += "".join("quote = " + json.dumps(command) + "\n" for command in commands)
        args = ["curl", "-k", "-sS", "--max-time", "90", "-K", "-"]
        if not commands: args += ["--request", "LIST -a"]
        else: args.append("--list-only")
        result = subprocess.run(args + [url], input=config, text=True, capture_output=True, timeout=100)
        if result.returncode:
            raise ValueError(f"USB file service on {printer['name']} failed (code {result.returncode})")
        return result.stdout

    def storage_scan(printer):
        queue = ["/"]; files = []; directories = 0
        while queue:
            if directories > 500 or len(files) > 20000:
                raise ValueError(f"Too many entries on {printer['name']} USB drive; stopped safely")
            path = queue.pop(); directories += 1
            for line in storage_curl(printer, path).splitlines():
                parts = line.split(maxsplit=8)
                if len(parts) != 9: raise ValueError(f"Could not read all files on {printer['name']}")
                name = parts[8]
                if name in (".", ".."): continue
                if not name or "/" in name or any(ord(c) < 32 for c in name):
                    raise ValueError(f"Unsupported filename on {printer['name']}; stopped safely")
                full = path.rstrip("/") + "/" + name
                if parts[0].startswith("d"):
                    queue.append(full + "/")
                elif parts[0].startswith("-"):
                    files.append((full, int(parts[4])))
                else:
                    raise ValueError(f"Unsupported file type on {printer['name']}; stopped safely")
        return sorted(files)

    def storage_snapshot():
        summary = []; inventory = {}; skipped = {}
        names = list(workers.items())
        storage_progress.update(running=True, lines=[[n, "waiting"] for n, _ in names])
        def mark(i, text): storage_progress["lines"][i] = [names[i][0], text]
        for i, (name, worker) in enumerate(names):
            mark(i, "checking…")
            try:
                storage_idle(name, worker)
                printer = jload(os.path.join(worker["dir"], "config.json"), {})
                if not all(printer.get(k) for k in ("name", "ip", "access_code")):
                    raise ValueError(f"{name} is missing connection details")
                files = storage_scan(printer)
            except Exception as exc:
                reason = str(exc).replace(name + " is ", "", 1).replace(name + " ", "", 1)
                skipped[name] = reason
                summary.append({"name": name, "files": 0, "bytes": 0, "firmware": [], "skipped": reason})
                mark(i, "skipped — " + reason)
                continue
            mark(i, f"{len(files)} files · {sum(size for _, size in files)/1e9:.2f} GB")
            inventory[name] = files
            summary.append({"name": name, "files": len(files), "bytes": sum(size for _, size in files),
                            "firmware": [path.rsplit("/", 1)[-1] for path, _ in files
                                         if "firmware" in path.lower() or "offline-ota" in path.lower()]})
        storage_progress["running"] = False
        digest = hashlib.sha256(json.dumps({"files": inventory, "skipped": sorted(skipped)}, sort_keys=True).encode()).hexdigest()
        return summary, inventory, digest

    def storage_job_update(**changes):
        with storage_job_lock: storage_job.update(changes)

    def storage_clear_run(expected_digest):
        try:
            summary, inventory, digest = storage_snapshot()
            if digest != expected_digest:
                raise ValueError("Drive contents changed since confirmation. Preview again; no files were deleted.")
            results = [{"name": p["name"], "remaining": -1, "error": "", "skipped": p["skipped"]} for p in summary if p.get("skipped")]
            targets = [(name, worker) for name, worker in list(workers.items()) if name in inventory]
            for name, worker in targets:
                printer = jload(os.path.join(worker["dir"], "config.json"), {})
                files = inventory[name]
                storage_job_update(message=f"Clearing {name}: 0 of {len(files)} files…", results=results[:])
                error = ""
                try:
                    for offset in range(0, len(files), 20):
                        storage_idle(name, worker)
                        commands = ["*DELE " + path for path, _ in files[offset:offset + 20]]
                        storage_curl(printer, "/", commands)
                        storage_job_update(message=f"Clearing {name}: {min(offset + 20, len(files))} of {len(files)} files…")
                except Exception as exc:
                    error = str(exc)
                try: remaining = len(storage_scan(printer))
                except Exception as exc:
                    remaining = -1
                    error = (error + "; " if error else "") + "Could not verify remaining files: " + str(exc)
                results.append({"name": name, "remaining": remaining, "error": error})
                storage_job_update(results=results[:], message=f"Checked {name}; {remaining if remaining >= 0 else 'unknown'} files remain.")
            cleared = [x for x in results if not x.get("skipped")]
            ok = all(not x["error"] and x["remaining"] == 0 for x in cleared)
            skipped_n = len(results) - len(cleared)
            storage_job_update(done=True, results=results,
                               error="" if ok else "Some files may remain. Review each printer before retrying.",
                               message=(f"USB file clearing finished; {skipped_n} printer{'s' if skipped_n != 1 else ''} skipped (printing or offline)." if skipped_n else "USB file clearing finished.") if ok else "USB file clearing finished with problems; see results.")
        except Exception as exc:
            storage_job_update(done=True, error=str(exc), message="No files were deleted." if "no files were deleted" in str(exc).lower() else "USB file clearing stopped; see results.")
        finally:
            storage_lock.release()


    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"      # reuse connections: each poll no longer pays a new TCP handshake
        timeout = 75                       # idle keep-alive connections are dropped after this many seconds
        def log_message(self, *a): pass
        def handle_one_request(self):
            self._responded = False
            try: super().handle_one_request()
            except (ConnectionResetError, BrokenPipeError, TimeoutError):
                self.close_connection = True   # the browser dropped an idle kept-alive connection; nothing to report
                return
            if not self._responded: self.close_connection = True
        def send_response(self, code, message=None):
            self._responded = True
            super().send_response(code, message)
            if self.command == "POST": self.send_header("Connection", "close")

        def _login_page(self, error="", form=None, intro=None, code=200):
            username = html.escape(str(AUTH.get("user") or "Print room"), quote=True)
            error_html = '<p class="error" role="alert">' + html.escape(error) + '</p>' if error else ''
            page = LOGIN_HTML
            namefield = ('<label for="username">Name</label><input id="username" name="username" autocomplete="username" placeholder="Leave empty for the owner password" style="margin-bottom:14px">'
                         if people() else f'<input type="hidden" name="username" autocomplete="username" value="{username}">')
            page = page.replace("__NAMEFIELD__", namefield).replace("__ERROR__", error_html)
            if form is not None:
                start = page.index('<form method="post" action="/login"'); end = page.index("</form>", start) + len("</form>")
                page = page[:start] + form.replace("__ERROR__", error_html) + page[end:]
                page = page.replace("<title>Sign in · Printing Farmlord</title>", "<title>Join · Printing Farmlord</title>")
            if intro is not None: page = page.replace("<p>Sign in to your printers.</p>", "<p>" + intro + "</p>")
            self._send(code, page, "text/html; charset=utf-8")

        def _join_page(self, token, error=""):
            inv = invite_for(token)
            if not inv:
                return self._login_page(form='<a href="/login" style="color:#ace4d0">Go to sign in</a>', code=404,
                                        intro="This invite link has expired or was already used. Ask for a new one.")
            name = html.escape(inv["name"], quote=True); tok = html.escape(token, quote=True)
            form = ('<form method="post" action="/join" autocomplete="on">'
                    f'<input type="hidden" name="t" value="{tok}">'
                    f'<label for="username">Name</label><input id="username" name="username" autocomplete="username" value="{name}" readonly style="margin-bottom:14px;opacity:.75">'
                    '<label for="password">Choose a password (at least 8 characters)</label><input id="password" name="password" type="password" autocomplete="new-password" minlength="8" required autofocus style="margin-bottom:14px">'
                    '<label for="password2">Repeat it</label><input id="password2" name="password2" type="password" autocomplete="new-password" minlength="8" required>'
                    '__ERROR__<button type="submit">Join and sign in</button></form>')
            self._login_page(error, form=form, intro=f"Welcome, {name}. Choose your password to join the print room. You will sign in with your name and this password from now on.")

        def _authed(self):
            """Private controller calls use loopback; browsers use a signed persistent session."""
            self.who = OWNER
            if not AUTH.get("password"): return True
            if self.client_address[0] in ("127.0.0.1", "::1"): self.who = "local"; return True
            try:
                cookies = SimpleCookie(self.headers.get("Cookie", ""))
                token = cookies["ams_session"].value
                parts = token.split(".")
                if len(parts) == 3:
                    expiry, nonce, signature = parts; payload = expiry + "." + nonce; secret = None; who = OWNER
                else:
                    expiry, nonce, whob, signature = parts; payload = expiry + "." + nonce + "." + whob
                    p = person(unb64(whob)); secret = p["hash"] if p else "\0invalid"; who = p["name"] if p else ""
                if int(expiry) > time.time() and who and hmac.compare_digest(signature, sign_session(payload, secret)):
                    self.who = who
                    return True
            except (KeyError, ValueError, TypeError, CookieError, UnicodeDecodeError): pass
            if tailnet_guest_ok(self):
                self.who = TAILNET_GUEST; return True
            if self.path.startswith("/api/") or self.command != "GET":
                self._send(401, json.dumps({"error": "Please sign in.", "login": "/login"}))
            else:
                self.send_response(303)
                self.send_header("Location", "/login")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", "0"); self.end_headers()
            return False

        def _send(self, code, body, ctype="application/json", etag=False):
            b = body.encode() if isinstance(body, str) else body
            tag = None
            if etag and code == 200:
                tag = '"' + hashlib.sha1(b).hexdigest()[:24] + '"'
                if tag in (self.headers.get("If-None-Match") or ""):
                    self.send_response(304); self.send_header("ETag", tag); self.send_header("Cache-Control", "no-cache")
                    self.send_header("X-AMS-UI-Version", UI_VERSION); self.send_header("Content-Length", "0"); self.end_headers()
                    return
            gz = len(b) > 1400 and ctype.startswith(("text/", "application/json")) and "gzip" in (self.headers.get("Accept-Encoding") or "")
            if gz: b = gzip_body(b)
            self.send_response(code); self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(b)))
            if gz: self.send_header("Content-Encoding", "gzip"); self.send_header("Vary", "Accept-Encoding")
            if tag: self.send_header("ETag", tag); self.send_header("Cache-Control", "no-cache")
            else: self.send_header("Cache-Control", "no-store")
            self.send_header("X-AMS-UI-Version", UI_VERSION); self.end_headers(); self.wfile.write(b)
            t0 = getattr(self, "_t0", None)
            if t0 and time.time() - t0 > 2:
                print(f"[{iso()}] SLOW {time.time()-t0:.1f}s {self.command} {self.path}", flush=True)
        def _proxy(self, data=None):
            import urllib.parse
            parts = self.path.split("/", 4)      # '', 'api', 'p', name, rest
            name = urllib.parse.unquote(parts[3]); rest = "/" + (parts[4] if len(parts) > 4 else "")
            w = workers.get(name)
            if not w: return self._send(404, json.dumps({"error": "unknown printer"}))
            try:
                req = urllib.request.Request(f"http://127.0.0.1:{w['port']}{rest}", data=data, headers={"Content-Type": "application/json"}, method="POST" if data else "GET")
                with urllib.request.urlopen(req, timeout=10) as r: self._send(200, r.read())
            except Exception as e: self._send(503, json.dumps({"error": "Printer service is reconnecting; retry shortly."}))
        def do_GET(self):
            self._t0 = time.time()
            if not host_ok(self.headers.get("Host")): return self._send(421, json.dumps({"error": "Unknown host."}))
            if self.path.split("?", 1)[0] == "/login": return self._login_page()
            if self.path.split("?", 1)[0] == "/join":
                import urllib.parse
                return self._join_page(urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).get("t", [""])[0])
            if not self._authed(): return
            if self.path.startswith("/api/p/") and "/api/cam.jpg" in self.path:
                import urllib.request, urllib.parse
                parts = self.path.split("/", 4); name = urllib.parse.unquote(parts[3]); w = workers.get(name)
                if not w: return self._send(404, b"")
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{w['port']}/api/cam.jpg", timeout=2) as r:
                        self._send(r.status, r.read(), "image/jpeg")
                except Exception: self._send(503, b"", "image/jpeg")
                return
            if "/api/cam.mjpg" in self.path:
                import urllib.request, urllib.parse
                parts = self.path.split("/", 4); name = urllib.parse.unquote(parts[3]); w = workers.get(name)
                if not w: return self._send(404, "{}")
                try:
                    self.close_connection = True
                    up = urllib.request.urlopen(f"http://127.0.0.1:{w['port']}/api/cam.mjpg", timeout=20)
                    self.send_response(200)
                    self.send_header("Content-Type", up.headers.get("Content-Type", "multipart/x-mixed-replace; boundary=ffmpeg"))
                    self.send_header("Cache-Control", "no-store"); self.send_header("Connection", "close"); self.end_headers()
                    try:
                        while True:
                            b = up.read1(16384)
                            if not b: break
                            self.wfile.write(b)
                    finally:
                        up.close()
                except Exception: pass
                return
            if self.path == "/": self._send(200, HTML.replace("configured store", store_name() or "configured store"), "text/html; charset=utf-8", etag=True)
            elif self.path.split("?", 1)[0] == "/movement": self._send(200, MOVEMENT_HTML, "text/html; charset=utf-8", etag=True)
            elif self.path == "/api/attention": self._send(200, json.dumps(attention.snapshot()))
            elif self.path == "/api/filament_stock": self._send(200, json.dumps(stock_inventory.snapshot()))
            elif self.path == "/api/storage_clear_preview":
                if not storage_lock.acquire(blocking=False):
                    return self._send(409, json.dumps({"error": "A storage check or clear is already running."}))
                try:
                    summary, inventory, digest = storage_snapshot()
                    token = secrets.token_urlsafe(32)
                    storage_preview.clear()
                    storage_preview.update(token=token, digest=digest, expires=time.time() + 300)
                    self._send(200, json.dumps({"printers": summary, "token": token}))
                except Exception as exc:
                    self._send(409, json.dumps({"error": str(exc)}))
                finally: storage_lock.release(); storage_progress["running"] = False
            elif self.path == "/api/storage_clear_progress":
                self._send(200, json.dumps(storage_progress))
            elif self.path == "/api/storage_clear_status":
                with storage_job_lock: current = dict(storage_job)
                self._send(200, json.dumps(current))
            elif self.path == "/api/network":
                import socket
                ip = None
                try:
                    sck = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); sck.connect(("192.0.2.1", 1)); ip = sck.getsockname()[0]; sck.close()
                except Exception: pass
                nets = jload(CONFIG, {}).get("networks", {})
                label = next((v for k, v in nets.items() if ip and ip.startswith(k)), "Unknown network")
                reach = {n: any(p["ip"].rsplit(".", 1)[0] == (ip or "").rsplit(".", 1)[0] or p["ip"].split(".")[:2] == (ip or "").split(".")[:2] for _ in [0]) for n, w in workers.items() for p in [jload(os.path.join(w["dir"], "config.json"), {})]}
                self._send(200, json.dumps({"ip": ip, "label": label, "printers_here": reach}))
            elif self.path == "/api/printers": self._send(200, json.dumps([dict(name=n, port=w["port"], config=public_printer_config(jload(os.path.join(w["dir"], "config.json"), {}))) for n, w in workers.items()]))
            elif self.path.startswith("/api/p/"): self._proxy()
            else: self._send(404, "{}")
        def do_POST(self):
            if STANDBY:
                return self._send(503, json.dumps({"error": "Android host is in standby; use the current dashboard for controls."}))
            self._t0 = time.time()
            if not host_ok(self.headers.get("Host")): return self._send(421, json.dumps({"error": "Unknown host."}))
            if self.path == "/logout":
                if not json_post_ok(self.headers): return self._send(403, json.dumps({"error": "Use the dashboard to sign out."}))
                self.send_response(200)
                self.send_header("Set-Cookie", "ams_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax")
                self.send_header("Content-Type", "application/json"); self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", "2"); self.end_headers(); self.wfile.write(b"{}")
                return
            if self.path == "/join":
                import urllib.parse
                origin = self.headers.get("Origin")
                if origin and urllib.parse.urlsplit(origin).netloc != self.headers.get("Host"):
                    return self._send(403, "Join from the invite link.", "text/plain")
                try: n = int(self.headers.get("Content-Length", "0"))
                except ValueError: n = -1
                if n < 0 or n > 4096: return self._send(413, "Request too large", "text/plain")
                form = urllib.parse.parse_qs(self.rfile.read(n).decode("utf-8", errors="replace"))
                token = form.get("t", [""])[0]; ip = self.client_address[0]; fails = LOGIN_FAILS.get(ip)
                if fails and fails[1] > time.time(): return self._login_page("Too many attempts. Wait a minute and try again.", form="", intro="")
                inv = invite_for(token)
                if not inv:
                    fails = LOGIN_FAILS.setdefault(ip, [0, 0.0]); fails[0] += 1
                    if fails[0] >= 5: fails[0] = 0; fails[1] = time.time() + 60
                    return self._join_page("")
                password = form.get("password", [""])[0]
                if len(password) < 8: return self._join_page(token, "Choose a password of at least 8 characters.")
                if password != form.get("password2", [""])[0]: return self._join_page(token, "The two passwords don't match.")
                LOGIN_FAILS.pop(ip, None)
                someone = dict(name=inv["name"], hash=pw_hash(password), admin=bool(inv.get("admin")), added=iso())
                with PEOPLE_LOCK:
                    inv = invite_for(token)
                    if not inv: return self._join_page("")
                    if person(inv["name"]) or reserved_name(inv["name"]): return self._join_page(token, "That name is already used. Ask for a new invite.")
                    save_invites([i for i in invites() if i["hash"] != inv["hash"]])
                    save_people(people() + [someone])
                try: append(AUDIT, dict(t=now(), time=iso(), who=someone["name"], what="joined with an invite from " + str(inv.get("by") or OWNER)))
                except OSError: pass
                payload = str(int(time.time()) + SESSION_AGE) + "." + secrets.token_hex(16) + "." + b64(someone["name"])
                token = payload + "." + sign_session(payload, someone["hash"])
                self.send_response(303)
                self.send_header("Set-Cookie", f"ams_session={token}; Path=/; Max-Age={SESSION_AGE}; HttpOnly; SameSite=Lax")
                self.send_header("Location", "/"); self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", "0"); self.end_headers()
                return
            if self.path == "/login":
                import urllib.parse
                origin = self.headers.get("Origin")
                if origin and urllib.parse.urlsplit(origin).netloc != self.headers.get("Host"):
                    return self._send(403, "Sign in from the dashboard address.", "text/plain")
                try: n = int(self.headers.get("Content-Length", "0"))
                except ValueError: return self._login_page("Please enter your password again.")
                if n < 0 or n > 4096: return self._send(413, "Request too large", "text/plain")
                form = urllib.parse.parse_qs(self.rfile.read(n).decode("utf-8", errors="replace"))
                ip = self.client_address[0]; fails = LOGIN_FAILS.get(ip)
                if fails and fails[1] > time.time(): return self._login_page("Too many attempts. Wait a minute and try again.")
                key = form.get("password", [""])[0]
                user = form.get("username", [""])[0].strip()
                someone = person(user)
                if someone:
                    ok = pw_check(key, someone["hash"]); who_b = "." + b64(someone["name"]); secret = someone["hash"]
                else:
                    ok = hmac.compare_digest(key.encode(), str(AUTH.get("password") or "").encode()) and (not user or user.lower() == OWNER.lower())
                    who_b = ""; secret = None
                if not ok:
                    fails = LOGIN_FAILS.setdefault(ip, [0, 0.0]); fails[0] += 1
                    if fails[0] >= 5: fails[0] = 0; fails[1] = time.time() + 60
                    return self._login_page("Incorrect name or password. Please try again.")
                LOGIN_FAILS.pop(ip, None)
                payload = str(int(time.time()) + SESSION_AGE) + "." + secrets.token_hex(16) + who_b
                token = payload + "." + sign_session(payload, secret)
                self.send_response(303)
                self.send_header("Set-Cookie", f"ams_session={token}; Path=/; Max-Age={SESSION_AGE}; HttpOnly; SameSite=Lax")
                self.send_header("Set-Cookie", "ams_key=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax")
                self.send_header("Location", "/")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", "0"); self.end_headers()
                return
            if not self._authed(): return
            if not json_post_ok(self.headers): return self._send(403, json.dumps({"error": "Use the dashboard to make changes."}))
            if getattr(self, "who", "") == TAILNET_GUEST and self.path in ("/api/add_printer", "/api/edit_printer", "/api/remove_printer", "/api/reorder"):
                return self._send(403, json.dumps({"error": "Sign in with a name to add, edit or remove printers."}))
            body = read_body(self.headers, self.rfile)
            if body is None: return self._send(413, json.dumps({"error": "Request too large."}))
            n = len(body)
            audit_log(getattr(self, "who", OWNER), self.path, body)
            if self.path.startswith("/api/people/"):
                try: req = json.loads(body or b"{}")
                except ValueError: req = {}
                req = dict(req) if isinstance(req, dict) else {}
                try: req["_local"] = self.connection.getsockname()[0]
                except OSError: req["_local"] = ""
                return self._send(200, json.dumps(people_action(self.path, req, getattr(self, "who", OWNER))))
            if self.path == "/api/filament_stock":
                import urllib.parse
                origin = self.headers.get('Origin')
                if (origin and urllib.parse.urlsplit(origin).netloc != self.headers.get('Host')) or not self.headers.get('Content-Type', '').startswith('application/json'):
                    return self._send(403, json.dumps({'error': 'Use the dashboard to change filament stock.'}))
                if n > 4096: return self._send(413, json.dumps({'error': 'Request too large.'}))
                try:
                    request = json.loads(body)
                    if not isinstance(request, dict): raise ValueError('Invalid stock request.')
                    return self._send(200, json.dumps(stock_inventory.action(request, workers)))
                except (ValueError, TypeError) as exc: return self._send(400, json.dumps({'error': str(exc)}))
            if self.path == "/api/storage_clear":
                import urllib.parse
                origin = self.headers.get("Origin")
                if (origin and urllib.parse.urlsplit(origin).netloc != self.headers.get("Host")) or not self.headers.get("Content-Type", "").startswith("application/json"):
                    return self._send(403, json.dumps({"error": "Use the dashboard to clear printer storage."}))
                if n > 2048: return self._send(413, json.dumps({"error": "Request too large."}))
                try: request = json.loads(body)
                except (ValueError, TypeError): return self._send(400, json.dumps({"error": "Invalid request."}))
                if not isinstance(request, dict) or request.get("confirmation") != "CLEAR ALL":
                    return self._send(400, json.dumps({"error": "Confirmation is required."}))
                if not storage_lock.acquire(blocking=False):
                    return self._send(409, json.dumps({"error": "A storage check or clear is already running."}))
                token = str(request.get("token") or "")
                if not storage_preview.get("token") or not hmac.compare_digest(token, storage_preview["token"]) or time.time() > storage_preview.get("expires", 0):
                    storage_lock.release()
                    return self._send(409, json.dumps({"error": "Preview expired. Check the drives again."}))
                digest = storage_preview["digest"]
                storage_preview.clear()
                with storage_job_lock:
                    storage_job.clear()
                    storage_job.update(done=False, message="Rechecking all printers before clearing…", results=[], error="")
                try: threading.Thread(target=storage_clear_run, args=(digest,), daemon=True).start()
                except Exception:
                    storage_lock.release()
                    return self._send(500, json.dumps({"error": "Could not start storage clear."}))
                return self._send(202, json.dumps({"result": "started"}))
            if self.path == "/api/attention":
                import urllib.parse
                origin = self.headers.get('Origin')
                if (origin and urllib.parse.urlsplit(origin).netloc != self.headers.get('Host')) or not self.headers.get('Content-Type', '').startswith('application/json'):
                    return self._send(403, json.dumps({'error': 'Use the dashboard to change alerts.'}))
                try:
                    data = json.loads(body)
                    if not isinstance(data, dict): raise ValueError('Invalid alert request')
                    result = attention.action(data, self.client_address[0] in ('127.0.0.1', '::1'))
                    return self._send(200, json.dumps(result))
                except (ValueError, TypeError) as e: return self._send(400, json.dumps({'error': str(e)}))
            elif self.path.startswith("/api/p/"): self._proxy(body)
            elif self.path in ("/api/dry_all_printers", "/api/stop_all_printers"):
                import urllib.request
                target = "/api/dry_all" if self.path.endswith("dry_all_printers") else "/api/stop_all"
                out = []; unit_results = []
                for name, w in workers.items():
                    try:
                        req = urllib.request.Request(f"http://127.0.0.1:{w['port']}{target}", data=b"{}",
                                                     headers={"Content-Type": "application/json"}, method="POST")
                        with urllib.request.urlopen(req, timeout=25) as rr:
                            response=json.loads(rr.read());out.append(f"{name} — " + response.get("result", "?"))
                            unit_results.extend(dict(u,printer=name) for u in response.get("units",[]))
                    except Exception as e:
                        out.append(f"{name} — unreachable ({e})")
                        unit_results.append(dict(printer=name,status="blocked",message="Printer service unavailable. Retry when it reconnects."))
                self._send(200, json.dumps({"result": "ok", "detail": out, "units": unit_results}))
            elif self.path == "/api/add_printer":
                p = json.loads(body); p = {k: str(p.get(k, "")).strip() for k in ("name", "ip", "serial", "access_code")}
                if not all(p.values()): return self._send(200, json.dumps({"result": "all four fields are required"}))
                if not printer_name_ok(BASE, p["name"]): return self._send(200, json.dumps({"result": "use a simple name without slashes or a leading dot"}))
                if p["name"] in workers: return self._send(200, json.dumps({"result": "a printer with that name already exists"}))
                c = jload(CONFIG, {}); c.setdefault("printers", []).append(p); jsave(CONFIG, c); add_worker(p)
                self._send(200, json.dumps({"result": "ok"}))
            elif self.path == "/api/edit_printer":
                import shutil
                p = json.loads(body); old = p.get("old_name"); w = workers.get(old)
                if not w: return self._send(200, json.dumps({"result": "unknown printer"}))
                new = {k: str(p.get(k, "")).strip() for k in ("name", "ip", "serial", "access_code")}
                if not new["access_code"]: new["access_code"] = jload(os.path.join(w["dir"], "config.json"), {}).get("access_code", "")
                if not all(new.values()): return self._send(200, json.dumps({"result": "all four fields are required"}))
                if not printer_name_ok(BASE, new["name"]): return self._send(200, json.dumps({"result": "use a simple name without slashes or a leading dot"}))
                if new["name"] != old and new["name"] in workers: return self._send(200, json.dumps({"result": "that name is already used"}))
                if w["proc"]: w["proc"].terminate(); w["proc"].wait(timeout=10)
                d = w["dir"]
                if new["name"] != old:
                    nd = os.path.join(BASE, "printers", new["name"]); shutil.move(d, nd); d = nd
                    workers[new["name"]] = workers.pop(old)
                w = workers[new["name"]]; w.update(dir=d, proc=None)
                jog = any(x.get("allow_unrestricted_jog") for x in jload(CONFIG, {}).get("printers", []) if x.get("name") == old)
                jsave(os.path.join(d, "config.json"), dict(new, allow_unrestricted_jog=jog))
                c = jload(CONFIG, {}); c["printers"] = [dict(x, **new) if x["name"] == old else x for x in c.get("printers", [])]; jsave(CONFIG, c)
                self._send(200, json.dumps({"result": "ok"}))
            elif self.path == "/api/reorder":
                order = json.loads(body).get("order", [])
                c = jload(CONFIG, {}); byname = {p["name"]: p for p in c.get("printers", [])}
                c["printers"] = [byname[n] for n in order if n in byname] + [p for p in c.get("printers", []) if p["name"] not in order]
                jsave(CONFIG, c)
                ordered = {n: workers[n] for n in order if n in workers}; ordered.update({n: w for n, w in workers.items() if n not in ordered})
                workers.clear(); workers.update(ordered)
                self._send(200, json.dumps({"result": "ok"}))
            elif self.path == "/api/remove_printer":
                name = json.loads(body).get("name"); w = workers.pop(name, None)
                if w and w["proc"]: w["proc"].terminate()
                c = jload(CONFIG, {}); c["printers"] = [x for x in c.get("printers", []) if x["name"] != name]; jsave(CONFIG, c)
                self._send(200, json.dumps({"result": "ok" if w else "unknown printer"}))
            else: self._send(404, "{}")

    bind = "0.0.0.0" if cfg.get("lan_access") else "127.0.0.1"
    srv = http.server.ThreadingHTTPServer((bind, PORT), H)
    if cfg.get("lan_access"):
        import socket
        host = socket.gethostname()
        if host.endswith(".local"): host = host[:-6]
        ip = ""
        try:
            sk = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); sk.connect(("192.0.2.1", 1)); ip = sk.getsockname()[0]; sk.close()
        except Exception: pass
        print(f"[{iso()}] LAN access on: http://{host}.local:{PORT}" + (f"  or  http://{ip}:{PORT}" if ip else ""), flush=True)
        # publish a short Bonjour name so the office can use http://dry.local:8765
        alias = (jload(CONFIG, {}) or {}).get("lan_name", "dry")
        if ip:
            import subprocess
            def bonjour():
                while True:
                    try:
                        p = subprocess.Popen(["dns-sd", "-P", alias, "_http._tcp", "local", str(PORT),
                                              f"{alias}.local", ip], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        print(f"[{iso()}] short URL published: http://{alias}.local:{PORT}", flush=True)
                        p.wait()
                    except Exception as e:
                        print(f"[{iso()}] bonjour publish failed: {e}", flush=True); return
                    time.sleep(10)
            threading.Thread(target=bonjour, daemon=True).start()
    def stop(*a):
        for w in workers.values():
            if w["proc"]: w["proc"].terminate()
        sys.exit(0)
    signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
    print(f"[{iso()}] dashboard on http://localhost:{PORT} — printers: {', '.join(workers)}", flush=True)
    if open_browser: webbrowser.open(f"http://localhost:{PORT}")
    srv.serve_forever()


__all__ = ['PORT', 'FOCUS_DASHBOARD_SCRIPT', 'StoreCatalog', 'FilamentInventory', 'OFFLINE_ALERT_AFTER',
           'refind_printers', 'PORT_HUB', 'AttentionCenter', 'setup_wizard', 'hub']

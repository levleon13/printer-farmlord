<p align="center">
  <img src="docs/img/banner.webp" alt="Printer Farmlord: one dashboard for a room full of Bambu printers" width="100%">
</p>

<p align="center">
  <a href="#install-on-an-android-phone"><b>Install on Android</b></a> ·
  <a href="#install-on-a-mac-or-linux">Mac or Linux</a> ·
  <a href="#screenshots">Screenshots</a> ·
  <a href="https://github.com/levleon13/printer-farmlord/issues/new?template=problem.yml">Report a problem</a>
</p>

# Printer Farmlord

A self-hosted dashboard for a room full of Bambu Lab printers: cameras, AMS spools, automatic drying and print controls over your LAN, with no cloud.

## Screenshots

<img src="docs/screenshots/room-overview.webp" alt="Room overview: every printer with live camera, status and AMS slots" width="100%">

<img src="docs/screenshots/printer-desk.webp" alt="One printer: camera, temperatures, filament and drying, activity" width="100%">

<img src="docs/screenshots/filament-management.webp" alt="Filament management: stock with reorder warnings and drying per AMS" width="100%">

<table>
  <tr>
    <td width="50%"><img src="docs/screenshots/room-overview-tablet.webp" alt="Room overview on a tablet" width="100%"></td>
    <td width="50%"><img src="docs/screenshots/filament-management-tablet.webp" alt="Filament management on a tablet" width="100%"></td>
  </tr>
</table>

<table>
  <tr>
    <td align="center"><img src="docs/screenshots/setup-step1-phone.webp" alt="Setup page: adding printers" width="260"></td>
    <td align="center"><img src="docs/screenshots/setup-step2-phone.webp" alt="Setup page: password and network access" width="260"></td>
  </tr>
</table>

## What it does

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/docs-features-dark.webp">
  <img src="docs/img/docs-features-light.webp" alt="Drying: tracks and dries every spool automatically. Spools: RFID and manual tracking with history. Printers: live status, cameras and controls. Attention: one list of faults from every printer." width="100%">
</picture>

## Requirements

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/docs-requirements-dark.webp">
  <img src="docs/img/docs-requirements-light.webp" alt="Bambu printers in LAN Only and Developer Mode; a host on the same network (a spare Android phone, a Mac or Linux); Python 3.9+ and ffmpeg." width="100%">
</picture>

## Install on an Android phone

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/guide-1-intro-dark.webp">
  <img src="docs/img/guide-1-intro-light.webp" alt="Turn a spare Android phone into your print farm server: about 10 minutes, once." width="100%">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/guide-2-steps-1-2-dark.webp">
  <img src="docs/img/guide-2-steps-1-2-light.webp" alt="Step 1: on each printer turn on LAN Only and Developer Mode and note IP, serial and access code. Step 2: connect the phone to the printers' Wi-Fi and switch on Tailscale." width="100%">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/guide-3-steps-3-4-dark.webp">
  <img src="docs/img/guide-3-steps-3-4-light.webp" alt="Step 3: install Termux from F-Droid and set its battery use to Unrestricted. Step 4: paste the install line into Termux." width="100%">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/guide-4-steps-5-6-dark.webp">
  <img src="docs/img/guide-4-steps-5-6-light.webp" alt="Step 5: add your printers on the setup page and test each one. Step 6: allow other devices, set a password, finish." width="100%">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/guide-5-steps-7-8-dark.webp">
  <img src="docs/img/guide-5-steps-7-8-light.webp" alt="Step 7: open http://100.x.y.z:8765 from your own phone or laptop on Tailscale. Step 8, optional: start after a reboot with Termux:Boot." width="100%">
</picture>

**Step 4, paste into Termux:**

```sh
pkg install -y python ffmpeg git && git clone https://github.com/levleon13/printer-farmlord.git && cd printer-farmlord && ./scripts/start.sh
```

**Step 8 (optional), start after a reboot:**

```sh
mkdir -p ~/.termux/boot && printf '#!/data/data/com.termux/files/usr/bin/bash\ncd ~/printer-farmlord && ./scripts/start.sh\n' > ~/.termux/boot/farmlord && chmod +x ~/.termux/boot/farmlord
```

## Install on a Mac or Linux

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/docs-mac-linux-dark.webp">
  <img src="docs/img/docs-mac-linux-light.webp" alt="Run two commands, then finish setup in the browser at localhost:8765/setup." width="100%">
</picture>

```sh
git clone https://github.com/levleon13/printer-farmlord.git
cd printer-farmlord && ./scripts/start.sh
```

## Reaching it from other devices

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/docs-remote-dark.webp">
  <img src="docs/img/docs-remote-light.webp" alt="Printers and the host share the office network; your phone or laptop reaches the host over Tailscale. Never forward port 8765 on your router." width="100%">
</picture>

## Keeping it running

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/docs-running-dark.webp">
  <img src="docs/img/docs-running-light.webp" alt="start.sh in the foreground, Termux:Boot on Android, launchd or systemd on Mac and Linux, and a watch-only standby mode." width="100%">
</picture>

## Optional extras

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/docs-extras-dark.webp">
  <img src="docs/img/docs-extras-light.webp" alt="Bridge-anchor post-processor for OrcaSlicer, slicing from a different network, store availability for stock, and unrestricted jogging." width="100%">
</picture>

```sh
/usr/bin/python3 /path/to/printer-farmlord/fix_bridge_anchor.py --mode slow
```

```sh
PRINT_ROOM_SSH_HOST=your-ssh-alias ./scripts/connect_orca_remote.sh
```

## Security

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/docs-security-dark.webp">
  <img src="docs/img/docs-security-light.webp" alt="Access codes never reach the browser, signed sessions, sign-in throttling, changes only from the dashboard page, unknown host names refused. Keep it on a trusted network or behind Tailscale." width="100%">
</picture>

## Problems?

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/docs-problems-dark.webp">
  <img src="docs/img/docs-problems-light.webp" alt="If setup fails, use the Report this problem link: it opens a prefilled GitHub issue with private details removed, and setup problems are fixed every Friday." width="100%">
</picture>

[Report a problem](https://github.com/levleon13/printer-farmlord/issues/new?template=problem.yml)

## Privacy and safety

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/docs-privacy-safety-dark.webp">
  <img src="docs/img/docs-privacy-safety-light.webp" alt="Everything identifying stays in .runtime and never goes to git; the printer's own safety interlocks always take precedence." width="100%">
</picture>

## Project layout

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/docs-layout-dark.webp">
  <img src="docs/img/docs-layout-light.webp" alt="ams_tracker.py is the whole application; scripts/ holds start, install, setup and privacy tools; docs/ holds the README pictures." width="100%">
</picture>

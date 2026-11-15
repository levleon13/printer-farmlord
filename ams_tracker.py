#!/usr/bin/env python3
"""Printer Farmlord entry point. The code lives in farmlord/ (start with farmlord/app.py).

  ams_tracker.py run      the hub: dashboard on port 8765, one worker per printer
  ams_tracker.py serve    one printer's worker (the hub starts these)
  ams_tracker.py report   text summary of every slot and spool
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from farmlord.app import main  # noqa: E402

if __name__ == "__main__":
    main()

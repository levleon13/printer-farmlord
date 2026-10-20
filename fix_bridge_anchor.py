#!/usr/bin/env python3
"""
Orca Slicer post-processing script:
Fix the internal-bridge "anchor loop" -- closed loops of Internal solid
infill that Orca emits on the layer at/below an Internal Bridge, hugging
the walls, printed at full solid-infill speed over mostly air.

Shape-independent: works on any contour (rectangles, organic curves,
concave shapes, multiple islands). Detection is per-subpath:

  A subpath (travel-separated extrusion chain) inside an
  '; FEATURE: Internal solid infill' block qualifies iff:
    1. an '; FEATURE: Internal Bridge' exists on the same or next layer
    2. the subpath is CLOSED: end point returns to start (< CLOSE_TOL)
    3. it has no direction reversals: raster/zigzag infill reverses
       ~180 deg at every line end; a perimeter loop of any shape never
       reverses onto itself (reversal fraction <= REV_FRAC)
    4. sane segment count and length

Anything not matching all conditions is left untouched (fail-safe).

Modes (set MODE below or pass --mode):
  slow    keep loops, retime to SLOW_F and set ANCHOR_FAN, restore after
  delete  remove qualifying loops (and their lead-in travel) entirely

Requires relative extrusion (M83 / Orca-Bambu default); aborts untouched
otherwise. G2/G3 arcs (arc fitting on) are handled: the arc endpoint is
used as a chord for closure/reversal tests, and F is retimed like G1.
Works with both Arachne and Classic wall generators.

Orca: Others -> Post-processing scripts:
  /path/to/python3 /path/to/fix_bridge_anchor.py --mode slow
(Orca appends the gcode path as the last argument.)
"""

import math
import re
import sys

# ------------------------- settings -------------------------
MODE = "slow"        # "slow" or "delete"  (overridden by --mode)
SLOW_F = 2100        # anchor loop feedrate in slow mode (mm/min) = 35 mm/s
FAN_WRAP = True      # slow mode: override fan during loops, restore after
ANCHOR_FAN = 127     # fan for loops, 0-255 (127 = 50%). None = bridge fan
FAN_FALLBACK = 255   # if ANCHOR_FAN is None and no M106 in bridge block
CLOSE_TOL = 3.0      # mm, closure tolerance (Orca seam gap leaves ~1-1.5 mm open)
REV_FRAC = 0.10      # max fraction of vertices with ~180 deg reversal
REV_COS = -0.7       # cos threshold: reversal if cos(turn) < this (>134 deg)
MIN_SEGMENTS = 3
MAX_SEGMENTS = 4000  # organic contours can have many small segments
MIN_LEN = 5.0        # mm, ignore tiny loops (gap-fill-ish)
# -------------------------------------------------------------

G1_RE = re.compile(r"^G[0123]\s")   # G0/G1 lines and G2/G3 arcs (arc endpoint used as chord)
COORD_RE = re.compile(r"([XYZEF])(-?(?:\d+\.?\d*|\.\d+))")
FAN_RE = re.compile(r"^M106\s+S(\d+\.?\d*)\s*$")


def parse_move(line):
    if not G1_RE.match(line):
        return None
    d = {}
    for axis, val in COORD_RE.findall(line.split(";")[0]):
        d[axis] = float(val)
    return d


def is_closed_loop(pts):
    """pts: [(x,y), ...] polyline vertices incl. start. Closed, no reversals."""
    if math.dist(pts[0], pts[-1]) > CLOSE_TOL:
        return False
    n = len(pts) - 1  # segments
    if not (MIN_SEGMENTS <= n <= MAX_SEGMENTS):
        return False
    length = 0.0
    revs = 0
    verts = 0
    prev_dir = None
    for i in range(n):
        dx = pts[i + 1][0] - pts[i][0]
        dy = pts[i + 1][1] - pts[i][1]
        seg = math.hypot(dx, dy)
        if seg < 1e-6:
            continue
        length += seg
        d = (dx / seg, dy / seg)
        if prev_dir is not None:
            verts += 1
            if prev_dir[0] * d[0] + prev_dir[1] * d[1] < REV_COS:
                revs += 1
        prev_dir = d
    if length < MIN_LEN or verts == 0:
        return False
    return (revs / verts) <= REV_FRAC


def main():
    args = sys.argv[1:]
    mode = MODE
    if "--mode" in args:
        i = args.index("--mode")
        mode = args[i + 1].lower()
        del args[i:i + 2]
    if mode not in ("slow", "delete"):
        sys.exit("mode must be 'slow' or 'delete'")
    if not args:
        sys.exit("no gcode file argument")
    path = args[-1]

    with open(path, "r", encoding="utf-8", errors="surrogateescape") as f:
        lines = f.readlines()

    if not any("use_relative_e_distances = 1" in l or l.startswith("M83") for l in lines):
        print("fix_bridge_anchor: absolute E detected, leaving file untouched")
        return

    # ---- pass 1: layers, feature blocks, positions ----
    layer = 0
    blocks = []  # (feature, start, end, layer, entry_xy)
    cur = None   # [feature, start, layer, entry]
    x = y = None
    bridge_layers = set()
    for i, line in enumerate(lines):
        if line.startswith("; CHANGE_LAYER"):
            layer += 1
        if line.startswith("; FEATURE:"):
            if cur:
                blocks.append((cur[0], cur[1], i, cur[2], cur[3]))
            feat = line.split(":", 1)[1].strip()
            cur = [feat, i, layer, (x, y)]
            if feat == "Internal Bridge":
                bridge_layers.add(layer)
        mv = parse_move(line)
        if mv:
            x = mv.get("X", x)
            y = mv.get("Y", y)
    if cur:
        blocks.append((cur[0], cur[1], len(lines), cur[2], cur[3]))

    bridge_spans = [(s, e) for f, s, e, _, _ in blocks if f == "Internal Bridge"]

    def bridge_fan_after(pos):
        for bs, be in bridge_spans:
            if bs > pos:
                for i in range(bs, be):
                    m = FAN_RE.match(lines[i])
                    if m:
                        return m.group(1)
                break
        return str(FAN_FALLBACK)

    def fan_before(pos):
        for i in range(pos - 1, -1, -1):
            m = FAN_RE.match(lines[i])
            if m:
                return m.group(1)
        return None

    # ---- pass 2: per-subpath detection ----
    # subpath: (ext_line_idxs, pts, lead_travel_idx, block_start, layer,
    #           f_at_start, f_after_end)
    loops = []
    for feat, s, e, lay, entry in blocks:
        if feat != "Internal solid infill":
            continue
        if lay not in bridge_layers and (lay + 1) not in bridge_layers:
            continue
        if entry is None or entry[0] is None:
            continue
        px, py = entry
        cur_f = None
        # find F active at block entry: scan back for last F
        for j in range(s - 1, -1, -1):
            mv = parse_move(lines[j])
            if mv and "F" in mv:
                cur_f = mv["F"]
                break
        sub_ext, sub_pts = [], [(px, py)]
        lead_travel = None
        pending_travel = None
        f_start = cur_f

        def flush(end_f):
            if sub_ext:
                loops.append({
                    "ext": list(sub_ext), "pts": list(sub_pts),
                    "lead": lead_travel, "block": s, "layer": lay,
                    "f_start": f_start, "f_end": end_f,
                })

        for i in range(s, e):
            mv = parse_move(lines[i])
            if not mv:
                continue
            f_before = cur_f
            if "F" in mv:
                cur_f = mv["F"]
            nx, ny = mv.get("X", px), mv.get("Y", py)
            if mv.get("E", 0) > 0 and ("X" in mv or "Y" in mv):
                if not sub_ext:
                    f_start = cur_f
                    lead_travel = pending_travel
                    sub_pts = [(px, py)]
                sub_ext.append(i)
                sub_pts.append((nx, ny))
            elif ("X" in mv or "Y" in mv):
                flush(f_before)
                sub_ext, sub_pts = [], []
                pending_travel = i
            px, py = nx, ny
        flush(cur_f)
        # find lead travel for first subpath if it was before the block
        # (pending_travel starts None -> locate last travel before block)
    # attach pre-block lead travel where missing
    for lp in loops:
        if lp["lead"] is None:
            j = lp["block"] - 1
            while j > 0:
                mv = parse_move(lines[j])
                if mv:
                    if ("X" in mv or "Y" in mv) and "E" not in mv:
                        lp["lead"] = j
                    break
                if not lines[j].startswith(";"):
                    break
                j -= 1

    targets = [lp for lp in loops if is_closed_loop(lp["pts"])]
    if not targets:
        print("fix_bridge_anchor: no anchor loops found, file unchanged")
        return

    # ---- pass 3: rewrite ----
    kill = set()
    inserts = []  # (index, text)
    marked_blocks = set()
    for lp in targets:
        first, last = lp["ext"][0], lp["ext"][-1]
        lay = lp["layer"]
        if mode == "slow":
            # retime: rewrite F on ext lines; ensure F at start; restore after
            # rewrite F on every G1 in the loop span, including the
            # standalone "G1 F..." lines Orca emits on Arachne width changes
            for i in range(first, last + 1):
                if "F" in (parse_move(lines[i]) or {}):
                    lines[i] = re.sub(r"F\d*\.?\d+", f"F{SLOW_F}", lines[i])
            inserts.append((first, f"G1 F{SLOW_F} ; anchor loop slow\n"))
            if lp["f_end"] is not None:
                inserts.append((last + 1, f"G1 F{lp['f_end']:g} ; restore speed\n"))
            if FAN_WRAP:
                prev = fan_before(lp["block"])
                if prev is not None:
                    bf = str(ANCHOR_FAN) if ANCHOR_FAN is not None else \
                        bridge_fan_after(lp["block"])
                    inserts.append((first, f"M106 S{bf} ; anchor loop fan\n"))
                    inserts.append((last + 1, f"M106 S{prev} ; restore fan\n"))
            if lp["block"] not in marked_blocks:
                lines[lp["block"]] = lines[lp["block"]].rstrip("\n") + \
                    " ; anchor loops retimed by fix_bridge_anchor\n"
                marked_blocks.add(lp["block"])
            print(f"fix_bridge_anchor: retimed anchor loop "
                  f"({len(lp['ext'])} segs) on layer {lay} to F{SLOW_F}")
        else:
            kill.update(lp["ext"])
            if lp["lead"] is not None:
                kill.add(lp["lead"])
            print(f"fix_bridge_anchor: deleted anchor loop "
                  f"({len(lp['ext'])} segs) on layer {lay}")

    if mode == "delete":
        # if a block lost ALL its extrusion, drop its comments/F-only lines too
        for feat, s, e, lay, entry in blocks:
            if feat != "Internal solid infill":
                continue
            remaining = False
            for i in range(s, e):
                if i in kill:
                    continue
                mv = parse_move(lines[i])
                if mv and mv.get("E", 0) > 0 and ("X" in mv or "Y" in mv):
                    remaining = True
                    break
            if not remaining and any(i in kill for i in range(s, e)):
                for i in range(s, e):
                    mv = parse_move(lines[i])
                    if mv and ("X" in mv or "Y" in mv) and "E" not in mv:
                        continue  # keep travels
                    kill.add(i)
        lines = [l for i, l in enumerate(lines) if i not in kill]
    else:
        for idx, text in sorted(inserts, key=lambda t: t[0], reverse=True):
            lines.insert(idx, text)

    with open(path, "w", encoding="utf-8", errors="surrogateescape", newline="") as f:
        f.writelines(lines)


if __name__ == "__main__":
    main()

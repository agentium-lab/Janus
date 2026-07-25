#!/usr/bin/env python3
"""GA readiness checker.

Parses docs/Janus-core-capability-matrix.md and verifies all P0
capabilities are 'Covered'. Exits non-zero if any P0 is not Covered.
Also lists P1 items for risk acceptance documentation.
"""

import re
import sys
from pathlib import Path

MATRIX = Path(__file__).resolve().parent.parent / "docs" / "Janus-core-capability-matrix.md"

def main():
    if not MATRIX.exists():
        print(f"FAIL: capability matrix not found at {MATRIX}")
        sys.exit(1)

    lines = MATRIX.read_text().splitlines()
    p0_not_covered = []
    p1_items = []
    p0_count = 0
    p1_count = 0

    for line in lines:
        line = line.strip()
        if not line.startswith("|"):
            continue

        cells = [c.strip() for c in line.split("|")]
        if len(cells) < 5:
            continue

        cap_id = cells[1]
        priority = cells[3]
        status = cells[4]

        if not cap_id or "-" in cap_id and len(cap_id) > 20:
            continue
        if not re.match(r"^[A-Z]+-\d+", cap_id):
            continue

        if priority == "P0":
            p0_count += 1
            if status != "Covered":
                p0_not_covered.append((cap_id, status, " ".join(cells[2:3])))
        elif priority == "P1":
            p1_count += 1
            if status != "Covered":
                p1_items.append((cap_id, status))

    print(f"P0 capabilities: {p0_count} total, {p0_count - len(p0_not_covered)} Covered")
    print(f"P1 capabilities: {p1_count} total, {len(p1_items)} not Covered")

    if p0_not_covered:
        print("\nFAIL: P0 capabilities not Covered:")
        for cap_id, status, desc in p0_not_covered:
            print(f"  {cap_id}: {status} — {desc}")
        sys.exit(1)

    if p1_items:
        print("\nP1 items requiring risk acceptance:")
        for cap_id, status in p1_items:
            print(f"  {cap_id}: {status}")

    print(f"\nOK: All {p0_count} P0 capabilities are Covered. GA readiness passed.")
    sys.exit(0)

if __name__ == "__main__":
    main()

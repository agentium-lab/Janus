#!/usr/bin/env python3
"""Config-wiring gate.

Asserts that every field declared on config.Config is actually consumed by
the runtime (referenced as cfg.<Field> somewhere under server/cmd). This
class of drift recurred five times (outbox settings, tracing, artifacts,
log, heartbeat), each time surfacing as a setting operators set that
silently did nothing.

Fail mode: any top-level Config field with zero runtime references.
"""

import re
import sys
from pathlib import Path

CONFIG = Path("server/internal/config/config.go")
CONSUMERS = sorted(Path("server/cmd").rglob("*.go"))

# Fields intentionally not referenced in server/cmd: none today. To grant
# an exemption, add the field name with a justification comment.
EXEMPT: dict[str, str] = {}


def main() -> int:
    src = CONFIG.read_text()
    m = re.search(r"type Config struct \{(.*?)\n\}", src, re.S)
    if not m:
        print(f"FATAL: Config struct not found in {CONFIG}")
        return 2
    fields = re.findall(r"^\t(\w+)\s+\w+", m.group(1), re.M)
    if not fields:
        print("FATAL: no fields parsed from Config struct")
        return 2

    corpus = "\n".join(p.read_text() for p in CONSUMERS if not p.name.endswith("_test.go"))

    dead = []
    for f in fields:
        if f in EXEMPT:
            continue
        if not re.search(rf"\bcfg\.{f}\b", corpus):
            dead.append(f)

    if dead:
        print("FAIL: config fields declared but never wired into the runtime:")
        for f in dead:
            print(f"  - Config.{f}  (set via config/env but has zero effect)")
        print("Wire them in server/cmd/janus-api or delete the declaration")
        print("(and its SetDefault/bindEnv entries) — do not ship dead knobs.")
        return 1

    print(f"OK: all {len(fields)} config fields are wired into the runtime")
    return 0


if __name__ == "__main__":
    sys.exit(main())

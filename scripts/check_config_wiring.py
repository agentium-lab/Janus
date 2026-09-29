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
    leaf_dead = []
    for f in fields:
        if f in EXEMPT:
            continue
        if not re.search(rf"\bcfg\.{f}\b", corpus):
            dead.append(f)
            continue
        if f in PASS_THROUGH:
            # struct is handed to a constructor whole; leaves are consumed
            # under the callee's parameter name
            continue
        for sub in sub_fields(src, f):
            if not re.search(rf"cfg\.{f}\.{sub}\b", corpus):
                leaf_dead.append(f"{f}.{sub}")

    if dead or leaf_dead:
        print("FAIL: config declared but never wired into the runtime:")
        for f in dead:
            print(f"  - Config.{f}  (set via config/env but has zero effect)")
        for f in leaf_dead:
            print(f"  - cfg.{f}  (leaf declared but never referenced)")
        print("Wire them in server/cmd/janus-api or delete the declaration")
        print("(and its SetDefault/bindEnv entries) — do not ship dead knobs.")
        return 1

    print(f"OK: all {len(fields)} config fields (and their leaves) are wired")
    return 0


def sub_fields(src: str, field: str) -> list[str]:
    m = re.search(rf"\t{field} \w+ `mapstructure", src)
    if not m:
        return []
    tm = re.search(rf"{field} (\w+) `", m.group(0))
    if not tm:
        return []
    typ = tm.group(1)
    sm = re.search(rf"type {typ} struct \{{(.*?)\n\}}", src, re.S)
    if not sm:
        return []
    return re.findall(r"^\t(\w+)\s+[^\t\s]", sm.group(1), re.M)


# Top-level structs handed whole to constructors: their leaves are read
# under the callee's parameter name, so cfg.X.Leaf greps miss them.
PASS_THROUGH = {"TLS": "buildTLSConfig(cfg.TLS) reads leaves as tlsCfg.X"}


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Print the ack_wait (seconds) of the tenant/mailbox consumer from a NATS
jsz?config=true&consumers=true payload; empty string when not found."""
import json
import os
import sys

tn = os.environ.get("TENANT_NAME", "")
mb = os.environ.get("MB_NAME", "")


def walk(o):
    if isinstance(o, dict):
        name = o.get("name", "")
        if tn in name and mb in name:
            return o
        for v in o.values():
            r = walk(v)
            if r:
                return r
    elif isinstance(o, list):
        for v in o:
            r = walk(v)
            if r:
                return r
    return None


try:
    c = walk(json.load(sys.stdin))
    if c:
        print(c.get("config", {}).get("ack_wait", 0) // 1_000_000_000)
    else:
        print("")
except Exception:
    print("")

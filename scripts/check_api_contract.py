#!/usr/bin/env python3
"""API contract drift checker.

Validates that proto HTTP annotations, SDK conformance fixtures, and
documented HTTP-only routes are consistent. Exits non-zero on drift.

Checks:
1. Parse google.api.http annotations from proto/janus/v1/*.proto
2. Read sdk/conformance/http_cases.json
3. Verify all stable SDK fixture routes have proto annotation backing
   (or are explicitly listed as HTTP-only)
4. Report any proto routes missing from the SDK fixture
"""

import json
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = REPO_ROOT / "proto" / "janus" / "v1"
FIXTURE_PATH = REPO_ROOT / "sdk" / "conformance" / "http_cases.json"

HTTP_ONLY_ROUTES = {
    "POST:/v1/tenants",
    "GET:/v1/tenants/{tenant_id}",
    "POST:/v1/tenants/{tenant_id}/tasks/{task_id}/cancel",
    "POST:/v1/tenants/{tenant_id}/tasks/{task_id}/replay",
    "POST:/v1/tenants/{tenant_id}/api-keys",
    "GET:/v1/tenants/{tenant_id}/api-keys",
    "POST:/v1/tenants/{tenant_id}/api-keys/{key_id}/revoke",
    "POST:/v1/tenants/{tenant_id}/policy-rules",
    "POST:/v1/tenants/{tenant_id}/policy-rules/templates",
    "GET:/v1/tenants/{tenant_id}/policy-rules",
    "POST:/v1/tenants/{tenant_id}/budgets",
    "GET:/v1/tenants/{tenant_id}/budgets/{scope_type}/{scope_id}",
    "GET:/v1/tenants/{tenant_id}/budgets",
    "POST:/v1/tenants/{tenant_id}/approvals/{approval_id}/approve",
    "POST:/v1/tenants/{tenant_id}/approvals/{approval_id}/reject",
}


def parse_proto_http_annotations():
    """Extract HTTP method+path from google.api.http annotations in proto files."""
    routes = set()
    http_re = re.compile(
        r'option\s*\(google\.api\.http\)\s*=\s*\{\s*'
        r'(post|get|put|patch|delete):\s*"([^"]+)"',
        re.IGNORECASE,
    )
    for proto_file in sorted(PROTO_DIR.glob("*.proto")):
        content = proto_file.read_text()
        for match in http_re.finditer(content):
            method = match.group(1).upper()
            path = match.group(2)
            routes.add(f"{method}:{path}")
    return routes


def normalize_fixture_path(path: str) -> str:
    """Normalize a fixture path by replacing {tenant} with {tenant_id}."""
    return path.replace("{tenant}", "{tenant_id}")


def parse_fixture_routes():
    """Extract HTTP method+path from the SDK conformance fixture."""
    if not FIXTURE_PATH.exists():
        return set()
    data = json.loads(FIXTURE_PATH.read_text())
    routes = set()
    for case in data.get("cases", []):
        method = case.get("method", "GET").upper()
        path = normalize_fixture_path(case["path"])
        routes.add(f"{method}:{path}")
    return routes


def route_matches_fixture(proto_route, fixture_path):
    """Check if a proto route pattern matches a concrete fixture path."""
    pm, pp = proto_route.split(":", 1)
    fm, fp = fixture_path.split(":", 1)
    if pm != fm:
        return False
    pp_parts = pp.strip("/").split("/")
    fp_parts = fp.strip("/").split("/")
    if len(pp_parts) != len(fp_parts):
        return False
    for pp_part, fp_part in zip(pp_parts, fp_parts):
        if pp_part.startswith("{") and pp_part.endswith("}"):
            continue
        if pp_part != fp_part:
            return False
    return True


def main():
    proto_routes = parse_proto_http_annotations()
    fixture_routes = parse_fixture_routes()

    errors = []

    for route in sorted(fixture_routes):
        method, path = route.split(":", 1)
        if route in HTTP_ONLY_ROUTES:
            continue
        matched = any(route_matches_fixture(pr, route) for pr in proto_routes)
        if not matched:
            errors.append(
                f"FIXTURE route {route} has no proto annotation backing "
                f"and is not in HTTP_ONLY_ROUTES"
            )

    for route in sorted(proto_routes):
        if route not in fixture_routes and route not in HTTP_ONLY_ROUTES:
            print(f"WARN: proto route {route} not in SDK fixture (may be internal)")

    if errors:
        print("FAIL: API contract drift detected:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

    print(f"OK: {len(proto_routes)} proto routes, {len(fixture_routes)} fixture routes, "
          f"{len(HTTP_ONLY_ROUTES)} HTTP-only routes - no drift")
    sys.exit(0)


if __name__ == "__main__":
    main()

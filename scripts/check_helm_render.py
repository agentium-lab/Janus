#!/usr/bin/env python3
"""Helm render assertions for the migration Job password escaping.

Guards the special-character-password hole in
deployments/helm/janus-core/templates/migration-job.yaml:

  * The DB password reaches the Job at RUNTIME via the JANUS_PG_PASSWORD
    env var (SecretKeyRef), so it must never be rendered inline inside a
    DSN URL — Helm's urlquery cannot escape a runtime env var, and a raw
    # ? / @ % or space breaks the URI parse inside migrate.
  * The Job must carry a runtime percent-encoding construct (a POSIX sed
    chain over $JANUS_PG_PASSWORD), or the janus-api auto-migration path
    (the binary builds its DSN via config.DSN(), which url.QueryEscape's).
  * postgres.sslMode must land in the migration Job DSN as configured.

Renders two variants (chart-managed Secret and postgres.existingSecret)
and asserts all of the above. Exit 0 = pass, 1 = regression.
"""
from __future__ import annotations

import re
import subprocess
import sys

CHART = "deployments/helm/janus-core"
RELEASE = "janus"
RAW_PASSWORD = "p@ss#w?rd/x"
EXISTING_SECRET = "ops-provided-pg-secret"
ENCODING_TABLE = ["%25", "%23", "%3F", "%2F", "%40", "%20"]

failures = []


def fail(msg: str) -> None:
    failures.append(msg)
    print(f"FAIL: {msg}")


def ok(msg: str) -> None:
    print(f"PASS: {msg}")


def run_helm(extra_sets: list[str]) -> str:
    cmd = [
        "helm", "template", RELEASE, CHART,
        "--set", f"postgres.password={RAW_PASSWORD}",
        "--set", "postgres.sslMode=require",
        *extra_sets,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"FAIL: helm template exited {proc.returncode} ({' '.join(extra_sets)})")
        print(proc.stderr.strip())
        sys.exit(1)
    return proc.stdout


def doc_for(rendered: str, template_path: str) -> str | None:
    for doc in re.split(r"(?m)^---\s*$", rendered):
        if f"# Source:" in doc and f"templates/{template_path}" in doc:
            return doc
    return None


def migration_job_doc(rendered: str) -> str | None:
    doc = doc_for(rendered, "migration-job.yaml")
    if doc is not None and re.search(r"(?m)^kind: Job$", doc):
        return doc
    return None


def check_common(label: str, job: str | None, rendered: str) -> None:
    if job is None:
        fail(f"[{label}] migration Job document not found in render")
        return
    print(f"--- {label} ---")

    if RAW_PASSWORD in job:
        fail(f"[{label}] raw password '{RAW_PASSWORD}' rendered inline in the migration Job")
    else:
        ok("no raw password in migration Job")

    # Defense in depth: sweep every rendered doc, not just the Job — a URL
    # anywhere in the release must not embed the raw password.
    if re.search(re.escape("://") + r"[^\n]*" + re.escape(RAW_PASSWORD), rendered):
        fail(f"[{label}] raw password found inside a URL in the rendered output")
    else:
        ok("no raw password inside any rendered URL")

    has_sed_chain = 'sed' in job and '"$JANUS_PG_PASSWORD"' in job and all(
        tok in job for tok in ENCODING_TABLE
    )
    has_auto_migration = ("janus-core" in job or "janus-api" in job) and "JANUS_MIGRATION_AUTO" in job
    if has_sed_chain or has_auto_migration:
        kind = "runtime sed percent-encoder" if has_sed_chain else "janus-api auto-migration"
        ok(f"escaping construct present ({kind})")
    else:
        fail(
            f"[{label}] no runtime percent-encoder found: expected /bin/sh script "
            f"escaping $JANUS_PG_PASSWORD via sed ({ENCODING_TABLE}), or the "
            "janus-api auto-migration path (JANUS_MIGRATION_AUTO)"
        )

    if re.search(r"secretKeyRef:\s*\n\s+name: \S+\s*\n\s+key: \S+", job):
        ok("JANUS_PG_PASSWORD sourced from a Secret")
    else:
        fail(f"[{label}] JANUS_PG_PASSWORD is not wired through secretKeyRef")

    if "sslmode=require" in job:
        ok("sslmode=require in migration Job DSN")
    else:
        fail(f"[{label}] sslmode=require missing from migration Job DSN")


def main() -> int:
    rendered = run_helm([])
    check_common("chart-managed secret", migration_job_doc(rendered), rendered)
    job = migration_job_doc(rendered)
    if job is not None:
        if re.search(r"secretKeyRef:\s*\n\s+name: \S+-migration\s*\n", job):
            ok("chart-managed variant references the generated migration Secret")
        else:
            fail("chart-managed variant does not reference the generated migration Secret")
        if doc_for(rendered, "migration-secret.yaml") is not None:
            ok("generated migration Secret present")
        else:
            fail("generated migration Secret missing")

    rendered = run_helm(["--set", f"postgres.existingSecret={EXISTING_SECRET}"])
    check_common("existingSecret", migration_job_doc(rendered), rendered)
    job = migration_job_doc(rendered)
    if job is not None:
        if f"name: {EXISTING_SECRET}" in job:
            ok(f"existingSecret variant references '{EXISTING_SECRET}'")
        else:
            fail(f"[existingSecret] migration Job does not reference '{EXISTING_SECRET}'")
        if doc_for(rendered, "migration-secret.yaml") is not None:
            fail("[existingSecret] chart still generates its own migration Secret")
        else:
            ok("chart does not duplicate the migration Secret when existingSecret is set")

    print()
    if failures:
        print(f"check_helm_render: {len(failures)} assertion(s) failed")
        return 1
    print("check_helm_render: all assertions passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())

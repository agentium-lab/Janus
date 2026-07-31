#!/usr/bin/env python3
"""
7-Agent Realistic Scenario Test for Janus.

Pipeline: Product Manager → Code Reviewer → Code Writer → Test Runner → (PASS) → Deploy
                                                   ↗ (FAIL) ↗ Code Writer (fix) ↗ Test Runner ↗ Deploy

7 agents:
  1. product-manager   — creates review request
  2. review-agent      — reviews and sends code task
  3. code-writer       — writes code, sends to test
  4. test-runner       — runs tests, PASS→deploy, FAIL→code-writer
  5. deploy-agent      — deploys
  6. doc-agent         — writes documentation for completed tasks
  7. audit-agent       — queries audit trail for full pipeline trace
"""
import httpx
import json
import time
import sys

BASE = "http://localhost:8080"
TENANT = "pipeline-demo"

def api(method, path, body=None):
    url = f"{BASE}/v1/tenants/{TENANT}{path}" if not path.startswith("/v1/") else f"{BASE}{path}"
    resp = httpx.request(method, url, json=body, timeout=30)
    if resp.status_code >= 400:
        print(f"  ERROR {resp.status_code}: {resp.text[:200]}")
    return resp

def setup():
    httpx.post(f"{BASE}/v1/tenants", json={"id": TENANT, "name": "Pipeline Demo"}, timeout=10)

    agents = [
        ("product-manager", "Product Manager", "Creates tasks and routes to review", "review_mb"),
        ("review-agent", "Code Reviewer", "Reviews code and routes to code-writer", "writer_mb"),
        ("code-writer", "Code Writer", "Writes code changes", "test_mb"),
        ("test-runner", "Test Runner", "Runs tests, routes pass/fail", "deploy_mb"),
        ("deploy-agent", "Deploy Agent", "Deploys approved code", "doc_mb"),
        ("doc-agent", "Doc Writer", "Documents completed work", "audit_mb"),
        ("audit-agent", "Audit Agent", "Traces pipeline execution", "audit_mb"),
    ]

    for agent_id, name, desc, _ in agents:
        r = api("POST", "/agents", {"id": agent_id, "display_name": name, "protocol": "a2a"})
        status = "✓" if r.status_code in (200, 201) else "✗"
        print(f"  {status} Register agent: {agent_id}")

    created_mbs = set()
    for agent_id, _, _, mb_id in agents:
        if mb_id in created_mbs:
            continue
        r = api("POST", "/mailboxes", {"id": mb_id, "agent_id": agent_id})
        status = "✓" if r.status_code in (200, 201) else "✗"
        print(f"  {status} Create mailbox: {mb_id}")
        created_mbs.add(mb_id)

def step_publish_task():
    print("\n=== Step 1: Product Manager creates review request ===")
    r = api("POST", "/tasks", {
        "id": "task-review-001",
        "source_agent": "product-manager",
        "target_type": "mailbox",
        "target_value": "review_mb",
        "envelope": {
            "janus_version": "0.3",
            "task_id": "task-review-001",
            "tenant_id": TENANT,
            "source_agent": "product-manager",
            "target": {"type": "mailbox", "value": "review_mb"},
            "payload": {"type": "code_review_request", "content": "Review PR #42: Add user authentication"},
            "trace": {"trace_id": "trace-pipeline-001"}
        }
    })
    print(f"  {'✓' if r.status_code in (200,201) else '✗'} Publish task-review-001 → review_mb")
    return r.status_code in (200, 201)

def step_pull_and_complete(mailbox, agent, task_id, result_content, trace_id):
    r = api("POST", f"/mailboxes/{mailbox}/pull", {"agent_id": agent})
    if r.status_code == 204 or not r.content:
        print(f"  ✗ No task in {mailbox} for {agent}")
        return False

    data = r.json()
    task = data.get("task", {})
    lease = data.get("lease", {})
    lease_id = lease.get("lease_id", "")
    pulled_task_id = task.get("id", "")

    print(f"  ✓ {agent} pulled task {pulled_task_id} (lease={lease_id[:12]}...)")

    api("POST", f"/tasks/{pulled_task_id}/start", {"lease_id": lease_id})
    api("POST", f"/tasks/{pulled_task_id}/heartbeat", {"lease_id": lease_id})
    ack_r = api("POST", f"/tasks/{pulled_task_id}/ack", {
        "lease_id": lease_id,
        "result_ref": f"result://{pulled_task_id}"
    })
    print(f"  {'✓' if ack_r.status_code == 200 else '✗'} {agent} ACK'd task {pulled_task_id}")
    return pulled_task_id

def step_review_completes():
    print("\n=== Step 2: Review Agent reviews and creates code task ===")
    step_pull_and_complete("review_mb", "review-agent", "task-review-001", "LGTM", "trace-pipeline-001")

    r = api("POST", "/tasks", {
        "id": "task-code-001",
        "source_agent": "review-agent",
        "target_type": "mailbox",
        "target_value": "writer_mb",
        "envelope": {
            "janus_version": "0.3",
            "task_id": "task-code-001",
            "tenant_id": TENANT,
            "source_agent": "review-agent",
            "target": {"type": "mailbox", "value": "writer_mb"},
            "payload": {"type": "code_change_request", "content": "Implement JWT auth in auth.go"},
            "trace": {"trace_id": "trace-pipeline-001", "parent_task_id": "task-review-001"}
        }
    })
    print(f"  {'✓' if r.status_code in (200,201) else '✗'} Publish task-code-001 → writer_mb")

def step_code_writer():
    print("\n=== Step 3: Code Writer writes code and sends to test ===")
    step_pull_and_complete("writer_mb", "code-writer", "task-code-001", "code written", "trace-pipeline-001")

    r = api("POST", "/tasks", {
        "id": "task-test-001",
        "source_agent": "code-writer",
        "target_type": "mailbox",
        "target_value": "test_mb",
        "envelope": {
            "janus_version": "0.3",
            "task_id": "task-test-001",
            "tenant_id": TENANT,
            "source_agent": "code-writer",
            "target": {"type": "mailbox", "value": "test_mb"},
            "payload": {"type": "run_tests", "content": "Run auth_test.go"},
            "trace": {"trace_id": "trace-pipeline-001", "parent_task_id": "task-code-001"}
        }
    })
    print(f"  {'✓' if r.status_code in (200,201) else '✗'} Publish task-test-001 → test_mb")

def step_test_runner_fail():
    print("\n=== Step 4: Test Runner FAILS — sends fix request back to code-writer ===")
    r = api("POST", "/mailboxes/test_mb/pull", {"agent_id": "test-runner"})
    if r.status_code == 204 or not r.content:
        print("  ✗ No task in test_mb — creating fix request anyway")
    elif "task" not in r.json():
        print("  ✗ No task in test_mb — consumer not ready")
        time.sleep(1)
        r = api("POST", "/mailboxes/test_mb/pull", {"agent_id": "test-runner"})
        if r.status_code == 204 or not r.content or "task" not in r.json():
            print("  ✗ Still no task — skipping NACK")
    if r.status_code == 200 and "task" in r.json():
        data = r.json()
        task_id = data["task"]["id"]
        lease_id = data["lease"]["lease_id"]
        print(f"  ✓ test-runner pulled {task_id}")

        api("POST", f"/tasks/{task_id}/start", {"lease_id": lease_id})
        fail_r = api("POST", f"/tasks/{task_id}/nack", {
            "lease_id": lease_id,
            "retriable": True,
            "error": {"code": "test_failure", "message": "TestAuthJWT: expected 200, got 500"}
        })
        print(f"  {'✓' if fail_r.status_code == 200 else '✗'} test-runner NACK'd (retriable)")

    r = api("POST", "/tasks", {
        "id": "task-code-fix-001",
        "source_agent": "test-runner",
        "target_type": "mailbox",
        "target_value": "writer_mb",
        "envelope": {
            "janus_version": "0.3",
            "task_id": "task-code-fix-001",
            "tenant_id": TENANT,
            "source_agent": "test-runner",
            "target": {"type": "mailbox", "value": "writer_mb"},
            "payload": {"type": "fix_request", "content": "Fix: token expiry check was inverted"},
            "trace": {"trace_id": "trace-pipeline-001", "parent_task_id": "task-test-001"}
        }
    })
    print(f"  {'✓' if r.status_code in (200,201) else '✗'} Publish task-code-fix-001 → writer_mb")

def step_code_fix_and_retest():
    print("\n=== Step 5: Code Writer fixes and test-runner PASSES ===")
    step_pull_and_complete("writer_mb", "code-writer", "task-code-fix-001", "fixed", "trace-pipeline-001")

    r = api("POST", "/tasks", {
        "id": "task-test-002",
        "source_agent": "code-writer",
        "target_type": "mailbox",
        "target_value": "test_mb",
        "envelope": {
            "janus_version": "0.3",
            "task_id": "task-test-002",
            "tenant_id": TENANT,
            "source_agent": "code-writer",
            "target": {"type": "mailbox", "value": "test_mb"},
            "payload": {"type": "run_tests", "content": "Re-run auth_test.go"},
            "trace": {"trace_id": "trace-pipeline-001", "parent_task_id": "task-code-fix-001"}
        }
    })
    print(f"  {'✓' if r.status_code in (200,201) else '✗'} Publish task-test-002 → test_mb")

    step_pull_and_complete("test_mb", "test-runner", "task-test-002", "all tests passed", "trace-pipeline-001")

    r = api("POST", "/tasks", {
        "id": "task-deploy-001",
        "source_agent": "test-runner",
        "target_type": "mailbox",
        "target_value": "deploy_mb",
        "envelope": {
            "janus_version": "0.3",
            "task_id": "task-deploy-001",
            "tenant_id": TENANT,
            "source_agent": "test-runner",
            "target": {"type": "mailbox", "value": "deploy_mb"},
            "payload": {"type": "deploy_request", "content": "Deploy v1.2.3"},
            "trace": {"trace_id": "trace-pipeline-001", "parent_task_id": "task-test-002"}
        }
    })
    print(f"  {'✓' if r.status_code in (200,201) else '✗'} Publish task-deploy-001 → deploy_mb")

def step_deploy():
    print("\n=== Step 6: Deploy Agent deploys ===")
    step_pull_and_complete("deploy_mb", "deploy-agent", "task-deploy-001", "deployed v1.2.3", "trace-pipeline-001")

def step_doc():
    print("\n=== Step 7: Doc Agent documents completed pipeline ===")
    events_r = api("GET", "/tasks/task-deploy-001/events")
    if events_r.status_code == 200:
        events = events_r.json()
        if isinstance(events, dict):
            events = events.get("events", [])
        print(f"  ✓ doc-agent queried {len(events)} events for task-deploy-001")

def step_audit():
    print("\n=== Step 8: Audit Agent traces full pipeline ===")
    trace_r = httpx.get(f"{BASE}/v1/tenants/{TENANT}/traces/trace-pipeline-001", timeout=10)
    if trace_r.status_code == 200:
        data = trace_r.json()
        events = data if isinstance(data, list) else data.get("events", [])
        if events is None:
            events = []
        print(f"  ✓ audit-agent traced {len(events)} events for trace-pipeline-001")
    else:
        print(f"  ○ audit-agent trace query returned {trace_r.status_code}")

    for tid in ["task-review-001", "task-code-001", "task-test-001",
                "task-code-fix-001", "task-test-002", "task-deploy-001"]:
        r = api("GET", f"/tasks/{tid}")
        if r.status_code == 200:
            status = r.json().get("status", "unknown")
            icon = "✓" if status in ("completed", "failed", "dead_lettered") else "○"
            print(f"  {icon} {tid}: {status}")

def step_idempotency():
    print("\n=== Step 9: Idempotency check — duplicate ACK ===")
    r = api("GET", "/tasks/task-review-001")
    if r.status_code != 200:
        print("  SKIP: task not found")
        return
    ack_r = api("POST", "/tasks/task-review-001/ack", {"lease_id": "expired-lease", "result_ref": "dup"})
    print(f"  {'✓' if ack_r.status_code in (200, 400) else '✗'} Duplicate ACK handled gracefully")

def main():
    print("=" * 60)
    print("  7-Agent Realistic Pipeline Test")
    print("  Product → Review → Code → Test (FAIL→FIX→RETEST) → Deploy")
    print("=" * 60)

    print("\n=== Setup: Create tenant, 7 agents, 7 mailboxes ===")
    setup()

    step_publish_task()
    step_review_completes()
    step_code_writer()
    step_test_runner_fail()
    step_code_fix_and_retest()
    step_deploy()
    step_doc()
    step_audit()
    step_idempotency()

    print("\n" + "=" * 60)
    print("  Pipeline simulation complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()

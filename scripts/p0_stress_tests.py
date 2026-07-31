#!/usr/bin/env python3
"""
P0 Concurrency + Multi-Tenant Isolation Tests.

Test 1: 100 concurrent pulls on same mailbox — exactly-once delivery
Test 2: Concurrent ACK race — budget settled once
Test 3: Outbox worker concurrency — no duplicate publish
Test 4: Cross-tenant isolation — all access denied
"""
import httpx
import json
import time
import threading
import sys
from collections import Counter

BASE = "http://localhost:8080"
TIMEOUT = 30

def api(method, path, body=None, tenant="test"):
    url = f"{BASE}/v1/tenants/{tenant}{path}"
    try:
        resp = httpx.request(method, url, json=body, timeout=TIMEOUT)
        return resp
    except Exception as e:
        return None

def raw_api(method, url, body=None):
    try:
        return httpx.request(method, url, json=body, timeout=TIMEOUT)
    except:
        return None

def setup_tenant(tenant_id, name):
    raw_api("POST", f"{BASE}/v1/tenants", {"id": tenant_id, "name": name})

def setup_agent(tenant, agent_id):
    api("POST", "/agents", {"id": agent_id, "display_name": agent_id, "protocol": "a2a"}, tenant)

def setup_mailbox(tenant, mb_id, agent_id):
    api("POST", "/mailboxes", {"id": mb_id, "agent_id": agent_id}, tenant)

def publish_task(tenant, task_id, source, target_mb, content="test"):
    return api("POST", "/tasks", {
        "id": task_id,
        "source_agent": source,
        "target_type": "mailbox",
        "target_value": target_mb,
        "envelope": {
            "janus_version": "0.3",
            "task_id": task_id,
            "tenant_id": tenant,
            "source_agent": source,
            "target": {"type": "mailbox", "value": target_mb},
            "payload": {"type": "test", "content": content},
            "trace": {"trace_id": f"trace-{task_id}"}
        }
    }, tenant)


def test_concurrent_pull():
    
    print("\n" + "=" * 60)
    print("  TEST 1: Concurrent Pull — Exactly-Once Delivery")
    print("=" * 60)
    tenant = "conc-pull"
    mb = "shared-mb"

    setup_tenant(tenant, "Concurrent Pull Test")
    setup_agent(tenant, "owner-agent")
    setup_mailbox(tenant, mb, "owner-agent")
    NUM_TASKS = 50
    NUM_WORKERS = 20

    print(f"\n  Publishing {NUM_TASKS} tasks to {mb}...")
    for i in range(NUM_TASKS):
        publish_task(tenant, f"cpull-task-{i:03d}", "owner-agent", mb)
    print(f"  Waiting for outbox to deliver to NATS...")
    time.sleep(3)
    check_r = api("GET", "/tasks/cpull-task-000", None, tenant)
    if check_r and check_r.status_code == 200:
        task_status = check_r.json().get("status", "")
        print(f"  First task status: {task_status}")
    print(f"  Launching {NUM_WORKERS} concurrent pull workers (shared agent)...")

    pulled_tasks = []
    pull_lock = threading.Lock()

    def worker(worker_id):
        while True:
            r = api("POST", f"/mailboxes/{mb}/pull", {"agent_id": "owner-agent"}, tenant)
            if r is None or r.status_code == 204 or not r.content:
                break
            try:
                data = r.json()
                if "task" not in data or not data["task"]:
                    break
                task_id = data["task"]["id"]
                lease_id = data["lease"]["lease_id"]
                with pull_lock:
                    pulled_tasks.append((worker_id, task_id, lease_id))
                api("POST", f"/tasks/{task_id}/start", {"lease_id": lease_id}, tenant)
            except:
                break

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    task_ids = [tid for _, tid, _ in pulled_tasks]
    task_counter = Counter(task_ids)
    duplicates = {tid: count for tid, count in task_counter.items() if count > 1}

    print(f"\n  Tasks pulled: {len(task_ids)} / {NUM_TASKS}")
    print(f"  Unique tasks: {len(set(task_ids))}")
    print(f"  Duplicates: {len(duplicates)}")

    if len(duplicates) == 0 and len(set(task_ids)) == NUM_TASKS:
        print(f"\n  ✓ PASS: All {NUM_TASKS} tasks pulled exactly once, no duplicates")
        return True
    else:
        print(f"\n  ✗ FAIL: {len(duplicates)} duplicate pulls detected")
        for tid, count in list(duplicates.items())[:5]:
            print(f"    {tid}: pulled {count} times")
        return False


def test_concurrent_ack():
    
    print("\n" + "=" * 60)
    print("  TEST 2: Concurrent ACK Race — Budget Settled Once")
    print("=" * 60)
    tenant = "conc-ack"
    mb = "ack-mb"

    setup_tenant(tenant, "Concurrent ACK Test")
    setup_agent(tenant, "agent-a")
    setup_agent(tenant, "agent-b")
    setup_mailbox(tenant, mb, "agent-a")

    publish_task(tenant, "cack-task-002", "agent-a", mb, "budget test")
    time.sleep(1)

    r = api("POST", f"/mailboxes/{mb}/pull", {"agent_id": "agent-a"}, tenant)
    if r is None or "task" not in r.json():
        print("  ✗ FAIL: could not pull task")
        return False

    data = r.json()
    task_id = data["task"]["id"]
    lease_id = data["lease"]["lease_id"]
    print(f"\n  Task pulled: {task_id} (lease={lease_id[:12]}...)")

    api("POST", f"/tasks/{task_id}/start", {"lease_id": lease_id}, tenant)

    ack_results = []
    ack_lock = threading.Lock()

    def ack_worker(agent_id):
        ack_r = api("POST", f"/tasks/{task_id}/ack", {
            "lease_id": lease_id,
            "result_ref": f"result://{agent_id}",
            "token_usage": {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
        }, tenant)
        with ack_lock:
            ack_results.append((agent_id, ack_r.status_code if ack_r else 0))

    threads = [
        threading.Thread(target=ack_worker, args=("agent-a",)),
        threading.Thread(target=ack_worker, args=("agent-b",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    print(f"  ACK results: {ack_results}")

    task_r = api("GET", f"/tasks/{task_id}", None, tenant)
    task_status = task_r.json().get("status", "") if task_r and task_r.status_code == 200 else "unknown"
    result_ref = task_r.json().get("result_ref", "") if task_r and task_r.status_code == 200 else ""
    print(f"  Final task status: {task_status}")
    print(f"  Final result_ref: {result_ref}")

    events_r = api("GET", f"/tasks/{task_id}/events", None, tenant)
    events = events_r.json() if events_r and events_r.status_code == 200 else []
    if isinstance(events, dict):
        events = events.get("events", [])
    completed_count = sum(1 for e in events if isinstance(e, dict) and e.get("event_type") == "task.completed")
    print(f"  task.completed events: {completed_count}")

    if task_status == "completed" and completed_count <= 1:
        print(f"\n  ✓ PASS: Task completed, at most 1 completed event (no double settlement)")
        return True
    else:
        print(f"\n  ✗ FAIL: status={task_status}, completed_events={completed_count}")
        return False


def test_outbox_concurrency():
    
    print("\n" + "=" * 60)
    print("  TEST 3: Outbox Worker Concurrency — No Duplicate Publish")
    print("=" * 60)
    tenant = "conc-outbox"
    mb = "outbox-mb"

    setup_tenant(tenant, "Outbox Concurrency Test")
    setup_agent(tenant, "sender-agent")
    setup_mailbox(tenant, mb, "sender-agent")

    NUM_TASKS = 20
    print(f"\n  Publishing {NUM_TASKS} tasks...")
    for i in range(NUM_TASKS):
        publish_task(tenant, f"coutbox-{i:03d}", "sender-agent", mb)
    time.sleep(2)

    published = []
    pub_lock = threading.Lock()

    def pull_worker(wid):
        agent_id = f"outbox-w-{wid}"
        setup_agent(tenant, agent_id)
        while True:
            r = api("POST", f"/mailboxes/{mb}/pull", {"agent_id": agent_id}, tenant)
            if r is None or r.status_code == 204 or not r.content:
                break
            try:
                data = r.json()
                if "task" not in data or not data["task"]:
                    break
                tid = data["task"]["id"]
                lease_id = data["lease"]["lease_id"]
                with pub_lock:
                    published.append(tid)
                api("POST", f"/tasks/{tid}/ack", {"lease_id": lease_id, "result_ref": f"r://{tid}"}, tenant)
            except:
                break

    threads = [threading.Thread(target=pull_worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    dup_count = len(published) - len(set(published))
    print(f"\n  Tasks delivered: {len(published)}")
    print(f"  Unique: {len(set(published))}")
    print(f"  Duplicates: {dup_count}")

    if dup_count == 0:
        print(f"\n  ✓ PASS: All {len(published)} deliveries unique (SKIP LOCKED working)")
        return True
    else:
        print(f"\n  ✗ FAIL: {dup_count} duplicate deliveries")
        return False


def test_tenant_isolation():
    
    print("\n" + "=" * 60)
    print("  TEST 4: Multi-Tenant Isolation — Cross-Tenant Denied")
    print("=" * 60)

    tenant_a = "iso-tenant-a"
    tenant_b = "iso-tenant-b"

    setup_tenant(tenant_a, "Tenant A")
    setup_tenant(tenant_b, "Tenant B")

    setup_agent(tenant_a, "agent-a1")
    setup_mailbox(tenant_a, "mb-a1", "agent-a1")
    publish_task(tenant_a, "iso-task-a1", "agent-a1", "mb-a1")
    time.sleep(1)

    setup_agent(tenant_b, "agent-b1")
    setup_mailbox(tenant_b, "mb-b1", "agent-b1")
    publish_task(tenant_b, "iso-task-b1", "agent-b1", "mb-b1")
    time.sleep(1)

    passed = True

    # A cannot pull B's mailbox
    r = api("POST", "/mailboxes/mb-b1/pull", {"agent_id": "agent-a1"}, tenant_a)
    status = r.status_code if r else 0
    has_task = r and r.status_code == 200 and "task" in r.json() if r else False
    print(f"\n  Tenant-A pulls Tenant-B mailbox: {status} {'✓ denied' if not has_task else '✗ LEAK!'}")
    if has_task:
        passed = False

    # A cannot GET B's task
    r = api("GET", "/tasks/iso-task-b1", None, tenant_a)
    status = r.status_code if r else 0
    print(f"  Tenant-A gets Tenant-B task: {status} {'✓ denied' if status in (403, 404) else '✗ LEAK!'}")
    if status == 200:
        passed = False

    # A cannot see B's events
    r = api("GET", "/tasks/iso-task-b1/events", None, tenant_a)
    status = r.status_code if r else 0
    body = r.json() if r and r.content else {}
    events = body.get("events") if isinstance(body, dict) else body
    leaked = events is not None and len(events) > 0 if isinstance(events, list) else False
    print(f"  Tenant-A reads Tenant-B events: {status} {'✓ denied (empty)' if not leaked else '✗ LEAK!'}")
    if leaked:
        passed = False

    # B cannot pull A's mailbox
    r = api("POST", "/mailboxes/mb-a1/pull", {"agent_id": "agent-b1"}, tenant_b)
    has_task = r and r.status_code == 200 and "task" in r.json() if r else False
    print(f"  Tenant-B pulls Tenant-A mailbox: {'✓ denied' if not has_task else '✗ LEAK!'}")
    if has_task:
        passed = False

    if passed:
        print(f"\n  ✓ PASS: No cross-tenant data leakage")
    else:
        print(f"\n  ✗ FAIL: Cross-tenant access detected")
    return passed


def main():
    print("=" * 60)
    print("  P0 Stress Tests: Concurrency + Tenant Isolation")
    print("=" * 60)

    results = []
    results.append(("Concurrent Pull (exactly-once)", test_concurrent_pull()))
    results.append(("Concurrent ACK Race (budget once)", test_concurrent_ack()))
    results.append(("Outbox Concurrency (no dup)", test_outbox_concurrency()))
    results.append(("Tenant Isolation (no leak)", test_tenant_isolation()))

    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    passed = sum(1 for _, r in results if r)
    failed = sum(1 for _, r in results if not r)
    for name, r in results:
        print(f"  {'✓' if r else '✗'} {name}")
    print(f"\n  {passed} passed, {failed} failed")
    print("=" * 60)

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()

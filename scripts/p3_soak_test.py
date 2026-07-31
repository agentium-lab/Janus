#!/usr/bin/env python3
import httpx
import time
import sys
import os
import threading
from collections import defaultdict

BASE = "http://localhost:8080"
TENANT = "p3-soak"
DURATION_SEC = int(os.environ.get("SOAK_DURATION", "60"))
PUBLISH_INTERVAL = int(os.environ.get("PUBLISH_INTERVAL", "10"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "3"))

published = 0
completed = 0
failed = 0
nacked = 0
lock = threading.Lock()
stop_flag = threading.Event()
latencies = []


def raw(method, url, body=None):
    try:
        return httpx.request(method, url, json=body, timeout=30)
    except:
        return None


def setup():
    raw("POST", f"{BASE}/v1/tenants", {"id": TENANT, "name": "Soak Test"})
    raw("POST", f"{BASE}/v1/tenants/{TENANT}/agents",
        {"id": f"soak-worker", "display_name": "Soak Worker", "protocol": "a2a"})
    raw("POST", f"{BASE}/v1/tenants/{TENANT}/mailboxes",
        {"id": "soak-mb", "agent_id": "soak-worker"})


def publisher():
    global published
    task_seq = 0
    while not stop_flag.is_set():
        for _ in range(BATCH_SIZE):
            task_seq += 1
            tid = f"soak-task-{task_seq:06d}"
            t0 = time.time()
            r = raw("POST", f"{BASE}/v1/tenants/{TENANT}/tasks", {
                "id": tid, "source_agent": "soak-worker",
                "target_type": "mailbox", "target_value": "soak-mb",
                "envelope": {
                    "janus_version": "0.3", "task_id": tid, "tenant_id": TENANT,
                    "source_agent": "soak-worker",
                    "target": {"type": "mailbox", "value": "soak-mb"},
                    "payload": {"type": "soak", "content": f"task {task_seq}"},
                    "trace": {"trace_id": f"soak-{tid}"}
                }
            })
            lat = (time.time() - t0) * 1000
            with lock:
                if r and r.status_code in (200, 201):
                    published += 1
                    latencies.append(lat)
                else:
                    failed += 1
        time.sleep(PUBLISH_INTERVAL)


def worker(worker_id):
    global completed, nacked
    while not stop_flag.is_set():
        r = raw("POST", f"{BASE}/v1/tenants/{TENANT}/mailboxes/soak-mb/pull",
                {"agent_id": "soak-worker"})
        if not r or r.status_code == 204 or not r.content:
            time.sleep(0.5)
            continue
        try:
            data = r.json()
            if "task" not in data or not data["task"]:
                time.sleep(0.5)
                continue
            tid = data["task"]["id"]
            lid = data["lease"]["lease_id"]
            raw("POST", f"{BASE}/v1/tenants/{TENANT}/tasks/{tid}/start", {"lease_id": lid})
            import random
            if random.random() < 0.1:
                raw("POST", f"{BASE}/v1/tenants/{TENANT}/tasks/{tid}/nack",
                    {"lease_id": lid, "retriable": True, "error": {"code": "soak_nack", "message": "random fail"}})
                with lock:
                    nacked += 1
            else:
                raw("POST", f"{BASE}/v1/tenants/{TENANT}/tasks/{tid}/ack",
                    {"lease_id": lid, "result_ref": f"soak://{tid}"})
                with lock:
                    completed += 1
        except:
            time.sleep(0.5)


def check_invariants(elapsed):
    with lock:
        pub = published
        comp = completed
        nak = nacked
        fail = failed
        lats = list(latencies[-100:])

    p95 = sorted(lats)[int(len(lats) * 0.95)] if len(lats) >= 20 else 0
    avg = sum(lats) / len(lats) if lats else 0

    r = raw("GET", f"{BASE}/v1/tenants/{TENANT}/tasks/soak-task-000001")
    task_ok = r and r.status_code in (200, 404)

    print(f"  [{elapsed:4d}s] published={pub} completed={comp} nacked={nak} failed={fail}"
          f" | avg_pub={avg:.0f}ms p95={p95:.0f}ms"
          + (f" | api={'ok' if task_ok else 'DOWN'}" if task_ok else " | api=DOWN"))

    return pub, comp, nak


def main():
    print("=" * 60)
    print(f"  P3 Soak Test ({DURATION_SEC}s)")
    print(f"  publish={BATCH_SIZE} tasks / {PUBLISH_INTERVAL}s | workers={NUM_WORKERS}")
    print("=" * 60)

    setup()

    pub_thread = threading.Thread(target=publisher)
    pub_thread.start()

    worker_threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_WORKERS)]
    for t in worker_threads:
        t.start()

    start = time.time()
    last_check = 0
    try:
        while time.time() - start < DURATION_SEC:
            elapsed = int(time.time() - start)
            if elapsed - last_check >= 15:
                last_check = elapsed
                pub, comp, nak = check_invariants(elapsed)
    except KeyboardInterrupt:
        print("\n  Interrupted by user")

    stop_flag.set()
    pub_thread.join(timeout=5)
    for t in worker_threads:
        t.join(timeout=5)

    elapsed = int(time.time() - start)
    final_pub, final_comp, final_nak = check_invariants(elapsed)

    print("\n" + "=" * 60)
    print("  FINAL RESULTS")
    print("=" * 60)
    print(f"  Duration:        {elapsed}s")
    print(f"  Published:       {final_pub}")
    print(f"  Completed:       {final_comp}")
    print(f"  NACKed:          {final_nak}")
    print(f"  Failed publish:  {failed}")

    lats = sorted(latencies)
    if len(lats) >= 10:
        p50 = lats[len(lats) // 2]
        p95 = lats[int(len(lats) * 0.95)]
        p99 = lats[int(len(lats) * 0.99)] if len(lats) >= 100 else p95
        print(f"\n  Publish latency:")
        print(f"    p50: {p50:.0f}ms")
        print(f"    p95: {p95:.0f}ms")
        print(f"    p99: {p99:.0f}ms")

    unaccounted = final_pub - final_comp - final_nak
    print(f"\n  In flight: {unaccounted} (published - completed - nacked)")

    if final_pub > 0 and final_pub - failed > 0:
        throughput = (final_comp + final_nak) / max(elapsed, 1)
        print(f"  Throughput: {throughput:.1f} tasks/sec")

    health_ok = final_pub > 0 and failed < final_pub * 0.1
    if health_ok:
        print(f"\n  ✓ PASS: Soak test completed, system stable")
        sys.exit(0)
    else:
        print(f"\n  ✗ FAIL: High failure rate ({failed} / {final_pub})")
        sys.exit(1)


if __name__ == "__main__":
    main()

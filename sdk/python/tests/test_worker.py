"""JanusWorker lease-loss default-abandon behavior.

On presumed lease loss (5 consecutive heartbeat failures) the worker must
ALWAYS abandon the task: stop driving the handler, neither ACK nor NACK
(the server re-queues it once the dead lease expires), and move on to the
next poll. The optional on_lease_lost callback is a notification hook
only — it fires as the abandon takes effect and cannot prevent it.
"""

import threading
import time

import httpx

from janus_broker.client import JanusAPIError, JanusClient
from janus_broker.models import LeaseInfo, PullResult, Task
from janus_broker.worker import JanusWorker


def wait_until(cond, timeout=5.0, interval=0.005):
    """Poll an observable until it is true, under a hard deadline."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return cond()


class Lease(LeaseInfo):
    """LeaseInfo carrying the attempt number the worker reads."""

    attempt: int = 1


def _pull(task_id, lease_id="lease-1"):
    return PullResult(
        task=Task(id=task_id, mailbox_id="mb", status="pending"),
        lease=Lease(lease_id=lease_id),
    )


def _lease_lost_api_error(task_id):
    """The error the server really answers once the lease is dead: 409."""
    request = httpx.Request(
        "POST", f"http://janus/v1/tenants/t/tasks/{task_id}/heartbeat"
    )
    response = httpx.Response(
        409,
        request=request,
        json={"error": "conflict", "code": "LEASE_LOST", "message": "lease expired"},
    )
    return JanusAPIError(response)


class FakeBroker(JanusClient):
    """A JanusClient with a scripted task lifecycle.

    pull_task hands out the queued tasks in order; heartbeat fails with a
    409 LEASE_LOST JanusAPIError for scripted tasks; ack/nack are recorded
    instead of sent — the assertion for abandoned tasks is that the worker
    NEVER calls them.
    """

    def __init__(self, tasks, heartbeat_fails_for=()):
        super().__init__(base_url="http://127.0.0.1:1")
        self._queue = list(tasks)
        self._lock = threading.Lock()
        self.heartbeat_fails_for = set(heartbeat_fails_for)
        self.pulls = 0
        self.heartbeats = []
        self.acked = []
        self.nacked = []

    def pull_task(self, mailbox_id, agent_id="default"):
        with self._lock:
            self.pulls += 1
            return self._queue.pop(0) if self._queue else None

    def start_task(self, task_id, lease_id, agent_id=""):
        pass

    def heartbeat(self, task_id, lease_id, agent_id=""):
        with self._lock:
            self.heartbeats.append(task_id)
        if task_id in self.heartbeat_fails_for:
            raise _lease_lost_api_error(task_id)

    def heartbeat_agent(self, agent_id):
        return {}

    def report_progress(self, task_id, message, percent=None, data=None, agent_id="default"):
        pass

    def ack_task(self, task_id, req):
        with self._lock:
            self.acked.append(task_id)

    def nack_task(self, task_id, req):
        with self._lock:
            self.nacked.append(task_id)


def _run_worker(broker, handler, on_lease_lost=None):
    """Run the real worker loop in a thread; caller must stop() it."""
    worker = JanusWorker(
        broker,
        agent_id="agent-1",
        mailbox_id="mb",
        poll_interval=0.01,
        heartbeat_interval=0.005,
        agent_heartbeat_interval=60.0,
        on_lease_lost=on_lease_lost,
    )
    thread = threading.Thread(target=worker.run, args=(handler,), daemon=True)
    thread.start()
    return worker, thread


class TestLeaseLossDefaultAbandon:
    def test_no_callback_abandons_task_and_continues_loop(self):
        broker = FakeBroker(
            [_pull("task-a"), _pull("task-b")], heartbeat_fails_for={"task-a"}
        )
        handled = []
        ran_to_completion = []

        def handler(task, progress):
            handled.append(task.id)
            if task.id == "task-a":
                for i in range(10000):  # ~10s; abandon must cut this short
                    progress(f"step {i}")
                    time.sleep(0.001)
                ran_to_completion.append("task-a")
                return "stale-ref", None
            return f"ref-{task.id}", None

        worker, thread = _run_worker(broker, handler)
        try:
            assert wait_until(lambda: "task-b" in handled), (
                "worker never moved on to the next task after lease loss"
            )
            assert wait_until(lambda: broker.acked == ["task-b"]), (
                "the worker must still ack tasks it owns (task-b)"
            )
        finally:
            worker.stop()
            thread.join(5)

        assert handled == ["task-a", "task-b"]
        assert ran_to_completion == [], (
            "handler for the abandoned task should have been aborted via "
            "LeaseLostError from progress(), not run to completion"
        )
        assert broker.acked == ["task-b"], "abandoned task must not be acked"
        assert broker.nacked == [], (
            "abandoned task must not be nacked either — the server re-queues it"
        )

    def test_callback_fires_and_abandon_still_happens(self):
        broker = FakeBroker([_pull("task-a")], heartbeat_fails_for={"task-a"})
        notified = []
        handler_done = threading.Event()

        def on_lease_lost(task_id):
            notified.append((task_id, handler_done.is_set()))

        def handler(task, progress):
            for i in range(10000):
                progress(f"step {i}")
                time.sleep(0.001)
            handler_done.set()
            return "stale-ref", None

        worker, thread = _run_worker(broker, handler, on_lease_lost=on_lease_lost)
        try:
            assert wait_until(lambda: len(notified) == 1), (
                "on_lease_lost callback never fired"
            )
            assert wait_until(lambda: broker.pulls >= 2), (
                "worker loop never continued past the abandoned task"
            )
        finally:
            worker.stop()
            thread.join(5)

        assert notified == [("task-a", False)], (
            "callback must fire exactly once with the task id, before the abandon"
        )
        assert not handler_done.is_set(), "handler must have been aborted, not completed"
        assert broker.acked == [], "abandon must still happen despite the callback"
        assert broker.nacked == []

    def test_successful_handler_result_dropped_after_lease_loss(self):
        broker = FakeBroker([_pull("task-a")], heartbeat_fails_for={"task-a"})
        release = threading.Event()
        abandoned = threading.Event()

        def legacy_handler(task):  # single-param handler, never uses progress
            release.wait(5)
            return "ref-a", None

        def on_lease_lost(task_id):
            abandoned.set()

        worker, thread = _run_worker(broker, legacy_handler, on_lease_lost=on_lease_lost)
        try:
            # Gate on the abandon itself, not on the failing-heartbeat
            # count: the 5th append is observable while lease_lost may
            # still be unset (the append->set gap is where the CI flake
            # ACKed a lost lease). The callback fires strictly after
            # lease_lost.set(), so releasing the handler here means the
            # post-handler check deterministically sees the abandon.
            assert wait_until(abandoned.is_set), (
                "lease was never presumed lost"
            )
            release.set()
            assert wait_until(lambda: broker.pulls >= 2), (
                "worker never moved past the task whose lease was lost"
            )
        finally:
            worker.stop()
            thread.join(5)

        assert broker.acked == [], (
            "a successful result must not be acked once the lease is lost — "
            "the server re-queues the task for the next owner"
        )
        assert broker.nacked == []


class TestHealthyLease:
    def test_healthy_lease_still_acks(self):
        broker = FakeBroker([_pull("task-a")])
        worker, thread = _run_worker(broker, lambda task: ("ref-a", None))
        try:
            assert wait_until(lambda: broker.acked == ["task-a"]), (
                "a task with a healthy lease must still be acked"
            )
        finally:
            worker.stop()
            thread.join(5)
        assert broker.nacked == []

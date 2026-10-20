import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import httpx
import respx

from janus_broker import (
    JanusClient,
    LeaseLostError,
    RegisterAgentRequest,
    PublishTaskRequest,
    PullResult,
    AckRequest,
    NackRequest,
    TaskError,
    TaskEnvelope,
    Target,
    Payload,
)


BASE = "http://localhost:8080"
TENANT = "test-tenant"
PREFIX = f"/v1/tenants/{TENANT}"


@pytest.fixture
def client():
    with JanusClient(BASE, TENANT) as c:
        yield c


class TestRegisterAgent:
    def test_success(self, client):
        with respx.mock:
            respx.post(f"{BASE}{PREFIX}/agents").mock(return_value=httpx.Response(201))
            client.register_agent(RegisterAgentRequest(id="a1", display_name="Agent 1"))

    def test_server_error(self, client):
        with respx.mock:
            respx.post(f"{BASE}{PREFIX}/agents").mock(return_value=httpx.Response(500))
            with pytest.raises(httpx.HTTPStatusError):
                client.register_agent(RegisterAgentRequest(id="a1", display_name="Agent 1"))


class TestPublishTask:
    def test_success(self, client):
        with respx.mock:
            respx.post(f"{BASE}{PREFIX}/tasks").mock(
                return_value=httpx.Response(202, json={"id": "t1", "status": "queued"})
            )
            result = client.publish_task(PublishTaskRequest(
                id="t1", source_agent="a1", target_value="a2",
                envelope=TaskEnvelope(
                    task_id="t1", tenant_id=TENANT, source_agent="a1",
                    target=Target(value="a2"), payload=Payload(content='{"key":"val"}'),
                ),
            ))
            assert result["id"] == "t1"


class TestGetTask:
    def test_success(self, client):
        with respx.mock:
            respx.get(f"{BASE}{PREFIX}/tasks/t1").mock(
                return_value=httpx.Response(200, json={"id": "t1", "status": "running"})
            )
            task = client.get_task("t1")
            assert task.id == "t1"
            assert task.status == "running"


class TestPullTask:
    def test_with_task(self, client):
        with respx.mock:
            respx.post(f"{BASE}{PREFIX}/mailboxes/mb1/pull").mock(
                return_value=httpx.Response(200, json={
                    "task": {"id": "t1", "status": "pending"},
                    "lease": {"lease_id": "l1", "expires_at": "2026-01-01T00:00:00Z"},
                })
            )
            result = client.pull_task("mb1", "agent-1")
            assert result is not None
            assert result.task.id == "t1"
            assert result.lease.lease_id == "l1"

    def test_empty(self, client):
        with respx.mock:
            respx.post(f"{BASE}{PREFIX}/mailboxes/mb1/pull").mock(
                return_value=httpx.Response(204)
            )
            result = client.pull_task("mb1")
            assert result is None


class TestAckTask:
    def test_success(self, client):
        with respx.mock:
            respx.post(f"{BASE}{PREFIX}/tasks/t1/ack").mock(
                return_value=httpx.Response(200)
            )
            client.ack_task("t1", AckRequest(lease_id="l1", result_ref="result://a1/t1"))


class TestNackTask:
    def test_with_error(self, client):
        with respx.mock:
            respx.post(f"{BASE}{PREFIX}/tasks/t1/nack").mock(
                return_value=httpx.Response(200)
            )
            client.nack_task("t1", NackRequest(
                lease_id="l1", retriable=True,
                error=TaskError(code="TIMEOUT", message="timed out"),
            ))

    def test_without_error(self, client):
        with respx.mock:
            respx.post(f"{BASE}{PREFIX}/tasks/t1/nack").mock(
                return_value=httpx.Response(200)
            )
            client.nack_task("t1", NackRequest(lease_id="l1"))


class TestCancelTask:
    def test_success(self, client):
        with respx.mock:
            respx.post(f"{BASE}{PREFIX}/tasks/t1/cancel").mock(
                return_value=httpx.Response(200)
            )
            client.cancel_task("t1")


class TestGetTaskEvents:
    def test_success(self, client):
        with respx.mock:
            respx.get(f"{BASE}{PREFIX}/tasks/t1/events").mock(
                return_value=httpx.Response(200, json={
                    "events": [
                        {"event_type": "task.created", "task_id": "t1"},
                        {"event_type": "task.started", "task_id": "t1"},
                    ]
                })
            )
            events = client.get_task_events("t1")
            assert len(events) == 2
            assert events[0]["event_type"] == "task.created"


class TestHeartbeat:
    def test_success(self, client):
        with respx.mock:
            respx.post(f"{BASE}{PREFIX}/tasks/t1/heartbeat").mock(
                return_value=httpx.Response(200)
            )
            client.heartbeat("t1", "l1")


class TestStartTask:
    def test_success(self, client):
        with respx.mock:
            respx.post(f"{BASE}{PREFIX}/tasks/t1/start").mock(
                return_value=httpx.Response(200)
            )
            client.start_task("t1", "l1")


class TestWorkerSignatureCompat:
    """v1.4 compat: handlers may take (task) or (task, progress)."""

    def _make_worker(self):
        from janus_broker.client import JanusClient
        from janus_broker.worker import JanusWorker

        client = JanusClient(base_url="http://localhost:1", api_key="k", tenant_id="acme")
        return JanusWorker(client=client, agent_id="a1", mailbox_id="mb")

    @staticmethod
    def _fake_pull(task_id):
        from janus_broker.models import Task

        task = Task(id=task_id, mailbox_id="mb", envelope={"payload": {"content": "hi"}})

        class PullResult:
            def __init__(self, t):
                self.task = t
                self.lease = type("L", (), {"lease_id": "lease-1", "attempt": 1})()

        return PullResult(task)

    def test_legacy_single_param_handler(self):
        w = self._make_worker()
        called = {}
        w._client.pull_task = lambda mb, ag: self._fake_pull("t1")
        w._client.start_task = lambda tid, lid, agent_id="": called.setdefault("started", tid)
        w._client.heartbeat = lambda tid, lid, agent_id="": None
        w._client.ack_task = lambda tid, req: called.setdefault("acked", True)
        w._client.nack_task = lambda tid, req: called.setdefault("nacked", True)

        def legacy_handler(t):
            called["legacy"] = True
            return "ref-1", None

        w._process_one(legacy_handler)
        assert called.get("legacy") is True
        assert called.get("acked") is True

    def test_progress_handler_receives_progress_fn(self):
        w = self._make_worker()
        called = {}
        w._client.pull_task = lambda mb, ag: self._fake_pull("t2")
        w._client.start_task = lambda tid, lid, agent_id="": None
        w._client.heartbeat = lambda tid, lid, agent_id="": None
        w._client.ack_task = lambda tid, req: None
        w._client.nack_task = lambda tid, req: None

        def modern_handler(t, progress):
            called["progress_type"] = callable(progress)
            return "ref-2", None

        w._process_one(modern_handler)
        assert called.get("progress_type") is True


class TestStreamTask:
    def _sse(self, *events):
        return "".join(e + "\n" for e in events)

    def test_terminal_event_stops_iteration(self, client):
        body = self._sse(
            'event: task.progress\ndata: {"percent": 50}',
            'event: task.completed\ndata: {"task_id": "t1"}',
            'event: task.progress\ndata: {"never": "yielded"}',
        )
        with respx.mock:
            respx.get(f"{BASE}{PREFIX}/tasks/t1/stream").mock(
                return_value=httpx.Response(200, text=body)
            )
            events = list(client.stream_task("t1"))
        assert [e["event_type"] for e in events] == ["task.progress", "task.completed"]

    def test_lease_lost_event_raises(self, client):
        body = self._sse(
            'event: task.progress\ndata: {"n": 1}',
            'event: task.lease_lost\ndata: {"task_id": "t1"}',
            'event: task.completed\ndata: {"task_id": "t1"}',
        )
        with respx.mock:
            respx.get(f"{BASE}{PREFIX}/tasks/t1/stream").mock(
                return_value=httpx.Response(200, text=body)
            )
            seen = []
            with pytest.raises(LeaseLostError) as exc_info:
                for event in client.stream_task("t1"):
                    seen.append(event["event_type"])
        assert exc_info.value.task_id == "t1"
        assert seen == ["task.progress", "task.lease_lost"]

    def test_lease_lost_event_alias_raises(self, client):
        body = self._sse('event: lease.lost\ndata: {"task_id": "t1"}')
        with respx.mock:
            respx.get(f"{BASE}{PREFIX}/tasks/t1/stream").mock(
                return_value=httpx.Response(200, text=body)
            )
            with pytest.raises(LeaseLostError):
                list(client.stream_task("t1"))

    def test_http_409_raises_lease_lost(self, client):
        with respx.mock:
            respx.get(f"{BASE}{PREFIX}/tasks/t1/stream").mock(
                return_value=httpx.Response(409, json={"error": "lease lost"})
            )
            with pytest.raises(LeaseLostError) as exc_info:
                list(client.stream_task("t1"))
        assert exc_info.value.task_id == "t1"


class _HoldStreamHandler(BaseHTTPRequestHandler):
    """Streams one event, then holds the connection with keep-alives.

    Only a client-side close can end the stream; the next keep-alive
    write then fails and records that the SDK closed the connection.
    """

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.wfile.write(b'event: task.progress\ndata: {"n": 1}\n\n')
        self.wfile.flush()
        try:
            while not self.server.stop.wait(0.02):
                self.wfile.write(b": keep-alive\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.server.client_closed.set()

    def log_message(self, *args):
        pass


class _HoldStreamServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        self.client_closed = threading.Event()
        self.stop = threading.Event()
        super().__init__(("127.0.0.1", 0), _HoldStreamHandler)


class TestStreamTaskStopSemantics:
    def test_closing_generator_closes_connection(self):
        server = _HoldStreamServer()
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = JanusClient(f"http://127.0.0.1:{port}", TENANT)
            gen = client.stream_task("t9")
            first = next(gen)
            assert first["event_type"] == "task.progress"

            deadline = time.monotonic() + 5.0
            gen.close()  # consumer stops early: must tear the connection down
            while not server.client_closed.is_set() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert server.client_closed.is_set(), (
                "closing the generator must close the underlying response, "
                "not leave the stream connection open"
            )
            client.close()
        finally:
            server.stop.set()
            server.shutdown()
            server.server_close()

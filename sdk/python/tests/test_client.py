import pytest
import httpx
import respx

from janus_sdk import (
    JanusClient,
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

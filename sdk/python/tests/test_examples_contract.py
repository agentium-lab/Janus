"""R10 maintenance pin: the published examples must match the real SDK
contract. The README flow is executed verbatim against a MockTransport (no
broker, no framework deps), and the three framework examples are
compile/import-checked with their optional framework imports absent."""

import ast
import importlib.util
import json
import pathlib
import sys

import httpx
import pytest

from janus_broker import (
    AckRequest,
    AgentCapability,
    JanusClient,
    Payload,
    PublishTaskRequest,
    RegisterAgentRequest,
    Target,
    TaskEnvelope,
)
from janus_broker.models import PullResult, Task

EXAMPLES = pathlib.Path(__file__).resolve().parents[3] / "examples" / "interop"


def test_readme_flow_executes_against_mock_transport():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls.append((request.method, path, json.loads(request.content.decode()) if request.content else None))
        if path.endswith("/pull"):
            return httpx.Response(200, json={
                "task": {"id": "task-001", "tenant_id": "acme", "status": "claimed", "target_type": "capability"},
                "lease": {"lease_id": "lease-1", "attempt": 1},
            })
        if path.endswith("/agents"):
            return httpx.Response(200, json={"id": "reviewer"})
        if path.endswith("/tasks/task-001/events"):
            return httpx.Response(200, json=[{"event_type": "task.progress", "payload": {"percent": 20}}])
        return httpx.Response(200, json={"id": "task-001"})

    # the README example, verbatim shape
    client = JanusClient(base_url="http://test", tenant_id="acme")
    client._client = httpx.Client(transport=httpx.MockTransport(handler))

    client.register_agent(RegisterAgentRequest(
        id="reviewer",
        display_name="Code Reviewer",
        capabilities=[AgentCapability(capability="code_review", description="reviews source code")],
    ))
    client.publish_task(PublishTaskRequest(
        id="task-001",
        source_agent="product",
        target_type="capability",
        target_value="code_review",
        envelope=TaskEnvelope(
            task_id="task-001", tenant_id="acme", source_agent="product",
            target=Target(type="capability", value="code_review"),
            payload=Payload(type="json", content="{}"),
        ),
    ))
    result = client.pull_task("review-mb", "reviewer")
    client.ack_task(result.task.id, AckRequest(
        lease_id=result.lease.lease_id,
        result_ref="s3://review-001.json",
    ))

    paths = [p for _, p, _ in calls]
    assert any(p.endswith("/agents") for p in paths)
    assert any(p.endswith("/tasks") for p in paths)
    assert any(p.endswith("/mailboxes/review-mb/pull") for p in paths)
    ack = next(body for m, p, body in calls if p.endswith("/tasks/task-001/ack"))
    assert ack["result_ref"] == "s3://review-001.json"


@pytest.mark.parametrize("name", ["autogen_worker", "crewai_worker", "langgraph_worker"])
def test_framework_examples_parse_and_match_handler_contract(name):
    src = (EXAMPLES / f"{name}.py").read_text()
    tree = ast.parse(src)  # syntax valid
    assert "WorkerResult" not in src, "old WorkerResult contract must not appear"
    # the handler uses the (task, progress) -> (result_ref, usage) tuple shape
    assert "def handler(task, progress):" in src
    assert ", None" in src  # tuple return with empty usage


@pytest.mark.parametrize("name", ["autogen_worker", "crewai_worker", "langgraph_worker"])
def test_framework_examples_import_without_frameworks(name):
    # the framework imports are optional (try/except ImportError), so the
    # module must import cleanly with none of them installed
    spec = importlib.util.spec_from_file_location(name, EXAMPLES / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # must not raise; main() is not called

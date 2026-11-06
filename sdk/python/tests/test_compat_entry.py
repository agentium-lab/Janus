"""Compat entry pins: request methods accept BOTH the request models and
plain dicts (validated through the model), so every documented quick-start
shape — site homepage, quickstart guide, README — executes as written."""

import httpx
import pytest

from janus_broker.client import JanusClient
from janus_broker.models import AckRequest, PublishTaskRequest


@pytest.fixture()
def captured():
    holder = {}

    def handler(request: httpx.Request) -> httpx.Response:
        holder.setdefault("calls", []).append(
            (request.url.path, httpx.Request.read(request).decode())
        )
        if request.url.path.endswith("/pull"):
            return httpx.Response(200, json={
                "task": {"id": "task-001", "tenant_id": "acme", "status": "claimed"},
                "lease": {"lease_id": "lease-1", "attempt": 1},
            })
        return httpx.Response(200, json={"id": "task-001"})

    client = JanusClient(base_url="http://test", tenant_id="acme")
    client._client = httpx.Client(transport=httpx.MockTransport(handler))
    return holder, client


def test_publish_task_accepts_dict_and_model_with_identical_wire(captured):
    holder, client = captured
    dict_resp = client.publish_task({
        "id": "task-001", "source_agent": "product",
        "target_type": "capability", "target_value": "code_review",
    })
    model_resp = client.publish_task(PublishTaskRequest(
        id="task-001", source_agent="product",
        target_type="capability", target_value="code_review",
    ))
    assert dict_resp == model_resp == {"id": "task-001"}
    _, dict_body = holder["calls"][0]
    _, model_body = holder["calls"][1]
    assert dict_body == model_body, "dict and model must serialize identically"


def test_ack_task_accepts_dict_after_pull(captured):
    holder, client = captured
    result = client.pull_task("review-mb", "reviewer")
    client.ack_task(result.task.id, {
        "lease_id": result.lease.lease_id,
        "result_ref": "s3://result.json",
    })
    path, body = holder["calls"][-1]
    assert path.endswith("/tasks/task-001/ack")
    assert '"result_ref":"s3://result.json"' in body or '"result_ref": "s3://result.json"' in body


def test_invalid_dict_still_raises_validation_error(captured):
    _, client = captured
    with pytest.raises(Exception):
        client.publish_task({"id": 12345})  # id must be a str

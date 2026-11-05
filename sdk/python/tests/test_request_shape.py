"""P2-2 request-shape pin: the client must send the SERVER's field names.
Captured through httpx's real serialization path (MockTransport), not by
inspecting a model dump by hand."""

import json

import httpx
import pytest

from janus_broker.client import JanusClient
from janus_broker.models import RegisterAgentRequest


@pytest.fixture()
def captured():
    holder = {}

    def handler(request: httpx.Request) -> httpx.Response:
        holder["json"] = json.loads(request.content.decode())
        return httpx.Response(200, json={"id": "a1"})

    client = JanusClient(base_url="http://test", tenant_id="acme")
    client._client = httpx.Client(transport=httpx.MockTransport(handler))
    return holder, client


def test_register_agent_sends_server_field_names(captured):
    holder, client = captured
    client.register_agent(
        RegisterAgentRequest.model_validate(
            {
                "id": "a1",
                "display_name": "A1",
                "capabilities": [{"capability": "review", "schema": "{}"}],
            }
        )
    )
    body = holder["json"]
    assert body["id"] == "a1"
    cap = body["capabilities"][0]
    assert "schema" in cap and cap["schema"] == "{}"
    assert "capability_schema" not in cap, "python-side attr name leaked onto the wire"

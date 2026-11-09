"""Quickstart doc-shape pins: the exact code shown on the site's Quick Start
must execute — including the create_mailbox call that used to receive a raw
string and crash on model_dump."""

import json

import httpx
import pytest

from janus_broker import CreateMailboxRequest, JanusClient


@pytest.fixture()
def captured():
    holder = {}

    def handler(request: httpx.Request) -> httpx.Response:
        holder.setdefault("calls", []).append(
            (request.method, request.url.path,
             json.loads(request.content.decode()) if request.content else None)
        )
        if request.url.path.endswith("/pull"):
            return httpx.Response(204)
        return httpx.Response(200, json={"id": "review-mb"})

    client = JanusClient(base_url="http://test", tenant_id="acme")
    client._client = httpx.Client(transport=httpx.MockTransport(handler))
    return holder, client


def test_create_mailbox_accepts_model_and_dict(captured):
    holder, client = captured
    client.create_mailbox(CreateMailboxRequest(id="review-mb", agent_id="reviewer"))
    client.create_mailbox({"id": "ops-mb", "agent_id": "ops"})
    paths = [p for _, p, _ in holder["calls"]]
    assert paths == ["/v1/tenants/acme/mailboxes"] * 2
    assert holder["calls"][0][2]["id"] == "review-mb"
    assert holder["calls"][0][2]["agent_id"] == "reviewer"
    assert holder["calls"][1][2]["id"] == "ops-mb"
    assert holder["calls"][1][2]["agent_id"] == "ops"


def test_create_mailbox_rejects_bare_string(captured):
    _, client = captured
    # the old quickstart passed "review-mb" positionally — that must fail
    # validation loudly, never send garbage to the wire
    with pytest.raises(Exception):
        client.create_mailbox("review-mb")


def test_empty_pull_returns_none(captured):
    _, client = captured
    assert client.pull_task("review-mb", "reviewer") is None

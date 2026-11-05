"""B1 compatibility pins: the Python models must round-trip what the server
actually sends — every canonical target type, structured capabilities, and
governance fields in the envelope."""

import json

from janus_broker.models import (
    AgentCapability,
    RegisterAgentRequest,
    Task,
    TaskEnvelope,
    Target,
    TargetType,
)


def test_every_canonical_target_type_parses():
    for tt in ("agent", "mailbox", "capability", "group", "human"):
        assert TargetType(tt).value == tt
    # legacy alias still parses so old client payloads do not break
    assert TargetType("semantic").value == "semantic"


def _task_json(target_type: str) -> dict:
    return {
        "id": "t1",
        "tenant_id": "acme",
        "source_agent": "producer",
        "target_type": target_type,
        "target_value": "code_review",
        "mailbox_id": "mb1",
        "status": "queued",
        "priority": "normal",
        "attempt_count": 0,
        "replay_count": 0,
        "created_at": "2026-11-05T00:00:00Z",
        "updated_at": "2026-11-05T00:00:00Z",
        "envelope": {
            "janus_version": "1.0",
            "task_id": "t1",
            "tenant_id": "acme",
            "source_agent": "producer",
            "target": {"type": target_type, "value": "code_review"},
            "payload": {"type": "json", "content": "{}"},
        },
    }


def test_capability_and_group_tasks_parse():
    for tt in ("capability", "group", "human"):
        task = Task.model_validate(_task_json(tt))
        assert task.target_type == tt
        assert task.envelope is not None
        assert task.envelope.target.type.value == tt


def test_structured_capabilities_roundtrip():
    req = RegisterAgentRequest.model_validate(
        {
            "id": "a1",
            "display_name": "A1",
            "protocol": "a2a",
            "capabilities": [
                {"capability": "code_review", "description": "reviews", "schema": "{}"}
            ],
        }
    )
    cap = req.capabilities[0]
    assert isinstance(cap, AgentCapability)
    assert cap.capability == "code_review"
    assert cap.capability_schema == "{}"
    dumped = req.model_dump(exclude_none=True, by_alias=True)
    assert dumped["capabilities"][0]["schema"] == "{}"


def test_envelope_governance_fields_survive_roundtrip():
    env = TaskEnvelope.model_validate(
        {
            "janus_version": "1.0",
            "task_id": "t1",
            "idempotency_key": "idem-1",
            "tenant_id": "acme",
            "source_agent": "producer",
            "target": {"type": "capability", "value": "code_review"},
            "ttl_seconds": 120,
            "budget": {"max_tokens": 1000},
            "policy": {"decision_id": "d1"},
            "context_refs": [{"type": "file", "uri": "s3://x"}],
            "payload": {"type": "json", "content": "{}"},
        }
    )
    raw = json.loads(env.model_dump_json())
    assert raw["idempotency_key"] == "idem-1"
    assert raw["ttl_seconds"] == 120
    assert raw["budget"] == {"max_tokens": 1000}
    assert raw["policy"] == {"decision_id": "d1"}
    assert raw["context_refs"] == [{"type": "file", "uri": "s3://x"}]


def test_intent_resolution_shape_parses():
    # intent resolution lands on the server as a capability target before
    # the task is ever returned; the client must accept it.
    task = Task.model_validate(_task_json("capability"))
    assert task.envelope.target == Target(type="capability", value="code_review")

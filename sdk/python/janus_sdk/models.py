from pydantic import BaseModel, Field
from typing import Any, Optional
from enum import Enum


class TargetType(str, Enum):
    AGENT = "agent"
    MAILBOX = "mailbox"
    SEMANTIC = "semantic"


class Target(BaseModel):
    type: TargetType = TargetType.AGENT
    value: str = ""


class Payload(BaseModel):
    type: str = "json"
    content: str = ""


class TraceContext(BaseModel):
    trace_id: str = ""
    span_id: str = ""
    parent_span_id: str = ""


class TaskEnvelope(BaseModel):
    janus_version: str = "0.1"
    task_id: str = ""
    tenant_id: str = ""
    source_agent: str = ""
    target: Target = Field(default_factory=Target)
    priority: str = "normal"
    payload: Payload = Field(default_factory=Payload)
    trace: TraceContext = Field(default_factory=TraceContext)


class Task(BaseModel):
    id: str = ""
    tenant_id: str = ""
    source_agent: str = ""
    target_type: str = ""
    target_value: str = ""
    mailbox_id: str = ""
    status: str = ""
    priority: str = ""
    envelope: Optional[TaskEnvelope] = None
    attempt_count: int = 0
    created_at: str = ""
    updated_at: str = ""


class TaskError(BaseModel):
    code: str = ""
    message: str = ""


class RegisterAgentRequest(BaseModel):
    id: str
    display_name: str = ""
    protocol: str = "a2a"
    capabilities: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class PublishTaskRequest(BaseModel):
    id: str = ""
    source_agent: str = ""
    target_type: str = "agent"
    target_value: str = ""
    mailbox_id: str = ""
    priority: str = "normal"
    envelope: Optional[TaskEnvelope] = None


class LeaseInfo(BaseModel):
    lease_id: str = ""
    expires_at: Any = None


class PullResult(BaseModel):
    task: Optional[Task] = None
    lease: LeaseInfo = Field(default_factory=LeaseInfo)


class AckRequest(BaseModel):
    lease_id: str = ""
    result_ref: str = ""
    token_usage: Optional[dict[str, int]] = None


class NackRequest(BaseModel):
    lease_id: str = ""
    retriable: bool = False
    error: Optional[TaskError] = None

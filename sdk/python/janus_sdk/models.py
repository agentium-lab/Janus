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
    attempt: int = 0
    result_ref: str = ""
    token_usage: Optional[dict[str, int]] = None


class NackRequest(BaseModel):
    lease_id: str = ""
    attempt: int = 0
    retriable: bool = False
    error: Optional[TaskError] = None


# --- Governance / management models ---


class Tenant(BaseModel):
    id: str = ""
    name: str = ""


class AgentCapability(BaseModel):
    capability: str = ""
    description: str = ""
    capability_schema: Optional[str] = Field(None, alias="schema")

    model_config = {"populate_by_name": True}


class Agent(BaseModel):
    id: str = ""
    tenant_id: str = ""
    team_id: str = ""
    display_name: str = ""
    protocol: str = ""
    endpoint: str = ""
    status: str = ""
    description: str = ""
    capabilities: list[AgentCapability] = []
    max_concurrency: int = 0
    rpm: int = 0
    tpm: int = 0


class RegisterAgentCapability(BaseModel):
    capability: str = ""
    description: str = ""
    capability_schema: str = Field("", alias="schema")

    model_config = {"populate_by_name": True}


class Budget(BaseModel):
    max_tokens: int = 0
    max_cost_usd: float = 0.0
    model_classes: list[str] = []


class BudgetSpec(BaseModel):
    tenant_id: str = ""
    scope_type: str = ""
    scope_id: str = ""
    max_concurrency: int = 0
    rpm: int = 0
    tpm: int = 0
    daily_cost_usd: float = 0.0
    monthly_cost_usd: float = 0.0


class BudgetRequest(BaseModel):
    scope_type: str = ""
    scope_id: str = ""
    rpm: int = 0
    tpm: int = 0
    max_concurrency: int = 0
    daily_cost_usd: float = 0.0
    monthly_cost_usd: float = 0.0


class ContextRef(BaseModel):
    type: str = ""
    uri: str = ""
    hash: str = ""
    classification: str = ""
    access_scope: list[str] = []


class RetryPolicy(BaseModel):
    max_attempts: int = 0
    backoff_type: str = ""
    initial_seconds: int = 0
    max_seconds: int = 0
    jitter: bool = False


class Mailbox(BaseModel):
    id: str = ""
    tenant_id: str = ""
    agent_id: str = ""
    status: str = ""
    priority: str = ""
    max_deliver: int = 0
    retry_policy: Optional[RetryPolicy] = None


class CreateMailboxRequest(BaseModel):
    id: str = ""
    agent_id: str = ""
    max_concurrency: int = 0
    ack_wait_seconds: int = 0
    max_deliver: int = 0
    retention_seconds: int = 0


class UpdateMailboxRequest(BaseModel):
    max_concurrency: Optional[int] = None
    ack_wait_seconds: Optional[int] = None
    max_deliver: Optional[int] = None
    retention_seconds: Optional[int] = None
    status: Optional[str] = None


class MailboxActionResponse(BaseModel):
    status: str = ""


class PolicyContext(BaseModel):
    data_classification: str = ""
    requires_human_approval: bool = False
    allowed_tools: list[str] = []


class PolicyRule(BaseModel):
    tenant_id: str = ""
    id: str = ""
    name: str = ""
    status: str = ""
    priority: int = 0
    condition: Optional[dict] = None
    action: Optional[dict] = None


class PolicyRuleRequest(BaseModel):
    name: str = ""
    status: str = ""
    priority: int = 0
    condition: Optional[dict] = None
    action: Optional[dict] = None


class PolicyRuleTemplateRequest(BaseModel):
    template: str = ""
    agent_id: str = ""
    team_id: str = ""
    capability: str = ""
    tool: str = ""
    data_classification: str = ""
    name: str = ""
    status: str = ""
    priority: int = 0


class ToolInvocation(BaseModel):
    id: str = ""
    name: str = ""
    namespace: str = ""
    source_protocol: str = ""


class CreateAPIKey(BaseModel):
    id: str = ""
    key: str = ""
    name: str = ""


class CreateAPIKeyRequest(BaseModel):
    name: str = ""


class APIKey(BaseModel):
    id: str = ""
    name: str = ""
    prefix: str = ""
    status: str = ""

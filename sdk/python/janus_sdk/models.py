from pydantic import BaseModel, ConfigDict, Field
from typing import Any, Optional
from enum import Enum


class TargetType(str, Enum):
    AGENT = "agent"
    MAILBOX = "mailbox"
    CAPABILITY = "capability"
    INTENT = "intent"
    GROUP = "group"
    HUMAN = "human"


class Target(BaseModel):
    type: TargetType = TargetType.AGENT
    value: str = ""


class Payload(BaseModel):
    type: str = "json"
    content: str = ""


class ToolInvocation(BaseModel):
    id: str = ""
    name: str = ""
    namespace: str = ""
    source_protocol: str = ""


class TraceContext(BaseModel):
    trace_id: str = ""
    parent_task_id: str = ""
    span_id: str = ""
    parent_span_id: str = ""


class Budget(BaseModel):
    max_tokens: int = 0
    max_cost_usd: float = 0
    model_classes: list[str] = Field(default_factory=list)


class PolicyContext(BaseModel):
    data_classification: str = ""
    requires_human_approval: bool = False
    allowed_tools: list[str] = Field(default_factory=list)


class ContextRef(BaseModel):
    tenant_id: str = ""
    id: str = ""
    type: str = ""
    uri: str = ""
    hash: str = ""
    classification: str = ""
    access_scope: list[str] = Field(default_factory=list)
    expires_at: str = ""
    created_at: str = ""


class TaskEnvelope(BaseModel):
    janus_version: str = "0.1"
    task_id: str = ""
    idempotency_key: str = ""
    tenant_id: str = ""
    source_agent: str = ""
    target: Target = Field(default_factory=Target)
    priority: str = "normal"
    deadline: Optional[str] = None
    ttl_seconds: int = 0
    budget: Optional[Budget] = None
    policy: Optional[PolicyContext] = None
    context_refs: list[ContextRef] = Field(default_factory=list)
    payload: Payload = Field(default_factory=Payload)
    tool_invocation: Optional[ToolInvocation] = None
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
    result_ref: str = ""
    attempt_count: int = 0
    created_at: str = ""
    updated_at: str = ""
    completed_at: str = ""


class TaskError(BaseModel):
    code: str = ""
    message: str = ""


class Tenant(BaseModel):
    id: str = ""
    name: str = ""


class RegisterAgentCapability(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    capability: str
    schema_: str = Field(default="", alias="schema")
    description: str = ""


class RegisterAgentRequest(BaseModel):
    id: str
    display_name: str = ""
    team_id: str = ""
    protocol: str = "a2a"
    endpoint: str = ""
    description: str = ""
    max_concurrency: int = 0
    rpm: int = 0
    tpm: int = 0
    capabilities: list[RegisterAgentCapability | str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class AgentCapability(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    tenant_id: str = ""
    agent_id: str = ""
    capability: str = ""
    schema_: str = Field(default="", alias="schema")
    description: str = ""


class Agent(BaseModel):
    id: str = ""
    tenant_id: str = ""
    display_name: str = ""
    team_id: str = ""
    protocol: str = ""
    endpoint: str = ""
    status: str = ""
    description: str = ""
    capabilities: list[AgentCapability] = Field(default_factory=list)
    max_concurrency: int = 0
    rpm: int = 0
    tpm: int = 0
    created_at: str = ""
    updated_at: str = ""
    last_heartbeat_at: Optional[str] = None


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
    attempt: int = 0
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


class APIKey(BaseModel):
    id: str = ""
    tenant_id: str = ""
    name: str = ""
    prefix: str = ""
    created_at: str = ""
    last_used_at: Optional[str] = None
    revoked_at: Optional[str] = None


class CreateAPIKeyRequest(BaseModel):
    name: str


class CreatedAPIKey(APIKey):
    key: str = ""


class PolicyRuleRequest(BaseModel):
    id: str
    name: str
    status: str = ""
    priority: int = 0
    condition: dict[str, Any] = Field(default_factory=dict)
    action: dict[str, Any] = Field(default_factory=dict)


class PolicyRuleTemplateRequest(BaseModel):
    template: str
    agent_id: str = ""
    team_id: str = ""
    capability: str = ""
    tool: str = ""
    data_classification: str = ""
    name: str = ""
    status: str = ""
    priority: int = 0


class PolicyRule(BaseModel):
    tenant_id: str = ""
    id: str = ""
    name: str = ""
    status: str = ""
    priority: int = 0
    condition: dict[str, Any] = Field(default_factory=dict)
    action: dict[str, Any] = Field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""


class BudgetRequest(BaseModel):
    scope_type: str = "tenant"
    scope_id: str = ""
    rpm: int = 0
    tpm: int = 0
    max_concurrency: int = 0
    daily_cost_usd: float = 0
    monthly_cost_usd: float = 0


class BudgetSpec(BudgetRequest):
    tenant_id: str = ""
    created_at: str = ""
    updated_at: str = ""


class RetryPolicy(BaseModel):
    max_attempts: int = 0
    backoff_type: str = ""
    initial_seconds: int = 0
    max_seconds: int = 0
    jitter: bool = False


class Mailbox(BaseModel):
    tenant_id: str = ""
    id: str = ""
    agent_id: str = ""
    status: str = ""
    priority: str = ""
    max_concurrency: int = 0
    ack_wait_seconds: int = 0
    max_deliver: int = 0
    retention_seconds: int = 0
    retry_policy: Optional[RetryPolicy] = None
    created_at: str = ""
    updated_at: str = ""


class CreateMailboxRequest(BaseModel):
    id: str
    agent_id: str
    max_concurrency: Optional[int] = None
    ack_wait_seconds: Optional[int] = None
    max_deliver: Optional[int] = None
    retention_seconds: Optional[int] = None


class UpdateMailboxRequest(BaseModel):
    max_concurrency: Optional[int] = None
    ack_wait_seconds: Optional[int] = None
    max_deliver: Optional[int] = None
    retention_seconds: Optional[int] = None


class MailboxActionResponse(BaseModel):
    id: str = ""
    status: str = ""

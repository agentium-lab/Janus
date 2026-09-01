// Janus TypeScript SDK — shared types

export interface Tenant {
  id: string;
  name?: string;
}

export interface AgentCapability {
  capability: string;
  description?: string;
  schema?: string;
}

export interface Agent {
  id: string;
  tenant_id: string;
  team_id?: string;
  display_name: string;
  protocol: string;
  endpoint?: string;
  status: string;
  description?: string;
  capabilities: AgentCapability[];
  max_concurrency: number;
  rpm: number;
  tpm: number;
}

export interface RegisterAgentRequest {
  id: string;
  team_id?: string;
  display_name?: string;
  protocol?: string;
  endpoint?: string;
  description?: string;
  max_concurrency?: number;
  rpm?: number;
  tpm?: number;
  capabilities?: AgentCapability[];
}

export interface CreateMailboxRequest {
  id?: string;
  agent_id: string;
  max_concurrency?: number;
  ack_wait_seconds?: number;
  max_deliver?: number;
  retention_seconds?: number;
}

export interface Mailbox {
  id: string;
  tenant_id: string;
  agent_id: string;
  status: string;
}

export interface Target {
  type: string;
  value: string;
}

export interface Budget {
  max_tokens?: number;
  max_cost_usd?: number;
  model_classes?: string[];
}

export interface PolicyContext {
  data_classification?: string;
  requires_human_approval?: boolean;
  allowed_tools?: string[];
}

export interface TraceContext {
  trace_id?: string;
  parent_task_id?: string;
  span_id?: string;
}

export interface TaskError {
  code: string;
  message: string;
}

export interface TokenUsage {
  prompt_tokens?: number;
  completion_tokens?: number;
  total_tokens?: number;
}

export interface Task {
  id: string;
  tenant_id: string;
  source_agent: string;
  target_type: string;
  target_value: string;
  mailbox_id?: string;
  status: string;
  priority?: string;
  result_ref?: string;
  error?: TaskError;
  attempt_count: number;
  created_at?: string;
  updated_at?: string;
}

export interface PullResult {
  task: Task;
  lease: {
    lease_id: string;
    attempt: number;
    expires_at?: string;
  };
}

export interface AckRequest {
  lease_id: string;
  attempt: number;
  result_ref?: string;
  token_usage?: TokenUsage;
}

export interface NackRequest {
  lease_id: string;
  attempt: number;
  retriable: boolean;
  error?: TaskError;
}

export interface TaskEnvelope {
  janus_version: string;
  task_id: string;
  tenant_id: string;
  source_agent: string;
  target: { type: string; value: string };
  payload: { type: string; content: string };
  trace: { trace_id: string; parent_task_id?: string; span_id?: string };
  priority?: string;
  ttl_seconds?: number;
  deadline?: string;
  budget?: Budget;
  policy?: PolicyContext;
  idempotency_key?: string;
  context_refs?: ContextRef[];
  tool_invocation?: ToolInvocation;
}

export interface ContextRef {
  type: string;
  uri: string;
  hash?: string;
  classification?: string;
  access_scope?: string[];
}

export interface ToolInvocation {
  id: string;
  name: string;
  namespace?: string;
  source_protocol?: string;
}

export interface PublishTaskRequest {
  id?: string;
  source_agent: string;
  target_type: string;
  target_value: string;
  mailbox_id?: string;
  idempotency_key?: string;
  priority?: string;
  ttl_seconds?: number;
  deadline?: string;
  budget?: Budget;
  policy?: PolicyContext;
  trace?: TraceContext;
  envelope: TaskEnvelope;}

export interface BudgetRequest {
  scope_type: string;
  scope_id: string;
  rpm?: number;
  tpm?: number;
  max_concurrency?: number;
  daily_cost_usd?: number;
  monthly_cost_usd?: number;
}

export interface BudgetSpec {
  tenant_id: string;
  scope_type: string;
  scope_id: string;
  max_concurrency: number;
  rpm: number;
  tpm: number;
  daily_cost_usd: number;
  monthly_cost_usd: number;
}

export interface PolicyRuleTemplateRequest {
  template: string;
  agent_id?: string;
  team_id?: string;
  capability?: string;
  tool?: string;
  data_classification?: string;
  name?: string;
  priority?: number;
}

export interface PolicyRule {
  tenant_id: string;
  id: string;
  name: string;
  status: string;
  priority: number;
}

export interface CreateAPIKeyRequest {
  name: string;
}

export interface CreatedAPIKey {
  id: string;
  key: string;
  name: string;
}

export interface APIKey {
  id: string;
  name: string;
  prefix: string;
  status: string;
}

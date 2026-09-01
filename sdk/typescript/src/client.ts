// Janus TypeScript SDK — HTTP client

import { JanusAPIError } from "./error";
import type {
  Agent,
  RegisterAgentRequest,
  CreateMailboxRequest,
  Mailbox,
  Task,
  PublishTaskRequest,
  PullResult,
  AckRequest,
  NackRequest,
  BudgetRequest,
  BudgetSpec,
  PolicyRule,
  PolicyRuleTemplateRequest,
  CreatedAPIKey,
  APIKey,
  CreateAPIKeyRequest,
} from "./types";

export interface ClientConfig {
  baseURL: string;
  tenantID: string;
  apiKey?: string;
  timeout?: number;
}

export class Client {
  private baseURL: string;
  private tenantID: string;
  private apiKey?: string;
  private timeout: number;

  constructor(config: ClientConfig) {
    this.baseURL = config.baseURL.replace(/\/$/, "");
    this.tenantID = config.tenantID;
    this.apiKey = config.apiKey;
    this.timeout = config.timeout || 30000;
  }

  private get prefix(): string {
    return `/v1/tenants/${this.tenantID}`;
  }

  private url(path: string): string {
    return `${this.baseURL}${this.prefix}${path}`;
  }

  private async doFetch(method: string, path: string, body?: unknown): Promise<Response> {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this.apiKey) headers["X-API-Key"] = this.apiKey;

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);

    try {
      const resp = await fetch(this.url(path), {
        method,
        headers,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
      if (!resp.ok) {
        let errBody: { error?: string; code?: string; message?: string; status?: number } = {};
        try { errBody = await resp.json() as any; } catch { /* non-JSON error */ }
        throw new JanusAPIError(resp.status, errBody);
      }
      return resp;
    } finally {
      clearTimeout(timer);
    }
  }

  private async json<T>(method: string, path: string, body?: unknown): Promise<T> {
    const resp = await this.doFetch(method, path, body);
    return resp.json() as Promise<T>;
  }

  // --- Tenant ---

  async getTenant(id: string): Promise<{ id: string }> {
    return this.json("GET", `/../${id}`);
  }

  // --- Agent ---

  async registerAgent(req: RegisterAgentRequest): Promise<{ id: string }> {
    return this.json("POST", "/agents", req);
  }

  async listAgents(): Promise<Agent[]> {
    const data = await this.json<{ agents?: Agent[] }>("GET", "/agents");
    return data.agents || [];
  }

  async getAgent(agentID: string): Promise<Agent> {
    return this.json("GET", `/agents/${agentID}`);
  }

  async heartbeatAgent(agentID: string): Promise<void> {
    await this.doFetch("POST", `/agents/${agentID}/heartbeat`);
  }

  // --- Mailbox ---

  async createMailbox(req: CreateMailboxRequest): Promise<Mailbox> {
    return this.json("POST", "/mailboxes", req);
  }

  async getMailbox(mailboxID: string): Promise<Mailbox> {
    return this.json("GET", `/mailboxes/${mailboxID}`);
  }

  async updateMailbox(mailboxID: string, config: { max_concurrency?: number; ack_wait_seconds?: number; max_deliver?: number; retention_seconds?: number }): Promise<Mailbox> {
    return this.json("PATCH", `/mailboxes/${mailboxID}`, config);
  }

  async pauseMailbox(mailboxID: string): Promise<{ status: string }> {
    return this.json("POST", `/mailboxes/${mailboxID}/pause`);
  }

  async resumeMailbox(mailboxID: string): Promise<{ status: string }> {
    return this.json("POST", `/mailboxes/${mailboxID}/resume`);
  }

  // --- Task ---

  async publishTask(req: PublishTaskRequest): Promise<Task> {
    const body: Record<string, unknown> = { ...req };
    // Server requires a nested envelope; auto-construct from flat fields
    // when the caller didn't provide one explicitly.
    if (!req.envelope) {
      const taskID = req.id ?? `ts-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      body.id = taskID;
      body.envelope = {
        janus_version: "1.0",
        task_id: taskID,
        tenant_id: this.tenantID,
        source_agent: req.source_agent,
        target: { type: req.target_type, value: req.target_value },
        payload: { type: "application/json", content: "{}" },
        trace: { trace_id: `ts-${taskID}` },
        ...(req.priority && { priority: req.priority }),
        ...(req.ttl_seconds && { ttl_seconds: req.ttl_seconds }),
        ...(req.deadline && { deadline: req.deadline }),
        ...(req.budget && { budget: req.budget }),
        ...(req.policy && { policy: req.policy }),
        ...(req.idempotency_key && { idempotency_key: req.idempotency_key }),
      };
    }
    return this.json("POST", "/tasks", body);
  }

  async getTask(taskID: string): Promise<Task> {
    return this.json("GET", `/tasks/${taskID}`);
  }

  async cancelTask(taskID: string): Promise<void> {
    await this.doFetch("POST", `/tasks/${taskID}/cancel`);
  }

  async replayTask(taskID: string): Promise<Task> {
    return this.json("POST", `/tasks/${taskID}/replay`);
  }

  async getTaskEvents(taskID: string): Promise<unknown[]> {
    const data = await this.json<{ events?: unknown[] } | unknown[]>("GET", `/tasks/${taskID}/events`);
    return Array.isArray(data) ? data : (data as { events?: unknown[] }).events || [];
  }

  // --- Dispatch lifecycle ---

  async pullTask(mailboxID: string, agentID: string): Promise<PullResult | null> {
    const resp = await this.doFetch("POST", `/mailboxes/${mailboxID}/pull`, { agent_id: agentID });
    if (resp.status === 204) return null;
    const text = await resp.text();
    if (!text) return null;
    return JSON.parse(text) as PullResult;
  }

  async startTask(taskID: string, attempt: number, leaseID: string): Promise<void> {
    await this.doFetch("POST", `/tasks/${taskID}/start`, { attempt, lease_id: leaseID });
  }

  async heartbeat(taskID: string, attempt: number, leaseID: string): Promise<void> {
    await this.doFetch("POST", `/tasks/${taskID}/heartbeat`, { attempt, lease_id: leaseID });
  }

  async ackTask(taskID: string, req: AckRequest): Promise<void> {
    await this.doFetch("POST", `/tasks/${taskID}/ack`, req);
  }

  async nackTask(taskID: string, req: NackRequest): Promise<void> {
    await this.doFetch("POST", `/tasks/${taskID}/nack`, req);
  }

  // --- API Keys ---

  async createAPIKey(req: CreateAPIKeyRequest): Promise<CreatedAPIKey> {
    return this.json("POST", "/api-keys", req);
  }

  async listAPIKeys(): Promise<APIKey[]> {
    const data = await this.json<{ api_keys?: APIKey[] }>("GET", "/api-keys");
    return data.api_keys || [];
  }

  async revokeAPIKey(keyID: string): Promise<APIKey> {
    return this.json("POST", `/api-keys/${keyID}/revoke`);
  }

  // --- Governance ---

  async createPolicyRuleFromTemplate(req: PolicyRuleTemplateRequest): Promise<PolicyRule> {
    return this.json("POST", "/policy-rules/templates", req);
  }

  async listPolicyRules(): Promise<PolicyRule[]> {
    const data = await this.json<{ policy_rules?: PolicyRule[] }>("GET", "/policy-rules");
    return data.policy_rules || [];
  }

  async upsertBudget(req: BudgetRequest): Promise<BudgetSpec> {
    return this.json("POST", "/budgets", req);
  }

  async getBudget(scopeType: string, scopeID: string): Promise<BudgetSpec> {
    return this.json("GET", `/budgets/${scopeType}/${scopeID}`);
  }

  async listBudgets(): Promise<BudgetSpec[]> {
    const data = await this.json<{ budgets?: BudgetSpec[] }>("GET", "/budgets");
    return data.budgets || [];
  }

  // --- DLQ ---

  async queryDLQ(opts?: { mailbox?: string; limit?: number }): Promise<Task[]> {
    const params = new URLSearchParams();
    if (opts?.mailbox) params.set("mailbox", opts.mailbox);
    if (opts?.limit) params.set("limit", String(opts.limit));
    const qs = params.toString();
    const path = qs ? `/dlq?${qs}` : "/dlq";
    const data = await this.json<{ tasks?: Task[] }>("GET", path);
    return data.tasks || [];
  }

  async replayDLQ(taskID: string): Promise<Task> {
    return this.json("POST", `/dlq/${taskID}/replay`);
  }

  async discardDLQ(taskID: string): Promise<void> {
    await this.doFetch("POST", `/dlq/${taskID}/discard`);
  }

  async createPolicyRule(req: { name: string; status?: string; priority?: number; condition?: unknown; action?: unknown }): Promise<PolicyRule> {
    return this.json("POST", "/policy-rules", req);
  }
}

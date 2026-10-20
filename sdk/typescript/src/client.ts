// Janus TypeScript SDK — HTTP client

import { JanusAPIError, LeaseLostError } from "./error";
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

function done0(timer: ReturnType<typeof setTimeout>, signal: AbortSignal | undefined, onAbort: () => void): void {
  clearTimeout(timer);
  signal?.removeEventListener("abort", onAbort);
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

  // The timeout covers connect, headers, AND body reads: aborting the
  // controller interrupts an in-flight body read too, so the timer stays
  // armed until the caller has consumed the body.
  private async doFetch(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<{ resp: Response; done: () => void }> {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this.apiKey) headers["X-API-Key"] = this.apiKey;

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    // Caller cancellation composes with the timeout.
    const onAbort = () => controller.abort();
    signal?.addEventListener("abort", onAbort);

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
      const done = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
      };
      return { resp, done };
    } catch (e) {
      done0(timer, signal, onAbort);
      throw e;
    }
  }

  // Body-less request: consumes nothing, so the timeout timer MUST be
  // cleaned up here — leaking it kept a live abort timer per call.
  private async voidCall(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<void> {
    const { done } = await this.doFetch(method, path, body, signal);
    done();
  }

  private async json<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    const { resp, done } = await this.doFetch(method, path, body, signal);
    try {
      return await resp.json() as T;
    } finally {
      done();
    }
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
    await this.voidCall("POST", `/agents/${agentID}/heartbeat`);
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
        payload: req.payload ?? { type: "application/json", content: "{}" },
        trace: { trace_id: `ts-${taskID}` },
        ...(req.priority && { priority: req.priority }),
        ...(req.ttl_seconds && { ttl_seconds: req.ttl_seconds }),
        ...(req.deadline && { deadline: req.deadline }),
        ...(req.budget && { budget: req.budget }),
        ...(req.policy && { policy: req.policy }),
        ...(req.idempotency_key && { idempotency_key: req.idempotency_key }),
      };
    }
    // Server returns the CreateTaskResponse envelope {id, status, task};
    // the Task itself is nested. (Contract verified against TaskHandler.)
    const resp = await this.json<{ id: string; status: string; task: Task }>("POST", "/tasks", body);
    return resp.task;
  }

  async getTask(taskID: string): Promise<Task> {
    return this.json("GET", `/tasks/${taskID}`);
  }

  async cancelTask(taskID: string): Promise<void> {
    await this.voidCall("POST", `/tasks/${taskID}/cancel`);
  }

  async reportProgress(taskID: string, message: string, opts?: {
    percent?: number; data?: Record<string, unknown>; agentID?: string;
  }): Promise<void> {
    const body: Record<string, unknown> = {
      message,
      agent_id: opts?.agentID ?? "default",
    };
    if (opts?.percent !== undefined) body.percent = opts.percent;
    if (opts?.data !== undefined) body.data = opts.data;
    await this.json("POST", `/tasks/${taskID}/progress`, body);
  }

  streamTask(taskID: string, signal?: AbortSignal): AsyncGenerator<{ event_type: string; [key: string]: unknown }> {
    const url = `${this.baseURL}/v1/tenants/${this.tenantID}/tasks/${taskID}/stream`;
    const terminal = new Set([
      "task.completed", "task.failed", "task.cancelled",
      "task.dead_lettered", "task.expired",
    ]);
    // Lease-loss terminal signals: the lease backing the stream is gone;
    // the consumer gets a typed LeaseLostError instead of a stream that
    // waits forever for a terminal state that will never arrive.
    const leaseLost = new Set(["task.lease_lost", "lease.lost"]);
    const self = this;
    async function* gen() {
      // Stream-stop semantics: whatever ends the generator — a terminal
      // event, consumer break/return(), a thrown error, or an aborted
      // caller signal — must tear down the underlying fetch. The finally
      // block cancels the reader and aborts the request so the socket
      // closes promptly; an aborted signal composes via onAbort.
      const controller = new AbortController();
      const onAbort = () => controller.abort();
      signal?.addEventListener("abort", onAbort);
      try {
        const resp = await fetch(url, {
          headers: self.apiKey ? { "X-API-Key": self.apiKey } : {},
          signal: controller.signal,
        });
        if (resp.status === 409) throw new LeaseLostError(taskID);
        if (!resp.ok || !resp.body) throw new Error(`stream failed: ${resp.status}`);
        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buf = "";
        let eventType = "";
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) return;
            buf += decoder.decode(value, { stream: true });
            const lines = buf.split("\n");
            buf = lines.pop() ?? "";
            for (const line of lines) {
              if (line.startsWith("event: ")) {
                eventType = line.slice(7).trim();
              } else if (line.startsWith("data: ") && eventType) {
                let data: Record<string, unknown> = {};
                try { data = JSON.parse(line.slice(6)); } catch {}
                const evt = { event_type: eventType, ...data };
                yield evt;
                if (leaseLost.has(eventType)) throw new LeaseLostError(taskID);
                if (terminal.has(eventType)) return;
              }
            }
          }
        } finally {
          await reader.cancel().catch(() => {});
          controller.abort();
        }
      } finally {
        signal?.removeEventListener("abort", onAbort);
      }
    }
    return gen();
  }

  async replayTask(taskID: string): Promise<Task> {
    return this.json("POST", `/tasks/${taskID}/replay`);
  }

  async getTaskEvents(taskID: string): Promise<unknown[]> {
    const data = await this.json<{ events?: unknown[] } | unknown[]>("GET", `/tasks/${taskID}/events`);
    return Array.isArray(data) ? data : (data as { events?: unknown[] }).events || [];
  }

  // --- Dispatch lifecycle ---

  async pullTask(mailboxID: string, agentID: string, signal?: AbortSignal): Promise<PullResult | null> {
    const { resp, done } = await this.doFetch("POST", `/mailboxes/${mailboxID}/pull`, { agent_id: agentID }, signal);
    try {
      if (resp.status === 204) return null;
      const text = await resp.text();
      if (!text) return null;
      return JSON.parse(text) as PullResult;
    } finally {
      done();
    }
  }

  async startTask(taskID: string, attempt: number, leaseID: string, agentID?: string): Promise<void> {
    await this.voidCall("POST", `/tasks/${taskID}/start`, { attempt, lease_id: leaseID, agent_id: agentID });
  }

  async heartbeat(taskID: string, attempt: number, leaseID: string, agentID?: string): Promise<void> {
    await this.voidCall("POST", `/tasks/${taskID}/heartbeat`, { attempt, lease_id: leaseID, agent_id: agentID });
  }

  async ackTask(taskID: string, req: AckRequest, agentID?: string): Promise<void> {
    await this.voidCall("POST", `/tasks/${taskID}/ack`, agentID ? { ...req, agent_id: agentID } : req);
  }

  async nackTask(taskID: string, req: NackRequest, agentID?: string): Promise<void> {
    await this.voidCall("POST", `/tasks/${taskID}/nack`, agentID ? { ...req, agent_id: agentID } : req);
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
    await this.voidCall("POST", `/dlq/${taskID}/discard`);
  }

  async createPolicyRule(req: { name: string; status?: string; priority?: number; condition?: unknown; action?: unknown }): Promise<PolicyRule> {
    return this.json("POST", "/policy-rules", req);
  }
}

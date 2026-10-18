// Janus TypeScript SDK — Worker helper

import { Client } from "./client";
import type { Task, TokenUsage } from "./types";

export type ProgressFn = (message: string, percent?: number, data?: Record<string, unknown>) => void;
// Backward compatible: progress is optional — old 2-param handlers still work.
export type TaskHandler = (task: Task, agentID: string, progress?: ProgressFn, signal?: AbortSignal) => Promise<{ resultRef: string; usage?: TokenUsage }>;

export interface WorkerConfig {
  agentID: string;
  mailboxID: string;
  pollIntervalMs?: number;
  heartbeatIntervalMs?: number;
  /** Retry delay after a pull error (default 1s, doubling to max). */
  errorBackoffMs?: number;
  maxErrorBackoffMs?: number;
  /** Agent-liveness heartbeat interval (default 30s) — without it the
   *  sweeper offlines the agent while it processes tasks. */
  agentHeartbeatIntervalMs?: number;
}

export class JanusWorker {
  private client: Client;
  private config: Required<WorkerConfig>;
  private stopped = false;

  constructor(client: Client, config: WorkerConfig) {
    this.client = client;
    this.config = {
      agentID: config.agentID,
      mailboxID: config.mailboxID,
      pollIntervalMs: config.pollIntervalMs ?? 2000,
      heartbeatIntervalMs: config.heartbeatIntervalMs ?? 30000,
      errorBackoffMs: config.errorBackoffMs ?? 1000,
      maxErrorBackoffMs: config.maxErrorBackoffMs ?? 30000,
      agentHeartbeatIntervalMs: config.agentHeartbeatIntervalMs ?? 30000,
    };
  }

  async run(handler: TaskHandler): Promise<void> {
    // Agent-liveness heartbeat: task heartbeats do NOT count as agent
    // heartbeats — without this the sweeper offlines the agent.
    const agentTimer = setInterval(() => {
      this.client.heartbeatAgent(this.config.agentID).catch(() => {});
    }, this.config.agentHeartbeatIntervalMs);

    let backoff = this.config.errorBackoffMs;
    try {
      while (!this.stopped) {
        try {
          await this.processOne(handler);
          backoff = this.config.errorBackoffMs;
        } catch (e) {
          console.error("janus worker:", e);
          await this.sleep(backoff);
          backoff = Math.min(backoff * 2, this.config.maxErrorBackoffMs);
        }
      }
    } finally {
      clearInterval(agentTimer);
    }
  }

  stop(): void {
    this.stopped = true;
  }

  private async processOne(handler: TaskHandler): Promise<void> {
    const result = await this.client.pullTask(this.config.mailboxID, this.config.agentID);
    if (!result) {
      await this.sleep(this.config.pollIntervalMs);
      return;
    }

    const task = result.task;
    const leaseID = result.lease.lease_id;
    const attempt = result.lease.attempt;

    await this.client.startTask(task.id, attempt, leaseID, this.config.agentID);

    // Heartbeat timer
    let hbFailures = 0;
    // Cooperative cancellation: sustained heartbeat failure means the
    // lease is gone and the task will be redelivered — handlers that
    // accept the signal can stop burning compute.
    const leaseCtl = new AbortController();
    const hbTimer = setInterval(() => {
      this.client.heartbeat(task.id, attempt, leaseID, this.config.agentID).then(() => {
        hbFailures = 0;
      }).catch(() => {
        hbFailures++;
        if (hbFailures >= 5) {
          console.error(`janus worker: lease for ${task.id} presumed lost after 5 heartbeat failures; cancelling handler`);
          leaseCtl.abort();
        }
      });
    }, this.config.heartbeatIntervalMs);

    const progress: ProgressFn = (message, percent, data) => {
      // Fire-and-forget: progress failures never block task processing.
      this.client.reportProgress(task.id, message, {
        percent, data, agentID: this.config.agentID,
      }).catch(() => {});
    };

    try {
      const { resultRef, usage } = await handler(task, this.config.agentID, progress, leaseCtl.signal);
      clearInterval(hbTimer);
      const ack: import("./types").AckRequest = {
        lease_id: leaseID, attempt, result_ref: resultRef,
      };
      if (usage) ack.token_usage = usage;
      // ACK transport failure is NOT a business failure — the handler
      // SUCCEEDED. Log and let the lease expire; the task will NOT be
      // NACKed as HANDLER_ERROR (the previous catch conflated the two
      // and re-queued successfully-completed work).
      try {
        await this.client.ackTask(task.id, ack, this.config.agentID);
      } catch (ackErr) {
        console.error(`janus worker: ack transport failed for ${task.id} (handler succeeded):`, ackErr);
      }
    } catch (e) {
      clearInterval(hbTimer);
      // This is a genuine HANDLER failure — NACK is correct.
      await this.client.nackTask(task.id, {
        lease_id: leaseID, attempt, retriable: true,
        error: { code: "HANDLER_ERROR", message: String(e) },
      }, this.config.agentID);
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

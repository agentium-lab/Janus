// Janus TypeScript SDK — Worker helper

import { Client } from "./client";
import type { Task, TokenUsage } from "./types";

export type ProgressFn = (message: string, percent?: number, data?: Record<string, unknown>) => void;
// Backward compatible: progress is optional — old 2-param handlers still work.
export type TaskHandler = (task: Task, agentID: string, progress?: ProgressFn) => Promise<{ resultRef: string; usage?: TokenUsage }>;

export interface WorkerConfig {
  agentID: string;
  mailboxID: string;
  pollIntervalMs?: number;
  heartbeatIntervalMs?: number;
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
    };
  }

  async run(handler: TaskHandler): Promise<void> {
    while (!this.stopped) {
      try {
        await this.processOne(handler);
      } catch (e) {
        console.error("janus worker:", e);
      }
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

    await this.client.startTask(task.id, attempt, leaseID);

    // Heartbeat timer
    const hbTimer = setInterval(() => {
      this.client.heartbeat(task.id, attempt, leaseID).catch(() => {});
    }, this.config.heartbeatIntervalMs);

    const progress: ProgressFn = (message, percent, data) => {
      // Fire-and-forget: progress failures never block task processing.
      this.client.reportProgress(task.id, message, {
        percent, data, agentID: this.config.agentID,
      }).catch(() => {});
    };

    try {
      const { resultRef, usage } = await handler(task, this.config.agentID, progress);
      clearInterval(hbTimer);
      const ack: import("./types").AckRequest = {
        lease_id: leaseID, attempt, result_ref: resultRef,
      };
      if (usage) ack.token_usage = usage;
      await this.client.ackTask(task.id, ack);
    } catch (e) {
      clearInterval(hbTimer);
      await this.client.nackTask(task.id, {
        lease_id: leaseID, attempt, retriable: true,
        error: { code: "HANDLER_ERROR", message: String(e) },
      });
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

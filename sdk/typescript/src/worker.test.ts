// Worker tests: JanusWorker is driven against a RecordingClient subclass that
// overrides every Client method the worker touches, so the pull -> start ->
// heartbeat -> handler -> ack/nack lifecycle is observed without any network.

import { test } from "node:test";
import { strict as assert } from "node:assert";

import { Client } from "./client";
import { JanusWorker, type TaskHandler } from "./worker";
import type { AckRequest, NackRequest, PullResult, Task } from "./types";

interface ProgressOpts {
  percent?: number;
  data?: Record<string, unknown>;
  agentID?: string;
}

class RecordingClient extends Client {
  pullQueue: Array<PullResult | null> = [];
  pullCalls: Array<[string, string]> = [];
  startCalls: Array<[string, number, string]> = [];
  heartbeatCalls: Array<[string, number, string]> = [];
  ackCalls: Array<[string, AckRequest]> = [];
  nackCalls: Array<[string, NackRequest]> = [];
  progressCalls: Array<[string, string, ProgressOpts]> = [];
  private pullIndex = 0;

  constructor() {
    super({ baseURL: "http://127.0.0.1:9", tenantID: "T1" });
  }

  override async pullTask(mailboxID: string, agentID: string): Promise<PullResult | null> {
    this.pullCalls.push([mailboxID, agentID]);
    const next = this.pullQueue[this.pullIndex];
    this.pullIndex += 1;
    return next === undefined ? null : next;
  }

  override async startTask(taskID: string, attempt: number, leaseID: string): Promise<void> {
    this.startCalls.push([taskID, attempt, leaseID]);
  }

  override async heartbeat(taskID: string, attempt: number, leaseID: string): Promise<void> {
    this.heartbeatCalls.push([taskID, attempt, leaseID]);
  }

  override async ackTask(taskID: string, req: AckRequest): Promise<void> {
    this.ackCalls.push([taskID, req]);
  }

  override async nackTask(taskID: string, req: NackRequest): Promise<void> {
    this.nackCalls.push([taskID, req]);
  }

  override async reportProgress(taskID: string, message: string, opts?: ProgressOpts): Promise<void> {
    this.progressCalls.push([taskID, message, opts ?? {}]);
  }
}

function makeTask(id: string): Task {
  return {
    id,
    tenant_id: "T1",
    source_agent: "producer",
    target_type: "mailbox",
    target_value: "mb-1",
    status: "leased",
    attempt_count: 1,
  };
}

function makePull(id: string, leaseID = "lease-1", attempt = 1): PullResult {
  return { task: makeTask(id), lease: { lease_id: leaseID, attempt } };
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test("worker: pulls, starts the lease, invokes the handler with a progress fn, and acks", async (t) => {
  const client = new RecordingClient();
  client.pullQueue = [makePull("t-1", "lease-9", 3)];
  const worker = new JanusWorker(client, { agentID: "ag-1", mailboxID: "mb-1", pollIntervalMs: 5 });
  t.after(() => worker.stop());

  let sawTaskID = "";
  let sawAgentID = "";
  let sawProgressFn = false;
  await worker.run(async (task, agentID, progress) => {
    sawTaskID = task.id;
    sawAgentID = agentID;
    sawProgressFn = typeof progress === "function";
    worker.stop();
    return { resultRef: "mem://done", usage: { total_tokens: 42 } };
  });

  assert.deepEqual(client.pullCalls, [["mb-1", "ag-1"]]);
  assert.deepEqual(client.startCalls, [["t-1", 3, "lease-9"]]);
  assert.equal(sawTaskID, "t-1");
  assert.equal(sawAgentID, "ag-1");
  assert.ok(sawProgressFn);
  assert.deepEqual(client.ackCalls, [
    ["t-1", { lease_id: "lease-9", attempt: 3, result_ref: "mem://done", token_usage: { total_tokens: 42 } }],
  ]);
  assert.equal(client.nackCalls.length, 0);
});

test("worker: ack omits token_usage when the handler returns none", async (t) => {
  const client = new RecordingClient();
  client.pullQueue = [makePull("t-2")];
  const worker = new JanusWorker(client, { agentID: "ag-1", mailboxID: "mb-1", pollIntervalMs: 5 });
  t.after(() => worker.stop());

  await worker.run(async () => {
    worker.stop();
    return { resultRef: "mem://plain" };
  });

  assert.equal(client.ackCalls.length, 1);
  const [taskID, ack] = client.ackCalls[0];
  assert.equal(taskID, "t-2");
  assert.equal(ack.lease_id, "lease-1");
  assert.equal(ack.result_ref, "mem://plain");
  assert.ok(!("token_usage" in ack));
});

test("worker: handler error nacks with HANDLER_ERROR and never acks", async (t) => {
  const client = new RecordingClient();
  client.pullQueue = [makePull("t-3", "lease-x", 2)];
  const worker = new JanusWorker(client, { agentID: "ag-1", mailboxID: "mb-1", pollIntervalMs: 5 });
  t.after(() => worker.stop());

  await worker.run(async () => {
    worker.stop();
    throw new Error("kaboom");
  });

  assert.equal(client.ackCalls.length, 0);
  assert.deepEqual(client.nackCalls, [
    [
      "t-3",
      { lease_id: "lease-x", attempt: 2, retriable: true, error: { code: "HANDLER_ERROR", message: "Error: kaboom" } },
    ],
  ]);
});

test("worker: empty poll sleeps and retries; the handler runs only for a real task", async (t) => {
  const client = new RecordingClient();
  client.pullQueue = [null, makePull("t-4")];
  const worker = new JanusWorker(client, { agentID: "ag-1", mailboxID: "mb-1", pollIntervalMs: 5 });
  t.after(() => worker.stop());

  let invocations = 0;
  await worker.run(async () => {
    invocations += 1;
    worker.stop();
    return { resultRef: "r" };
  });

  assert.equal(invocations, 1);
  assert.deepEqual(client.pullCalls, [
    ["mb-1", "ag-1"],
    ["mb-1", "ag-1"],
  ]);
  assert.equal(client.startCalls.length, 1);
});

test("worker: progress fn reports message, percent and data under the worker agentID", async (t) => {
  const client = new RecordingClient();
  client.pullQueue = [makePull("t-5")];
  const worker = new JanusWorker(client, { agentID: "ag-7", mailboxID: "mb-1", pollIntervalMs: 5 });
  t.after(() => worker.stop());

  await worker.run(async (_task, _agentID, progress) => {
    worker.stop();
    assert.ok(progress);
    progress("working", 25, { step: 2 });
    progress("tick");
    return { resultRef: "r" };
  });

  assert.equal(client.progressCalls.length, 2);
  const [taskID1, msg1, opts1] = client.progressCalls[0];
  assert.equal(taskID1, "t-5");
  assert.equal(msg1, "working");
  assert.equal(opts1.percent, 25);
  assert.deepEqual(opts1.data, { step: 2 });
  assert.equal(opts1.agentID, "ag-7");
  const [taskID2, msg2, opts2] = client.progressCalls[1];
  assert.equal(taskID2, "t-5");
  assert.equal(msg2, "tick");
  assert.equal(opts2.percent, undefined);
  assert.equal(opts2.agentID, "ag-7");
});

test("worker: two-parameter handlers from the old signature still work", async (t) => {
  const client = new RecordingClient();
  client.pullQueue = [makePull("t-6")];
  const worker = new JanusWorker(client, { agentID: "ag-1", mailboxID: "mb-1", pollIntervalMs: 5 });
  t.after(() => worker.stop());

  const legacyHandler: TaskHandler = async (task, agentID) => {
    worker.stop();
    assert.equal(task.id, "t-6");
    assert.equal(agentID, "ag-1");
    return { resultRef: "legacy" };
  };
  await worker.run(legacyHandler);

  assert.equal(client.ackCalls.length, 1);
  assert.equal(client.ackCalls[0][1].result_ref, "legacy");
});

test("worker: heartbeats the lease while the handler is running", async (t) => {
  const client = new RecordingClient();
  client.pullQueue = [makePull("t-7")];
  const worker = new JanusWorker(client, {
    agentID: "ag-1",
    mailboxID: "mb-1",
    pollIntervalMs: 5,
    heartbeatIntervalMs: 5,
  });
  t.after(() => worker.stop());

  await worker.run(async () => {
    await delay(25);
    worker.stop();
    return { resultRef: "r" };
  });

  assert.ok(client.heartbeatCalls.length >= 1);
  assert.deepEqual(client.heartbeatCalls[0], ["t-7", 1, "lease-1"]);
});

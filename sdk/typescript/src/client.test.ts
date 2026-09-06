// Client tests: a real node:http server on 127.0.0.1 (random port) plays the
// Janus API, so requests go through real fetch/HTTP while the server records
// method/path/headers/body for wire-shape assertions. Each test installs its
// own responder; without one the server answers 404. Uses only node:test +
// node:assert.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, beforeEach, test } from "node:test";
import { strict as assert } from "node:assert";

import { Client } from "./client";
import { JanusAPIError } from "./error";
import type { Task, TaskEnvelope } from "./types";

interface CapturedRequest {
  method: string;
  url: string;
  headers: IncomingMessage["headers"];
  body: any;
}

type Responder = (req: IncomingMessage, res: ServerResponse, rawBody: string) => void;

function notFound(): Responder {
  return (_req, res) => {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "no responder configured" }));
  };
}

const captured: CapturedRequest[] = [];
let responder: Responder = notFound();

const server: Server = createServer(async (req, res) => {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const raw = Buffer.concat(chunks).toString("utf8");
  let body: any;
  if (raw) {
    try {
      body = JSON.parse(raw);
    } catch {
      body = raw;
    }
  }
  captured.push({ method: req.method ?? "", url: req.url ?? "", headers: req.headers, body });
  responder(req, res, raw);
});

let baseURL = "";

before(async () => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  baseURL = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
});

after(async () => {
  // Undici keeps idle keep-alive sockets open; drop them so close() resolves.
  server.closeAllConnections();
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

beforeEach(() => {
  captured.length = 0;
  responder = notFound();
});

function makeClient(apiKey?: string): Client {
  // Trailing slash on purpose: the constructor must strip it.
  return new Client({
    baseURL: `${baseURL}/`,
    tenantID: "T1",
    ...(apiKey ? { apiKey } : {}),
  });
}

function jsonResponder(obj: unknown, status = 200): Responder {
  return (_req, res) => {
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end(JSON.stringify(obj));
  };
}

test("publishTask: POSTs to /tasks with auto-constructed envelope and parses the Task response", async () => {
  const task: Task = {
    id: "t-1",
    tenant_id: "T1",
    source_agent: "a1",
    target_type: "mailbox",
    target_value: "m1",
    status: "queued",
    attempt_count: 0,
  };
  responder = jsonResponder(task);
  const client = makeClient("secret");

  const out = await client.publishTask({
    source_agent: "a1",
    target_type: "mailbox",
    target_value: "m1",
  });

  assert.deepEqual(out, task, "response JSON should parse into the Task");

  assert.equal(captured.length, 1);
  const req = captured[0];
  assert.equal(req.method, "POST");
  assert.equal(req.url, "/v1/tenants/T1/tasks");
  assert.equal(req.headers["x-api-key"], "secret");
  assert.match(String(req.headers["content-type"]), /^application\/json/);

  const body = req.body;
  assert.equal(body.source_agent, "a1", "flat routing fields should be preserved on the body");

  const env = body.envelope;
  assert.equal(env.janus_version, "1.0");
  assert.equal(env.tenant_id, "T1");
  assert.equal(env.source_agent, "a1");
  assert.deepEqual(env.target, { type: "mailbox", value: "m1" });
  assert.deepEqual(env.payload, { type: "application/json", content: "{}" });
  assert.ok(typeof body.id === "string" && body.id.startsWith("ts-"), "id should be generated");
  assert.equal(env.task_id, body.id);
  assert.equal(env.trace.trace_id, `ts-${body.id}`);

  for (const key of ["priority", "ttl_seconds", "deadline", "budget", "policy", "idempotency_key"]) {
    assert.ok(!(key in env), `optional field "${key}" should be absent when not requested`);
  }
});

test("publishTask: explicit id is used for body.id and envelope.task_id", async () => {
  responder = jsonResponder({ id: "fixed-1", status: "queued" });
  const client = makeClient();

  await client.publishTask({
    id: "fixed-1",
    source_agent: "a",
    target_type: "agent",
    target_value: "b",
  });

  const body = captured[0].body;
  assert.equal(body.id, "fixed-1");
  assert.equal(body.envelope.task_id, "fixed-1");
  assert.equal(body.envelope.trace.trace_id, "ts-fixed-1");
});

test("publishTask: explicit envelope passes through untouched", async () => {
  responder = jsonResponder({ id: "e-1", status: "queued" });
  const envelope: TaskEnvelope = {
    janus_version: "0.9",
    task_id: "e-1",
    tenant_id: "OTHER",
    source_agent: "x",
    target: { type: "agent", value: "y" },
    payload: { type: "text/plain", content: "hi" },
    trace: { trace_id: "tr-1" },
  };
  const client = makeClient();

  await client.publishTask({
    source_agent: "x",
    target_type: "agent",
    target_value: "y",
    envelope,
  });

  assert.deepEqual(captured[0].body.envelope, envelope);
});

test("publishTask: optional routing fields land in the envelope when provided", async () => {
  responder = jsonResponder({ id: "o-1", status: "queued" });
  const client = makeClient();

  await client.publishTask({
    source_agent: "a",
    target_type: "mailbox",
    target_value: "m",
    payload: { type: "application/json", content: '{"k":1}' },
    priority: "high",
    ttl_seconds: 60,
    deadline: "2026-01-01T00:00:00Z",
    budget: { max_tokens: 1000 },
    policy: { data_classification: "internal" },
    idempotency_key: "idem-7",
  });

  const env = captured[0].body.envelope;
  assert.deepEqual(env.payload, { type: "application/json", content: '{"k":1}' });
  assert.equal(env.priority, "high");
  assert.equal(env.ttl_seconds, 60);
  assert.equal(env.deadline, "2026-01-01T00:00:00Z");
  assert.deepEqual(env.budget, { max_tokens: 1000 });
  assert.deepEqual(env.policy, { data_classification: "internal" });
  assert.equal(env.idempotency_key, "idem-7");
});

test("pullTask: 204 means empty mailbox (null); request carries agent_id", async () => {
  responder = (_req, res) => {
    res.writeHead(204);
    res.end();
  };
  const client = makeClient();

  const out = await client.pullTask("mb-1", "ag-1");

  assert.equal(out, null);
  const req = captured[0];
  assert.equal(req.method, "POST");
  assert.equal(req.url, "/v1/tenants/T1/mailboxes/mb-1/pull");
  assert.deepEqual(req.body, { agent_id: "ag-1" });
});

test("pullTask: parses task + lease from the JSON response", async () => {
  responder = jsonResponder({
    task: {
      id: "t-9",
      tenant_id: "T1",
      source_agent: "s",
      target_type: "mailbox",
      target_value: "mb-1",
      status: "leased",
      attempt_count: 1,
    },
    lease: { lease_id: "lease-77", attempt: 3, expires_at: "2026-01-01T00:00:10Z" },
  });
  const client = makeClient();

  const out = await client.pullTask("mb-1", "ag-1");

  assert.ok(out);
  assert.equal(out.task.id, "t-9");
  assert.equal(out.lease.lease_id, "lease-77");
  assert.equal(out.lease.attempt, 3);
});

test("pullTask: 200 with an empty body also means null", async () => {
  responder = (_req, res) => {
    res.writeHead(200);
    res.end();
  };
  const client = makeClient();

  assert.equal(await client.pullTask("mb-1", "ag-1"), null);
});

test("ackTask: POSTs lease, attempt and result to /ack", async () => {
  responder = jsonResponder({});
  const client = makeClient();

  await client.ackTask("t-9", {
    lease_id: "lease-77",
    attempt: 3,
    result_ref: "mem://done",
    token_usage: { total_tokens: 42 },
  });

  const req = captured[0];
  assert.equal(req.method, "POST");
  assert.equal(req.url, "/v1/tenants/T1/tasks/t-9/ack");
  assert.deepEqual(req.body, {
    lease_id: "lease-77",
    attempt: 3,
    result_ref: "mem://done",
    token_usage: { total_tokens: 42 },
  });
});

test("error envelope becomes JanusAPIError with code and message", async () => {
  responder = jsonResponder(
    { error: "invalid target", code: "INVALID_ARGUMENT", message: "target mailbox not found", status: 1201 },
    422,
  );
  const client = makeClient();

  await assert.rejects(client.getTask("t-1"), (err: unknown) => {
    assert.ok(err instanceof JanusAPIError);
    const e = err as JanusAPIError;
    assert.equal(e.statusCode, 422);
    assert.equal(e.code, "INVALID_ARGUMENT");
    assert.equal(e.message, "target mailbox not found");
    assert.equal(e.status, 1201);
    return true;
  });
});

test("error with only an `error` field falls back to it for the message", async () => {
  responder = jsonResponder({ error: "mailbox is paused", code: "UNAVAILABLE" }, 503);
  const client = makeClient();

  await assert.rejects(client.getTask("t-2"), (err: unknown) => {
    const e = err as JanusAPIError;
    assert.equal(e.code, "UNAVAILABLE");
    assert.equal(e.statusCode, 503);
    assert.equal(e.message, "mailbox is paused");
    return true;
  });
});

test("non-JSON error body yields UNKNOWN code and a status-derived message", async () => {
  responder = (_req, res) => {
    res.writeHead(502);
    res.end("bad gateway text");
  };
  const client = makeClient();

  await assert.rejects(client.getTask("t-3"), (err: unknown) => {
    const e = err as JanusAPIError;
    assert.equal(e.code, "UNKNOWN");
    assert.equal(e.statusCode, 502);
    assert.equal(e.message, "Janus API error 502");
    return true;
  });
});

test("reportProgress: defaults agent_id to 'default' and omits percent/data", async () => {
  responder = jsonResponder({});
  const client = makeClient();

  await client.reportProgress("t-4", "step done");

  const req = captured[0];
  assert.equal(req.method, "POST");
  assert.equal(req.url, "/v1/tenants/T1/tasks/t-4/progress");
  assert.deepEqual(req.body, { message: "step done", agent_id: "default" });
});

test("reportProgress: forwards percent, data and agentID when given", async () => {
  responder = jsonResponder({});
  const client = makeClient("secret");

  await client.reportProgress("t-4", "half way", { percent: 50, data: { page: 2 }, agentID: "ag-9" });

  assert.deepEqual(captured[0].body, {
    message: "half way",
    agent_id: "ag-9",
    percent: 50,
    data: { page: 2 },
  });
});

test("streamTask: parses SSE events, merges data and stops after a terminal event", async () => {
  responder = (_req, res) => {
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.write('event: task.accepted\ndata: {"task_id":"t-5"}\n\n');
    res.write("event: task.pro");
    res.write('gress\ndata: {"percent":50}\n\n');
    res.write('event: task.completed\ndata: {"task_id":"t-5","result_ref":"mem://r1"}\n\n');
    res.write('event: task.progress\ndata: {"never":"yielded"}\n\n');
    res.end();
  };
  const client = makeClient("secret");

  const events: Array<{ event_type: string; [key: string]: unknown }> = [];
  for await (const ev of client.streamTask("t-5")) events.push(ev);

  assert.deepEqual(events, [
    { event_type: "task.accepted", task_id: "t-5" },
    { event_type: "task.progress", percent: 50 },
    { event_type: "task.completed", task_id: "t-5", result_ref: "mem://r1" },
  ]);
  assert.equal(captured.length, 1);
  assert.equal(captured[0].url, "/v1/tenants/T1/tasks/t-5/stream");
  assert.equal(captured[0].headers["x-api-key"], "secret");
});

test("streamTask: HTTP error status throws 'stream failed'", async () => {
  responder = (_req, res) => {
    res.writeHead(500);
    res.end("boom");
  };
  const client = makeClient();

  const events: unknown[] = [];
  await assert.rejects(
    async () => {
      for await (const ev of client.streamTask("t-6")) events.push(ev);
    },
    /stream failed: 500/,
  );
  assert.equal(events.length, 0);
});

test("listAgents: unwraps {agents: [...]} and defaults to [] when absent", async () => {
  responder = jsonResponder({
    agents: [
      {
        id: "a1",
        tenant_id: "T1",
        display_name: "A1",
        protocol: "a2a",
        status: "active",
        capabilities: [],
        max_concurrency: 1,
        rpm: 10,
        tpm: 100,
      },
    ],
  });
  const client = makeClient();

  const agents = await client.listAgents();

  assert.equal(agents.length, 1);
  assert.equal(agents[0].id, "a1");
  assert.equal(captured[0].method, "GET");
  assert.equal(captured[0].url, "/v1/tenants/T1/agents");

  responder = jsonResponder({});
  assert.deepEqual(await client.listAgents(), []);
});

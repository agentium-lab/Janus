// Janus TypeScript SDK — typed API error

export type ErrorCode =
  | "INVALID_ARGUMENT"
  | "UNAUTHENTICATED"
  | "PERMISSION_DENIED"
  | "NOT_FOUND"
  | "CONFLICT"
  | "RESOURCE_EXHAUSTED"
  | "UNAVAILABLE"
  | "INTERNAL"
  | "UNKNOWN";

export class JanusAPIError extends Error {
  readonly statusCode: number;
  readonly code: ErrorCode;
  readonly status: number;

  constructor(statusCode: number, body: { error?: string; code?: string; message?: string; status?: number }) {
    super(body.message || body.error || `Janus API error ${statusCode}`);
    this.name = "JanusAPIError";
    this.statusCode = statusCode;
    this.status = body.status || statusCode;
    this.code = (body.code as ErrorCode) || "UNKNOWN";
  }
}

// LeaseLostError: the stream surfaced a lease-lost terminal signal — the
// stream request answered 409 Conflict, or a task.lease_lost / lease.lost
// SSE event arrived — so the lease backing the observed task is gone and
// the stream will never reach a normal terminal state. All three SDKs
// surface the same shape for this signal.
export class LeaseLostError extends Error {
  readonly taskID: string;

  constructor(taskID: string) {
    super(taskID ? `lease lost for task ${taskID}` : "lease lost");
    this.name = "LeaseLostError";
    this.taskID = taskID;
  }
}

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

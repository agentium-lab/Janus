import httpx
from typing import Optional

from .models import (
    RegisterAgentRequest,
    PublishTaskRequest,
    PullResult,
    AckRequest,
    NackRequest,
    Task,
)


class JanusAPIError(httpx.HTTPStatusError):
    """Typed error raised when the Janus API returns a non-2xx response.

    Compatible with httpx.HTTPStatusError (so existing try/except still works)
    but also exposes the structured fields from the Janus error envelope:
    code, message, and status.

    Attributes:
        code: Canonical error code (e.g. "NOT_FOUND", "INVALID_ARGUMENT").
        message: Human-readable error message.
        status: HTTP status code (int).
    """

    code: str
    message: str
    status: int

    def __init__(self, response: httpx.Response):
        super().__init__(response.text, request=response.request, response=response)
        self.status = response.status_code
        try:
            body = response.json()
            self.code = body.get("code", "UNKNOWN")
            self.message = body.get("message", body.get("error", response.text))
        except Exception:
            self.code = "UNKNOWN"
            self.message = response.text

    def __str__(self) -> str:
        return f"{self.code} ({self.status}): {self.message}"


class JanusClient:
    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        tenant_id: str = "default",
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self._base_url = base_url.rstrip("/")
        self._tenant_id = tenant_id
        headers = {}
        if api_key:
            headers["X-API-Key"] = api_key
        self._client = httpx.Client(timeout=timeout, headers=headers)

    def _check(self, resp: httpx.Response) -> None:
        """Raise JanusAPIError on non-2xx responses."""
        if resp.is_success:
            return
        raise JanusAPIError(resp)

    @property
    def _prefix(self) -> str:
        return f"/v1/tenants/{self._tenant_id}"

    def _url(self, path: str) -> str:
        return f"{self._base_url}{self._prefix}{path}"

    def register_agent(self, req: RegisterAgentRequest) -> None:
        resp = self._client.post(self._url("/agents"), json=req.model_dump(exclude_none=True))
        self._check(resp)

    def publish_task(self, req: PublishTaskRequest) -> dict:
        resp = self._client.post(self._url("/tasks"), json=req.model_dump(exclude_none=True))
        self._check(resp)
        return resp.json()

    def get_task(self, task_id: str) -> Task:
        resp = self._client.get(self._url(f"/tasks/{task_id}"))
        self._check(resp)
        return Task.model_validate(resp.json())

    def pull_task(self, mailbox_id: str, agent_id: str = "default") -> Optional[PullResult]:
        resp = self._client.post(
            self._url(f"/mailboxes/{mailbox_id}/pull"),
            json={"agent_id": agent_id},
        )
        self._check(resp)
        if resp.status_code == 204 or not resp.content:
            return None
        return PullResult.model_validate(resp.json())

    def start_task(self, task_id: str, lease_id: str) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/start"),
            json={"lease_id": lease_id},
        )
        self._check(resp)

    def heartbeat(self, task_id: str, lease_id: str) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/heartbeat"),
            json={"lease_id": lease_id},
        )
        self._check(resp)

    def ack_task(self, task_id: str, req: AckRequest) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/ack"),
            json=req.model_dump(exclude_none=True),
        )
        self._check(resp)

    def nack_task(self, task_id: str, req: NackRequest) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/nack"),
            json=req.model_dump(exclude_none=True),
        )
        self._check(resp)

    def cancel_task(self, task_id: str) -> None:
        resp = self._client.post(self._url(f"/tasks/{task_id}/cancel"))
        self._check(resp)

    def get_task_events(self, task_id: str) -> list[dict]:
        resp = self._client.get(self._url(f"/tasks/{task_id}/events"))
        self._check(resp)
        data = resp.json()
        return data.get("events", data) if isinstance(data, dict) else data

    def create_tenant(self, tenant_id: str, name: str) -> dict:
        resp = self._client.post(
            f"{self._base_url}/v1/tenants",
            json={"id": tenant_id, "name": name},
        )
        self._check(resp)
        return resp.json()

    def get_tenant(self, tenant_id: str) -> dict:
        resp = self._client.get(f"{self._base_url}/v1/tenants/{tenant_id}")
        self._check(resp)
        return resp.json()

    def create_mailbox(self, req) -> dict:
        resp = self._client.post(self._url("/mailboxes"), json=req.model_dump(exclude_none=True))
        self._check(resp)
        return resp.json()

    def get_mailbox(self, mailbox_id: str) -> dict:
        resp = self._client.get(self._url(f"/mailboxes/{mailbox_id}"))
        self._check(resp)
        return resp.json()

    def update_mailbox(self, mailbox_id: str, req) -> dict:
        resp = self._client.patch(self._url(f"/mailboxes/{mailbox_id}"), json=req.model_dump(exclude_none=True))
        self._check(resp)
        return resp.json()

    def pause_mailbox(self, mailbox_id: str) -> dict:
        resp = self._client.post(self._url(f"/mailboxes/{mailbox_id}/pause"))
        self._check(resp)
        return resp.json()

    def resume_mailbox(self, mailbox_id: str) -> dict:
        resp = self._client.post(self._url(f"/mailboxes/{mailbox_id}/resume"))
        self._check(resp)
        return resp.json()

    def heartbeat_agent(self, agent_id: str) -> dict:
        resp = self._client.post(self._url(f"/agents/{agent_id}/heartbeat"))
        self._check(resp)
        return resp.json()

    def list_agents(self) -> list[dict]:
        resp = self._client.get(self._url("/agents"))
        self._check(resp)
        return resp.json().get("agents", [])

    def get_agent(self, agent_id: str) -> dict:
        resp = self._client.get(self._url(f"/agents/{agent_id}"))
        self._check(resp)
        return resp.json()

    def query_dlq(self, mailbox_id: str = "", limit: int = 50) -> list[dict]:
        params = {"limit": str(limit)}
        if mailbox_id:
            params["mailbox"] = mailbox_id
        resp = self._client.get(self._url("/dlq"), params=params)
        self._check(resp)
        return resp.json().get("tasks", [])

    def replay_dlq(self, task_id: str) -> dict:
        resp = self._client.post(self._url(f"/dlq/{task_id}/replay"))
        self._check(resp)
        return resp.json()

    def discard_dlq(self, task_id: str) -> None:
        resp = self._client.post(self._url(f"/dlq/{task_id}/discard"))
        self._check(resp)

    def create_api_key(self, name: str) -> dict:
        resp = self._client.post(self._url("/api-keys"), json={"name": name})
        self._check(resp)
        return resp.json()

    def list_api_keys(self) -> list[dict]:
        resp = self._client.get(self._url("/api-keys"))
        self._check(resp)
        return resp.json().get("api_keys", [])

    def revoke_api_key(self, key_id: str) -> dict:
        resp = self._client.post(self._url(f"/api-keys/{key_id}/revoke"))
        self._check(resp)
        return resp.json()

    def create_policy_rule(self, req) -> dict:
        resp = self._client.post(self._url("/policy-rules"), json=req.model_dump(exclude_none=True))
        self._check(resp)
        return resp.json()

    def create_policy_rule_from_template(self, req) -> dict:
        resp = self._client.post(self._url("/policy-rules/templates"), json=req.model_dump(exclude_none=True))
        self._check(resp)
        return resp.json()

    def list_policy_rules(self) -> list[dict]:
        resp = self._client.get(self._url("/policy-rules"))
        self._check(resp)
        return resp.json().get("policy_rules", [])

    def upsert_budget(self, req) -> dict:
        resp = self._client.post(self._url("/budgets"), json=req.model_dump(exclude_none=True))
        self._check(resp)
        return resp.json()

    def get_budget(self, scope_type: str, scope_id: str) -> dict:
        resp = self._client.get(self._url(f"/budgets/{scope_type}/{scope_id}"))
        self._check(resp)
        return resp.json()

    def list_budgets(self) -> list[dict]:
        resp = self._client.get(self._url("/budgets"))
        self._check(resp)
        return resp.json().get("budgets", [])

    def report_progress(self, task_id: str, message: str, percent: Optional[int] = None,
                        data: Optional[dict] = None, agent_id: str = "default") -> None:
        """Report mid-task progress (visible via stream_task / SSE)."""
        body: dict = {"message": message, "agent_id": agent_id}
        if percent is not None:
            body["percent"] = percent
        if data is not None:
            body["data"] = data
        resp = self._client.post(self._url(f"/tasks/{task_id}/progress"), json=body)
        resp.raise_for_status()

    def stream_task(self, task_id: str, timeout: Optional[float] = 300.0):
        """Yield SSE events for a task until it reaches a terminal state.

        Usage:
            for event in client.stream_task("task-123"):
                if event["event_type"] == "task.progress":
                    print(event["payload"].get("message"))
        """
        url = f"{self._base_url}/v1/tenants/{self._tenant_id}/tasks/{task_id}/stream"
        terminal = {"task.completed", "task.failed", "task.cancelled",
                     "task.dead_lettered", "task.expired"}
        try:
            with self._client.stream("GET", url, timeout=timeout) as resp:
                resp.raise_for_status()
                event_type = ""
                buffer = ""
                for chunk in resp.iter_text():
                    buffer += chunk
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.rstrip("\r")
                        if line.startswith("event: "):
                            event_type = line[7:].strip()
                        elif line.startswith("data: ") and event_type:
                            import json as _json
                            try:
                                data = _json.loads(line[6:])
                            except (ValueError, TypeError):
                                data = {"raw": line[6:]}
                            yield {"event_type": event_type, **data}
                            if event_type in terminal:
                                return
        except (httpx.TimeoutException, httpx.ConnectError):
            return

    def replay_task(self, task_id: str) -> dict:
        resp = self._client.post(self._url(f"/tasks/{task_id}/replay"))
        self._check(resp)
        return resp.json()

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

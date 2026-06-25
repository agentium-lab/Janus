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

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

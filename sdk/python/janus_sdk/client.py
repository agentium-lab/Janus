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


class JanusClient:
    def __init__(self, base_url: str = "http://localhost:8080", tenant_id: str = "default"):
        self._base_url = base_url.rstrip("/")
        self._tenant_id = tenant_id
        self._client = httpx.Client(timeout=30.0)

    @property
    def _prefix(self) -> str:
        return f"/v1/tenants/{self._tenant_id}"

    def _url(self, path: str) -> str:
        return f"{self._base_url}{self._prefix}{path}"

    def register_agent(self, req: RegisterAgentRequest) -> None:
        resp = self._client.post(self._url("/agents"), json=req.model_dump(exclude_none=True))
        resp.raise_for_status()

    def publish_task(self, req: PublishTaskRequest) -> dict:
        resp = self._client.post(self._url("/tasks"), json=req.model_dump(exclude_none=True))
        resp.raise_for_status()
        return resp.json()

    def get_task(self, task_id: str) -> Task:
        resp = self._client.get(self._url(f"/tasks/{task_id}"))
        resp.raise_for_status()
        return Task.model_validate(resp.json())

    def pull_task(self, mailbox_id: str, agent_id: str = "default") -> Optional[PullResult]:
        resp = self._client.post(
            self._url(f"/mailboxes/{mailbox_id}/pull"),
            json={"agent_id": agent_id},
        )
        resp.raise_for_status()
        if resp.status_code == 204 or not resp.content:
            return None
        return PullResult.model_validate(resp.json())

    def start_task(self, task_id: str, lease_id: str) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/start"),
            json={"lease_id": lease_id},
        )
        resp.raise_for_status()

    def heartbeat(self, task_id: str, lease_id: str) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/heartbeat"),
            json={"lease_id": lease_id},
        )
        resp.raise_for_status()

    def ack_task(self, task_id: str, req: AckRequest) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/ack"),
            json=req.model_dump(exclude_none=True),
        )
        resp.raise_for_status()

    def nack_task(self, task_id: str, req: NackRequest) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/nack"),
            json=req.model_dump(exclude_none=True),
        )
        resp.raise_for_status()

    def cancel_task(self, task_id: str) -> None:
        resp = self._client.post(self._url(f"/tasks/{task_id}/cancel"))
        resp.raise_for_status()

    def get_task_events(self, task_id: str) -> list[dict]:
        resp = self._client.get(self._url(f"/tasks/{task_id}/events"))
        resp.raise_for_status()
        data = resp.json()
        return data.get("events", data) if isinstance(data, dict) else data

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

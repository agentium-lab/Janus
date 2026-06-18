import httpx
from typing import Optional

from .models import (
    APIKey,
    Agent,
    BudgetRequest,
    BudgetSpec,
    CreatedAPIKey,
    CreateAPIKeyRequest,
    CreateMailboxRequest,
    Mailbox,
    MailboxActionResponse,
    RegisterAgentRequest,
    PublishTaskRequest,
    PullResult,
    AckRequest,
    NackRequest,
    PolicyRule,
    PolicyRuleRequest,
    PolicyRuleTemplateRequest,
    Task,
    Tenant,
    UpdateMailboxRequest,
)


class JanusAPIError(httpx.HTTPStatusError):
    def __init__(self, response: httpx.Response, code: str, message: str, status: int):
        self.code = code
        self.message = message
        self.status = status
        detail = f"api error ({status} {code}): {message}"
        try:
            request = response.request
        except RuntimeError:
            request = httpx.Request("GET", "http://janus.local")
        super().__init__(detail, request=request, response=response)


class JanusClient:
    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        tenant_id: str = "default",
        api_key: str = "",
    ):
        self._base_url = base_url.rstrip("/")
        self._tenant_id = tenant_id
        headers = {"X-API-Key": api_key} if api_key else None
        self._client = httpx.Client(timeout=30.0, headers=headers)

    @property
    def _prefix(self) -> str:
        return f"/v1/tenants/{self._tenant_id}"

    def _url(self, path: str) -> str:
        return f"{self._base_url}{self._prefix}{path}"

    def _tenant_url(self, tenant_id: str) -> str:
        return f"{self._base_url}/v1/tenants/{tenant_id}"

    def get_tenant(self, tenant_id: str = "") -> Tenant:
        resp = self._client.get(self._tenant_url(tenant_id or self._tenant_id))
        self._raise_for_status(resp)
        return Tenant.model_validate(resp.json())

    def register_agent(self, req: RegisterAgentRequest) -> None:
        resp = self._client.post(self._url("/agents"), json=_request_json(req))
        self._raise_for_status(resp)

    def list_agents(self) -> list[Agent]:
        resp = self._client.get(self._url("/agents"))
        self._raise_for_status(resp)
        data = resp.json()
        items = data.get("agents", data) if isinstance(data, dict) else data
        return [Agent.model_validate(item) for item in items]

    def get_agent(self, agent_id: str) -> Agent:
        resp = self._client.get(self._url(f"/agents/{agent_id}"))
        self._raise_for_status(resp)
        return Agent.model_validate(resp.json())

    def heartbeat_agent(self, agent_id: str) -> None:
        resp = self._client.post(self._url(f"/agents/{agent_id}/heartbeat"))
        self._raise_for_status(resp)

    def publish_task(self, req: PublishTaskRequest) -> dict:
        resp = self._client.post(self._url("/tasks"), json=req.model_dump(exclude_none=True))
        self._raise_for_status(resp)
        return resp.json()

    def get_task(self, task_id: str) -> Task:
        resp = self._client.get(self._url(f"/tasks/{task_id}"))
        self._raise_for_status(resp)
        return Task.model_validate(resp.json())

    def pull_task(self, mailbox_id: str, agent_id: str) -> Optional[PullResult]:
        if not agent_id or not agent_id.strip():
            raise ValueError("agent_id is required")
        resp = self._client.post(
            self._url(f"/mailboxes/{mailbox_id}/pull"),
            json={"agent_id": agent_id},
        )
        self._raise_for_status(resp)
        if resp.status_code == 204 or not resp.content:
            return None
        return PullResult.model_validate(resp.json())

    def start_task(self, task_id: str, attempt: int, lease_id: str) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/start"),
            json={"attempt": attempt, "lease_id": lease_id},
        )
        self._raise_for_status(resp)

    def heartbeat(self, task_id: str, attempt: int, lease_id: str) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/heartbeat"),
            json={"attempt": attempt, "lease_id": lease_id},
        )
        self._raise_for_status(resp)

    def ack_task(self, task_id: str, req: AckRequest) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/ack"),
            json=_request_json(req),
        )
        self._raise_for_status(resp)

    def nack_task(self, task_id: str, req: NackRequest) -> None:
        resp = self._client.post(
            self._url(f"/tasks/{task_id}/nack"),
            json=_request_json(req),
        )
        self._raise_for_status(resp)

    def cancel_task(self, task_id: str) -> None:
        resp = self._client.post(self._url(f"/tasks/{task_id}/cancel"))
        self._raise_for_status(resp)

    def replay_task(self, task_id: str) -> Task:
        resp = self._client.post(self._url(f"/tasks/{task_id}/replay"))
        self._raise_for_status(resp)
        return Task.model_validate(resp.json())

    def get_task_events(self, task_id: str) -> list[dict]:
        resp = self._client.get(self._url(f"/tasks/{task_id}/events"))
        self._raise_for_status(resp)
        data = resp.json()
        return data.get("events", data) if isinstance(data, dict) else data

    def create_mailbox(self, req: CreateMailboxRequest) -> MailboxActionResponse:
        resp = self._client.post(self._url("/mailboxes"), json=_request_json(req))
        self._raise_for_status(resp)
        return MailboxActionResponse.model_validate(resp.json())

    def get_mailbox(self, mailbox_id: str) -> Mailbox:
        resp = self._client.get(self._url(f"/mailboxes/{mailbox_id}"))
        self._raise_for_status(resp)
        return Mailbox.model_validate(resp.json())

    def update_mailbox(self, mailbox_id: str, req: UpdateMailboxRequest) -> MailboxActionResponse:
        resp = self._client.patch(
            self._url(f"/mailboxes/{mailbox_id}"),
            json=_request_json(req),
        )
        self._raise_for_status(resp)
        return MailboxActionResponse.model_validate(resp.json())

    def pause_mailbox(self, mailbox_id: str) -> MailboxActionResponse:
        resp = self._client.post(self._url(f"/mailboxes/{mailbox_id}/pause"))
        self._raise_for_status(resp)
        return MailboxActionResponse.model_validate(resp.json())

    def resume_mailbox(self, mailbox_id: str) -> MailboxActionResponse:
        resp = self._client.post(self._url(f"/mailboxes/{mailbox_id}/resume"))
        self._raise_for_status(resp)
        return MailboxActionResponse.model_validate(resp.json())

    def query_dlq(self, mailbox_id: str = "", limit: int = 0) -> list[Task]:
        params = {}
        if mailbox_id:
            params["mailbox"] = mailbox_id
        if limit > 0:
            params["limit"] = str(limit)
        resp = self._client.get(self._url("/dlq"), params=params)
        self._raise_for_status(resp)
        data = resp.json()
        items = data.get("tasks", data) if isinstance(data, dict) else data
        return [Task.model_validate(item) for item in items]

    def replay_dlq(self, task_id: str) -> Task:
        resp = self._client.post(self._url(f"/dlq/{task_id}/replay"))
        self._raise_for_status(resp)
        return Task.model_validate(resp.json())

    def discard_dlq(self, task_id: str) -> None:
        resp = self._client.post(self._url(f"/dlq/{task_id}/discard"))
        self._raise_for_status(resp)

    def create_api_key(self, name: str) -> CreatedAPIKey:
        resp = self._client.post(
            self._url("/api-keys"),
            json=_request_json(CreateAPIKeyRequest(name=name)),
        )
        self._raise_for_status(resp)
        return CreatedAPIKey.model_validate(resp.json())

    def list_api_keys(self) -> list[APIKey]:
        resp = self._client.get(self._url("/api-keys"))
        self._raise_for_status(resp)
        data = resp.json()
        items = data.get("api_keys", data) if isinstance(data, dict) else data
        return [APIKey.model_validate(item) for item in items]

    def revoke_api_key(self, key_id: str) -> APIKey:
        resp = self._client.post(self._url(f"/api-keys/{key_id}/revoke"))
        self._raise_for_status(resp)
        return APIKey.model_validate(resp.json())

    def create_policy_rule(self, req: PolicyRuleRequest) -> PolicyRule:
        resp = self._client.post(self._url("/policy-rules"), json=_request_json(req))
        self._raise_for_status(resp)
        return PolicyRule.model_validate(resp.json())

    def create_policy_rule_from_template(self, req: PolicyRuleTemplateRequest) -> PolicyRule:
        resp = self._client.post(self._url("/policy-rules/templates"), json=_request_json(req))
        self._raise_for_status(resp)
        return PolicyRule.model_validate(resp.json())

    def list_policy_rules(self) -> list[PolicyRule]:
        resp = self._client.get(self._url("/policy-rules"))
        self._raise_for_status(resp)
        data = resp.json()
        items = data.get("policy_rules", data) if isinstance(data, dict) else data
        return [PolicyRule.model_validate(item) for item in items]

    def upsert_budget(self, req: BudgetRequest) -> BudgetSpec:
        resp = self._client.post(self._url("/budgets"), json=_request_json(req))
        self._raise_for_status(resp)
        return BudgetSpec.model_validate(resp.json())

    def get_budget(self, scope_type: str, scope_id: str) -> BudgetSpec:
        resp = self._client.get(self._url(f"/budgets/{scope_type}/{scope_id}"))
        self._raise_for_status(resp)
        return BudgetSpec.model_validate(resp.json())

    def list_budgets(self) -> list[BudgetSpec]:
        resp = self._client.get(self._url("/budgets"))
        self._raise_for_status(resp)
        data = resp.json()
        items = data.get("budgets", data) if isinstance(data, dict) else data
        return [BudgetSpec.model_validate(item) for item in items]

    def _raise_for_status(self, resp: httpx.Response) -> None:
        if resp.status_code < 400:
            return
        payload = {}
        if resp.content:
            try:
                payload = resp.json()
            except ValueError:
                payload = {}
        code = payload.get("code") or _api_error_code(resp.status_code)
        message = payload.get("message") or payload.get("error") or resp.reason_phrase
        status = payload.get("status") or resp.status_code
        raise JanusAPIError(resp, code=code, message=message, status=status)

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _api_error_code(status: int) -> str:
    if status == 400:
        return "INVALID_ARGUMENT"
    if status == 401:
        return "UNAUTHENTICATED"
    if status == 403:
        return "PERMISSION_DENIED"
    if status == 404:
        return "NOT_FOUND"
    if status == 409:
        return "CONFLICT"
    if status == 429:
        return "RESOURCE_EXHAUSTED"
    if status == 503:
        return "UNAVAILABLE"
    if status >= 500:
        return "INTERNAL"
    return "UNKNOWN"


def _request_json(model):
    return model.model_dump(exclude_none=True, exclude_unset=True, by_alias=True)

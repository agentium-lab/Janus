# Janus Project Config

`janus-server.yaml` configures the Janus API process and infrastructure dependencies.
`janus.project.yaml` configures tenant-scoped Janus resources that users manage day to day.

The project file is intentionally compact. It is not a second runtime model. `janus project apply` compiles it into the existing Core resources:

| Project field | Core resource |
| --- | --- |
| `tenants` | tenants |
| `agents` | Agent Registry records |
| agent `capabilities` | `agent_capabilities` records |
| agent `mailbox` / default mailbox | mailboxes |
| `budgets` | `budgets` rows |
| `policies` | policy templates that generate standard `policy_rules` |

## Default File

Most commands do not need `--file`. Janus looks for `janus.project.yaml` in this order:

1. explicit `--file`;
2. `JANUS_PROJECT_FILE`;
3. current directory;
4. parent directories until a Git root or filesystem root.

`--file` remains useful for CI/CD or environment-specific project files.

## Example

```yaml
version: v1
default_tenant: acme

defaults:
protocol: custom-sdk
capacity:
max_concurrency: 1

tenants:
acme:
name: Acme Engineering
agents:
code-review:
team: engineering
capabilities:
- id: code_review
data_classifications: [public, internal, confidential]
concurrency: 4
budgets:
tenant:
tpm: 2000000
daily_usd: 500
policies:
approve:
capabilities: [prod_deploy]
allow:
- agent: code-review
capability: code_review
```

## Commands

```sh
janus project init
janus project validate
janus project diff
janus project apply
janus project sync
```

`janus validate`, `janus diff`, and `janus apply` are kept as short aliases.

When a project has more than one tenant, either set `default_tenant` or pass `--tenant`:

```sh
janus project apply --tenant acme
janus project apply --all-tenants
```

Dynamic tenant and agent additions are persisted after the remote API call succeeds:

```sh
janus tenant add acme --name "Acme Engineering"

janus agent add code-review \
--tenant acme \
--team engineering \
--capability code_review \
--classification internal \
--classification confidential \
--concurrency 4
```

## Runtime Boundaries

- Agent config stays descriptive: identity, team, capabilities, description, and capacity.
- Governance stays in policy rules. Project `policies` compile through policy templates.
- Budgets stay in the `budgets` table.
- Tenant boundary is still enforced by tenant-scoped API keys and auth guards, not by project YAML.
- SDK explicit registration APIs, CLI commands, or other control-plane integrations write to Janus first. `JanusWorker` only runs heartbeat, pull, start, ack, and nack for already-provisioned agents/mailboxes. Use `janus project sync` to merge dynamically created resources back into `janus.project.yaml`.

`project sync` currently syncs tenant metadata, agents, budgets, and policy rules. Mailbox list is not a separate stable HTTP surface, so dynamic mailbox discovery is represented through each agent's default mailbox convention unless the project file already contains an explicit mailbox override.
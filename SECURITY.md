# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.1.x   | ✅ |
| < 1.1   | ❌ |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report privately via [GitHub Security Advisories](https://github.com/agentium-lab/Janus/security/advisories/new)
or email **security@agentium-lab.com** (placeholder — replace with the monitored inbox before publishing).

Include: affected version/commit, reproduction steps or PoC, impact assessment, and any known mitigations.

## Response Targets

| Severity | First response | Fix target |
|----------|---------------|------------|
| Critical | 48 hours | 7 days |
| High     | 72 hours | 14 days |
| Medium   | 7 days | 30 days |
| Low      | 14 days | Next minor |

## Scope

In scope: the Janus broker (`server/`, `core/`, `cli/`, `proto/`), Go/Python/TypeScript SDKs,
official container images, and Helm charts.

Out of scope: vulnerabilities requiring a compromised host, social engineering,
and reports against deliberately insecure dev defaults documented in
[deployments/dev/seed-dev-key.sql](deployments/dev/seed-dev-key.sql).

## Disclosure Policy

We coordinate disclosure with reporters: acknowledge → triage → fix → release → public advisory.
Credit is given unless anonymity is requested.

## Security Controls

- API keys stored as SHA-256 hashes with indexed prefix lookup (`server/internal/auth/apikey.go`)
- Tenant isolation enforced by `TenantGuard` (path tenant must match authenticated tenant)
- CI runs build + vet + staticcheck + unit/E2E tests on every change to `main`
- Release artifacts ship with SHA-256 checksums

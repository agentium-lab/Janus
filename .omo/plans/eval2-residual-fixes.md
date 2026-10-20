# Plan: Janus eval-2 residual fixes (mass-ulw)

Repo: /home/silv/ai/janus/Janus @ 1619272. Source: Codex re-evaluation #2 (2026-10-20), claims first-hand verified.

## Phase 1 — run `eval2-fixwave` (one dag, 4 producer lanes + verify)

| Node | Cat | Owns (write scope) | Deliverable | Depends |
|---|---|---|---|---|
| b5-plan | unspecified-high | service/context_ref_service.go, service/task_service.go, postgres/context_ref_repo.go, service/{replay_gate_cov_test.go,cov_task_test.go,context_ref_pg_test.go(new)} | explicit write plan replacing CreatedAt heuristic; real-PG 4 acceptance | — |
| a4-replay | unspecified-low | driver/nats/driver.go, driver_test.go | ReplayEvents uses adopted EVENTS stream; upgrade test replays events | — |
| b1-startx | unspecified-low | service/{dispatch_service.go,interfaces.go,tx_adapter.go,dispatch_service_test.go,dispatch_pg_test.go}, postgres/task_attempt_repo.go | GetLatestTx; Start validates lease inside ApplyTx; PG regression | — |
| a1-scope | unspecified-high | server/internal/grpc/**, server/internal/auth/apikey.go, server/internal/handler/{ws_handler.go,ws_handler_test.go} (+router guard if needed), tests | per-method parity: DLQ replay/discard admin, GetTrace read parity, WS task:read; cross-protocol tests | — |
| verify | unspecified-low | none (report-only) | make verify (>=90%), targeted pkgs, acceptance matrix w/ PG+NATS; verbatim FAILs | all four |

Lane rules: no commits; targeted tests only; PG/NATS suites skip-if-env-unset; throwaway infra cleaned; report to `$RUN2/waves/<node>.md`.

## Phase 2 — after phase-1 settle, audit, and commit
- a3-governance run: tool.invoke/task.route templates fail-closed (real checks or deny), no fake "configured" success.

## Lead keeps
Diff audit, retries/amends, phase-2 shape, final gates, commits (co-author agentium-lab only), cleanup receipts.

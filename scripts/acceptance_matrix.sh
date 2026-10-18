#!/bin/bash
# T01–T13 acceptance matrix — one-shot verification of every finding fixed
# in the 2026-10-12 comprehensive baseline audit (baseline commit 82f4cea).
#
# Matrix (T# = audit finding, verified by its dedicated regression test):
#   T01 B1 terminal-state revival     — dispatch guards, Cancel closes attempt
#   T02 B2 replay attempt collision   — ResetForReplayTx deletes old attempts
#   T03 B3 pool self-starvation       — tx reads on tx executor (1-conn pool)
#   T04 B4 pgqueue status corruption  — Nack/DLQ never write task.status
#   T05 B5 approval orphan            — gate failure surfaces to caller
#   T06 B6 cross-replica broker ACK   — unknown refs settle via AckWait
#   T07 B7 recovery audit closure     — expiry outbox, NACK classification
#   T08 C1 helm first-install         — hooks render, gates respected
#   T09 C2 dead config wiring         — every Config field consumed
#   T10 C3 SDK worker semantics       — backoff/heartbeat/ACK (3 languages)
#   T11 C4 gate negatives             — unreachable API is FATAL, not SKIP
#   T12 A3 policy routing real data   — trusted-record attributes + template API
#   T13 A4 NATS naming collision      — a.b ≠ a_b after sanitize
#
# Usage: scripts/acceptance_matrix.sh   (from repo root; PG/NATS optional —
# PG-backed checks skip when unreachable, matching repo test convention)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

export GOFLAGS="${GOFLAGS:-}"
GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
export GOPROXY

PASS=0; FAIL=0; SKIP=0
declare -a RESULTS

record() { # id name status [note] — status MUST be exactly PASS|FAIL|SKIP;
  # annotations ride in note so the counters can never miss a failure.
  local id="$1" name="$2" status="$3" note="${4:-}"
  case "$status" in
    PASS|FAIL|SKIP) ;;
    *) echo "internal error: bad status '$status' for $id"; exit 2 ;;
  esac
  RESULTS+=("$id|$name|$status${note:+ ($note)}")
  case "$status" in
    PASS) PASS=$((PASS+1)) ;;
    FAIL) FAIL=$((FAIL+1)) ;;
    SKIP) SKIP=$((SKIP+1)) ;;
  esac
}

go_check() { # id name pkg run_pattern
  local id="$1" name="$2" pkg="$3" run="$4"
  local out
  if ! out=$(go test "$pkg" -run "$run" -count=1 -timeout 300s -v 2>&1); then
    echo "---- $id output ----"; echo "$out" | tail -15
    record "$id" "$name" FAIL
    return
  fi
  local ran
  ran=$(printf '%s\n' "$out" | grep -cE '^(=== RUN|--- PASS|--- FAIL)')
  if [ "$ran" -eq 0 ] || printf '%s\n' "$out" | grep -q "no tests to run"; then
    echo "---- $id output ----"; echo "$out" | tail -15
    record "$id" "$name" FAIL "0 tests matched"
    return
  fi
  local passed skipped
  passed=$(printf '%s\n' "$out" | grep -cE '^--- PASS')
  skipped=$(printf '%s\n' "$out" | grep -cE '^--- SKIP')
  if [ "$passed" -eq 0 ] && [ "$skipped" -gt 0 ]; then
    echo "---- $id output ----"; echo "$out" | tail -15
    record "$id" "$name" FAIL "all ${skipped} SKIP"
    return
  fi
  record "$id" "$name" PASS
}

echo "=============================================="
echo " Janus T01–T13 Acceptance Matrix (audit 2026-10-12, baseline 82f4cea)"
echo "=============================================="

# --- T01 B1: terminal-state revival ---------------------------------------
go_check T01 "B1 terminal states: guards + Cancel closes attempt" \
  ./server/internal/service 'TestDispatchService_(Pull|Start|Ack|Nack)_.*Transition|TestTaskService_Cancel_LifecycleOutbox|TestValidTransitions'

# --- T02 B2: replay attempt collision -------------------------------------
go_check T02 "B2 replay collision: attempts reset in-tx, two full rounds" \
  ./server/internal/service 'TestReplay_FullSecondRound_Execution|TestTaskService_Replay_(RepeatDeliversEachGeneration|ConcurrentSingleDelivery|OutboxPublish)'

# --- T03 B3: pool self-starvation -----------------------------------------
go_check T03 "B3 pool starvation: full flow on 1-connection pool" \
  ./server/internal/service 'TestSingleConnectionPool_FullTaskFlow'

# --- T04 B4: pgqueue status corruption ------------------------------------
go_check T04 "B4 pgqueue status: Nack/DLQ leave task.status alone" \
  ./server/internal/driver/pgqueue 'TestFetchTasks_TenantIsolation|.'

# --- T05 B5: approval orphan ----------------------------------------------
go_check T05 "B5 approval gate failure surfaces to caller" \
  ./server/internal/service 'TestTaskService_WithApproval|TestCov_ApprovalService_Expire_TaskGetFailure|TestApprovalService_RequestApproval_RepoError'

# --- T06 B6: cross-replica broker ACK -------------------------------------
go_check T06 "B6 cross-replica ACK: AckWait + reconcile settlement" \
  ./server/internal/driver/nats 'TestDriver_EnsureConsumerWithACKWait|TestReconcileMailboxDLQ|TestDriver_PublishAndNackRetriable'

# --- T07 B7: recovery audit closure ---------------------------------------
go_check T07 "B7 recovery audit: expiry outbox + progress surface" \
  ./server/internal/lease 'TestExpireLeases_DeadLetterWritesBothOutboxEvents'
go_check T07b "B7 recovery audit: ReportProgress outbox persisted" \
  ./server/internal/service 'TestCovPG_ReportProgress_OutboxPersisted'

# --- T08 C1: helm first-install -------------------------------------------
if command -v helm &>/dev/null; then
  C1_OUT=$(helm template deployments/helm/janus-core 2>/dev/null)
  ok=1
  echo "$C1_OUT" | grep -q '"helm.sh/hook"' || ok=0          # hooks present
  echo "$C1_OUT" | grep -q 'PersistentVolumeClaim' && ok=0              # artifacts PVC gated off by default
  if [ "$ok" = 1 ]; then record T08 "C1 helm first-install render" PASS
  else record T08 "C1 helm first-install render" FAIL; fi
else
  record T08 "C1 helm first-install render" SKIP
fi

# --- T09 C2: dead config wiring -------------------------------------------
if python3 scripts/check_config_wiring.py >/dev/null 2>&1; then
  record T09 "C2 config wiring: every field consumed" PASS
else
  record T09 "C2 config wiring: every field consumed" FAIL
fi

# --- T10 C3: SDK worker semantics (3 languages) ---------------------------
go_check T10 "C3 SDK worker (Go): backoff + heartbeats" \
  ./sdk/go 'TestWorker'
if (cd sdk/python && python3 -m pytest tests/ -q >/dev/null 2>&1); then
  record T10b "C3 SDK worker (Python)" PASS
else
  record T10b "C3 SDK worker (Python)" FAIL
fi
if (cd sdk/typescript && npx tsc --noEmit >/dev/null 2>&1 && npm test --silent >/dev/null 2>&1); then
  record T10c "C3 SDK worker (TypeScript)" PASS
else
  record T10c "C3 SDK worker (TypeScript)" FAIL
fi

# --- T11 C4: gate negatives -------------------------------------------------
# An unreachable API must be FATAL (non-zero), never a green SKIP.
if JANUS_URL="http://127.0.0.1:1" timeout 30 bash scripts/smoke_release_ops.sh >/dev/null 2>&1; then
  record T11 "C4 gate negative: unreachable API is FATAL" FAIL
else
  rc=$?
  if [ "$rc" -ne 0 ] && [ "$rc" -ne 124 ]; then
    record T11 "C4 gate negative: unreachable API is FATAL" PASS
  else
    record T11 "C4 gate negative: unreachable API is FATAL" FAIL  # timeout/signal, not a clean FATAL
  fi
fi

# --- T12 A3: policy routing real data --------------------------------------
go_check T12 "A3 actor enrichment + trusted-record routing attributes" \
  ./server/internal/service 'TestPolicyService_ActorEnrichment_TeamScopedRuleMatches'
go_check T12b "A3 lookup returns real governance attributes" \
  ./server/internal/driver/postgres 'TestAgentLookupRepo_ListOnlineByCapability_RealGovernanceAttributes'
go_check T12c "A3 template API creates executable rules" \
  ./server/internal/handler 'TestPolicyRuleHandler_CreateFromTemplate'

# --- T13 A4: NATS naming collision -----------------------------------------
go_check T13 "A4 sanitize: a.b ≠ a_b" \
  ./server/internal/driver/nats 'TestSanitize_NoDotCollision'

# --- Report -----------------------------------------------------------------
echo ""
echo "=============================================="
echo " Results"
echo "=============================================="
printf "%-6s %-52s %s\n" "ID" "CHECK" "STATUS"
for r in "${RESULTS[@]}"; do
  IFS='|' read -r id name status <<< "$r"
  printf "%-6s %-52s %s\n" "$id" "$name" "$status"
done
echo "=============================================="
echo " PASS: $PASS   FAIL: $FAIL   SKIP: $SKIP   TOTAL: $((PASS+FAIL+SKIP))"
echo "=============================================="

[ "$FAIL" -eq 0 ]

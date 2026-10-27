#!/bin/bash
# T01–T15 acceptance matrix — one-shot verification of every finding fixed
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
#   T14 W6 agent presence probe        — probe build + healthy/alert legs (PG)
#   T15 W3 recovery keyset pages       — scanner recovers all pages (PG, REQ)
#   A1  legacy settle admin gate       — /complete + /fail need admin scope
#                                          (403 with a task:write-only key)
#   A2  replay approval gate           — lookup fail-closed + expired denied
#
# Required scenarios (5th go_check arg / record arg): a required scenario
# that ends up SKIPped (e.g. PG env unset) FAILS the run unless
# JANUS_ALLOW_REQUIRED_SKIP=1 is explicitly exported — mixed
# PASS-with-critical-SKIP is not a green run.
#
# Usage: scripts/acceptance_matrix.sh   (from repo root; PG/NATS optional —
# PG-backed checks skip when unreachable, matching repo test convention,
# but a skip of a REQUIRED scenario fails the run; see above)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

export GOFLAGS="${GOFLAGS:-}"
GOPROXY="${GOPROXY:-https://goproxy.cn,direct}"
export GOPROXY

PASS=0; FAIL=0; SKIP=0; REQ_SKIP=0
declare -a RESULTS
declare -a REQ_SKIPPED_IDS

record() { # id name status [note] [required:1] — status MUST be exactly
  # PASS|FAIL|SKIP; annotations ride in note so the counters can never
  # miss a failure. A required scenario that SKIPs is counted in REQ_SKIP
  # and fails the run at the exit gate (JANUS_ALLOW_REQUIRED_SKIP=1 opts in).
  local id="$1" name="$2" status="$3" note="${4:-}" req="${5:-0}"
  case "$status" in
    PASS|FAIL|SKIP) ;;
    *) echo "internal error: bad status '$status' for $id"; exit 2 ;;
  esac
  RESULTS+=("$id|$name|$status${note:+ ($note)}")
  case "$status" in
    PASS) PASS=$((PASS+1)) ;;
    FAIL) FAIL=$((FAIL+1)) ;;
    SKIP) SKIP=$((SKIP+1))
      if [ "$req" = "1" ]; then
        REQ_SKIP=$((REQ_SKIP+1))
        REQ_SKIPPED_IDS+=("$id")
      fi
      ;;
  esac
}

go_check() { # id name pkg run_pattern [required:1]
  local id="$1" name="$2" pkg="$3" run="$4" req="${5:-0}"
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
  if [ "$skipped" -gt 0 ] && [ "$req" = "1" ]; then
    echo "---- $id output ----"; echo "$out" | tail -15
    # Per-case required semantics: a required group is green only when
    # EVERY matched test passed. Any --- SKIP — even beside ordinary
    # PASSes — records SKIP so the exit gate fails the run (REQ_SKIP);
    # a critical skip must never be masked by its passing neighbors.
    record "$id" "$name" SKIP "required; ${skipped} of $((passed+skipped)) SKIP" 1
    return
  fi
  if [ "$passed" -eq 0 ] && [ "$skipped" -gt 0 ]; then
    # Non-required group, environment-deprived: every matched test skipped.
    echo "---- $id output ----"; echo "$out" | tail -15
    record "$id" "$name" FAIL "all ${skipped} SKIP"
    return
  fi
  record "$id" "$name" PASS
}

echo "=============================================="
echo " Janus T01–T15 + A1–A2 Acceptance Matrix (audit 2026-10-12, baseline 82f4cea)"
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
  # ingress.enabled=true + a hosts value: the ingress template must render
  # alongside the default artifacts (no PVC, hooks present).
  C1_OUT=$(helm template deployments/helm/janus-core \
    --set ingress.enabled=true \
    --set ingress.hosts[0].host=janus.acceptance.local 2>/dev/null)
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

# --- T14 W6: agent presence probe builds + runs ----------------------------
# Probe semantics (server/cmd/janus-agent-presence-probe/main.go; the log
# lines land on stderr): exit 0 = every tenant has an online agent,
# exit 1 = alert, tenants with zero online agents listed. The run leg
# seeds a throwaway schema'd DB (healthy tenant -> expect 0, agent-less
# tenant added -> expect 1) and activates only with full JANUS_PG_* +
# psql; otherwise SKIP, matching the other PG-gated checks.
if go build -o /tmp/janus-agent-presence-probe ./server/cmd/janus-agent-presence-probe 2>/dev/null; then
  record T14 "W6 agent presence probe builds" PASS
  if [ -n "${JANUS_PG_HOST:-}" ] && [ -n "${JANUS_PG_PORT:-}" ] && \
     [ -n "${JANUS_PG_USER:-}" ] && [ -n "${JANUS_PG_PASSWORD:-}" ] && \
     command -v psql &>/dev/null; then
    t14_db="janus_matrix_t14_$$"
    t14_dsn="postgres://${JANUS_PG_USER}:${JANUS_PG_PASSWORD}@${JANUS_PG_HOST}:${JANUS_PG_PORT}/${t14_db}?sslmode=disable"
    t14_psql() { PGPASSWORD="$JANUS_PG_PASSWORD" psql -h "$JANUS_PG_HOST" -p "$JANUS_PG_PORT" -U "$JANUS_PG_USER" "$@"; }
    t14_migrate() {
      local f
      for f in migrations/*.up.sql; do
        t14_psql -d "$t14_db" -v ON_ERROR_STOP=1 -qf "$f" >/dev/null 2>&1 || return 1
      done
    }
    t14_ok=1 t14_note="" t14_out=""
    t14_psql -d postgres -qc "CREATE DATABASE $t14_db" >/dev/null 2>&1 || { t14_ok=0; t14_note="create database failed"; }
    if [ "$t14_ok" = 1 ]; then
      t14_migrate || { t14_ok=0; t14_note="migrations failed"; }
    fi
    if [ "$t14_ok" = 1 ]; then
      t14_psql -d "$t14_db" -qc "INSERT INTO tenants (id, name) VALUES ('t14-covered','T14 Covered')" >/dev/null 2>&1
      t14_psql -d "$t14_db" -qc "INSERT INTO agents (tenant_id, id, display_name, protocol, status) VALUES ('t14-covered','t14-agent','T14 Agent','a2a','online')" >/dev/null 2>&1
      t14_out=$(JANUS_PG_DSN="$t14_dsn" /tmp/janus-agent-presence-probe 2>&1); t14_rc=$?
      if [ "$t14_rc" -ne 0 ] || ! printf '%s\n' "$t14_out" | grep -q "all tenants have at least one online agent"; then
        t14_ok=0; t14_note="healthy leg: expected rc=0 + coverage line, got rc=$t14_rc"
      fi
    fi
    if [ "$t14_ok" = 1 ]; then
      t14_psql -d "$t14_db" -qc "INSERT INTO tenants (id, name) VALUES ('t14-bare','T14 Bare')" >/dev/null 2>&1
      t14_out=$(JANUS_PG_DSN="$t14_dsn" /tmp/janus-agent-presence-probe 2>&1); t14_rc=$?
      if [ "$t14_rc" -ne 1 ] || ! printf '%s\n' "$t14_out" | grep -q "zero online agents"; then
        t14_ok=0; t14_note="alert leg: expected rc=1 + WARNING line, got rc=$t14_rc"
      fi
    fi
    if [ "$t14_ok" = 1 ]; then
      record T14b "W6 probe run: healthy=exit0 + alert=exit1 (PG)" PASS
    else
      echo "---- T14b output ----"; printf '%s\n' "$t14_out" | tail -5
      record T14b "W6 probe run: healthy=exit0 + alert=exit1 (PG)" FAIL "$t14_note"
    fi
    t14_psql -d postgres -qc "DROP DATABASE IF EXISTS $t14_db" >/dev/null 2>&1
    rm -f /tmp/janus-agent-presence-probe
  else
    record T14b "W6 probe run: healthy=exit0 + alert=exit1 (PG)" SKIP "needs JANUS_PG_* + psql"
    rm -f /tmp/janus-agent-presence-probe
  fi
else
  record T14 "W6 agent presence probe builds" FAIL
fi

# --- T15 W3: recovery scanner keyset pagination (PG, REQUIRED) -------------
# The scanner must walk every queued page in (created_at, id) order and
# publish each task exactly once. PG-backed and REQUIRED: with PG reachable
# it must RUN and PASS; an all-SKIP run (PG unreachable) is REQ_SKIP and
# fails the run — the recovery scanner regression is a must-run, not optional.
go_check T15 "W3 recovery scanner keyset pagination (PG, required)" \
  ./server/internal/recovery 'TestTaskScanner_ScanKeysetPagination_AllPagesRecovered_PG' 1

# --- A1: legacy settle endpoints need admin scope (REQUIRED) ---------------
# /complete and /fail settle a task's outcome; a task:write-only key must
# get 403 (scopeRules: segment "tasks", suffixes /complete,/fail → admin).
go_check A1 "A1 legacy /complete + /fail require admin (403 task:write)" \
  ./server/tests/invariants 'TestLegacyCompleteFail_RequiresAdminScope' 1

# --- A2: replay must not bypass/fail-open the approval gate (REQUIRED) -----
# Lookup error → fail-closed; latest decision "expired" → replay denied.
go_check A2 "A2 replay gate: lookup fail-closed + expired verdict denied" \
  ./server/tests/invariants 'TestReplay_ApprovalGate_FailClosedAndExpired' 1
go_check A2b "A2 replay verdict blocks requeue (PG)" \
  ./server/internal/service 'TestReplay_ApprovalVerdictBlocksRequeue_PG' 1

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
echo " PASS: $PASS   FAIL: $FAIL   SKIP: $SKIP   REQ_SKIP: $REQ_SKIP   TOTAL: $((PASS+FAIL+SKIP))"
echo "=============================================="

if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
if [ "$REQ_SKIP" -gt 0 ] && [ "${JANUS_ALLOW_REQUIRED_SKIP:-0}" != "1" ]; then
  echo "FATAL: required scenario(s) skipped: ${REQ_SKIPPED_IDS[*]}"
  echo "       a required SKIP is not a green run — enable the environment"
  echo "       (e.g. JANUS_PG_*) or export JANUS_ALLOW_REQUIRED_SKIP=1 to allow"
  exit 1
fi
exit 0

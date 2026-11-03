#!/usr/bin/env bash
# GA-07 four-cell deployment matrix smoke: {queue driver: pg-only, pg+nats} x
# {api replicas: 1, 2}. Each cell boots real janus-api process(es) against the
# configured PostgreSQL, drives a FULL task chain over HTTP (create -> pull ->
# start -> progress -> ack), a NACK-retry leg, and a cancel leg, then asserts
# the PERSISTED business state in PostgreSQL (tasks, task_attempts,
# outbox_events, audit_event_projection) — never HTTP response markers.
#
# Per this repo's rule, an unreachable dependency is FATAL (never a green SKIP):
#   - PostgreSQL unreachable  -> exit 2
#   - NATS unreachable        -> the two PG+NATS cells FAIL (non-zero exit)
#
# Usage:
#   bash scripts/ga07_four_cell.sh
#
# Env knobs (all overridable):
#   JANUS_PG_HOST / JANUS_PG_PORT / JANUS_PG_USER / JANUS_PG_PASSWORD /
#   JANUS_PG_DBNAME     (defaults localhost/5432/janus/janus/janus_test; the
#                        server binary itself reads JANUS_PG_DATABASE)
#   JANUS_PG_SSLMODE    (default disable — local dev PG runs without TLS,
#                        see docker-compose.yml; server default is require)
#   JANUS_NATS_URL      (default nats://localhost:4222; PG+NATS cells only)
#   JANUS_API_BIN       (skip the build and use this binary)
#   JANUS_HTTP_PORT_BASE (default 18110) / JANUS_GRPC_PORT_BASE (default 19110)
#   GA07_CELLS          (comma list of cell names; run a subset, e.g. ga07-nats-1r)
#   KEEP_WORK=1         (retain $WORK and the per-cell databases for debugging)
#
# Each cell runs against its OWN throwaway database (janus_ga07_<cell>,
# created empty and migrated on boot, dropped at cell end). The outbox_events
# drain is GLOBAL (FetchPending is not tenant-scoped), so on a shared database
# a still-running pg-mode replica of a previous cell swallows the next nats
# cell's task_publish rows through the pg-queue no-op publisher and marks them
# published without ever reaching NATS — silent loss. Per-cell databases plus
# reaped replica shutdown keep the deployment modes isolated.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PG_HOST="${JANUS_PG_HOST:-localhost}"
PG_PORT="${JANUS_PG_PORT:-5432}"
PG_USER="${JANUS_PG_USER:-janus}"
PG_PASSWORD="${JANUS_PG_PASSWORD:-janus}"
PG_DBNAME="${JANUS_PG_DBNAME:-janus_test}"
PG_SSLMODE="${JANUS_PG_SSLMODE:-disable}"
NATS_URL="${JANUS_NATS_URL:-nats://localhost:4222}"
HTTP_PORT_BASE="${JANUS_HTTP_PORT_BASE:-18110}"
GRPC_PORT_BASE="${JANUS_GRPC_PORT_BASE:-19110}"
DEAD_REDIS_ADDR="localhost:59999" # PG-only cells run without a live Redis

WORK="$(mktemp -d /tmp/ga07-fourcell.XXXXXX)"
PIDS=()
CELL_DBS=()
CELL_DB=""
CELLS_PASS=0
CELLS_FAIL=0

say()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
die()  { printf '\033[0;31mFATAL: %s\033[0m\n' "$*" >&2; exit 2; }

cleanup() {
  local pid
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  local db
  for db in ${CELL_DBS[@]+"${CELL_DBS[@]}"}; do
    if [ "${KEEP_WORK:-0}" != "1" ]; then
      psql_admin "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$db'" >/dev/null 2>&1 || true
      psql_admin "DROP DATABASE IF EXISTS $db" >/dev/null 2>&1 || true
    fi
  done
  if [ "${KEEP_WORK:-0}" = "1" ]; then
    echo "KEEP_WORK=1: workdir retained at $WORK"
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------- psql helper
sql() { # sql "<query>" -> stdout (-t -A); targets the current cell's DB
  PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
    -d "${CELL_DB:-$PG_DBNAME}" -t -A -c "$1"
}

psql_admin() { # psql_admin "<sql>" against the maintenance db
  PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
    -d postgres -t -A -c "$1"
}

# ------------------------------------------------------------- check counting
check() { # check "<name>" "<condition>" "[evidence]"
  local name="$1" condition="$2" evidence="${3:-}"
  if eval "$condition"; then
    echo "  ✓ $name${evidence:+  [$evidence]}"
    CPASS=$((CPASS+1))
  else
    echo "  ✗ $name${evidence:+  [$evidence]}"
    CFAIL=$((CFAIL+1))
  fi
}

# ------------------------------------------------------------ json extraction
jget() { # jget <json> <pyexpr over d>
  printf '%s' "$1" | python3 -c "import sys,json;d=json.load(sys.stdin);print($2)" 2>/dev/null || echo ""
}

# ------------------------------------------------------------------ HTTP bits
api() { # api <method> <url> <body|-> -> "code body"
  local method="$1" url="$2" body="${3:-}"
  local tmp="$WORK/resp.$$"
  local code
  if [ "$body" = "-" ]; then
    code=$(curl -s -o "$tmp" -w '%{http_code}' --max-time 10 -X "$method" "$url" || echo 000)
  else
    code=$(curl -s -o "$tmp" -w '%{http_code}' --max-time 10 -X "$method" \
      -H 'Content-Type: application/json' -d "$body" "$url" || echo 000)
  fi
  printf '%s %s' "$code" "$(cat "$tmp" 2>/dev/null || true)"
  rm -f "$tmp"
}

# Fixture create that tolerates "already exists" (400 + GET 200), like
# scripts/smoke_two_replica_convergence.sh.
create_or_exists() { # create_or_exists <url> <body> <get_url>
  local out code
  out="$(api POST "$1" "$2")"
  code="${out%% *}"
  case "$code" in
    200|201) return 0 ;;
    400)
      out="$(api GET "$3" -)"
      [ "${out%% *}" = "200" ] && return 0
      echo "  FATAL: fixture POST $1 -> $code" >&2
      return 1 ;;
    *) echo "  FATAL: fixture POST $1 -> $code" >&2; return 1 ;;
  esac
}

# ------------------------------------------------------------------ boot bits
NEXT_HTTP_PORT="$HTTP_PORT_BASE"
NEXT_GRPC_PORT="$GRPC_PORT_BASE"

boot_replica() { # boot_replica <cell> <idx> <driver> <redis_addr|-> ; sets R_URL
  local cell="$1" idx="$2" driver="$3" redis_addr="$4"
  local port="$NEXT_HTTP_PORT"; NEXT_HTTP_PORT=$((NEXT_HTTP_PORT+1))
  local gport="$NEXT_GRPC_PORT"; NEXT_GRPC_PORT=$((NEXT_GRPC_PORT+1))
  local log="$WORK/$cell-r$idx.log"

  # JANUS_HTTP_HOST=localhost: auth-disabled mode refuses non-loopback binds
  # (server/cmd/janus-api/main.go). Absolute JANUS_MIGRATION_PATH + CWD=$WORK
  # keeps viper from picking up a stray janus.yaml in the repo root.
  local envs=(
    "JANUS_HTTP_PORT=$port"
    "JANUS_HTTP_HOST=localhost"
    "JANUS_GRPC_PORT=$gport"
    "JANUS_MIGRATION_AUTO=true"
    "JANUS_MIGRATION_PATH=$ROOT/migrations"
    "JANUS_AUTH_ENABLED=false"
    "JANUS_PG_HOST=$PG_HOST"
    "JANUS_PG_PORT=$PG_PORT"
    "JANUS_PG_USER=$PG_USER"
    "JANUS_PG_PASSWORD=$PG_PASSWORD"
    "JANUS_PG_DATABASE=$CELL_DB"
    "JANUS_PG_SSLMODE=$PG_SSLMODE"
    "JANUS_QUEUE_DRIVER=$driver"
  )
  [ "$driver" = "nats" ] && envs+=("JANUS_NATS_URL=$NATS_URL")
  [ "$redis_addr" != "-" ] && envs+=("JANUS_REDIS_ADDR=$redis_addr")

  ( cd "$WORK" && exec env "${envs[@]}" "$BIN" >"$log" 2>&1 ) &
  local pid=$!
  PIDS+=("$pid")

  local i
  for i in $(seq 1 60); do
    if curl -sf -m 2 "http://localhost:$port/readyz" >/dev/null 2>&1; then
      R_URL="http://localhost:$port"
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "  FATAL: $cell replica $idx exited early; log tail:" >&2
      tail -n 20 "$log" >&2 || true
      return 1
    fi
    sleep 1
  done
  echo "  FATAL: $cell replica $idx never became ready; log tail:" >&2
  tail -n 20 "$log" >&2 || true
  return 1
}

stop_cell_replicas() { # stop + reap the cell's replicas; a survivor keeps its global
  # outbox drain alive and would swallow the next cell's publishes
  local pid i
  for pid in "${PIDS[@]:$CELL_START}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]:$CELL_START}"; do
    for i in $(seq 1 100); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}

# ------------------------------------------------------------- chain helpers
TASK_BODY() { # TASK_BODY <tid> -> create-task JSON
  local tid="$1"
  printf '{"id":"%s","source_agent":"%s","target_type":"mailbox","target_value":"%s","envelope":{"janus_version":"1.0","task_id":"%s","tenant_id":"%s","source_agent":"%s","target":{"type":"mailbox","value":"%s"},"payload":{"type":"ga07","content":"{}"},"trace":{"trace_id":"ga07-%s"}}}' \
    "$tid" "$AGENT" "$MB" "$tid" "$TENANT" "$AGENT" "$MB" "$tid"
}

pull_until() { # pull_until <base_url> <tid> <max_tries> -> sets LEASE ("" on timeout)
  local base="$1" tid="$2" tries="${3:-30}" i out code body got
  LEASE=""
  for i in $(seq 1 "$tries"); do
    out="$(api POST "$base/v1/tenants/$TENANT/mailboxes/$MB/pull" "{\"agent_id\":\"$AGENT\"}")"
    code="${out%% *}"
    if [ "$code" = "200" ]; then
      body="${out#* }"
      got="$(jget "$body" "d['task']['id']")"
      if [ "$got" = "$tid" ]; then
        LEASE="$(jget "$body" "d['lease']['lease_id']")"
        return 0
      fi
    fi
    sleep 1
  done
  return 1
}

wait_audit_events() { # wait_audit_events <tid> -> sorted distinct event types for the task
  local tid="$1" i types=""
  for i in $(seq 1 20); do
    types="$(sql "SELECT string_agg(DISTINCT event_type, ',' ORDER BY event_type) FROM audit_event_projection WHERE tenant_id='$TENANT' AND task_id='$tid'")"
    # the projector drains in created_at order: once task.completed (the last
    # lifecycle write) is projected, every earlier event is too.
    if [[ ",$types," == *",task.created,"* && ",$types," == *",task.completed,"* ]]; then
      break
    fi
    sleep 1
  done
  printf '%s' "$types"
}

# ==================================================================== prelude
say "GA-07 four-cell matrix: {pg, pg+nats} x {1, 2} replicas"

for tool in curl psql python3; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool missing: $tool"
done

say "1. PostgreSQL reachability ($PG_HOST:$PG_PORT/$PG_DBNAME)"
if ! sql "SELECT 1" >/dev/null 2>&1; then
  die "PostgreSQL unreachable at $PG_HOST:$PG_PORT (user=$PG_USER db=$PG_DBNAME) — GA-07 requires a live database; refusing to SKIP green"
fi
echo "  OK: postgres reachable"

say "2. NATS reachability ($NATS_URL) — required by the pg+nats cells only"
NATS_REACHABLE=0
NATS_HP="${NATS_URL#*://}"
NATS_HOST="${NATS_HP%%:*}"
NATS_PORT="${NATS_HP##*:}"
[ "$NATS_HOST" = "localhost" ] && NATS_HOST="127.0.0.1"
for _ in 1 2 3; do
  if (exec 3<>"/dev/tcp/$NATS_HOST/$NATS_PORT") 2>/dev/null; then NATS_REACHABLE=1; break; fi
  sleep 1
done
if [ "$NATS_REACHABLE" = 1 ]; then
  echo "  OK: nats reachable"
else
  echo "  !! nats UNREACHABLE — cells ga07-nats-1r and ga07-nats-2r will FAIL (not skip)"
fi

say "3. Build janus-api (server module; repo root is NOT a Go module)"
if [ -n "${JANUS_API_BIN:-}" ]; then
  BIN="$JANUS_API_BIN"
  [ -x "$BIN" ] || die "JANUS_API_BIN=$BIN is not executable"
  echo "  using provided binary: $BIN"
else
  BIN="$WORK/janus-api"
  ( cd "$ROOT/server" && go build -o "$BIN" ./cmd/janus-api ) \
    || die "go build ./cmd/janus-api failed"
  echo "  built: $BIN"
fi

# ================================================================== one cell
run_cell() { # run_cell <name> <driver> <replicas> <redis_addr|->
  local CELL="$1" DRIVER="$2" REPLICAS="$3" REDIS="$4"
  TENANT="$CELL"
  AGENT="ga07-agent"
  MB="ga07-mb"
  CPASS=0; CFAIL=0
  CELL_START="${#PIDS[@]}"

  CELL_DB="janus_$(printf '%s' "$CELL" | tr -c 'a-z0-9' '_')"
  CELL_DBS+=("$CELL_DB")
  psql_admin "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$CELL_DB'" >/dev/null 2>&1 || true
  psql_admin "DROP DATABASE IF EXISTS $CELL_DB" >/dev/null \
    || die "cannot drop stale cell DB $CELL_DB"
  psql_admin "CREATE DATABASE $CELL_DB OWNER $PG_USER" >/dev/null \
    || die "cannot create cell DB $CELL_DB (does user $PG_USER have CREATEDB?)"

  say "CELL $CELL (driver=$DRIVER replicas=$REPLICAS redis=${REDIS:-(default)})"

  # --- boot ------------------------------------------------------------
  local URL_A="" URL_B=""
  boot_replica "$CELL" 1 "$DRIVER" "$REDIS" || { cell_result; return; }
  URL_A="$R_URL"
  if [ "$REPLICAS" -ge 2 ]; then
    boot_replica "$CELL" 2 "$DRIVER" "$REDIS" || { stop_cell_replicas; cell_result; return; }
    URL_B="$R_URL"
  fi
  # delivery target: replica B in 2-replica cells (proves cross-replica delivery)
  local URL_PULL="${URL_B:-$URL_A}"
  echo "  booted: A=$URL_A${URL_B:+ B=$URL_B} (pull via $URL_PULL)"

  # --- fixture: tenant + agent + mailbox (via replica A) ----------------
  create_or_exists "$URL_A/v1/tenants" \
    "{\"id\":\"$TENANT\",\"name\":\"GA07 $CELL\"}" \
    "$URL_A/v1/tenants/$TENANT" || { stop_cell_replicas; cell_result; return; }
  create_or_exists "$URL_A/v1/tenants/$TENANT/agents" \
    "{\"id\":\"$AGENT\",\"display_name\":\"GA07 $CELL\",\"protocol\":\"a2a\"}" \
    "$URL_A/v1/tenants/$TENANT/agents/$AGENT" || { stop_cell_replicas; cell_result; return; }
  create_or_exists "$URL_A/v1/tenants/$TENANT/mailboxes" \
    "{\"id\":\"$MB\",\"agent_id\":\"$AGENT\"}" \
    "$URL_A/v1/tenants/$TENANT/mailboxes/$MB" || { stop_cell_replicas; cell_result; return; }
  echo "  fixture: tenant/agent/mailbox ready"
  check "tenant row persisted in PG" \
    "[ \"$(sql "SELECT count(*) FROM tenants WHERE id='$TENANT'")\" = 1 ]" \
    "tenants.id=$TENANT"

  # --- leg 1: full chain ------------------------------------------------
  echo "  leg 1: create -> pull -> start -> progress -> ack"
  local TID1="ga07-chain-$(date +%s%N)"
  local out code row
  out="$(api POST "$URL_A/v1/tenants/$TENANT/tasks" "$(TASK_BODY "$TID1")")"
  code="${out%% *}"
  check "task create accepted (HTTP $code)" \
    "[ \"$(sql "SELECT count(*) FROM tasks WHERE tenant_id='$TENANT' AND id='$TID1'")\" -ge 1 ]" \
    "tasks.id=$TID1 row exists in PG"

  if pull_until "$URL_PULL" "$TID1" 30; then
    check "task delivered via dispatch pull${URL_B:+ (created on A, pulled on B)}" "[ -n '$LEASE' ]" "lease=${LEASE:0:12}..."
  else
    check "task delivered via dispatch pull${URL_B:+ (created on A, pulled on B)}" "[ 1 -eq 2 ]" "pull never returned $TID1"
  fi

  api POST "$URL_A/v1/tenants/$TENANT/tasks/$TID1/start" "{\"lease_id\":\"$LEASE\",\"agent_id\":\"$AGENT\"}" >/dev/null
  check "start -> tasks.status=running" \
    "[ \"$(sql "SELECT status FROM tasks WHERE tenant_id='$TENANT' AND id='$TID1'")\" = running ]" \
    "status=$(sql "SELECT status FROM tasks WHERE tenant_id='$TENANT' AND id='$TID1'")"

  out="$(api POST "$URL_A/v1/tenants/$TENANT/tasks/$TID1/progress" "{\"message\":\"ga07 working\",\"percent\":50,\"agent_id\":\"$AGENT\"}")"
  check "progress accepted (HTTP ${out%% *})" "[ \"${out%% *}\" = 202 ]" "progress handler status"

  local RESULT_REF="ga07://results/$TID1"
  api POST "$URL_A/v1/tenants/$TENANT/tasks/$TID1/ack" "{\"lease_id\":\"$LEASE\",\"agent_id\":\"$AGENT\",\"result_ref\":\"$RESULT_REF\"}" >/dev/null
  row="$(sql "SELECT status || '|' || coalesce(result_ref,'') FROM tasks WHERE tenant_id='$TENANT' AND id='$TID1'")"
  check "ack -> tasks.status=completed" "[ \"${row%%|*}\" = completed ]" "tasks row: $row"
  check "ack -> tasks.result_ref persisted" "[ \"${row#*|}\" = \"$RESULT_REF\" ]" "result_ref=${row#*|}"

  local a_cnt a_done a_list
  a_cnt="$(sql "SELECT count(*) FROM task_attempts WHERE tenant_id='$TENANT' AND task_id='$TID1'")"
  a_done="$(sql "SELECT count(*) FROM task_attempts WHERE tenant_id='$TENANT' AND task_id='$TID1' AND status='completed' AND length(lease_id) > 0")"
  a_list="$(sql "SELECT coalesce(string_agg(attempt || '=' || status || ' lease=' || CASE WHEN lease_id='' THEN 'EMPTY' ELSE 'set' END, ', ' ORDER BY attempt), 'NONE') FROM task_attempts WHERE tenant_id='$TENANT' AND task_id='$TID1'")"
  check "exactly one attempt row" "[ \"$a_cnt\" -eq 1 ]" "task_attempts: $a_list"
  check "attempt completed with non-empty lease_id" "[ \"$a_done\" -eq 1 ]" "completed+lease rows: $a_done"

  # outbox terminal state is 'published' (values from
  # server/internal/driver/postgres/outbox_repo.go: pending / retry /
  # publishing / published / dead / quarantine)
  local o_bad="" i o_list
  for i in $(seq 1 20); do
    o_bad="$(sql "SELECT count(*) FROM outbox_events WHERE tenant_id='$TENANT' AND (payload->>'TaskID'='$TID1' OR payload->>'task_id'='$TID1') AND status <> 'published'")"
    [ "$o_bad" = "0" ] && break
    sleep 1
  done
  o_list="$(sql "SELECT coalesce(string_agg(kind || '/' || status, ' ' ORDER BY created_at), 'NONE') FROM outbox_events WHERE tenant_id='$TENANT' AND (payload->>'TaskID'='$TID1' OR payload->>'task_id'='$TID1')")"
  check "outbox rows all published" "[ \"$o_bad\" = 0 ]" "outbox: $o_list"

  # lifecycle event types verified against core/event.go
  # (task.created/queued/claimed/started/completed)
  local types ev
  types="$(wait_audit_events "$TID1")"
  for ev in task.created task.queued task.claimed task.started task.completed; do
    check "audit event $ev projected" "[[ \",$types,\" == *\",$ev,\"* ]]" "events: ${types:-NONE}"
  done

  # --- leg 2: NACK retriable -> retry_scheduled -> promote -> complete ----
  echo "  leg 2: nack retriable -> retry_scheduled -> scheduler promote -> complete"
  local TID2="ga07-retry-$(date +%s%N)"
  api POST "$URL_A/v1/tenants/$TENANT/tasks" "$(TASK_BODY "$TID2")" >/dev/null
  if pull_until "$URL_PULL" "$TID2" 30; then
    check "retry-leg task delivered" "[ -n '$LEASE' ]" "lease=${LEASE:0:12}..."
  else
    check "retry-leg task delivered" "[ 1 -eq 2 ]" "pull never returned $TID2"
  fi
  api POST "$URL_A/v1/tenants/$TENANT/tasks/$TID2/start" "{\"lease_id\":\"$LEASE\",\"agent_id\":\"$AGENT\"}" >/dev/null
  api POST "$URL_A/v1/tenants/$TENANT/tasks/$TID2/nack" "{\"lease_id\":\"$LEASE\",\"agent_id\":\"$AGENT\",\"retriable\":true,\"error\":{\"code\":\"GA07_RETRY\",\"message\":\"retry me\"}}" >/dev/null
  row="$(sql "SELECT status || '|' || coalesce(retry_at::text,'NULL') || '|' || (retry_at > now())::text FROM tasks WHERE tenant_id='$TENANT' AND id='$TID2'")"
  check "nack -> tasks.status=retry_scheduled" "[ \"${row%%|*}\" = retry_scheduled ]" "tasks row: $row"
  check "retry_at scheduled in the future" "[ \"${row##*|}\" = true ]" "retry_at=${row#*|}"

  # retry scheduler (1s tick) promotes retry_at<=now() back to queued and
  # re-publishes; mailbox default retry policy initial backoff is 10s
  local promoted=0
  for i in $(seq 1 45); do
    [ "$(sql "SELECT count(*) FROM tasks WHERE tenant_id='$TENANT' AND id='$TID2' AND status IN ('queued','claimed','running','completed')")" -ge 1 ] && { promoted=1; break; }
    sleep 1
  done
  check "retry scheduler promoted the task" "[ \"$promoted\" = 1 ]" \
    "status=$(sql "SELECT status FROM tasks WHERE tenant_id='$TENANT' AND id='$TID2'")"

  if pull_until "$URL_PULL" "$TID2" 30; then
    api POST "$URL_A/v1/tenants/$TENANT/tasks/$TID2/start" "{\"lease_id\":\"$LEASE\",\"agent_id\":\"$AGENT\"}" >/dev/null
    api POST "$URL_A/v1/tenants/$TENANT/tasks/$TID2/ack" "{\"lease_id\":\"$LEASE\",\"agent_id\":\"$AGENT\",\"result_ref\":\"ga07://retry-ok/$TID2\"}" >/dev/null
  fi
  row="$(sql "SELECT status FROM tasks WHERE tenant_id='$TENANT' AND id='$TID2'")"
  check "retry leg final tasks.status=completed" "[ \"$row\" = completed ]" "status=$row"
  a_cnt="$(sql "SELECT count(*) FROM task_attempts WHERE tenant_id='$TENANT' AND task_id='$TID2'")"
  a_list="$(sql "SELECT coalesce(string_agg(attempt || '=' || status, ',' ORDER BY attempt), 'NONE') FROM task_attempts WHERE tenant_id='$TENANT' AND task_id='$TID2'")"
  check "retry leg has exactly two attempts" "[ \"$a_cnt\" -eq 2 ]" "attempts: $a_list"

  # --- leg 3: cancel ------------------------------------------------------
  echo "  leg 3: create -> claim -> cancel"
  local TID3="ga07-cancel-$(date +%s%N)"
  api POST "$URL_A/v1/tenants/$TENANT/tasks" "$(TASK_BODY "$TID3")" >/dev/null
  pull_until "$URL_PULL" "$TID3" 30 || true
  api POST "$URL_A/v1/tenants/$TENANT/tasks/$TID3/start" "{\"lease_id\":\"$LEASE\",\"agent_id\":\"$AGENT\"}" >/dev/null
  api POST "$URL_A/v1/tenants/$TENANT/tasks/$TID3/cancel" "{}" >/dev/null
  row="$(sql "SELECT status FROM tasks WHERE tenant_id='$TENANT' AND id='$TID3'")"
  check "cancel -> tasks.status=cancelled" "[ \"$row\" = cancelled ]" "status=$row"
  local open_cnt
  open_cnt="$(sql "SELECT count(*) FROM task_attempts WHERE tenant_id='$TENANT' AND task_id='$TID3' AND finished_at IS NULL")"
  a_list="$(sql "SELECT coalesce(string_agg(attempt || '=' || status || ' fin=' || coalesce(finished_at::text,'NULL'), ', ' ORDER BY attempt), 'NONE') FROM task_attempts WHERE tenant_id='$TENANT' AND task_id='$TID3'")"
  check "cancel leaves no open attempt" "[ \"$open_cnt\" -eq 0 ]" "open=$open_cnt attempts: $a_list"

  # --- leg 4: priority order (PG scheduler; NATS delivery is FIFO) -------
  if [ "$DRIVER" = "pg" ]; then
    echo "  leg 4: priority order critical > high > normal > low"
    local pfx="ga07-prio-$(date +%s%N)"
    local p prio tid_p
    for prio in low normal high critical; do
      api POST "$URL_A/v1/tenants/$TENANT/tasks" "$(TASK_BODY "$pfx-$prio")" >/dev/null
      sql "UPDATE tasks SET priority='$prio' WHERE tenant_id='$TENANT' AND id='$pfx-$prio'" >/dev/null
    done
    local order="" got_id
    local prio_ids=""
    for p in 1 2 3 4; do
      got_id=""
      out="$(api POST "$URL_PULL/v1/tenants/$TENANT/mailboxes/$MB/pull" "{\"agent_id\":\"$AGENT\"}")"
      if [ "${out%% *}" = "200" ]; then
        got_id="$(jget "${out#* }" "d['task']['id']")"
      fi
      if [ -n "$got_id" ]; then
        order="$order ${got_id##*-}"
        prio_ids="$prio_ids $got_id"
      fi
    done
    check "pg queue delivers critical > high > normal > low" \
      "[ \"$order\" = \" critical high normal low\" ]" "order:$order"

    local pair_id pair_lease
    for pair_id in $prio_ids; do
      pair_lease="$(sql "SELECT lease_id FROM task_attempts WHERE tenant_id='$TENANT' AND task_id='$pair_id' AND finished_at IS NULL ORDER BY attempt DESC LIMIT 1")"
      api POST "$URL_A/v1/tenants/$TENANT/tasks/$pair_id/start" "{\"lease_id\":\"$pair_lease\",\"agent_id\":\"$AGENT\"}" >/dev/null 2>&1 || true
      api POST "$URL_A/v1/tenants/$TENANT/tasks/$pair_id/ack" "{\"lease_id\":\"$pair_lease\",\"agent_id\":\"$AGENT\",\"result_ref\":\"ga07://prio/$pair_id\"}" >/dev/null 2>&1 || true
    done
  fi

  # --- leg 5: mailbox pause / resume ------------------------------------
  # Pause stops NEW work: routing admission rejects creates aimed at a
  # paused mailbox; the backlog may still drain. Resume reopens intake.
  echo "  leg 5: pause gates new-task intake -> resume reopens -> delivery"
  local TID5="ga07-pause-$(date +%s%N)"
  api POST "$URL_A/v1/tenants/$TENANT/mailboxes/$MB/pause" "{}" >/dev/null
  check "pause -> mailboxes.status=paused" \
    "[ \"$(sql "SELECT status FROM mailboxes WHERE tenant_id='$TENANT' AND id='$MB'")\" = paused ]" \
    "status=$(sql "SELECT status FROM mailboxes WHERE tenant_id='$TENANT' AND id='$MB'")"
  out="$(api POST "$URL_A/v1/tenants/$TENANT/tasks" "$(TASK_BODY "$TID5")")"
  check "paused mailbox rejects new tasks (HTTP ${out%% *})" \
    "[ \"${out%% *}\" != 201 ] && [ \"${out%% *}\" != 200 ]" \
    "$(printf '%s' "${out#* }" | head -c 120)"
  api POST "$URL_A/v1/tenants/$TENANT/mailboxes/$MB/resume" "{}" >/dev/null
  check "resume -> mailboxes.status=active" \
    "[ \"$(sql "SELECT status FROM mailboxes WHERE tenant_id='$TENANT' AND id='$MB'")\" = active ]" \
    "status=$(sql "SELECT status FROM mailboxes WHERE tenant_id='$TENANT' AND id='$MB'")"
  api POST "$URL_A/v1/tenants/$TENANT/tasks" "$(TASK_BODY "$TID5")" >/dev/null
  if pull_until "$URL_PULL" "$TID5" 30; then
    api POST "$URL_A/v1/tenants/$TENANT/tasks/$TID5/start" "{\"lease_id\":\"$LEASE\",\"agent_id\":\"$AGENT\"}" >/dev/null
    api POST "$URL_A/v1/tenants/$TENANT/tasks/$TID5/ack" "{\"lease_id\":\"$LEASE\",\"agent_id\":\"$AGENT\",\"result_ref\":\"ga07://pause-ok\"}" >/dev/null
    check "post-resume intake delivers and completes" \
      "[ \"$(sql "SELECT status FROM tasks WHERE tenant_id='$TENANT' AND id='$TID5'")\" = completed ]" \
      "status=$(sql "SELECT status FROM tasks WHERE tenant_id='$TENANT' AND id='$TID5'")"
  else
    check "post-resume intake delivers and completes" "[ 1 -eq 2 ]" "pull never returned $TID5"
  fi

  # --- leg 6: degraded-dependency readiness (redis down, PG cells) -------
  if [ "$REDIS" != "-" ]; then
    echo "  leg 6: readyz stays ready with redis unreachable"
    local ready_body
    ready_body="$(api GET "$URL_A/readyz" -)"
    check "readyz 200 with redis down (degraded, not unavailable)" \
      "[ \"${ready_body%% *}\" = 200 ]" "$(printf '%s' "${ready_body#* }" | head -c 160)"
  fi

  stop_cell_replicas
  cell_result
  drop_cell_db
}

# pull_next_id posts one pull and prints the delivered task id ("" on empty).
pull_next_id() {
  local base="$1" tries="${2:-30}" i out code body got
  for i in $(seq 1 "$tries"); do
    out="$(api POST "$base/v1/tenants/$TENANT/mailboxes/$MB/pull" "{\"agent_id\":\"$AGENT\"}")"
    code="${out%% *}"
    if [ "$code" = "200" ]; then
      body="${out#* }"
      got="$(jget "$body" "d['task']['id']")"
      [ -n "$got" ] && { printf '%s' "$got"; return 0; }
    fi
    sleep 1
  done
  return 1
}

drop_cell_db() {
  if [ "${KEEP_WORK:-0}" = "1" ]; then
    echo "  KEEP_WORK=1: cell DB $CELL_DB retained"
    return 0
  fi
  psql_admin "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$CELL_DB'" >/dev/null 2>&1 || true
  psql_admin "DROP DATABASE IF EXISTS $CELL_DB" >/dev/null 2>&1 || true
}

cell_result() {
  if [ "$CFAIL" -eq 0 ] && [ "$CPASS" -gt 0 ]; then
    echo "CELL $CELL -> PASS (checks: $CPASS)"
    CELLS_PASS=$((CELLS_PASS+1))
  else
    echo "CELL $CELL -> FAIL (checks: $CPASS passed, $CFAIL failed)"
    CELLS_FAIL=$((CELLS_FAIL+1))
  fi
}

# ==================================================================== matrix
# GA07_CELLS (comma list of cell names, e.g. "ga07-nats-1r") runs a subset.
want_cell() { case ",${GA07_CELLS:-}," in *",$1,"*|",,") return 0 ;; *) return 1 ;; esac; }
want_cell "ga07-pg-1r"   && run_cell "ga07-pg-1r"   "pg"   1 "$DEAD_REDIS_ADDR"
want_cell "ga07-pg-2r"   && run_cell "ga07-pg-2r"   "pg"   2 "$DEAD_REDIS_ADDR"
want_cell "ga07-nats-1r" && run_cell "ga07-nats-1r" "nats" 1 "-"
want_cell "ga07-nats-2r" && run_cell "ga07-nats-2r" "nats" 2 "-"

say "Summary"
echo "GA07_FOUR_CELL: PASS=$CELLS_PASS FAIL=$CELLS_FAIL"
[ "$CELLS_FAIL" -eq 0 ] || exit 1
exit 0

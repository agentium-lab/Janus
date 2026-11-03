#!/usr/bin/env bash
# dr_ga10_pitr.sh — GA-10 point-in-time-recovery drill (data-loss recovery contract).
#
# Contract under test (confirmed with the product owner):
#   * PITR RPO <= 5 minutes, bounded by archive_timeout. The drill runs with
#     archive_timeout=60s (GA10_PITR_ARCHIVE_TIMEOUT_S) so it finishes fast;
#     production would run 300s to sit exactly on the 5-minute bound.
#   * PITR RTO <= 30 minutes (GA10_PITR_RTO_BUDGET_S, default 1800). The drill
#     restore is expected to take seconds; it is measured and gated.
#
# Method: run a throwaway archiving PG cluster, boot janus-api against it with
# the pg queue driver (no NATS, no Redis), drive batch 1 through the real HTTP
# API, take a pg_basebackup, drive batch 2 (rows that exist only in WAL),
# snapshot per-object counts+checksums, simulate catastrophic data loss
# (DELETE FROM tasks/task_attempts/budget_usage_ledger), then restore a second
# cluster from the base backup + archived WAL to a recovery_target_time taken
# after batch 2. Every reconciled object must match snapshot #2 exactly —
# proving both that batch-2 rows came back through WAL replay and that the
# DELETE was undone.
#
# The six reconciliation objects: tasks, task_attempts, outbox_events,
# budget_usage_ledger, audit_event_projection, and — because this schema has no
# dlq table — the count of tasks with status='dead_lettered' as the DLQ proxy.
#
# Exit codes: 0 PASS; 1 gate/verification FAIL; 2 missing tool / unreachable
# dependency (never a green SKIP).
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK=$(mktemp -d /tmp/janus-ga10-pitr.XXXXXX)
PRIMARY_PORT=${GA10_PITR_PRIMARY_PORT:-55440}
RESTORE_PORT=${GA10_PITR_RESTORE_PORT:-55441}
HTTP_PORT=${GA10_PITR_HTTP_PORT:-18220}
RTO_BUDGET_S=${GA10_PITR_RTO_BUDGET_S:-1800}
ARCHIVE_TIMEOUT_S=${GA10_PITR_ARCHIVE_TIMEOUT_S:-60}
PG_ROLE=janus
PG_DB=janus_dr
TENANT=dr-ga10
MB=mb-dr
AGENT=worker
SRC_AGENT=producer
SOCK_DIR="$WORK/sock"
ARCHIVE_DIR="$WORK/archive"
PGDATA_PRIMARY="$WORK/primary"
PGDATA_RESTORE="$WORK/restore"
API_PID_FILE="$WORK/api.pid"

log()  { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { printf '[%s] FATAL: %s\n' "$(date +%H:%M:%S)" "$*" >&2; exit 2; }

# ---------------------------------------------------------------- cleanup ---
cleanup() {
    local rc=$?
    trap - EXIT
    if [ -s "$API_PID_FILE" ] && kill -0 "$(cat "$API_PID_FILE")" 2>/dev/null; then
        kill "$(cat "$API_PID_FILE")" 2>/dev/null || true
    fi
    if [ -n "${PG_BIN:-}" ] && [ -x "$PG_BIN/pg_ctl" ]; then
        "$PG_BIN/pg_ctl" -D "$PGDATA_RESTORE" stop -m immediate -t 20 >/dev/null 2>&1 || true
        "$PG_BIN/pg_ctl" -D "$PGDATA_PRIMARY" stop -m immediate -t 20 >/dev/null 2>&1 || true
    fi
    if [ "${KEEP_WORK:-0}" = "1" ]; then
        echo "KEEP_WORK=1: workdir retained at $WORK (api.log, primary.log, restore.log)"
    else
        rm -rf "$WORK"
    fi
    exit "$rc"
}
trap cleanup EXIT

# --------------------------------------------------------- toolchain check ---
detect_pg_bindir() {
    local d
    if [ -n "${PG_BINDIR:-}" ] && [ -x "$PG_BINDIR/pg_ctl" ]; then printf '%s' "$PG_BINDIR"; return 0; fi
    if [ -x "$HOME/.local/pgsql/bin/pg_ctl" ]; then printf '%s' "$HOME/.local/pgsql/bin"; return 0; fi
    if command -v pg_ctl >/dev/null 2>&1; then dirname "$(command -v pg_ctl)"; return 0; fi
    for d in /usr/lib/postgresql/*/bin; do
        if [ -x "$d/pg_ctl" ]; then printf '%s' "$d"; return 0; fi
    done
    return 1
}
PG_BIN=$(detect_pg_bindir) || fail "no PostgreSQL toolchain found (tried \$PG_BINDIR, ~/.local/pgsql/bin, PATH, /usr/lib/postgresql/*/bin)"
log "pg toolchain: $PG_BIN"
for t in initdb pg_ctl psql pg_basebackup createdb; do
    if [ ! -x "$PG_BIN/$t" ]; then fail "required tool missing: $PG_BIN/$t"; fi
done
for t in curl jq go awk; do
    command -v "$t" >/dev/null 2>&1 || fail "required tool missing from PATH: $t"
done
log "GA-10 PITR drill: primary=:$PRIMARY_PORT restore=:$RESTORE_PORT api=:$HTTP_PORT work=$WORK"
log "RPO contract <= 300s, drill archive_timeout=${ARCHIVE_TIMEOUT_S}s; RTO budget ${RTO_BUDGET_S}s"

# Our clusters must never collide with the dev instance on 5432; also refuse
# to start if the drill ports are already taken. /dev/tcp probing: host and
# port are separate path segments (no 'nc' on this box).
tcp_connect_ok() { # host port
    (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null
}
if tcp_connect_ok 127.0.0.1 "$PRIMARY_PORT"; then fail "port $PRIMARY_PORT already in use"; fi
if tcp_connect_ok 127.0.0.1 "$RESTORE_PORT"; then fail "port $RESTORE_PORT already in use"; fi

PSQL() { # port sql -> stdout
    "$PG_BIN/psql" -X -h "$SOCK_DIR" -p "$1" -U "$PG_ROLE" -d "$PG_DB" \
        -v ON_ERROR_STOP=1 -Atqc "$2"
}

# ------------------------------------------- reconciliation object queries ---
# Columns read from migrations/*.up.sql (000001/000004/000010/000017), not guessed.
OBJ_SQL=$(cat <<'SQL'
SELECT 'tasks', count(*)::text,
       md5(coalesce(string_agg(id || ':' || status, ',' ORDER BY id), '<none>'))
FROM tasks
UNION ALL
SELECT 'task_attempts', count(*)::text,
       md5(coalesce(string_agg(task_id || ':' || attempt || ':' || agent_id || ':' || status,
                               ',' ORDER BY tenant_id, task_id, attempt), '<none>'))
FROM task_attempts
UNION ALL
SELECT 'outbox_events', count(*)::text,
       md5(coalesce(string_agg(id || ':' || kind || ':' || status, ',' ORDER BY id), '<none>'))
FROM outbox_events
UNION ALL
SELECT 'budget_usage_ledger', count(*)::text,
       md5(coalesce(string_agg(task_id || ':' || attempt || ':' || scope_type || ':' || scope_id || ':' || total_tokens,
                               ',' ORDER BY tenant_id, task_id, attempt, scope_type, scope_id), '<none>'))
FROM budget_usage_ledger
UNION ALL
SELECT 'audit_event_projection', count(*)::text,
       md5(coalesce(string_agg(event_id || ':' || event_type, ',' ORDER BY tenant_id, event_id), '<none>'))
FROM audit_event_projection
UNION ALL
SELECT 'dlq_proxy', count(*)::text,
       md5(coalesce(string_agg(id || ':' || status, ',' ORDER BY tenant_id, id), '<none>'))
FROM tasks WHERE status = 'dead_lettered'
ORDER BY 1
SQL
)
COUNT_SQL="SELECT (SELECT count(*) FROM tasks) || '/' || (SELECT count(*) FROM task_attempts) \
|| '/' || (SELECT count(*) FROM outbox_events) || '/' || (SELECT count(*) FROM budget_usage_ledger) \
|| '/' || (SELECT count(*) FROM audit_event_projection) \
|| '/' || (SELECT count(*) FROM tasks WHERE status = 'dead_lettered')"

snapshot() { # port outfile -> "obj|count|md5" lines
    PSQL "$1" "$OBJ_SQL" > "$2"
}

snap_val() { # outfile obj field(2|3)
    awk -F'|' -v o="$2" -v f="$3" '$1 == o { print $f; exit }' "$1"
}

# =================================================================== step 1 ==
log "step 1: init archiving cluster (archive_mode=on, archive_timeout=${ARCHIVE_TIMEOUT_S}s, wal_level=replica)"
mkdir -p "$SOCK_DIR" "$ARCHIVE_DIR"
chmod 700 "$SOCK_DIR"
"$PG_BIN/initdb" -D "$PGDATA_PRIMARY" -U "$PG_ROLE" --auth=trust >"$WORK/initdb.log" 2>&1 \
    || fail "initdb failed (see $WORK/initdb.log)"
cat >> "$PGDATA_PRIMARY/postgresql.conf" <<EOF

# --- dr_ga10_pitr drill overrides ---
listen_addresses = 'localhost'
port = $PRIMARY_PORT
unix_socket_directories = '$SOCK_DIR'
wal_level = replica
archive_mode = on
archive_command = 'test ! -f $ARCHIVE_DIR/%f && cp "%p" $ARCHIVE_DIR/%f'
archive_timeout = ${ARCHIVE_TIMEOUT_S}s
EOF
"$PG_BIN/pg_ctl" -D "$PGDATA_PRIMARY" -l "$WORK/primary.log" -w -t 60 start >/dev/null \
    || fail "primary cluster failed to start (see $WORK/primary.log)"
"$PG_BIN/createdb" -h "$SOCK_DIR" -p "$PRIMARY_PORT" -U "$PG_ROLE" "$PG_DB" \
    || fail "createdb $PG_DB failed"
log "primary cluster up on :$PRIMARY_PORT, empty db '$PG_DB' created"

# =================================================================== step 2 ==
log "step 2: build + boot janus-api (pg queue driver: no NATS, no Redis)"
if ! (cd "$REPO_ROOT/server" && go build -o "$WORK/janus-api" ./cmd/janus-api); then
    fail "go build ./cmd/janus-api failed"
fi
(
    cd "$REPO_ROOT"
    export JANUS_QUEUE_DRIVER=pg
    export JANUS_PG_HOST=127.0.0.1
    export JANUS_PG_PORT="$PRIMARY_PORT"
    export JANUS_PG_USER="$PG_ROLE"
    export JANUS_PG_PASSWORD="$PG_ROLE"
    export JANUS_PG_DATABASE="$PG_DB"
    export JANUS_PG_SSLMODE=disable
    export JANUS_MIGRATION_AUTO=true
    export JANUS_MIGRATION_PATH=migrations
    export JANUS_AUTH_ENABLED=false
    export JANUS_HTTP_HOST=127.0.0.1
    export JANUS_HTTP_PORT="$HTTP_PORT"
    export JANUS_GRPC_PORT=19220
    export JANUS_REDIS_ADDR=localhost:59999
    exec "$WORK/janus-api"
) >"$WORK/api.log" 2>&1 &
echo $! > "$API_PID_FILE"
ready=0
for _ in $(seq 1 60); do
    if curl -fsS --max-time 2 "http://127.0.0.1:$HTTP_PORT/readyz" >/dev/null 2>&1; then ready=1; break; fi
    sleep 1
done
if [ "$ready" != 1 ]; then
    echo "--- api boot diagnostics (pid=$(cat "$API_PID_FILE" 2>/dev/null), port $HTTP_PORT) ---" >&2
    kill -0 "$(cat "$API_PID_FILE" 2>/dev/null)" 2>/dev/null && echo "api process: RUNNING (never became ready)" >&2 || echo "api process: DEAD (crashed at boot)" >&2
    ss -ltn 2>/dev/null | grep ":$HTTP_PORT " >&2 || true
    echo "--- api.log tail ---" >&2
    tail -30 "$WORK/api.log" >&2 2>/dev/null || true
    fail "API did not become ready on :$HTTP_PORT within 60s"
fi
log "janus-api ready on 127.0.0.1:$HTTP_PORT (schema via auto-migration)"

api() { # method path [json body]
    local out
    if [ $# -ge 3 ]; then
        out=$(curl -fsS --max-time 15 -X "$1" -H 'Content-Type: application/json' -d "$3" \
            "http://127.0.0.1:$HTTP_PORT$2") || fail "HTTP $1 $2 request failed"
    else
        out=$(curl -fsS --max-time 15 -X "$1" "http://127.0.0.1:$HTTP_PORT$2") || fail "HTTP $1 $2 request failed"
    fi
    printf '%s' "$out"
}
create_task() { # task-id — full envelope per core.TaskEnvelope.Validate()
    api POST "/v1/tenants/$TENANT/tasks" \
        "{\"id\":\"$1\",\"source_agent\":\"$SRC_AGENT\",\"target_type\":\"agent\",\"target_value\":\"$AGENT\",\"mailbox_id\":\"$MB\",\"envelope\":{\"janus_version\":\"1.0\",\"task_id\":\"$1\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"$SRC_AGENT\",\"target\":{\"type\":\"agent\",\"value\":\"$AGENT\"},\"payload\":{\"type\":\"text\",\"content\":\"GA-10 PITR drill payload $1\"},\"trace\":{\"trace_id\":\"dr-$1\"}}}" >/dev/null
}
pull_one() { # -> "task_id lease_id" (retries until a task is deliverable)
    local body i
    for i in $(seq 1 60); do
        if body=$(api POST "/v1/tenants/$TENANT/mailboxes/$MB/pull" "{\"agent_id\":\"$AGENT\"}" 2>/dev/null) && [ -n "$body" ]; then
            printf '%s %s\n' "$(jq -r '.task.id' <<<"$body")" "$(jq -r '.lease.lease_id' <<<"$body")"
            return 0
        fi
        sleep 0.5
    done
    fail "pull: no task became deliverable on $MB within 30s"
}

log "fixtures: tenant=$TENANT agents=$SRC_AGENT,$AGENT mailbox=$MB"
api POST /v1/tenants "{\"id\":\"$TENANT\",\"name\":\"GA-10 PITR drill\"}" >/dev/null
api POST "/v1/tenants/$TENANT/agents" \
    "{\"id\":\"$SRC_AGENT\",\"display_name\":\"dr producer\",\"protocol\":\"a2a\",\"endpoint\":\"http://127.0.0.1:9/a2a\"}" >/dev/null
api POST "/v1/tenants/$TENANT/agents" \
    "{\"id\":\"$AGENT\",\"display_name\":\"dr worker\",\"protocol\":\"a2a\",\"endpoint\":\"http://127.0.0.1:9/a2a\"}" >/dev/null
api POST "/v1/tenants/$TENANT/mailboxes" "{\"id\":\"$MB\",\"agent_id\":\"$AGENT\",\"ack_wait_seconds\":300}" >/dev/null

# =================================================================== step 3 ==
log "step 3: batch 1 through the real HTTP API (lifecycle coverage)"
run_ack() { # task-id prompt completion total
    local tid lease pl
    create_task "$1"
    pl=$(pull_one)
    read -r tid lease <<<"$pl"
    api POST "/v1/tenants/$TENANT/tasks/$tid/start" "{\"lease_id\":\"$lease\",\"agent_id\":\"$AGENT\"}" >/dev/null
    api POST "/v1/tenants/$TENANT/tasks/$tid/ack" \
        "{\"lease_id\":\"$lease\",\"agent_id\":\"$AGENT\",\"result_ref\":\"dr://results/$tid\",\"token_usage\":{\"prompt_tokens\":$2,\"completion_tokens\":$3,\"total_tokens\":$4}}" >/dev/null
    log "  $tid: pulled -> started -> acked with usage (ledger row)"
}
run_nack() { # task-id retriable(0|1)
    local tid lease pl retriable
    if [ "$2" = 1 ]; then retriable=true; else retriable=false; fi
    create_task "$1"
    pl=$(pull_one)
    read -r tid lease <<<"$pl"
    api POST "/v1/tenants/$TENANT/tasks/$tid/start" "{\"lease_id\":\"$lease\",\"agent_id\":\"$AGENT\"}" >/dev/null
    api POST "/v1/tenants/$TENANT/tasks/$tid/nack" \
        "{\"lease_id\":\"$lease\",\"agent_id\":\"$AGENT\",\"retriable\":$retriable,\"error\":{\"code\":\"dr_pitr\",\"message\":\"drill injected failure\"}}" >/dev/null
    log "  $tid: pulled -> started -> nacked (retriable=$retriable)"
}
run_running() { # task-id: leave in-flight (live lease + running attempt)
    local tid lease pl
    create_task "$1"
    pl=$(pull_one)
    read -r tid lease <<<"$pl"
    api POST "/v1/tenants/$TENANT/tasks/$tid/start" "{\"lease_id\":\"$lease\",\"agent_id\":\"$AGENT\"}" >/dev/null
    log "  $tid: pulled -> started (left running, live lease)"
}
run_cancel() { # task-id: cancel from queue (never pulled)
    create_task "$1"
    api POST "/v1/tenants/$TENANT/tasks/$1/cancel" >/dev/null
    log "  $1: cancelled from queue"
}

run_ack b1-ack-a 120 80 200
run_ack b1-ack-b 90 60 150
run_nack b1-nack-r 1
run_nack b1-nack-n 0
run_running b1-run
run_cancel b1-cancel
run_cancel b1-queued   # nothing deliverable may span the backup boundary

snapshot "$PRIMARY_PORT" "$WORK/snap1.txt"
log "snapshot #1 (post batch 1, pre base backup):"
while IFS='|' read -r o c m; do log "  $o: count=$c md5=${m:0:8}"; done < "$WORK/snap1.txt"

# =================================================================== step 4 ==
log "step 4: flush WAL to archive, then pg_basebackup"
switch_and_wait() {
    local seg i
    seg=$(PSQL "$PRIMARY_PORT" "SELECT pg_walfile_name(pg_switch_wal())")
    for i in $(seq 1 60); do
        if [ -f "$ARCHIVE_DIR/$seg" ]; then log "  archived segment $seg"; return 0; fi
        sleep 1
    done
    fail "WAL segment $seg did not reach the archive within 60s"
}
switch_and_wait
if ! "$PG_BIN/pg_basebackup" -h 127.0.0.1 -p "$PRIMARY_PORT" -U "$PG_ROLE" \
        -D "$WORK/backup" -Fp -Xs -c fast; then
    fail "pg_basebackup failed"
fi
log "  base backup taken to $WORK/backup"

# =================================================================== step 5 ==
log "step 5: batch 2 AFTER the base backup (recoverable only via WAL replay)"
run_ack b2-ack-c 70 50 120       # completed + ledger row, WAL-only
run_nack b2-nack-w 0             # dead_lettered, WAL-only (DLQ proxy row)
run_cancel b2-cancel             # cancelled, WAL-only
create_task b2-queued >/dev/null
log "  b2-queued: left queued, WAL-only"

# Quiesce background writers (retry scheduler flips retry_scheduled -> queued,
# outbox publisher marks entries published, audit projector drains event_publish)
# so the recovery target and snapshot #2 describe the same quiescent state.
log "  waiting for background loops to quiesce (retry scheduler, outbox, audit projector)"
prev=
quiesced=0
zb=
for i in $(seq 1 90); do
    zb=$(PSQL "$PRIMARY_PORT" "SELECT (SELECT count(*) FROM tasks WHERE status='retry_scheduled') \
        || '/' || (SELECT count(*) FROM outbox_events WHERE status='pending') \
        || '/' || (SELECT count(*) FROM outbox_events WHERE kind='event_publish' AND projected_at IS NULL)")
    cur=$(PSQL "$PRIMARY_PORT" "$COUNT_SQL")
    if [ "$zb" = "0/0/0" ] && [ -n "$prev" ] && [ "$cur" = "$prev" ]; then
        quiesced=1; break
    fi
    prev="$cur"
    sleep 2
done
if [ "$quiesced" != 1 ]; then fail "system did not quiesce in 180s (retry/pending/unprojected=$zb)"; fi
log "  quiesced (no retry_scheduled, outbox drained, audit projected)"

RECOVERY_TARGET=$(PSQL "$PRIMARY_PORT" "SELECT now()")
log "  RECOVERY_TARGET = $RECOVERY_TARGET"
snapshot "$PRIMARY_PORT" "$WORK/snap2.txt"
log "snapshot #2 (recovery truth, post batch 2):"
while IFS='|' read -r o c m; do log "  $o: count=$c md5=${m:0:8}"; done < "$WORK/snap2.txt"

# Fixture sanity: every reconciled object must actually hold data, DLQ >= 2
# (one dead-letter per batch — batch 2's is the WAL-only proof), tasks >= 11.
s2_tasks=$(snap_val "$WORK/snap2.txt" tasks 2)
s2_attempts=$(snap_val "$WORK/snap2.txt" task_attempts 2)
s2_outbox=$(snap_val "$WORK/snap2.txt" outbox_events 2)
s2_ledger=$(snap_val "$WORK/snap2.txt" budget_usage_ledger 2)
s2_audit=$(snap_val "$WORK/snap2.txt" audit_event_projection 2)
s2_dlq=$(snap_val "$WORK/snap2.txt" dlq_proxy 2)
if [ "$s2_tasks" -lt 11 ] || [ "$s2_attempts" -lt 7 ] || [ "$s2_outbox" -lt 1 ] \
    || [ "$s2_ledger" -lt 3 ] || [ "$s2_audit" -lt 1 ] || [ "$s2_dlq" -lt 2 ]; then
    echo "GA10_PITR: FAIL fixtures incomplete before restore (tasks=$s2_tasks attempts=$s2_attempts outbox=$s2_outbox ledger=$s2_ledger audit=$s2_audit dlq=$s2_dlq)"
    exit 1
fi
switch_and_wait

# =================================================================== step 6 ==
log "step 6: DISASTER — wipe the data tables"
PSQL "$PRIMARY_PORT" "WITH d1 AS (DELETE FROM task_attempts RETURNING 1), \
    d2 AS (DELETE FROM budget_usage_ledger RETURNING 1), \
    d3 AS (DELETE FROM tasks RETURNING 1) \
    SELECT (SELECT count(*) FROM d1) || '/' || (SELECT count(*) FROM d2) || '/' || (SELECT count(*) FROM d3)" >/dev/null
gone=$(PSQL "$PRIMARY_PORT" "SELECT (SELECT count(*) FROM tasks) || '/' || (SELECT count(*) FROM task_attempts) || '/' || (SELECT count(*) FROM budget_usage_ledger)")
log "  post-delete counts (tasks/attempts/ledger): $gone"
if [ "$gone" != "0/0/0" ]; then
    echo "GA10_PITR: FAIL disaster delete did not fully remove rows (got $gone)"
    exit 1
fi

# =================================================================== step 7 ==
log "step 7: restore from base backup + archived WAL to recovery_target_time"
RTO_START=$(date +%s.%3N)
kill "$(cat "$API_PID_FILE")" 2>/dev/null || true
: > "$API_PID_FILE"
"$PG_BIN/pg_ctl" -D "$PGDATA_PRIMARY" -l "$WORK/primary.log" -w -t 60 stop -m fast >/dev/null \
    || fail "primary cluster failed to stop cleanly"
cp -a "$WORK/backup" "$PGDATA_RESTORE"
rm -f "$PGDATA_RESTORE/postmaster.pid"
chmod 700 "$PGDATA_RESTORE"
cat >> "$PGDATA_RESTORE/postgresql.conf" <<EOF

# --- dr_ga10_pitr restore overrides ---
listen_addresses = 'localhost'
port = $RESTORE_PORT
unix_socket_directories = '$SOCK_DIR'
archive_mode = off
restore_command = 'cp $ARCHIVE_DIR/%f "%p"'
recovery_target_time = '$RECOVERY_TARGET'
recovery_target_action = 'promote'
EOF
touch "$PGDATA_RESTORE/recovery.signal"
if ! "$PG_BIN/pg_ctl" -D "$PGDATA_RESTORE" -l "$WORK/restore.log" -w -t 180 start >/dev/null; then
    tail -n 20 "$WORK/restore.log" >&2 || true
    fail "restore cluster failed to start (see $WORK/restore.log)"
fi
out_of_recovery=0
for i in $(seq 1 300); do
    if [ "$(PSQL "$RESTORE_PORT" 'SELECT pg_is_in_recovery()' 2>/dev/null || true)" = "f" ]; then
        out_of_recovery=1; break
    fi
    sleep 0.5
done
if [ "$out_of_recovery" != 1 ]; then fail "restore cluster still in recovery after 150s (see $WORK/restore.log)"; fi
first_query=$(PSQL "$RESTORE_PORT" "SELECT count(*) FROM tasks")
RTO_END=$(date +%s.%3N)
RTO_S=$(awk "BEGIN { printf \"%.1f\", $RTO_END - $RTO_START }")
log "  restore cluster promoted on :$RESTORE_PORT, answering queries (tasks=$first_query)"
log "  RTO = ${RTO_S}s (budget ${RTO_BUDGET_S}s)"

# =================================================================== step 8 ==
log "step 8: RTO gate — ${RTO_S}s <= budget ${RTO_BUDGET_S}s"
if ! awk "BEGIN { exit !($RTO_S <= $RTO_BUDGET_S) }"; then
    echo "GA10_PITR: FAIL rto ${RTO_S}s exceeds budget ${RTO_BUDGET_S}s"
    exit 1
fi

# =================================================================== step 9 ==
log "step 9: reconciliation gates — restored state must equal snapshot #2"
snapshot "$RESTORE_PORT" "$WORK/snap3.txt"
printf '%-24s | %-18s | %-18s | %s\n' "object" "snapshot2" "restored" "match"
pass=0
total=6
for obj in tasks task_attempts outbox_events budget_usage_ledger audit_event_projection dlq_proxy; do
    c2=$(snap_val "$WORK/snap2.txt" "$obj" 2); m2=$(snap_val "$WORK/snap2.txt" "$obj" 3)
    c3=$(snap_val "$WORK/snap3.txt" "$obj" 2); m3=$(snap_val "$WORK/snap3.txt" "$obj" 3)
    if [ "$c2" = "$c3" ] && [ "$m2" = "$m3" ] && [ -n "$c2" ]; then
        verdict="yes"; pass=$((pass + 1))
    else
        verdict="NO"
    fi
    printf '%-24s | %s @ %s | %s @ %s | %s\n' "$obj" "$c2" "${m2:0:8}" "$c3" "${m3:0:8}" "$verdict"
done
log "  batch-2 rows (created after the base backup) exist again only through WAL replay;"
log "  the step-6 DELETE is absent because recovery stopped at $RECOVERY_TARGET"
log "  RPO bound: archive_timeout=${ARCHIVE_TIMEOUT_S}s here (contract <= 300s in production)"

# =================================================================== step 10 ==
log "step 10: business recovery — API serves old + new tasks on the restored cluster"
BUSINESS_RTO_START=$(date +%s.%3N)
(
    cd "$REPO_ROOT"
    export JANUS_QUEUE_DRIVER=pg
    export JANUS_PG_HOST=127.0.0.1
    export JANUS_PG_PORT="$RESTORE_PORT"
    export JANUS_PG_USER="$PG_ROLE"
    export JANUS_PG_PASSWORD="$PG_ROLE"
    export JANUS_PG_DATABASE="$PG_DB"
    export JANUS_PG_SSLMODE=disable
    export JANUS_MIGRATION_AUTO=false
    export JANUS_MIGRATION_PATH=migrations
    export JANUS_AUTH_ENABLED=false
    export JANUS_HTTP_HOST=127.0.0.1
    export JANUS_HTTP_PORT="$((HTTP_PORT + 1))"
    export JANUS_GRPC_PORT="19221"
    export JANUS_REDIS_ADDR=localhost:59999
    exec "$WORK/janus-api"
) >"$WORK/api-restored.log" 2>&1 &
echo $! > "$API_PID_FILE"
restored_ready=0
for _ in $(seq 1 60); do
    if curl -fsS --max-time 2 "http://127.0.0.1:$((HTTP_PORT + 1))/readyz" >/dev/null 2>&1; then
        restored_ready=1; break
    fi
    sleep 1
done
if [ "$restored_ready" != 1 ]; then
    echo "--- restored API boot diagnostics ---" >&2
    tail -30 "$WORK/api-restored.log" >&2 || true
    fail "restored API did not become ready"
fi
BUSINESS_RTO_END=$(date +%s.%3N)
BUSINESS_RTO_S=$(awk "BEGIN { printf \"%.1f\", $BUSINESS_RTO_END - $BUSINESS_RTO_START }")
log "  restored API ready on :$((HTTP_PORT + 1)) (business RTO +${BUSINESS_RTO_S}s)"

RESTORED_URL="http://127.0.0.1:$((HTTP_PORT + 1))"
api_restored() {
    local method="$1" path="$2" body="$3"
    if [ -n "$body" ]; then
        curl -s --max-time 15 -X "$method" -H 'Content-Type: application/json' -d "$body" "$RESTORED_URL$path" 2>/dev/null || echo ""
    else
        curl -s --max-time 15 -X "$method" "$RESTORED_URL$path" 2>/dev/null || echo ""
    fi
}

old_queued=$(PSQL "$RESTORE_PORT" "SELECT id FROM tasks WHERE status='queued' LIMIT 1")
if [ -n "$old_queued" ]; then
    pull_body=$(api_restored POST "/v1/tenants/$TENANT/mailboxes/$MB/pull" "{\"agent_id\":\"$AGENT\"}")
    old_lease=$(printf '%s' "$pull_body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('lease',{}).get('lease_id',''))" 2>/dev/null || echo "")
    old_tid=$(printf '%s' "$pull_body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('task',{}).get('id',''))" 2>/dev/null || echo "")
    if [ -n "$old_lease" ] && [ -n "$old_tid" ]; then
        api_restored POST "/v1/tenants/$TENANT/tasks/$old_tid/start" "{\"lease_id\":\"$old_lease\",\"agent_id\":\"$AGENT\"}" >/dev/null
        ack_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST \
            -H 'Content-Type: application/json' \
            -d "{\"lease_id\":\"$old_lease\",\"agent_id\":\"$AGENT\",\"result_ref\":\"ga10://restored/$old_tid\"}" \
            "$RESTORED_URL/v1/tenants/$TENANT/tasks/$old_tid/ack" 2>/dev/null || echo 000)
        old_status=$(PSQL "$RESTORE_PORT" "SELECT status FROM tasks WHERE tenant_id='$TENANT' AND id='$old_tid'")
        log "  pre-disaster task $old_tid: pull+start+ack -> $ack_code, status=$old_status"
        if [ "$old_status" != "completed" ]; then
            echo "GA10_PITR: FAIL pre-disaster task did not settle after restore (status=$old_status)"
            exit 1
        fi
    else
        log "  note: no queued pre-disaster task pullable (all terminal)"
    fi
else
    log "  note: no queued pre-disaster tasks (all terminal)"
fi

new_tid="post-restore-$(date +%s%N)"
new_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"$new_tid\",\"source_agent\":\"$SRC_AGENT\",\"target_type\":\"mailbox\",\"target_value\":\"$MB\",\"envelope\":{\"janus_version\":\"1\",\"task_id\":\"$new_tid\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"$SRC_AGENT\",\"target\":{\"type\":\"mailbox\",\"value\":\"$MB\"},\"payload\":{\"type\":\"dr-verify\",\"content\":\"post-restore\"},\"trace\":{\"trace_id\":\"dr-$new_tid\"}}}" \
    "$RESTORED_URL/v1/tenants/$TENANT/tasks" 2>/dev/null || echo 000)
if [ "$new_code" != "201" ] && [ "$new_code" != "200" ]; then
    echo "GA10_PITR: FAIL post-restore task create -> $new_code"
    exit 1
fi
sleep 2
pull_body=$(api_restored POST "/v1/tenants/$TENANT/mailboxes/$MB/pull" "{\"agent_id\":\"$AGENT\"}")
new_lease=$(printf '%s' "$pull_body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('lease',{}).get('lease_id',''))" 2>/dev/null || echo "")
new_pulled=$(printf '%s' "$pull_body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('task',{}).get('id',''))" 2>/dev/null || echo "")
if [ "$new_pulled" != "$new_tid" ]; then
    for _ in $(seq 1 15); do
        pull_body=$(api_restored POST "/v1/tenants/$TENANT/mailboxes/$MB/pull" "{\"agent_id\":\"$AGENT\"}")
        new_pulled=$(printf '%s' "$pull_body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('task',{}).get('id',''))" 2>/dev/null || echo "")
        new_lease=$(printf '%s' "$pull_body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('lease',{}).get('lease_id',''))" 2>/dev/null || echo "")
        [ "$new_pulled" = "$new_tid" ] && break
        sleep 1
    done
fi
if [ "$new_pulled" = "$new_tid" ] && [ -n "$new_lease" ]; then
    api_restored POST "/v1/tenants/$TENANT/tasks/$new_tid/start" "{\"lease_id\":\"$new_lease\",\"agent_id\":\"$AGENT\"}" >/dev/null
    ack_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST \
        -H 'Content-Type: application/json' \
        -d "{\"lease_id\":\"$new_lease\",\"agent_id\":\"$AGENT\",\"result_ref\":\"ga10://restored/new\"}" \
        "$RESTORED_URL/v1/tenants/$TENANT/tasks/$new_tid/ack" 2>/dev/null || echo 000)
    new_status=$(PSQL "$RESTORE_PORT" "SELECT status FROM tasks WHERE tenant_id='$TENANT' AND id='$new_tid'")
    log "  post-restore task $new_tid: create+pull+start+ack -> $ack_code, status=$new_status"
    if [ "$new_status" != "completed" ]; then
        echo "GA10_PITR: FAIL post-restore task did not settle (status=$new_status)"
        exit 1
    fi
else
    echo "GA10_PITR: FAIL post-restore task never delivered via pull (got: $new_pulled)"
    exit 1
fi
log "  business recovery proven: old + new tasks pull and settle on the restored cluster"

# =================================================================== step 11 ==
if [ "$pass" -eq "$total" ]; then
    echo "GA10_PITR: PASS rto_seconds=$RTO_S objects=$pass/$total business_rto_additional_s=$BUSINESS_RTO_S"
    exit 0
fi
echo "GA10_PITR: FAIL objects=$pass/$total reconciled (rto_seconds=$RTO_S business_rto_additional_s=$BUSINESS_RTO_S)"
exit 1

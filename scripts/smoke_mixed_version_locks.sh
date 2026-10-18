#!/bin/bash
set -euo pipefail

# Mixed-version lock contention: v1.9.0 (no advisory locks) and current HEAD
# (advisory-locked mailbox writes) run as concurrent replicas against one
# PG/NATS. This is the rolling-upgrade shape of the next release: the locked
# writer must tolerate a lockless peer mutating the same rows.
#
# Stages:
#   1. build OLD (v1.9.0) + NEW (HEAD) binaries
#   2. fresh DB; NEW runs migrations alone, then both replicas start
#   3. two-replica convergence smoke (raced PATCH updates: OLD lockless vs
#      NEW locked) — reuses smoke_two_replica_convergence.sh unchanged
#   4. reverse task flow: publish via NEW, pull+ack via OLD
#   5. teardown
#
# Requires: local PG (5432), NATS (4222 + monitor 8222), Redis (6379),
# git tags for OLD_REF.

OLD_REF="${JANUS_OLD_REF:-v1.9.0}"
WORKDIR="${MIX_WORKDIR:-/tmp/opencode/janus-mix}"
DB_NAME="janus_mixtest_$(date +%s)"
PG_HOST="${JANUS_PG_HOST:-localhost}"
PG_USER="${JANUS_PG_USER:-janus}"
PG_PASSWORD="${JANUS_PG_PASSWORD:-janus}"
NATS_URL="${JANUS_NATS_URL:-nats://localhost:4222}"
REDIS_ADDR="${JANUS_REDIS_ADDR:-127.0.0.1:6379}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

export PGPASSWORD="$PG_PASSWORD"

cleanup() {
  if [ "${MIX_KEEP_LOGS:-}" = 1 ] && [ -d "$WORKDIR" ]; then
    mkdir -p /tmp/opencode/mix-fail-logs
    cp -f "$WORKDIR"/*.log /tmp/opencode/mix-fail-logs/ 2>/dev/null || true
  fi
  for f in "$WORKDIR"/old.pid "$WORKDIR"/new.pid; do
    [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true
  done
  sleep 1
  psql -h "$PG_HOST" -U "$PG_USER" -d postgres -c "DROP DATABASE IF EXISTS $DB_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== Mixed-version lock contention: OLD=$OLD_REF (lockless) vs HEAD (advisory locks) ==="

# --- Stage 1: build both binaries -------------------------------------------
rm -rf "$WORKDIR"; mkdir -p "$WORKDIR"
git -C "$REPO_ROOT" worktree prune
# Shallow CI clones may not carry release tags: fetch them when the ref
# is missing instead of dying with a bare exit 128.
if ! git -C "$REPO_ROOT" cat-file -e "$OLD_REF^{commit}" 2>/dev/null; then
  git -C "$REPO_ROOT" fetch --tags --quiet || { echo "FATAL: cannot fetch $OLD_REF"; exit 1; }
fi
git -C "$REPO_ROOT" worktree add "$WORKDIR/old-src" "$OLD_REF" --detach   || { echo "FATAL: worktree add $OLD_REF failed"; exit 1; }
( cd "$WORKDIR/old-src" && GOPROXY="${GOPROXY:-https://goproxy.cn,direct}" go build -o "$WORKDIR/janus-old" ./server/cmd/janus-api ) \
  || { echo "FATAL: build OLD ($OLD_REF) failed"; exit 1; }
( cd "$REPO_ROOT" && GOPROXY="${GOPROXY:-https://goproxy.cn,direct}" go build -o "$WORKDIR/janus-new" ./server/cmd/janus-api ) \
  || { echo "FATAL: build NEW (HEAD) failed"; exit 1; }
echo "  ✓ binaries built"

# --- Stage 2: fresh DB + fresh broker, migrations via NEW, both replicas up --
# JetStream is file-persistent: JANUS_* streams from earlier runs survive
# NATS restarts and deliver ghost tasks (rows in dropped databases) that
# fail pull with task-not-found. The mixed run must start from an empty
# broker, like CI's throwaway containers.
JANUS_NATS_URL="$NATS_URL" python3 - <<'PYEOF' || { echo "FATAL: NATS stream purge failed"; exit 1; }
import asyncio
import os

async def purge():
    import nats
    nc = await nats.connect(os.environ["JANUS_NATS_URL"])
    js = nc.jetstream()
    for s in await js.streams_info():
        if s.config.name.startswith("JANUS_"):
            await js.delete_stream(s.config.name)
    await nc.close()

asyncio.run(purge())
PYEOF
psql -h "$PG_HOST" -U "$PG_USER" -d postgres -c "CREATE DATABASE $DB_NAME" >/dev/null

COMMON_ENV="JANUS_QUEUE_DRIVER=nats JANUS_PG_HOST=$PG_HOST JANUS_PG_USER=$PG_USER JANUS_PG_PASSWORD=$PG_PASSWORD JANUS_PG_DATABASE=$DB_NAME JANUS_PG_SSLMODE=disable JANUS_NATS_URL=$NATS_URL JANUS_REDIS_ADDR=$REDIS_ADDR JANUS_AUTH_ENABLED=false"

# NEW migrates alone (rolling-upgrade order: migrate before mixed replicas).
env $COMMON_ENV JANUS_MIGRATION_AUTO=true JANUS_MIGRATION_PATH="$REPO_ROOT/migrations" \
  JANUS_HTTP_HOST=localhost JANUS_HTTP_PORT=8099 "$WORKDIR/janus-new" > "$WORKDIR/migrate.log" 2>&1 &
MIG_PID=$!
for i in $(seq 1 60); do curl -sf http://localhost:8099/readyz >/dev/null 2>&1 && break; sleep 1; done
curl -sf http://localhost:8099/readyz >/dev/null || { echo "FATAL: migration boot failed"; cat "$WORKDIR/migrate.log"; exit 1; }
kill "$MIG_PID" 2>/dev/null || true; sleep 1
echo "  ✓ migrations applied (schema at HEAD)"

start_replica() { # bin port pidfile logfile extra-env...
  local bin="$1" port="$2" pidfile="$3" logfile="$4"; shift 4
  env $COMMON_ENV "$@" JANUS_HTTP_HOST=localhost JANUS_HTTP_PORT="$port" \
    "$bin" > "$logfile" 2>&1 &
  echo $! > "$pidfile"
}
start_replica "$WORKDIR/janus-old" 8080 "$WORKDIR/old.pid" "$WORKDIR/old.log" JANUS_MIGRATION_AUTO=false
for i in $(seq 1 60); do curl -sf http://localhost:8080/readyz >/dev/null 2>&1 && break; sleep 1; done
curl -sf http://localhost:8080/readyz >/dev/null || { echo "FATAL: OLD replica not ready"; tail -20 "$WORKDIR/old.log"; exit 1; }
start_replica "$WORKDIR/janus-new" 8081 "$WORKDIR/new.pid" "$WORKDIR/new.log" JANUS_MIGRATION_AUTO=false JANUS_GRPC_PORT=9091
for i in $(seq 1 60); do curl -sf http://localhost:8081/readyz >/dev/null 2>&1 && break; sleep 1; done
curl -sf http://localhost:8081/readyz >/dev/null || { echo "FATAL: NEW replica not ready"; tail -20 "$WORKDIR/new.log"; exit 1; }
echo "  ✓ OLD :8080 (lockless) + NEW :8081 (advisory locks) both ready"

# --- Stage 3: raced config updates across versions ---------------------------
JANUS_URL_A=http://localhost:8080 JANUS_URL_B=http://localhost:8081 \
JANUS_PG_HOST="$PG_HOST" JANUS_PG_USER="$PG_USER" JANUS_PG_DBNAME="$DB_NAME" \
CONVERGE_TRIES=90 \
  bash "$SCRIPT_DIR/smoke_two_replica_convergence.sh"
echo "  ✓ convergence smoke PASS under mixed versions"

# --- Stage 4: reverse task flow (publish NEW -> consume OLD) -----------------
# Fixtures are created through BOTH replicas (an LB spreading creates across
# a mixed fleet): v1.9.0 keeps a per-process tenant map with no runtime sync,
# so a tenant created only via NEW is invisible to OLD until restart.
TENANT="mix-ver"; MB="mix-mb"; TASK_ID="mix-task-$(date +%s%N)"
post() { curl -s -o /dev/null -w "%{http_code}" --max-time 5 -X POST "$@"; }
tolerate_dup() { [ "$1" = 201 ] || [ "$1" = 400 ]; }
for U in http://localhost:8080 http://localhost:8081; do
  c=$(post "$U/v1/tenants" -H 'Content-Type: application/json' -d "{\"id\":\"$TENANT\",\"name\":\"Mix\"}")
  tolerate_dup "$c" || { echo "FATAL: tenant create via $U -> $c"; exit 1; }
  c=$(post "$U/v1/tenants/$TENANT/agents" -H 'Content-Type: application/json' -d '{"id":"mix-agent","display_name":"M","protocol":"a2a"}')
  tolerate_dup "$c" || { echo "FATAL: agent create via $U -> $c"; exit 1; }
  c=$(post "$U/v1/tenants/$TENANT/mailboxes" -H 'Content-Type: application/json' -d "{\"id\":\"$MB\",\"agent_id\":\"mix-agent\"}")
  tolerate_dup "$c" || { echo "FATAL: mailbox create via $U -> $c"; exit 1; }
done

c=$(post "http://localhost:8081/v1/tenants/$TENANT/tasks" -H 'Content-Type: application/json' \
  -d "{\"id\":\"$TASK_ID\",\"source_agent\":\"mix-agent\",\"target_type\":\"mailbox\",\"target_value\":\"$MB\",\"envelope\":{\"janus_version\":\"0.3\",\"task_id\":\"$TASK_ID\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"mix-agent\",\"target\":{\"type\":\"mailbox\",\"value\":\"$MB\"},\"payload\":{\"type\":\"smoke\",\"content\":\"mix\"},\"trace\":{\"trace_id\":\"mix-$TASK_ID\"}}}")
[ "$c" = 201 ] || { echo "FATAL: task publish via NEW -> $c"; exit 1; }

PULLED=""; LEASE=""
for _ in $(seq 1 90); do
  PULL=$(curl -sf --max-time 5 -X POST "http://localhost:8080/v1/tenants/$TENANT/mailboxes/$MB/pull" \
    -H 'Content-Type: application/json' -d '{"agent_id":"mix-agent"}' 2>/dev/null || echo "")
  GOT=$(echo "$PULL" | python3 -c "import sys,json; d=json.load(sys.stdin); t=d.get('task') or {}; print(t.get('id',''))" 2>/dev/null || echo "")
  [ "$GOT" = "$TASK_ID" ] && { PULLED="$GOT"; LEASE=$(echo "$PULL" | python3 -c "import sys,json; print(json.load(sys.stdin).get('lease',{}).get('lease_id',''))"); break; }
  sleep 1
done
[ -n "$PULLED" ] || {
  echo "FATAL: OLD replica never delivered NEW-published task after 90s"
  echo "--- diagnostics ---"
  echo "PG task row:"; psql -h "$PG_HOST" -U "$PG_USER" -d "$DB_NAME" -t -A -c "SELECT id,status,mailbox_id FROM tasks WHERE tenant_id='$TENANT'" || true
  echo "OLD (v1.9.0) log tail:"; tail -10 "$WORKDIR/old.log" || true
  echo "NEW log tail:"; tail -10 "$WORKDIR/new.log" || true
  echo "broker consumers:"; curl -sf "http://localhost:8222/jsz?consumers=true" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(' ', c.get('name'), 'ack_wait=', c.get('config',{}).get('ack_wait'), 'delivered=', c.get('delivered',{}).get('consumer_seq')) for acc in d.get('account_details',[]) for s2 in acc.get('stream_detail',[]) for c in s2.get('state',{}).get('consumers',[])]" 2>/dev/null || echo "  (introspection failed)"
  exit 1
}
echo "  ✓ task published via NEW, pulled via OLD"

c=$(curl -s -o /tmp/opencode/ack-body.txt -w "%{http_code}" --max-time 5 -X POST "http://localhost:8080/v1/tenants/$TENANT/tasks/$TASK_ID/ack" -H 'Content-Type: application/json' \
  -d "{\"lease_id\":\"$LEASE\",\"result_ref\":\"smoke://mix-ok\"}")
[ "$c" = 200 ] || { echo "FATAL: ack via OLD -> $c body=$(cat /tmp/opencode/ack-body.txt 2>/dev/null)"; exit 1; }
echo "  ✓ acked via OLD"

# --- Stage 5: post-race integrity ---------------------------------------------
STATUS=$(psql -h "$PG_HOST" -U "$PG_USER" -d "$DB_NAME" -t -A -c \
  "SELECT status FROM tasks WHERE tenant_id='$TENANT' AND id='$TASK_ID'" 2>/dev/null)
[ "$STATUS" = "completed" ] || { echo "FATAL: task status after cross-version ack = '$STATUS' (want completed)"; exit 1; }
echo "  ✓ durable status = completed"

curl -sf http://localhost:8080/readyz >/dev/null || { echo "FATAL: OLD replica degraded after race"; exit 1; }
curl -sf http://localhost:8081/readyz >/dev/null || { echo "FATAL: NEW replica degraded after race"; exit 1; }
echo "  ✓ both replicas healthy post-race"

echo ""
echo "=== Mixed-version lock contention PASS: $OLD_REF + HEAD coexist safely ==="

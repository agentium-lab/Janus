#!/bin/bash
set -euo pipefail

JANUS_URL="${JANUS_URL:-http://localhost:8080}"
PG_HOST="${JANUS_PG_HOST:-/tmp}"
PG_PORT="${JANUS_PG_PORT:-5432}"
PG_USER="${JANUS_PG_USER:-silv}"
PG_DB="${JANUS_PG_DBNAME:-janus_test}"
NATS_URL="${JANUS_NATS_URL:-nats://localhost:4222}"
REDIS_ADDR="${JANUS_REDIS_ADDR:-127.0.0.1:6379}"
PGDATA="${JANUS_PGDATA:-/tmp/janus/pgdata}"
PASS=0; FAIL=0

check() {
  local name="$1"; local condition="$2"
  if eval "$condition"; then
    echo "  ✓ $name"; PASS=$((PASS+1))
  else
    echo "  ✗ $name"; FAIL=$((FAIL+1))
  fi
}

echo "=== M4 Ops Chaos: dependency restart + readiness + recovery ==="
echo ""

echo "--- Phase 1: Baseline readiness ---"
READY=$(curl -sf "$JANUS_URL/readyz" 2>/dev/null || echo "")
if [ -z "$READY" ]; then
  echo "SKIP: Janus API not reachable at $JANUS_URL"
  echo "This test requires a running Janus API instance."
  exit 0
fi
PASS=$((PASS+1))

echo "--- Phase 2: Redis restart + heartbeat restore ---"
echo "  Restarting Redis..."
redis-cli -h "${REDIS_ADDR%%:*}" -p "${REDIS_ADDR##*:}" SHUTDOWN NOSAVE 2>/dev/null || true
sleep 2
redis-server --daemonize yes --port "${REDIS_ADDR##*:}" 2>/dev/null
sleep 2
REDIS_OK=$(redis-cli -h "${REDIS_ADDR%%:*}" -p "${REDIS_ADDR##*:}" PING 2>/dev/null || echo "")
check "Redis restarted and responding" "[ '$REDIS_OK' = 'PONG' ]"

HB=$(curl -sf -X POST "$JANUS_URL/v1/tenants/ops-chaos/agents/agent-1/heartbeat" 2>/dev/null && echo "ok" || echo "fail")
check "Heartbeat after Redis restore" "[ '$HB' = 'ok' ]"

echo "--- Phase 3: NATS outage → outbox retry ---"
echo "  Stopping NATS..."
pkill -f "nats-server" 2>/dev/null || true
sleep 3

PUB_DURING_OUTAGE=$(curl -sf -X POST "$JANUS_URL/v1/tenants/ops-chaos/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"id":"task-nats-outage","source_agent":"agent-1","target_type":"agent","target_value":"agent-1","envelope":{"janus_version":"0.3","task_id":"task-nats-outage","tenant_id":"ops-chaos","source_agent":"agent-1","target":{"type":"agent","value":"agent-1"},"payload":{"type":"test","content":"outage"},"trace":{"trace_id":"chaos-nats"}}}' \
  2>/dev/null && echo "accepted" || echo "failed")
check "Task accepted during NATS outage (outbox holds)" "[ '$PUB_DURING_OUTAGE' = 'accepted' ]"

echo "  Restarting NATS..."
nohup nats-server -js -p 4222 -m 8222 > /tmp/janus/nats-restart.log 2>&1 &
sleep 3
NATS_OK=$(curl -sf http://localhost:8222/jsz 2>/dev/null | head -c 10 || echo "")
check "NATS restarted" "[ -n '$NATS_OK' ]"

echo "--- Phase 4: PostgreSQL restart + state persistence ---"
echo "  Restarting PostgreSQL..."
pg_ctl -D "$PGDATA" -o "-k $PG_HOST -h localhost" restart 2>/dev/null
sleep 3
PG_OK=$(psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -t -c "SELECT 1" 2>/dev/null | tr -d ' ' || echo "")
check "PostgreSQL restarted and responding" "[ '$PG_OK' = '1' ]"

TASK_AFTER_RESTART=$(curl -sf "$JANUS_URL/v1/tenants/ops-chaos/tasks/task-nats-outage" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
check "Task persisted across PG restart" "[ '$TASK_AFTER_RESTART' = 'task-nats-outage' ]"

echo "--- Phase 5: Readiness degradation recovery ---"
READY_STATUS=$(curl -sf "$JANUS_URL/readyz" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "unknown")
check "Readiness recovered to ready/degraded" "[ '$READY_STATUS' = 'ready' ] || [ '$READY_STATUS' = 'degraded' ]"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit $FAIL

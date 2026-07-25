#!/bin/bash
set -euo pipefail

JANUS_URL="${JANUS_URL:-http://localhost:8080}"
PG_HOST="${JANUS_PG_HOST:-/tmp}"
PG_USER="${JANUS_PG_USER:-silv}"
PG_DB="${JANUS_PG_DBNAME:-janus_test}"
NATS_URL="${JANUS_NATS_URL:-nats://localhost:4222}"
REDIS_ADDR="${JANUS_REDIS_ADDR:-127.0.0.1:6379}"
PASS=0; FAIL=0

check() {
  local name="$1"; local condition="$2"
  if eval "$condition"; then
    echo "  ✓ $name"; PASS=$((PASS+1))
  else
    echo "  ✗ $name"; FAIL=$((FAIL+1))
  fi
}

echo "=== M4 Smoke Prod: API + dependencies + metrics + observability ==="

if ! curl -sf "$JANUS_URL/healthz" >/dev/null 2>&1; then
  echo "SKIP: Janus API not reachable at $JANUS_URL"
  echo "Start Janus API with PG/NATS/Redis, then re-run."
  exit 0
fi

echo "--- Phase 1: Dependency readiness ---"
PG_OK=$(psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -t -c "SELECT 1" 2>/dev/null | tr -d ' ' || echo "")
check "PostgreSQL reachable" "[ '$PG_OK' = '1' ]"

NATS_OK=$(curl -sf http://localhost:8222/jsz 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print('ok' if d.get('accounts',1) >= 1 else '')" 2>/dev/null || echo "")
check "NATS JetStream reachable" "[ -n '$NATS_OK' ]"

REDIS_OK=$(redis-cli -h "${REDIS_ADDR%%:*}" -p "${REDIS_ADDR##*:}" PING 2>/dev/null || echo "")
check "Redis reachable" "[ '$REDIS_OK' = 'PONG' ]"

echo "--- Phase 2: Task lifecycle ---"
TENANT="smoke-prod"
curl -sf -X POST "$JANUS_URL/v1/tenants" -H 'Content-Type: application/json' \
  -d "{\"id\":\"$TENANT\",\"name\":\"Smoke Prod\"}" >/dev/null 2>&1 || true

curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/agents" -H 'Content-Type: application/json' \
  -d '{"id":"agent-smoke","display_name":"Smoke Agent","protocol":"a2a"}' >/dev/null 2>&1 || true

curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/mailboxes" -H 'Content-Type: application/json' \
  -d '{"id":"mb-smoke","agent_id":"agent-smoke"}' >/dev/null 2>&1 || true

TASK_RESP=$(curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/tasks" -H 'Content-Type: application/json' \
  -d '{"id":"task-smoke-prod","source_agent":"agent-smoke","target_type":"mailbox","target_value":"mb-smoke","envelope":{"janus_version":"0.3","task_id":"task-smoke-prod","tenant_id":"smoke-prod","source_agent":"agent-smoke","target":{"type":"mailbox","value":"mb-smoke"},"payload":{"type":"smoke","content":"prod test"},"trace":{"trace_id":"smoke-prod-1"}}}' \
  2>/dev/null || echo "{}")
TASK_ID=$(echo "$TASK_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
check "Task published" "[ -n '$TASK_ID' ]"

TASK_STATUS=$(curl -sf "$JANUS_URL/v1/tenants/$TENANT/tasks/$TASK_ID" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
check "Task retrievable" "[ -n '$TASK_STATUS' ]"

echo "--- Phase 3: Metrics endpoint ---"
METRICS=$(curl -sf "$JANUS_URL/metrics" 2>/dev/null || echo "")
check "Prometheus metrics exposed" "[ -n '$METRICS' ]"
METRIC_COUNT=$(echo "$METRICS" | grep -c "^janus_" 2>/dev/null || echo "0")
check "Has Janus metrics ($METRIC_COUNT metrics)" "[ $METRIC_COUNT -gt 5 ]"

echo "--- Phase 4: Audit trace query ---"
EVENTS=$(curl -sf "$JANUS_URL/v1/tenants/$TENANT/tasks/$TASK_ID/events" 2>/dev/null || echo "[]")
EVENT_COUNT=$(echo "$EVENTS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d) if isinstance(d, list) else len(d.get('events',[])))" 2>/dev/null || echo "0")
check "Audit events exist ($EVENT_COUNT events)" "[ $EVENT_COUNT -gt 0 ]"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit $FAIL

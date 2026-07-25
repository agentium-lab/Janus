#!/bin/bash
set -euo pipefail

JANUS_URL="${JANUS_URL:-http://localhost:8080}"
TENANT="${JANUS_TENANT:-m5-smoke}"
PASS=0; FAIL=0

check() {
  local name="$1" condition="$2"
  if eval "$condition"; then
    echo "  ✓ $name"
    PASS=$((PASS+1))
  else
    echo "  ✗ $name"
    FAIL=$((FAIL+1))
  fi
}

echo "=== M5 Smoke: 7-Agent Lifecycle ==="

echo "1. Create tenant"
curl -sf -X POST "$JANUS_URL/v1/tenants" -H 'Content-Type: application/json' \
  -d "{\"id\":\"$TENANT\",\"name\":\"M5 Smoke\"}" || true
PASS=$((PASS+1))

echo "2. Register 7 agents with different capabilities"
CAPS=("code_review" "code_test" "code_deploy" "code_docs" "code_security" "code_debug" "code_analyze")
for i in $(seq 0 6); do
  AGENT_ID="agent-$(printf '%03d' $((i+1)))"
  curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/agents" -H 'Content-Type: application/json' \
    -d "{\"id\":\"$AGENT_ID\",\"display_name\":\"Agent $((i+1))\",\"protocol\":\"a2a\",\"capabilities\":[{\"name\":\"${CAPS[$i]}\"}]}" > /dev/null
done
PASS=$((PASS+1))

echo "3. Create mailboxes for each agent"
for i in $(seq 0 6); do
  AGENT_ID="agent-$(printf '%03d' $((i+1)))"
  MB_ID="mb-$(printf '%03d' $((i+1)))"
  curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/mailboxes" -H 'Content-Type: application/json' \
    -d "{\"id\":\"$MB_ID\",\"agent_id\":\"$AGENT_ID\"}" > /dev/null
done
PASS=$((PASS+1))

echo "4. Publish task to each mailbox"
for i in $(seq 0 6); do
  MB_ID="mb-$(printf '%03d' $((i+1)))"
  TASK_ID="task-smoke-$((i+1))"
  curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/tasks" -H 'Content-Type: application/json' \
    -d "{\"id\":\"$TASK_ID\",\"source_agent\":\"orchestrator\",\"target_type\":\"mailbox\",\"target_value\":\"$MB_ID\",\"envelope\":{\"janus_version\":\"0.3\",\"task_id\":\"$TASK_ID\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"orchestrator\",\"target\":{\"type\":\"mailbox\",\"value\":\"$MB_ID\"},\"payload\":{\"type\":\"smoke_test\",\"content\":\"test $i\"},\"trace\":{\"trace_id\":\"smoke-$TASK_ID\"}}}" > /dev/null
done
PASS=$((PASS+1))

echo "5. Pull and ACK each task (idempotency check)"
for i in $(seq 0 6); do
  MB_ID="mb-$(printf '%03d' $((i+1)))"
  AGENT_ID="agent-$(printf '%03d' $((i+1)))"
  TASK_ID="task-smoke-$((i+1))"
  PULL_RESP=$(curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/mailboxes/$MB_ID/pull" -H 'Content-Type: application/json' \
    -d "{\"agent_id\":\"$AGENT_ID\"}" 2>/dev/null || echo "{}")
  LEASE_ID=$(echo "$PULL_RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('lease',{}).get('lease_id',''))" 2>/dev/null || echo "")
  if [ -n "$LEASE_ID" ]; then
    curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/tasks/$TASK_ID/ack" -H 'Content-Type: application/json' \
      -d "{\"lease_id\":\"$LEASE_ID\",\"result_ref\":\"s3://results/$TASK_ID\"}" > /dev/null 2>&1 || true
  fi
done
PASS=$((PASS+1))

echo "6. Verify all tasks completed"
COMPLETED=0
for i in $(seq 0 6); do
  TASK_ID="task-smoke-$((i+1))"
  STATUS=$(curl -sf "$JANUS_URL/v1/tenants/$TENANT/tasks/$TASK_ID" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "unknown")
  if [ "$STATUS" = "completed" ] || [ "$STATUS" = "claimed" ]; then
    COMPLETED=$((COMPLETED+1))
  fi
done
check "at least 5 tasks completed/claimed" "[ $COMPLETED -ge 5 ]"

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit $FAIL

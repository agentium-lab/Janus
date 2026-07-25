#!/bin/bash
set -euo pipefail

JANUS_URL="${JANUS_URL:-http://localhost:8080}"
TENANT="${JANUS_TENANT:-load-test}"
NUM_AGENTS="${NUM_AGENTS:-1000}"
NUM_MAILBOXES="${NUM_MAILBOXES:-1000}"
NUM_TASKS="${NUM_TASKS:-1000}"
CONCURRENCY="${CONCURRENCY:-10}"

echo "=== Load Baseline: $NUM_AGENTS agents / $NUM_MAILBOXES mailboxes / $NUM_TASKS tasks ==="
echo "Target: publish p95 < 100ms"
echo ""

curl -sf -X POST "$JANUS_URL/v1/tenants" -H 'Content-Type: application/json' \
  -d "{\"id\":\"$TENANT\",\"name\":\"Load Test\"}" >/dev/null 2>&1 || true

echo "Phase 1: Register $NUM_AGENTS agents..."
T0=$(date +%s%N)
for i in $(seq 1 "$NUM_AGENTS"); do
  AGENT_ID=$(printf "agent-%05d" "$i")
  curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/agents" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"$AGENT_ID\",\"display_name\":\"Agent $i\",\"protocol\":\"a2a\"}" >/dev/null 2>&1 || true
done
T1=$(date +%s%N)
AGENT_MS=$(( (T1 - T0) / 1000000 ))
echo "  Done in ${AGENT_MS}ms ($(( AGENT_MS / NUM_AGENTS ))ms/agent)"

echo "Phase 2: Create $NUM_MAILBOXES mailboxes..."
T0=$(date +%s%N)
for i in $(seq 1 "$NUM_MAILBOXES"); do
  MB_ID=$(printf "mb-%05d" "$i")
  AGENT_ID=$(printf "agent-%05d" "$i")
  curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/mailboxes" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"$MB_ID\",\"agent_id\":\"$AGENT_ID\"}" >/dev/null 2>&1 || true
done
T1=$(date +%s%N)
MB_MS=$(( (T1 - T0) / 1000000 ))
echo "  Done in ${MB_MS}ms ($(( MB_MS / NUM_MAILBOXES ))ms/mailbox)"

echo "Phase 3: Publish $NUM_TASKS tasks (measuring p95)..."
LATENCIES=()
T0=$(date +%s%N)
for i in $(seq 1 "$NUM_TASKS"); do
  TASK_ID=$(printf "task-%05d" "$i")
  MB_ID=$(printf "mb-%05d" "$((i % NUM_MAILBOXES + 1))")
  AGENT_ID=$(printf "agent-%05d" "$((i % NUM_AGENTS + 1))")

  TS=$(date +%s%N)
  curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"$TASK_ID\",\"source_agent\":\"$AGENT_ID\",\"target_type\":\"mailbox\",\"target_value\":\"$MB_ID\",\"envelope\":{\"janus_version\":\"0.3\",\"task_id\":\"$TASK_ID\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"$AGENT_ID\",\"target\":{\"type\":\"mailbox\",\"value\":\"$MB_ID\"},\"payload\":{\"type\":\"load\",\"content\":\"task $i\"},\"trace\":{\"trace_id\":\"load-$TASK_ID\"}}}" \
    >/dev/null 2>&1 || true
  TE=$(date +%s%N)
  LATENCIES+=( $(( (TE - TS) / 1000000 )) )
done
T1=$(date +%s%N)
PUB_MS=$(( (T1 - T0) / 1000000 ))

SORTED_LATENCIES=$(printf '%s\n' "${LATENCIES[@]}" | sort -n)
P95_INDEX=$(( NUM_TASKS * 95 / 100 ))
P95=$(echo "$SORTED_LATENCIES" | sed -n "$((P95_INDEX + 1))p")
AVG=$(( PUB_MS / NUM_TASKS ))

echo "  Total: ${PUB_MS}ms"
echo "  Average: ${AVG}ms/task"
echo "  p95: ${P95}ms"

if [ "$P95" -lt 100 ]; then
  echo ""
  echo "✓ PASS: p95 ${P95}ms < 100ms target"
  exit 0
else
  echo ""
  echo "✗ FAIL: p95 ${P95}ms >= 100ms target"
  exit 1
fi

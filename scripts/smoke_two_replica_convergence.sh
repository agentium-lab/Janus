#!/bin/bash
set -euo pipefail

# Two-replica convergence smoke: both APIs run against the same PG/NATS;
# concurrent config updates from both must converge on the broker.

JANUS_URL_A="${JANUS_URL_A:-http://localhost:8080}"
JANUS_URL_B="${JANUS_URL_B:-http://localhost:8081}"
PG_HOST="${JANUS_PG_HOST:-localhost}"
PG_USER="${JANUS_PG_USER:-janus}"
PG_DB="${JANUS_PG_DBNAME:-janus_test}"
TENANT="two-replica"
MB="conv-mb"
PASS=0; FAIL=0

check() {
  local name="$1"; local condition="$2"
  if eval "$condition"; then
    echo "  ✓ $name"; PASS=$((PASS+1))
  else
    echo "  ✗ $name"; FAIL=$((FAIL+1))
  fi
}

echo "=== Two-Replica Convergence: cross-replica config updates ==="

for U in "$JANUS_URL_A" "$JANUS_URL_B"; do
  curl -sf --max-time 5 "$U/healthz" >/dev/null || { echo "FATAL: replica not reachable at $U"; exit 1; }
done
curl -sf --max-time 5 "http://localhost:8222/jsz" >/dev/null || { echo "FATAL: NATS monitor not reachable on 8222"; exit 1; }

# Fixture is idempotent (201 or 409 are both fine); connection errors are fatal.
for ep in tenants tenants/$TENANT/agents tenants/$TENANT/mailboxes; do
  case "$ep" in
    tenants) BODY="{\"id\":\"$TENANT\",\"name\":\"Two Replica\"}" ;;
    */agents) BODY='{"id":"conv-agent","display_name":"C","protocol":"a2a"}' ;;
    *) BODY="{\"id\":\"$MB\",\"agent_id\":\"conv-agent\"}" ;;
  esac
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 -X POST "$JANUS_URL_A/v1/$ep" \
    -H 'Content-Type: application/json' -d "$BODY")
  case "$code" in 201|409) ;; *) echo "FATAL: fixture $ep -> HTTP $code"; exit 1 ;; esac
done

echo "  racing concurrent config updates across both replicas..."
for i in 1 2 3 4 5; do
  AW=$((60 + i)); BW=$((120 + i))
  curl -sf --max-time 5 -X PUT "$JANUS_URL_A/v1/tenants/$TENANT/mailboxes/$MB/config" \
    -H 'Content-Type: application/json' \
    -d "{\"max_concurrency\":4,\"ack_wait_seconds\":$AW,\"max_deliver\":5,\"retention_seconds\":3600}" >/dev/null &
  PA=$!
  curl -sf --max-time 5 -X PUT "$JANUS_URL_B/v1/tenants/$TENANT/mailboxes/$MB/config" \
    -H 'Content-Type: application/json' \
    -d "{\"max_concurrency\":4,\"ack_wait_seconds\":$BW,\"max_deliver\":5,\"retention_seconds\":3600}" >/dev/null &
  PB=$!
  wait $PA || { echo "FATAL: replica A config update failed"; exit 1; }
  wait $PB || { echo "FATAL: replica B config update failed"; exit 1; }
done
echo "  ✓ all 10 raced updates accepted"

# The durable PG version must be the highest committed (>= the 5 paired bumps).
FINAL_AW=$(psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -t -A -c \
  "SELECT ack_wait_seconds FROM mailboxes WHERE tenant_id='$TENANT' AND id='$MB'" 2>/dev/null)
[ -n "$FINAL_AW" ] || { echo "FATAL: PG config unreadable after the race"; exit 1; }
echo "  ✓ PG ack_wait_seconds = $FINAL_AW"

# Both APIs must still serve; the consumer must exist on the broker.
curl -sf --max-time 5 "$JANUS_URL_A/readyz" >/dev/null || { echo "FATAL: replica A not ready"; exit 1; }
curl -sf --max-time 5 "$JANUS_URL_B/readyz" >/dev/null || { echo "FATAL: replica B not ready"; exit 1; }
echo "  ✓ both replicas ready"

echo "4. Real task through A: publish -> pull on B -> ack..."
TASK_ID="conv-task-$(date +%s%N)"
curl -sf --max-time 5 -X POST "$JANUS_URL_A/v1/tenants/$TENANT/tasks" -H 'Content-Type: application/json' \
  -d "{\"id\":\"$TASK_ID\",\"source_agent\":\"conv-agent\",\"target_type\":\"mailbox\",\"target_value\":\"$MB\",\"envelope\":{\"janus_version\":\"0.3\",\"task_id\":\"$TASK_ID\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"conv-agent\",\"target\":{\"type\":\"mailbox\",\"value\":\"$MB\"},\"payload\":{\"type\":\"smoke\",\"content\":\"conv\"},\"trace\":{\"trace_id\":\"conv-$TASK_ID\"}}}" >/dev/null \
  || { echo "FATAL: task publish failed"; exit 1; }

PULLED=""; LEASE=""
for _ in $(seq 1 20); do
  PULL=$(curl -sf --max-time 5 -X POST "$JANUS_URL_B/v1/tenants/$TENANT/mailboxes/$MB/pull" \
    -H 'Content-Type: application/json' -d '{"agent_id":"conv-agent"}' 2>/dev/null || echo "")
  GOT=$(echo "$PULL" | python3 -c "import sys,json; d=json.load(sys.stdin); t=d.get('task') or {}; print(t.get('id',''))" 2>/dev/null || echo "")
  if [ "$GOT" = "$TASK_ID" ]; then
    PULLED="$GOT"
    LEASE=$(echo "$PULL" | python3 -c "import sys,json; print(json.load(sys.stdin).get('lease',{}).get('lease_id',''))")
    break
  fi
  sleep 1
done
[ -n "$PULLED" ] || { echo "FATAL: replica B never delivered the task"; exit 1; }
echo "  ✓ pulled on replica B"
curl -sf --max-time 5 -X POST "$JANUS_URL_B/v1/tenants/$TENANT/tasks/$TASK_ID/ack" \
  -H 'Content-Type: application/json' \
  -d "{\"lease_id\":\"$LEASE\",\"result_ref\":\"smoke://conv-ok\"}" >/dev/null \
  || { echo "FATAL: ack failed"; exit 1; }
echo "  ✓ acked"

# 5. Broker consumer config equals the durable PG value (strict).
sleep 5
BROKER_AW=$(curl -sf --max-time 5 "http://localhost:8222/jsz?config=true&consumers=true" 2>/dev/null \
  | TENANT_NAME="$TENANT" MB_NAME="$MB" python3 scripts/jsz_consumer_ackwait.py 2>/dev/null || echo "")
if [ -z "$BROKER_AW" ]; then
  echo "FATAL: broker consumer introspection unavailable (jsz config) — cannot verify convergence"
  exit 1
elif [ "$BROKER_AW" != "$FINAL_AW" ]; then
  echo "FATAL: broker ack_wait=${BROKER_AW}s != PG ack_wait=${FINAL_AW}s (not converged)"
  exit 1
else
  echo "  ✓ broker == PG (ack_wait=${FINAL_AW}s)"
fi

echo ""
echo "=== Results: two-replica convergence PASS ==="

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit $FAIL

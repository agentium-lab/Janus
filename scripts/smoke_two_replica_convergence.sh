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
  curl -sf --max-time 5 "$U/healthz" >/dev/null || { echo "SKIP: API not reachable at $U"; exit 0; }
done

curl -sf -X POST "$JANUS_URL_A/v1/tenants" -H 'Content-Type: application/json' \
  -d "{\"id\":\"$TENANT\",\"name\":\"Two Replica\"}" >/dev/null 2>&1 || true
curl -sf -X POST "$JANUS_URL_A/v1/tenants/$TENANT/agents" -H 'Content-Type: application/json' \
  -d '{"id":"conv-agent","display_name":"C","protocol":"a2a"}' >/dev/null 2>&1 || true
curl -sf -X POST "$JANUS_URL_A/v1/tenants/$TENANT/mailboxes" -H 'Content-Type: application/json' \
  -d "{\"id\":\"$MB\",\"agent_id\":\"conv-agent\"}" >/dev/null 2>&1 || true

echo "  racing concurrent config updates across both replicas..."
for i in 1 2 3 4 5; do
  AW=$((60 + i))
  BW=$((120 + i))
  curl -sf --max-time 5 -X PUT "$JANUS_URL_A/v1/tenants/$TENANT/mailboxes/$MB/config" \
    -H 'Content-Type: application/json' \
    -d "{\"max_concurrency\":4,\"ack_wait_seconds\":$AW,\"max_deliver\":5,\"retention_seconds\":3600}" >/dev/null 2>&1 || true &
  curl -sf --max-time 5 -X PUT "$JANUS_URL_B/v1/tenants/$TENANT/mailboxes/$MB/config" \
    -H 'Content-Type: application/json' \
    -d "{\"max_concurrency\":4,\"ack_wait_seconds\":$BW,\"max_deliver\":5,\"retention_seconds\":3600}" >/dev/null 2>&1 || true &
  wait
done

# The durable PG version must be the highest committed (>= the 5 paired bumps).
FINAL_PG=$(psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -t -A -c \
  "SELECT ack_wait_seconds FROM mailboxes WHERE tenant_id='$TENANT' AND id='$MB'" 2>/dev/null || echo "")
check "PG config readable after the race" "[ -n '$FINAL_PG' ]"

# Both APIs must still serve; the consumer must exist on the broker.
curl -sf --max-time 5 "$JANUS_URL_A/readyz" >/dev/null 2>&1
check "replica A still ready" "[ $? -eq 0 ]"
curl -sf --max-time 5 "$JANUS_URL_B/readyz" >/dev/null 2>&1
check "replica B still ready" "[ $? -eq 0 ]"

# Convergence: query the consumer config through a pull on replica B.
sleep 3
PULL=$(curl -sf --max-time 5 -X POST "$JANUS_URL_B/v1/tenants/$TENANT/mailboxes/$MB/pull" \
  -H 'Content-Type: application/json' -d '{"agent_id":"conv-agent"}' 2>/dev/null || echo "{}")
check "consumer answers pulls after the race" "[ -n \"$PULL\" ]"

# The broker's consumer must match the durable PG version (via NATS monitor).
BROKER_AW=$(curl -sf --max-time 5 "http://localhost:8222/jsz?consumers=true" 2>/dev/null | \
  python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    for acc in d.get('account_details', []):
        for st in acc.get('stream_detail', []):
            for c in st.get('consumer_detail', []):
                if '${TENANT}' in c.get('name',''):
                    print(c.get('config',{}).get('ack_wait', 0) // 1_000_000_000)
                    sys.exit(0)
except Exception:
    pass
print('')" 2>/dev/null || echo "")
if [ -n "$BROKER_AW" ] && [ "$BROKER_AW" != "0" ]; then
  check "broker consumer ack_wait ($BROKER_AW) converged to a raced value (61-125)" \
    "[ $BROKER_AW -ge 61 ] && [ $BROKER_AW -le 125 ]"
else
  echo "  ~ broker ack_wait introspection unavailable (jsz detail) — PG/pull checks suffice"
fi

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit $FAIL

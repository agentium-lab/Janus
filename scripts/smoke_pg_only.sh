#!/usr/bin/env bash
# Smoke test: PostgreSQL-only mode (no NATS).
# Boots docker compose (postgres + redis + janus-api with JANUS_QUEUE_DRIVER=pg),
# verifies NATS is absent, then walks publish -> pull -> ack over HTTP.
#
# Usage:
#   bash scripts/smoke_pg_only.sh            # cleanup after
#   KEEP=1 bash scripts/smoke_pg_only.sh     # keep stack running afterwards
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FULL_KEY="janus_e12ebdab09a71c856666756ab26fd12db630041a96559b74066f66820c1d6d48"
BASE="http://localhost:8080"
TENANT="acme"
AGENT="smoke-agent"
MAILBOX="smoke-mb"
TID="pgonly-smoke-$(date +%s)"

say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
die() { printf '\n\033[0;31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    say "Cleanup"
    docker compose down -v >/dev/null 2>&1 || true
  else
    echo "KEEP=1 -> stack left running (dev key still valid)"
  fi
}
trap cleanup EXIT

say "1. Boot compose (queue=pg, nats profile OFF)"
JANUS_QUEUE_DRIVER=pg docker compose up -d --quiet-pull

say "2. Assert NATS container is NOT running"
if docker compose ps --services | grep -qx "nats"; then
  die "nats service is running — pg-only mode compromised"
fi
echo "   OK: no nats container"

say "3. Wait for API health"
for i in $(seq 1 60); do
  curl -sf -m 2 "$BASE/healthz" >/dev/null && break
  [ "$i" = 60 ] && die "API never became healthy"
  sleep 2
done
curl -sf "$BASE/readyz" >/dev/null || die "readyz not ready"
echo "   OK: healthy & ready"

say "4. Register agent + mailbox"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPOST -H "X-API-Key: $FULL_KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$AGENT\",\"display_name\":\"PG-only Smoke\",\"protocol\":\"http\"}" \
  "$BASE/v1/tenants/$TENANT/agents")
{ [ "$code" = 201 ] || [ "$code" = 200 ]; } || die "agent register http $code"

code=$(curl -s -o /dev/null -w '%{http_code}' -XPOST -H "X-API-Key: $FULL_KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$MAILBOX\",\"agent_id\":\"$AGENT\"}" \
  "$BASE/v1/tenants/$TENANT/mailboxes")
{ [ "$code" = 201 ] || [ "$code" = 200 ]; } || die "mailbox create http $code"
echo "   OK: agent=$AGENT mailbox=$MAILBOX"

say "5. Publish task"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPOST -H "X-API-Key: $FULL_KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$TID\",\"source_agent\":\"$AGENT\",\"target_type\":\"mailbox\",\"target_value\":\"$MAILBOX\",\"envelope\":{\"janus_version\":\"1.0\",\"task_id\":\"$TID\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"$AGENT\",\"target\":{\"type\":\"mailbox\",\"value\":\"$MAILBOX\"},\"content_type\":\"application/json\",\"payload\":{\"type\":\"job\",\"content\":\"{}\"},\"trace\":{\"trace_id\":\"tr-$TID\"}}}" \
  "$BASE/v1/tenants/$TENANT/tasks")
[ "$code" = 201 ] || die "publish http $code (want 201)"
echo "   OK: task $TID created (201)"

say "6. Pull (claim via SKIP LOCKED)"
PULL=$(curl -s -XPOST -H "X-API-Key: $FULL_KEY" -H 'Content-Type: application/json' \
  -d "{\"mailbox_id\":\"$MAILBOX\",\"agent_id\":\"$AGENT\"}" \
  "$BASE/v1/tenants/$TENANT/pull")
LEASE=$(printf '%s' "$PULL" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("lease",{}).get("lease_id",""))')
[ -n "$LEASE" ] || die "pull returned no lease: $(printf '%s' "$PULL" | head -c 200)"
echo "   OK: claimed, lease=${LEASE:0:12}…"

say "7. Ack"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPOST -H "X-API-Key: $FULL_KEY" \
  -H 'Content-Type: application/json' \
  -d "{\"lease_id\":\"$LEASE\",\"result_ref\":\"local://smoke\"}" \
  "$BASE/v1/tenants/$TENANT/tasks/$TID/ack")
[ "$code" = 200 ] || die "ack http $code"
echo "   OK: acked"

say "PASS ✅  PostgreSQL-only loop verified (publish → claim → ack, zero NATS)"

#!/bin/bash
# PG-only (no NATS, no Redis) boot + agent registration guard.
# Validated locally against initdb PostgreSQL before being embedded in ci.yml.
set -uo pipefail

HTTP_PORT="${HTTP_PORT:-18099}"
BASE="http://localhost:${HTTP_PORT}"

fail() { echo "::error::$*"; if [ -f /tmp/pgonly.log ]; then tail -40 /tmp/pgonly.log; fi; exit 1; }

# status_and_body METHOD URL [DATA] -> prints "code body"; curl errors print "000 <curl error>"
status_and_body() {
  local method="$1" url="$2" data="${3:-}"
  local args=(-s -o /tmp/curl_body -w '%{http_code}' -X "$method" -H 'Content-Type: application/json')
  [ -n "$data" ] && args+=(-d "$data")
  local code
  code=$(curl "${args[@]}" "$url" 2>/tmp/curl_err) || true
  if [ "$code" = "000" ]; then
    echo "000 $(cat /tmp/curl_err)"
  else
    echo "$code $(cat /tmp/curl_body)"
  fi
}

# ensure NAME POST_URL DATA GET_URL: idempotent create — POST, or accept
# when a GET proves the entity already exists (re-runs against a kept DB).
ensure() {
  local name="$1" post_url="$2" data="$3" get_url="$4"
  local resp code
  resp=$(status_and_body POST "$post_url" "$data")
  code="${resp%% *}"
  case "$code" in 200|201|409) return 0 ;; esac
  resp=$(status_and_body GET "$get_url")
  [ "${resp%% *}" = "200" ] && return 0
  fail "$name create failed: $resp"
}

# ---- wait for health ----
for i in $(seq 1 30); do curl -sf "$BASE/healthz" >/dev/null 2>&1 && break; sleep 1; done
resp=$(status_and_body GET "$BASE/readyz")
[ "${resp%% *}" = "200" ] || fail "readyz failed: $resp"

# ---- tenant must exist before agents (FK) ----
ensure tenant "$BASE/v1/tenants" '{"id":"acme","name":"ACME"}' "$BASE/v1/tenants/acme"

# ---- agent registration: the actual typed-nil regression guard ----
ensure agent "$BASE/v1/tenants/acme/agents" \
  '{"id":"ci-agent","display_name":"CI Agent","protocol":"http","endpoint":"http://localhost:9"}' \
  "$BASE/v1/tenants/acme/agents/ci-agent"

resp=$(status_and_body GET "$BASE/v1/tenants/acme/agents/ci-agent")
echo "$resp" | grep -q '"online"' || fail "agent did not register online: $resp"

# ---- task routing validates the target mailbox exists ----
ensure mailbox "$BASE/v1/tenants/acme/mailboxes" '{"id":"ci-mb","agent_id":"ci-agent"}' "$BASE/v1/tenants/acme/mailboxes/ci-mb"

# ---- task create exercises the nil rate-limiter path ----
resp=$(status_and_body POST "$BASE/v1/tenants/acme/tasks" \
  '{"id":"ci-task-'$(date +%s)'","source_agent":"ci-agent","target_type":"mailbox","target_value":"ci-mb","mailbox_id":"ci-mb","envelope":{"janus_version":"1","task_id":"ci-task-'$(date +%s)'","tenant_id":"acme","source_agent":"ci-agent","target":{"type":"mailbox","value":"ci-mb"},"priority":"normal","payload":{"type":"text","content":"ci"},"trace":{"trace_id":"ci-1"}}}')
code="${resp%% *}"
case "$code" in
  200|201) : ;;
  *) fail "task create failed (nil rate-limiter path): $resp" ;;
esac

# ---- panic audit: only claim panic when the log actually has one ----
if grep -qi 'panic' /tmp/pgonly.log; then fail "panic found in server log"; fi

echo "PG-ONLY GUARD PASSED: boot + tenant + agent(online) + task without Redis/NATS"

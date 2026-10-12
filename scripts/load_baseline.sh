#!/bin/bash
set -euo pipefail

JANUS_URL="${JANUS_URL:-http://localhost:8080}"
TENANT="${JANUS_TENANT:-load-test}"
NUM_AGENTS="${NUM_AGENTS:-1000}"
NUM_MAILBOXES="${NUM_MAILBOXES:-1000}"
NUM_TASKS="${NUM_TASKS:-1000}"
CONCURRENCY="${CONCURRENCY:-10}"
PG_DSN="${JANUS_PG_DSN:-}"

echo "=== Load Baseline: $NUM_AGENTS agents / $NUM_MAILBOXES mailboxes / $NUM_TASKS tasks (concurrency $CONCURRENCY) ==="
echo "Targets: publish success 100%, success-path p95 < 100ms, persisted == published"
echo ""

code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST "$JANUS_URL/v1/tenants" \
  -H 'Content-Type: application/json' -d "{\"id\":\"$TENANT\",\"name\":\"Load Test\"}")
case "$code" in
  200|201) ;;
  400)
    gcode=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$JANUS_URL/v1/tenants/$TENANT")
    [ "$gcode" = "200" ] || { echo "FATAL: tenant create -> $code (GET -> $gcode)"; exit 1; } ;;
  *) echo "FATAL: tenant create -> $code"; exit 1 ;;
esac

echo "Phase 1: Register $NUM_AGENTS agents..."
T0=$(date +%s%N)
AGENT_OK=0; AGENT_DUP=0
for i in $(seq 1 "$NUM_AGENTS"); do
  AGENT_ID=$(printf "agent-%05d" "$i")
  c=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST "$JANUS_URL/v1/tenants/$TENANT/agents" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"$AGENT_ID\",\"display_name\":\"Agent $i\",\"protocol\":\"a2a\"}")
  case "$c" in
    200|201) AGENT_OK=$((AGENT_OK+1)) ;;
    400) AGENT_DUP=$((AGENT_DUP+1)) ;;
    *) echo "FATAL: agent $AGENT_ID -> HTTP $c"; exit 1 ;;
  esac
done
T1=$(date +%s%N)
echo "  Done in $(( (T1 - T0) / 1000000 ))ms ($(( (T1 - T0) / 1000000 / NUM_AGENTS ))ms/agent, ok=$AGENT_OK dup=$AGENT_DUP)"

echo "Phase 2: Create $NUM_MAILBOXES mailboxes..."
T0=$(date +%s%N)
MB_OK=0; MB_DUP=0
for i in $(seq 1 "$NUM_MAILBOXES"); do
  MB_ID=$(printf "mb-%05d" "$i")
  AGENT_ID=$(printf "agent-%05d" "$i")
  c=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST "$JANUS_URL/v1/tenants/$TENANT/mailboxes" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"$MB_ID\",\"agent_id\":\"$AGENT_ID\"}")
  case "$c" in
    200|201) MB_OK=$((MB_OK+1)) ;;
    400) MB_DUP=$((MB_DUP+1)) ;;
    *) echo "FATAL: mailbox $MB_ID -> HTTP $c"; exit 1 ;;
  esac
done
T1=$(date +%s%N)
echo "  Done in $(( (T1 - T0) / 1000000 ))ms ($(( (T1 - T0) / 1000000 / NUM_MAILBOXES ))ms/mailbox, ok=$MB_OK dup=$MB_DUP)"

echo "Phase 3: Publish $NUM_TASKS tasks CONCURRENTLY ($CONCURRENCY workers)..."
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
seq 1 "$NUM_TASKS" | split -l $(( (NUM_TASKS + CONCURRENCY - 1) / CONCURRENCY )) -d -a 4 - "$WORK/chunk."

worker() {
  local wid="$1" chunk="$2" idx task_id mb_id agent_id ts te c
  : > "$WORK/lat-$wid"; : > "$WORK/fail-$wid"
  while read -r idx; do
    task_id=$(printf "task-%05d" "$idx")
    mb_id=$(printf "mb-%05d" "$(( (idx - 1) % NUM_MAILBOXES + 1 ))")
    agent_id=$(printf "agent-%05d" "$(( (idx - 1) % NUM_AGENTS + 1 ))")
    ts=$(date +%s%N)
    c=$(curl -s -o /dev/null -w "%{http_code}" --max-time 15 -X POST "$JANUS_URL/v1/tenants/$TENANT/tasks" \
      -H 'Content-Type: application/json' \
      -d "{\"id\":\"$task_id\",\"source_agent\":\"$agent_id\",\"target_type\":\"mailbox\",\"target_value\":\"$mb_id\",\"envelope\":{\"janus_version\":\"0.3\",\"task_id\":\"$task_id\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"$agent_id\",\"target\":{\"type\":\"mailbox\",\"value\":\"$mb_id\"},\"payload\":{\"type\":\"load\",\"content\":\"task $idx\"},\"trace\":{\"trace_id\":\"load-$task_id\"}}}")
    te=$(date +%s%N)
    if [ "$c" = "201" ] || [ "$c" = "200" ]; then
      echo $(( (te - ts) / 1000000 )) >> "$WORK/lat-$wid"
    else
      echo "FAIL-$c-$task_id" >> "$WORK/fail-$wid"
    fi
  done < "$chunk"
}

pids=""
c=0
T0=$(date +%s%N)
for chunk in "$WORK"/chunk.*; do
  worker "$c" "$chunk" &
  pids="$pids $!"
  c=$((c+1))
done
for p in $pids; do wait "$p"; done
T1=$(date +%s%N)
PUB_MS=$(( (T1 - T0) / 1000000 ))

SUCCESSES=$(cat "$WORK"/lat-* 2>/dev/null | wc -l | tr -d ' ')
FAILURES=$(cat "$WORK"/fail-* 2>/dev/null | grep -c . 2>/dev/null || echo 0)
FAIL_DETAIL=$(cat "$WORK"/fail-* 2>/dev/null | head -3 | tr '\n' ' ')

echo "  Total: ${PUB_MS}ms at concurrency $CONCURRENCY"
echo "  Successes: $SUCCESSES / $NUM_TASKS   Failures: $FAILURES"
[ -n "$FAIL_DETAIL" ] && echo "  Failure sample: $FAIL_DETAIL"

if [ "$SUCCESSES" -eq 0 ]; then
  echo ""
  echo "FAIL: zero successful publishes"
  exit 1
fi

SORTED=$(cat "$WORK"/lat-* | sort -n)
P95_INDEX=$(( SUCCESSES * 95 / 100 ))
P95=$(echo "$SORTED" | sed -n "$((P95_INDEX + 1))p")
echo "  Success-path p95: ${P95}ms"

if [ -n "$PG_DSN" ]; then
  PERSISTED=$(psql "$PG_DSN" -t -A -c \
    "SELECT count(*) FROM tasks WHERE tenant_id='$TENANT' AND id LIKE 'task-%'" 2>/dev/null || echo "")
  if [ -n "$PERSISTED" ]; then
    echo "  Persisted in PG: $PERSISTED"
    if [ "$PERSISTED" != "$SUCCESSES" ]; then
      echo ""
      echo "FAIL: persisted ($PERSISTED) != HTTP successes ($SUCCESSES)"
      exit 1
    fi
  fi
fi

RC=0
if [ "$FAILURES" -gt 0 ]; then
  echo "FAIL: $FAILURES publishes failed"
  RC=1
fi
if [ "$P95" -ge 100 ]; then
  echo "FAIL: success-path p95 ${P95}ms >= 100ms target"
  RC=1
fi
if [ "$RC" -eq 0 ]; then
  echo ""
  echo "PASS: $SUCCESSES/$NUM_TASKS published (100%), success-path p95 ${P95}ms < 100ms"
fi
exit $RC

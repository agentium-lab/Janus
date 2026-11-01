#!/bin/bash
# GA-11 sustained-load gate: a rate-limited publisher and concurrent consumers
# running against one janus-api for a sustained window, with end-of-window
# gates derived from PostgreSQL (not just HTTP counters).
#
# Unlike scripts/load_baseline.sh (a short concurrent publish burst + row
# count), this script holds a configured tasks/sec for the whole window while
# CONSUMERS workers loop pull -> start -> ack over mailbox shards, then gates:
#   - published HTTP successes == persisted tasks
#   - every published task reaches terminal 'completed' within a bounded drain
#   - zero dead_lettered
#   - outbox backlog reaches 0 (outbox_events.status, published is terminal)
#   - publish p95 < P95_GATE_MS
#   - pull p95 < P95_GATE_MS (non-empty pulls only; empty pulls are not samples)
#   - observed audit event rate >= derived expectation (see EVENT SEMANTICS)
#
# Usage:
#   SCALE_PROFILE=smoke JANUS_URL=http://localhost:18123 bash scripts/load_ga11_sustained.sh
#   JANUS_URL=... JANUS_PG_DSN=... bash scripts/load_ga11_sustained.sh   # full profile
#
# Knobs (env-overridable; SCALE_PROFILE only supplies defaults):
#   JANUS_URL            broker base URL                (default http://localhost:8080)
#   JANUS_PG_DSN         psql DSN for assertions; if unset it is composed from
#                        JANUS_PG_USER/PASSWORD/HOST/PORT/DATABASE
#                        (defaults janus/janus/localhost/5432/janus_test)
#   NUM_AGENTS           agents to register             (full 1000, smoke 50)
#   NUM_MAILBOXES        mailboxes, round-robin to agents (full 10000, smoke 200)
#   TASK_RATE            tasks/sec held for the window  (full 100, smoke 20)
#   WINDOW_SECONDS       sustained window length        (full 120, smoke 20)
#   CONSUMERS            concurrent pull->start->ack workers (default 8)
#   P95_GATE_MS          publish/pull p95 gate in ms    (default 100)
#   SCALE_PROFILE        full|smoke (default full)
#   Plus: DRAIN_TIMEOUT_S (full 600 / smoke 120), FIXTURE_PARALLELISM (16),
#   PUB_WORKERS (4), EVENT_RATE_GATE (override the derived event-rate gate).
#
# NOTE on the discovery budget: a pull against an EMPTY mailbox long-polls
# ~2s server-side (dispatch FetchOptions WaitTime), so one consumer sweeps its
# shard of NUM_MAILBOXES/CONSUMERS mailboxes in ~2s x shard size. Consumers
# therefore sweep their shard in a deterministic order and their default count
# keeps a shard at <= 8 mailboxes (~16s per sweep, capped at 128 processes);
# a random cursor starves cold mailboxes and leaves published tasks
# undiscovered inside the drain.
#
# NOTE on server sizing: the outbox publisher AND the audit projector each
# drain outbox.batch_size rows per outbox.worker_interval tick (defaults 50 /
# 500ms = ~100 rows/s each). At TASK_RATE=100 the full profile produces ~600
# outbox rows/s, so the server under test must raise JANUS_OUTBOX_BATCH_SIZE
# (e.g. 1000) or the drain gates will honestly fail on projection lag.

set -euo pipefail

JANUS_URL="${JANUS_URL:-http://localhost:8080}"
SCALE_PROFILE="${SCALE_PROFILE:-full}"

case "$SCALE_PROFILE" in
  full)
    : "${NUM_AGENTS:=1000}"
    : "${NUM_MAILBOXES:=10000}"
    : "${TASK_RATE:=100}"
    : "${WINDOW_SECONDS:=120}"
    : "${DRAIN_TIMEOUT_S:=600}"
    ;;
  smoke)
    : "${NUM_AGENTS:=50}"
    : "${NUM_MAILBOXES:=200}"
    : "${TASK_RATE:=20}"
    : "${WINDOW_SECONDS:=20}"
    : "${DRAIN_TIMEOUT_S:=120}"
    ;;
  *)
    echo "FATAL: SCALE_PROFILE must be 'full' or 'smoke', got '$SCALE_PROFILE'"
    exit 1
    ;;
esac

DEFAULT_CONSUMERS=$(( NUM_MAILBOXES / 8 ))
[ "$DEFAULT_CONSUMERS" -lt 8 ] && DEFAULT_CONSUMERS=8
[ "$DEFAULT_CONSUMERS" -gt 128 ] && DEFAULT_CONSUMERS=128
CONSUMERS="${CONSUMERS:-$DEFAULT_CONSUMERS}"
P95_GATE_MS="${P95_GATE_MS:-100}"
FIXTURE_PARALLELISM="${FIXTURE_PARALLELISM:-16}"
PUB_WORKERS="${PUB_WORKERS:-4}"
TENANT="${JANUS_TENANT:-ga11-$(date +%s)}"

PG_DSN="${JANUS_PG_DSN:-}"
if [ -z "$PG_DSN" ]; then
  PG_USER="${JANUS_PG_USER:-janus}"
  PG_PASSWORD="${JANUS_PG_PASSWORD:-janus}"
  PG_HOST="${JANUS_PG_HOST:-localhost}"
  PG_PORT="${JANUS_PG_PORT:-5432}"
  PG_DATABASE="${JANUS_PG_DATABASE:-janus_test}"
  PG_DSN="postgres://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:${PG_PORT}/${PG_DATABASE}"
fi

pgq() { psql "$PG_DSN" -t -A -c "$1"; }

WORK="$(mktemp -d)"
STOP_FILE="$WORK/STOP"
PIDS=""
cleanup() {
  local rc=$?
  touch "$STOP_FILE" 2>/dev/null || true
  for p in $PIDS; do kill "$p" 2>/dev/null || true; done
  sleep 0.2
  for p in $PIDS; do kill -9 "$p" 2>/dev/null || true; done
  [ -n "${KEEP_WORK:-}" ] || rm -rf "$WORK"
  return $rc
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Preflight: unreachable dependencies are FATAL, never a green SKIP.
# ---------------------------------------------------------------------------
for bin in curl psql jq; do
  command -v "$bin" >/dev/null 2>&1 || { echo "FATAL: required binary '$bin' not found"; exit 1; }
done

ready=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "$JANUS_URL/readyz" 2>/dev/null) || ready=000
[ "$ready" = "200" ] || { echo "FATAL: $JANUS_URL/readyz -> $ready (broker unreachable)"; exit 1; }

probe=$(pgq "SELECT 1" 2>&1) || { echo "FATAL: PG unreachable via psql: $probe"; exit 1; }
[ "$probe" = "1" ] || { echo "FATAL: PG probe returned '$probe' (expected 1)"; exit 1; }

echo "=== GA-11 Sustained Load (profile=$SCALE_PROFILE) ==="
echo "Target: $JANUS_URL  tenant=$TENANT"
echo "Agents=$NUM_AGENTS  Mailboxes=$NUM_MAILBOXES  Rate=${TASK_RATE}/s  Window=${WINDOW_SECONDS}s  Consumers=$CONSUMERS  Publishers=$PUB_WORKERS"
echo ""

# ---------------------------------------------------------------------------
# Tenant
# ---------------------------------------------------------------------------
code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST "$JANUS_URL/v1/tenants" \
  -H 'Content-Type: application/json' -d "{\"id\":\"$TENANT\",\"name\":\"GA-11 Sustained Load\"}")
case "$code" in
  200|201) ;;
  400)
    gcode=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$JANUS_URL/v1/tenants/$TENANT")
    [ "$gcode" = "200" ] || { echo "FATAL: tenant create -> $code (GET -> $gcode)"; exit 1; } ;;
  *) echo "FATAL: tenant create -> $code"; exit 1 ;;
esac

# ---------------------------------------------------------------------------
# Fixtures, created in PARALLEL (xargs -P). Mailbox i is bound round-robin to
# agent ((i-1) % NUM_AGENTS) + 1 — the same mapping the consumers invert.
# ---------------------------------------------------------------------------
export JANUS_URL TENANT NUM_AGENTS NUM_MAILBOXES

ga11_register_agent() {
  local i="$1" c
  c=$(curl -s -o /dev/null -w "%{http_code}" --max-time 15 -X POST \
    "$JANUS_URL/v1/tenants/$TENANT/agents" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"agent-$(printf '%05d' "$i")\",\"display_name\":\"GA11 Agent $i\",\"protocol\":\"a2a\"}")
  case "$c" in
    200|201) echo "OK" ;;
    400|409) echo "DUP" ;;
    *) echo "FAIL-$c-agent-$(printf '%05d' "$i")" ;;
  esac
}
export -f ga11_register_agent

ga11_create_mailbox() {
  local i="$1" c ag
  ag=$(( (i - 1) % NUM_AGENTS + 1 ))
  c=$(curl -s -o /dev/null -w "%{http_code}" --max-time 15 -X POST \
    "$JANUS_URL/v1/tenants/$TENANT/mailboxes" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"mb-$(printf '%05d' "$i")\",\"agent_id\":\"agent-$(printf '%05d' "$ag")\"}")
  case "$c" in
    200|201) echo "OK" ;;
    400|409) echo "DUP" ;;
    *) echo "FAIL-$c-mb-$(printf '%05d' "$i")" ;;
  esac
}
export -f ga11_create_mailbox

echo "Phase 1: Register $NUM_AGENTS agents (parallelism $FIXTURE_PARALLELISM)..."
T0=$(date +%s%N)
AGENT_OUT=$(seq 1 "$NUM_AGENTS" | xargs -P "$FIXTURE_PARALLELISM" -n 1 bash -c 'ga11_register_agent "$1"' _)
AGENT_OK=$(grep -c '^OK$' <<<"$AGENT_OUT" || true)
AGENT_DUP=$(grep -c '^DUP$' <<<"$AGENT_OUT" || true)
AGENT_FAIL=$(grep -c '^FAIL' <<<"$AGENT_OUT" || true)
AGENT_FAIL_DETAIL=$(grep '^FAIL' <<<"$AGENT_OUT" | head -3 | tr '\n' ' ' || true)
T1=$(date +%s%N)
echo "  done in $(( (T1 - T0) / 1000000 ))ms (ok=$AGENT_OK dup=$AGENT_DUP fail=$AGENT_FAIL)"
[ "$AGENT_FAIL" -eq 0 ] || { echo "FATAL: agent registration failures: $AGENT_FAIL_DETAIL"; exit 1; }

PG_AGENTS=$(pgq "SELECT count(*) FROM agents WHERE tenant_id='$TENANT' AND id LIKE 'agent-%'")
echo "  persisted in PG: $PG_AGENTS"
[ "$PG_AGENTS" = "$NUM_AGENTS" ] || { echo "FATAL: persisted agents ($PG_AGENTS) != created ($NUM_AGENTS)"; exit 1; }

echo "Phase 2: Create $NUM_MAILBOXES mailboxes (parallelism $FIXTURE_PARALLELISM)..."
T0=$(date +%s%N)
MB_OUT=$(seq 1 "$NUM_MAILBOXES" | xargs -P "$FIXTURE_PARALLELISM" -n 1 bash -c 'ga11_create_mailbox "$1"' _)
MB_OK=$(grep -c '^OK$' <<<"$MB_OUT" || true)
MB_DUP=$(grep -c '^DUP$' <<<"$MB_OUT" || true)
MB_FAIL=$(grep -c '^FAIL' <<<"$MB_OUT" || true)
MB_FAIL_DETAIL=$(grep '^FAIL' <<<"$MB_OUT" | head -3 | tr '\n' ' ' || true)
T1=$(date +%s%N)
echo "  done in $(( (T1 - T0) / 1000000 ))ms (ok=$MB_OK dup=$MB_DUP fail=$MB_FAIL)"
[ "$MB_FAIL" -eq 0 ] || { echo "FATAL: mailbox creation failures: $MB_FAIL_DETAIL"; exit 1; }

PG_MAILBOXES=$(pgq "SELECT count(*) FROM mailboxes WHERE tenant_id='$TENANT' AND id LIKE 'mb-%'")
echo "  persisted in PG: $PG_MAILBOXES"
[ "$PG_MAILBOXES" = "$NUM_MAILBOXES" ] || { echo "FATAL: persisted mailboxes ($PG_MAILBOXES) != created ($NUM_MAILBOXES)"; exit 1; }

# ---------------------------------------------------------------------------
# Publisher worker: token bucket over the global task index space. Worker w
# owns indices w, w+PUB_WORKERS, ...; the due-count is derived from elapsed
# time x TASK_RATE so the aggregate rate self-corrects. The only sleeps are
# the pacing wake-ups; publish latency is measured around the curl alone.
# Task n targets mailbox ((n-1) % NUM_MAILBOXES)+1 from agent ((n-1) % NUM_AGENTS)+1.
# ---------------------------------------------------------------------------
publisher() {
  local wid="$1" start_ns now elapsed_ns due n mb ag tid ts te code
  : > "$WORK/lat-pub-$wid"; : > "$WORK/pub-fail-$wid"
  start_ns=$(date +%s%N)
  local idx=$wid   # 0-based global index owned by this worker
  while :; do
    now=$(date +%s%N)
    elapsed_ns=$(( now - start_ns ))
    [ "$elapsed_ns" -ge $(( WINDOW_SECONDS * 1000000000 )) ] && break
    due=$(( elapsed_ns * TASK_RATE / 1000000000 ))
    while [ "$idx" -le "$due" ]; do
      n=$(( idx + 1 ))
      mb=$(( (n - 1) % NUM_MAILBOXES + 1 ))
      ag=$(( (n - 1) % NUM_AGENTS + 1 ))
      tid=$(printf 'task-%07d' "$n")
      ts=$(date +%s%N)
      code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 15 -X POST \
        "$JANUS_URL/v1/tenants/$TENANT/tasks" \
        -H 'Content-Type: application/json' \
        -d "{\"id\":\"$tid\",\"source_agent\":\"agent-$(printf '%05d' "$ag")\",\"target_type\":\"mailbox\",\"target_value\":\"mb-$(printf '%05d' "$mb")\",\"envelope\":{\"janus_version\":\"0.3\",\"task_id\":\"$tid\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"agent-$(printf '%05d' "$ag")\",\"target\":{\"type\":\"mailbox\",\"value\":\"mb-$(printf '%05d' "$mb")\"},\"payload\":{\"type\":\"ga11-sustained\",\"content\":\"task $n\"},\"trace\":{\"trace_id\":\"ga11-$tid\"}}}")
      te=$(date +%s%N)
      if [ "$code" = "201" ] || [ "$code" = "200" ]; then
        echo $(( (te - ts) / 1000000 )) >> "$WORK/lat-pub-$wid"
      else
        echo "FAIL-$code-$tid" >> "$WORK/pub-fail-$wid"
      fi
      idx=$(( idx + PUB_WORKERS ))
    done
    sleep 0.1
  done
}

# ---------------------------------------------------------------------------
# Consumer worker: loops pull -> start -> ack over its mailbox shard until the
# STOP file appears. Empty pulls (204) are tolerated: not failures, not
# latency samples. Pull latency is recorded only for pulls that return a task.
# A failed start/ack is recorded to consfail for the summary (the server's
# lease/redelivery machinery owns recovery; the consumer moves on).
# ---------------------------------------------------------------------------
consumer() {
  local cid="$1" i n=0 cursor mb ag ts te body code json tid lid sc ac
  : > "$WORK/lat-pull-$cid"; : > "$WORK/empty-$cid"
  : > "$WORK/pullerr-$cid"; : > "$WORK/consfail-$cid"
  local mbs=() ags=()
  for (( i = cid + 1; i <= NUM_MAILBOXES; i += CONSUMERS )); do
    mbs+=("mb-$(printf '%05d' "$i")")
    ags+=("agent-$(printf '%05d' "$(( (i - 1) % NUM_AGENTS + 1 ))")")
    n=$(( n + 1 ))
  done
  [ "$n" -gt 0 ] || return 0
  cursor=$(( RANDOM % n ))
  while [ ! -f "$STOP_FILE" ]; do
    mb="${mbs[cursor]}"
    ag="${ags[cursor]}"
    cursor=$(( (cursor + 1) % n ))
    ts=$(date +%s%N)
    body=$(curl -s --max-time 10 -w $'\n%{http_code}' -X POST \
      "$JANUS_URL/v1/tenants/$TENANT/mailboxes/$mb/pull" \
      -H 'Content-Type: application/json' \
      -d "{\"agent_id\":\"$ag\"}" || echo $'\n000')
    te=$(date +%s%N)
    code="${body##*$'\n'}"
    json="${body%$'\n'*}"
    if [ "$code" = "204" ]; then
      echo 1 >> "$WORK/empty-$cid"
      continue
    fi
    if [ "$code" != "200" ]; then
      echo "PULL-$code-$mb" >> "$WORK/pullerr-$cid"
      continue
    fi
    echo $(( (te - ts) / 1000000 )) >> "$WORK/lat-pull-$cid"
    tid=$(jq -r '.task.id // empty' <<<"$json")
    lid=$(jq -r '.lease.lease_id // empty' <<<"$json")
    if [ -z "$tid" ] || [ -z "$lid" ]; then
      echo "PARSE-$mb" >> "$WORK/pullerr-$cid"
      continue
    fi
    sc=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST \
      "$JANUS_URL/v1/tenants/$TENANT/tasks/$tid/start" \
      -H 'Content-Type: application/json' \
      -d "{\"lease_id\":\"$lid\",\"agent_id\":\"$ag\"}")
    [ "$sc" = "200" ] || echo "START-$sc-$tid" >> "$WORK/consfail-$cid"
    ac=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST \
      "$JANUS_URL/v1/tenants/$TENANT/tasks/$tid/ack" \
      -H 'Content-Type: application/json' \
      -d "{\"lease_id\":\"$lid\",\"agent_id\":\"$ag\",\"result_ref\":\"result://ga11/$tid\"}")
    [ "$ac" = "200" ] || echo "ACK-$ac-$tid" >> "$WORK/consfail-$cid"
  done
}

# ---------------------------------------------------------------------------
# Sustained window: publishers + consumers run concurrently.
# ---------------------------------------------------------------------------
echo ""
echo "Phase 3: Sustained window — ${TASK_RATE} tasks/s for ${WINDOW_SECONDS}s with $CONSUMERS consumers..."
WIN_START=$(date +%s)
for (( w = 0; w < PUB_WORKERS; w++ )); do
  publisher "$w" &
  PIDS="$PIDS $!"
done
for (( c = 0; c < CONSUMERS; c++ )); do
  consumer "$c" &
  PIDS="$PIDS $!"
done

# Publishers self-terminate at their own WINDOW_SECONDS deadline. They were
# spawned first, so wait on the first PUB_WORKERS pids only; consumers keep
# running into the drain phase.
i=0
for p in $PIDS; do
  i=$(( i + 1 ))
  if [ "$i" -le "$PUB_WORKERS" ]; then wait "$p" || true; fi
done
WIN_END=$(( WIN_START + WINDOW_SECONDS ))

PUBLISHED=$(cat "$WORK"/lat-pub-* 2>/dev/null | wc -l | tr -d ' ')
PUB_ATTEMPT=$(( PUBLISHED + $(cat "$WORK"/pub-fail-* 2>/dev/null | grep -c . || true) ))
PUB_FAILURES=$(cat "$WORK"/pub-fail-* 2>/dev/null | grep -c . || true)
PUB_FAIL_DETAIL=$(cat "$WORK"/pub-fail-* 2>/dev/null | head -3 | tr '\n' ' ' || true)

# ---------------------------------------------------------------------------
# Drain: consumers keep working until every published task is terminal
# 'completed' AND the outbox is fully drained (published + projected).
# ---------------------------------------------------------------------------
echo ""
echo "Phase 4: Drain (timeout ${DRAIN_TIMEOUT_S}s)..."
DRAIN_START=$(date +%s)
COMPLETED=0; OUTBOX_BACKLOG=1; UNPROJECTED=1
while :; do
  COMPLETED=$(pgq "SELECT count(*) FROM tasks WHERE tenant_id='$TENANT' AND status='completed'")
  OUTBOX_BACKLOG=$(pgq "SELECT count(*) FROM outbox_events WHERE tenant_id='$TENANT' AND status <> 'published'")
  UNPROJECTED=$(pgq "SELECT count(*) FROM outbox_events WHERE tenant_id='$TENANT' AND kind='event_publish' AND projected_at IS NULL")
  [ "$COMPLETED" = "$PUBLISHED" ] && [ "$OUTBOX_BACKLOG" = "0" ] && [ "$UNPROJECTED" = "0" ] && break
  NOW=$(date +%s)
  [ $(( NOW - DRAIN_START )) -ge "$DRAIN_TIMEOUT_S" ] && break
  sleep 2
done
DRAIN_S=$(( $(date +%s) - DRAIN_START ))
touch "$STOP_FILE"
for p in $PIDS; do kill "$p" 2>/dev/null || true; done
for p in $PIDS; do wait "$p" 2>/dev/null || true; done

# Re-read final state after the consumers stopped.
COMPLETED=$(pgq "SELECT count(*) FROM tasks WHERE tenant_id='$TENANT' AND status='completed'")
PERSISTED=$(pgq "SELECT count(*) FROM tasks WHERE tenant_id='$TENANT' AND id LIKE 'task-%'")
DEAD_LETTERED=$(pgq "SELECT count(*) FROM tasks WHERE tenant_id='$TENANT' AND status='dead_lettered'")
OUTBOX_BACKLOG=$(pgq "SELECT count(*) FROM outbox_events WHERE tenant_id='$TENANT' AND status <> 'published'")
UNPROJECTED=$(pgq "SELECT count(*) FROM outbox_events WHERE tenant_id='$TENANT' AND kind='event_publish' AND projected_at IS NULL")

# ---------------------------------------------------------------------------
# Latency percentiles (same sorted-index method as load_baseline.sh).
# ---------------------------------------------------------------------------
percentile() { # files-glob index-percent
  local values idx p95
  values=$(cat $1 2>/dev/null | sort -n)
  local count
  count=$(<<<"$values" grep -c . || true)
  [ "$count" -gt 0 ] || { echo ""; return; }
  idx=$(( count * $2 / 100 ))
  p95=$(<<<"$values" sed -n "$(( idx + 1 ))p" | tr -d '[:space:]')
  [ -n "$p95" ] && [[ "$p95" =~ ^[0-9]+$ ]] || { echo ""; return; }
  echo "$p95"
}

PUB_P95=$(percentile "$WORK/lat-pub-*" 95)
PULL_P95=$(percentile "$WORK/lat-pull-*" 95)
PULL_SAMPLES=$(cat "$WORK"/lat-pull-* 2>/dev/null | wc -l | tr -d ' ')
EMPTY_PULLS=$(cat "$WORK"/empty-* 2>/dev/null | wc -l | tr -d ' ')
PULL_ERRORS=$(cat "$WORK"/pullerr-* 2>/dev/null | grep -c . || true)
CONS_ERRORS=$(cat "$WORK"/consfail-* 2>/dev/null | grep -c . || true)
CONS_ERROR_DETAIL=$(cat "$WORK"/consfail-* 2>/dev/null | head -3 | tr '\n' ' ' || true)

echo "  published: $PUBLISHED ok / $PUB_ATTEMPT attempts (failures=$PUB_FAILURES ${PUB_FAIL_DETAIL})"
echo "  pull samples: $PULL_SAMPLES non-empty (empty=$EMPTY_PULLS, errors=$PULL_ERRORS, start/ack errors=$CONS_ERRORS ${CONS_ERROR_DETAIL})"
echo "  drain: ${DRAIN_S}s"

# ---------------------------------------------------------------------------
# EVENT SEMANTICS (source-derived, then verified against measurement):
# Each happy-path task writes exactly 5 audit events — task.created +
# task.queued (server/internal/service/task_service.go createAtomic),
# task.claimed (dispatch_service.go PullTask tx), task.started (StartTaskAs),
# task.completed (AckTaskAs) — via outbox kind='event_publish', projected into
# audit_event_projection with occurred_at = the transition-time timestamp
# (event_helper.go MarshalEvent stamps it at the write point), so counting
# rows by occurred_at within the window measures when events HAPPENED, not
# when the projector caught up. The drain above also waits for projected_at
# to be set on every event row, so the count below is complete.
# ---------------------------------------------------------------------------
EXPECTED_EVENTS_PER_TASK=5
EVENTS=$(pgq "SELECT count(*) FROM audit_event_projection WHERE tenant_id='$TENANT' AND occurred_at >= to_timestamp($WIN_START) AND occurred_at < to_timestamp($WIN_END)")
EVENTS_ALL=$(pgq "SELECT count(*) FROM audit_event_projection WHERE tenant_id='$TENANT'")
EVENT_RATE_GATE="${EVENT_RATE_GATE:-$(( TASK_RATE * EXPECTED_EVENTS_PER_TASK * 8 / 10 ))}"
EVENTS_PER_S=$(awk -v e="$EVENTS" -v w="$WINDOW_SECONDS" 'BEGIN { printf "%.1f", e / w }')
EVENTS_PER_TASK=$(awk -v e="$EVENTS_ALL" -v t="$PUBLISHED" 'BEGIN { if (t > 0) printf "%.2f", e / t; else print "n/a" }')
EVENT_BREAKDOWN=$(pgq "SELECT event_type || '=' || count(*) FROM audit_event_projection WHERE tenant_id='$TENANT' GROUP BY event_type ORDER BY count(*) DESC" | tr '\n' ' ')

echo ""
echo "=== Event-rate evidence ==="
echo "  audit events in window [$WIN_START, $WIN_END): $EVENTS -> ${EVENTS_PER_S}/s over ${WINDOW_SECONDS}s"
echo "  events per published task (all-time this tenant): $EVENTS_PER_TASK (expected $EXPECTED_EVENTS_PER_TASK)"
echo "  breakdown: $EVENT_BREAKDOWN"
if [ "$PUBLISHED" -gt 0 ] && [ "$EVENTS_PER_TASK" != "$EXPECTED_EVENTS_PER_TASK" ]; then
  echo "  NOTE: measured events/task ($EVENTS_PER_TASK) differs from the source-derived"
  echo "  expectation ($EXPECTED_EVENTS_PER_TASK). The gate below uses the derived"
  echo "  expectation (${TASK_RATE}/s x ${EXPECTED_EVENTS_PER_TASK} x 80% = ${EVENT_RATE_GATE}/s);"
  echo "  see the breakdown above for which lifecycle events the broker actually emitted."
fi

# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------
echo ""
echo "=== Gates ==="
GATES_FAILED=0
gate() { # name measured comparator target extra
  local name="$1" measured="$2" op="$3" target="$4" extra="${5:-}"
  local ok=0
  case "$op" in
    eq) [ "$measured" = "$target" ] && ok=1 ;;
    lt) [ "$measured" -lt "$target" ] && ok=1 ;;
    ge) awk -v a="$measured" -v b="$target" 'BEGIN { exit !(a >= b) }' && ok=1 ;;
  esac
  if [ "$ok" = 1 ]; then
    echo "  PASS $name: $measured $op $target $extra"
  else
    echo "  FAIL $name: $measured $op $target $extra"
    GATES_FAILED=$(( GATES_FAILED + 1 ))
  fi
}

gate "persisted==published" "$PERSISTED" eq "$PUBLISHED" "(persisted=$PERSISTED published=$PUBLISHED)"
gate "all_completed" "$COMPLETED" eq "$PUBLISHED" "(completed=$COMPLETED published=$PUBLISHED drain=${DRAIN_S}s)"
gate "zero_dead_lettered" "$DEAD_LETTERED" eq "0" "(dead_lettered=$DEAD_LETTERED)"
gate "outbox_backlog_zero" "$OUTBOX_BACKLOG" eq "0" "(backlog=$OUTBOX_BACKLOG unprojected=$UNPROJECTED)"
if [ -n "$PUB_P95" ]; then
  gate "publish_p95_lt_${P95_GATE_MS}ms" "$PUB_P95" lt "$P95_GATE_MS" "(publish_p95=${PUB_P95}ms)"
else
  echo "  FAIL publish_p95: no successful publish samples"
  GATES_FAILED=$(( GATES_FAILED + 1 ))
fi
if [ -n "$PULL_P95" ]; then
  gate "pull_p95_lt_${P95_GATE_MS}ms" "$PULL_P95" lt "$P95_GATE_MS" "(pull_p95=${PULL_P95}ms over $PULL_SAMPLES non-empty pulls)"
else
  echo "  FAIL pull_p95: no non-empty pull samples"
  GATES_FAILED=$(( GATES_FAILED + 1 ))
fi
gate "event_rate_ge_${EVENT_RATE_GATE}ps" "$EVENTS_PER_S" ge "$EVENT_RATE_GATE" "(events_per_s=${EVENTS_PER_S} gate=${EVENT_RATE_GATE}/s = ${TASK_RATE}/s x ${EXPECTED_EVENTS_PER_TASK} events x 80%)"

echo ""
if [ "$GATES_FAILED" -gt 0 ]; then
  echo "SUSTAINED_PASS=0 ($GATES_FAILED gate(s) failed)"
  echo "SUSTAINED_METRICS agents=$NUM_AGENTS mailboxes=$NUM_MAILBOXES tasks=$PUBLISHED window_s=$WINDOW_SECONDS publish_p95_ms=${PUB_P95:-n/a} pull_p95_ms=${PULL_P95:-n/a} events_per_s=$EVENTS_PER_S drain_s=$DRAIN_S"
  exit 1
fi
echo "SUSTAINED_PASS=1"
echo "SUSTAINED_METRICS agents=$NUM_AGENTS mailboxes=$NUM_MAILBOXES tasks=$PUBLISHED window_s=$WINDOW_SECONDS publish_p95_ms=$PUB_P95 pull_p95_ms=$PULL_P95 events_per_s=$EVENTS_PER_S drain_s=$DRAIN_S"
exit 0

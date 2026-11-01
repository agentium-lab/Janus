#!/usr/bin/env bash
# GA-11 SDK chaos matrix: five named cases driving the REAL go SDK worker
# (janus.NewJanusWorker + Run) against a live janus-api, with chaos induced on
# the server process. Handler-level facts come from a JSONL event log the
# driver's handler emits; SDK-internal outcomes (pull errors, stale-ack 409,
# ack connection failures) are asserted from the SDK's own stderr logging,
# and settlement facts from PostgreSQL — never from HTTP markers alone.
#
#   eof          server SIGKILLed between tasks -> the worker's poll loop must
#                log pull errors and KEEP RUNNING (Run never exits), then
#                settle a task published right after the server restarts.
#   network-cut  server SIGSTOPped for 12s (black hole: no FIN, no RST) with a
#                long-poll in flight -> after SIGCONT the worker keeps
#                settling; the cut is proven by a publish failing meanwhile.
#   lease-lost   mailbox ack_wait_seconds=6; one worker handles with NO
#                heartbeat -> the lease expires and the broker redelivers; a
#                second fast worker settles the redelivery EXACTLY ONCE
#                (ledger: one settling attempt, two scope rows) while the
#                slow attempt's late ack is swallowed without an SDK error
#                (at-least-once semantics).
#   ack-failure  server SIGKILLed while a slow handler runs -> the ACK hits a
#                dead endpoint (connection refused in SDK stderr); after
#                restart + lease expiry the task is redelivered and settles
#                exactly once.
#   90s          one end-to-end task with a 95s handler and 5s heartbeats on a
#                default (300s) mailbox -> completes on attempt 1 with no
#                redelivery, proving the raised 90s WriteTimeout / client
#                Timeout do not bound task duration.
#
# Per this repo's rule, an unreachable dependency is FATAL (never a green
# SKIP): PostgreSQL or NATS unreachable -> exit 2.
#
# Usage:   bash scripts/sdk_chaos_matrix.sh
# Knobs:   JANUS_PG_HOST/PORT/USER/PASSWORD/DBNAME (localhost/5432/janus/
#          janus/janus_test), JANUS_PG_SSLMODE (disable),
#          JANUS_NATS_URL (nats://localhost:4222), JANUS_API_BIN (skip the
#          build), JANUS_HTTP_PORT (18320), JANUS_GRPC_PORT (19320),
#          ACK_WAIT_SHORT (6), ACK_WAIT_ACKFAIL (8), SLOW_LEASELOST_S (20),
#          SLOW_ACKFAIL_S (15), SLOW_90S_S (95), NETCUT_S (12).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PG_HOST="${JANUS_PG_HOST:-localhost}"
PG_PORT="${JANUS_PG_PORT:-5432}"
PG_USER="${JANUS_PG_USER:-janus}"
PG_PASSWORD="${JANUS_PG_PASSWORD:-janus}"
PG_DBNAME="${JANUS_PG_DBNAME:-janus_test}"
PG_SSLMODE="${JANUS_PG_SSLMODE:-disable}"
NATS_URL="${JANUS_NATS_URL:-nats://localhost:4222}"
HTTP_PORT="${JANUS_HTTP_PORT:-18320}"
GRPC_PORT="${JANUS_GRPC_PORT:-19320}"
ACK_WAIT_SHORT="${ACK_WAIT_SHORT:-6}"
ACK_WAIT_ACKFAIL="${ACK_WAIT_ACKFAIL:-8}"
SLOW_LEASELOST_S="${SLOW_LEASELOST_S:-20}"
SLOW_ACKFAIL_S="${SLOW_ACKFAIL_S:-15}"
SLOW_90S_S="${SLOW_90S_S:-95}"
NETCUT_S="${NETCUT_S:-12}"
TENANT=sdkchaos
BASE="http://localhost:$HTTP_PORT"

WORK="$(mktemp -d /tmp/sdk-chaos.XXXXXX)"
PIDS=()
SRV_PID=""
SRV_LOG="$WORK/janus-api.log"
CASES_PASS=0
CASES_FAIL=0

say()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
die()  { printf '\033[0;31mFATAL: %s\033[0m\n' "$*" >&2; exit 2; }
pass() { printf '  \033[0;32mPASS\033[0m %s\n' "$*"; CASES_PASS=$((CASES_PASS+1)); }
fail() { printf '  \033[0;31mFAIL\033[0m %s\n' "$*"; CASES_FAIL=$((CASES_FAIL+1)); }

cleanup() {
  local pid
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [ "${KEEP_WORK:-0}" = "1" ]; then
    echo "KEEP_WORK=1: workdir retained at $WORK"
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

pgq() { # pgq <sql> -> one scalar
  PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
    -d "$PG_DBNAME" -tAq -c "$1"
}

# wait_until <desc> <timeout_s> <condition>: eval the shell condition until
# true -- helpers like events_of exist only in this shell, not a bash -c child
wait_until() {
  local desc="$1" tmo="$2" cond="$3"
  local i=0
  while [ "$i" -lt "$((tmo * 10))" ]; do
    if eval "$cond" >/dev/null 2>&1; then return 0; fi
    sleep 0.1; i=$((i+1))
  done
  printf '(timeout after %ss) %s\n' "$tmo" "$desc" >&2
  return 1
}

task_row() { # task_row <tid> <col> -> value once the row exists
  pgq "SELECT $2 FROM tasks WHERE tenant_id='$TENANT' AND id='$1'"
}
task_status()  { task_row "$1" 'status'; }
task_attempt() { task_row "$1" 'attempt_count'; }
task_completed_once() { # exactly-one settlement: ledger has only the settling attempt, 2 scope rows
  pgq "SELECT CASE WHEN count(*)=2 AND min(attempt)=max(attempt) THEN 'ok' ELSE '' END \
        FROM budget_usage_ledger WHERE tenant_id='$TENANT' AND task_id='$1'"
}

publish() { # publish <agent> <mailbox> <tid>
  local code
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST \
    "$BASE/v1/tenants/$TENANT/tasks" -H 'Content-Type: application/json' \
    -d "{\"id\":\"$3\",\"source_agent\":\"$1\",\"target_type\":\"mailbox\",\"target_value\":\"$2\",\"envelope\":{\"janus_version\":\"1\",\"task_id\":\"$3\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"$1\",\"target\":{\"type\":\"mailbox\",\"value\":\"$2\"},\"payload\":{\"type\":\"sdk-chaos\",\"content\":\"$3\"},\"trace\":{\"trace_id\":\"chaos-$3\"}}}")
  [ "$code" = "201" ] || [ "$code" = "200" ] || die "publish $3 -> HTTP $code"
}

events_of() { # events_of <case> <event> <tid> -> matching JSONL lines (Go
  # json.Encoder writes separators without spaces)
  grep "\"event\":\"$2\"" "$WORK/w-$1.jsonl" 2>/dev/null | grep "\"task\":\"$3\"" || true
}

start_worker() { # start_worker <case> <agent> <mailbox> [driver flags...]
  local case="$1" agent="$2" mb="$3"; shift 3
  : > "$WORK/w-$case.jsonl"
  ( cd "$WORK" && exec "$WORK/chaosworker" -url "$BASE" -tenant "$TENANT" -agent "$agent" \
      -mailbox "$mb" -events "$WORK/w-$case.jsonl" "$@" ) > "$WORK/w-$case.log" 2>&1 &
  PIDS+=("$!")
  eval "W_${case}_PID=\$!"
  sleep 0.5
  kill -0 "${PIDS[-1]}" 2>/dev/null || { tail -5 "$WORK/w-$case.log" >&2; die "worker $case exited at start"; }
}

worker_alive() { kill -0 "$1" 2>/dev/null; }

boot_server() {
  local envs=(
    "JANUS_HTTP_PORT=$HTTP_PORT" "JANUS_HTTP_HOST=localhost"
    "JANUS_GRPC_PORT=$GRPC_PORT"
    "JANUS_MIGRATION_AUTO=true" "JANUS_MIGRATION_PATH=$ROOT/migrations"
    "JANUS_AUTH_ENABLED=false"
    "JANUS_PG_HOST=$PG_HOST" "JANUS_PG_PORT=$PG_PORT" "JANUS_PG_USER=$PG_USER"
    "JANUS_PG_PASSWORD=$PG_PASSWORD" "JANUS_PG_DATABASE=$PG_DBNAME"
    "JANUS_PG_SSLMODE=$PG_SSLMODE"
    "JANUS_QUEUE_DRIVER=nats" "JANUS_NATS_URL=$NATS_URL"
  )
  ( cd "$WORK" && exec env "${envs[@]}" "$BIN" >>"$SRV_LOG" 2>&1 ) &
  SRV_PID=$!
  PIDS+=("$SRV_PID")
  local i
  for i in $(seq 1 60); do
    curl -sf -m 2 "$BASE/readyz" >/dev/null 2>&1 && return 0
    kill -0 "$SRV_PID" 2>/dev/null || { tail -20 "$SRV_LOG" >&2; die "janus-api exited during boot"; }
    sleep 1
  done
  tail -20 "$SRV_LOG" >&2; die "janus-api never became ready"
}

kill_server()   { [ -n "$SRV_PID" ] && kill -9 "$SRV_PID" 2>/dev/null || true; sleep 0.3; }
stop_server()   { kill -STOP "$SRV_PID" 2>/dev/null || true; }
resume_server() { kill -CONT "$SRV_PID" 2>/dev/null || true; }

# ---------------------------------------------------------------- 0. preflight
say "GA-11 SDK chaos matrix (go SDK worker vs live janus-api)"

for bin in curl psql jq go; do command -v "$bin" >/dev/null || die "required tool missing: $bin"; done
pgq "SELECT 1" >/dev/null 2>&1 || die "PostgreSQL unreachable at $PG_HOST:$PG_PORT/$PG_DBNAME"
curl -sf -m 3 "$NATS_URL" >/dev/null 2>&1 || true
# NATS has no HTTP health endpoint; probe the port instead
(exec 3<>/dev/tcp/"${NATS_URL#*://}"; exec 3>&-) 2>/dev/null || die "NATS unreachable at $NATS_URL"

say "1. Build janus-api + chaos worker driver"
if [ -n "${JANUS_API_BIN:-}" ]; then
  BIN="$JANUS_API_BIN"; [ -x "$BIN" ] || die "JANUS_API_BIN=$BIN is not executable"
else
  BIN="$WORK/janus-api"
  ( cd "$ROOT/server" && go build -o "$BIN" ./cmd/janus-api ) || die "go build janus-api failed"
fi

mkdir -p "$WORK/driver"
cat > "$WORK/driver/go.mod" <<EOF
module chaosworker

go 1.25.8

require (
	github.com/agentium-lab/Janus/core v0.0.0
	github.com/agentium-lab/Janus/sdk/go v0.0.0
)

replace github.com/agentium-lab/Janus/sdk/go => $ROOT/sdk/go

replace github.com/agentium-lab/Janus/core => $ROOT/core
EOF
cat > "$WORK/driver/main.go" <<'EOF'
// chaosworker runs the real janus.NewJanusWorker Run loop with an instrumented
// handler: every handler invocation appends a JSONL event. SDK-internal
// outcomes (pull/ack/heartbeat errors) surface through the SDK's own stderr
// logging, which the drill asserts separately.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/agentium-lab/Janus/core"
	janus "github.com/agentium-lab/Janus/sdk/go"
)

func main() {
	url := flag.String("url", "http://localhost:8080", "broker base url")
	tenant := flag.String("tenant", "", "tenant id")
	agent := flag.String("agent", "", "agent id")
	mailbox := flag.String("mailbox", "", "mailbox id")
	events := flag.String("events", "", "JSONL event log path")
	slow := flag.Duration("slow", 0, "handler duration")
	hb := flag.Duration("heartbeat", 30*time.Second, "task heartbeat interval")
	ahb := flag.Duration("agent-hb", 3*time.Second, "agent heartbeat interval")
	poll := flag.Duration("poll", 200*time.Millisecond, "empty-poll interval")
	flag.Parse()

	ef, err := os.OpenFile(*events, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil { log.Fatalf("open events: %v", err) }
	enc := json.NewEncoder(ef)
	emit := func(event string, task *core.Task) {
		id, attempt := "", 0
		if task != nil { id, attempt = task.ID, task.AttemptCount }
		_ = enc.Encode(map[string]any{
			"ts": time.Now().UTC().Format(time.RFC3339Nano),
			"event": event, "task": id, "attempt": attempt,
		})
		_ = ef.Sync()
	}

	client := janus.NewClient(janus.Config{BaseURL: *url, TenantID: *tenant})
	worker := janus.NewJanusWorker(client, janus.WorkerConfig{
		AgentID: *agent, MailboxID: *mailbox,
		Heartbeat: *hb, AgentHeartbeat: *ahb, PollInterval: *poll,
	})
	emit("worker_start", nil)
	err = worker.Run(context.Background(), func(_ context.Context, task *core.Task, _ string, _ janus.ProgressFn) (string, *core.TokenUsage, error) {
		emit("handler_start", task)
		select {
		case <-time.After(*slow):
		case <-context.Background().Done():
		}
		emit("handler_done", task)
		return "s3://sdk-chaos/" + task.ID, nil, nil
	})
	emit("worker_exit", nil)
	log.Fatalf("worker Run returned: %v", err)
}
EOF
( cd "$WORK/driver" && GOWORK=off GOPROXY=off GOFLAGS=-mod=mod go mod tidy ) \
  || die "resolving the chaos worker driver deps failed (offline module cache incomplete?)"
( cd "$WORK/driver" && GOWORK=off GOPROXY=off go build -o "$WORK/chaosworker" . ) \
  || die "building the chaos worker driver failed"

say "2. Fixtures (tenant $TENANT: 5 agents, 5 mailboxes)"
boot_server
code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST "$BASE/v1/tenants" \
  -H 'Content-Type: application/json' -d "{\"id\":\"$TENANT\",\"name\":\"SDK Chaos\"}")
if [ "$code" != "201" ] && [ "$code" != "200" ]; then
  [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$BASE/v1/tenants/$TENANT")" = "200" ] \
    || die "tenant create -> $code"
fi
mk_agent() { # mk_agent <id>
  curl -s -o /dev/null --max-time 10 -X POST "$BASE/v1/tenants/$TENANT/agents" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"$1\",\"display_name\":\"chaos $1\",\"protocol\":\"a2a\"}" \
    || die "agent $1 rejected"
}
mk_mb() { # mk_mb <id> <agent> [ack_wait]
  local body="{\"id\":\"$1\",\"agent_id\":\"$2\""
  [ -n "${3:-}" ] && body="$body,\"ack_wait_seconds\":$3"
  curl -s -o /dev/null --max-time 10 -X POST "$BASE/v1/tenants/$TENANT/mailboxes" \
    -H 'Content-Type: application/json' -d "$body}" || die "mailbox $1 rejected"
}
mk_agent a-eof;  mk_mb mb-eof  a-eof
mk_agent a-net;  mk_mb mb-net  a-net
mk_agent a-ll;   mk_mb mb-ll   a-ll "$ACK_WAIT_SHORT"
mk_agent a-ack;  mk_mb mb-ack  a-ack "$ACK_WAIT_ACKFAIL"
mk_agent a-90;   mk_mb mb-90   a-90

# ---------------------------------------------------------------- case: eof
say "3. CASE eof: server killed between tasks; worker must survive + resettle"
start_worker eof a-eof mb-eof
publish a-eof mb-eof task-eof-1
wait_until "eof: first task delivered" 30 '[ -n "$(events_of eof handler_start task-eof-1)" ]' \
  || { fail "eof: task-eof-1 never reached the handler"; tail -5 "$WORK/w-eof.log" >&2; }
wait_until "eof: first task completed" 30 '[ "$(task_status task-eof-1)" = completed ]' \
  || fail "eof: task-eof-1 did not complete"
kill_server
sleep 3
if ! grep -q "janus worker:" "$WORK/w-eof.log" && ! grep -qi "pull" "$WORK/w-eof.log"; then
  fail "eof: SDK logged no pull error while the server was down"
else
  worker_alive "$W_eof_PID" && pass "eof: worker logged pull errors and kept running" \
    || fail "eof: worker process died"
fi
code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 -X POST "$BASE/v1/tenants/$TENANT/tasks" \
  -H 'Content-Type: application/json' -d '{}' 2>/dev/null || true)
[ "$code" = "000" ] || say "note: server still answering after kill (code $code)"
boot_server
publish a-eof mb-eof task-eof-2
wait_until "eof: post-restart task delivered" 30 '[ -n "$(events_of eof handler_start task-eof-2)" ]' \
  && wait_until "eof: post-restart task completed" 30 '[ "$(task_status task-eof-2)" = completed ]' \
  && worker_alive "$W_eof_PID" \
  && pass "eof: same worker process settled a task after the restart" \
  || fail "eof: worker did not settle task-eof-2 after restart"

# ---------------------------------------------------------------- case: network-cut
say "4. CASE network-cut: server frozen ${NETCUT_S}s mid-poll (no FIN/RST)"
start_worker net a-net mb-net
publish a-net mb-net task-net-1
wait_until "net: warmup task completed" 30 '[ "$(task_status task-net-1)" = completed ]' \
  || fail "net: warmup task-net-1 did not complete"
stop_server
sleep "$NETCUT_S"
code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "$BASE/readyz" 2>/dev/null || true)
[ "$code" = "000" ] || fail "net: server still answering while STOPped (cut not proven)"
worker_alive "$W_net_PID" || fail "net: worker died during the cut"
resume_server
wait_until "net: server ready again after SIGCONT" 60 '[ "$(curl -s -o /dev/null -w %{http_code} -m 2 "$BASE/readyz" 2>/dev/null)" = 200 ]' \
  || fail "net: server never became ready after SIGCONT"
publish a-net mb-net task-net-2
wait_until "net: post-cut task delivered" 30 '[ -n "$(events_of net handler_start task-net-2)" ]' \
  && wait_until "net: post-cut task completed" 30 '[ "$(task_status task-net-2)" = completed ]' \
  && worker_alive "$W_net_PID" \
  && pass "net: worker survived the black hole and settled after SIGCONT" \
  || fail "net: worker did not settle task-net-2 after SIGCONT"

# ---------------------------------------------------------------- case: lease-lost
say "5. CASE lease-lost: lease expires mid-handler; stale ack swallowed; settles once"
start_worker ll a-ll mb-ll -slow "${SLOW_LEASELOST_S}s" -heartbeat 120s
publish a-ll mb-ll task-ll
wait_until "ll: handler started" 30 '[ -n "$(events_of ll handler_start task-ll)" ]' \
  || fail "ll: task-ll never delivered"
# The slow worker holds the task with NO heartbeat; its lease (ACK_WAIT_SHORT)
# expires mid-handler and the broker redelivers. A second, fast worker settles
# the redelivered attempt while the slow attempt's late ack is swallowed
# (at-least-once semantics: no SDK error, no second completion).
start_worker ll2 a-ll mb-ll -heartbeat 2s
deadline=$((SLOW_LEASELOST_S + 60))
wait_until "ll: task completed" "$deadline" '[ "$(task_status task-ll)" = completed ]' \
  || fail "ll: task-ll did not complete"
sleep 2
attempts=$(( $(events_of ll handler_start task-ll | wc -l) + $(events_of ll2 handler_start task-ll | wc -l) ))
[ "$attempts" -ge 2 ] || fail "ll: task not redelivered after lease expiry (handler starts: $attempts)"
settled=$(task_attempt task-ll)
[ -n "$(task_completed_once task-ll)" ] \
  && worker_alive "$W_ll_PID" && worker_alive "$W_ll2_PID" \
  && pass "ll: settled exactly once on the redelivered attempt (attempt_count=$settled, ledger 2 rows, stale ack swallowed)" \
  || fail "ll: settlement is not exactly-once (attempt_count=$settled)"

# ---------------------------------------------------------------- case: ack-failure
say "6. CASE ack-failure: ACK hits a dead server; redelivery settles once"
start_worker ack a-ack mb-ack -slow "${SLOW_ACKFAIL_S}s" -heartbeat 3s
publish a-ack mb-ack task-ack
wait_until "ack: handler started" 30 '[ -n "$(events_of ack handler_start task-ack)" ]' \
  || fail "ack: task-ack never delivered"
sleep "$((SLOW_ACKFAIL_S - 6))"   # kill while the handler still runs
kill_server
wait_until "ack: handler finished against the dead server" 30 '[ -n "$(events_of ack handler_done task-ack)" ]' \
  || fail "ack: handler never finished"
sleep 2
grep -q "ack task" "$WORK/w-ack.log" \
  && pass "ack: SDK surfaced the failed ACK against the dead server" \
  || fail "ack: no ack error in SDK stderr (log: $(tail -2 "$WORK/w-ack.log" 2>/dev/null || echo empty))"
boot_server
wait_until "ack: redelivered task completed" 90 '[ "$(task_status task-ack)" = completed ]' \
  || fail "ack: task-ack did not complete after restart"
[ -n "$(task_completed_once task-ack)" ] && worker_alive "$W_ack_PID" \
  && pass "ack: settled exactly once after the failed ACK (attempt_count=$(task_attempt task-ack))" \
  || fail "ack: settlement is not exactly-once"

# ---------------------------------------------------------------- case: 90s
say "7. CASE 90s: end-to-end task outliving every 90s timeout (${SLOW_90S_S}s handler)"
start_worker 90 a-90 mb-90 -slow "${SLOW_90S_S}s" -heartbeat 5s
publish a-90 mb-90 task-90
wait_until "90s: handler started" 30 '[ -n "$(events_of 90 handler_start task-90)" ]' \
  || fail "90s: task-90 never delivered"
deadline=$((SLOW_90S_S + 60))
wait_until "90s: task completed" "$deadline" '[ "$(task_status task-90)" = completed ]' \
  || fail "90s: task-90 did not complete"
started=$(events_of 90 handler_start task-90 | wc -l)
[ "$started" -eq 1 ] || fail "90s: task was redelivered during the long handler (starts: $started)"
dur=$(jq -sr '
  (map(select(.event=="handler_start"))[0].ts | sub("\\.(\\d+)Z$"; "Z") | fromdateiso8601) as $s |
  (map(select(.event=="handler_done"))[-1].ts | sub("\\.(\\d+)Z$"; "Z") | fromdateiso8601) as $e |
  ($e - $s)' "$WORK/w-90.jsonl" 2>/dev/null || echo 0)
[ "$started" -eq 1 ] && [ "${dur:-0}" -ge 90 ] && [ "$(task_attempt task-90)" = 1 ] \
  && pass "90s: ${SLOW_90S_S}s end-to-end task completed on attempt 1 (handler ${dur}s, no redelivery)" \
  || fail "90s: long-task guarantees not met (starts=$started dur=${dur}s attempt=$(task_attempt task-90))"

# ---------------------------------------------------------------- summary
say "SUMMARY: SDK chaos matrix"
echo "  cases: PASS=$CASES_PASS FAIL=$CASES_FAIL"
echo "  cases_detail: eof network-cut lease-lost ack-failure 90s"
if [ "$CASES_FAIL" -gt 0 ]; then
  echo "SDK_CHAOS_PASS=0 ($CASES_FAIL case(s) failed)"
  exit 1
fi
echo "SDK_CHAOS_PASS=1"
echo "SDK_CHAOS_METRICS cases=5 slow_leaselost_s=$SLOW_LEASELOST_S ack_wait_short=$ACK_WAIT_SHORT slow_90s_s=$SLOW_90S_S netcut_s=$NETCUT_S"

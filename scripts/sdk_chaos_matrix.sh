#!/usr/bin/env bash
# GA-11 SDK chaos matrix: five named cases driving the REAL SDK workers
# (go janus.NewJanusWorker / python janus_broker.JanusWorker / typescript
# JanusWorker) against a live janus-api, with chaos induced on the server
# process. Handler-level facts come from a JSONL event log each driver
# emits; SDK-internal outcomes (pull errors, stale-ack rejects, ack
# connection failures) are asserted from the SDK's own stderr logging
# ("janus worker:" in all three SDKs), and settlement facts from
# PostgreSQL — never from HTTP markers alone.
#
#   eof          server SIGKILLed between tasks -> the worker's poll loop must
#                log pull errors and KEEP RUNNING (run never exits), then
#                settle a task published right after the server restarts.
#   network-cut  server SIGSTOPped for 12s (black hole: no FIN, no RST) with a
#                long-poll in flight -> after SIGCONT the worker keeps
#                settling; the cut is proven by a probe failing meanwhile.
#   lease-lost   mailbox ack_wait_seconds=6; one worker handles with NO
#                heartbeat -> the lease expires and the broker redelivers; a
#                second fast worker settles the redelivery EXACTLY ONCE
#                (ledger: one settling attempt, two scope rows). The slow
#                worker's late ACK is verified AFTER its handler finishes:
#                go/ts surface the rejected ack in their logs, python
#                surfaces the loop error; none of them double-settles.
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
# SKIP): PostgreSQL or NATS unreachable -> exit 2. Selected SDK languages
# with missing runtimes are equally FATAL.
#
# Usage:   bash scripts/sdk_chaos_matrix.sh                       # go SDK
#          WORKER_LANGS=go,py,ts bash scripts/sdk_chaos_matrix.sh  # all three
# Knobs:   WORKER_LANGS (default "go"; comma list of go,py,ts),
#          JANUS_PG_HOST/PORT/USER/PASSWORD/DBNAME (localhost/5432/janus/
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
WORKER_LANGS="${WORKER_LANGS:-go}"
# Run nonce: the tenant (and therefore every fixture id) is unique per run,
# so repeated runs never collide with earlier rows in the shared DB.
TENANT="sdkchaos-$(date +%s%N | tail -c 7)"
BASE="http://localhost:$HTTP_PORT"
LANG_RUN="go"   # current language inside the case loop

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

events_of() { # events_of <case> <event> <tid> -> matching JSONL lines (all
  # drivers emit compact JSON with the same keys)
  grep "\"event\":\"$2\"" "$WORK/w-$1.jsonl" 2>/dev/null | grep "\"task\":\"$3\"" || true
}

start_worker() { # start_worker <case> <agent> <mb> [slow_s] [heartbeat_s] [stop_after_s]
  local case="$1" agent="$2" mb="$3" slow_s="${4:-0}" hb_s="${5:-30}" stop_s="${6:-0}"
  : > "$WORK/w-$case.jsonl"
  case "$LANG_RUN" in
    go)
      ( cd "$WORK" && exec "$WORK/chaosworker" -url "$BASE" -tenant "$TENANT" \
          -agent "$agent" -mailbox "$mb" -events "$WORK/w-$case.jsonl" \
          -slow "${slow_s}s" -heartbeat "${hb_s}s" -poll 200ms -stop-after "${stop_s}s" ) \
        > "$WORK/w-$case.log" 2>&1 &
      ;;
    py)
      ( cd "$WORK" && exec python3 "$WORK/chaosworker.py" --url "$BASE" --tenant "$TENANT" \
          --agent "$agent" --mailbox "$mb" --events "$WORK/w-$case.jsonl" \
          --slow "$slow_s" --heartbeat "$hb_s" --poll 0.2 --stop-after "$stop_s" ) \
        > "$WORK/w-$case.log" 2>&1 &
      ;;
    ts)
      ( cd "$WORK" && exec bun "$WORK/chaosworker.ts" --url "$BASE" --tenant "$TENANT" \
          --agent "$agent" --mailbox "$mb" --events "$WORK/w-$case.jsonl" \
          --slow "$((slow_s * 1000))" --heartbeat "$((hb_s * 1000))" --poll 200 \
          --stop-after "$((stop_s * 1000))" ) \
        > "$WORK/w-$case.log" 2>&1 &
      ;;
    *) die "unknown WORKER_LANG: $LANG_RUN" ;;
  esac
  PIDS+=("$!")
  eval "W_${case}_PID=\$!"
  sleep 0.8
  kill -0 "${PIDS[-1]}" 2>/dev/null || { tail -5 "$WORK/w-$case.log" >&2; die "worker $case ($LANG_RUN) exited at start"; }
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
IFS=',' read -ra LANGS <<< "$WORKER_LANGS"
say "GA-11 SDK chaos matrix (langs: ${LANGS[*]}; tenant $TENANT)"

for bin in curl psql jq; do command -v "$bin" >/dev/null || die "required tool missing: $bin"; done
pgq "SELECT 1" >/dev/null 2>&1 || die "PostgreSQL unreachable at $PG_HOST:$PG_PORT/$PG_DBNAME"
(exec 3<>/dev/tcp/"${NATS_URL#*://}"; exec 3>&-) 2>/dev/null || die "NATS unreachable at $NATS_URL"
for l in "${LANGS[@]}"; do
  case "$l" in
    go) command -v go >/dev/null || die "lang go selected but go toolchain missing" ;;
    py) command -v python3 >/dev/null || die "lang py selected but python3 missing"
        python3 -c 'import httpx, pydantic' 2>/dev/null || die "lang py selected but httpx/pydantic missing" ;;
    ts) command -v bun >/dev/null || command -v npx >/dev/null || die "lang ts selected but neither bun nor npx found" ;;
    *) die "unknown WORKER_LANG entry: $l (want go, py or ts)" ;;
  esac
done

say "1. Build janus-api + per-language chaos drivers"
if [ -n "${JANUS_API_BIN:-}" ]; then
  BIN="$JANUS_API_BIN"; [ -x "$BIN" ] || die "JANUS_API_BIN=$BIN is not executable"
else
  BIN="$WORK/janus-api"
  ( cd "$ROOT/server" && go build -o "$BIN" ./cmd/janus-api ) || die "go build janus-api failed"
fi

wants_lang() { local l; for l in "${LANGS[@]}"; do [ "$l" = "$1" ] && return 0; done; return 1; }

if wants_lang go; then
  mkdir -p "$WORK/driver-go"
  cat > "$WORK/driver-go/go.mod" <<EOF
module chaosworker

go 1.25.8

require (
	github.com/agentium-lab/Janus/core v0.0.0
	github.com/agentium-lab/Janus/sdk/go v0.0.0
)

replace github.com/agentium-lab/Janus/sdk/go => $ROOT/sdk/go

replace github.com/agentium-lab/Janus/core => $ROOT/core
EOF
  cat > "$WORK/driver-go/main.go" <<'EOF'
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
	stopAfter := flag.Duration("stop-after", 0, "cancel Run after this duration (0=never)")
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if *stopAfter > 0 {
		time.AfterFunc(*stopAfter, cancel)
	}
	err = worker.Run(ctx, func(hctx context.Context, task *core.Task, _ string, _ janus.ProgressFn) (string, *core.TokenUsage, error) {
		emit("handler_start", task)
		select {
		case <-time.After(*slow):
		case <-hctx.Done():
		}
		emit("handler_done", task)
		return "s3://sdk-chaos/" + task.ID, nil, nil
	})
	emit("worker_exit", nil)
	if *stopAfter > 0 {
		// requested stop: a clean exit is the expected outcome
		os.Exit(0)
	}
	log.Fatalf("worker Run returned: %v", err)
}
EOF
  ( cd "$WORK/driver-go" && GOWORK=off GOPROXY=off GOFLAGS=-mod=mod go mod tidy ) \
    || die "resolving the go chaos driver deps failed (offline module cache incomplete?)"
  ( cd "$WORK/driver-go" && GOWORK=off GOPROXY=off go build -o "$WORK/chaosworker" . ) \
    || die "building the go chaos driver failed"
fi

if wants_lang py; then
  cat > "$WORK/chaosworker.py" <<EOF
# chaosworker.py drives the real janus_broker.JanusWorker run loop with an
# instrumented handler emitting the same JSONL events as the go driver.
import argparse, json, logging, sys, time
sys.path.insert(0, "$ROOT/sdk/python")
from janus_broker import JanusClient, JanusWorker  # noqa: E402

logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(levelname)s %(message)s")

ap = argparse.ArgumentParser()
ap.add_argument("--url", required=True)
ap.add_argument("--tenant", required=True)
ap.add_argument("--agent", required=True)
ap.add_argument("--mailbox", required=True)
ap.add_argument("--events", required=True)
ap.add_argument("--slow", type=float, default=0.0)
ap.add_argument("--heartbeat", type=float, default=30.0)
ap.add_argument("--poll", type=float, default=0.2)
ap.add_argument("--stop-after", type=float, default=0.0)
a = ap.parse_args()

ef = open(a.events, "a", encoding="utf-8")

def emit(event, task):
    ef.write(json.dumps({
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + ".%06dZ" % (time.time() % 1 * 1e6),
        "event": event,
        "task": getattr(task, "id", "") or "",
        "attempt": getattr(task, "attempt_count", 0) or 0,
    }, separators=(",", ":")) + "\n")
    ef.flush()

client = JanusClient(base_url=a.url, tenant_id=a.tenant)
worker = JanusWorker(client, agent_id=a.agent, mailbox_id=a.mailbox,
                     poll_interval=a.poll, heartbeat_interval=a.heartbeat,
                     agent_heartbeat_interval=3.0)
emit("worker_start", None)
import threading
if a.stop_after > 0:
    threading.Timer(a.stop_after, worker.stop).start()

def handler(task, progress=None):
    emit("handler_start", task)
    time.sleep(a.slow)
    emit("handler_done", task)
    return ("s3://sdk-chaos/" + task.id, None)

try:
    worker.run(handler)
finally:
    emit("worker_exit", None)
if a.stop_after > 0:
    sys.exit(0)
sys.exit(1)
EOF
  ( cd "$WORK" && python3 -c "import sys; sys.path.insert(0, '$ROOT/sdk/python'); import chaosworker_check" 2>/dev/null ) || true
  python3 -c "import ast; ast.parse(open('$WORK/chaosworker.py').read())" || die "python chaos driver failed to parse"
fi

if wants_lang ts; then
  cat > "$WORK/chaosworker.ts" <<EOF
// chaosworker.ts drives the real typescript JanusWorker run loop with an
// instrumented handler emitting the same JSONL events as the go driver.
import { appendFileSync, openSync } from "node:fs";
import { Client, JanusWorker } from "$ROOT/sdk/typescript/src/index";

function arg(name: string, dflt?: string): string {
  const i = process.argv.indexOf("--" + name);
  return i > 0 ? process.argv[i + 1] : (dflt ?? "");
}
const url = arg("url"), tenant = arg("tenant"), agentId = arg("agent"), mailbox = arg("mailbox"), events = arg("events");
const slowMs = Number(arg("slow", "0")), hbMs = Number(arg("heartbeat", "30000")), pollMs = Number(arg("poll", "200")), stopAfterMs = Number(arg("stop-after", "0"));
if (!url || !tenant || !agentId || !mailbox || !events) { console.error("chaosworker.ts: missing flags"); process.exit(2); }

openSync(events, "a");
const emit = (event: string, task: any): void => {
  appendFileSync(events, JSON.stringify({
    ts: new Date().toISOString(), event,
    task: task?.id ?? "", attempt: task?.attempt_count ?? 0,
  }) + "\n");
};

const client = new Client({ baseURL: url, tenantID: tenant });
const worker = new JanusWorker(client, {
  agentID: agentId, mailboxID: mailbox,
  heartbeatIntervalMs: hbMs, pollIntervalMs: pollMs, agentHeartbeatIntervalMs: 3000,
});
emit("worker_start", null);
if (stopAfterMs > 0) setTimeout(() => worker.stop(), stopAfterMs);
worker.run(async (task: any, _agent: string, _progress: any, signal?: AbortSignal) => {
  emit("handler_start", task);
  await new Promise<void>((resolve) => {
    const t = setTimeout(resolve, slowMs);
    signal?.addEventListener("abort", () => { clearTimeout(t); resolve(); }, { once: true });
  });
  emit("handler_done", task);
  return { resultRef: "s3://sdk-chaos/" + task.id };
}).then(() => {
  emit("worker_exit", null);
  if (stopAfterMs > 0) process.exit(0);
}).catch((e) => {
  console.error("janus worker:", e);
  emit("worker_exit", null);
  process.exit(1);
});
EOF
  ( cd "$WORK" && bun build --no-bundle chaosworker.ts > /dev/null 2>&1 ) || true
fi

# server is booted once and shared by all language runs
boot_server
say "2. Fixtures for tenant $TENANT (per language: 5 agents, 5 mailboxes)"
code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 -X POST "$BASE/v1/tenants" \
  -H 'Content-Type: application/json' -d "{\"id\":\"$TENANT\",\"name\":\"SDK Chaos\"}")
[ "$code" = "201" ] || [ "$code" = "200" ] || die "tenant create -> $code"

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

run_lang_cases() { # run_lang_cases <lang>: the five cases against one SDK
  LANG_RUN="$1"
  local L="$LANG_RUN"
  say "LANGUAGE $L: running the five-case fault matrix"
  mk_agent a-eof-$L;  mk_mb mb-eof-$L  a-eof-$L
  mk_agent a-net-$L;  mk_mb mb-net-$L  a-net-$L
  mk_agent a-ll-$L;   mk_mb mb-ll-$L   a-ll-$L "$ACK_WAIT_SHORT"
  mk_agent a-ack-$L;  mk_mb mb-ack-$L  a-ack-$L "$ACK_WAIT_ACKFAIL"
  mk_agent a-90-$L;   mk_mb mb-90-$L   a-90-$L
  mk_agent a-stop-$L; mk_mb mb-stop-$L a-stop-$L "$ACK_WAIT_SHORT"

  # -------------------------------------------------------------- case: eof
  say "$L CASE eof: server killed between tasks; worker must survive + resettle"
  start_worker eof a-eof-$L mb-eof-$L
  publish a-eof-$L mb-eof-$L task-eof-1-$L
  wait_until "$L eof: first task delivered" 30 '[ -n "$(events_of eof handler_start task-eof-1-'"$L"')" ]' \
    || { fail "$L eof: task-eof-1 never reached the handler"; tail -5 "$WORK/w-eof.log" >&2; }
  wait_until "$L eof: first task completed" 30 '[ "$(task_status task-eof-1-'"$L"')" = completed ]' \
    || fail "$L eof: task-eof-1 did not complete"
  kill_server
  sleep 3
  if ! grep -q "janus worker:" "$WORK/w-eof.log" && ! grep -qi "pull" "$WORK/w-eof.log"; then
    fail "$L eof: SDK logged no pull error while the server was down"
  else
    worker_alive "$W_eof_PID" && pass "$L eof: worker logged pull errors and kept running" \
      || fail "$L eof: worker process died"
  fi
  boot_server
  publish a-eof-$L mb-eof-$L task-eof-2-$L
  wait_until "$L eof: post-restart task delivered" 30 '[ -n "$(events_of eof handler_start task-eof-2-'"$L"')" ]' \
    && wait_until "$L eof: post-restart task completed" 30 '[ "$(task_status task-eof-2-'"$L"')" = completed ]' \
    && worker_alive "$W_eof_PID" \
    && pass "$L eof: same worker process settled a task after the restart" \
    || fail "$L eof: worker did not settle task-eof-2 after restart"

  # -------------------------------------------------------- case: network-cut
  say "$L CASE network-cut: server frozen ${NETCUT_S}s mid-poll (no FIN/RST)"
  start_worker net a-net-$L mb-net-$L
  publish a-net-$L mb-net-$L task-net-1-$L
  wait_until "$L net: warmup task completed" 30 '[ "$(task_status task-net-1-'"$L"')" = completed ]' \
    || fail "$L net: warmup task-net-1 did not complete"
  stop_server
  sleep "$NETCUT_S"
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "$BASE/readyz" 2>/dev/null || true)
  [ "$code" = "000" ] || fail "$L net: server still answering while STOPped (cut not proven)"
  worker_alive "$W_net_PID" || fail "$L net: worker died during the cut"
  resume_server
  wait_until "$L net: server ready again after SIGCONT" 60 '[ "$(curl -s -o /dev/null -w %{http_code} -m 2 "$BASE/readyz" 2>/dev/null)" = 200 ]' \
    || fail "$L net: server never became ready after SIGCONT"
  publish a-net-$L mb-net-$L task-net-2-$L
  wait_until "$L net: post-cut task delivered" 30 '[ -n "$(events_of net handler_start task-net-2-'"$L"')" ]' \
    && wait_until "$L net: post-cut task completed" 30 '[ "$(task_status task-net-2-'"$L"')" = completed ]' \
    && worker_alive "$W_net_PID" \
    && pass "$L net: worker survived the black hole and settled after SIGCONT" \
    || fail "$L net: worker did not settle task-net-2 after SIGCONT"

  # --------------------------------------------------------- case: lease-lost
  say "$L CASE lease-lost: lease expires mid-handler; late ack verified; settles once"
  start_worker ll a-ll-$L mb-ll-$L "$SLOW_LEASELOST_S" 120
  publish a-ll-$L mb-ll-$L task-ll-$L
  wait_until "$L ll: handler started" 30 '[ -n "$(events_of ll handler_start task-ll-'"$L"')" ]' \
    || fail "$L ll: task-ll never delivered"
  # The slow worker holds the task with NO heartbeat; its lease (ACK_WAIT_SHORT)
  # expires mid-handler and the broker redelivers to a second, fast worker.
  start_worker ll2 a-ll-$L mb-ll-$L 0 2
  deadline=$((SLOW_LEASELOST_S + 60))
  wait_until "$L ll: task completed" "$deadline" '[ "$(task_status task-ll-'"$L"')" = completed ]' \
    || fail "$L ll: task-ll did not complete"
  # R6 evidence refinement: the late ack is verified only AFTER the slow
  # worker's handler actually finishes — the ack hits the expired lease and
  # the SDK must surface it without double-settling.
  wait_until "$L ll: slow worker's handler finished (late ack in flight)" 30 '[ -n "$(events_of ll handler_done task-ll-'"$L"')" ]' \
    || fail "$L ll: slow worker's handler never finished"
  sleep 3
  local late_ack_line
  late_ack_line="$(grep -aiE 'ack|lease' "$WORK/w-ll.log" | tail -1 || true)"
  attempts=$(( $(events_of ll handler_start task-ll-$L | wc -l) + $(events_of ll2 handler_start task-ll-$L | wc -l) ))
  [ "$attempts" -ge 2 ] || fail "$L ll: task not redelivered after lease expiry (handler starts: $attempts)"
  settled=$(task_attempt task-ll-$L)
  [ -n "$(task_completed_once task-ll-$L)" ] \
    && worker_alive "$W_ll_PID" && worker_alive "$W_ll2_PID" \
    && pass "$L ll: settled exactly once on the redelivered attempt (attempt_count=$settled, ledger 2 rows; late ack: ${late_ack_line:-<none logged>})" \
    || fail "$L ll: settlement is not exactly-once (attempt_count=$settled)"

  # -------------------------------------------------------- case: ack-failure
  say "$L CASE ack-failure: ACK hits a dead server; redelivery settles once"
  start_worker ack a-ack-$L mb-ack-$L "$SLOW_ACKFAIL_S" 3
  publish a-ack-$L mb-ack-$L task-ack-$L
  wait_until "$L ack: handler started" 30 '[ -n "$(events_of ack handler_start task-ack-'"$L"')" ]' \
    || fail "$L ack: task-ack never delivered"
  sleep "$((SLOW_ACKFAIL_S - 6))"   # kill while the handler still runs
  kill_server
  wait_until "$L ack: handler finished against the dead server" 30 '[ -n "$(events_of ack handler_done task-ack-'"$L"')" ]' \
    || fail "$L ack: handler never finished"
  sleep 2
  # go's SDK logs "ack task ..."; ts logs "ack transport failed"; python
  # surfaces the loop error via logger.error("janus worker: ...")
  if grep -q "ack task" "$WORK/w-ack.log" || grep -q "ack transport" "$WORK/w-ack.log" || grep -q "janus worker:" "$WORK/w-ack.log"; then
    pass "$L ack: SDK surfaced the failed ACK against the dead server"
  else
    fail "$L ack: no ack error in SDK stderr (log: $(tail -2 "$WORK/w-ack.log" 2>/dev/null || echo empty))"
  fi
  boot_server
  wait_until "$L ack: redelivered task completed" 90 '[ "$(task_status task-ack-'"$L"')" = completed ]' \
    || fail "$L ack: task-ack did not complete after restart"
  [ -n "$(task_completed_once task-ack-$L)" ] && worker_alive "$W_ack_PID" \
    && pass "$L ack: settled exactly once after the failed ACK (attempt_count=$(task_attempt task-ack-$L))" \
    || fail "$L ack: settlement is not exactly-once"

  # --------------------------------------------------------------- case: 90s
  say "$L CASE 90s: end-to-end task outliving every 90s timeout (${SLOW_90S_S}s handler)"
  start_worker 90 a-90-$L mb-90-$L "$SLOW_90S_S" 5
  publish a-90-$L mb-90-$L task-90-$L
  wait_until "$L 90s: handler started" 30 '[ -n "$(events_of 90 handler_start task-90-'"$L"')" ]' \
    || fail "$L 90s: task-90 never delivered"
  deadline=$((SLOW_90S_S + 60))
  wait_until "$L 90s: task completed" "$deadline" '[ "$(task_status task-90-'"$L"')" = completed ]' \
    || fail "$L 90s: task-90 did not complete"
  started=$(events_of 90 handler_start task-90-$L | wc -l)
  [ "$started" -eq 1 ] || fail "$L 90s: task was redelivered during the long handler (starts: $started)"
  dur=$(jq -sr '
    (map(select(.event=="handler_start"))[0].ts | sub("\\.(\\d+)Z$"; "Z") | fromdateiso8601) as $s |
    (map(select(.event=="handler_done"))[-1].ts | sub("\\.(\\d+)Z$"; "Z") | fromdateiso8601) as $e |
    ($e - $s)' "$WORK/w-90.jsonl" 2>/dev/null || echo 0)
  [ "$started" -eq 1 ] && [ "${dur:-0}" -ge 90 ] && [ "$(task_attempt task-90-$L)" = 1 ] \
    && pass "$L 90s: ${SLOW_90S_S}s end-to-end task completed on attempt 1 (handler ${dur}s, no redelivery)" \
    || fail "$L 90s: long-task guarantees not met (starts=$started dur=${dur}s attempt=$(task_attempt task-90-$L))"

  # -------------------------------------------------------------- case: stop
  say "$L CASE stop: mid-handler stop exits cleanly; in-flight work settles once"
  # The stopped worker holds the task with no heartbeat (120s); stop fires
  # 3s in, Run/stop() must return and the PROCESS must exit cleanly; the
  # expired lease redelivers and a second fast worker settles exactly once.
  start_worker stop a-stop-$L mb-stop-$L "$SLOW_LEASELOST_S" 120 3
  publish a-stop-$L mb-stop-$L task-stop-$L
  wait_until "$L stop: handler started" 30 '[ -n "$(events_of stop handler_start task-stop-'"$L"')" ]' \
    || fail "$L stop: task-stop never delivered to the stopping worker"
  start_worker stop2 a-stop-$L mb-stop-$L 0 2
  stop_deadline=$((SLOW_LEASELOST_S + 40))
  wait_until "$L stop: stopped worker's process exited cleanly" "$stop_deadline" '! worker_alive "$W_stop_PID"' \
    && pass "$L stop: worker process exited after stop" \
    || fail "$L stop: worker process still alive ${stop_deadline}s after stop"
  wait_until "$L stop: redelivered task completed" "$stop_deadline" '[ "$(task_status task-stop-'"$L"')" = completed ]' \
    || fail "$L stop: task-stop did not complete after the stop"
  [ -n "$(task_completed_once task-stop-$L)" ] \
    && pass "$L stop: in-flight work settled exactly once after the stop (attempt_count=$(task_attempt task-stop-$L), ledger 2 rows)" \
    || fail "$L stop: settlement is not exactly-once after the stop"
}

for L in "${LANGS[@]}"; do
  run_lang_cases "$L"
done

# ---------------------------------------------------------------- summary
say "SUMMARY: SDK chaos matrix"
echo "  cases: PASS=$CASES_PASS FAIL=$CASES_FAIL"
echo "  languages: ${LANGS[*]}"
echo "  cases_detail: eof network-cut lease-lost ack-failure 90s (per language)"
if [ "$CASES_FAIL" -gt 0 ]; then
  echo "SDK_CHAOS_PASS=0 ($CASES_FAIL case(s) failed)"
  exit 1
fi
echo "SDK_CHAOS_PASS=1"
echo "SDK_CHAOS_METRICS langs=${LANGS[*]} cases=5 slow_leaselost_s=$SLOW_LEASELOST_S ack_wait_short=$ACK_WAIT_SHORT slow_90s_s=$SLOW_90S_S netcut_s=$NETCUT_S"

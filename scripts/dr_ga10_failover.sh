#!/usr/bin/env bash
# GA-10 HA failover drill: proves the confirmed recovery targets on a real
# PostgreSQL primary/standby pair, not on paper.
#
#   RPO = 0      synchronous_commit=on with synchronous_standby_names
#                'ANY 1 (ga10_standby)': a write the API acknowledged (HTTP
#                201) is on the standby before the commit returns. The drill
#                asserts EVERY acknowledged task id survives an immediate
#                primary crash, and prints the sync_state it verified first.
#   RTO <= 120s  measured from the primary's death to the API answering
#                /readyz AND completing a create->pull->ack round trip on the
#                promoted standby (the repoint is part of recovery: in k8s the
#                Service or a proxy moves the endpoint).
#
# Reconciliation covers the six durable objects: tasks, task_attempts,
# outbox_events, budget_usage_ledger, audit_event_projection, and the
# dead_lettered task count (this schema has no DLQ table).
#
# The drill runs the API in PG-queue mode, so it needs no NATS and no Redis.
# An unreachable dependency or a missing PostgreSQL tool is FATAL (exit 2),
# never a green SKIP.
#
# Usage: bash scripts/dr_ga10_failover.sh
# Knobs: PG_BINDIR, GA10_PRIMARY_PORT (55432), GA10_STANDBY_PORT (55433),
#        GA10_API_PORT (18210), GA10_LOAD_SECONDS (10), GA10_LOAD_WRITERS (4),
#        GA10_RTO_BUDGET_S (120), JANUS_API_BIN.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PRIMARY_PORT="${GA10_PRIMARY_PORT:-55432}"
STANDBY_PORT="${GA10_STANDBY_PORT:-55433}"
API_PORT="${GA10_API_PORT:-18210}"
LOAD_SECONDS="${GA10_LOAD_SECONDS:-10}"
LOAD_WRITERS="${GA10_LOAD_WRITERS:-4}"
RTO_BUDGET="${GA10_RTO_BUDGET_S:-120}"
DBNAME=janus_dr
TENANT=ga10-dr
AGENT=ga10-agent
MB=ga10-mb

say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
die() { printf '\033[0;31mFATAL: %s\033[0m\n' "$*" >&2; exit 2; }

# ------------------------------------------------------------ PG toolchain
find_pgbindir() {
  local c
  for c in "${PG_BINDIR:-}" "$HOME/.local/pgsql/bin" \
           "$(dirname "$(command -v pg_ctl 2>/dev/null || echo /nonexistent)")" \
           /usr/lib/postgresql/*/bin; do
    [ -n "$c" ] || continue
    if [ -x "$c/initdb" ] && [ -x "$c/pg_ctl" ] && [ -x "$c/pg_basebackup" ] && [ -x "$c/psql" ]; then
      printf '%s' "$c"; return 0
    fi
  done
  return 1
}
PGB="$(find_pgbindir)" || die "no PostgreSQL toolchain with initdb/pg_ctl/pg_basebackup/psql (set PG_BINDIR)"
say "PostgreSQL toolchain: $PGB ($("$PGB/postgres" --version))"

WORK="$(mktemp -d /tmp/ga10-failover.XXXXXX)"
SOCK="$WORK/sock"; mkdir -p "$SOCK" "$WORK/archive"
PRIMARY_PID=""; STANDBY_PID=""; API_PID=""
cleanup() {
  [ -n "$API_PID" ] && kill "$API_PID" 2>/dev/null || true
  for d in "$WORK/primary" "$WORK/standby"; do
    [ -d "$d" ] && "$PGB/pg_ctl" -D "$d" stop -m immediate >/dev/null 2>&1 || true
  done
  if [ "${KEEP_WORK:-0}" = "1" ]; then
    echo "KEEP_WORK=1: workdir retained at $WORK"
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

psqlp() { # psqlp <port> <sql>
  PGPASSWORD=janus "$PGB/psql" -h 127.0.0.1 -p "$1" -U janus -d "$DBNAME" -tAX -c "$2"
}
psqlm() { # psqlm <port> <sql> against the maintenance db
  PGPASSWORD=janus "$PGB/psql" -h 127.0.0.1 -p "$1" -U janus -d postgres -tAX -c "$2"
}
wait_port() { # wait_port <port> <seconds>
  local i
  for i in $(seq 1 "$2"); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
    sleep 1
  done
  return 1
}

# ------------------------------------------------------------------ primary
say "1. Primary cluster on :$PRIMARY_PORT (synchronous_commit=on, ANY 1 standby)"
"$PGB/initdb" -U janus --auth=trust -D "$WORK/primary" >"$WORK/initdb.log" 2>&1 \
  || { tail -5 "$WORK/initdb.log"; die "initdb failed"; }
cat >> "$WORK/primary/postgresql.conf" <<EOF
port = $PRIMARY_PORT
listen_addresses = 'localhost'
unix_socket_directories = '$SOCK'
wal_level = replica
max_wal_senders = 5
max_replication_slots = 5
wal_keep_size = 512MB
synchronous_commit = on
hot_standby = on
EOF
"$PGB/pg_ctl" -D "$WORK/primary" -l "$WORK/primary.log" -w start >/dev/null \
  || { tail -10 "$WORK/primary.log"; die "primary did not start"; }
wait_port "$PRIMARY_PORT" 30 || die "primary port $PRIMARY_PORT never opened"
psqlm "$PRIMARY_PORT" "CREATE ROLE ga10_repl LOGIN REPLICATION" >/dev/null
psqlm "$PRIMARY_PORT" "CREATE DATABASE $DBNAME OWNER janus" >/dev/null
echo "  primary up; database $DBNAME created"

# ------------------------------------------------------------------ standby
say "2. Streaming standby on :$STANDBY_PORT via pg_basebackup"
"$PGB/pg_basebackup" -h 127.0.0.1 -p "$PRIMARY_PORT" -U ga10_repl -D "$WORK/standby" \
  -X stream -c fast >"$WORK/basebackup.log" 2>&1 \
  || { tail -5 "$WORK/basebackup.log"; die "pg_basebackup failed"; }
touch "$WORK/standby/standby.signal"
cat >> "$WORK/standby/postgresql.auto.conf" <<EOF
primary_conninfo = 'host=127.0.0.1 port=$PRIMARY_PORT user=ga10_repl application_name=ga10_standby'
port = $STANDBY_PORT
unix_socket_directories = '$SOCK'
EOF
"$PGB/pg_ctl" -D "$WORK/standby" -l "$WORK/standby.log" -w start >/dev/null \
  || { tail -10 "$WORK/standby.log"; die "standby did not start"; }
wait_port "$STANDBY_PORT" 30 || die "standby port $STANDBY_PORT never opened"

# Enable synchronous replication only now that the standby is streaming: with
# synchronous_standby_names set on a primary that has no standby yet, every
# COMMIT (including the fixture writes) blocks forever waiting for a sync ack.
psqlm "$PRIMARY_PORT" "ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 (ga10_standby)'" >/dev/null
psqlm "$PRIMARY_PORT" "SELECT pg_reload_conf()" >/dev/null

SYNC_STATE=""
for _ in $(seq 1 30); do
  SYNC_STATE="$(psqlp "$PRIMARY_PORT" "SELECT coalesce(sync_state,'') FROM pg_stat_replication WHERE application_name='ga10_standby'" 2>/dev/null | tail -1)"
  [ "$SYNC_STATE" = "quorum" ] && break
  sleep 1
done
# ANY 1 (quorum commit) members report sync_state='quorum' in pg_stat_replication
[ "$SYNC_STATE" = "quorum" ] || die "standby never joined the sync quorum (sync_state='$SYNC_STATE') - RPO=0 would be unproven"
echo "  pg_stat_replication.sync_state = $SYNC_STATE  (a commit returns only after the standby confirms it)"

# --------------------------------------------------------------------- API
say "3. Boot janus-api against the primary (PG queue mode, auto-migrate)"
if [ -n "${JANUS_API_BIN:-}" ]; then
  BIN="$JANUS_API_BIN"; [ -x "$BIN" ] || die "JANUS_API_BIN=$BIN not executable"
else
  BIN="$WORK/janus-api"
  ( cd "$ROOT/server" && go build -o "$BIN" ./cmd/janus-api ) || die "go build ./cmd/janus-api failed"
fi
boot_api() { # boot_api <pg_port> <migrate:true|false> <logfile>
  local holder
  holder="$(ss -ltnp 2>/dev/null | awk -v p=":$API_PORT " '$4 ~ p {print $0}' | grep -o 'pid=[0-9]*' | head -1)"
  [ -z "$holder" ] || die "port $API_PORT already held ($holder) — a stale drill API survived; kill it before rerunning"
  local pgport="$1" migrate="$2" log="$3"
  ( cd "$WORK" && \
    JANUS_QUEUE_DRIVER=pg \
    JANUS_PG_HOST=127.0.0.1 JANUS_PG_PORT="$pgport" JANUS_PG_USER=janus \
    JANUS_PG_PASSWORD=janus JANUS_PG_DATABASE="$DBNAME" JANUS_PG_SSLMODE=disable \
    JANUS_REDIS_ADDR=localhost:59999 \
    JANUS_MIGRATION_AUTO="$migrate" JANUS_MIGRATION_PATH="$ROOT/migrations" \
    JANUS_AUTH_ENABLED=false \
    JANUS_HTTP_HOST=127.0.0.1 JANUS_HTTP_PORT="$API_PORT" JANUS_GRPC_PORT=19210 \
    exec "$BIN" >"$log" 2>&1 ) &
  API_PID=$!
  local i
  for i in $(seq 1 60); do
    curl -sf -m 2 "http://127.0.0.1:$API_PORT/readyz" >/dev/null 2>&1 && return 0
    sleep 1
  done
  tail -15 "$log"
  return 1
}
boot_api "$PRIMARY_PORT" true "$WORK/api-primary.log" || die "API never became ready against the primary"
echo "  API ready on :$API_PORT (migrated schema: $(psqlp "$PRIMARY_PORT" "SELECT version FROM schema_migrations") )"

# ----------------------------------------------------------------- fixtures
say "4. Fixtures + sustained acknowledged writes ($LOAD_WRITERS writers, ${LOAD_SECONDS}s)"
api() { # api <method> <path> [json]
  if [ $# -ge 3 ]; then
    curl -s -m 10 -o "$WORK/last-body" -w '%{http_code}' -X "$1" "http://127.0.0.1:$API_PORT$2" \
      -H 'Content-Type: application/json' -d "$3"
  else
    curl -s -m 10 -o "$WORK/last-body" -w '%{http_code}' -X "$1" "http://127.0.0.1:$API_PORT$2"
  fi
}
code="$(api POST "/v1/tenants" "{\"id\":\"$TENANT\",\"name\":\"GA10 DR\"}")"
case "$code" in 200|201|400) ;; *) die "tenant create -> $code $(cat "$WORK/last-body")" ;; esac
code="$(api POST "/v1/tenants/$TENANT/agents" "{\"id\":\"$AGENT\",\"display_name\":\"DR\",\"protocol\":\"a2a\"}")"
case "$code" in 200|201|400) ;; *) die "agent register -> $code" ;; esac
code="$(api POST "/v1/tenants/$TENANT/mailboxes" "{\"id\":\"$MB\",\"agent_id\":\"$AGENT\"}")"
case "$code" in 200|201|400) ;; *) die "mailbox create -> $code" ;; esac

task_body() { # task_body <id>
  printf '{"id":"%s","source_agent":"%s","target_type":"mailbox","target_value":"%s","envelope":{"janus_version":"0.3","task_id":"%s","tenant_id":"%s","source_agent":"%s","target":{"type":"mailbox","value":"%s"},"payload":{"type":"dr","content":"ga10"},"trace":{"trace_id":"dr-%s"}}}' \
    "$1" "$AGENT" "$MB" "$1" "$TENANT" "$AGENT" "$MB" "$1"
}
LOAD_START=$(date +%s)
writer() { # writer <wid>
  local wid="$1" i=0 tid c
  : > "$WORK/acked-$wid"
  while [ $(( $(date +%s) - LOAD_START )) -lt "$LOAD_SECONDS" ]; do
    i=$(( i + 1 ))
    tid="dr-$wid-$(printf '%06d' "$i")"
    c="$(api POST "/v1/tenants/$TENANT/tasks" "$(task_body "$tid")")"
    if [ "$c" = 201 ] || [ "$c" = 200 ]; then echo "$tid" >> "$WORK/acked-$wid"; fi
  done
}
WPIDS=()
for w in $(seq 1 "$LOAD_WRITERS"); do writer "$w" & WPIDS+=("$!"); done
# wait ONLY for the writers: a bare `wait` also blocks on the API subshell
# (a background job that never exits) and deadlocks the drill
wait "${WPIDS[@]}"
cat "$WORK"/acked-* > "$WORK/acked.txt"
ACKED=$(grep -c . "$WORK/acked.txt" || true)
[ "$ACKED" -gt 0 ] || die "no acknowledged writes - nothing to prove RPO against"
echo "  acknowledged (HTTP 201) tasks: $ACKED"

# A few full lifecycles so attempts / ledger / audit / dead_lettered have rows.
# A few full lifecycles so attempts / ledger / audit / dead_lettered have rows.
# The pull may return ANY of the writer tasks queued on this mailbox, so the
# task id comes from the pull response itself — never assume it is the newest.
settle_one() {
  local lease tid
  api POST "/v1/tenants/$TENANT/mailboxes/$MB/pull" "{\"agent_id\":\"$AGENT\"}" >/dev/null
  tid="$(python3 -c "import json;d=json.load(open('$WORK/last-body'));print(d.get('task',{}).get('id',''))" 2>/dev/null || true)"
  lease="$(python3 -c "import json;d=json.load(open('$WORK/last-body'));print((d.get('lease') or {}).get('lease_id',''))" 2>/dev/null || true)"
  [ -n "$lease" ] && [ -n "$tid" ] || return 0
  api POST "/v1/tenants/$TENANT/tasks/$tid/start" "{\"lease_id\":\"$lease\",\"agent_id\":\"$AGENT\"}" >/dev/null
  api POST "/v1/tenants/$TENANT/tasks/$tid/ack" "{\"lease_id\":\"$lease\",\"agent_id\":\"$AGENT\",\"result_ref\":\"ga10://$tid\"}" >/dev/null
}
for i in 1 2 3 4 5; do settle_one; done
echo "  settled lifecycles: $(psqlp "$PRIMARY_PORT" "SELECT count(*) FROM tasks WHERE tenant_id='$TENANT' AND status='completed'")"

snapshot() { # snapshot <port> <label>
  local p="$1" label="$2"
  printf '%s\n' "$label"
  for q in \
    "tasks|SELECT count(*) FROM tasks WHERE tenant_id='$TENANT'" \
    "task_attempts|SELECT count(*) FROM task_attempts WHERE tenant_id='$TENANT'" \
    "outbox_events|SELECT count(*) FROM outbox_events WHERE tenant_id='$TENANT'" \
    "budget_usage_ledger|SELECT count(*) FROM budget_usage_ledger WHERE tenant_id='$TENANT'" \
    "audit_event_projection|SELECT count(*) FROM audit_event_projection WHERE tenant_id='$TENANT'" \
    "dead_lettered|SELECT count(*) FROM tasks WHERE tenant_id='$TENANT' AND status='dead_lettered'"; do
    printf '  %-24s %s\n' "${q%%|*}" "$(psqlp "$p" "${q#*|}")"
  done
}
say "5. Pre-crash snapshot read FROM THE STANDBY (proves it had caught up)"
snapshot "$STANDBY_PORT" "standby :$STANDBY_PORT (pre-crash)" > "$WORK/snap-before.txt"
cat "$WORK/snap-before.txt"
BEFORE=$(awk '/^  [a-z_]+ /{print $1" "$2}' "$WORK/snap-before.txt")

# ------------------------------------------------------------- crash+promote
say "6. Kill the primary (immediate) and promote the standby - RTO clock starts"
T0=$(date +%s%N)
"$PGB/pg_ctl" -D "$WORK/primary" stop -m immediate >/dev/null 2>&1 || true
"$PGB/pg_ctl" -D "$WORK/standby" -l "$WORK/standby.log" -w promote >/dev/null \
  || { tail -5 "$WORK/standby.log"; die "standby promotion failed"; }
for _ in $(seq 1 60); do
  [ "$(psqlp "$STANDBY_PORT" "SELECT pg_is_in_recovery()" 2>/dev/null | tail -1)" = "f" ] && break
  sleep 1
done
[ "$(psqlp "$STANDBY_PORT" "SELECT pg_is_in_recovery()" | tail -1)" = "f" ] || die "standby never left recovery"
PROMOTED_S=$(( ( $(date +%s%N) - T0 ) / 1000000000 ))
echo "  standby promoted and writable after ${PROMOTED_S}s"

say "7. Repoint the API at the promoted standby and prove a full round trip"
[ -n "$API_PID" ] && kill "$API_PID" 2>/dev/null || true
wait "$API_PID" 2>/dev/null || true
API_PID=""
boot_api "$STANDBY_PORT" false "$WORK/api-standby.log" || die "API never became ready against the promoted standby"
ROUNDTRIP_TID="dr-post-failover-1"
code="$(api POST "/v1/tenants/$TENANT/tasks" "$(task_body "$ROUNDTRIP_TID")")"
[ "$code" = 201 ] || [ "$code" = 200 ] || die "post-failover create -> $code $(cat "$WORK/last-body")"
api POST "/v1/tenants/$TENANT/mailboxes/$MB/pull" "{\"agent_id\":\"$AGENT\"}" >/dev/null
LEASE="$(python3 -c "import json;d=json.load(open('$WORK/last-body'));print((d.get('lease') or {}).get('lease_id',''))" 2>/dev/null || true)"
PULLED="$(python3 -c "import json;d=json.load(open('$WORK/last-body'));print(d.get('task',{}).get('id',''))" 2>/dev/null || true)"
[ -n "$LEASE" ] && [ -n "$PULLED" ] || die "post-failover pull returned no lease: $(cat "$WORK/last-body")"
api POST "/v1/tenants/$TENANT/tasks/$PULLED/start" "{\"lease_id\":\"$LEASE\",\"agent_id\":\"$AGENT\"}" >/dev/null
code="$(api POST "/v1/tenants/$TENANT/tasks/$PULLED/ack" "{\"lease_id\":\"$LEASE\",\"agent_id\":\"$AGENT\",\"result_ref\":\"ga10://post-failover\"}")"
[ "$code" = 200 ] || die "post-failover ack ($PULLED) -> $code $(cat "$WORK/last-body")"
T1=$(date +%s%N)
RTO_S=$(( (T1 - T0) / 1000000000 ))
RTO_MS=$(( (T1 - T0) / 1000000 ))
echo "  create -> pull -> start -> ack succeeded on the promoted standby"

# ------------------------------------------------------------------- gates
say "8. Gates"
FAILS=0
MISSING=$(PGPASSWORD=janus "$PGB/psql" -h 127.0.0.1 -p "$STANDBY_PORT" -U janus -d "$DBNAME" -tA 2>/dev/null <<SQL | tail -1
CREATE TEMP TABLE ga10_acked(id text);
\\copy ga10_acked FROM '$WORK/acked.txt'
SELECT count(*) FROM ga10_acked a LEFT JOIN tasks t ON t.tenant_id='$TENANT' AND t.id=a.id WHERE t.id IS NULL;
SQL
)
if [ "$MISSING" = "0" ]; then
  echo "  PASS rpo_zero: all $ACKED acknowledged writes survived the crash (missing=0)"
else
  echo "  FAIL rpo_zero: $MISSING of $ACKED acknowledged writes are MISSING after promotion"
  FAILS=$(( FAILS + 1 ))
fi
if [ "$RTO_S" -le "$RTO_BUDGET" ]; then
  echo "  PASS rto_within_budget: ${RTO_S}s (${RTO_MS}ms) <= ${RTO_BUDGET}s"
else
  echo "  FAIL rto_within_budget: ${RTO_S}s > ${RTO_BUDGET}s"
  FAILS=$(( FAILS + 1 ))
fi

snapshot "$STANDBY_PORT" "promoted :$STANDBY_PORT (post-failover)" > "$WORK/snap-after.txt"
AFTER=$(awk '/^  [a-z_]+ /{print $1" "$2}' "$WORK/snap-after.txt")
echo "  reconciliation (before -> after, no object may lose rows):"
while read -r name before_n; do
  after_n="$(awk -v n="$name" '$1==n{print $2}' <<<"$AFTER")"
  if [ "${after_n:-0}" -ge "$before_n" ]; then
    printf '    PASS %-24s %s -> %s\n' "$name" "$before_n" "$after_n"
  else
    printf '    FAIL %-24s %s -> %s (rows lost)\n' "$name" "$before_n" "$after_n"
    FAILS=$(( FAILS + 1 ))
  fi
done <<<"$BEFORE"

echo ""
if [ "$FAILS" -eq 0 ]; then
  echo "GA10_FAILOVER: PASS rpo_missing=0 acked=$ACKED rto_seconds=$RTO_S sync_state=$SYNC_STATE objects=6/6"
else
  echo "GA10_FAILOVER: FAIL failures=$FAILS rpo_missing=$MISSING acked=$ACKED rto_seconds=$RTO_S sync_state=$SYNC_STATE"
  exit 1
fi

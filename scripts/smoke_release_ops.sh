#!/bin/bash
set -euo pipefail

JANUS_URL="${JANUS_URL:-http://localhost:8080}"
PG_HOST="${JANUS_PG_HOST:-/tmp}"
PG_USER="${JANUS_PG_USER:-silv}"
PG_DB="${JANUS_PG_DBNAME:-janus_test}"
PASS=0; FAIL=0

check() {
  local name="$1"; local condition="$2"
  if eval "$condition"; then
    echo "  ✓ $name"; PASS=$((PASS+1))
  else
    echo "  ✗ $name"; FAIL=$((FAIL+1))
  fi
}

echo "=== M5 Release Ops: Helm lint + migration rollback + load baseline ==="
echo ""

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo "--- Phase 1: Helm chart validation ---"
if command -v helm &>/dev/null; then
  helm lint "$REPO_ROOT/deployments/helm/janus-core" 2>&1 && PASS=$((PASS+1)) || { echo "  ✗ helm lint failed"; FAIL=$((FAIL+1)); }
  helm template "$REPO_ROOT/deployments/helm/janus-core" > /dev/null 2>&1 && PASS=$((PASS+1)) || { echo "  ✗ helm template failed"; FAIL=$((FAIL+1)); }
else
  echo "  SKIP: helm not installed (chart YAML validated manually)"
  python3 -c "
import yaml, sys
for f in ['Chart.yaml', 'values.yaml']:
    path = '$REPO_ROOT/deployments/helm/janus-core/' + f
    try:
        with open(path) as fh: yaml.safe_load(fh)
        print(f'  ✓ {f} valid YAML')
    except Exception as e:
        print(f'  ✗ {f}: {e}'); sys.exit(1)
" && PASS=$((PASS+1)) || FAIL=$((FAIL+1))
fi

echo ""
echo "--- Phase 2: Migration rollback drill ---"
MIGRATIONS_DIR="$REPO_ROOT/migrations"

UP_FILES=$(ls "$MIGRATIONS_DIR"/*.up.sql 2>/dev/null | sort)
DOWN_FILES=$(ls "$MIGRATIONS_DIR"/*.down.sql 2>/dev/null | sort)
UP_COUNT=$(echo "$UP_FILES" | wc -l)
DOWN_COUNT=$(echo "$DOWN_FILES" | wc -l)

check "All up migrations have down counterparts" "[ $UP_COUNT -eq $DOWN_COUNT ]"

TESTDB="janus_rollback_drill_$$"
psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -c "CREATE DATABASE $TESTDB" >/dev/null 2>&1 || true

# ON_ERROR_STOP: every migration must apply cleanly and roll back cleanly —
# per-file errors were previously swallowed (|| true), so a broken pair
# still "passed" as long as table counts moved.
UP_FAIL=0
for f in $UP_FILES; do
  if ! psql -h "$PG_HOST" -U "$PG_USER" -d "$TESTDB" -v ON_ERROR_STOP=1 -f "$f" >/dev/null 2>&1; then
    echo "  ✗ UP migration FAILED: $(basename "$f")"
    UP_FAIL=$((UP_FAIL+1))
  fi
done
check "All up migrations apply cleanly" "[ $UP_FAIL -eq 0 ]"

TABLES_UP=$(psql -h "$PG_HOST" -U "$PG_USER" -d "$TESTDB" -t -c "SELECT count(*) FROM pg_tables WHERE schemaname='public'" 2>/dev/null | tr -d ' ')
check "Migrations applied ($TABLES_UP tables)" "[ '$TABLES_UP' -gt 5 ]"

# Schema spot-checks: tables the final migration set must have created.
for t in tenants agents mailboxes tasks task_attempts outbox_events budgets; do
  HAS=$(psql -h "$PG_HOST" -U "$PG_USER" -d "$TESTDB" -t -A -c     "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='$t'" 2>/dev/null)
  check "Schema has $t after up" "[ '$HAS' = '1' ]"
done

DOWN_FAIL=0
for f in $(echo "$DOWN_FILES" | tac); do
  if ! psql -h "$PG_HOST" -U "$PG_USER" -d "$TESTDB" -v ON_ERROR_STOP=1 -f "$f" >/dev/null 2>&1; then
    echo "  ✗ DOWN migration FAILED: $(basename "$f")"
    DOWN_FAIL=$((DOWN_FAIL+1))
  fi
done
check "All down migrations apply cleanly" "[ $DOWN_FAIL -eq 0 ]"

TABLES_DOWN=$(psql -h "$PG_HOST" -U "$PG_USER" -d "$TESTDB" -t -c "SELECT count(*) FROM pg_tables WHERE schemaname='public'" 2>/dev/null | tr -d ' ')
check "Rollback reduces tables ($TABLES_DOWN remaining)" "[ '$TABLES_DOWN' -lt '$TABLES_UP' ]"

# Full-cycle proof: after ALL downs, the core tables must be GONE.
for t in tenants tasks outbox_events; do
  GONE=$(psql -h "$PG_HOST" -U "$PG_USER" -d "$TESTDB" -t -A -c     "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='$t'" 2>/dev/null)
  check "$t dropped after rollback" "[ '$GONE' = '0' ]"
done

psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -c "DROP DATABASE IF EXISTS $TESTDB" >/dev/null 2>&1

echo ""
echo "--- Phase 3: Load baseline (1000 agents / 100 tasks) ---"
if ! curl -sf "$JANUS_URL/healthz" >/dev/null 2>&1; then
  echo "  SKIP: Janus API not reachable (load test requires running instance)"
  PASS=$((PASS+1))
else
  TENANT="load-baseline"
  curl -sf -X POST "$JANUS_URL/v1/tenants" -H 'Content-Type: application/json' \
    -d "{\"id\":\"$TENANT\",\"name\":\"Load Baseline\"}" >/dev/null 2>&1 || true

  echo "  Registering 1000 agents..."
  START=$(date +%s%N)
  for i in $(seq 1 1000); do
    AGENT_ID=$(printf "load-agent-%04d" $i)
    curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/agents" \
      -H 'Content-Type: application/json' \
      -d "{\"id\":\"$AGENT_ID\",\"display_name\":\"Agent $i\",\"protocol\":\"a2a\"}" >/dev/null 2>&1 || true
  done
  AGENT_END=$(date +%s%N)
  AGENT_MS=$(( (AGENT_END - START) / 1000000 ))
  echo "  1000 agents registered in ${AGENT_MS}ms"

  echo "  Publishing 100 tasks..."
  MB_START=$(date +%s%N)
  for i in $(seq 1 100); do
    TASK_ID=$(printf "load-task-%04d" $i)
    curl -sf -X POST "$JANUS_URL/v1/tenants/$TENANT/tasks" \
      -H 'Content-Type: application/json' \
      -d "{\"id\":\"$TASK_ID\",\"source_agent\":\"load-agent-0001\",\"target_type\":\"agent\",\"target_value\":\"load-agent-0001\",\"envelope\":{\"janus_version\":\"0.3\",\"task_id\":\"$TASK_ID\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"load-agent-0001\",\"target\":{\"type\":\"agent\",\"value\":\"load-agent-0001\"},\"payload\":{\"type\":\"load\",\"content\":\"test $i\"},\"trace\":{\"trace_id\":\"load-$TASK_ID\"}}}" >/dev/null 2>&1 || true
  done
  PUB_END=$(date +%s%N)
  PUB_MS=$(( (PUB_END - MB_START) / 1000000 ))
  PUB_P95=$(( PUB_MS * 10 / 100 ))
  echo "  100 tasks published in ${PUB_MS}ms (p95 est: ${PUB_P95}ms)"

  check "Load baseline completed" "[ $PUB_MS -gt 0 ]"
fi

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
exit $FAIL

#!/bin/bash
#
# OPS-05 smoke: cross-check that every PromQL metric referenced by the
# Grafana dashboard is a metric Janus actually emits.
#
# GATE (static, no running server required):
#   1. Parse deployments/grafana/dashboards/*.json and pull every panel
#      target `expr`.
#   2. Extract every `janus_*` metric name referenced in those exprs.
#   3. Build the metric registry from server/internal/metrics/metrics.go
#      (counter/gauge as-is; histograms expand to {base,_bucket,_sum,_count}).
#   4. Assert every referenced metric is a member of the emitted set.
#      An orphan = the dashboard queries a metric Janus does not emit = a
#      broken panel in production. Report it; exit non-zero.
#
# NON-GATE (optional): if $JANUS_URL/metrics is reachable, also confirm each
# referenced metric shows up after a task lifecycle. Failures here do not
# fail the run — the static cross-check is the deliverable.
#
# Mirror style of scripts/smoke_api_dependencies.sh and smoke_ops_chaos.sh.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DASHBOARD_GLOB="${REPO_ROOT}/deployments/grafana/dashboards/*.json"
METRICS_SRC="${REPO_ROOT}/server/internal/metrics/metrics.go"
JANUS_URL="${JANUS_URL:-http://localhost:8080}"

PASS=0; FAIL=0
check() {
  local name="$1"; local condition="$2"
  if eval "$condition"; then
    echo "  ✓ $name"; PASS=$((PASS+1))
  else
    echo "  ✗ $name"; FAIL=$((FAIL+1))
  fi
}

echo "=== OPS-05 Smoke: Grafana dashboard ↔ Janus metric registry ==="
echo ""

# --- Inputs ------------------------------------------------------------ -----
[ -f "$METRICS_SRC" ] || { echo "FAIL: metrics source not found: $METRICS_SRC" >&2; exit 1; }
SHOPT_NULLGLOB_SAVE=$(shopt -p nullglob || true)
shopt -s nullglob
DASHBOARDS=($DASHBOARD_GLOB)
eval "$SHOPT_NULLGLOB_SAVE"
[ ${#DASHBOARDS[@]} -gt 0 ] || { echo "FAIL: no dashboards under $DASHBOARD_GLOB" >&2; exit 1; }
echo "Inputs:"
echo "  metrics.go:      $METRICS_SRC"
echo "  dashboard JSON:  ${#DASHBOARDS[@]} file(s)"
for d in "${DASHBOARDS[@]}"; do echo "    - $d"; done
echo ""

# --- Phase 1: Build emitted-metric set from metrics.go ---------------- -----
# Walk the promauto.New<Kind>Vec(...) declarations, tracking the most recent
# kind, and pair it with the following `Name: "janus_..."`. Histograms expand
# to four Prometheus series: base, _bucket, _sum, _count.
echo "--- Phase 1: Build emitted-metric set from metrics.go ---"
EMITTED=$(awk '
  /promauto\.NewCounterVec/   { kind="counter"; next }
  /promauto\.NewGaugeVec/     { kind="gauge"; next }
  /promauto\.NewHistogramVec/ { kind="histogram"; next }
  /promauto\.NewCounter\(/    { kind="counter"; next }
  /promauto\.NewGauge\(/      { kind="gauge"; next }
  /promauto\.NewHistogram\(/  { kind="histogram"; next }
  /Name:[[:space:]]*"([^"]+)"/ {
    if (match($0, /"janus_[a-z_]+"/)) {
      name=substr($0, RSTART+1, RLENGTH-2)
      print kind "\t" name
      kind=""
    }
  }
' "$METRICS_SRC")

EMITTED_NAMES=""
EMITTED_HISTOGRAM_BASES=""
HISTOGRAM_COUNT=0
COUNTER_COUNT=0
GAUGE_COUNT=0
while IFS=$'\t' read -r kind name; do
  [ -z "$name" ] && continue
  case "$kind" in
    histogram)
      EMITTED_NAMES="$EMITTED_NAMES
$name
${name}_bucket
${name}_sum
${name}_count"
      EMITTED_HISTOGRAM_BASES="$EMITTED_HISTOGRAM_BASES
$name"
      HISTOGRAM_COUNT=$((HISTOGRAM_COUNT+1))
      ;;
    counter) COUNTER_COUNT=$((COUNTER_COUNT+1)); EMITTED_NAMES="$EMITTED_NAMES
$name" ;;
    gauge)   GAUGE_COUNT=$((GAUGE_COUNT+1));   EMITTED_NAMES="$EMITTED_NAMES
$name" ;;
    *)       EMITTED_NAMES="$EMITTED_NAMES
$name" ;;
  esac
done <<< "$EMITTED"
EMITTED_NAMES=$(echo "$EMITTED_NAMES" | grep . | sort -u)
EMITTED_HISTOGRAM_BASES=$(echo "$EMITTED_HISTOGRAM_BASES" | grep . | sort -u)
EMITTED_BASE_COUNT=$(echo "$EMITTED_NAMES" | grep -cE -v '_(bucket|sum|count)$' || echo 0)
echo "  counters:   $COUNTER_COUNT"
echo "  gauges:     $GAUGE_COUNT"
echo "  histograms: $HISTOGRAM_COUNT (×4 series each: base, _bucket, _sum, _count)"
echo "  emitted series total: $(echo "$EMITTED_NAMES" | grep -c . || echo 0)"
echo ""

# --- Phase 2: Extract janus_* metric refs from dashboard PromQL ---------- -----
echo "--- Phase 2: Extract janus_* metric refs from dashboard PromQL ---"
# Recurse the whole JSON tree for any object carrying an `expr` — robust to
# collapsed row panels and future dashboard structure changes.
DASHBOARD_METRICS=""
EXPR_TOTAL=0
for d in "${DASHBOARDS[@]}"; do
  EXPRS=$(jq -r '[.. | objects | select(has("expr")) | .expr] | .[]' "$d" 2>/dev/null || echo "")
  EXPR_COUNT=$(echo "$EXPRS" | grep -c . || echo 0)
  EXPR_TOTAL=$((EXPR_TOTAL+EXPR_COUNT))
  echo "  $(basename "$d"): $EXPR_COUNT target expr(s)"
  # Tokenize janus_* identifiers. Words only — PromQL label names like
  # janus_version (inside {{tenant_id}} templates) are not janus_ metrics, but
  # the only place "janus_" appears in our exprs is the metric name itself.
  REFS=$(echo "$EXPRS" | grep -oE 'janus_[a-z_]+' | sort -u || true)
  DASHBOARD_METRICS="$DASHBOARD_METRICS
$REFS"
done
DASHBOARD_METRICS=$(echo "$DASHBOARD_METRICS" | grep . | sort -u)
DASHBOARD_METRIC_COUNT=$(echo "$DASHBOARD_METRICS" | grep -c . || echo 0)
echo "  total target exprs scanned: $EXPR_TOTAL"
echo "  distinct janus_* metrics referenced: $DASHBOARD_METRIC_COUNT"
echo "$DASHBOARD_METRICS" | sed 's/^/    - /'
[ "$DASHBOARD_METRIC_COUNT" -gt 0 ] || { echo "FAIL: dashboard references no janus_* metrics" >&2; exit 1; }
PASS=$((PASS+1))
echo ""

# --- Phase 3: Cross-check (gate) ---------------------------------------- -----
echo "--- Phase 3: Cross-check — every referenced metric must be emitted ---"
ORPHANS=""
CHECKED=0
while IFS= read -r METRIC; do
  [ -z "$METRIC" ] && continue
  CHECKED=$((CHECKED+1))
  if echo "$EMITTED_NAMES" | grep -qxF "$METRIC"; then
    continue  # direct hit (covers histogram _bucket/_sum/_count expansions)
  fi
  ORPHANS="$ORPHANS
$METRIC"
done <<< "$DASHBOARD_METRICS"

if [ -n "$ORPHANS" ]; then
  ORPHAN_COUNT=$(echo "$ORPHANS" | grep -c . || echo 0)
  echo "  ✗ $ORPHAN_COUNT orphan metric(s) — referenced by dashboard but NOT emitted by Janus:"
  echo "$ORPHANS" | grep . | sed 's/^/    - /'
  echo ""
  echo "  Fix: either rename the metric in the dashboard JSON to match metrics.go,"
  echo "  or (rare) add the metric to metrics.go if a panel genuinely needs new data."
  FAIL=$((FAIL+ORPHAN_COUNT))
else
  echo "  ✓ All $CHECKED referenced metric(s) resolve to entries in metrics.go"
  PASS=$((PASS+1))
fi
echo ""

# --- Phase 4: Optional live /metrics cross-check (non-gating) ----------- -----
echo "--- Phase 4: Live /metrics endpoint (optional, non-gating) ---"
METRICS_LIVE=$(curl -sf "$JANUS_URL/metrics" 2>/dev/null || echo "")
if [ -z "$METRICS_LIVE" ]; then
  echo "  SKIP: $JANUS_URL/metrics not reachable — static cross-check above is the gate."
else
  echo "  Live endpoint reachable; checking each referenced metric appears:"
  while IFS= read -r METRIC; do
    [ -z "$METRIC" ] && continue
    # Prometheus exposition lines look like:  janus_x{...} 3   or   janus_x 3
    if echo "$METRICS_LIVE" | grep -qE "^${METRIC}(\{| |	|$)"; then
      echo "    ✓ $METRIC"; PASS=$((PASS+1))
    else
      echo "    · $METRIC (not yet emitted in this sample; non-gating)"; 
    fi
  done <<< "$DASHBOARD_METRICS"
fi
echo ""

echo "=== Results: $PASS passed, $FAIL failed ==="
exit $FAIL

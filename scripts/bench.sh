#!/usr/bin/env bash
set -euo pipefail
URL="http://localhost:8080"
TENANT="pipeline"

ts() { date +%s%N; }
ms() { echo $(( ($2 - $1) / 1000000 )); }

echo "========================================"
echo "Janus v0.2.0 Performance Benchmark"
echo "========================================"

echo ""
echo "--- Test 1: Single-Task Lifecycle (10 rounds) ---"
printf "  %-6s %8s %8s %8s %8s %8s %8s\n" "Round" "publish" "pull" "start" "hb" "ack" "total"

p_sum=0; pu_sum=0; s_sum=0; h_sum=0; a_sum=0; t_sum=0
p_min=9999; pu_min=9999; s_min=9999; h_min=9999; a_min=9999; t_min=9999
p_max=0; pu_max=0; s_max=0; h_max=0; a_max=0; t_max=0

for i in $(seq 1 10); do
  TASK_ID="lat-$i-$(date +%s%N)"

  t1=$(ts)
  curl -s -X POST "$URL/v1/tenants/$TENANT/tasks" -H "Content-Type: application/json" -d "{
    \"id\":\"$TASK_ID\",\"source_agent\":\"bench\",\"target_type\":\"mailbox\",\"target_value\":\"product-inbox\",
    \"envelope\":{\"janus_version\":\"0.1\",\"task_id\":\"$TASK_ID\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"bench\",
      \"target\":{\"type\":\"mailbox\",\"value\":\"product-inbox\"},
      \"payload\":{\"type\":\"json\",\"content\":\"{\\\"i\\\":$i}\"},
      \"trace\":{\"trace_id\":\"t-$TASK_ID\"}}}" > /dev/null
  t2=$(ts); pub=$(ms $t1 $t2)

  t1=$(ts)
  PULL=$(curl -s -X POST "$URL/v1/tenants/$TENANT/mailboxes/product-inbox/pull" \
    -H "Content-Type: application/json" -d '{"agent_id":"product-agent"}')
  t2=$(ts); pull=$(ms $t1 $t2)

  LEASE=$(echo "$PULL" | jq -r '.lease.lease_id')
  TID=$(echo "$PULL" | jq -r '.task.id')

  t1=$(ts)
  curl -s -X POST "$URL/v1/tenants/$TENANT/tasks/$TID/start" \
    -H "Content-Type: application/json" -d "{\"agent_id\":\"product-agent\",\"lease_id\":\"$LEASE\"}" > /dev/null
  t2=$(ts); start=$(ms $t1 $t2)

  t1=$(ts)
  curl -s -X POST "$URL/v1/tenants/$TENANT/agents/product-agent/heartbeat" > /dev/null
  t2=$(ts); hb=$(ms $t1 $t2)

  t1=$(ts)
  curl -s -X POST "$URL/v1/tenants/$TENANT/tasks/$TID/ack" \
    -H "Content-Type: application/json" -d "{\"lease_id\":\"$LEASE\",\"result_ref\":\"r://b/$TID\"}" > /dev/null
  t2=$(ts); ack=$(ms $t1 $t2)

  total=$((pub + pull + start + hb + ack))
  printf "  %-6s %8s %8s %8s %8s %8s %8s\n" "$i" "${pub}ms" "${pull}ms" "${start}ms" "${hb}ms" "${ack}ms" "${total}ms"

  p_sum=$((p_sum+pub)); pu_sum=$((pu_sum+pull)); s_sum=$((s_sum+start)); h_sum=$((h_sum+hb)); a_sum=$((a_sum+ack)); t_sum=$((t_sum+total))
  [ $pub -lt $p_min ] && p_min=$pub; [ $pub -gt $p_max ] && p_max=$pub
  [ $pull -lt $pu_min ] && pu_min=$pull; [ $pull -gt $pu_max ] && pu_max=$pull
  [ $start -lt $s_min ] && s_min=$start; [ $start -gt $s_max ] && s_max=$start
  [ $hb -lt $h_min ] && h_min=$hb; [ $hb -gt $h_max ] && h_max=$hb
  [ $ack -lt $a_min ] && a_min=$ack; [ $ack -gt $a_max ] && a_max=$ack
  [ $total -lt $t_min ] && t_min=$total; [ $total -gt $t_max ] && t_max=$total
done

echo ""
printf "  %-6s %8s %8s %8s %8s %8s %8s\n" "avg" "$((p_sum/10))ms" "$((pu_sum/10))ms" "$((s_sum/10))ms" "$((h_sum/10))ms" "$((a_sum/10))ms" "$((t_sum/10))ms"
printf "  %-6s %8s %8s %8s %8s %8s %8s\n" "min" "${p_min}ms" "${pu_min}ms" "${s_min}ms" "${h_min}ms" "${a_min}ms" "${t_min}ms"
printf "  %-6s %8s %8s %8s %8s %8s %8s\n" "max" "${p_max}ms" "${pu_max}ms" "${s_max}ms" "${h_max}ms" "${a_max}ms" "${t_max}ms"

chain() {
  local mb=$1 ag=$2 next_mb=$3 stage=$4 round=$5
  local pr="" pi="" li=""
  for a in $(seq 1 20); do
    pr=$(curl -s -X POST "$URL/v1/tenants/$TENANT/mailboxes/$mb/pull" \
      -H "Content-Type: application/json" -d "{\"agent_id\":\"$ag\"}")
    pi=$(echo "$pr" | jq -r '.task.id // empty')
    li=$(echo "$pr" | jq -r '.lease.lease_id // empty')
    [ -n "$pi" ] && [ "$pi" != "null" ] && break
    sleep 0.1
  done
  [ -z "$pi" ] || [ "$pi" = "null" ] && return 1
  curl -s -X POST "$URL/v1/tenants/$TENANT/tasks/$pi/ack" \
    -H "Content-Type: application/json" \
    -d "{\"lease_id\":\"$li\",\"result_ref\":\"r://$ag/$pi\"}" > /dev/null
  if [ -n "$next_mb" ]; then
    local ni="${stage}-r${round}-$(date +%s%N)"
    curl -s -X POST "$URL/v1/tenants/$TENANT/tasks" -H "Content-Type: application/json" -d "{
      \"id\":\"$ni\",\"source_agent\":\"$ag\",\"target_type\":\"mailbox\",\"target_value\":\"$next_mb\",
      \"envelope\":{\"janus_version\":\"0.1\",\"task_id\":\"$ni\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"$ag\",
        \"target\":{\"type\":\"mailbox\",\"value\":\"$next_mb\"},
        \"payload\":{\"type\":\"json\",\"content\":\"{\\\"stage\\\":\\\"$stage\\\",\\\"round\\\":$round}\"},
        \"trace\":{\"trace_id\":\"p-r$round\"}}}" > /dev/null
  fi
  return 0
}

echo ""
echo "--- Test 2: 7-Agent Pipeline Sequential (10 rounds) ---"
pipe_sum=0; pipe_min=99999; pipe_max=0; pipe_ok=0
for round in $(seq 1 10); do
  REQ_ID="preq-r${round}-$(date +%s%N)"
  t_start=$(ts)

  curl -s -X POST "$URL/v1/tenants/$TENANT/tasks" -H "Content-Type: application/json" -d "{
    \"id\":\"$REQ_ID\",\"source_agent\":\"orch\",\"target_type\":\"mailbox\",\"target_value\":\"product-inbox\",
    \"envelope\":{\"janus_version\":\"0.1\",\"task_id\":\"$REQ_ID\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"orch\",
      \"target\":{\"type\":\"mailbox\",\"value\":\"product-inbox\"},
      \"payload\":{\"type\":\"json\",\"content\":\"{\\\"round\\\":$round}\"},
      \"trace\":{\"trace_id\":\"p-r$round\"}}}" > /dev/null

  ok=true
  chain product-inbox product-agent coding-inbox code $round || ok=false
  $ok && chain coding-inbox coding-agent review-inbox review $round || ok=false
  $ok && chain review-inbox review-agent test-inbox test $round || ok=false
  $ok && chain test-inbox test-agent security-inbox security $round || ok=false
  $ok && chain security-inbox security-agent approval-inbox approval $round || ok=false
  $ok && chain approval-inbox approver-agent release-inbox release $round || ok=false
  $ok && chain release-inbox release-agent "" done $round || ok=false

  t_end=$(ts); dur=$(ms $t_start $t_end)
  if $ok; then
    echo "  Round $round: ${dur}ms"
    pipe_sum=$((pipe_sum+dur)); pipe_ok=$((pipe_ok+1))
    [ $dur -lt $pipe_min ] && pipe_min=$dur
    [ $dur -gt $pipe_max ] && pipe_max=$dur
  fi
done
if [ $pipe_ok -gt 0 ]; then
  echo "  avg=$((pipe_sum/pipe_ok))ms  min=${pipe_min}ms  max=${pipe_max}ms  per-agent=$((pipe_sum/pipe_ok/7))ms  ($pipe_ok/10)"
fi

run_pipe() {
  local pid=$1 rid="cp-${pid}-$(date +%s%N)" t0=$(ts)
  curl -s -X POST "$URL/v1/tenants/$TENANT/tasks" -H "Content-Type: application/json" -d "{
    \"id\":\"$rid\",\"source_agent\":\"conc\",\"target_type\":\"mailbox\",\"target_value\":\"product-inbox\",
    \"envelope\":{\"janus_version\":\"0.1\",\"task_id\":\"$rid\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"conc\",
      \"target\":{\"type\":\"mailbox\",\"value\":\"product-inbox\"},
      \"payload\":{\"type\":\"json\",\"content\":\"{\\\"p\\\":$pid}\"},
      \"trace\":{\"trace_id\":\"cp-$pid\"}}}" > /dev/null
  local mbs=("product-inbox" "coding-inbox" "review-inbox" "test-inbox" "security-inbox" "approval-inbox" "release-inbox")
  local ags=("product-agent" "coding-agent" "review-agent" "test-agent" "security-agent" "approver-agent" "release-agent")
  local nxt=("coding-inbox" "review-inbox" "test-inbox" "security-inbox" "approval-inbox" "release-inbox" "")
  local sts=("code" "review" "test" "security" "approval" "release" "done")
  for i in 0 1 2 3 4 5 6; do
    local mb=${mbs[$i]} ag=${ags[$i]} nx=${nxt[$i]} st=${sts[$i]} pr="" pi="" li=""
    for a in $(seq 1 30); do
      pr=$(curl -s -X POST "$URL/v1/tenants/$TENANT/mailboxes/$mb/pull" -H "Content-Type: application/json" -d "{\"agent_id\":\"$ag\"}")
      pi=$(echo "$pr" | jq -r '.task.id // empty'); li=$(echo "$pr" | jq -r '.lease.lease_id // empty')
      [ -n "$pi" ] && [ "$pi" != "null" ] && break; sleep 0.1
    done
    [ -z "$pi" ] || [ "$pi" = "null" ] && echo "  Pipeline $pid FAIL at $ag" && return
    curl -s -X POST "$URL/v1/tenants/$TENANT/tasks/$pi/ack" -H "Content-Type: application/json" -d "{\"lease_id\":\"$li\",\"result_ref\":\"r://$ag/$pi\"}" > /dev/null
    if [ -n "$nx" ]; then
      local ni="${st}-c${pid}-$(date +%s%N)"
      curl -s -X POST "$URL/v1/tenants/$TENANT/tasks" -H "Content-Type: application/json" -d "{
        \"id\":\"$ni\",\"source_agent\":\"$ag\",\"target_type\":\"mailbox\",\"target_value\":\"$nx\",
        \"envelope\":{\"janus_version\":\"0.1\",\"task_id\":\"$ni\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"$ag\",
          \"target\":{\"type\":\"mailbox\",\"value\":\"$nx\"},
          \"payload\":{\"type\":\"json\",\"content\":\"{\\\"stage\\\":\\\"$st\\\",\\\"p\\\":$pid}\"},
          \"trace\":{\"trace_id\":\"cp-$pid\"}}}" > /dev/null
    fi
  done
  local t1=$(ts)
  echo "  Pipeline $pid: $(( (t1-t0)/1000000 ))ms"
}

echo ""
echo "--- Test 3: Concurrent Pipelines (5 parallel) ---"
wall_start=$(ts)
for p in 1 2 3 4 5; do run_pipe $p & done
wait
wall_end=$(ts); wall=$(( (wall_end-wall_start)/1000000 ))
echo "  wall=${wall}ms  avg=$((wall/5))ms"

echo ""
echo "--- Test 4: Publish Throughput (100 tasks) ---"
t0=$(ts)
for i in $(seq 1 100); do
  curl -s -X POST "$URL/v1/tenants/$TENANT/tasks" -H "Content-Type: application/json" -d "{
    \"id\":\"tp-$i\",\"source_agent\":\"tp\",\"target_type\":\"mailbox\",\"target_value\":\"product-inbox\",
    \"envelope\":{\"janus_version\":\"0.1\",\"task_id\":\"tp-$i\",\"tenant_id\":\"$TENANT\",\"source_agent\":\"tp\",
      \"target\":{\"type\":\"mailbox\",\"value\":\"product-inbox\"},
      \"payload\":{\"type\":\"json\",\"content\":\"{\\\"i\\\":$i}\"},
      \"trace\":{\"trace_id\":\"tp-$i\"}}}" > /dev/null
done
t1=$(ts); dur=$(( (t1-t0)/1000000 ))
echo "  100 publishes: ${dur}ms ($((100000/dur)) ops/sec)"

echo ""
echo "========================================"
echo "Done"
echo "========================================"

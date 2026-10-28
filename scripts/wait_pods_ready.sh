#!/usr/bin/env bash
# Usage: wait_pods_ready.sh <namespace> <timeout>
#
# `kubectl wait pod -l <selector>` lists first and watches after; a pod
# deleted while the watch is open fails the whole command with NotFound.
# After a rollout/rollback the replaced ReplicaSet's pods are still
# terminating within their grace period and match the same selector, so a
# selector-based ready assertion races their deletion. Pods already
# terminating (deletionTimestamp set) are excluded here; an empty pod
# list fails.
set -euo pipefail

NS=${1:?usage: wait_pods_ready.sh <namespace> <timeout>}
TIMEOUT=${2:?usage: wait_pods_ready.sh <namespace> <timeout>}
SELECTOR='app.kubernetes.io/instance=janus-core,!app.kubernetes.io/component'

attempt=1
while :; do
  PODS=$(kubectl -n "$NS" get pods -l "$SELECTOR" -o json | python3 -c '
import sys, json
items = json.load(sys.stdin)["items"]
names = [p["metadata"]["name"] for p in items
         if not p["metadata"].get("deletionTimestamp")]
print(" ".join(f"pod/{n}" for n in names))')
  [ -n "$PODS" ] || { echo "::error::no janus-core pods matched -l $SELECTOR in namespace $NS"; exit 1; }
  echo "waiting for Ready: $PODS"
  LOG=$(mktemp /tmp/wait-pods-ready.XXXXXX)
  set +e
  kubectl -n "$NS" wait --for=condition=ready --timeout="$TIMEOUT" $PODS >"$LOG" 2>&1
  rc=$?
  set -e
  cat "$LOG"
  if [ "$rc" -eq 0 ]; then
    rm -f "$LOG"
    exit 0
  fi
  if grep -q 'NotFound' "$LOG" && [ "$attempt" -lt 3 ]; then
    echo "::warning::listed pod deleted mid-wait; re-listing (attempt $attempt/3)"
    rm -f "$LOG"
    attempt=$((attempt + 1))
    continue
  fi
  rm -f "$LOG"
  exit "$rc"
done

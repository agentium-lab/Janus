#!/bin/bash
# Syncs canonical migrations + schema into the Helm chart.
# Run from repo root. CI also validates the sync is current.
set -euo pipefail
cp migrations/*.sql deployments/helm/janus-core/migrations/
echo "Helm migrations synced from canonical"

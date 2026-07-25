# Janus Core Operations Runbook

## Helm Install / Upgrade / Rollback

### Install
```bash
helm upgrade --install janus-core ./deployments/helm/janus-core \
  --namespace janus \
  --create-namespace \
  --set postgres.password="$PG_PASSWORD" \
  --set redis.password="$REDIS_PASSWORD" \
  --wait
```

### Upgrade
```bash
helm upgrade janus-core ./deployments/helm/janus-core \
  --namespace janus \
  --set postgres.password="$PG_PASSWORD" \
  --set redis.password="$REDIS_PASSWORD" \
  --wait
```

### Rollback
```bash
helm rollback janus-core <revision> --namespace janus
```

View history:
```bash
helm history janus-core --namespace janus
```

## Controlled Migration

Migrations run as a Helm pre-install/pre-upgrade Job (`janus-core-migration`). The deployment waits for the migration Job to report healthy before starting pods.

### Manual migration check
```bash
kubectl run migration-check --rm -i --restart=Never \
  --image=ghcr.io/agentium-lab/janus-core:latest \
  --env="JANUS_PG_HOST=postgres" \
  --env="JANUS_PG_PASSWORD=$PG_PASSWORD" \
  -- ./janus-migration-probe
```

### Force migration re-run
Delete the Helm hook secret and upgrade:
```bash
kubectl delete job janus-core-migration -n janus
helm upgrade janus-core ./deployments/helm/janus-core --namespace janus
```

## Artifact PVC

Artifacts are stored on a PVC mounted at `/app/artifacts`. The chart creates a PVC named `{release}-artifacts`.

### Resize PVC
```bash
kubectl patch pvc janus-core-artifacts -n janus \
  -p '{"spec":{"resources":{"requests":{"storage":"20Gi"}}}}'
```

> Note: Storage class must support volume expansion.

### Backup artifacts
```bash
kubectl exec deploy/janus-core -n janus -- tar czf - /app/artifacts > artifacts-backup.tar.gz
```

## Prometheus Scrape Config

The chart adds the following annotations to the Service:
```yaml
prometheus.io/scrape: "true"
prometheus.io/port: "8080"
prometheus.io/path: "/metrics"
```

### Prometheus ServiceMonitor (optional)
```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: janus-core
  namespace: janus
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: janus-core
  endpoints:
    - port: http
      path: /metrics
      interval: 15s
```

## Backup / Restore

### PostgreSQL

**Dump:**
```bash
pg_dump -h $PG_HOST -U janus -d janus -Fc > janus-$(date +%Y%m%d).dump
```

**Restore:**
```bash
pg_restore -h $PG_HOST -U janus -d janus --clean --if-exists janus-YYYYMMDD.dump
```

### NATS Persistence

NATS JetStream streams are stored on NATS server volumes. Back up the NATS data directory or use NATS stream snapshots:

```bash
nats stream snapshot JANUS_default_TASKS --dir /backup/nats
```

## Rolling Upgrade Procedure

1. **Check current health:**
   ```bash
   kubectl get pods -n janus -l app.kubernetes.io/name=janus-core
   ```

2. **Run migration probe:**
   ```bash
   ./janus-migration-probe
   ```

3. **Upgrade with Helm:**
   ```bash
   helm upgrade janus-core ./deployments/helm/janus-core --namespace janus --wait
   ```

4. **Verify new pods:**
   ```bash
   kubectl rollout status deploy/janus-core -n janus
   ```

5. **Run persistence probes:**
   ```bash
   ./janus-nats-persistence-probe
   ./janus-event-replay-probe
   ```

## OTLP Tracing Setup

Janus Core can export traces via OTLP. Configure the OpenTelemetry collector sidecar or environment variables:

```yaml
extraEnv:
  - name: OTEL_EXPORTER_OTLP_ENDPOINT
    value: "http://otel-collector.monitoring:4317"
  - name: OTEL_SERVICE_NAME
    value: "janus-core"
  - name: OTEL_TRACES_SAMPLER
    value: "parentbased_traceidratio"
  - name: OTEL_TRACES_SAMPLER_ARG
    value: "0.1"
```

### Jaeger Query
```bash
kubectl port-forward svc/jaeger-query 16686:16686 -n monitoring
```

## Troubleshooting

### Pod stuck in Init
Check the migration Job:
```bash
kubectl logs job/janus-core-migration -n janus
```

### High queue backlog
Check mailbox consumers and agent heartbeats:
```bash
# Queue backlog metric
sum(janus_queue_backlog) by (tenant_id, mailbox_id)

# Agents online
sum(janus_agent_online) by (tenant_id)
```

### Policy denials spike
```bash
sum(rate(janus_policy_denied_total[5m])) by (tenant_id)
```

### Budget throttles
```bash
sum(rate(janus_budget_throttle_total[5m])) by (tenant_id, reason)
```

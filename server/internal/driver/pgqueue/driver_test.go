package pgqueue

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

// Cross-tenant isolation: same task ID + mailbox in two tenants; pulling from
// one must never return or mutate the other's data. Requires PG at localhost.
func TestFetchTasks_TenantIsolation(t *testing.T) {
	dsn := "postgres://silv@localhost:5432/janus_test?sslmode=disable"
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Skipf("pg not available: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(context.Background()); err != nil {
		t.Skipf("pg not reachable: %v", err)
	}

	ctx := context.Background()
	d := NewDriver(pool)

	_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE tenant_id IN ('iso-a','iso-b')`)
	_, _ = pool.Exec(ctx, `DELETE FROM tenants WHERE id IN ('iso-a','iso-b')`)
	_, _ = pool.Exec(ctx, `INSERT INTO tenants (id,name) VALUES ('iso-a','Iso A'),('iso-b','Iso B')`)
	defer func() {
		_, _ = pool.Exec(ctx, `DELETE FROM tasks WHERE tenant_id IN ('iso-a','iso-b')`)
		_, _ = pool.Exec(ctx, `DELETE FROM tenants WHERE id IN ('iso-a','iso-b')`)
	}()

	// Same task ID + mailbox, different secret in envelope payload per tenant.
	_, _ = pool.Exec(ctx, `INSERT INTO tasks (tenant_id,id,source_agent,target_type,target_value,mailbox_id,status,priority,envelope,created_at,updated_at)
		VALUES ('iso-a','iso-task','agent-a','mailbox','iso-mb','iso-mb','queued','normal',
		'{"janus_version":"1.0","payload":{"secret":"tenant-A-data"}}',now(),now())`)
	_, _ = pool.Exec(ctx, `INSERT INTO tasks (tenant_id,id,source_agent,target_type,target_value,mailbox_id,status,priority,envelope,created_at,updated_at)
		VALUES ('iso-b','iso-task','agent-b','mailbox','iso-mb','iso-mb','queued','normal',
		'{"janus_version":"1.0","payload":{"secret":"tenant-B-data"}}',now(),now())`)

	deliveries, err := d.FetchTasks(ctx, "iso-a", "iso-mb", core.FetchOptions{MaxMessages: 1, WaitTime: time.Second})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("want 1 delivery, got %d", len(deliveries))
	}

	payload := string(deliveries[0].Payload)
	if !strings.Contains(payload, "tenant-A-data") {
		t.Fatalf("expected tenant-A-data, got: %s", payload)
	}
	if strings.Contains(payload, "tenant-B-data") {
		t.Fatal("CROSS-TENANT LEAK: tenant B data in tenant A delivery")
	}

	// Verify only tenant A's task got the lease.
	var leaseB *time.Time
	_ = pool.QueryRow(ctx, `SELECT queue_lease_until FROM tasks WHERE tenant_id='iso-b' AND id='iso-task'`).Scan(&leaseB)
	if leaseB != nil {
		t.Fatal("CROSS-TENANT MUTATION: tenant B task got a lease from tenant A's pull")
	}
}

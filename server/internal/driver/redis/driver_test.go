package redis

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

var redisAddr string

func startRedisServer(t *testing.T) {
	t.Helper()
	redisAddr = os.Getenv("JANUS_REDIS_ADDR")
	if redisAddr != "" {
		return
	}

	port := 16379
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err == nil {
		c.Close()
		redisAddr = addr
		return
	}

	cmd := exec.Command(os.ExpandEnv("$HOME/.local/bin/redis-server"),
		"--port", fmt.Sprintf("%d", port), "--save", "", "--appendonly", "no",
		"--dir", t.TempDir())
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cmd.Process.Signal(os.Interrupt)
		cmd.Wait()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for redis-server to start")
		default:
		}
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			redisAddr = addr
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func openDriver(t *testing.T) *Driver {
	startRedisServer(t)
	d, err := NewDriver(Config{Addr: redisAddr})
	require.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	return d
}

func TestDriver_PingAndGet(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	err := d.Ping(ctx, "acme", "agent-1")
	require.NoError(t, err)

	hb, err := d.GetLastHeartbeat(ctx, "acme", "agent-1")
	require.NoError(t, err)
	require.NotNil(t, hb)
	assert.WithinDuration(t, time.Now().UTC(), *hb, 2*time.Second)
}

func TestDriver_GetLastHeartbeatNotSet(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	hb, err := d.GetLastHeartbeat(ctx, "acme", "nonexistent")
	require.NoError(t, err)
	assert.Nil(t, hb)
}

func TestDriver_PingOverwrite(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	require.NoError(t, d.Ping(ctx, "acme", "agent-2"))
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, d.Ping(ctx, "acme", "agent-2"))

	hb, err := d.GetLastHeartbeat(ctx, "acme", "agent-2")
	require.NoError(t, err)
	assert.NotNil(t, hb)
}

func TestDriver_Remove(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	require.NoError(t, d.Ping(ctx, "acme", "agent-3"))
	require.NoError(t, d.Remove(ctx, "acme", "agent-3"))

	hb, err := d.GetLastHeartbeat(ctx, "acme", "agent-3")
	require.NoError(t, err)
	assert.Nil(t, hb)
}

func TestDriver_RemoveNonexistent(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	err := d.Remove(ctx, "acme", "nonexistent")
	assert.NoError(t, err)
}

func TestDriver_ScanExpired(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	require.NoError(t, d.Ping(ctx, "acme", "alive-agent"))
	d.rdb.Set(ctx, heartbeatKey("acme", "stale-agent"), "0", 0)

	expired, err := d.ScanExpired(ctx, "acme")
	require.NoError(t, err)
	assert.Contains(t, expired, "stale-agent")
	assert.NotContains(t, expired, "alive-agent")
}

func TestDriver_ScanExpiredEmpty(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	expired, err := d.ScanExpired(ctx, "empty-tenant")
	require.NoError(t, err)
	assert.Len(t, expired, 0)
}

func TestDriver_MultipleTenants(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	require.NoError(t, d.Ping(ctx, "tenant-a", "agent-1"))
	require.NoError(t, d.Ping(ctx, "tenant-b", "agent-1"))

	hbA, err := d.GetLastHeartbeat(ctx, "tenant-a", "agent-1")
	require.NoError(t, err)
	assert.NotNil(t, hbA)

	hbB, err := d.GetLastHeartbeat(ctx, "tenant-b", "agent-1")
	require.NoError(t, err)
	assert.NotNil(t, hbB)

	require.NoError(t, d.Remove(ctx, "tenant-a", "agent-1"))
	hbA, err = d.GetLastHeartbeat(ctx, "tenant-a", "agent-1")
	require.NoError(t, err)
	assert.Nil(t, hbA)

	hbB, err = d.GetLastHeartbeat(ctx, "tenant-b", "agent-1")
	require.NoError(t, err)
	assert.NotNil(t, hbB)
}

func TestDriver_Close(t *testing.T) {
	startRedisServer(t)
	d, err := NewDriver(Config{Addr: redisAddr})
	require.NoError(t, err)
	require.NoError(t, d.Close())
}

func TestDriver_NewDriverBadAddr(t *testing.T) {
	_, err := NewDriver(Config{Addr: "127.0.0.1:1"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connect to Redis")
}

func TestDriver_Client(t *testing.T) {
	d := openDriver(t)
	assert.NotNil(t, d.Client())
}

func TestHeartbeatKey(t *testing.T) {
	assert.Equal(t, "agent:heartbeat:acme:agent-1", heartbeatKey("acme", "agent-1"))
}

func TestExtractAgentID(t *testing.T) {
	assert.Equal(t, "agent-1", extractAgentID("agent:heartbeat:acme:agent-1", "acme"))
	assert.Equal(t, "reviewer.default", extractAgentID("agent:heartbeat:acme:reviewer.default", "acme"))
}

func TestDriver_PingManyAgents(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		require.NoError(t, d.Ping(ctx, "acme", fmt.Sprintf("agent-%03d", i)))
	}

	for i := 0; i < 20; i++ {
		hb, err := d.GetLastHeartbeat(ctx, "acme", fmt.Sprintf("agent-%03d", i))
		require.NoError(t, err)
		assert.NotNil(t, hb)
	}

	d.rdb.Set(ctx, heartbeatKey("acme", "agent-005"), "0", 0)
	d.rdb.Set(ctx, heartbeatKey("acme", "agent-010"), "0", 0)

	expired, err := d.ScanExpired(ctx, "acme")
	require.NoError(t, err)
	assert.Len(t, expired, 2)
	assert.Contains(t, expired, "agent-005")
	assert.Contains(t, expired, "agent-010")
}

func TestDriver_InterfaceConformance(t *testing.T) {
	d := openDriver(t)
	var _ core.HeartbeatDriver = d
	_ = d
}

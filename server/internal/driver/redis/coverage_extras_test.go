package redis

import (
	"context"
	"testing"
	"time"

	go_redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDriver_CheckRPM_ZeroLimit(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	err := d.CheckRPM(ctx, "acme", "agent", "agent-1", 0)
	assert.NoError(t, err)
	err = d.CheckRPM(ctx, "acme", "agent", "agent-1", -1)
	assert.NoError(t, err)
}

func TestDriver_CheckRPM_UnderLimit(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		err := d.CheckRPM(ctx, "acme", "agent", "agent-1", 10)
		require.NoError(t, err)
	}
}

func TestDriver_CheckRPM_AtLimit(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		err := d.CheckRPM(ctx, "acme", "agent", "agent-1", 10)
		require.NoError(t, err)
	}
}

func TestDriver_CheckRPM_ExceedsLimit(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_ = d.CheckRPM(ctx, "acme", "agent", "agent-1", 10)
	}
	err := d.CheckRPM(ctx, "acme", "agent", "agent-1", 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rpm limit exceeded")
}

func TestDriver_CheckRPM_DifferentScopes(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		err := d.CheckRPM(ctx, "acme", "agent", "agent-1", 10)
		require.NoError(t, err)
	}
	for i := 0; i < 5; i++ {
		err := d.CheckRPM(ctx, "acme", "agent", "agent-2", 10)
		require.NoError(t, err)
	}
}

func TestDriver_CheckTPM_ZeroLimit(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	err := d.CheckTPM(ctx, "acme", "agent", "agent-1", 0, 100)
	assert.NoError(t, err)
	err = d.CheckTPM(ctx, "acme", "agent", "agent-1", 100, 0)
	assert.NoError(t, err)
	err = d.CheckTPM(ctx, "acme", "agent", "agent-1", -1, 100)
	assert.NoError(t, err)
}

func TestDriver_CheckTPM_UnderLimit(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	err := d.CheckTPM(ctx, "acme", "agent", "agent-1", 1000, 100)
	require.NoError(t, err)
	err = d.CheckTPM(ctx, "acme", "agent", "agent-1", 1000, 200)
	require.NoError(t, err)
}

func TestDriver_CheckTPM_ExceedsLimit(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	err := d.CheckTPM(ctx, "acme", "agent", "agent-1", 500, 600)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tpm limit exceeded")
}

func TestDriver_CheckTPM_DifferentScopes(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	err := d.CheckTPM(ctx, "acme", "agent", "agent-1", 1000, 500)
	require.NoError(t, err)
	err = d.CheckTPM(ctx, "acme", "agent", "agent-2", 1000, 500)
	require.NoError(t, err)
}

func TestDriver_GetLastHeartbeat_ExistingReturnsTime(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	require.NoError(t, d.Ping(ctx, "acme", "agent-1"))
	hb, err := d.GetLastHeartbeat(ctx, "acme", "agent-1")
	require.NoError(t, err)
	require.NotNil(t, hb)
	assert.WithinDuration(t, time.Now().UTC(), *hb, 2*time.Second)
}

func TestDriver_GetLastHeartbeat_PingTwice(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	require.NoError(t, d.Ping(ctx, "acme", "agent-1"))
	time.Sleep(20 * time.Millisecond)
	require.NoError(t, d.Ping(ctx, "acme", "agent-1"))
	hb, err := d.GetLastHeartbeat(ctx, "acme", "agent-1")
	require.NoError(t, err)
	require.NotNil(t, hb)
}

func TestDriver_ScanExpired_WithAliveAndStale(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()

	require.NoError(t, d.Ping(ctx, "acme2", "alive-agent"))
	zsetKey := heartbeatSetKey("acme2")
	d.rdb.ZAdd(ctx, zsetKey, go_redis.Z{
		Score:  float64(time.Now().Add(-2 * time.Hour).UTC().UnixMilli()),
		Member: "stale-1",
	})
	d.rdb.ZAdd(ctx, zsetKey, go_redis.Z{
		Score:  float64(time.Now().Add(-1 * time.Hour).UTC().UnixMilli()),
		Member: "stale-2",
	})

	expired, err := d.ScanExpired(ctx, "acme2")
	require.NoError(t, err)
	assert.Contains(t, expired, "stale-1")
	assert.Contains(t, expired, "stale-2")
	assert.NotContains(t, expired, "alive-agent")
}

func TestDriver_Remove_ExistingAgent(t *testing.T) {
	d := openDriver(t)
	ctx := context.Background()
	require.NoError(t, d.Ping(ctx, "acme3", "agent-1"))
	require.NoError(t, d.Remove(ctx, "acme3", "agent-1"))
	hb, err := d.GetLastHeartbeat(ctx, "acme3", "agent-1")
	require.NoError(t, err)
	assert.Nil(t, hb)
}
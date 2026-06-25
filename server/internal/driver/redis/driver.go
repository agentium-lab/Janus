package redis

import (
	"context"
	"fmt"
	"time"

	go_redis "github.com/redis/go-redis/v9"

	"github.com/agentium-lab/Janus/core"
)

const (
	heartbeatSetPrefix = "agent:heartbeat:"
	heartbeatTTL       = 60 * time.Second
)

type Config struct {
	Addr     string
	Password string
	DB       int
}

type Driver struct {
	rdb *go_redis.Client
}

func NewDriver(cfg Config) (*Driver, error) {
	rdb := go_redis.NewClient(&go_redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("connect to Redis: %w", err)
	}

	return &Driver{rdb: rdb}, nil
}

func (d *Driver) Ping(ctx context.Context, tenantID, agentID string) error {
	key := heartbeatSetKey(tenantID)
	expireAt := float64(time.Now().Add(heartbeatTTL).UTC().UnixMilli())
	return d.rdb.ZAdd(ctx, key, go_redis.Z{
		Score:  expireAt,
		Member: agentID,
	}).Err()
}

func (d *Driver) GetLastHeartbeat(ctx context.Context, tenantID, agentID string) (*time.Time, error) {
	key := heartbeatSetKey(tenantID)
	score, err := d.rdb.ZScore(ctx, key, agentID).Result()
	if err == go_redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get heartbeat %s/%s: %w", tenantID, agentID, err)
	}

	expireAtMs := int64(score)
	lastPing := time.UnixMilli(expireAtMs - heartbeatTTL.Milliseconds()).UTC()
	return &lastPing, nil
}

func (d *Driver) ScanExpired(ctx context.Context, tenantID string) ([]string, error) {
	key := heartbeatSetKey(tenantID)
	now := float64(time.Now().UTC().UnixMilli())

	members, err := d.rdb.ZRangeArgs(ctx, go_redis.ZRangeArgs{
		Key:     key,
		Start:   "-inf",
		Stop:    now,
		ByScore: true,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("scan expired heartbeats for tenant %s: %w", tenantID, err)
	}

	return members, nil
}

func (d *Driver) Remove(ctx context.Context, tenantID, agentID string) error {
	key := heartbeatSetKey(tenantID)
	return d.rdb.ZRem(ctx, key, agentID).Err()
}

func (d *Driver) Close() error {
	return d.rdb.Close()
}

func (d *Driver) Client() *go_redis.Client {
	return d.rdb
}

func (d *Driver) CheckRPM(ctx context.Context, tenantID, scopeType, scopeID string, limit int) error {
	if limit <= 0 {
		return nil
	}
	key := fmt.Sprintf("ratelimit:rpm:%s:%s:%s:%s", tenantID, scopeType, scopeID, time.Now().UTC().Truncate(time.Minute).Format("200601021504"))
	count, err := d.rdb.Incr(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("rpm check: %w", err)
	}
	if count == 1 {
		d.rdb.Expire(ctx, key, 2*time.Minute)
	}
	if int(count) > limit {
		return fmt.Errorf("rpm limit exceeded: %d > %d for %s/%s/%s", count, limit, tenantID, scopeType, scopeID)
	}
	return nil
}

func (d *Driver) CheckTPM(ctx context.Context, tenantID, scopeType, scopeID string, limit, tokenCount int) error {
	if limit <= 0 || tokenCount <= 0 {
		return nil
	}
	key := fmt.Sprintf("ratelimit:tpm:%s:%s:%s", tenantID, scopeType, scopeID)
	added, err := d.rdb.IncrBy(ctx, key, int64(tokenCount)).Result()
	if err != nil {
		return fmt.Errorf("tpm check: %w", err)
	}
	ttl, _ := d.rdb.TTL(ctx, key).Result()
	if ttl < 0 {
		d.rdb.Expire(ctx, key, 2*time.Minute)
	}
	if int(added) > limit {
		return fmt.Errorf("tpm limit exceeded: %d > %d for %s/%s/%s", added, limit, tenantID, scopeType, scopeID)
	}
	return nil
}

func heartbeatSetKey(tenantID string) string {
	return fmt.Sprintf("%s%s", heartbeatSetPrefix, tenantID)
}

var _ core.HeartbeatDriver = (*Driver)(nil)

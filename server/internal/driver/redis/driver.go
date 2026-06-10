package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	go_redis "github.com/redis/go-redis/v9"

	"github.com/agentium-lab/Janus/core"
)

const (
	heartbeatKeyPrefix = "agent:heartbeat:"
	heartbeatTTL       = 60 * time.Second
	scanBatchSize      = 100
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
	key := heartbeatKey(tenantID, agentID)
	now := time.Now().UTC().UnixMilli()
	return d.rdb.Set(ctx, key, now, heartbeatTTL).Err()
}

func (d *Driver) GetLastHeartbeat(ctx context.Context, tenantID, agentID string) (*time.Time, error) {
	key := heartbeatKey(tenantID, agentID)
	val, err := d.rdb.Get(ctx, key).Result()
	if err == go_redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get heartbeat %s: %w", key, err)
	}

	ms, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse heartbeat timestamp: %w", err)
	}

	t := time.UnixMilli(ms).UTC()
	return &t, nil
}

func (d *Driver) ScanExpired(ctx context.Context, tenantID string) ([]string, error) {
	pattern := heartbeatKey(tenantID, "*")
	var expiredAgents []string

	var cursor uint64
	for {
		keys, nextCursor, err := d.rdb.Scan(ctx, cursor, pattern, scanBatchSize).Result()
		if err != nil {
			return nil, fmt.Errorf("scan heartbeats for tenant %s: %w", tenantID, err)
		}

		if len(keys) > 0 {
			for _, key := range keys {
				ttl, err := d.rdb.TTL(ctx, key).Result()
				if err != nil {
					continue
				}
				if ttl < 0 {
					agentID := extractAgentID(key, tenantID)
					expiredAgents = append(expiredAgents, agentID)
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return expiredAgents, nil
}

func (d *Driver) Remove(ctx context.Context, tenantID, agentID string) error {
	key := heartbeatKey(tenantID, agentID)
	return d.rdb.Del(ctx, key).Err()
}

func (d *Driver) Close() error {
	return d.rdb.Close()
}

func (d *Driver) Client() *go_redis.Client {
	return d.rdb
}

func heartbeatKey(tenantID, agentID string) string {
	return fmt.Sprintf("%s%s:%s", heartbeatKeyPrefix, tenantID, agentID)
}

func extractAgentID(key, tenantID string) string {
	prefix := fmt.Sprintf("%s%s:", heartbeatKeyPrefix, tenantID)
	return strings.TrimPrefix(key, prefix)
}

var _ core.HeartbeatDriver = (*Driver)(nil)
